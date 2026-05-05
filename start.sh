#!/usr/bin/env bash
# start.sh — bring up the client-requested deployment end-to-end.
#
#   - Artemis: Docker (from D:/pinkline/code/messaging-infra)
#   - Everything else (Zookeeper, Kafka, Kafdrop, Kafka Connect, bridge,
#     RabbitMQ, SCADA API): minikube
#
# Idempotent: re-running after a crash or partial failure converges to the
# desired state. Safe to invoke repeatedly.
#
# Prerequisites: docker, minikube, kubectl on PATH.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Default MESSAGING_INFRA matches FRESH-PC-SETUP.md / MANUAL-RUN.md.
# Override by exporting MESSAGING_INFRA before invoking this script.
MESSAGING_INFRA="${MESSAGING_INFRA:-/d/pinkline/messaging-infra}"

# Set START_PF=0 to skip the auto-port-forward block at the end (e.g. if you
# already have your own port-forwards running, or you only want the cluster up).
START_PF="${START_PF:-1}"

log()  { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*" >&2; exit 1; }

# ── 0. Kill rogue host containers that bind ports we'll port-forward ────
# A previous standalone `docker run` of scada-api / monitor / etc. can keep
# binding 0.0.0.0:8091 etc. after we move to k8s. Chrome then hits the old
# container instead of `kubectl port-forward` (which binds 127.0.0.1 only).
# Symptom: dashboards show old UI no matter how many times you rebuild.
log "Removing any host-side scada-api/monitor/demo containers"
for name in pas-scada-api pas-scada-monitor pas-scada-demo external-scada-scada-api; do
  if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    docker rm -f "$name" >/dev/null 2>&1 || true
    ok "removed rogue container: $name"
  fi
done

# ── 0b. Detect containers from a different project bound to Artemis ports ─
# Common case on a shared dev PC: a `rail-artemis` (or similar) container
# from another project is bound to 8161 / 61616, so Step 1 fails with
# "port is already allocated". Warn loudly so the user can stop it.
log "Checking for port conflicts on 8161 / 61616"
PORT_HOG=$(docker ps --format '{{.Names}}\t{{.Ports}}' \
  | grep -E ':(8161|61616)->' | grep -v -E '^\s*artemis\s' || true)
if [ -n "$PORT_HOG" ]; then
  warn "Another container is bound to Artemis ports — Step 1 will fail:"
  printf '%s\n' "$PORT_HOG" | sed 's/^/      /'
  warn "Stop it with: docker stop <name>   (you can restart it later with: docker start <name>)"
  warn "Continuing anyway — docker compose may still error below."
fi

# ── 1. minikube ─────────────────────────────────────────────────────────
log "Checking minikube"
if minikube status 2>/dev/null | grep -q "host: Running"; then
  ok "minikube already running"
else
  minikube start --cpus=4 --memory=6144 --driver=docker
  ok "minikube started"
fi

# ── 2. Artemis (Docker on host) ─────────────────────────────────────────
log "Starting Artemis from $MESSAGING_INFRA"
[ -f "$MESSAGING_INFRA/docker-compose.yml" ] \
  || die "$MESSAGING_INFRA/docker-compose.yml not found — set MESSAGING_INFRA env var if path differs"
docker compose -f "$MESSAGING_INFRA/docker-compose.yml" up -d
ok "Artemis up (port 61616 / console 8161)"

# ── 3. Build Connect image if not present ───────────────────────────────
log "Connect image"
if docker image inspect pinkline/pas-scada-connect:latest >/dev/null 2>&1; then
  ok "pinkline/pas-scada-connect:latest already built"
else
  warn "building pinkline/pas-scada-connect:latest (this takes a few minutes the first time)"
  docker build -t pinkline/pas-scada-connect:latest "$SCRIPT_DIR/connect/"
  ok "Connect image built"
fi

# ── 3b. Build Bridge image from current source ──────────────────────────
# Rebuild whenever Java sources change. Maven layer cache keeps it cheap
# when nothing changed; full build is ~3 min on cold cache.
log "Building Bridge image"
docker build -q -t pinkline/pas-scada-bridge:latest "$SCRIPT_DIR/tms/" >/dev/null
ok "bridge image built"

# ── 4. Build SCADA API image from current source ────────────────────────
# Always rebuild so app.py edits ship into the running pod. Layers are
# cached so this is cheap when nothing changed.
log "Building SCADA API image"
docker build -q \
  -t ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest \
  -t external-scada-scada-api:latest \
  "$SCRIPT_DIR/external-scada/scada-api/" >/dev/null
ok "scada-api image built"

# ── 4b. Build Monitor + Demo images ─────────────────────────────────────
log "Building Monitor image"
docker build -q -t pinkline/pas-scada-monitor:latest "$SCRIPT_DIR/monitor/" >/dev/null
ok "monitor image built"

log "Building Demo image"
docker build -q -t pinkline/pas-scada-demo:1.0.0 "$SCRIPT_DIR/demo/" >/dev/null
ok "demo image built"

# ── 5. Load images into minikube ────────────────────────────────────────
log "Loading images into minikube"
IMAGES=(
  pinkline/pas-scada-bridge:latest
  pinkline/pas-scada-connect:latest
  pinkline/pas-scada-monitor:latest
  pinkline/pas-scada-demo:1.0.0
  ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest
  obsidiandynamics/kafdrop:4.0.1
  confluentinc/cp-zookeeper:7.5.0
  confluentinc/cp-kafka:7.5.0
  rabbitmq:3.12-management
  curlimages/curl:8.10.1
)
for img in "${IMAGES[@]}"; do
  # For locally-built images (bridge, connect, scada-api), HARD-REPLACE so
  # rebuilds always take effect. `minikube image load --overwrite` is
  # unreliable across versions, so rmi from inside the minikube node first.
  case "$img" in
    pinkline/*|ghcr.io/thirunavukkarasuthangaraj/*)
      minikube ssh -- "docker rmi -f $img" >/dev/null 2>&1 || true
      minikube image load "$img" >/dev/null 2>&1 \
        && ok "loaded (force-replaced): $img" \
        || warn "failed to load: $img"
      ;;
    *)
      if minikube image ls 2>/dev/null | grep -qF "$img"; then
        ok "already loaded: $img"
      else
        minikube image load "$img" && ok "loaded: $img" || warn "failed to load: $img"
      fi
      ;;
  esac
done

# ── 6. Apply tms/k8s manifests ──────────────────────────────────────────
log "Applying tms/k8s manifests"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/00-namespace.yaml"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/20-zookeeper.yaml"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/30-kafka.yaml"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/40-kafdrop.yaml"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/overlay-minikube.yaml"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/deployment.yaml"
ok "tms manifests applied"

# ── 7. Apply external-scada/k8s manifests ───────────────────────────────
log "Applying external-scada/k8s manifests"
for f in 00-namespace.yaml 10-rabbitmq-configmap.yaml 20-rabbitmq-secret.yaml \
         30-rabbitmq-pvc.yaml 40-rabbitmq-deployment.yaml 50-rabbitmq-service.yaml \
         60-scada-api-secret.yaml 70-scada-api-deployment.yaml; do
  kubectl apply -f "$SCRIPT_DIR/external-scada/k8s/$f"
done
ok "scada manifests applied"

# ── 8. Patch imagePullPolicy + bridge probe timings ─────────────────────
# imagePullPolicy=IfNotPresent  → use loaded local images instead of pulling :latest
# Bridge probes need ~3min headroom — Spring Boot + Camel boot takes ~100s on minikube.
log "Patching imagePullPolicy and bridge probe timings"
kubectl -n pinkline patch deploy pas-scada-bridge --type=json \
  -p='[
    {"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"},
    {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/initialDelaySeconds","value":180},
    {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/failureThreshold","value":5},
    {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/initialDelaySeconds","value":120},
    {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/failureThreshold","value":10}
  ]' 2>/dev/null \
  || kubectl -n pinkline patch deploy pas-scada-bridge --type=json \
       -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' \
       2>/dev/null || true
kubectl -n scada patch deploy scada-api --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' \
  2>/dev/null \
  || kubectl -n scada patch deploy scada-api --type=json \
       -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' \
       2>/dev/null || true
ok "pull policies + probe timings patched"

# Force a rollout restart NOW so pods pick up freshly-loaded images with
# correct probe timings already in place. Triggers redeploy of the
# locally-built apps; no-op on first run when no rollout history exists.
log "Restarting locally-built deployments to pick up rebuilt images"
kubectl -n scada    rollout restart deploy/scada-api          2>/dev/null || true
kubectl -n pinkline rollout restart deploy/pas-scada-bridge   2>/dev/null || true
ok "rollout restarts kicked"

# ── 9. Wait for Kafka, then create topics ───────────────────────────────
log "Waiting for Kafka"
kubectl -n pinkline rollout status deploy/kafka --timeout=300s
kubectl -n pinkline wait --for=condition=ready pod -l app=kafka --timeout=300s
ok "Kafka ready"

log "Bootstrapping Kafka topics"
kubectl -n pinkline delete job bootstrap-kafka-topics --ignore-not-found
kubectl apply -f "$SCRIPT_DIR/bootstrap/k8s/10-kafka-topics-job.yaml"
kubectl -n pinkline wait --for=condition=complete job/bootstrap-kafka-topics --timeout=240s
ok "topics ready"

# ── 10. Wait for RabbitMQ, declare scada queue + binding in-cluster ─────
log "Waiting for RabbitMQ"
kubectl -n scada rollout status deploy/rabbitmq --timeout=300s
kubectl -n scada wait --for=condition=ready pod -l app=rabbitmq --timeout=300s
ok "RabbitMQ ready"

log "Declaring scada.tms.alarms.queue + binding"
# Use rabbitmqadmin inside the rabbitmq pod itself. This avoids:
#   - rabbitmqctl eval (image-version sensitive, has failed silently before)
#   - kubectl run --rm <curl pod> (some antiviruses, e.g. K7 Total Security,
#     block the spawned PowerShell process with EPERM uv_spawn).
# Both commands are idempotent (rabbitmqadmin returns 0 on "already exists").
kubectl -n scada exec deploy/rabbitmq -- \
  rabbitmqadmin --username=thiru --password=password \
    declare queue name=scada.tms.alarms.queue durable=true auto_delete=false \
  >/dev/null 2>&1 \
  && ok "queue declared (or already existed)" \
  || warn "queue declare failed — verify with: kubectl -n scada exec deploy/rabbitmq -- rabbitmqctl list_queues"

kubectl -n scada exec deploy/rabbitmq -- \
  rabbitmqadmin --username=thiru --password=password \
    declare binding source=amq.topic destination=scada.tms.alarms.queue routing_key=scada.tms.alarms \
  >/dev/null 2>&1 \
  && ok "binding declared (or already existed)" \
  || warn "binding declare failed — verify in the RabbitMQ admin UI"

# ── 11. Apply Connect + register connectors ─────────────────────────────
log "Applying connect/k8s manifests"
kubectl apply -f "$SCRIPT_DIR/connect/k8s/10-secret.yaml"
kubectl apply -f "$SCRIPT_DIR/connect/k8s/20-configmap.yaml"
kubectl apply -f "$SCRIPT_DIR/connect/k8s/30-deployment.yaml"
# Restart so a force-loaded connect image rolls into the running pod.
kubectl -n pinkline rollout restart deploy/kafka-connect 2>/dev/null || true
ok "Connect deployment applied"

log "Waiting for Kafka Connect REST API"
kubectl -n pinkline rollout status deploy/kafka-connect --timeout=480s
kubectl -n pinkline wait --for=condition=ready pod -l app=kafka-connect --timeout=480s
ok "Connect ready"

log "Registering connectors"
kubectl -n pinkline delete job register-connectors --ignore-not-found
kubectl apply -f "$SCRIPT_DIR/connect/k8s/40-job-register.yaml"
kubectl -n pinkline wait --for=condition=complete job/register-connectors --timeout=180s \
  || warn "register-connectors job did not complete cleanly — check: kubectl -n pinkline logs job/register-connectors"

# ── 11b. Apply Monitor + Demo ───────────────────────────────────────────
log "Applying Monitor manifests"
kubectl apply -f "$SCRIPT_DIR/monitor/k8s/30-pvc.yaml"
kubectl apply -f "$SCRIPT_DIR/monitor/k8s/20-secret.yaml"
kubectl apply -f "$SCRIPT_DIR/monitor/k8s/overlay-minikube.yaml"
kubectl apply -f "$SCRIPT_DIR/monitor/k8s/40-deployment.yaml"
kubectl -n pinkline patch deploy pas-scada-monitor --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' \
  2>/dev/null \
  || kubectl -n pinkline patch deploy pas-scada-monitor --type=json \
       -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' \
       2>/dev/null || true
kubectl -n pinkline rollout restart deploy/pas-scada-monitor 2>/dev/null || true
ok "Monitor applied"

log "Applying Demo manifests"
kubectl apply -f "$SCRIPT_DIR/demo/k8s/10-configmap.yaml"
kubectl apply -f "$SCRIPT_DIR/demo/k8s/20-deployment.yaml"
kubectl -n pinkline patch deploy pas-scada-demo --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' \
  2>/dev/null \
  || kubectl -n pinkline patch deploy pas-scada-demo --type=json \
       -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' \
       2>/dev/null || true
kubectl -n pinkline rollout restart deploy/pas-scada-demo 2>/dev/null || true
ok "Demo applied"

# ── 12. Status + auto port-forwards + URLs ──────────────────────────────
log "Final status"
kubectl -n pinkline get pods
echo
kubectl -n scada get pods
echo

PF_DIR="$SCRIPT_DIR/.port-forwards"
PF_PIDFILE="$PF_DIR/pids"
mkdir -p "$PF_DIR"

# Pairs of: <namespace>|<service>|<host:pod port mapping(s)>|<label>
PF_TARGETS=(
  "pinkline|pas-scada-bridge|8085:8085|Bridge"
  "pinkline|kafdrop|9000:9000|Kafdrop"
  "pinkline|kafka-connect|8083:8083|Kafka Connect"
  "pinkline|pas-scada-monitor|8080:8080|Health monitor"
  "pinkline|pas-scada-demo|8090:8090|Demo"
  "scada|rabbitmq-internal|15672:15672 1883:1883|RabbitMQ admin + MQTT"
  "scada|scada-api-internal|8091:8091|SCADA API"
)

stop_existing_pfs() {
  if [ -f "$PF_PIDFILE" ]; then
    while read -r pid; do
      [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done < "$PF_PIDFILE"
    rm -f "$PF_PIDFILE"
  fi
  # Belt-and-braces: also kill any orphan kubectl port-forwards from prior runs.
  pkill -f "kubectl.*port-forward" 2>/dev/null || true
}

if [ "$START_PF" = "1" ]; then
  log "Stopping any prior port-forwards"
  stop_existing_pfs
  : > "$PF_PIDFILE"

  log "Starting port-forwards in background"
  for entry in "${PF_TARGETS[@]}"; do
    IFS='|' read -r ns svc ports label <<< "$entry"
    logfile="$PF_DIR/$svc.log"
    # shellcheck disable=SC2086
    nohup kubectl -n "$ns" port-forward "svc/$svc" $ports >"$logfile" 2>&1 &
    pf_pid=$!
    echo "$pf_pid" >> "$PF_PIDFILE"
    ok "$label  ($ns/$svc $ports)  pid=$pf_pid"
  done
  echo
  ok "Port-forwards running. Logs in $PF_DIR/*.log"
  ok "Stop them later with:  bash \"$SCRIPT_DIR/stop.sh\"   (or just close this terminal)"
else
  warn "START_PF=0 set — skipped auto port-forwards. Start them manually if needed."
fi

log "Access URLs"
cat <<EOF

  Open these in your browser:

    Health monitor    http://localhost:8080            (19-component dashboard)
    Demo (table)      http://localhost:8090
    Demo (flow)       http://localhost:8090/flow
    SCADA simulator   http://localhost:8091
    Bridge health     http://localhost:8085/actuator/health
    Bridge messages   http://localhost:8085/api/messages
    Kafdrop           http://localhost:9000
    Connect REST      http://localhost:8083/connectors?expand=status
    RabbitMQ admin    http://localhost:15672            (thiru / password)
    Artemis console   http://localhost:8161/console     (admin / admin)

EOF
ok "Stack is up."
ok "Tear down with: bash \"$SCRIPT_DIR/stop.sh\""
ok "  or fully wipe: kubectl delete ns pinkline scada && docker compose -f \"$MESSAGING_INFRA/docker-compose.yml\" down && minikube delete"
