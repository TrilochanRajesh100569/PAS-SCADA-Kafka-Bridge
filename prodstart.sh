#!/usr/bin/env bash
# prodstart.sh — production bring-up against an existing Kubernetes cluster.
#
# Differs from start.sh:
#   - NO minikube (assumes `kubectl` is already configured for the prod cluster)
#   - NO docker compose Artemis (assumes client's Artemis is reachable from the cluster)
#   - NO `minikube image load` (images must live in a registry the cluster can pull)
#   - NO LoadBalancer / Ingress (uses kubectl port-forward for access)
#
# Required env vars (script exits if any are missing):
#   ARTEMIS_BROKER_URL  e.g. tcp://artemis.client.example:61616
#                        (or set ARTEMIS_HOST + ARTEMIS_PORT instead)
#   ARTEMIS_USER        client-given JMS username
#   ARTEMIS_PASSWORD    client-given JMS password   (alias: ARTEMIS_PASS)
#
# Optional env vars (sane defaults if unset):
#   RABBITMQ_USER, RABBITMQ_PASS, MQTT_USER, MQTT_PASS, SCADA_AES_KEY
#   IMAGE_REGISTRY      if set with BUILD_IMAGES=1, images are tagged + pushed
#                       (e.g. ghcr.io/your-org)
#   BUILD_IMAGES=1      build images locally and (if IMAGE_REGISTRY set) push
#                       Default: 0 — assumes images are already in the registry
#   START_PF=0          skip auto port-forwards at the end
#   KUBE_CONTEXT        kubectl context to use (otherwise current-context)
#
# Companion: docs/PROD-DEPLOY.md

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

START_PF="${START_PF:-1}"
BUILD_IMAGES="${BUILD_IMAGES:-0}"
IMAGE_REGISTRY="${IMAGE_REGISTRY:-}"

log()  { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*" >&2; exit 1; }

# ── Required env vars ───────────────────────────────────────────────────
if [ -n "${ARTEMIS_HOST:-}" ] && [ -n "${ARTEMIS_PORT:-}" ]; then
  ARTEMIS_BROKER_URL="${ARTEMIS_BROKER_URL:-tcp://${ARTEMIS_HOST}:${ARTEMIS_PORT}}"
fi
[ -n "${ARTEMIS_BROKER_URL:-}" ] || die "ARTEMIS_BROKER_URL not set.
    Example: export ARTEMIS_BROKER_URL=tcp://artemis.client.example:61616
    (or set ARTEMIS_HOST + ARTEMIS_PORT instead)"
[ -n "${ARTEMIS_USER:-}" ]     || die "ARTEMIS_USER not set."
ARTEMIS_PASSWORD="${ARTEMIS_PASSWORD:-${ARTEMIS_PASS:-}}"
[ -n "$ARTEMIS_PASSWORD" ]     || die "ARTEMIS_PASSWORD (or ARTEMIS_PASS) not set."

RABBITMQ_USER="${RABBITMQ_USER:-thiru}"
RABBITMQ_PASS="${RABBITMQ_PASS:-password}"
MQTT_USER="${MQTT_USER:-thiru}"
MQTT_PASS="${MQTT_PASS:-password}"
SCADA_AES_KEY="${SCADA_AES_KEY:-k7Qh2NfT8vR0mC9aXy4pLwZbE3sG6uJtH1iKd5oArMw=}"

# Parse tcp://host:port for the bridge ConfigMap (envFrom mounts ARTEMIS_HOST/PORT).
ARTEMIS_HOST="$(echo "$ARTEMIS_BROKER_URL" | sed -E 's|^tcp://||; s|:.*||')"
ARTEMIS_PORT="$(echo "$ARTEMIS_BROKER_URL" | sed -E 's|.*:||')"

# ── Preflight: kubectl reachable ────────────────────────────────────────
if [ -n "${KUBE_CONTEXT:-}" ]; then
  kubectl config use-context "$KUBE_CONTEXT" >/dev/null \
    || die "Failed to switch kubectl context to '$KUBE_CONTEXT'."
fi
log "Checking cluster connectivity"
kubectl cluster-info >/dev/null 2>&1 \
  || die "kubectl cannot reach the cluster. Check your kubeconfig / KUBE_CONTEXT."
ok "context: $(kubectl config current-context)"
ok "server:  $(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"

# ── Optional: build + push images ───────────────────────────────────────
if [ "$BUILD_IMAGES" = "1" ]; then
  docker info >/dev/null 2>&1 || die "Docker daemon not reachable but BUILD_IMAGES=1."

  # Local tags used by manifests today. If IMAGE_REGISTRY is set, we also
  # tag $IMAGE_REGISTRY/<name>:latest and push.
  log "Building images"
  docker build -q -t pinkline/pas-scada-bridge:latest  "$SCRIPT_DIR/tms/"     >/dev/null && ok "bridge built"
  docker build -q -t pinkline/pas-scada-connect:latest "$SCRIPT_DIR/connect/" >/dev/null && ok "connect built"
  docker build -q -t pinkline/pas-scada-monitor:latest "$SCRIPT_DIR/monitor/" >/dev/null && ok "monitor built"
  docker build -q -t pinkline/pas-scada-demo:1.0.0     "$SCRIPT_DIR/demo/"    >/dev/null && ok "demo built"
  docker build -q \
    -t ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest \
    "$SCRIPT_DIR/external-scada/scada-api/" >/dev/null && ok "scada-api built"

  if [ -n "$IMAGE_REGISTRY" ]; then
    log "Tagging + pushing to $IMAGE_REGISTRY"
    declare -A PUSH=(
      [pinkline/pas-scada-bridge:latest]="$IMAGE_REGISTRY/pas-scada-bridge:latest"
      [pinkline/pas-scada-connect:latest]="$IMAGE_REGISTRY/pas-scada-connect:latest"
      [pinkline/pas-scada-monitor:latest]="$IMAGE_REGISTRY/pas-scada-monitor:latest"
      [pinkline/pas-scada-demo:1.0.0]="$IMAGE_REGISTRY/pas-scada-demo:1.0.0"
      [ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest]="$IMAGE_REGISTRY/pas-scada-api:latest"
    )
    for src in "${!PUSH[@]}"; do
      dst="${PUSH[$src]}"
      docker tag "$src" "$dst"
      docker push "$dst" >/dev/null && ok "pushed $dst" || warn "push failed: $dst"
    done
    warn "Manifests still reference original tags (pinkline/*, ghcr.io/thirunavukkarasuthangaraj/*)."
    warn "Update deployment YAMLs to use $IMAGE_REGISTRY/* OR mirror those tags in your registry."
  else
    warn "IMAGE_REGISTRY not set — built images stay local. The cluster CANNOT pull them."
    warn "Set IMAGE_REGISTRY=<your-registry> and re-run, or push the images manually."
  fi
else
  ok "BUILD_IMAGES=0 — assuming images are already pushed to a registry the cluster can pull."
fi

# ── Apply tms manifests ─────────────────────────────────────────────────
log "Applying tms/k8s manifests"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/00-namespace.yaml"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/20-zookeeper.yaml"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/30-kafka.yaml"
kubectl apply -f "$SCRIPT_DIR/tms/k8s/40-kafdrop.yaml"

# overlay-minikube.yaml hardcodes ARTEMIS_HOST=host.minikube.internal — apply
# it then patch ARTEMIS_HOST/PORT to the prod broker.
kubectl apply -f "$SCRIPT_DIR/tms/k8s/overlay-minikube.yaml"
kubectl -n pinkline patch configmap bridge-config --type merge -p "$(cat <<EOF
{"data":{"ARTEMIS_HOST":"$ARTEMIS_HOST","ARTEMIS_PORT":"$ARTEMIS_PORT"}}
EOF
)"
ok "bridge-config patched: ARTEMIS_HOST=$ARTEMIS_HOST ARTEMIS_PORT=$ARTEMIS_PORT"

# bridge-secret from env (overrides the dev yaml).
kubectl -n pinkline create secret generic bridge-secret \
  --from-literal=ARTEMIS_USER="$ARTEMIS_USER" \
  --from-literal=ARTEMIS_PASS="$ARTEMIS_PASSWORD" \
  --from-literal=RABBITMQ_USER="$RABBITMQ_USER" \
  --from-literal=RABBITMQ_PASS="$RABBITMQ_PASS" \
  --from-literal=MQTT_USER="$MQTT_USER" \
  --from-literal=MQTT_PASS="$MQTT_PASS" \
  --from-literal=SCADA_AES_KEY="$SCADA_AES_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f "$SCRIPT_DIR/tms/k8s/deployment.yaml"
ok "tms manifests applied"

# ── Apply external-scada manifests ──────────────────────────────────────
log "Applying external-scada/k8s manifests"
for f in 00-namespace.yaml 10-rabbitmq-configmap.yaml 20-rabbitmq-secret.yaml \
         30-rabbitmq-pvc.yaml 40-rabbitmq-deployment.yaml 50-rabbitmq-service.yaml \
         70-scada-api-deployment.yaml; do
  kubectl apply -f "$SCRIPT_DIR/external-scada/k8s/$f"
done

kubectl -n scada create secret generic scada-api-secret \
  --from-literal=SCADA_AES_KEY="$SCADA_AES_KEY" \
  --from-literal=MQTT_USER="$MQTT_USER" \
  --from-literal=MQTT_PASS="$MQTT_PASS" \
  --dry-run=client -o yaml | kubectl apply -f -
ok "scada manifests applied"

# ── imagePullPolicy=Always for prod (always fetch latest from registry) ─
log "Setting imagePullPolicy=Always on app deployments"
for entry in "pinkline pas-scada-bridge" "scada scada-api"; do
  read -r ns dep <<< "$entry"
  kubectl -n "$ns" patch deploy "$dep" --type=json \
    -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' \
    2>/dev/null \
    || kubectl -n "$ns" patch deploy "$dep" --type=json \
         -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' \
         2>/dev/null || true
done

kubectl -n scada    rollout restart deploy/scada-api        2>/dev/null || true
kubectl -n pinkline rollout restart deploy/pas-scada-bridge 2>/dev/null || true

# ── Wait for Kafka, then create topics ──────────────────────────────────
log "Waiting for Kafka"
kubectl -n pinkline rollout status deploy/kafka --timeout=300s
kubectl -n pinkline wait --for=condition=ready pod -l app=kafka --timeout=300s
ok "Kafka ready"

log "Bootstrapping Kafka topics"
kubectl -n pinkline delete job bootstrap-kafka-topics --ignore-not-found
kubectl apply -f "$SCRIPT_DIR/bootstrap/k8s/10-kafka-topics-job.yaml"
kubectl -n pinkline wait --for=condition=complete job/bootstrap-kafka-topics --timeout=240s
ok "topics ready"

# ── Wait for RabbitMQ, declare scada queue + binding ────────────────────
log "Waiting for RabbitMQ"
kubectl -n scada rollout status deploy/rabbitmq --timeout=300s
kubectl -n scada wait --for=condition=ready pod -l app=rabbitmq --timeout=300s
ok "RabbitMQ ready"

log "Declaring scada.tms.alarms.queue + binding"
kubectl -n scada exec deploy/rabbitmq -- \
  rabbitmqadmin --username="$RABBITMQ_USER" --password="$RABBITMQ_PASS" \
    declare queue name=scada.tms.alarms.queue durable=true auto_delete=false \
  >/dev/null 2>&1 \
  && ok "queue declared (or already existed)" \
  || warn "queue declare failed — verify with: kubectl -n scada exec deploy/rabbitmq -- rabbitmqctl list_queues"

kubectl -n scada exec deploy/rabbitmq -- \
  rabbitmqadmin --username="$RABBITMQ_USER" --password="$RABBITMQ_PASS" \
    declare binding source=amq.topic destination=scada.tms.alarms.queue routing_key=scada.tms.alarms \
  >/dev/null 2>&1 \
  && ok "binding declared (or already existed)" \
  || warn "binding declare failed — verify in the RabbitMQ admin UI"

# ── Apply Connect (with prod broker URL) + register connectors ──────────
log "Applying connect/k8s manifests"
kubectl -n pinkline create secret generic connect-secret \
  --from-literal=ARTEMIS_USER="$ARTEMIS_USER" \
  --from-literal=ARTEMIS_PASSWORD="$ARTEMIS_PASSWORD" \
  --from-literal=RABBITMQ_USER="$RABBITMQ_USER" \
  --from-literal=RABBITMQ_PASS="$RABBITMQ_PASS" \
  --from-literal=MQTT_USER="$MQTT_USER" \
  --from-literal=MQTT_PASS="$MQTT_PASS" \
  --dry-run=client -o yaml | kubectl apply -f -

sed "s|tcp://host.minikube.internal:61616|${ARTEMIS_BROKER_URL}|g" \
  "$SCRIPT_DIR/connect/k8s/20-configmap.yaml" \
  | kubectl apply -f -
ok "connect-configmap applied with ARTEMIS_BROKER_URL=$ARTEMIS_BROKER_URL"

kubectl apply -f "$SCRIPT_DIR/connect/k8s/30-deployment.yaml"
kubectl -n pinkline patch deploy kafka-connect --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' \
  2>/dev/null \
  || kubectl -n pinkline patch deploy kafka-connect --type=json \
       -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' \
       2>/dev/null || true
kubectl -n pinkline rollout restart deploy/kafka-connect 2>/dev/null || true

log "Waiting for Kafka Connect REST API"
kubectl -n pinkline rollout status deploy/kafka-connect --timeout=480s
kubectl -n pinkline wait --for=condition=ready pod -l app=kafka-connect --timeout=480s
ok "Connect ready"

log "Registering connectors"
kubectl -n pinkline delete job register-connectors --ignore-not-found
kubectl apply -f "$SCRIPT_DIR/connect/k8s/40-job-register.yaml"
kubectl -n pinkline wait --for=condition=complete job/register-connectors --timeout=180s \
  || warn "register-connectors job did not complete cleanly — check: kubectl -n pinkline logs job/register-connectors"

# ── Apply Monitor + Demo ────────────────────────────────────────────────
log "Applying Monitor manifests"
kubectl apply -f "$SCRIPT_DIR/monitor/k8s/30-pvc.yaml"
kubectl apply -f "$SCRIPT_DIR/monitor/k8s/20-secret.yaml"
kubectl apply -f "$SCRIPT_DIR/monitor/k8s/overlay-minikube.yaml"
kubectl apply -f "$SCRIPT_DIR/monitor/k8s/40-deployment.yaml"
kubectl -n pinkline patch deploy pas-scada-monitor --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' \
  2>/dev/null \
  || kubectl -n pinkline patch deploy pas-scada-monitor --type=json \
       -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' \
       2>/dev/null || true
kubectl -n pinkline rollout restart deploy/pas-scada-monitor 2>/dev/null || true

log "Applying Demo manifests"
kubectl apply -f "$SCRIPT_DIR/demo/k8s/10-configmap.yaml"
kubectl apply -f "$SCRIPT_DIR/demo/k8s/20-deployment.yaml"
kubectl -n pinkline patch deploy pas-scada-demo --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' \
  2>/dev/null \
  || kubectl -n pinkline patch deploy pas-scada-demo --type=json \
       -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Always"}]' \
       2>/dev/null || true
kubectl -n pinkline rollout restart deploy/pas-scada-demo 2>/dev/null || true

# ── Status + port-forwards + URLs ───────────────────────────────────────
log "Final status"
kubectl -n pinkline get pods
echo
kubectl -n scada get pods
echo

PF_DIR="$SCRIPT_DIR/.port-forwards"
PF_PIDFILE="$PF_DIR/pids"
mkdir -p "$PF_DIR"

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
  pkill -f "kubectl.*port-forward" 2>/dev/null || true
}

if [ "$START_PF" = "1" ]; then
  log "Stopping any prior port-forwards"
  stop_existing_pfs
  : > "$PF_PIDFILE"

  log "Starting port-forwards in background (no LoadBalancer in this cluster)"
  for entry in "${PF_TARGETS[@]}"; do
    IFS='|' read -r ns svc ports label <<< "$entry"
    logfile="$PF_DIR/$svc.log"
    # shellcheck disable=SC2086
    nohup kubectl -n "$ns" port-forward --address 0.0.0.0 "svc/$svc" $ports >"$logfile" 2>&1 &
    pf_pid=$!
    echo "$pf_pid" >> "$PF_PIDFILE"
    ok "$label  ($ns/$svc $ports)  pid=$pf_pid"
  done
  echo
  ok "Port-forwards bound to 0.0.0.0 — reachable on the server's external IP."
  ok "Logs in $PF_DIR/*.log. Stop with: bash \"$SCRIPT_DIR/stop.sh\""
else
  warn "START_PF=0 — skipped auto port-forwards."
fi

log "Access URLs"
SERVER_HOST="${PUBLIC_HOST:-localhost}"
cat <<EOF

  PROD MODE — Artemis is on a remote host.
  Effective config:
    ARTEMIS_BROKER_URL = $ARTEMIS_BROKER_URL
    ARTEMIS_USER       = $ARTEMIS_USER
    Cluster context    = $(kubectl config current-context)

  Replace 'localhost' below with the cluster server's external IP/hostname
  (or set PUBLIC_HOST and re-run for a copy-pasteable URL list):

    Health monitor    http://$SERVER_HOST:8080
    Demo (table)      http://$SERVER_HOST:8090
    Demo (flow)       http://$SERVER_HOST:8090/flow
    SCADA simulator   http://$SERVER_HOST:8091
    Bridge health     http://$SERVER_HOST:8085/actuator/health
    Bridge messages   http://$SERVER_HOST:8085/api/messages
    Kafdrop           http://$SERVER_HOST:9000
    Connect REST      http://$SERVER_HOST:8083/connectors?expand=status
    RabbitMQ admin    http://$SERVER_HOST:15672    ($RABBITMQ_USER / $RABBITMQ_PASS)

  Artemis console:    use the client-provided URL.

  Verify Artemis reachability from inside the cluster:
    kubectl -n pinkline run -i --rm tcptest --image=busybox --restart=Never -- \\
      sh -c "nc -vz $ARTEMIS_HOST $ARTEMIS_PORT"
EOF

ok "Stack is up."
ok "Tear down port-forwards with: bash \"$SCRIPT_DIR/stop.sh\""
ok "  or wipe everything: kubectl delete ns pinkline scada"
