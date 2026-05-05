# Start Commands — per-service

Each section is self-contained. Run from the repo root unless noted.
Use this when you want to bring up / restart just one piece without
running the whole `start.sh`.

> **First time on a new PC?** Read [SETUP.md](./SETUP.md) first — it covers
> installing Docker / minikube / kubectl / git and cloning both repos.
> Once the prerequisites are in place, come back here.

## 0 · Prerequisites (run once per shell session)

```bash
# Set the path to your local messaging-infra repo. EVERY section below
# uses $MESSAGING_INFRA — adjust this once and everything just works.
export MESSAGING_INFRA="$HOME/code/messaging-infra"     # Mac / Linux / WSL
# Windows Git Bash:  export MESSAGING_INFRA="/c/Users/you/code/messaging-infra"
# Windows native:    set MESSAGING_INFRA=C:\Users\you\code\messaging-infra

# minikube must be running before anything else can deploy
minikube start --cpus=4 --memory=6144 --driver=docker

# Verify
kubectl get nodes
```

> Replace `pinkline/...`, `ghcr.io/thirunavukkarasuthangaraj/...` image
> names with your own registry/tag if you push to a private registry.
> All commands below assume the local-build flow used by `start.sh`.

---

## 1 · TMS  (Artemis · Zookeeper · Kafka · Kafdrop · Bridge · Kafka Connect)

The TMS side runs Artemis on the host (Docker), the rest in k8s `pinkline` namespace.

```bash
# 1.1 — Artemis (Docker on host)
docker compose -f $MESSAGING_INFRA/docker-compose.yml up -d
# Verify:  docker ps --filter name=artemis
#          → Artemis console: http://localhost:8161  (admin/admin)

# 1.2 — Build bridge image (rebuild whenever Java source changes)
docker build -t pinkline/pas-scada-bridge:latest tms/

# 1.3 — Build Connect image (only first time, plugins cached after)
docker build -t pinkline/pas-scada-connect:latest connect/

# 1.4 — Hard-replace both images inside minikube
for img in pinkline/pas-scada-bridge:latest pinkline/pas-scada-connect:latest; do
  minikube ssh -- "docker rmi -f $img" 2>/dev/null
  minikube image load $img
done

# 1.5 — Apply manifests in order
kubectl apply -f tms/k8s/00-namespace.yaml
kubectl apply -f tms/k8s/20-zookeeper.yaml
kubectl apply -f tms/k8s/30-kafka.yaml
kubectl apply -f tms/k8s/40-kafdrop.yaml
kubectl apply -f tms/k8s/overlay-minikube.yaml
kubectl apply -f tms/k8s/deployment.yaml

# 1.6 — Patch bridge probes (slow Spring Boot startup needs 180s)
kubectl -n pinkline patch deploy pas-scada-bridge --type=json -p='[
  {"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"},
  {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/initialDelaySeconds","value":180},
  {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/failureThreshold","value":5},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/initialDelaySeconds","value":120},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/failureThreshold","value":10}]'

# 1.7 — Wait for Kafka, then bootstrap topics
kubectl -n pinkline wait --for=condition=ready pod -l app=kafka --timeout=180s
kubectl -n pinkline delete job bootstrap-kafka-topics --ignore-not-found
kubectl apply -f bootstrap/k8s/10-kafka-topics-job.yaml
kubectl -n pinkline wait --for=condition=complete job/bootstrap-kafka-topics --timeout=120s

# 1.8 — Apply Connect + register all 7 connectors
kubectl apply -f connect/k8s/10-secret.yaml
kubectl apply -f connect/k8s/20-configmap.yaml
kubectl apply -f connect/k8s/30-deployment.yaml
kubectl -n pinkline rollout status deploy/kafka-connect --timeout=300s
kubectl -n pinkline delete job register-connectors --ignore-not-found
kubectl apply -f connect/k8s/40-job-register.yaml

# 1.9 — Wait for bridge ready (~3 min)
kubectl -n pinkline rollout status deploy/pas-scada-bridge --timeout=300s

# 1.10 — Port-forward (run in separate terminals or with &)
kubectl -n pinkline port-forward svc/pas-scada-bridge 8085:8085 &
kubectl -n pinkline port-forward svc/kafdrop          9000:9000 &
kubectl -n pinkline port-forward svc/kafka-connect    8083:8083 &
```

**TMS URLs:**
- http://localhost:8161/console — Artemis (admin/admin)
- http://localhost:8085/actuator/health — Bridge health
- http://localhost:8085/api/messages — Bridge in-app monitor
- http://localhost:9000 — Kafdrop
- http://localhost:8083/connectors?expand=status — Connect REST

---

## 2 · SCADA  (RabbitMQ · SCADA API)

```bash
# 2.1 — Build SCADA API image (rebuild on app.py / dashboard.html changes)
docker build \
  -t ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest \
  -t external-scada-scada-api:latest \
  external-scada/scada-api/

# 2.2 — Hard-replace in minikube
minikube ssh -- "docker rmi -f ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest" 2>/dev/null
minikube image load ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest

# 2.3 — Apply manifests in order
for f in 00-namespace.yaml 10-rabbitmq-configmap.yaml 20-rabbitmq-secret.yaml \
         30-rabbitmq-pvc.yaml 40-rabbitmq-deployment.yaml 50-rabbitmq-service.yaml \
         60-scada-api-secret.yaml 70-scada-api-deployment.yaml; do
  kubectl apply -f external-scada/k8s/$f
done

# 2.4 — Patch image pull policy
kubectl -n scada patch deploy scada-api --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'

# 2.5 — Wait for RabbitMQ ready
kubectl -n scada wait --for=condition=ready pod -l app=rabbitmq --timeout=180s

# 2.6 — Declare scada.tms.alarms.queue + binding (one-time, idempotent)
kubectl -n scada run rmq-bind-$$ --rm -i --restart=Never \
  --image=curlimages/curl:8.10.1 -- sh -c '
    AUTH="-u thiru:password"
    BASE="http://rabbitmq-internal:15672/api"
    curl -fsS $AUTH -X PUT -H "Content-Type: application/json" \
      --data "{\"durable\":true,\"auto_delete\":false}" \
      "$BASE/queues/%2F/scada.tms.alarms.queue" >/dev/null
    curl -fsS $AUTH -X POST -H "Content-Type: application/json" \
      --data "{\"routing_key\":\"scada.tms.alarms\"}" \
      "$BASE/bindings/%2F/e/amq.topic/q/scada.tms.alarms.queue" >/dev/null
    echo "queue + binding OK"'

# 2.7 — Wait for scada-api ready
kubectl -n scada rollout status deploy/scada-api --timeout=120s

# 2.8 — Port-forward
kubectl -n scada port-forward svc/scada-api-internal     8091:8091 &
kubectl -n scada port-forward svc/rabbitmq-internal      1883:1883 15672:15672 &
```

**SCADA URLs:**
- http://localhost:8091 — SCADA Simulator dashboard
- http://localhost:8091/api/status — connection state
- http://localhost:8091/api/received — last 100 decrypted TMS messages
- http://localhost:15672 — RabbitMQ admin (thiru/password)
- MQTT broker: `mqtt://localhost:1883` (thiru/password)

---

## 3 · Monitor  (FastAPI health dashboard with audible alarm)

Lives in the `pinkline` namespace, but is logically independent — can be
deployed to any cluster that can reach the probe URLs.

```bash
# 3.1 — Build monitor image (rebuild on monitor.py changes)
docker build -t pinkline/pas-scada-monitor:latest monitor/

# 3.2 — Hard-replace in minikube
minikube ssh -- "docker rmi -f pinkline/pas-scada-monitor:latest" 2>/dev/null
minikube image load pinkline/pas-scada-monitor:latest

# 3.3 — Apply manifests
kubectl apply -f monitor/k8s/30-pvc.yaml
kubectl apply -f monitor/k8s/20-secret.yaml
kubectl apply -f monitor/k8s/overlay-minikube.yaml
kubectl apply -f monitor/k8s/40-deployment.yaml

# 3.4 — Patch image pull policy
kubectl -n pinkline patch deploy pas-scada-monitor --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'

# 3.5 — Restart so the latest image takes effect
kubectl -n pinkline rollout restart deploy/pas-scada-monitor
kubectl -n pinkline rollout status  deploy/pas-scada-monitor --timeout=120s

# 3.6 — Port-forward
kubectl -n pinkline port-forward svc/pas-scada-monitor 8080:8080 &
```

**Monitor URLs:**
- http://localhost:8080 — Live status dashboard (alarm toggle in top-right)
- http://localhost:8080/state — Full probe state JSON (19 components)
- http://localhost:8080/healthz — k8s liveness
- http://localhost:8080/docs — Swagger API

> The audible alarm is browser-side (Web Audio API). On first load click
> the **Sound off** pill once to arm — required by browser autoplay rules.

---

## 4 · Demo  (FastAPI customer-facing live UI)

Same namespace as monitor, also logically independent.

```bash
# 4.1 — Build demo image (rebuild on app.py / template changes)
docker build -t pinkline/pas-scada-demo:1.0.0 demo/

# 4.2 — Hard-replace in minikube
minikube ssh -- "docker rmi -f pinkline/pas-scada-demo:1.0.0" 2>/dev/null
minikube image load pinkline/pas-scada-demo:1.0.0

# 4.3 — Apply manifests
kubectl apply -f demo/k8s/10-configmap.yaml
kubectl apply -f demo/k8s/20-deployment.yaml

# 4.4 — Patch image pull policy
kubectl -n pinkline patch deploy pas-scada-demo --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'

# 4.5 — Restart so the latest image takes effect
kubectl -n pinkline rollout restart deploy/pas-scada-demo
kubectl -n pinkline rollout status  deploy/pas-scada-demo --timeout=120s

# 4.6 — Port-forward
kubectl -n pinkline port-forward svc/pas-scada-demo 8090:8090 &
```

**Demo URLs:**
- http://localhost:8090 — Live data table (TMS↔SCADA messages)
- http://localhost:8090/flow — Animated flow diagram
- http://localhost:8090/api/stream — SSE event stream

---

## Restart-only (after a code change, no full re-bring-up)

```bash
# After Java change → bridge
docker build -t pinkline/pas-scada-bridge:latest tms/
minikube ssh -- "docker rmi -f pinkline/pas-scada-bridge:latest"
minikube image load pinkline/pas-scada-bridge:latest
kubectl -n pinkline rollout restart deploy/pas-scada-bridge

# After scada-api/app.py or dashboard.html change
docker build \
  -t ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest \
  external-scada/scada-api/
minikube ssh -- "docker rmi -f ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest"
minikube image load ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest
kubectl -n scada rollout restart deploy/scada-api

# After monitor.py change
docker build -t pinkline/pas-scada-monitor:latest monitor/
minikube ssh -- "docker rmi -f pinkline/pas-scada-monitor:latest"
minikube image load pinkline/pas-scada-monitor:latest
kubectl -n pinkline rollout restart deploy/pas-scada-monitor

# After demo template change
docker build -t pinkline/pas-scada-demo:1.0.0 demo/
minikube ssh -- "docker rmi -f pinkline/pas-scada-demo:1.0.0"
minikube image load pinkline/pas-scada-demo:1.0.0
kubectl -n pinkline rollout restart deploy/pas-scada-demo

# After Connect connector config change (no image rebuild needed)
kubectl apply -f connect/k8s/20-configmap.yaml
kubectl -n pinkline delete job register-connectors --ignore-not-found
kubectl apply -f connect/k8s/40-job-register.yaml
```

---

## Stop commands

```bash
# Stop one component (deployment) — preserves namespace + state
kubectl -n pinkline scale deploy pas-scada-bridge   --replicas=0
kubectl -n pinkline scale deploy pas-scada-monitor  --replicas=0
kubectl -n pinkline scale deploy pas-scada-demo     --replicas=0
kubectl -n pinkline scale deploy kafka-connect      --replicas=0
kubectl -n scada    scale deploy scada-api          --replicas=0

# Restart with --replicas=1

# Tear down a whole namespace (loses PVCs)
kubectl delete ns pinkline
kubectl delete ns scada

# Stop Artemis (Docker)
docker compose -f $MESSAGING_INFRA/docker-compose.yml down

# Stop minikube — preserve state
minikube stop

# Stop minikube — wipe everything
minikube delete
```

---

## URL master list (after all 4 sections are running)

| URL | What |
|---|---|
| http://localhost:8161/console | Artemis (admin/admin) |
| http://localhost:8085/actuator/health | Bridge health |
| http://localhost:8085/api/messages | Bridge in-app monitor |
| http://localhost:9000 | Kafdrop |
| http://localhost:8083/connectors?expand=status | Connect REST |
| http://localhost:8091 | SCADA Simulator |
| http://localhost:15672 | RabbitMQ admin (thiru/password) |
| http://localhost:8080 | Health monitor + alarm |
| http://localhost:8090 | Demo (data table) |
| http://localhost:8090/flow | Demo (flow diagram) |
