# PAS-SCADA Kafka Bridge

Pink Line TMS ↔ SCADA integration. The existing Java Spring Boot Camel
bridge stays. **Kafka Connect** adds source/sink connectors in front of
and behind it. A **FastAPI health monitor** watches everything and
emails on failure. A **FastAPI demo app** shows real-time data flow on
two tabs (Monitor + Live Flow).

This single README is everything you need to understand the whole flow.
Per-module README files exist for deep-dives only.

---

## Table of contents

1. [Architecture in one picture](#architecture-in-one-picture)
2. [The 6 modules](#the-6-modules)
3. [Data flow — forward (TMS → SCADA)](#data-flow--forward-tms--scada)
4. [Data flow — reverse (SCADA → TMS)](#data-flow--reverse-scada--tms)
5. [Dead-letter queues — all 5](#dead-letter-queues--all-5)
6. [Two clusters, cross-VM via NodePorts](#two-clusters-cross-vm-via-nodeports)
7. [Deploy to k8s](#deploy-to-k8s)
8. [Verify after deploy](#verify-after-deploy)
9. [Cutover — flip the bridge toggles](#cutover--flip-the-bridge-toggles)
10. [Rollback](#rollback)
11. [Run the customer demo](#run-the-customer-demo)
12. [Troubleshooting](#troubleshooting)
13. [File map](#file-map)
14. [Per-module deep-dives](#per-module-deep-dives)

---

## Architecture in one picture

```
                         FORWARD                                                    REVERSE
   ┌─────────────────────────────────────────────────────────────┐          ┌─────────────────────────┐
   │ TMS VM (10.4.0.23) · namespace: pinkline                     │          │ SCADA VM (10.4.0.25)    │
   │                                                               │          │ namespace: scada        │
   │   Artemis ──JMS──► [tms-artemis-source]                       │          │                         │
   │   ▲                       │                                   │          │ RabbitMQ                │
   │   │                       ▼                                   │          │  amq.topic              │
   │   │                Kafka tms.raw                              │          │  └─► MQTT plugin        │
   │   │                       │                                   │          │      │                  │
   │   │                       ▼                                   │          │      ▼                  │
   │   │            ┌──────────────────────┐                       │          │  SCADA API              │
   │   │            │ tms/ Spring Boot     │                       │          │  (Python · Flask)        │
   │   │            │ XmlToJson + Encrypt  │ when                  │          │                         │
   │   │            │ BRIDGE_INPUT_FROM_KAFKA=true │                │          │  decrypts AES-GCM       │
   │   │            └──────────────────────┘                       │          │  publishes RSAE back    │
   │   │                       │                                   │          │  via MQTT               │
   │   │                       ▼                                   │          │                         │
   │   │              Kafka tms.scada.encrypted                    │          │                         │
   │   │                       │                                   │          │                         │
   │   │                       ▼                                   │          │                         │
   │   │           [tms-rabbitmq-sink] ─────cross-VM AMQP──────────┼──────────►                         │
   │   │                                              :30672       │          │                         │
   │   │                                                           │          │                         │
   │   │                                              :30672       │          │                         │
   │   │           [scada-rabbitmq-source]◄─────cross-VM AMQP──────┼──────────│ RabbitMQ queue          │
   │   │                       │                                   │          │ scada.tms.alarms.queue  │
   │   │                       ▼                                   │          └─────────────────────────┘
   │   │              Kafka scada.tms.raw                          │
   │   │                       │                                   │
   │   │                       ▼                                   │          ┌─────────────────────────┐
   │   │            ┌──────────────────────┐                       │          │ Health Monitor (TMS)    │
   │   │            │ tms/ Spring Boot     │                       │          │ /healthz · /state · /  │
   │   │            │ Decrypt+JsonToXml(opt)│when                   │          │ probes both VMs every   │
   │   │            └──────────────────────┘                       │          │ 30s, emails on failure  │
   │   │                       │                                   │          └─────────────────────────┘
   │   │                       ▼                                   │
   │   │              Kafka scada.tms.processed                    │          ┌─────────────────────────┐
   │   │                       │                                   │          │ Customer Demo UI        │
   │   │                       ▼                                   │          │ Tab 1: Monitor          │
   │   └────────[scada-artemis-sink] ──JMS──► Artemis              │          │ Tab 2: Live Flow         │
   │                                          SCADA.TMS.Alarms     │          │ (real or synthetic)     │
   └─────────────────────────────────────────────────────────────┘          └─────────────────────────┘
```

---

## The 6 modules

| Module | Status | What it is | Built with |
|---|---|---|---|
| `tms/` | ✅ Existing (kept) — *2 new toggleable Camel routes added* | Spring Boot Camel bridge: XmlToJson + AES + JsonToXml | Java 17 · Spring Boot 3.2 · Camel 4.4 |
| `external-scada/` | ✅ Existing (untouched) | SCADA simulator + RabbitMQ on the SCADA VM | Python · Flask · paho-mqtt |
| `connect/` | 🆕 **NEW** | Kafka Connect worker + 4 Camel-based connectors | Confluent cp-kafka-connect 7.5 + Camel KC 4.8.3 |
| `bootstrap/` | 🆕 **NEW** | Two run-once k8s Jobs that pre-create topics + RMQ queue | shell scripts in k8s Jobs |
| `monitor/` | 🆕 **NEW** | Health monitor — probes everything every 30s, emails on failure, dashboard | Python · FastAPI + uvicorn |
| `demo/` | 🆕 **NEW** | Customer demo UI — Monitor tab + Live Flow tab | Python · FastAPI + Jinja2 + uvicorn |

---

## Data flow — forward (TMS → SCADA)

**Default-on (toggles OFF)** — bridge runs as it always has:

```
TMS publishes XML
   ↓ JMS
Artemis topic TMS.PISInfo
   ↓
tms/ Camel route consumes Artemis
   ↓
XmlToJson + AES encrypt
   ↓
Kafka tms.scada.encrypted     +   RabbitMQ amq.topic / tms.scada.pas   +   MQTT direct
                                  (bridge writes all three sinks itself)
   ↓
SCADA API subscribes via MQTT plugin
```

**After cutover (toggle BRIDGE_INPUT_FROM_KAFKA=true, BRIDGE_PIPELINE=kafka):**

```
TMS publishes XML
   ↓ JMS
Artemis topic TMS.PISInfo
   ↓
tms-artemis-source  (Connect)         ◄── on err: dlq.connect.tms-artemis-source
   ↓
Kafka tms.raw
   ↓
tms/ Camel — XmlToJson + AES encrypt   ◄── on err: Artemis DLQ.kafka-bridge
   ↓
Kafka tms.scada.encrypted
   ↓
tms-rabbitmq-sink  (Connect)           ◄── on err: dlq.connect.tms-rabbitmq-sink
   ↓ AMQP cross-VM (10.4.0.25:30672)
RabbitMQ amq.topic / tms.scada.pas
   ↓ MQTT plugin
SCADA API
```

---

## Data flow — reverse (SCADA → TMS)

**After flipping BRIDGE_REVERSE_KAFKA_ENABLED=true:**

```
SCADA publishes RSAE response
   ↓ MQTT topic scada/tms/alarms
RabbitMQ MQTT plugin
   ↓ routing key scada.tms.alarms
RabbitMQ queue scada.tms.alarms.queue
   ↓ AMQP cross-VM (10.4.0.25:30672)
scada-rabbitmq-source  (Connect)        ◄── on err: dlq.connect.scada-rabbitmq-source
   ↓
Kafka scada.tms.raw
   ↓
tms/ Camel — AES decrypt + JsonToXml(opt)
   ↓
Kafka scada.tms.processed
   ↓
scada-artemis-sink  (Connect)           ◄── on err: dlq.connect.scada-artemis-sink
   ↓ JMS
Artemis topic SCADA.TMS.Alarms
   ↓
TMS consumers
```

---

## Dead-letter queues — all 5

| DLQ | Storage | Retention | Fills when |
|---|---|---|---|
| `dlq.connect.tms-artemis-source` | Kafka topic | 14d | Connect can't read Artemis |
| `dlq.connect.tms-rabbitmq-sink` | Kafka topic | 14d | Connect can't write RabbitMQ (cross-VM down, auth) |
| `dlq.connect.scada-rabbitmq-source` | Kafka topic | 14d | Connect can't read SCADA-side RMQ queue |
| `dlq.connect.scada-artemis-sink` | Kafka topic | 14d | Connect can't write back to Artemis |
| `DLQ.kafka-bridge` | Artemis queue (existing) | broker default | tms/ Camel `onException` fires (XmlToJson parse, AES fail) |

**Inspect:**

```bash
# Connect-side DLQ (Kafka)
kubectl -n pinkline run -it --rm kcat --image=edenhill/kcat:1.7.1 --restart=Never -- \
  kcat -b kafka-service:9092 -t dlq.connect.tms-rabbitmq-sink -C -e \
       -f 'KEY=%k\nHEADERS=%h\nVALUE=%S bytes\n---\n'

# tms/ Camel DLQ (Artemis web console)
kubectl -n pinkline port-forward svc/artemis-service 8161:8161
# → http://localhost:8161 → queues → DLQ.kafka-bridge
```

**Replay** a record back to the input topic after fixing the cause:

```bash
kubectl -n pinkline run -it --rm kcat --image=edenhill/kcat:1.7.1 --restart=Never -- /bin/sh -c '
  kcat -b kafka-service:9092 -t dlq.connect.tms-rabbitmq-sink -C -e -o <offset> -c1 -f "%s" \
    | kcat -b kafka-service:9092 -t tms.scada.encrypted -P
'
```

---

## Two clusters, cross-VM via NodePorts

```
                    TMS cluster                                SCADA cluster
                    10.4.0.23                                  10.4.0.25
                    ns: pinkline                                ns: scada
   ┌────────────────────────────────────────┐         ┌──────────────────────────────────┐
   │ • tms/      pas-scada-bridge :8085     │         │ • RabbitMQ                        │
   │ • Kafka                                │         │     :30672 (AMQP, NodePort)       │
   │ • Artemis                              │  ──────►│     :31883 (MQTT, NodePort)       │
   │ • Zookeeper                            │  ◄──────│     :31567 (mgmt, NodePort)       │
   │ • connect/  kafka-connect    :8083     │         │ • SCADA API                       │
   │ • monitor/  pas-scada-monitor :8080    │         │     :30891 (HTTP, NodePort)       │
   │ • demo/     pas-scada-demo   :8090     │         │                                   │
   │ • bootstrap (one-shot Jobs)            │         │                                   │
   └────────────────────────────────────────┘         └──────────────────────────────────┘
```

The TMS cluster reaches the SCADA cluster only over those four NodePorts.
NodePort mapping is defined in `external-scada/k8s/50-rabbitmq-service.yaml`.

---

## Deploy to k8s

### Pre-flight (do once)

| # | Action | Time |
|---|---|---|
| 1 | Build + push 3 images: `pinkline/pas-scada-{monitor,connect,demo}:1.0.0` | ~5 min |
| 2 | Create 2 secrets (`monitor-secret`, `connect-secret`) — see commands below | ~2 min |
| 3 | Set real SMTP server in `monitor/k8s/10-configmap.yaml` | ~1 min |
| 4 | Confirm cross-VM reach: `nc -zv 10.4.0.25 30672 31567 31883 30891` from a TMS pod | ~1 min |

```bash
# 1. Build + push images
docker build -t YOUR_REGISTRY/pas-scada-monitor:1.0.0 monitor/
docker build -t YOUR_REGISTRY/pas-scada-connect:1.0.0 connect/
docker build -t YOUR_REGISTRY/pas-scada-demo:1.0.0    demo/
docker push   YOUR_REGISTRY/pas-scada-monitor:1.0.0
docker push   YOUR_REGISTRY/pas-scada-connect:1.0.0
docker push   YOUR_REGISTRY/pas-scada-demo:1.0.0

# Update image: lines in monitor/k8s/40-deployment.yaml,
#                       connect/k8s/30-deployment.yaml,
#                       demo/k8s/20-deployment.yaml

# 2. Connect secret (Artemis + RabbitMQ + MQTT creds)
kubectl -n pinkline create secret generic connect-secret \
  --from-literal=ARTEMIS_USER='pasbridge' \
  --from-literal=ARTEMIS_PASSWORD='testpass123' \
  --from-literal=RABBITMQ_USER='thiru' \
  --from-literal=RABBITMQ_PASS='password' \
  --from-literal=MQTT_USER='thiru' \
  --from-literal=MQTT_PASS='password' \
  --dry-run=client -o yaml | kubectl apply -f -

# Monitor secret (SMTP creds + RMQ probe creds)
kubectl -n pinkline create secret generic monitor-secret \
  --from-literal=SMTP_USER='alerts@yourcompany.com' \
  --from-literal=SMTP_PASS='gmail-app-password-here' \
  --from-literal=RABBITMQ_USER='thiru' \
  --from-literal=RABBITMQ_PASS='password' \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. SMTP server — edit monitor/k8s/10-configmap.yaml:
#    SMTP_HOST: "smtp.yourcompany.com"
#    SMTP_FROM: "alerts@yourcompany.com"
#    SMTP_TO:   "ops@yourcompany.com,oncall@yourcompany.com"
```

### One-command deploy

```bash
./deploy.sh                  # all 4 new modules in dependency order
```

Or layer by layer:

```bash
./deploy.sh monitor          # health monitor only
./deploy.sh bootstrap        # topic + queue creation Jobs
./deploy.sh connect          # connect worker + 4 connectors
./deploy.sh demo             # customer-facing UI
```

### What it does

`deploy.sh all` runs in this order:

1. Pre-flight checks (cluster reachable, namespace exists)
2. **monitor** → applies 4 manifests, waits for Deployment available
3. **bootstrap** → applies 2 Jobs, waits for both to complete (creates 8 Kafka topics + RabbitMQ queue/binding)
4. **connect** → applies worker, waits, runs registration Job to PUT 4 connector configs
5. **demo** → applies ConfigMap + Deployment, waits for Available
6. Prints a summary of all pods + connector statuses

---

## Verify after deploy

```bash
# 1. All pods Running
kubectl -n pinkline get pods -l 'app in (pas-scada-monitor,kafka-connect,pas-scada-bridge,pas-scada-demo)'
# All 1/1 Running

# 2. All 4 Connect connectors RUNNING
kubectl -n pinkline run -it --rm probe --image=curlimages/curl:8.10.1 --restart=Never -- \
  curl -s http://kafka-connect.pinkline.svc.cluster.local:8083/connectors?expand=status \
  | python -m json.tool
# every connector + every task: "state": "RUNNING"

# 3. Monitor sees everything UP
kubectl -n pinkline port-forward svc/pas-scada-monitor 8080:8080 &
curl -s http://localhost:8080/state | jq '. | map_values(.current_state)'

# 4. Demo UI loads (open in browser)
kubectl -n pinkline port-forward svc/pas-scada-demo 8090:8090
# http://localhost:8090/      → Tab 1: Monitor (live UP/DOWN cards)
# http://localhost:8090/flow  → Tab 2: Live Flow (animation)

# 5. End-to-end: publish XML, watch tms.raw fill (Connect source is working)
kubectl -n pinkline run -it --rm artemis-cli \
  --image=apache/activemq-artemis:2.31.2 --restart=Never --command -- /bin/sh -c \
  'artemis producer --url tcp://artemis-service:61616 \
     --user pasbridge --password testpass123 \
     --destination topic://TMS.PISInfo --message-count 1 \
     --message "<rcsMsg><data>verify</data></rcsMsg>"'
# Then check Kafdrop for the new record on tms.raw
```

---

## Cutover — flip the bridge toggles

Until you flip toggles, the bridge runs **exactly as it does today** —
Artemis-direct routes, RabbitMQ/MQTT direct sinks. Connect runs alongside
but its data is unused (no double delivery).

When you've verified Connect is healthy:

```bash
kubectl -n pinkline set env deploy/pas-scada-bridge \
  BRIDGE_INPUT_FROM_KAFKA=true \
  BRIDGE_PIPELINE=kafka \
  BRIDGE_REVERSE_KAFKA_ENABLED=true
```

Watch the bridge restart:

```bash
kubectl -n pinkline logs -f deploy/pas-scada-bridge | grep -E 'input-kafka|reverse-kafka|Artemis-direct'
```

Healthy output:
```
... bridge.input-kafka.enabled=true — Artemis-direct routes SKIPPED
... ← Kafka source [tms.raw] — pipeline: kafka | encrypt: true
... → Kafka [tms.scada.encrypted]
... ← Kafka reverse [scada.tms.raw] — decrypt=true convertXml=false
... → Kafka reverse [scada.tms.processed]
```

---

## Rollback

```bash
kubectl -n pinkline set env deploy/pas-scada-bridge \
  BRIDGE_INPUT_FROM_KAFKA=false \
  BRIDGE_REVERSE_KAFKA_ENABLED=false \
  BRIDGE_PIPELINE=kafka,rabbitmq,mqtt
```

Bridge reverts to the original Artemis-direct flow. Connect connectors
keep running but their messages aren't consumed downstream — harmless.

To remove the new modules entirely:

```bash
kubectl delete -f demo/k8s/
kubectl delete -f connect/k8s/
kubectl delete -f bootstrap/k8s/
kubectl delete -f monitor/k8s/
```

---

## Run the customer demo

The `demo/` app has 2 tabs and 2 modes:

| Tab | URL | Shows |
|---|---|---|
| 🩺 Monitor | `/` | Component health (UP/DOWN cards, auto-refresh) — fetches monitor's `/state` |
| ⚡ Live Flow | `/flow` | Animated architecture, packets fly through arrows per real message |

| Mode | When | Set via |
|---|---|---|
| `synthetic` (default) | Rehearsing, offline laptop demo, no bridge | `MODE=synthetic` |
| `live` | Real customer demo, bridge + monitor reachable | `MODE=live` + `BRIDGE_URL` + `MONITOR_URL` |

```bash
# Local laptop, synthetic
cd demo && python app.py
# http://localhost:8090/

# In k8s, switch to live
kubectl -n pinkline edit configmap demo-config
# change MODE: "live"
kubectl -n pinkline rollout restart deploy/pas-scada-demo
```

The synthetic mode also simulates outages — every ~30s one component
goes DOWN for 20-45s on the Monitor tab. Keeps the demo dynamic.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `kafka-connect` CrashLoopBackOff with `Failed to find any class that implements Connector` | Image built without plugins | Rebuild `connect/Dockerfile` with `--no-cache`, verify `/usr/share/confluent-hub-components/` has 4 plugins |
| `${env:RABBITMQ_USER}` shows as literal in connector config | EnvVarConfigProvider not loaded | Check `connect/k8s/30-deployment.yaml` has `CONNECT_CONFIG_PROVIDERS=env` |
| Connector registration Job: HTTP 400 `Invalid value` | Connector class name typo | `curl /connector-plugins` for the exact class string |
| Bridge after toggle flip: logs show "Artemis-direct routes SKIPPED" but no Kafka consumer | `kafka.brokers` placeholder unresolved | Set `KAFKA_HOST/PORT` env in the bridge Deployment |
| Reverse path: decrypt fails | `SCADA_AES_KEY` mismatch | Must equal SCADA API's key — copy from `bridge-secret` |
| Bootstrap RMQ Job: HTTP 401 | Wrong creds in `connect-secret` | `kubectl -n pinkline edit secret connect-secret` |
| Bootstrap Kafka Job: `Connection refused` | Wrong `KAFKA_BOOTSTRAP` env | Match `kafka-service.pinkline.svc.cluster.local:9092` |
| Monitor pod healthy but no emails | Wrong SMTP creds, App Password not used (Gmail) | For Gmail enable 2FA + create an App Password |
| Cross-VM probe fails | Firewall blocks NodePorts | Open `10.4.0.25:30672, 31567, 31883, 30891` from TMS subnet |
| Demo Tab 1 says "✗ HTTP 502" in live mode | Monitor not reachable | Check `MONITOR_URL` in demo-config matches the actual monitor service DNS |

---

## File map

```
PAS-SCADA-Kafka-Bridge/
├── README.md                        ← this file (single source of truth)
├── deploy.sh                        ← one-command k8s deploy
├── architecture-diagram-connect.html ← visual diagram (open in browser)
├── docker-compose.yml               ← local-only dev (no k8s)
│
├── tms/                             ← Java Spring Boot Camel bridge (existing)
│   ├── src/main/java/...            ← XmlToJson, AES, routes
│   ├── k8s/                         ← bridge Deployment manifests
│   └── pom.xml
│
├── external-scada/                  ← SCADA simulator (existing, untouched)
│   ├── scada-api/                   ← Python Flask
│   └── k8s/                         ← RabbitMQ + SCADA API manifests
│
├── connect/                         🆕 Kafka Connect worker + 4 connectors
│   ├── Dockerfile
│   ├── k8s/                         ← Secret, ConfigMap, Deployment, register-Job
│   └── README.md                    ← connector inventory + REST API
│
├── bootstrap/                       🆕 run-once Jobs (topics + RMQ queue)
│   ├── k8s/                         ← 2 Jobs
│   └── README.md
│
├── monitor/                         🆕 Health probe + email + dashboard
│   ├── monitor.py                   ← FastAPI app
│   ├── Dockerfile
│   ├── k8s/                         ← ConfigMap, Secret, PVC, Deployment+Service
│   └── README.md
│
├── demo/                            🆕 Customer-facing 2-tab UI
│   ├── app.py                       ← FastAPI app
│   ├── templates/                   ← Jinja2: base, monitor, flow
│   ├── static/style.css
│   ├── Dockerfile
│   ├── k8s/                         ← ConfigMap, Deployment+Service
│   └── README.md
│
└── cicd/                            ← CI/CD pipeline (existing)
    └── README.md
```

---

## Per-module deep-dives

For day-to-day work on a single module, the per-module README has the
detail. This README covers the system; those cover the parts.

* [`tms/`](tms/) — Spring Boot Camel app source, processors, routes
* [`monitor/README.md`](monitor/README.md) — health monitor internals, FastAPI endpoints, SMTP config, alert templates
* [`connect/README.md`](connect/README.md) — full connector list, Camel Kamelet limitations, Connect REST API cheatsheet
* [`bootstrap/README.md`](bootstrap/README.md) — Kafka topic spec, RabbitMQ queue declare, customizing for a different cluster
* [`demo/README.md`](demo/README.md) — synthetic-vs-live mode, custom demo scripts, embedding in the customer pitch
* [`external-scada/scada-api/README.md`](external-scada/scada-api/README.md) — SCADA simulator endpoints

---

## RSAE message types (SCADA → TMS reverse direction)

| Type | When SCADA sends it |
|---|---|
| `UpdateAlarm` | Equipment state change (auto every 10s in simulator) |
| `KeepAlive` | Heartbeat (auto every 30s) |
| `SendAllAlarms` | Full alarm broadcast (auto every 60s) |
| `GetAllAlarms` | Request TMS alarm state (auto every 120s) |
"# PAS-SCADA-Kafka-Bridge" 
