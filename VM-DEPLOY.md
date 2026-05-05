# VM Deploy — overview (TMS / SCADA / Monitor on separate VMs)

For when you're moving past local dev (minikube on one PC) into a real
deployment with **three separate VMs**:

- **TMS Linux VM** — Java / Kafka / Connect side: Artemis, Kafka,
  Zookeeper, Kafdrop, Bridge, Kafka Connect, Demo.
  Detailed runbook: **[TMS-LINUX-VM.md](TMS-LINUX-VM.md)**
- **SCADA Windows VM** — SCADA-side endpoints: RabbitMQ (with MQTT
  plugin) and the `scada-api` Python service.
  Detailed runbook: **[SCADA-WINDOWS-VM.md](SCADA-WINDOWS-VM.md)**
- **Monitor VM** (separate; ops machine — Linux or Windows is fine) —
  the `pas-scada-monitor` health dashboard. Probes TMS and SCADA VMs
  over HTTP. Kept off the data path so a TMS or SCADA outage doesn't
  also kill your visibility.
  Detailed runbook: **[MONITOR-VM.md](MONITOR-VM.md)**

The data-path VMs (TMS ↔ SCADA) talk over **one** network direction:
`TMS → SCADA` on AMQP port **5672**. The Monitor VM only makes
**outbound HTTP** to TMS (8085/8083) and SCADA (15672/8091). It never
sits in the data path.

> This doc is **not** the dev guide. For dev on one PC use
> `MANUAL-RUN.md` (step-by-step) or `start.sh` (one-shot).
> Use this file when promoting to a multi-VM deployment, and the
> per-VM runbooks above for the actual install steps on each machine.

---

## 1 · Architecture

```
            ┌───────────────────────────── TMS Linux VM ─────────────────────────────┐
            │                                                                        │
PAS source ─┼─→ Artemis(61616) ─→ Bridge(8085) ──→ Kafka(9092) ──→ tms-rabbitmq-sink ┼─┐
            │       ▲                                                  (Connect)     │ │
            │       │                                                                │ │ AMQP 5672
            │  scada-artemis-sink (Connect) ←── Kafka ←── scada-rabbitmq-source ─────┼─┤ (cross-VM)
            │                                                          (Connect)     │ │
            │  Monitor(8080)  Demo(8090)  Kafdrop(9000)                              │ │
            └────────────────────────────────────────────────────────────────────────┘ │
                                                                                       │
            ┌────────────────────────── SCADA Windows VM ──────────────────────────┐   │
            │                                                                      │   │
            │   RabbitMQ ←──────────────────── (5672 in) ──────────────────────────┼───┘
            │   ▲ ▲    │                                                           │
            │   │ │    └→ (5672 out) ─────────── (back to TMS Connect)             │
            │   │ │                                                                │
            │   │ └─ AMQP 5672 (local)  scada-api (8091) ── publishes alarms       │
            │   └─── MQTT 1883 (local)                                             │
            │                                                                      │
            └──────────────────────────────────────────────────────────────────────┘
```

**Key points:**
- Bridge encrypts before publishing to RabbitMQ. SCADA decrypts on receipt.
- Only **one cross-VM TCP port** is required for data: `5672` (AMQP).
- All of Kafka / Artemis / Connect's internal chatter stays inside the TMS VM.
- Reverse direction (SCADA → TMS) flows over the **same** AMQP 5672 from
  the TMS Connect pod *initiating* the consumer connection — Windows VM
  never initiates a connection to TMS VM.

---

## 2 · Component placement

| Component | TMS Linux VM | SCADA Windows VM | Monitor VM | Why |
|---|---|---|---|---|
| Artemis | ✅ | — | — | Source/sink for TMS messages; native to TMS network. |
| Zookeeper + Kafka + Kafdrop | ✅ | — | — | Backbone — only one cluster, lives near the bridge. |
| Bridge (Spring Boot) | ✅ | — | — | Reads Artemis, writes Kafka, calls RabbitMQ across VMs. |
| Kafka Connect (+ all 7 connectors) | ✅ | — | — | Centralized; all connector pods open AMQP 5672 to SCADA VM. |
| Bootstrap jobs (topics, queues) | ✅ run from here | — | — | Kafka topics on TMS; RabbitMQ queue declared remotely or via SCADA admin. |
| Demo | ✅ | — | — | Reads Kafka directly; lives near the broker. |
| **RabbitMQ** (3.12-management + MQTT plugin) | — | ✅ | — | Endpoint the bridge writes to / Connect reads from. |
| **scada-api** (Python + Flask + paho-mqtt) | — | ✅ | — | Subscribes to MQTT, publishes alarms; SCADA-facing dashboard at :8091. |
| **Monitor** (`pas-scada-monitor`) | — | — | ✅ | Read-only HTTP probes over the network. Off the data path so a broker outage doesn't blind ops. |

---

## 3 · Network / firewall requirements

| From | To | Port | Protocol | Required? | Purpose |
|---|---|---|---|---|---|
| TMS Linux VM | SCADA Windows VM | **5672** | AMQP (TCP) | **YES** | Bridge sink + Connect source. The only required cross-VM data port. |
| TMS Linux VM | SCADA Windows VM | 15672 | HTTP (TCP) | optional | If you want to use RabbitMQ admin UI from TMS VM. |
| Monitor VM | TMS Linux VM | **8085** | HTTP | for monitor | Bridge actuator probe. |
| Monitor VM | TMS Linux VM | **8083** | HTTP | for monitor | Kafka Connect REST probe (connector states). |
| Monitor VM | TMS Linux VM | 9000, 8161 | HTTP | optional | Kafdrop / Artemis console probes. |
| Monitor VM | SCADA Windows VM | **15672** | HTTP | for monitor | RabbitMQ admin API probe. |
| Monitor VM | SCADA Windows VM | **8091** | HTTP | for monitor | scada-api `/api/status` probe. |
| Ops workstation | Monitor VM | 8080 | HTTP | for ops | The 19-component dashboard. |
| Ops workstation | TMS Linux VM | 8085, 8090, 8161, 9000 | HTTP | for ops | Bridge / Demo / Artemis console / Kafdrop. |
| Ops workstation | SCADA Windows VM | 8091, 15672 | HTTP | for ops | scada-api dashboard, RabbitMQ admin. |
| PAS source system | TMS Linux VM | 61616 | AMQP/CORE | depends on PAS deployment | TMS source publisher. |

> **Windows VM doesn't need any inbound from internal Windows services to
> the TMS VM.** All cross-VM traffic is initiated from TMS VM toward
> SCADA VM. This makes Windows-side firewall rules simple.

**You will need:**
- A stable hostname **or static IP** for the SCADA Windows VM (the bridge
  and Connect connector configs reference it). Examples in this doc use
  `scada-host.internal` — replace with your real value.
- TMS Linux VM's outbound firewall must allow TCP 5672 to the SCADA host.
- SCADA Windows VM's inbound firewall must allow TCP 5672 from the TMS host.

---

## 4 · Deployment approach — docker compose on both sides

Both VMs use **Docker Compose**. Reasons:

- You're already using docker-compose for Artemis (`messaging-infra/`).
- Existing Docker images work as-is — no rebuild needed for the VM split.
- Identical operator workflow on Linux and Windows: `docker compose up -d`.
- Simpler than k3s/k8s for a 2-VM deployment; no cluster networking to debug.
- The k8s manifests in this repo can stay around for local dev (minikube)
  but aren't used in this deployment.

> Alternative: **k3s on the Linux VM** lets you reuse the existing k8s
> manifests almost verbatim. Choose this only if your ops team prefers
> k8s. Notes for that path are at the bottom of this doc.

---

## 5 · TMS Linux VM — setup

### 5.1 · Prerequisites on the Linux VM

```bash
# Tested on Ubuntu 22.04 / 24.04. Adapt for RHEL/Debian.
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin git curl jq
sudo systemctl enable --now docker
sudo usermod -aG docker $USER     # then log out / back in
```

Verify: `docker version && docker compose version`.

### 5.2 · Pull the project onto the VM

```bash
sudo mkdir -p /opt/pinkline
sudo chown $USER /opt/pinkline
cd /opt/pinkline
git clone <your-repo-url> PAS-SCADA-Kafka-Bridge
git clone <messaging-infra-repo-url> messaging-infra
```

### 5.3 · Bring up Artemis (unchanged from dev)

```bash
cd /opt/pinkline/messaging-infra
docker compose up -d
docker ps --filter name=artemis     # expect: Up + ports 8161/61616
```

### 5.4 · Build / pull the project images on the VM

You have two choices:

**Option A — build on the VM** (simplest, no registry needed):
```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge
docker build -t pinkline/pas-scada-bridge:latest tms/
docker build -t pinkline/pas-scada-connect:latest connect/
docker build -t pinkline/pas-scada-monitor:latest monitor/
docker build -t pinkline/pas-scada-demo:1.0.0 demo/
```

**Option B — push images from your dev PC to a registry, pull on the VM:**
```bash
# On dev PC, after `docker build`:
docker tag pinkline/pas-scada-bridge:latest <registry>/pinkline/pas-scada-bridge:latest
docker push <registry>/pinkline/pas-scada-bridge:latest
# repeat for connect / monitor / demo

# On VM:
docker login <registry>
docker pull <registry>/pinkline/pas-scada-bridge:latest
# repeat for connect / monitor / demo
```

(`scada-api` is built on the **Windows VM**, not here.)

### 5.5 · Create the TMS-side compose file

This is **not** in the repo yet — write it from the existing k8s manifests.
Save as `/opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/tms-vm/docker-compose.yml`.

The shape (fill in the env values from the existing k8s configmaps):

```yaml
# deploy/tms-vm/docker-compose.yml — TMS Linux VM stack
version: "3.8"

networks:
  tms:
    driver: bridge

volumes:
  zk-data:
  zk-log:
  kafka-data:
  monitor-state:

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zk-data:/var/lib/zookeeper/data
      - zk-log:/var/lib/zookeeper/log
    networks: [tms]
    restart: unless-stopped

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    volumes:
      - kafka-data:/var/lib/kafka/data
    networks: [tms]
    restart: unless-stopped

  kafdrop:
    image: obsidiandynamics/kafdrop:4.0.1
    depends_on: [kafka]
    environment:
      KAFKA_BROKERCONNECT: kafka:9092
    ports: ["9000:9000"]
    networks: [tms]
    restart: unless-stopped

  bridge:
    image: pinkline/pas-scada-bridge:latest
    depends_on: [kafka]
    environment:
      # Artemis on the same VM, reached via host networking shortcut.
      ARTEMIS_HOST: host.docker.internal     # see 5.6 if this doesn't work
      ARTEMIS_PORT: "61616"
      ARTEMIS_USER: admin
      ARTEMIS_PASS: admin
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      # ⚠️ THIS is the cross-VM endpoint — point at the SCADA Windows VM.
      RABBITMQ_HOST: scada-host.internal      # ← change to real DNS / IP
      RABBITMQ_PORT: "5672"
      RABBITMQ_USER: thiru
      RABBITMQ_PASS: password                  # ← change for prod
      SPRING_PROFILES_ACTIVE: prod
    ports: ["8085:8085"]
    networks: [tms]
    extra_hosts:
      - "host.docker.internal:host-gateway"   # Linux: makes Artemis reachable
    restart: unless-stopped

  kafka-connect:
    image: pinkline/pas-scada-connect:latest
    depends_on: [kafka]
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:9092
      CONNECT_REST_PORT: "8083"
      CONNECT_GROUP_ID: pas-scada-connect
      CONNECT_CONFIG_STORAGE_TOPIC: connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: connect-statuses
      # Artemis on same VM
      ARTEMIS_HOST: host.docker.internal
      ARTEMIS_USER: admin
      ARTEMIS_PASS: admin
      # Cross-VM: RabbitMQ on Windows VM
      RABBITMQ_HOST: scada-host.internal     # ← change to real DNS / IP
      RABBITMQ_PORT: "5672"
      RABBITMQ_USER: thiru
      RABBITMQ_PASS: password                # ← change for prod
    ports: ["8083:8083"]
    networks: [tms]
    extra_hosts:
      - "host.docker.internal:host-gateway"
    restart: unless-stopped

  monitor:
    image: pinkline/pas-scada-monitor:latest
    depends_on: [bridge, kafka-connect]
    environment:
      # Targets the monitor probes — match the service names above
      BRIDGE_URL: http://bridge:8085
      KAFKA_CONNECT_URL: http://kafka-connect:8083
      RABBITMQ_URL: http://scada-host.internal:15672
      SCADA_API_URL: http://scada-host.internal:8091
    ports: ["8080:8080"]
    networks: [tms]
    restart: unless-stopped

  demo:
    image: pinkline/pas-scada-demo:1.0.0
    depends_on: [kafka, bridge]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    ports: ["8090:8090"]
    networks: [tms]
    restart: unless-stopped
```

**Important — the env keys above must match what the existing images
expect.** Cross-check by reading:

- `tms/k8s/overlay-minikube.yaml` — bridge env keys
- `connect/k8s/20-configmap.yaml` — Connect worker + connector env keys
- `monitor/k8s/20-secret.yaml` and `40-deployment.yaml` — monitor env keys
- `demo/k8s/10-configmap.yaml` — demo env keys

If a key name differs (e.g. the bridge expects `BRIDGE_RABBITMQ_HOST`
instead of `RABBITMQ_HOST`), use the name the image expects. **Don't
guess — check the YAML.**

### 5.6 · Bring up the TMS stack

```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/tms-vm
docker compose up -d
docker compose ps           # expect: all services Up
```

Then bootstrap Kafka topics (one-shot):
```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 --create \
  --topic tms.raw --partitions 3 --replication-factor 1
# repeat for: tms.scada.encrypted, scada.tms.raw, scada.tms.processed,
# scada.tms.alarms.state, plus the 4 dlq.connect.* topics.
```

(Or run the bootstrap job in `bootstrap/k8s/10-kafka-topics-job.yaml`
manually as a `docker run` — same idea, just outside k8s.)

Register the 7 Kafka Connect connectors via the REST API:
```bash
# After a few minutes for Connect to start:
for f in connect/connectors/*.json; do
  curl -X POST http://localhost:8083/connectors \
    -H 'Content-Type: application/json' --data @"$f"
done
curl http://localhost:8083/connectors | jq
```

> **The connector JSON files in `connect/connectors/` (referenced by
> `connect/k8s/40-job-register.yaml`) hardcode `rabbitmq-internal.scada.svc.cluster.local`
> for the RabbitMQ host. You MUST edit them to use `scada-host.internal`
> (or your real DNS / IP) before the source/sink connectors will work.**

### 5.7 · Verify TMS side

```bash
curl -s http://localhost:8085/actuator/health         # bridge: {"status":"UP"}
curl -s http://localhost:8083/connectors | jq         # 7 connector names
curl -s http://localhost:9000 -o /dev/null -w '%{http_code}\n'   # 200
```

---

## 6 · SCADA Windows VM — setup

### 6.1 · Prerequisites on the Windows VM

- **Docker Desktop for Windows** (with WSL 2 backend) — install and open it.
  After install, WSL 2 Linux containers must be enabled.
- **Git for Windows** (provides Git Bash) — for cloning the repo.
- **PowerShell 5.1+** (built in).

### 6.2 · Pull the SCADA-only files onto the VM

You need only `external-scada/` from the repo on this machine:
```powershell
mkdir C:\pinkline
cd C:\pinkline
git clone <your-repo-url> PAS-SCADA-Kafka-Bridge
```

### 6.3 · Build the scada-api image on the VM

```powershell
cd C:\pinkline\PAS-SCADA-Kafka-Bridge
docker build -t pinkline/pas-scada-api:latest external-scada/scada-api/
```

### 6.4 · Create the SCADA-side compose file

Save as `C:\pinkline\PAS-SCADA-Kafka-Bridge\deploy\scada-vm\docker-compose.yml`:

```yaml
# deploy/scada-vm/docker-compose.yml — SCADA Windows VM stack
version: "3.8"

networks:
  scada:
    driver: bridge

volumes:
  rabbitmq-data:

services:
  rabbitmq:
    image: rabbitmq:3.12-management
    hostname: rabbitmq
    environment:
      RABBITMQ_DEFAULT_USER: thiru
      RABBITMQ_DEFAULT_PASS: password           # ← change for prod
      # Enable MQTT plugin so scada-api can publish via MQTT
      RABBITMQ_PLUGINS: rabbitmq_management,rabbitmq_mqtt
    ports:
      - "5672:5672"          # AMQP — exposed to TMS VM
      - "15672:15672"        # admin UI
      - "1883:127.0.0.1:1883"  # MQTT — bind to localhost only (scada-api uses it)
    volumes:
      - rabbitmq-data:/var/lib/rabbitmq
    networks: [scada]
    restart: unless-stopped

  scada-api:
    image: pinkline/pas-scada-api:latest
    depends_on: [rabbitmq]
    environment:
      MQTT_HOST: rabbitmq
      MQTT_PORT: "1883"
      MQTT_USER: thiru
      MQTT_PASS: password
      AMQP_HOST: rabbitmq
      AMQP_USER: thiru
      AMQP_PASS: password
    ports:
      - "8091:8091"          # dashboard / API
    networks: [scada]
    restart: unless-stopped
```

> **Cross-check env keys** against `external-scada/k8s/60-scada-api-secret.yaml`
> and `external-scada/k8s/70-scada-api-deployment.yaml`. Use whatever
> names the image actually reads.

### 6.5 · Open the Windows firewall for AMQP 5672

PowerShell, **as Administrator**:
```powershell
New-NetFirewallRule -DisplayName "RabbitMQ AMQP from TMS VM" `
  -Direction Inbound -Protocol TCP -LocalPort 5672 `
  -RemoteAddress <TMS_VM_IP_OR_SUBNET> -Action Allow
```

(If you also want to reach the admin UI from TMS VM, repeat with `-LocalPort 15672`.)

### 6.6 · Bring up the SCADA stack

```powershell
cd C:\pinkline\PAS-SCADA-Kafka-Bridge\deploy\scada-vm
docker compose up -d
docker compose ps
```

### 6.7 · Declare the alarm queue + binding

(One-shot, idempotent — same as Step 5 in `MANUAL-RUN.md`):
```powershell
docker compose exec rabbitmq rabbitmqadmin --username=thiru --password=password `
  declare queue name=scada.tms.alarms.queue durable=true auto_delete=false

docker compose exec rabbitmq rabbitmqadmin --username=thiru --password=password `
  declare binding source=amq.topic destination=scada.tms.alarms.queue routing_key=scada.tms.alarms
```

### 6.8 · Verify SCADA side locally

```powershell
docker compose exec rabbitmq rabbitmq-plugins list | Select-String mqtt
# expect: rabbitmq_mqtt    [E*] (enabled)

# scada-api status
Invoke-RestMethod http://localhost:8091/api/status
# expect: mqtt_connected = true
```

Open **http://localhost:15672** (thiru/password) — RabbitMQ admin should
load. Queues tab should list `scada.tms.alarms.queue`.

Open **http://localhost:8091** — scada-api dashboard should load.

---

## 7 · Cross-VM connectivity test

From the **TMS Linux VM**:
```bash
# DNS / IP reachable?
ping -c 3 scada-host.internal

# AMQP port open?
nc -vz scada-host.internal 5672      # expect: succeeded / connected

# RabbitMQ AMQP login works (using a temporary container)?
docker run --rm rabbitmq:3.12-management timeout 5 rabbitmqctl \
  -n rabbit@scada-host.internal eval 'true.'
# (or simpler: check Connect's scada-rabbitmq-source connector status)
```

From the **SCADA Windows VM**:
```powershell
# scada-api should be self-sufficient — no outbound to TMS needed.
# But you can sanity-check the bridge dashboard from here for ops:
Invoke-WebRequest http://<TMS_VM_IP>:8085/actuator/health
```

### End-to-end test

On TMS VM:
```bash
# Forward direction: publish a TMS message into Artemis
docker compose -f /opt/pinkline/PAS-SCADA-Kafka-Bridge/test-publish.yaml ...
# (adapt the test-publish.yaml from k8s Job to a docker run / curl)

# Reverse direction: scada-api auto-publishes alarms every 10s — just watch
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic scada.tms.processed \
  --from-beginning --max-messages 1 --timeout-ms 8000
```

Or use the SCADA dashboard at `http://<SCADA_VM_IP>:8091` — the right
pane "SCADA → TMS" auto-publishes UpdateAlarm / KeepAlive every 10–120s.

---

## 8 · Configuration changes vs the dev setup

If you're carrying configs over from `MANUAL-RUN.md` / `start.sh`, these
**must** change for VM split:

| Where | Dev value | Prod value |
|---|---|---|
| Bridge `RABBITMQ_HOST` env | `rabbitmq-internal.scada.svc.cluster.local` | `scada-host.internal` (your real DNS / IP) |
| Connect `scada-rabbitmq-source` connector JSON `rabbitmq.host` | `rabbitmq-internal.scada.svc.cluster.local` | `scada-host.internal` |
| Connect `tms-rabbitmq-sink` connector JSON `rabbitmq.host` | `rabbitmq-internal.scada.svc.cluster.local` | `scada-host.internal` |
| Monitor `RABBITMQ_URL` / `SCADA_API_URL` env | in-cluster service names | `http://scada-host.internal:15672` / `:8091` |
| Bridge `ARTEMIS_HOST` env | `host.minikube.internal` | `host.docker.internal` (Linux: with `extra_hosts: host-gateway`) |
| RabbitMQ user/pass | `thiru` / `password` | **change to a strong password**, store outside YAML |
| Artemis user/pass | `admin` / `admin` | **change**, store outside YAML |

---

## 9 · What's NOT production-ready yet

Be aware of these gaps before going live with real customer data:

- **Plaintext credentials** — the compose files above embed passwords in
  env. Use Docker secrets, HashiCorp Vault, AWS Secrets Manager, or at
  minimum a `.env` file with restricted permissions (`chmod 600`).
- **No TLS** between TMS VM and SCADA VM. AMQP 5672 carries the encrypted
  payload but the AMQP credentials and metadata are in clear. Enable
  TLS on RabbitMQ (port 5671) and update bridge / Connect connector
  configs to use it. Strongly recommended for any non-trusted network.
- **Single instance everything.** No HA — a Kafka pod restart causes a
  brief gap, a RabbitMQ pod restart drops in-flight non-persisted msgs.
  For HA you'd run 3-broker Kafka, RabbitMQ cluster, and an active-passive
  Artemis. Out of scope for this doc.
- **No log/metric aggregation.** Add Prometheus + Grafana + Loki (or the
  ELK stack) for prod. The monitor at :8080 only does up/down; it isn't
  a metrics solution.
- **No backup strategy.** Snapshot the `kafka-data` and `rabbitmq-data`
  volumes regularly. Test restores.
- **Bridge probe timings** are tuned for slow minikube cold start (180s).
  Once you're on a real VM with faster I/O you can tighten them.
- **DLQ topics** auto-recreate but aren't actively monitored. Wire an
  alert (Prometheus rule, Slack webhook) on non-zero DLQ size.

---

## 10 · Alternative — k3s on the TMS Linux VM

If your ops team strongly prefers k8s, you can use **k3s** (lightweight
single-node k8s) on the TMS VM and reuse the existing manifests:

```bash
curl -sfL https://get.k3s.io | sh -
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER ~/.kube/config

cd /opt/pinkline/PAS-SCADA-Kafka-Bridge
kubectl apply -f tms/k8s/                 # everything in pinkline namespace
kubectl apply -f connect/k8s/
kubectl apply -f monitor/k8s/
kubectl apply -f demo/k8s/
kubectl apply -f bootstrap/k8s/
```

**You still need to edit:**
- the bridge configmap (`tms/k8s/overlay-minikube.yaml`) to point
  `RABBITMQ_HOST` at the Windows VM
- the Connect configmap (`connect/k8s/20-configmap.yaml`) likewise
- the connector JSON files (used by `connect/k8s/40-job-register.yaml`)

You **don't** apply `external-scada/k8s/` on the Linux VM — those go on
the Windows VM (or use docker-compose there as in section 6).

Trade-offs:
- ➕ Reuse existing manifests, all the dev-time muscle memory carries over.
- ➖ One more layer to operate (k3s upgrades, kubelet, etcd) for a
  single-node deployment.
- ➖ Manifests use ClusterIP services that need NodePort / LoadBalancer
  for external (Windows VM) reachability.

For 99% of 2-VM deployments, **docker-compose is simpler**. Pick k3s
only if you have an ops reason.

---

## 11 · Quick reference — URLs once everything is up

**TMS Linux VM** (replace `tms-host` with the VM's IP/DNS):

| URL | What | Login |
|---|---|---|
| http://tms-host:8161/console | Artemis | admin / admin |
| http://tms-host:8085/actuator/health | Bridge health | — |
| http://tms-host:9000 | Kafdrop | — |
| http://tms-host:8083/connectors?expand=status | Connect REST | — |
| http://tms-host:8080 | Monitor dashboard | — |
| http://tms-host:8090 | Demo | — |

**SCADA Windows VM** (replace `scada-host` with the VM's IP/DNS):

| URL | What | Login |
|---|---|---|
| http://scada-host:15672 | RabbitMQ admin | thiru / password |
| http://scada-host:8091 | scada-api dashboard | — |

---

## 12 · Bringing it all up — checklist

Run in this order:

1. ☐ SCADA Windows VM — Docker Desktop installed and running
2. ☐ SCADA Windows VM — `docker compose up -d` (RabbitMQ + scada-api)
3. ☐ SCADA Windows VM — declare queue + binding (rabbitmqadmin)
4. ☐ SCADA Windows VM — verify `http://localhost:8091/api/status` returns `mqtt_connected: true`
5. ☐ SCADA Windows VM — open firewall port 5672 to TMS VM
6. ☐ TMS Linux VM — Docker installed and running
7. ☐ TMS Linux VM — `docker compose up -d` for messaging-infra (Artemis)
8. ☐ TMS Linux VM — `docker compose up -d` for tms-vm stack
9. ☐ TMS Linux VM — bootstrap Kafka topics
10. ☐ TMS Linux VM — register 7 Connect connectors (with `scada-host` real value)
11. ☐ TMS Linux VM — `nc -vz scada-host 5672` succeeds
12. ☐ TMS Linux VM — `curl localhost:8083/connectors?expand=status` shows all 7 RUNNING
13. ☐ End-to-end — publish test on TMS, check `scada-api/api/received` shows decoded JSON

If any step fails, fix it before moving on (same rule as `MANUAL-RUN.md`).
