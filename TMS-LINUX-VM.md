# TMS Linux VM — runbook

Self-contained install + ops guide for the **TMS Linux VM**. Runs:
Artemis, Zookeeper, Kafka, Kafdrop, Bridge (Spring Boot), Kafka Connect
(with all 7 connectors), and the Demo dashboard.

The Monitor and SCADA components live on **separate VMs** — see
`MONITOR-VM.md` and `SCADA-WINDOWS-VM.md`. Architecture overview in
`VM-DEPLOY.md`.

---

## 1 · Before you start — values to gather

You need these before running anything:

| Variable | What | Example |
|---|---|---|
| `SCADA_HOST` | DNS name or IP of the SCADA Windows VM | `scada-host.internal` or `10.20.0.42` |
| `SCADA_RABBITMQ_USER` | RabbitMQ user (matches what's set on SCADA VM) | `thiru` |
| `SCADA_RABBITMQ_PASS` | RabbitMQ password (matches SCADA VM) | (strong password) |
| `ARTEMIS_USER` / `ARTEMIS_PASS` | Artemis admin creds | `admin` / `admin` (CHANGE for prod) |
| `PAS_SOURCE_HOST` | Where PAS publishes from (if remote) | (your value) |

**Confirm the SCADA VM is up first** (see `SCADA-WINDOWS-VM.md`) and that
its RabbitMQ is reachable on port 5672 from this VM:
```bash
nc -vz $SCADA_HOST 5672      # expect: succeeded
```
If that fails, fix the network / firewall / SCADA-side install before
continuing — there's no point starting TMS until SCADA is reachable.

---

## 2 · Install prerequisites

Tested on Ubuntu 22.04 / 24.04. Adapt for RHEL/Debian if needed.

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin git curl jq netcat-openbsd
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
# log out and back in so docker group takes effect
```

Verify:
```bash
docker version
docker compose version
```

---

## 3 · Get the code

```bash
sudo mkdir -p /opt/pinkline
sudo chown $USER:$USER /opt/pinkline
cd /opt/pinkline
git clone <PAS-SCADA-Kafka-Bridge repo URL> PAS-SCADA-Kafka-Bridge
git clone <messaging-infra repo URL> messaging-infra
```

Set `MESSAGING_INFRA` for convenience:
```bash
export MESSAGING_INFRA=/opt/pinkline/messaging-infra
echo 'export MESSAGING_INFRA=/opt/pinkline/messaging-infra' >> ~/.bashrc
```

---

## 4 · Build the images

You build **4 images** on this VM (scada-api builds on the Windows VM,
monitor builds on the Monitor VM):

```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge
docker build -t pinkline/pas-scada-bridge:latest tms/
docker build -t pinkline/pas-scada-connect:latest connect/
docker build -t pinkline/pas-scada-demo:1.0.0 demo/
```

The bridge build is Maven-based and takes ~5 min on first run.
Subsequent builds use Docker's layer cache and are ~30s.

> **Alternative — push from your dev PC instead of building here:**
> ```bash
> # on dev PC: docker tag … && docker push <registry>/…
> # on this VM:
> docker pull <registry>/pinkline/pas-scada-bridge:latest
> # repeat for connect, demo
> ```

---

## 5 · Bring up Artemis

```bash
cd $MESSAGING_INFRA
docker compose up -d
docker ps --filter name=artemis    # expect: Up + ports 8161/61616
curl -s http://localhost:8161 -o /dev/null -w "%{http_code}\n"   # 200/302/303
```

---

## 6 · Create the TMS compose file

Create `/opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/tms-vm/docker-compose.yml`:

```yaml
version: "3.8"
networks:
  tms:
    driver: bridge
volumes:
  zk-data:
  zk-log:
  kafka-data:

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
      ARTEMIS_HOST: host.docker.internal
      ARTEMIS_PORT: "61616"
      ARTEMIS_USER: ${ARTEMIS_USER:-admin}
      ARTEMIS_PASS: ${ARTEMIS_PASS:-admin}
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      # Cross-VM endpoint:
      RABBITMQ_HOST: ${SCADA_HOST}
      RABBITMQ_PORT: "5672"
      RABBITMQ_USER: ${SCADA_RABBITMQ_USER:-thiru}
      RABBITMQ_PASS: ${SCADA_RABBITMQ_PASS}
      SPRING_PROFILES_ACTIVE: prod
    ports: ["8085:8085"]
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks: [tms]
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
      ARTEMIS_HOST: host.docker.internal
      ARTEMIS_USER: ${ARTEMIS_USER:-admin}
      ARTEMIS_PASS: ${ARTEMIS_PASS:-admin}
      RABBITMQ_HOST: ${SCADA_HOST}
      RABBITMQ_PORT: "5672"
      RABBITMQ_USER: ${SCADA_RABBITMQ_USER:-thiru}
      RABBITMQ_PASS: ${SCADA_RABBITMQ_PASS}
    ports: ["8083:8083"]
    extra_hosts:
      - "host.docker.internal:host-gateway"
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

Create `deploy/tms-vm/.env` with restricted permissions:
```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/tms-vm
cat > .env <<EOF
SCADA_HOST=scada-host.internal
SCADA_RABBITMQ_USER=thiru
SCADA_RABBITMQ_PASS=<strong-password-here>
ARTEMIS_USER=admin
ARTEMIS_PASS=<strong-password-here>
EOF
chmod 600 .env
```

> **Cross-check the env keys** against `tms/k8s/overlay-minikube.yaml`
> and `connect/k8s/20-configmap.yaml` — if the bridge image expects
> `BRIDGE_RABBITMQ_HOST` (or similar) instead of `RABBITMQ_HOST`,
> rename above. Don't guess — read those YAMLs.

---

## 7 · Start the TMS stack

```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/tms-vm
docker compose up -d
docker compose ps                  # all services should be Up
docker compose logs -f bridge      # watch for "Started KafkaBridgeApplication"
# Ctrl-C the logs once it's up.
```

The bridge takes ~100s to fully boot (Spring Boot + Camel). Health:
```bash
curl -s http://localhost:8085/actuator/health
# expect: {"status":"UP"}
```

---

## 8 · Bootstrap Kafka topics

Topics needed: `tms.raw`, `tms.scada.encrypted`, `scada.tms.raw`,
`scada.tms.processed`, `scada.tms.alarms.state`, plus 4 DLQ topics.

Easiest way — run the existing bootstrap script inside the kafka container:
```bash
docker compose exec kafka bash -c '
for t in tms.raw tms.scada.encrypted scada.tms.raw scada.tms.processed scada.tms.alarms.state \
         dlq.connect.tms-artemis-source dlq.connect.tms-rabbitmq-sink \
         dlq.connect.scada-rabbitmq-source dlq.connect.scada-artemis-sink; do
  kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic $t --partitions 3 --replication-factor 1
done
kafka-topics --bootstrap-server kafka:9092 --list
'
```

Expect 9 topics + Kafka internals listed.

---

## 9 · Register the 7 Kafka Connect connectors

⚠️ **Before registering**, you MUST edit the 7 connector JSON files in
`connect/connectors/` (referenced by `connect/k8s/40-job-register.yaml`)
and replace every occurrence of:

```
rabbitmq-internal.scada.svc.cluster.local
```

with your actual SCADA host (e.g. `scada-host.internal`). The dev value
is a Kubernetes-internal DNS name that doesn't resolve outside minikube.

Then register:
```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge

# Wait for Connect REST to be ready
until curl -fs http://localhost:8083/connectors >/dev/null; do sleep 5; done

# Register all 7
for f in connect/connectors/*.json; do
  echo "Registering $(basename "$f")"
  curl -X POST http://localhost:8083/connectors \
    -H 'Content-Type: application/json' --data @"$f"
  echo
done

# Check states
curl -s "http://localhost:8083/connectors?expand=status" \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state}'
# expect: every state = "RUNNING"
```

Expected names:
- `tms-artemis-source`
- `tms-artemis-source-trafficreport`
- `tms-artemis-source-tsinfo`
- `tms-artemis-source-routeinfo`
- `tms-rabbitmq-sink`
- `scada-rabbitmq-source`
- `scada-artemis-sink`

---

## 10 · Verify

```bash
# Bridge healthy
curl -s http://localhost:8085/actuator/health     # {"status":"UP"}

# 7 connectors all RUNNING
curl -s "http://localhost:8083/connectors?expand=status" \
  | jq 'to_entries[] | select(.value.status.connector.state != "RUNNING")'
# expect: empty (no non-running connectors)

# Cross-VM connectivity to SCADA
nc -vz $SCADA_HOST 5672                           # succeeded

# Push a test message into Artemis (forward direction)
docker compose exec kafka bash -c 'kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic tms.scada.encrypted \
  --from-beginning --max-messages 3 --timeout-ms 8000'
# expect: encrypted (binary) payloads if the bridge has been processing
```

---

## 11 · Stop / restart / wipe

```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/tms-vm

# Stop one service
docker compose stop bridge

# Restart one service (e.g. after image rebuild)
docker compose up -d --force-recreate --no-deps bridge

# Stop everything (preserve volumes)
docker compose down

# Stop everything AND wipe Kafka data + Connect state (DESTRUCTIVE)
docker compose down -v

# Stop Artemis too
docker compose -f $MESSAGING_INFRA/docker-compose.yml down
```

---

## 12 · Common issues

| Symptom | Fix |
|---|---|
| Bridge logs `UnknownHostException: <SCADA_HOST>` | DNS doesn't resolve. Use IP in `.env`, or fix DNS / `/etc/hosts`. |
| Bridge logs `Connection refused` to RabbitMQ | SCADA VM firewall blocks 5672 from this VM. Open it on Windows side (see `SCADA-WINDOWS-VM.md` §5). |
| Bridge logs `ACCESS_REFUSED` from RabbitMQ | Wrong creds. Confirm `.env` `SCADA_RABBITMQ_USER/PASS` matches what RabbitMQ on SCADA VM expects. |
| `docker compose up` errors `Cannot start: bind 8161 already in use` | Another Artemis on the host. `docker stop <name>` it first. |
| Bridge stuck `Starting` for >3 min | Spring Boot cold start can take ~100s — wait. Beyond 3 min, check `docker compose logs bridge` for an exception (usually Artemis or RabbitMQ unreachable). |
| `host.docker.internal` doesn't resolve from bridge container | The `extra_hosts: host.docker.internal:host-gateway` line is missing or your Docker version is too old. Update Docker, or replace `host.docker.internal` with the VM's actual IP. |
| Connect connector state = `FAILED` | `curl localhost:8083/connectors/<name>/status \| jq` for the trace. Common: SCADA RabbitMQ creds, host wrong, or port blocked. After fixing, `curl -X POST localhost:8083/connectors/<name>/restart`. |
| `scada-rabbitmq-source` produces literal `[B@<hash>` | Old configmap with `StringConverter`. Confirm `value.converter: ByteArrayConverter` in connector JSON. Re-register the connector. |
| Kafka `CrashLoopBackOff`-equivalent (container restarting) with `InconsistentClusterIdException` | Stale data in `kafka-data` volume. Stop kafka, `docker volume rm <project>_kafka-data`, restart. **DESTRUCTIVE — drops all topics.** |
