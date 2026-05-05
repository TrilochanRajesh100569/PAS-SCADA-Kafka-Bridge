# Client-Requested Deployment

What the client asked for, what gets deployed, and how to bring it up.

## The ask

> Run all apps (TMS, SCADA, monitor) on Kubernetes. Run **Artemis alone**
> from `D:\pinkline\code\messaging-infra` (Docker on the host).
> The Kafka Connect sink/source path must be active.

So the layout is:

```
┌────────────────────────────────────────┐   ┌──────────────────────────────┐
│           HOST (Docker)                │   │       MINIKUBE (k8s)         │
│                                        │   │                              │
│  Artemis  (messaging-infra/            │◄──┼─ host.minikube.internal     │
│            docker-compose.yml)         │   │                              │
│  - 61616  core / JMS                   │   │ namespace: pinkline          │
│  - 8161   web console                  │   │   - zookeeper                │
└────────────────────────────────────────┘   │   - kafka                    │
                                             │   - kafdrop                  │
                                             │   - kafka-connect            │
                                             │   - pas-scada-bridge         │
                                             │     (Connect-mode)           │
                                             │                              │
                                             │ namespace: scada             │
                                             │   - rabbitmq (+ MQTT plugin) │
                                             │   - scada-api                │
                                             └──────────────────────────────┘
```

## What runs where

| Component | Where | Image |
|---|---|---|
| Artemis | Docker (host) | `apache/activemq-artemis:2.33.0-alpine` |
| Zookeeper | k8s `pinkline` | `confluentinc/cp-zookeeper:7.5.0` |
| Kafka | k8s `pinkline` | `confluentinc/cp-kafka:7.5.0` |
| Kafdrop (Kafka UI) | k8s `pinkline` | `obsidiandynamics/kafdrop:4.0.1` |
| Kafka Connect | k8s `pinkline` | `pinkline/pas-scada-connect:latest` (built locally from `connect/Dockerfile`) |
| Bridge (Camel) | k8s `pinkline` | `pinkline/pas-scada-bridge:latest` |
| RabbitMQ | k8s `scada` | `rabbitmq:3.12-management` |
| SCADA API (mock) | k8s `scada` | `ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest` |
| Health monitor | k8s `pinkline` (deployable anywhere) | `pinkline/pas-scada-monitor:latest` |
| Customer demo | k8s `pinkline` (deployable anywhere) | `pinkline/pas-scada-demo:1.0.0` |

## Observability

- **Monitor** (`pinkline/pas-scada-monitor`) — FastAPI dashboard at port 8080.
  Probes 19 things on a 30 s cycle: every infra component (Artemis, Kafka,
  Zookeeper, RabbitMQ, Kafdrop, scada-api, bridge, demo) plus all 7 Connect
  connector statuses. Email alert on DOWN/RECOVERY (SMTP_USER/PASS in
  `monitor/k8s/20-secret.yaml` are placeholders — replace before going live).
- **Demo** (`pinkline/pas-scada-demo`) — FastAPI live data table + animated
  flow diagram at port 8090. `MODE=live` proxies real bridge messages.

Both can run on Server 1, Server 2, or a separate operator host — they only
need network access to the services they probe.

## Connectors registered (full Connect path)

| Connector | Direction |
|---|---|
| `tms-artemis-source` | Artemis `TMS.PISInfo` → Kafka `tms.raw` |
| `tms-artemis-source-trafficreport` | Artemis `RCS.E2K.TMS.TrafficReportClient` → Kafka `tms.raw` |
| `tms-artemis-source-tsinfo` | Artemis `TSInfo` → Kafka `tms.raw` |
| `tms-artemis-source-routeinfo` | Artemis `RCS.E2K.TMS.RouteInfo` → Kafka `tms.raw` |
| `tms-rabbitmq-sink` | Kafka `tms.scada.encrypted` → RabbitMQ `amq.topic`/`tms.scada.pas` |
| `scada-rabbitmq-source` | RabbitMQ queue `scada.tms.alarms.queue` → Kafka `scada.tms.raw` |
| `scada-artemis-sink` | Kafka `scada.tms.processed` → Artemis `SCADA.TMS.Alarms` |

The four `tms-artemis-source-*` connectors all write into the same Kafka
`tms.raw` topic, so the bridge's downstream transform/encrypt pipeline is
unchanged. The Camel JMS kamelet only accepts a single `destinationName`
per connector, hence the 1-connector-per-Artemis-topic pattern.

> Note: `bridge.artemis.topics` (env `BRIDGE_ARTEMIS_TOPICS`) in Spring Boot
> `application.properties` lists those same 4 topics, but it is only used
> by the **legacy direct-mode** Camel route in `KafkaBridgeRoutes`. In the
> Connect-mode setup it is dead config — Connect owns Artemis subscriptions.

Bridge is switched to **Connect-mode** (`BRIDGE_INPUT_FROM_KAFKA=true`,
`BRIDGE_REVERSE_KAFKA_ENABLED=true`) so it does NOT also subscribe to Artemis
directly — Connect owns that boundary.

## Encryption split

- **TMS → SCADA: encrypted.** Bridge `EncryptProcessor` (AES-256-GCM)
  on the forward path, SCADA API decrypts inbound.
- **SCADA → TMS: plain JSON.** Controlled by two flags introduced this
  session:
  - `bridge.reverse-kafka.encrypt-enabled=false` (env
    `BRIDGE_REVERSE_KAFKA_ENCRYPT_ENABLED=false`) — bridge skips the
    decrypt step on the reverse route.
  - `SCADA_OUTBOUND_ENCRYPT=false` (default in `scada-api/app.py`) —
    `publish()` writes the JSON envelope as plain UTF-8 bytes instead of
    AES-encrypted base64.

## Configuration deltas vs. baseline

Files edited to make the in-cluster RabbitMQ + Connect-mode + split-encryption flow work:

1. `connect/k8s/20-configmap.yaml`
   - RabbitMQ sink/source connectors point at
     `rabbitmq-internal.scada.svc.cluster.local:5672` (in-cluster) instead of
     `host.minikube.internal:5673`.
   - `scada-rabbitmq-source.value.converter` changed from `StringConverter`
     to `ByteArrayConverter`. The Camel kamelet emits the AMQP body as
     `byte[]`; StringConverter wrapped it via `Object.toString()` and
     produced literal `[B@<hash>` instead of UTF-8 bytes, breaking the
     reverse path silently.
2. `tms/k8s/overlay-minikube.yaml`
   - `BRIDGE_INPUT_FROM_KAFKA=true`, `BRIDGE_REVERSE_KAFKA_ENABLED=true`,
     `BRIDGE_REVERSE_KAFKA_ENCRYPT_ENABLED=false` (split encryption).
   - `BRIDGE_PIPELINE=kafka` (was `kafka,rabbitmq,mqtt`). Connect owns
     RabbitMQ + MQTT delivery; the bridge only transforms and writes Kafka.
     Without this change SCADA receives each message 3× (Camel rabbitmq
     step + Camel mqtt step + Connect tms-rabbitmq-sink). With it, exactly
     one canonical copy through Connect.
3. `external-scada/k8s/60-scada-api-secret.yaml`
   - `MQTT_USER/PASS` changed from `admin/admin` → `thiru/password` to match
     RabbitMQ default user (rc=4 "bad credentials" otherwise → SCADA API
     never connects to MQTT).
4. `tms/src/main/java/.../config/BridgeConfig.java` — added
   `ReverseKafka.encryptEnabled` field (default `true`).
5. `tms/src/main/java/.../routes/KafkaBridgeRoutes.java` — reverse-kafka
   route now reads `rev.isEncryptEnabled()` instead of the global
   `config.getEncrypt().isEnabled()`.
6. `external-scada/scada-api/app.py` — added `SCADA_OUTBOUND_ENCRYPT` env
   toggle (default `false`) on the publish path.

In-cluster patches applied by `start.sh` on every run:

- `pas-scada-bridge` — `imagePullPolicy=IfNotPresent` (use the locally-loaded
  image instead of pulling `:latest`) and probe timings bumped
  (`liveness initialDelaySeconds=180`, `readiness initialDelaySeconds=120`)
  because Spring Boot + Camel takes ~100 s to boot on minikube and the
  shipped 30 s/20 s defaults caused crash-loops.
- `scada-api` — `imagePullPolicy=IfNotPresent`.

`bootstrap/k8s/20-rabbitmq-queue-job.yaml` is **not** used in this setup
(it points to external IP `10.4.0.25`). The RabbitMQ queue is declared
in-cluster via the management API from `start.sh`.

## One-time fixes that may need re-applying after a `minikube stop` / wipe

- **Stale `kafka-data` PVC** — minikube's hostpath provisioner reuses the
  same on-disk directory across PVC delete+recreate, so a Kafka cluster-id
  written by an old Zookeeper survives. Symptom: Kafka pod
  `CrashLoopBackOff` with `InconsistentClusterIdException`. Fix: run
  ```bash
  kubectl -n pinkline scale deploy kafka --replicas=0
  kubectl -n pinkline run kafka-wipe --rm -i --restart=Never --image=busybox \
    --overrides='{"spec":{"containers":[{"name":"kafka-wipe","image":"busybox","command":["sh","-c","rm -rf /data/* /data/.[!.]*"],"volumeMounts":[{"name":"d","mountPath":"/data"}]}],"volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"kafka-data"}}]}}'
  kubectl -n pinkline scale deploy kafka --replicas=1
  ```

## How to bring it up

```bash
./start.sh
```

That script is idempotent: starts minikube if stopped, brings up Artemis if
not running, builds the Connect image only if missing, loads images into
minikube, applies all manifests, runs the bootstrap jobs, declares the
SCADA queue, registers the connectors, and prints the access URLs.

## How to verify

```bash
kubectl -n pinkline get pods
kubectl -n scada get pods

# Connect REST API
kubectl -n pinkline port-forward svc/kafka-connect 8083:8083 &
curl -s localhost:8083/connectors
# expect: ["tms-artemis-source","tms-rabbitmq-sink","scada-rabbitmq-source","scada-artemis-sink"]

# Forward path (TMS → SCADA, encrypted)
kubectl apply -f test-publish.yaml
# Then check SCADA decoded payload:
kubectl -n scada port-forward svc/scada-api-internal 8091:8091 &
curl -s localhost:8091/api/received | jq '.[0].decoded'
# expect: {"messageType":"TMS_PAS_UPDATE","platformPredictions":[...]}

# Reverse path (SCADA → TMS, plain JSON) — SCADA auto-publishes alarms
kubectl exec -n pinkline deploy/kafka -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 --topic scada.tms.processed \
  --partition 2 --offset earliest --max-messages 1 --timeout-ms 8000
# expect: {"CreatorId":"ScateX","Type":"UpdateAlarm","Alarm":{...}}

# Reverse path (final hop) — Artemis SCADA.TMS.Alarms message count
docker exec artemis sh -c "/var/lib/artemis-instance/bin/artemis queue stat \
  --user admin --password admin --url tcp://localhost:61616" | grep SCADA.TMS.Alarms
# expect: MESSAGE_COUNT > 0 and growing
```

## Verified end-to-end (2026-04-29 session)

Both directions traced byte-by-byte through every hop:

**TMS → SCADA (encrypted):**
- Artemis `TMS.PISInfo` (XML, e.g. `<TTGUID>test-realtime-001</TTGUID>`)
- → Kafka `tms.raw` (raw XML via `tms-artemis-source`)
- → Kafka `tms.scada.encrypted` (656-byte AES-GCM base64 via bridge)
- → RabbitMQ `amq.topic` rk `tms.scada.pas` (via `tms-rabbitmq-sink`)
- → MQTT `tms/scada/pas` (via RabbitMQ MQTT plugin)
- → SCADA API `/api/received` decoded `messageType: TMS_PAS_UPDATE`,
  `trainId: test-realtime-001` ← matches the originating XML

**SCADA → TMS (plain JSON):**
- SCADA API `/api/sent` (RSAE envelope JSON, e.g. `Type: UpdateAlarm`,
  `Alarm.Id: FIRE_DETECT_OFFICE_A`)
- → MQTT `scada/tms/alarms`
- → RabbitMQ queue `scada.tms.alarms.queue`
- → Kafka `scada.tms.raw` (via `scada-rabbitmq-source`, ByteArrayConverter)
- → Kafka `scada.tms.processed` (via bridge, no-decrypt path)
- → Artemis `SCADA.TMS.Alarms` (via `scada-artemis-sink`)

## How to tear down

```bash
kubectl delete ns pinkline scada
docker compose -f /d/pinkline/code/messaging-infra/docker-compose.yml down
minikube stop      # or: minikube delete
```
