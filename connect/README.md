# Kafka Connect — full pipeline

A single Kafka Connect worker running 4 Camel-based connectors that
together provide both forward (TMS → SCADA) and reverse (SCADA → TMS)
flow, fully replacing the I/O of the existing Camel bridge:

| # | Connector | Direction | Source / Sink |
|---|---|---|---|
| 1 | `tms-artemis-source`     | TMS → Kafka       | Artemis topic `TMS.PISInfo`             ──► Kafka `tms.raw` |
| 2 | `tms-rabbitmq-sink`      | Kafka → SCADA     | Kafka `tms.scada.encrypted`             ──► RabbitMQ exchange `amq.topic` / key `tms.scada.pas` |
| 3 | `scada-rabbitmq-source`  | SCADA → Kafka     | RabbitMQ queue `scada.tms.alarms.queue` ──► Kafka `scada.tms.raw` |
| 4 | `scada-artemis-sink`     | Kafka → TMS       | Kafka `scada.tms.processed`             ──► Artemis topic `SCADA.TMS.Alarms` |

A direct **Kafka → MQTT** sink is documented below as an *optional*
swap-in for the RabbitMQ sink; it is intentionally **not** in the
default ConfigMap.

The existing `tms/` Camel bridge sits in the middle, transforming
`tms.raw` → `tms.scada.encrypted` (forward) and
`scada.tms.raw` → `scada.tms.processed` (reverse) — but only after
you flip `BRIDGE_INPUT_FROM_KAFKA=true` and `BRIDGE_REVERSE_KAFKA_ENABLED=true`.
See the project [`README.md`](../README.md) for the full architecture and cutover steps.

## Architecture (full picture)

```
                         FORWARD                                     
   ┌─────────┐                                                   ┌─────────┐
   │ Artemis │  ─JMS─►  [1] tms-artemis-source  ──► tms.raw      │ SCADA   │
   │  TMS    │                                       │           │  API    │
   └─────────┘                                       ▼           │         │
                                          ┌──────────────────┐   │         │
                                          │  Streams app     │   │         │
                                          │  Forward topo    │   │         │
                                          └──────┬───────────┘   │         │
                                                 ▼               │         │
                                          tms.scada.encrypted    │         │
                                                 │               │         │
                                                 ├──► [2] tms-rabbitmq-sink  ──AMQP──► amq.topic ─MQTT─►
                                                 └──► [3] tms-mqtt-sink (off) ─direct MQTT─────────────►
                                                                                             ▲
                                                                                             │
                                                                                             ▼
                         REVERSE                                                     SCADA publishes
   ┌─────────┐                                                                       RSAE responses
   │ Artemis │  ◄─JMS─  [5] scada-artemis-sink  ◄── scada.tms.processed ◄────────────────────│
   │  TMS    │                                          ▲                                    │
   └─────────┘                                          │                                    │
                                          ┌──────────────────┐                               │
                                          │  Streams app     │                               │
                                          │  Reverse topo    │                               │
                                          └──────┬───────────┘                               │
                                                 │                                           │
                                              scada.tms.raw                                  │
                                                 ▲                                           │
                                                 └── [4] scada-rabbitmq-source ◄──RabbitMQ───┘
                                                                              queue
```

## Why "pooled" Artemis variants

The plain `camel-jms-apache-artemis-{source,sink}` Kamelets in 4.8.x
do not expose `username`/`password`. Their pooled cousins do, and as a
free bonus give you connection pooling. We use the pooled variants for
both the source (Phase 2) and the sink (Phase 4).

## Direct MQTT sink (optional swap-in)

If you ever want to drop RabbitMQ from the forward path and have
Connect write directly to MQTT, swap `tms-rabbitmq-sink` for the JSON
below. **Do not register both** — they consume from the same
`tms.scada.encrypted` topic and would double-deliver to SCADA.

To switch:

1. Add this block to `connect/k8s/20-configmap.yaml` under `data:`.
2. Delete the `tms-rabbitmq-sink.json` block from the same file.
3. `kubectl apply -f connect/k8s/20-configmap.yaml`
4. `curl -X DELETE http://kafka-connect.pinkline.svc.cluster.local:8083/connectors/tms-rabbitmq-sink`
5. Re-run the registration Job.

```yaml
  tms-mqtt-sink.json: |
    {
      "connector.class": "org.apache.camel.kafkaconnector.mqtt5sink.CamelMqtt5sinkSinkConnector",
      "tasks.max": "1",
      "topics": "tms.scada.encrypted",
      "camel.kamelet.mqtt5-sink.brokerUrl": "tcp://10.4.0.25:31883",
      "camel.kamelet.mqtt5-sink.topic": "tms/scada/pas",
      "camel.kamelet.mqtt5-sink.username": "${env:MQTT_USER}",
      "camel.kamelet.mqtt5-sink.password": "${env:MQTT_PASS}",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
      "errors.tolerance": "all",
      "errors.log.enable": "true",
      "errors.deadletterqueue.topic.name": "dlq.connect.tms-mqtt-sink",
      "errors.deadletterqueue.topic.replication.factor": "1",
      "errors.deadletterqueue.context.headers.enable": "true",
      "errors.retry.timeout": "600000",
      "errors.retry.delay.max.ms": "30000"
    }
```

The Kamelet only exposes `topic`, `brokerUrl`, `username`, `password` —
no QoS or retain. The `paho-mqtt5` defaults (QoS=1, no retain) match
SCADA's existing subscription. If you need different settings, fall
back to the generic `camel-paho-mqtt5-kafka-connector`.

## Why Streams sits between the source and the sinks

The Connect connectors are dumb pipes; they cannot run AES encryption
or XML→JSON conversion. The Streams app does that work:

- Forward: reads `tms.raw` (XML), writes `tms.scada.encrypted` (encrypted JSON).
- Reverse: reads `scada.tms.raw` (encrypted JSON), writes `scada.tms.processed` (decrypted, optionally XML).

So the connectors handle protocol bridging; the Streams app handles
content transform. The split keeps connector configs simple and lets
us keep the existing crypto/transform logic identical to the Camel
bridge.

## Build the image

```bash
docker build -t pinkline/pas-scada-connect:latest connect/
docker push pinkline/pas-scada-connect:latest
```

The Dockerfile downloads 5 plugin tarballs from Maven Central and
unpacks them into `/usr/share/confluent-hub-components/`. Verify after
build:

```bash
docker run --rm pinkline/pas-scada-connect:latest \
  ls /usr/share/confluent-hub-components/
# Expect 5 directories — one per connector.
```

## Deploy

```bash
kubectl apply -f connect/k8s/10-secret.yaml
kubectl apply -f connect/k8s/20-configmap.yaml
kubectl apply -f connect/k8s/30-deployment.yaml
kubectl -n pinkline wait --for=condition=available deploy/kafka-connect --timeout=180s
kubectl apply -f connect/k8s/40-job-register.yaml
kubectl -n pinkline logs -f job/register-connectors
```

The Job registers every `<name>.json` in the `connect-config` ConfigMap.
Order does not matter — `PUT /connectors/<name>/config` is idempotent.

Healthy output:

```
→ tms-artemis-source
  PUT /config -> HTTP 201
  registered
→ tms-rabbitmq-sink
  PUT /config -> HTTP 201
  registered
→ scada-rabbitmq-source
  PUT /config -> HTTP 201
  registered
→ scada-artemis-sink
  PUT /config -> HTTP 201
  registered
...
Connector statuses (after 5s):
  "name":"tms-artemis-source"
  "state":"RUNNING"
  "name":"tms-rabbitmq-sink"
  "state":"RUNNING"
  ...
```

## Reliability features (built into every connector here)

| Concern | What protects it |
|---|---|
| Connect pod restart | State (configs, offsets, status) lives in `_connect-*` Kafka topics → resumes cleanly |
| Source broker restart | Camel JMS / RabbitMQ clients auto-reconnect with backoff; pooled connections + durable queues mean nothing is lost while down |
| Kafka broker restart | Producer config: `acks=all`, `enable.idempotence=true`, `retries=Integer.MAX_VALUE` |
| Bad message | `errors.tolerance=all` + per-connector DLQ topic `dlq.connect.<name>` with original payload + error in headers |
| Long downstream outage | Source connectors keep polling; messages buffer at the source until Connect catches up |

## Verify the full pipeline (Phase 4 end-to-end)

Three things should happen in sequence when you publish to TMS:

```bash
# 1. Publish XML to Artemis
kubectl -n pinkline run -it --rm artemis-cli \
  --image=apache/activemq-artemis:2.31.2 --restart=Never --command -- /bin/sh -c \
  'artemis producer --url tcp://artemis-service:61616 \
     --user pasbridge --password testpass123 \
     --destination topic://TMS.PISInfo --message-count 1 \
     --message "<rcsMsg><data>phase 4 e2e</data></rcsMsg>"'

# 2. Open Kafdrop and confirm one message in:
#    tms.raw                   ← from tms-artemis-source
#    tms.scada.encrypted       ← from Streams forward
#
# 3. Confirm RabbitMQ web console shows one message published to:
#    Exchange: amq.topic
#    Routing key: tms.scada.pas
#
# 4. Confirm the SCADA API dashboard (port 8091) shows one decrypted
#    message received.
```

For the reverse path, publish from SCADA:

```bash
# Trigger an RSAE response (the SCADA simulator publishes UpdateAlarm
# automatically every 10s). Then watch:
#    scada.tms.raw             ← from scada-rabbitmq-source
#    scada.tms.processed       ← from Streams reverse
#    Artemis topic SCADA.TMS.Alarms ← from scada-artemis-sink
```

## REST API — common operations

```bash
CONNECT=http://kafka-connect.pinkline.svc.cluster.local:8083

# Status of every connector at once
curl $CONNECT/connectors?expand=status | jq

# Restart a failed connector + its tasks
curl -X POST "$CONNECT/connectors/<name>/restart?includeTasks=true"

# Pause / resume
curl -X PUT $CONNECT/connectors/<name>/pause
curl -X PUT $CONNECT/connectors/<name>/resume

# Show effective config (verifies env-var substitution worked)
curl $CONNECT/connectors/<name>/config

# Delete (tasks stop, config removed from _connect-configs)
curl -X DELETE $CONNECT/connectors/<name>

# Inspect DLQ of a specific connector
kubectl -n pinkline run -it --rm kcat --image=edenhill/kcat:1.7.1 --restart=Never -- \
  kcat -b kafka-service:9092 -t dlq.connect.<name> -C -e
```

## Known limitations

* **Non-durable JMS subscription on tms-artemis-source.** The Kamelet
  doesn't expose `clientId` / `durableSubscriptionName`. Messages
  published while the Connect pod is restarting are missed (k8s
  restarts in <30s, so the gap is small). Fix path: replace this one
  connector with `camel-jms-kafka-connector` (generic) and an inline
  Camel JMS URI that includes `durableSubscriptionName=...`.
* **MQTT sink ignores QoS / retain.** The `mqtt5-sink` Kamelet only
  exposes `topic`, `brokerUrl`, `username`, `password`. Defaults
  (QoS=1, no retain) match SCADA's existing subscription, but if you
  need different settings, fall back to the generic `camel-paho-mqtt5-kafka-connector`.
* **`auto.create.topics.enable` is on the broker.** Connect's DLQ
  topics are auto-created on first error. In a future production
  cluster with auto-create off, pre-create the 5 `dlq.connect.*` topics.

## Files

```
connect/
  Dockerfile               — cp-kafka-connect 7.5.0 + 5 Camel plugins (4.8.3)
  README.md                — this file
  k8s/
    10-secret.yaml         — Artemis + RabbitMQ + MQTT credentials
    20-configmap.yaml      — 5 connector definitions
    30-deployment.yaml     — Connect worker + Service
    40-job-register.yaml   — generic registration loop over all definitions
```

## Sources

- [Camel Kafka Connector 4.8.x — Maven Central](https://central.sonatype.com/artifact/org.apache.camel.kafkaconnector/camel-kafka-connector)
- [Confluent cp-kafka-connect image config reference](https://docs.confluent.io/platform/current/installation/docker/config-reference.html#connect-distributed-configuration)
- [Kafka Connect REST API reference](https://docs.confluent.io/platform/current/connect/references/restapi.html)
