# Workflow — TMS ↔ SCADA, internals deep-dive

How a single message moves through every component, what happens **inside**
each connector and Camel route, how the dashboards observe it, and how the
DLQ system prevents data loss.

---

## Table of contents

1. [Forward path: TMS → SCADA](#1--forward-path-tms--scada)
   - 1.1 [End-to-end flow](#11--end-to-end-flow)
   - 1.2 [Inside the source connector](#12--inside-the-source-connector-tms-artemis-source)
   - 1.3 [Inside the Camel bridge route](#13--inside-the-camel-bridge-route-forward)
   - 1.4 [Inside the sink connector](#14--inside-the-sink-connector-tms-rabbitmq-sink)
   - 1.5 [Inside SCADA-API (consumer)](#15--inside-scada-api-the-consumer)
2. [Reverse path: SCADA → TMS](#2--reverse-path-scada--tms)
   - 2.1 [End-to-end flow](#21--end-to-end-flow)
   - 2.2 [Inside the source connector](#22--inside-the-source-connector-scada-rabbitmq-source)
   - 2.3 [Inside the Camel reverse route](#23--inside-the-camel-reverse-route)
   - 2.4 [Inside the sink connector](#24--inside-the-sink-connector-scada-artemis-sink)
3. [Dashboard / monitor — how it observes everything](#3--dashboard--monitor)
4. [DLQ — preventing data loss](#4--dlq--preventing-data-loss)
5. [The two operating modes](#5--the-two-operating-modes)
6. [Mode A — Artemis-direct flows in detail](#6--mode-a--artemis-direct-flows-in-detail)
7. [Bootstrap — Kafka topics and RabbitMQ queue](#7--bootstrap)
8. [Encryption deep-dive — AES-256-GCM mechanics](#8--encryption-deep-dive)
9. [Bridge pipeline configuration](#9--bridge-pipeline-configuration)
10. [Alarm-state snapshot replay](#10--alarm-state-snapshot-replay)
11. [In-bridge monitor route](#11--in-bridge-monitor-route)
12. [Concrete example — UpdateAlarm every 10s, traced](#12--concrete-example--updatealarm-every-10s-traced)
13. [Connectivity matrix — ports and protocols](#13--connectivity-matrix)
14. [Configuration file map](#14--configuration-file-map)
15. [Per-environment config files](#15--per-environment-config-files)

---

# 1 · Forward path: TMS → SCADA

**Direction:** TMS publisher → Artemis → Kafka → Bridge (encrypt) → Kafka → RabbitMQ → MQTT → SCADA
**Encryption:** AES-256-GCM applied by the bridge
**Body shape changes:** XML → JSON → encrypted bytes → MQTT bytes → decrypted JSON

## 1.1 · End-to-end flow

```
   ┌──────────────┐    XML over JMS    ┌──────────────────┐
   │ TMS publisher│ ───────────────►   │ ARTEMIS          │
   └──────────────┘  (port 61616)      │  TMS.PISInfo     │
                                       │  multicast addr  │
                                       └────────┬─────────┘
                                                │ JMS subscriber drains
                                                ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  KAFKA CONNECT pod  ─  tms-artemis-source connector              │
   │  (Section 1.2 — internals)                                       │
   └────────┬─────────────────────────────────────────────────────────┘
            │ XML String → byte[]
            ▼
   ┌──────────────────────────┐
   │ Kafka topic: tms.raw     │   ← durable, replayable
   └────────┬─────────────────┘
            │ consumed by:
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  BRIDGE pod  ─  Camel route inbound-kafka-tms-raw                │
   │  (Section 1.3 — internals)                                       │
   │                                                                  │
   │  bytes → String → XmlToJson → Encrypt → bytes                    │
   └────────┬─────────────────────────────────────────────────────────┘
            │ encrypted byte[]
            ▼
   ┌─────────────────────────────────┐
   │ Kafka topic: tms.scada.encrypted│   ← wire format SCADA receives
   └────────┬────────────────────────┘
            │ consumed by:
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  KAFKA CONNECT pod  ─  tms-rabbitmq-sink connector               │
   │  (Section 1.4 — internals)                                       │
   └────────┬─────────────────────────────────────────────────────────┘
            │ AMQP publish to amq.topic / tms.scada.pas
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  RABBITMQ                                                        │
   │   amq.topic  ─routingKey "tms.scada.pas"─►  MQTT plugin          │
   │   re-publishes to MQTT topic "tms/scada/pas" (dot→slash mapping) │
   └────────┬─────────────────────────────────────────────────────────┘
            │ MQTT (port 1883)
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  SCADA-API pod                                                   │
   │  (Section 1.5 — internals)                                       │
   │                                                                  │
   │  MQTT subscriber → decrypt → JSON → ring buffer → /api/received  │
   └──────────────────────────────────────────────────────────────────┘
```

## 1.2 · Inside the source connector (`tms-artemis-source`)

**Job:** drain XML messages from Artemis topics into Kafka topic `tms.raw`.

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: kafka-connect pod                                   │
   │  Process:   Kafka Connect worker (Java)                         │
   │  Image:     pinkline/pas-scada-connect (Confluent + kamelets)   │
   │                                                                 │
   │  Connector class:                                               │
   │     CamelJmspooledapacheartemissourceSourceConnector            │
   │                                                                 │
   │  ┌───────────────────────────────────────────────────────────┐  │
   │  │  Connector lifecycle                                      │  │
   │  │                                                           │  │
   │  │  1. REST POST /connectors with config JSON                │  │
   │  │     (done by job-register.yaml on first install)          │  │
   │  │                                                           │  │
   │  │  2. Connect framework spawns 1 task (tasks.max=1)         │  │
   │  │                                                           │  │
   │  │  3. Task initializes Camel context with kamelet:          │  │
   │  │       jms-pooled-apache-artemis-source                    │  │
   │  │                                                           │  │
   │  │  4. Kamelet creates a JMS connection pool to:             │  │
   │  │       tcp://host.minikube.internal:61616                  │  │
   │  │       user/pass = ${ARTEMIS_USER}/${ARTEMIS_PASSWORD}     │  │
   │  │                                                           │  │
   │  │  5. Subscribes to JMS topic "TMS.PISInfo"                 │  │
   │  │       Artemis creates an internal multicast queue         │  │
   │  │       (the random UUID like d228db79-... you see in       │  │
   │  │       the Artemis console)                                │  │
   │  │                                                           │  │
   │  │  6. Loop forever:                                         │  │
   │  │       a. JMS.receive() blocks until message arrives       │  │
   │  │       b. Convert TextMessage → SourceRecord with:         │  │
   │  │            key   = null                                   │  │
   │  │            value = String (the XML body)                  │  │
   │  │            topic = "tms.raw"   (from "topics" config)     │  │
   │  │       c. Apply key.converter / value.converter            │  │
   │  │            both = StringConverter → String → byte[] UTF-8 │  │
   │  │       d. Forward to Kafka producer (Connect-managed)      │  │
   │  │       e. After Kafka ACK: JMS commit → Artemis deletes    │  │
   │  │            the message from the queue                     │  │
   │  └───────────────────────────────────────────────────────────┘  │
   │                                                                 │
   │  Error handling (errors.tolerance=all):                         │
   │    • Transient (broker down, timeout) → retry up to 600000ms    │
   │      with backoff capped at 30000ms                             │
   │    • Permanent (parse error etc.) → write SourceRecord to       │
   │      dlq.connect.tms-artemis-source with __connect.errors.*     │
   │      headers (exception class, stack trace, original topic)     │
   │    • Connector stays RUNNING; bad messages don't stop the flow  │
   └─────────────────────────────────────────────────────────────────┘
```

There are **4 of these** registered, one per Artemis topic (`TMS.PISInfo`,
`RCS.E2K.TMS.TrafficReportClient`, `TSInfo`, `RCS.E2K.TMS.RouteInfo`).
All 4 write to the same Kafka topic `tms.raw` — downstream doesn't care
which source.

## 1.3 · Inside the Camel bridge route (forward)

**Job:** consume `tms.raw`, transform XML→JSON, encrypt, publish to `tms.scada.encrypted`.

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: pas-scada-bridge pod                                │
   │  Process:   Spring Boot + Camel (Java 17)                       │
   │  Image:     pinkline/pas-scada-bridge                           │
   │                                                                 │
   │  Defined in: tms/.../routes/KafkaBridgeRoutes.java:182          │
   │  Activated when: bridge.input-kafka.enabled=true                │
   │                                                                 │
   │  ┌───────────────────────────────────────────────────────────┐  │
   │  │  Camel route: inbound-kafka-tms-raw                       │  │
   │  │                                                           │  │
   │  │   from("kafka:tms.raw?...")              ◄── Step 1: SOURCE
   │  │     .process(bytes → UTF-8 String)       ◄── Step 2: ADAPT
   │  │     .process(new XmlToJsonProcessor())   ◄── Step 3: TRANSFORM
   │  │     .process(new EncryptProcessor())     ◄── Step 4: ENCRYPT
   │  │     .to("kafka:tms.scada.encrypted?...") ◄── Step 5: SINK
   │  │                                                           │  │
   │  │  Body type at each step:                                  │  │
   │  │    after Step 1:  byte[]   (raw Kafka value)              │  │
   │  │    after Step 2:  String   (UTF-8 XML)                    │  │
   │  │    after Step 3:  String   (JSON, parsed by Jackson)      │  │
   │  │    after Step 4:  byte[]   (12B IV ‖ ciphertext ‖ 16B tag)│  │
   │  │    after Step 5:  (forwarded to Kafka, route ends)        │  │
   │  └───────────────────────────────────────────────────────────┘  │
   │                                                                 │
   │  Inside XmlToJsonProcessor.process(Exchange):                   │
   │    • Read body as String                                        │
   │    • Use Jackson XmlMapper to parse XML into a Java tree        │
   │    • Map RCS-specific tags (header, server, version, etc.)      │
   │    • Use Jackson ObjectMapper to serialize tree to JSON String  │
   │    • Set body back on the exchange                              │
   │                                                                 │
   │  Inside EncryptProcessor.process(Exchange):                     │
   │    • Get JSON body as String, encode UTF-8 → byte[]             │
   │    • Generate 12-byte random IV (SecureRandom)                  │
   │    • Init AES/GCM/NoPadding cipher with SCADA_AES_KEY env var   │
   │    • Encrypt: ciphertext + 16-byte GCM auth tag appended        │
   │    • Pack into single byte[]:                                   │
   │         [ IV (12B) | ciphertext | tag (16B) ]                   │
   │    • Optionally base64-wrap (configurable)                      │
   │    • Set body to byte[] on the exchange                         │
   │                                                                 │
   │  Global error handler (KafkaBridgeRoutes.java:80):              │
   │    onException(Exception.class)                                 │
   │       .maximumRedeliveries(3) .redeliveryDelay(2000)            │
   │       .useOriginalMessage()                                     │
   │       .to("activemq:queue:DLQ.kafka-bridge")                    │
   │       .handled(true)                                            │
   │                                                                 │
   │    → After 3 retries with 2s delay, ORIGINAL bytes go to        │
   │      Artemis queue DLQ.kafka-bridge (browse via Artemis console)│
   └─────────────────────────────────────────────────────────────────┘
```

## 1.4 · Inside the sink connector (`tms-rabbitmq-sink`)

**Job:** drain Kafka `tms.scada.encrypted` and publish to RabbitMQ.

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: kafka-connect pod (same as source)                  │
   │  Connector class:                                               │
   │     CamelSpringrabbitmqsinkSinkConnector                        │
   │                                                                 │
   │  ┌───────────────────────────────────────────────────────────┐  │
   │  │  Sink connector lifecycle                                 │  │
   │  │                                                           │  │
   │  │  1. Connect framework subscribes a Kafka consumer to      │  │
   │  │     topic "tms.scada.encrypted" with consumer group       │  │
   │  │     "connect-tms-rabbitmq-sink"                           │  │
   │  │                                                           │  │
   │  │  2. On each poll batch, for every record:                 │  │
   │  │       a. Apply value.converter (StringConverter):         │  │
   │  │            byte[] → String  ⚠ NOTE: encrypted bytes are   │  │
   │  │            interpreted as UTF-8 here. RabbitMQ stores     │  │
   │  │            them as a string; the MQTT plugin re-emits     │  │
   │  │            them as bytes again on subscribe.              │  │
   │  │       b. Camel kamelet spring-rabbitmq-sink publishes:    │  │
   │  │            host:        rabbitmq-internal.scada.svc...    │  │
   │  │            port:        5672 (AMQP)                       │  │
   │  │            exchange:    amq.topic                         │  │
   │  │            routingKey:  tms.scada.pas                     │  │
   │  │       c. After AMQP confirm: commit Kafka offset          │  │
   │  └───────────────────────────────────────────────────────────┘  │
   │                                                                 │
   │  Why amq.topic + tms.scada.pas?                                 │
   │    • amq.topic is a built-in RabbitMQ topic exchange            │
   │    • routingKey "tms.scada.pas" matches MQTT plugin pattern     │
   │      (dots → slashes), so MQTT subscribers to "tms/scada/pas"   │
   │      receive the same payload                                   │
   │                                                                 │
   │  Error handling:                                                │
   │    • RabbitMQ down → retry indefinitely (errors.retry.timeout)  │
   │    • Persistent failure → dlq.connect.tms-rabbitmq-sink         │
   └─────────────────────────────────────────────────────────────────┘
```

## 1.5 · Inside SCADA-API (the consumer)

**Job:** subscribe to MQTT, decrypt, expose decoded messages on the dashboard.

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: scada-api pod (scada namespace)                     │
   │  Process:   Python Flask + paho-mqtt                            │
   │  File:      external-scada/scada-api/app.py                     │
   │                                                                 │
   │  ┌───────────────────────────────────────────────────────────┐  │
   │  │  MQTT subscriber loop                                     │  │
   │  │                                                           │  │
   │  │  client = mqtt.Client(...)                                │  │
   │  │  client.username_pw_set("thiru", "password")              │  │
   │  │  client.connect("rabbitmq-internal.scada", 1883)          │  │
   │  │  client.subscribe("tms/scada/pas")                        │  │
   │  │                                                           │  │
   │  │  on_message(client, userdata, msg):                       │  │
   │  │     1. msg.payload is raw bytes (encrypted)               │  │
   │  │     2. decrypt_payload(msg.payload):                      │  │
   │  │          • auto-detect base64 → b64decode if needed       │  │
   │  │          • slice IV (first 12 bytes)                      │  │
   │  │          • slice tag (last 16 bytes)                      │  │
   │  │          • AES-GCM decrypt with SCADA_AES_KEY             │  │
   │  │          • return JSON string                             │  │
   │  │     3. json.loads(decrypted) → Python dict                │  │
   │  │     4. RECEIVED_BUFFER.append({                           │  │
   │  │           "topic": "tms/scada/pas",                       │  │
   │  │           "decoded": dict,                                │  │
   │  │           "timestamp": now()                              │  │
   │  │        })                                                 │  │
   │  │     5. Increment counter for /api/status                  │  │
   │  └───────────────────────────────────────────────────────────┘  │
   │                                                                 │
   │  HTTP endpoints (Flask):                                        │
   │    GET /api/received  → last 100 decrypted messages             │
   │    GET /api/status    → mqtt_connected, decrypt_fail, counters  │
   │    GET /              → dashboard.html (live SSE updates)       │
   │                                                                 │
   │  Failure modes:                                                 │
   │    • MQTT disconnect    → paho auto-reconnects (set in client)  │
   │    • Decrypt fails      → counter "decrypt_fail" increments,    │
   │                           message dropped (no DLQ on this side) │
   │    • Wrong AES key      → all messages fail; check that         │
   │                           SCADA_AES_KEY matches the bridge's    │
   └─────────────────────────────────────────────────────────────────┘
```

---

# 2 · Reverse path: SCADA → TMS

**Direction:** SCADA-API → MQTT → RabbitMQ → Kafka → Bridge (decrypt + log + state) → Kafka → Artemis → TMS legacy consumers
**Encryption:** Plain JSON by default (no decrypt step needed in standard config)
**Body shape:** JSON throughout

## 2.1 · End-to-end flow

```
   ┌──────────────────────────────────────────────────────────────────┐
   │  SCADA-API pod                                                   │
   │  publishes plain JSON to MQTT topic "scada/tms/alarms"           │
   │  payload: {"CreatorId":"ScateX","Type":"UpdateAlarm",...}        │
   └────────┬─────────────────────────────────────────────────────────┘
            │ MQTT (port 1883)
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  RABBITMQ                                                        │
   │   MQTT plugin maps:                                              │
   │     "scada/tms/alarms" → routingKey "scada.tms.alarms"           │
   │     bound to queue "scada.tms.alarms.queue"                      │
   └────────┬─────────────────────────────────────────────────────────┘
            │ AMQP
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  KAFKA CONNECT  ─  scada-rabbitmq-source connector               │
   │  (Section 2.2 — internals)                                       │
   └────────┬─────────────────────────────────────────────────────────┘
            │ byte[] (raw JSON)
            ▼
   ┌──────────────────────────┐
   │ Kafka topic: scada.tms.raw│
   └────────┬─────────────────┘
            │ consumed by:
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  BRIDGE pod  ─  Camel route reverse-kafka-scada-tms-raw          │
   │  (Section 2.3 — internals)                                       │
   │                                                                  │
   │  bytes → (optional decrypt) → log RSAE type → fan-out alarm      │
   │  state → (optional JSON→XML) → bytes                             │
   └────────┬─────────────────────────────────────────────────────────┘
            │ byte[]
            ▼
   ┌─────────────────────────────────────┐
   │ Kafka topic: scada.tms.processed    │
   └────────┬────────────────────────────┘
            │ consumed by:
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  KAFKA CONNECT  ─  scada-artemis-sink connector                  │
   │  (Section 2.4 — internals)                                       │
   └────────┬─────────────────────────────────────────────────────────┘
            │ JMS publish to topic SCADA.TMS.Alarms
            ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  ARTEMIS                                                         │
   │   topic SCADA.TMS.Alarms                                         │
   │   TMS legacy consumers (subscribed to this topic) get the alarm  │
   └──────────────────────────────────────────────────────────────────┘
```

## 2.2 · Inside the source connector (`scada-rabbitmq-source`)

**Job:** drain RabbitMQ queue `scada.tms.alarms.queue` into Kafka `scada.tms.raw`.

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: kafka-connect pod                                   │
   │  Connector class:                                               │
   │     CamelSpringrabbitmqsourceSourceConnector                    │
   │                                                                 │
   │  ┌───────────────────────────────────────────────────────────┐  │
   │  │  Source connector lifecycle                               │  │
   │  │                                                           │  │
   │  │  1. Camel kamelet spring-rabbitmq-source connects to:     │  │
   │  │       host:     rabbitmq-internal.scada.svc...            │  │
   │  │       port:     5672 (AMQP)                               │  │
   │  │       exchange: amq.topic                                 │  │
   │  │       queues:   scada.tms.alarms.queue                    │  │
   │  │       binding key: scada.tms.alarms                       │  │
   │  │                                                           │  │
   │  │  2. NOTE: autoDeclare=false                               │  │
   │  │     → queue + binding must be pre-declared (Step 5 of     │  │
   │  │       FRESH-PC-SETUP creates them via rabbitmqadmin)      │  │
   │  │                                                           │  │
   │  │  3. AMQP basic.consume on the queue                       │  │
   │  │                                                           │  │
   │  │  4. For every delivered message:                          │  │
   │  │       a. Body comes as byte[] (MQTT plugin emits bytes)   │  │
   │  │       b. value.converter = ByteArrayConverter             │  │
   │  │            ⚠ NOT StringConverter — see why below          │  │
   │  │       c. SourceRecord:                                    │  │
   │  │            key=null,                                      │  │
   │  │            value=byte[] (preserves exact bytes)           │  │
   │  │            topic="scada.tms.raw"                          │  │
   │  │       d. After Kafka ACK: AMQP basic.ack                  │  │
   │  └───────────────────────────────────────────────────────────┘  │
   │                                                                 │
   │  Why ByteArrayConverter (not StringConverter)?                  │
   │    StringConverter would call new String(bytes, UTF-8). If the  │
   │    payload were ever encrypted (raw bytes, not valid UTF-8),    │
   │    this would corrupt it (replace invalid bytes with U+FFFD).   │
   │    ByteArrayConverter preserves the original bytes byte-perfect.│
   │    Currently SCADA→TMS is plain JSON (UTF-8 safe), but the      │
   │    converter is set up for forward-compat with encryption.      │
   │                                                                 │
   │  Earlier bug (fixed): if StringConverter was left here,         │
   │  downstream consumers saw values like "[B@1a2b3c4" — Java's     │
   │  default Object.toString() of a byte array reference. That's    │
   │  the smoking gun for "ByteArrayConverter not configured".       │
   │                                                                 │
   │  Error handling:                                                │
   │    • Persistent failure → dlq.connect.scada-rabbitmq-source     │
   └─────────────────────────────────────────────────────────────────┘
```

## 2.3 · Inside the Camel reverse route

**Job:** consume `scada.tms.raw`, log the RSAE message type, fan out alarm
state to the compacted topic, optionally convert to XML, write to
`scada.tms.processed`.

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: pas-scada-bridge pod                                │
   │  Defined in: tms/.../routes/KafkaBridgeRoutes.java:255          │
   │  Activated when: bridge.reverse-kafka.enabled=true              │
   │                                                                 │
   │  ┌───────────────────────────────────────────────────────────┐  │
   │  │  Camel route: reverse-kafka-scada-tms-raw                 │  │
   │  │                                                           │  │
   │  │   from("kafka:scada.tms.raw?...")                         │  │
   │  │     .process(decrypt OR utf8-decode)    ◄── Step 1        │  │
   │  │     .process(new ScadaInboundProcessor) ◄── Step 2: LOG   │  │
   │  │     .process(new AlarmStateFanout...)   ◄── Step 3: FANOUT
   │  │     .process(JsonToXmlProcessor)?       ◄── Step 4 (opt)  │  │
   │  │     .to("kafka:scada.tms.processed")    ◄── Step 5        │  │
   │  │                                                           │  │
   │  │  Body type at each step:                                  │  │
   │  │    after Step 0:  byte[]   (from Kafka)                   │  │
   │  │    after Step 1:  String   (JSON)                         │  │
   │  │    after Step 2:  String   (unchanged, just logs)         │  │
   │  │    after Step 3:  String   (unchanged, fans out a copy)   │  │
   │  │    after Step 4:  String   (XML if convert-to-xml=true)   │  │
   │  │    after Step 5:  (sent to Kafka, route ends)             │  │
   │  └───────────────────────────────────────────────────────────┘  │
   │                                                                 │
   │  Step 1 — decrypt or just decode:                               │
   │     if (bridge.reverse-kafka.encrypt-enabled) {                 │
   │       byte[] payload = body.getBytes()                          │
   │       body = DecryptExample.decrypt(payload)                    │
   │     } else {                                                    │
   │       body = new String(bytes, UTF_8)                           │
   │     }                                                           │
   │     → Default is encrypt-enabled=false (SCADA sends plain).     │
   │                                                                 │
   │  Step 2 — ScadaInboundProcessor:                                │
   │     Parse JSON, extract "Type" field (UpdateAlarm/KeepAlive/    │
   │     SendAllAlarms/GetAllAlarms), log it for traceability.       │
   │     Body unchanged.                                             │
   │                                                                 │
   │  Step 3 — AlarmStateFanoutProcessor:                            │
   │     • Skipped when bridge.alarm-state.enabled=false             │
   │     • If enabled: parse JSON, get alarm Id, send a SEPARATE     │
   │       Kafka record to topic scada.tms.alarms.state              │
   │       with key=alarmId, value=current state.                    │
   │     • Topic is log-compacted — Kafka keeps only the latest      │
   │       value per key, used for SCADA reconnect snapshots         │
   │       (/api/snapshot/replay endpoint).                          │
   │     • Original message continues unchanged through the route.   │
   │                                                                 │
   │  Step 4 — JsonToXmlProcessor (optional):                        │
   │     If bridge.reverse-kafka.convert-to-xml=true, serialize JSON │
   │     to XML for legacy TMS XML consumers. Default false.         │
   │                                                                 │
   │  Step 5 — write byte[] to Kafka:                                │
   │     Always emit byte[] regardless of upstream String/XML.       │
   └─────────────────────────────────────────────────────────────────┘
```

## 2.4 · Inside the sink connector (`scada-artemis-sink`)

**Job:** drain Kafka `scada.tms.processed` and publish to Artemis topic
`SCADA.TMS.Alarms`.

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: kafka-connect pod                                   │
   │  Connector class:                                               │
   │     CamelJmspooledapacheartemissinkSinkConnector                │
   │                                                                 │
   │  ┌───────────────────────────────────────────────────────────┐  │
   │  │  Sink connector lifecycle                                 │  │
   │  │                                                           │  │
   │  │  1. Kafka consumer subscribes to "scada.tms.processed"    │  │
   │  │     with consumer group "connect-scada-artemis-sink"      │  │
   │  │                                                           │  │
   │  │  2. For each polled record:                               │  │
   │  │       a. value.converter = StringConverter                │  │
   │  │            byte[] → String                                │  │
   │  │       b. Camel kamelet jms-pooled-apache-artemis-sink     │  │
   │  │            broker:    tcp://host.minikube.internal:61616  │  │
   │  │            topic:     SCADA.TMS.Alarms                    │  │
   │  │            JMS publish as TextMessage                     │  │
   │  │       c. After JMS commit: commit Kafka offset            │  │
   │  └───────────────────────────────────────────────────────────┘  │
   │                                                                 │
   │  Result: TMS legacy consumers subscribed to SCADA.TMS.Alarms    │
   │  on the existing Artemis broker see new alarms appearing —      │
   │  zero changes needed on their side.                             │
   │                                                                 │
   │  Error handling:                                                │
   │    • Artemis down → retry up to errors.retry.timeout (10 min)   │
   │    • Persistent failure → dlq.connect.scada-artemis-sink        │
   └─────────────────────────────────────────────────────────────────┘
```

---

# 3 · Dashboard / monitor

Three dashboards, three different jobs:

## 3.1 · Monitor (http://localhost:8080) — health probes

**What it watches:** 19 components across both namespaces.
**File:** `monitor/monitor.py` + `monitor/config.yaml`

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: pas-scada-monitor pod                               │
   │  Process:   FastAPI + asyncio                                   │
   │                                                                 │
   │  ┌───────────────────────────────────────────────────────────┐  │
   │  │  Probe loop (every 15 seconds, configurable)              │  │
   │  │                                                           │  │
   │  │  For each of 19 probes defined in config.yaml:            │  │
   │  │     1. HTTP GET, TCP connect, or kubectl exec check       │  │
   │  │     2. If 2 consecutive failures → state = DOWN           │  │
   │  │     3. If 2 consecutive successes after DOWN → UP         │  │
   │  │     4. Push state into in-memory dict                     │  │
   │  │     5. SSE stream (/state) pushes updates to browser      │  │
   │  │                                                           │  │
   │  │  Probes cover:                                            │  │
   │  │     • bridge /actuator/health                             │  │
   │  │     • each of 7 Kafka Connect connectors via REST status  │  │
   │  │     • Kafka, Zookeeper, Kafdrop liveness                  │  │
   │  │     • RabbitMQ management API                             │  │
   │  │     • scada-api /api/status                               │  │
   │  │     • scada-api MQTT-link (boolean from /api/status)      │  │
   │  │     • Artemis console (best-effort)                       │  │
   │  │     • demo / monitor self-checks                          │  │
   │  └───────────────────────────────────────────────────────────┘  │
   │                                                                 │
   │  Frontend (single HTML file):                                   │
   │    • Tile per probe; green=UP, red=DOWN, grey=unknown           │
   │    • Audible alarm: 880→740→600 Hz tone every 4s while any DOWN │
   │    • "Sound off" toggle (browser autoplay rule — click once)    │
   │    • Tab title becomes "(N DOWN) PAS-SCADA" so it's visible     │
   │      from a different window                                    │
   └─────────────────────────────────────────────────────────────────┘
```

**What it does NOT watch:** message content. It only checks that
endpoints respond. Use Demo (8090) or Kafdrop (9000) for content.

## 3.2 · Demo (http://localhost:8090) — live data table + flow

**What it shows:** decoded TMS messages and SCADA alarms in real time.
**File:** `demo/app.py`

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: pas-scada-demo pod                                  │
   │                                                                 │
   │  Two views:                                                     │
   │    /        → table of last N messages from both directions     │
   │    /flow    → animated flow diagram with arrows pulsing on each │
   │               message (uses SSE)                                │
   │                                                                 │
   │  Data source:                                                   │
   │    • subscribes to MQTT (same as scada-api) for forward msgs    │
   │    • subscribes to scada.tms.alarms via RabbitMQ for reverse    │
   │    • decrypts on the forward side                               │
   │    • exposes SSE stream /api/stream                             │
   └─────────────────────────────────────────────────────────────────┘
```

## 3.3 · SCADA simulator (http://localhost:8091) — drive + observe

This is the dashboard you've been using. **It both publishes AND subscribes:**

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Container: scada-api pod (scada namespace)                     │
   │                                                                 │
   │  Auto-publishes (timers in dashboard):                          │
   │    • UpdateAlarm    every 10s   → MQTT scada/tms/alarms         │
   │    • KeepAlive      every 30s   → MQTT scada/tms/alarms         │
   │    • SendAllAlarms  every 60s   → MQTT scada/tms/alarms         │
   │    • GetAllAlarms   every 120s  → MQTT scada/tms/alarms         │
   │                                                                 │
   │  Subscribes:                                                    │
   │    • MQTT topic tms/scada/pas (forward path output)             │
   │    • Decrypts payload, shows in TMS→SCADA pane                  │
   │                                                                 │
   │  Manual publish:                                                │
   │    • TMS → row publishes XML to Artemis topic (TMS.PISInfo etc.)│
   │    • Drives the forward path manually for testing               │
   │                                                                 │
   │  /api/received   — last 100 forward messages (decrypted JSON)   │
   │  /api/status     — mqtt_connected, decrypt_fail, counters       │
   └─────────────────────────────────────────────────────────────────┘
```

---

# 4 · DLQ — preventing data loss

The system has **two independent DLQ mechanisms** because Camel and
Kafka Connect are separate processes with separate failure modes.

## 4.1 · The two DLQs

```
   ┌────────────────────────────────────────────┬─────────────────┐
   │  Failure inside the BRIDGE (Camel)         │  Goes to:       │
   │  e.g. XmlToJson parse error,               │  Artemis queue  │
   │       AES decrypt failure,                 │  DLQ.kafka-     │
   │       Kafka write timeout                  │     bridge      │
   ├────────────────────────────────────────────┼─────────────────┤
   │  Failure inside a CONNECTOR (Connect)      │  Goes to:       │
   │  e.g. Artemis broker unreachable,          │  Kafka topic    │
   │       RabbitMQ auth failure,               │  dlq.connect.   │
   │       payload schema mismatch              │     <name>      │
   └────────────────────────────────────────────┴─────────────────┘
```

## 4.2 · Camel DLQ — in Artemis

Configured in `KafkaBridgeRoutes.java:80`:

```java
   onException(Exception.class)
       .maximumRedeliveries(3)            // try up to 4 total times
       .redeliveryDelay(2000)             // 2s between attempts
       .useOriginalMessage()              // preserve original bytes
       .to("activemq:queue:DLQ.kafka-bridge")
       .handled(true);                    // don't bubble up
```

**How to inspect:**

1. Open http://localhost:8161/console → admin/admin
2. addresses → `DLQ.kafka-bridge` → queues → first queue → **Browse**
3. Look at message body + JMS headers (CamelExchangeException etc.)

**What lands here:** any exception thrown inside any Camel route
processor (XmlToJson, Encrypt, Decrypt, etc.) after 3 retries.

## 4.3 · Connect DLQs — Kafka topics

Each connector has its own DLQ topic with a 14-day retention:

| Connector | DLQ topic |
|---|---|
| `tms-artemis-source` (×4) | `dlq.connect.tms-artemis-source` |
| `tms-rabbitmq-sink` | `dlq.connect.tms-rabbitmq-sink` |
| `scada-rabbitmq-source` | `dlq.connect.scada-rabbitmq-source` |
| `scada-artemis-sink` | `dlq.connect.scada-artemis-sink` |

Configured per-connector via:
```json
"errors.tolerance":          "all",
"errors.retry.timeout":      "600000",     // 10 min retry window
"errors.retry.delay.max.ms": "30000",      // 30s max backoff
"errors.deadletterqueue.topic.name":         "dlq.connect.<name>",
"errors.deadletterqueue.context.headers.enable": "true"
```

**Behavior:**
1. Transient failure → retry with exponential backoff up to 10 minutes
2. Still failing after 10 min → send original record to DLQ topic, ACK upstream
3. Connector stays `RUNNING` so other messages keep flowing

**Headers attached to a DLQ record (`__connect.errors.*`):**
- `__connect.errors.exception.class.name` — what failed
- `__connect.errors.exception.message` — error string
- `__connect.errors.exception.stacktrace` — full Java stack
- `__connect.errors.topic` — original topic
- `__connect.errors.partition` — original partition
- `__connect.errors.offset` — original offset

## 4.4 · Inspect DLQ topics

### Quick — count messages in each DLQ
```bash
kubectl -n pinkline exec deploy/kafka -- bash -c "
  for t in dlq.connect.tms-artemis-source dlq.connect.tms-rabbitmq-sink \
           dlq.connect.scada-rabbitmq-source dlq.connect.scada-artemis-sink; do
    echo -n \"\$t: \"
    kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list kafka-service:9092 --topic \$t 2>/dev/null \
      | awk -F: '{s+=\$3} END{print s+0}'
  done"
# Expect: all four = 0 in a healthy system
```

### Or use Kafdrop
http://localhost:9000 → look for `dlq.connect.*` topics. Non-zero
message count = something failed.

### Read what's in a specific DLQ
```bash
kubectl -n pinkline exec deploy/kafka -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic dlq.connect.tms-artemis-source \
  --from-beginning --max-messages 5 \
  --property print.headers=true --property print.value=true \
  --timeout-ms 5000
```

The headers tell you why it failed; the value is the original bytes.

## 4.5 · Replay DLQ messages back into the pipeline

There's no auto-replay. Manual procedure once you've fixed the root cause:

```bash
# 1. Dump DLQ to a file inside the kafka pod
kubectl -n pinkline exec deploy/kafka -- bash -c "
  kafka-console-consumer --bootstrap-server kafka-service:9092 \
    --topic dlq.connect.tms-artemis-source \
    --from-beginning --timeout-ms 5000 > /tmp/replay.txt"

# 2. Pipe back into the original topic
kubectl -n pinkline exec deploy/kafka -- bash -c "
  cat /tmp/replay.txt | kafka-console-producer \
    --bootstrap-server kafka-service:9092 --topic tms.raw"

# 3. Once confirmed processed, purge the DLQ
kubectl -n pinkline exec deploy/kafka -- kafka-topics \
  --bootstrap-server kafka-service:9092 --delete \
  --topic dlq.connect.tms-artemis-source
# (auto-recreates next time a message fails)
```

## 4.6 · Restart a FAILED connector

If an entire connector (not just individual records) is in `FAILED` state:

```bash
# Check status
curl -s localhost:8083/connectors/<name>/status | jq

# Restart it (no rebuild needed)
curl -X POST localhost:8083/connectors/<name>/restart

# Verify
curl -s localhost:8083/connectors/<name>/status | jq
# expect: connector.state=RUNNING, all tasks RUNNING
```

## 4.7 · Common DLQ root causes

| Cause | Where it shows up | Fix |
|---|---|---|
| Schema/converter mismatch (e.g. `StringConverter` reading byte[]) | Connect DLQ — exception "InvalidUTF8" or "[B@hash" downstream | Set `value.converter=ByteArrayConverter` in configmap, reapply, restart connector |
| AES key mismatch (bridge vs SCADA-API) | scada-api `decrypt_fail` counter; not a DLQ | Confirm `SCADA_AES_KEY` k8s Secret is identical on both sides |
| Encrypted payload but bridge has decrypt disabled (or vice versa) | Camel DLQ — body looks like binary garbage | Toggle `BRIDGE_REVERSE_KAFKA_ENCRYPT_ENABLED` to match the publisher |
| RabbitMQ unreachable for >10 min | Connect DLQ — exception "ConnectException" | Restore connectivity, then **replay** (4.5) |
| Artemis unreachable for >10 min | Connect DLQ + Camel DLQ both | Restore Artemis (`docker start artemis`), restart connectors, replay |
| Malformed payload from upstream | Connect DLQ on the source side | Inspect, fix upstream, then replay or accept the loss |

## 4.8 · No data loss happens here

```
   Kafka topics — message stays for retention period (≥7 days for
                  data topics, 14 days for DLQ topics) regardless
                  of consumer state. Replayable.

   Artemis      — broker holds messages until ACKed. Connect ACKs
                  only after Kafka write succeeds.

   RabbitMQ     — durable queues; messages stay until ACKed by
                  Connect or scada-api.

   Camel route  — at-least-once via Kafka commit-on-success and
                  Artemis transactional consume.

   Connect      — at-least-once via offset commit on success.
                  errors.tolerance=all + DLQ means a single bad
                  message never blocks others.
```

The only place messages are lost is **scada-api decryption failure**
(the SCADA simulator drops the message and increments `decrypt_fail`).
Everything else either retries forever or routes to a DLQ where it
can be inspected and replayed.

---

# 5 · The two operating modes

The bridge supports two completely different topologies, switched by env vars
on the `pas-scada-bridge` deployment:

| Env var | Mode A (default) | Mode B (Connect-mode) |
|---|---|---|
| `BRIDGE_INPUT_FROM_KAFKA` | `false` (or unset) | `true` |
| `BRIDGE_REVERSE_KAFKA_ENABLED` | `false` (or unset) | `true` |

Sections 1 and 2 of this document describe **Mode B**, because that's
where Connect connectors are interesting. Mode A is described in Section 6.

## 5.1 · Mode A vs Mode B side-by-side

```
   ─────────  MODE A (Artemis-direct, default)  ─────────

   Artemis ─[Camel JMS]→ Bridge ─[Camel pipeline]→ Kafka       (audit only)
                            │                       │
                            ├─[Camel rabbitmq]──→ RabbitMQ ─→ MQTT plugin ─→ SCADA
                            └─[Camel paho]─────→ MQTT broker (optional direct)

   SCADA ─[MQTT]→ RabbitMQ queue ─[Camel]→ Bridge ─[Camel JMS]→ Artemis ─→ TMS

   Pros:  one app does everything; simpler ops; pipeline reorderable via config
   Cons:  if the bridge crashes, Artemis backs up; no Kafka buffer in the middle


   ─────────  MODE B (Connect-mode)  ─────────

   Artemis ─[Connect tms-artemis-source]→ Kafka tms.raw
                                              │
                                          [Camel route]→ Kafka tms.scada.encrypted
                                                              │
                                                  [Connect tms-rabbitmq-sink]
                                                              │
                                                              ▼
                                                          RabbitMQ → MQTT → SCADA

   SCADA → MQTT → RabbitMQ → [Connect scada-rabbitmq-source] → Kafka scada.tms.raw
                                                                   │
                                                            [Camel reverse route]
                                                                   │
                                                                   ▼
                                                          Kafka scada.tms.processed
                                                                   │
                                                  [Connect scada-artemis-sink]
                                                                   │
                                                                   ▼
                                                          Artemis SCADA.TMS.Alarms → TMS

   Pros:  Connect buffers across crashes; Kafka holds full history; bridge can be
          restarted without losing any messages
   Cons:  more moving parts; 5 connectors to monitor; longer end-to-end latency
```

## 5.2 · How to tell which mode is running

```bash
# Inspect bridge env vars
kubectl -n pinkline exec deploy/pas-scada-bridge -- env | grep -E "BRIDGE_INPUT|BRIDGE_REVERSE"

# Or look at the bridge logs at startup — Mode B announces itself:
kubectl -n pinkline logs deploy/pas-scada-bridge | grep -E "input-kafka.enabled|Artemis-direct"
# Mode A: ... bridge.input-kafka.enabled=false — using Artemis-direct routes
# Mode B: ... bridge.input-kafka.enabled=true  — Artemis-direct routes SKIPPED, consuming from Kafka topic [tms.raw]
```

---

# 6 · Mode A — Artemis-direct flows in detail

When `BRIDGE_INPUT_FROM_KAFKA=false`, the forward and reverse paths look
very different from Sections 1–2.

## 6.1 · Mode A forward (Artemis → bridge → all sinks)

```
   ┌──────────────┐    XML over JMS     ┌──────────────────┐
   │ TMS publisher│ ──────────────────► │  ARTEMIS         │
   └──────────────┘   (port 61616)      │   TMS.PISInfo    │
                                        │   TSInfo         │
                                        │   RCS.E2K.TMS.   │
                                        │     RouteInfo    │
                                        │   RCS.E2K.TMS.   │
                                        │     TrafficReport│
                                        └────────┬─────────┘
                                                 │
                                                 │ JMS subscribers (one per topic)
                                                 ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  BRIDGE  —  Camel routes (one per Artemis topic)                │
   │  Defined in: KafkaBridgeRoutes.java:104                         │
   │                                                                 │
   │  for (String topic : config.getArtemisTopics().split(",")) {    │
   │      from("activemq:topic:" + topic)                            │
   │        .process(new XmlToJsonProcessor())                       │
   │        .process(config.getEncrypt().isEnabled()                 │
   │                  ? new EncryptProcessor()                       │
   │                  : utf8-encode passthrough)                     │
   │        // pipeline=kafka,rabbitmq,mqtt drives the .to() chain   │
   │        .to("kafka:tms.scada.encrypted")                         │
   │        .to("spring-rabbitmq:amq.topic?routingKey=tms.scada.pas")│
   │        .to("paho:tms/scada/pas?brokerUrl=...")                  │
   │  }                                                              │
   │                                                                 │
   │  4 routes total — outbound-tms-pisinfo, outbound-tsinfo,        │
   │  outbound-rcs-e2k-tms-routeinfo,                                │
   │  outbound-rcs-e2k-tms-trafficreportclient                       │
   └─────────────────────────────────────────────────────────────────┘
```

Notice: **Kafka is just one of three sinks here**, not the central buffer.
If `bridge.pipeline=mqtt` only, Kafka is skipped entirely — the message
goes Artemis → bridge → MQTT → SCADA, no Kafka involvement.

## 6.2 · Mode A reverse (RabbitMQ → bridge → Artemis)

When `BRIDGE_REVERSE_KAFKA_ENABLED=false`, the reverse path skips Kafka:

```
   SCADA → MQTT scada/tms/alarms → RabbitMQ
                                       │
                                       │ AMQP basic.consume
                                       ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  BRIDGE  —  Inbound Camel route                                 │
   │  Defined in: KafkaBridgeRoutes.java:350                         │
   │                                                                 │
   │  for (InboundRoute inb : config.getInbound()) {                 │
   │      from("spring-rabbitmq:amq.topic"                           │
   │           + "?queues=scada.tms.alarms.queue"                    │
   │           + "&routingKey=scada.tms.alarms")                     │
   │        .process(new ScadaInboundProcessor())   // log RSAE type │
   │        .process(inb.isConvertToXml() ? JsonToXml : noop)        │
   │        .to("activemq:topic:SCADA.TMS.Alarms");                  │
   │  }                                                              │
   │                                                                 │
   │  Configured by application.properties:102-106:                  │
   │    bridge.inbound[0].from-exchange=amq.topic                    │
   │    bridge.inbound[0].routing-key=scada.tms.alarms               │
   │    bridge.inbound[0].queue=scada.tms.alarms.queue               │
   │    bridge.inbound[0].to-topic=SCADA.TMS.Alarms                  │
   │    bridge.inbound[0].convert-to-xml=false                       │
   │                                                                 │
   │  Skipped automatically when bridge.reverse-kafka.enabled=true,  │
   │  to prevent two consumers competing on the same RabbitMQ queue. │
   └─────────────────────────────────────────────────────────────────┘
                                       │
                                       │ JMS publish
                                       ▼
                                 ARTEMIS SCADA.TMS.Alarms → TMS
```

## 6.3 · Why both modes exist

This project went through a Phase 1→Phase 5 migration. Mode A is the
**legacy** path (Camel does everything, what the original project looked
like). Mode B was layered on so Connect could become the broker boundary
without rewriting the bridge. Both still work; toggle with env vars.

For minikube dev: Mode A is the default and easier to reason about.
For production with multi-VM (TMS + SCADA on separate hosts): Mode B
because Connect handles cross-VM transport better than Camel JMS over WAN.

---

# 7 · Bootstrap

Two one-shot Kubernetes Jobs run **before** the connectors register, to
prepare Kafka and RabbitMQ. They're idempotent — safe to re-apply.

## 7.1 · Kafka topics (`bootstrap/k8s/10-kafka-topics-job.yaml`)

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Job: bootstrap-kafka-topics                                    │
   │  Image: confluentinc/cp-kafka:7.5.0                             │
   │                                                                 │
   │  Reads spec from inline ConfigMap kafka-topics-spec:            │
   │    tms.raw                            3  1  604800000   delete  │
   │    tms.scada.encrypted                3  1  604800000   delete  │
   │    scada.tms.raw                      3  1  604800000   delete  │
   │    scada.tms.processed                3  1  604800000   delete  │
   │    scada.tms.alarms.state             3  1  604800000   compact │
   │    dlq.connect.tms-artemis-source     1  1  1209600000  delete  │
   │    dlq.connect.tms-rabbitmq-sink      1  1  1209600000  delete  │
   │    dlq.connect.scada-rabbitmq-source  1  1  1209600000  delete  │
   │    dlq.connect.scada-artemis-sink     1  1  1209600000  delete  │
   │                                                                 │
   │  Columns: name  partitions  replication  retention.ms  policy   │
   │                                                                 │
   │  For each line, runs:                                           │
   │    kafka-topics --bootstrap-server kafka-service:9092 \         │
   │       --create --if-not-exists --topic <name>                   │
   │       --partitions <n> --replication-factor <n>                 │
   │       --config retention.ms=<ms>                                │
   │       --config cleanup.policy=<delete|compact>                  │
   │                                                                 │
   │  Replication factor = 1 (single-broker dev cluster).            │
   │  Bump to 3 once Kafka has ≥3 brokers in production.             │
   │                                                                 │
   │  Compacted topic (alarms.state) keeps only the latest record    │
   │  per key forever — used for SCADA snapshot replay (Section 10). │
   └─────────────────────────────────────────────────────────────────┘
```

**Retention rationale:**
- Pipeline data topics: 7 days — enough for Connect to recover from a
  multi-day outage without losing messages.
- DLQ topics: 14 days — gives operators time to inspect + replay before
  the bad records age out.
- Compacted topic: ignores retention; keeps latest record per key forever.

## 7.2 · RabbitMQ queue (`bootstrap/k8s/20-rabbitmq-queue-job.yaml`)

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Job: bootstrap-rabbitmq-queue                                  │
   │  Image: curlimages/curl:8.10.1                                  │
   │                                                                 │
   │  Calls the RabbitMQ management API:                             │
   │    1. PUT /api/queues/<vhost>/scada.tms.alarms.queue            │
   │         {"durable": true, "auto_delete": false}                 │
   │                                                                 │
   │    2. POST /api/bindings/<vhost>/e/amq.topic/q/<queue>          │
   │         {"routing_key": "scada.tms.alarms"}                     │
   │                                                                 │
   │  Why these two specifically?                                    │
   │    • amq.topic is the built-in topic exchange RabbitMQ ships    │
   │      with. We don't declare it; we just bind to it.             │
   │    • The MQTT plugin re-publishes any "scada/tms/alarms" MQTT   │
   │      message to amq.topic with routingKey "scada.tms.alarms".   │
   │    • Without this binding+queue, scada-rabbitmq-source has      │
   │      nothing to consume from.                                   │
   │                                                                 │
   │  Both calls are HTTP-idempotent (204 if already exists), so     │
   │  re-running the Job is safe.                                    │
   └─────────────────────────────────────────────────────────────────┘
```

## 7.3 · Order of bootstrap operations

```
   1. minikube starts                       (FRESH-PC-SETUP Step 4 / start.sh)
   2. Namespaces + Zookeeper + Kafka apply  → kafka pod becomes ready (~60s)
   3. bootstrap-kafka-topics Job runs       → creates 9 topics
   4. RabbitMQ pod ready                    (~30s)
   5. bootstrap-rabbitmq-queue Job runs     → declares queue + binding
   6. Connect pod ready                     (~60s)
   7. register-connectors Job runs          → POSTs 7 configs to Connect REST
   8. Bridge starts                         (~3 min, Spring Boot is slow)
   9. Demo + Monitor pods start             (~30s each)
```

If you skip step 3 or 5, the connectors will register but immediately
go FAILED — `tms-artemis-source` can't write to a non-existent Kafka
topic, `scada-rabbitmq-source` can't consume from a non-existent queue.

---

# 8 · Encryption deep-dive

Forward path uses **AES-256-GCM**. Here's exactly what happens:

## 8.1 · Key provenance

```
   Source of truth:        k8s Secret "scada-aes-key" in pinkline namespace
                              data.SCADA_AES_KEY = base64-encoded 32-byte key

   Bridge (Java):          tms/k8s/secret.yaml mounts SCADA_AES_KEY env var
                           EncryptProcessor reads System.getenv("SCADA_AES_KEY")
                           Decodes base64 → 32 bytes for AES-256

   SCADA-API (Python):     external-scada/k8s/60-scada-api-secret.yaml mounts
                           same key. app.py reads os.environ["SCADA_AES_KEY"],
                           base64-decodes, uses with cryptography.AESGCM

   ⚠ If the bridge and scada-api Secrets diverge, scada-api's decrypt_fail
     counter increments and forward messages silently disappear from the
     dashboard. Always update both Secrets together.
```

## 8.2 · Encryption format on the wire

A single encrypted payload looks like this:

```
   ┌──────────────┬──────────────────────────────┬──────────────┐
   │   IV (12 B)  │       ciphertext (var)       │  GCM tag     │
   │              │                              │   (16 B)     │
   └──────────────┴──────────────────────────────┴──────────────┘
       random        AES-256-GCM(plaintext_JSON)    auth tag

   Total length = 12 + plaintext_length + 16 bytes
```

Sometimes wrapped in base64 (configurable). SCADA-API auto-detects:
```python
def decrypt_payload(raw):
    # auto-detect: if it looks like base64, decode it
    if all(c in B64_CHARS for c in raw[:32]):
        raw = base64.b64decode(raw)
    iv         = raw[:12]
    tag        = raw[-16:]
    ciphertext = raw[12:-16]
    return aesgcm.decrypt(iv, ciphertext + tag, associated_data=None)
```

## 8.3 · Why GCM (not CBC, not ECB)?

| Mode | Authenticated? | IV unique needed? | Use here? |
|---|---|---|---|
| ECB | ❌ | ❌ | Never — same plaintext = same ciphertext |
| CBC | ❌ | Yes (random) | Possible but unauthenticated; needs HMAC bolted on |
| **GCM** | ✅ (built-in tag) | Yes (random or counter) | **Used** — single primitive, AEAD |

GCM gives integrity + authenticity for free. If anyone tampers with the
ciphertext, decrypt fails (`InvalidTag` exception). No need for a separate
HMAC step.

## 8.4 · IV uniqueness

Each `EncryptProcessor.process()` call generates a fresh 12-byte IV via
`SecureRandom.nextBytes()`. **Reusing an IV with the same key on different
plaintexts breaks GCM catastrophically** — but `SecureRandom` makes
collision probability negligible (~1 in 2^48 after 2^48 messages).

## 8.5 · Reverse path is plain JSON

`bridge.encrypt.enabled=true` ONLY applies to forward (TMS → SCADA).
The reverse path (SCADA → TMS) uses `bridge.reverse-kafka.encrypt-enabled`
which defaults to `false`. SCADA publishes plain JSON.

Why the asymmetry?
- TMS data is internal/sensitive — encrypted as it crosses the SCADA boundary
- SCADA alarms are operational signals — already on the SCADA-trusted side
- Simplifies SCADA-side clients (no encryption library needed)

To enable reverse encryption: set `BRIDGE_REVERSE_KAFKA_ENCRYPT_ENABLED=true`
**and** make scada-api encrypt before publishing. Both must agree.

---

# 9 · Bridge pipeline configuration

The forward path's behavior in Mode A is driven by ONE config line:

```properties
bridge.pipeline=${BRIDGE_PIPELINE:kafka,rabbitmq,mqtt}
```

## 9.1 · How the Camel route is built

`KafkaBridgeRoutes.java:125` walks the comma-separated list and calls
`.to()` for each step **in listed order**:

```java
   for (String step : config.getPipeline().split(",")) {
       switch (step.trim().toLowerCase()) {
           case "kafka"    -> route.to("kafka:" + config.getKafka().getTopic() + "?...");
           case "rabbitmq" -> route.to("spring-rabbitmq:" + rmq.getExchange() + "?routingKey=" + rmq.getRoutingKey());
           case "mqtt"     -> route.to("paho:" + mqtt.getTopic() + "?brokerUrl=" + mqtt.getBrokerUrl() + "...");
       }
   }
```

So `pipeline=kafka,rabbitmq,mqtt` (default) means each Artemis message
is delivered to **all three** sinks, in order. Order matters: Kafka
gets it first, then RabbitMQ, then MQTT.

## 9.2 · Common pipeline variants

| Setting | Behavior |
|---|---|
| `kafka,rabbitmq,mqtt` (default) | Full chain: persisted in Kafka, routed via RabbitMQ, also direct MQTT |
| `mqtt` | Direct only — Artemis → bridge → MQTT, no Kafka, no RabbitMQ |
| `kafka,mqtt` | Kafka audit + direct MQTT — skip RabbitMQ |
| `kafka,rabbitmq` | Kafka + RabbitMQ — no direct MQTT (rely on RabbitMQ MQTT plugin) |
| `kafka` | Kafka audit only — useful for debugging upstream |

## 9.3 · Why three parallel sinks (not one)?

Historically the bridge wrote to all three so different consumers could
choose their preferred broker:
- Kafka consumers (Connect, Streams) → audit, replay, transformations
- RabbitMQ consumers → AMQP-native apps
- MQTT consumers → IoT-style devices

In this project, **only the RabbitMQ MQTT plugin path actually feeds SCADA**.
The direct `paho:mqtt` step writes to the same MQTT broker (RabbitMQ's plugin)
on the same topic, which means **SCADA receives every message twice in Mode A**
unless `pipeline=kafka,rabbitmq` (drop direct MQTT).

The duplicate is usually harmless because SCADA's idempotent on alarm IDs,
but for production you typically run `pipeline=kafka,rabbitmq` to avoid it.

## 9.4 · Changing the pipeline at runtime

The pipeline is read at startup. To change it:

```bash
kubectl -n pinkline set env deploy/pas-scada-bridge BRIDGE_PIPELINE=kafka,rabbitmq
kubectl -n pinkline rollout status deploy/pas-scada-bridge
```

Verify in logs:
```bash
kubectl -n pinkline logs deploy/pas-scada-bridge | grep "pipeline:"
# expect: ... pipeline: kafka,rabbitmq | encrypt: true
```

---

# 10 · Alarm-state snapshot replay

A bonus feature for SCADA reconnect scenarios.

## 10.1 · The problem

When the SCADA-API pod restarts (deploy or crash), it loses its in-memory
view of which alarms are active. It would need TMS to re-send every
`UpdateAlarm` from history — but Kafka topic retention is 7 days and
TMS doesn't replay on demand.

## 10.2 · The solution — compacted topic + REST endpoint

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Reverse path Camel route — AlarmStateFanoutProcessor           │
   │                                                                 │
   │  When bridge.alarm-state.enabled=true (default):                │
   │     1. Parse JSON, extract Alarm.Id field                       │
   │     2. Send a SEPARATE Kafka record to topic                    │
   │          scada.tms.alarms.state                                 │
   │        with key=alarmId, value=current full alarm state         │
   │     3. Continue original pipeline (this is a fan-out, not a     │
   │        replacement — message still flows downstream)            │
   │                                                                 │
   │  Topic is configured with cleanup.policy=compact                │
   │   → Kafka periodically compacts: only the LATEST record per     │
   │     key is retained, forever.                                   │
   │   → After 1000 alarms have updated 100 times, the topic still  │
   │     holds only 1000 records (the latest state of each).         │
   └─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ on demand:
   ┌─────────────────────────────────────────────────────────────────┐
   │  Bridge REST endpoint: POST /api/snapshot/replay                │
   │  Defined in: tms/.../api/SnapshotController.java                │
   │                                                                 │
   │  When SCADA reconnects and calls this endpoint:                 │
   │     1. Bridge consumes scada.tms.alarms.state from offset 0     │
   │     2. For each record (latest per alarmId):                    │
   │          → re-publish to RabbitMQ amq.topic with routingKey     │
   │            tms.scada.pas (same path as forward output)          │
   │     3. SCADA receives a "fresh" copy of every active alarm      │
   │        without TMS having to re-send anything                   │
   └─────────────────────────────────────────────────────────────────┘
```

## 10.3 · Configuration

```properties
bridge.alarm-state.enabled=${BRIDGE_ALARM_STATE_ENABLED:true}
bridge.alarm-state.topic=${BRIDGE_ALARM_STATE_TOPIC:scada.tms.alarms.state}
```

Topic created by the bootstrap Job with compact policy + tuned segment
rolling so demo replays show effects within seconds, not hours.

---

# 11 · In-bridge monitor route

Optional 4th route inside the bridge that powers `GET /api/messages` on
port 8085. Useful when you want an at-a-glance view of recent decrypted
messages without running the full Demo dashboard.

```
   ┌─────────────────────────────────────────────────────────────────┐
   │  Camel route: monitor-rabbitmq-to-api                           │
   │  Defined in: KafkaBridgeRoutes.java:384                         │
   │  Activated when: bridge.monitor.enabled=true                    │
   │                                                                 │
   │   from("spring-rabbitmq:amq.topic"                              │
   │         + "?queues=scada.monitor.queue"                         │
   │         + "&routingKey=tms.scada.pas")                          │
   │     .process(decrypt encrypted bytes → JSON String)             │
   │     .process(MessageStore.add(topic, json))                     │
   │                                                                 │
   │  MessageStore is a ConcurrentLinkedDeque holding the last N     │
   │  messages. Exposed via:                                         │
   │     GET /api/messages       — JSON array of recent messages     │
   │     GET /api/messages?topic=tms.scada.pas — filter by topic     │
   └─────────────────────────────────────────────────────────────────┘
```

Default: **disabled**. Production uses the separate scada-api or demo
pods instead. Enable for quick debugging:

```bash
kubectl -n pinkline set env deploy/pas-scada-bridge BRIDGE_MONITOR_ENABLED=true
```

---

# 12 · Concrete example — UpdateAlarm every 10s, traced

The SCADA simulator dashboard auto-publishes an `UpdateAlarm` message
every 10 seconds. Let's trace ONE such message from generation to TMS
receipt, with timestamps and what each component does.

## 12.1 · The message

```json
{
  "CreatorId": "ScateX",
  "Type": "UpdateAlarm",
  "Timestamp": "2026-05-08 08:14:54.215",
  "Alarm": {
    "Timestamp": "2026-05-08 08:14:54.215",
    "Id": "DOOR_PLT_2_3",
    "State": "ACTIVE",
    "Priority": "HIGH"
  }
}
```

This is the same message you can see at http://localhost:8091 in the
SCADA → TMS pane, top right.

## 12.2 · Step-by-step trace (Mode B)

```
   t=0.000s   SCADA-API timer fires (UpdateAlarm.tsx, every 10s)
   ────────   external-scada/scada-api/app.py
              ▼
              client.publish(
                topic="scada/tms/alarms",
                payload=json.dumps(alarm_msg),
                qos=1
              )
              → Paho MQTT publishes plain JSON bytes to RabbitMQ:1883

   t=0.005s   RabbitMQ MQTT plugin receives the publish
   ────────   ▼ Translates MQTT topic to AMQP routing key:
              ▼   "scada/tms/alarms" → routingKey "scada.tms.alarms"
              ▼ Routes via amq.topic exchange to bound queue:
              ▼   queue = "scada.tms.alarms.queue" (declared by
              ▼            bootstrap-rabbitmq-queue Job)
              ▼ Message sits in the queue waiting for a consumer

   t=0.010s   scada-rabbitmq-source connector polls the queue
   ────────   (k8s pod kafka-connect, consumer group connect-...)
              ▼ AMQP basic.get returns the message (raw bytes)
              ▼ Camel kamelet wraps it as SourceRecord:
              ▼   key=null, value=byte[], topic="scada.tms.raw"
              ▼ value.converter=ByteArrayConverter (preserves bytes)
              ▼ Connect's Kafka producer writes to scada.tms.raw

   t=0.025s   Kafka topic scada.tms.raw receives record
   ────────   partition: hash(null) → 0
              offset: monotonically increasing
              durably persisted (replication factor 1 in dev, 3 in prod)

   t=0.030s   Bridge Camel reverse route consumes
   ────────   route ID: reverse-kafka-scada-tms-raw
              consumer group: pas-bridge-reverse
              ▼ Step 1: bytes → UTF-8 String (encrypt-enabled=false)
              ▼ Step 2: ScadaInboundProcessor logs "Type=UpdateAlarm"
              ▼ Step 3: AlarmStateFanoutProcessor:
              ▼   - parses JSON, extracts Alarm.Id="DOOR_PLT_2_3"
              ▼   - sends SEPARATE record to scada.tms.alarms.state
              ▼     with key="DOOR_PLT_2_3", value=full state
              ▼   - original message continues unchanged
              ▼ Step 4: convert-to-xml=false → JSON pass-through
              ▼ Step 5: Camel Kafka producer writes to scada.tms.processed

   t=0.045s   Kafka topic scada.tms.processed receives record
   ────────   (and scada.tms.alarms.state receives the compaction copy)

   t=0.050s   scada-artemis-sink connector polls scada.tms.processed
   ────────   ▼ Camel kamelet jms-pooled-apache-artemis-sink:
              ▼   broker = tcp://host.minikube.internal:61616
              ▼   destination = topic SCADA.TMS.Alarms
              ▼ JMS producer sends as TextMessage (string body)

   t=0.060s   Artemis publishes to subscribers of SCADA.TMS.Alarms
   ────────   ▼ Any TMS consumer subscribed to that topic gets the
              ▼ alarm immediately
              ▼ Total end-to-end: ~60 ms in healthy state
```

## 12.3 · What you'd see in each tool at t=0.060s

| Tool | What you see |
|---|---|
| http://localhost:8091 SCADA → TMS pane | New UpdateAlarm card at top, blue border, timestamp 08:14:54 |
| http://localhost:9000 Kafdrop, topic `scada.tms.raw` | New offset, value=raw JSON bytes |
| http://localhost:9000 Kafdrop, topic `scada.tms.processed` | New offset, value=same JSON |
| http://localhost:9000 Kafdrop, topic `scada.tms.alarms.state` | New offset, key="DOOR_PLT_2_3", value=full alarm state |
| http://localhost:8161 Artemis, addr `SCADA.TMS.Alarms` | `messagesRoutedCount` incremented by 1 |
| http://localhost:8083/connectors?expand=status | `scada-rabbitmq-source` and `scada-artemis-sink` both RUNNING, increasing record counts |
| Bridge logs | `← Kafka reverse [scada.tms.raw] ...` followed by `→ Kafka reverse [scada.tms.processed]` |

## 12.4 · Multiply by 4 timer types

The simulator auto-publishes 4 different message types on different intervals:

| Type | Interval | Purpose |
|---|---|---|
| UpdateAlarm | every 10 s | Most frequent — equipment state change |
| KeepAlive | every 30 s | Heartbeat — proves SCADA is alive |
| SendAllAlarms | every 60 s | Periodic full state broadcast |
| GetAllAlarms | every 120 s | Request from SCADA to TMS for sync |

So at any moment, at least one timer is firing every ~5 seconds on average,
and the same workflow above runs for each. You can confirm by watching the
counters at the top of http://localhost:8091 (each starts at 0 and
increments).

## 12.5 · Forward direction — the manual TMS publisher

The simulator's "MANUAL PUBLISH → TMS →" row at the bottom of 8091 is the
inverse: it publishes XML to an Artemis topic (e.g. `TMS.PISInfo`) which
drives the **forward path** in Sections 1 and 6.1. Pick a topic, set the
interval (e.g. 3 s), click Start, and watch the **TMS → SCADA** left pane
fill with decoded JSON.

That left pane is the same data SCADA-API decrypts at the end of Section 1.5.

---

# 13 · Connectivity matrix

Every network hop in the system, with port and protocol:

```
   ┌──────────────────┬──────────────────────┬────────┬──────────┬─────────┐
   │ FROM             │ TO                   │ PORT   │ PROTOCOL │ MODE    │
   ├──────────────────┼──────────────────────┼────────┼──────────┼─────────┤
   │ TMS XML pub      │ Artemis (host docker)│ 61616  │ JMS/Open │ A + B   │
   │                  │                      │        │   Wire   │         │
   ├──────────────────┼──────────────────────┼────────┼──────────┼─────────┤
   │ Kafka Connect    │ Artemis (via         │ 61616  │ JMS      │ B only  │
   │ (tms-artemis-    │  host.minikube.      │        │          │         │
   │  source)         │  internal)           │        │          │         │
   ├──────────────────┼──────────────────────┼────────┼──────────┼─────────┤
   │ Bridge (Camel    │ Artemis (via         │ 61616  │ JMS      │ A only  │
   │  ActiveMQ comp.) │  host.minikube.      │        │          │         │
   │                  │  internal)           │        │          │         │
   ├──────────────────┼──────────────────────┼────────┼──────────┼─────────┤
   │ Bridge / Connect │ Kafka                │ 9092   │ Kafka    │ A + B   │
   │                  │ (kafka-service)      │        │  protocol│         │
   ├──────────────────┼──────────────────────┼────────┼──────────┼─────────┤
   │ Bridge / Connect │ RabbitMQ             │ 5672   │ AMQP 0-9 │ A + B   │
   │                  │ (rabbitmq-internal   │        │   .1     │         │
   │                  │  .scada)             │        │          │         │
   ├──────────────────┼──────────────────────┼────────┼──────────┼─────────┤
   │ SCADA-API /      │ RabbitMQ             │ 1883   │ MQTT     │ A + B   │
   │ Demo / Bridge    │ (MQTT plugin)        │        │  v3.1.1  │         │
   ├──────────────────┼──────────────────────┼────────┼──────────┼─────────┤
   │ Bridge actuator  │ pod port 8085        │ 8085   │ HTTP     │ both    │
   │ Connect REST     │ pod port 8083        │ 8083   │ HTTP     │ both    │
   │ Kafdrop          │ pod port 9000        │ 9000   │ HTTP     │ both    │
   │ Monitor          │ pod port 8080        │ 8080   │ HTTP     │ both    │
   │ Demo             │ pod port 8090        │ 8090   │ HTTP/SSE │ both    │
   │ SCADA-API        │ pod port 8091        │ 8091   │ HTTP     │ both    │
   │ Artemis console  │ host port 8161       │ 8161   │ HTTP     │ both    │
   │ RabbitMQ admin   │ pod port 15672       │ 15672  │ HTTP     │ both    │
   └──────────────────┴──────────────────────┴────────┴──────────┴─────────┘

   host.minikube.internal — special hostname inside minikube that resolves
                            to the host machine. Lets pods talk to host
                            Docker containers (Artemis lives there, not
                            in k8s).
```

For production VM deployment, ports change — see `VM-DEPLOY.md` for the
firewall matrix between TMS-VM and SCADA-VM.

---

# 14 · Configuration file map

Every knob, where to turn it:

| Concern | File | Key |
|---|---|---|
| Mode A vs B (forward) | `tms/k8s/deployment.yaml` env | `BRIDGE_INPUT_FROM_KAFKA` |
| Mode A vs B (reverse) | `tms/k8s/deployment.yaml` env | `BRIDGE_REVERSE_KAFKA_ENABLED` |
| Pipeline order/sinks | `tms/k8s/deployment.yaml` env | `BRIDGE_PIPELINE` |
| Forward encryption | `application.properties` | `bridge.encrypt.enabled` |
| Reverse encryption | env | `BRIDGE_REVERSE_KAFKA_ENCRYPT_ENABLED` |
| Reverse XML conversion | env | `BRIDGE_REVERSE_KAFKA_CONVERT_TO_XML` |
| Alarm-state fan-out | env | `BRIDGE_ALARM_STATE_ENABLED` |
| In-bridge monitor route | env | `BRIDGE_MONITOR_ENABLED` |
| Artemis topics list | `application.properties` | `bridge.artemis-topics` |
| Inbound RMQ→Artemis | `application.properties` | `bridge.inbound[0].*` |
| AES key (forward) | k8s Secret | `scada-aes-key.SCADA_AES_KEY` |
| Artemis credentials | k8s Secret | `connect-secret.ARTEMIS_USER/PASSWORD` |
| RabbitMQ credentials | k8s Secret | `connect-secret.RABBITMQ_USER/PASS` |
| Connector configs (5) | `connect/k8s/20-configmap.yaml` | each connector JSON |
| Kafka topic specs | `bootstrap/k8s/10-kafka-topics-job.yaml` | inline ConfigMap `topics-spec.txt` |
| RabbitMQ queue/binding | `bootstrap/k8s/20-rabbitmq-queue-job.yaml` | env vars in Job |
| Probe definitions (19) | `monitor/config.yaml` | `probes:` array |
| Demo data sources | `demo/app.py` | hardcoded; reads same MQTT/RabbitMQ as scada-api |

---

# 15 · Per-environment config files

Bridge has 4 Spring profiles. Pick one with `SPRING_PROFILES_ACTIVE`:

| Profile | File | Activated when |
|---|---|---|
| (default) | `application.properties` | always — base config inherited by all |
| `local` | `application-local.properties` | running bridge directly with Maven (no Docker) |
| `dev` | `application-dev.properties` | development server / minikube |
| `staging` | `application-staging.properties` | staging server |
| `prod` | `application-prod.properties` | production VM |

Per-environment overrides typically include:
- Artemis broker URL (`broker-url=tcp://...`)
- RabbitMQ host (cluster-internal vs external IP)
- Kafka brokers list
- MQTT broker URL
- Log levels

All files share the same `bridge.*` keys; profile-specific files override
only the network endpoints and credentials. Don't put secrets here in
production — use k8s Secrets and refer to them as `${ENV_VAR}`.

To switch profile in k8s:
```bash
kubectl -n pinkline set env deploy/pas-scada-bridge SPRING_PROFILES_ACTIVE=prod
kubectl -n pinkline rollout status deploy/pas-scada-bridge
```
