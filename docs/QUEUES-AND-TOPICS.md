# Queues, topics & viewer queues — full inventory

Every address, queue, topic, and binding in the system, plus the
manual-create commands for the Artemis viewer queues you need to browse
data in the Artemis console.

---

## 1 · Where everything lives

| System | Concept | What it does |
|---|---|---|
| **Artemis** | Address (multicast) | JMS pub/sub topic — drops messages if no subscriber |
| **Artemis** | Queue (under address) | Durable subscription — retains messages |
| **Kafka** | Topic | Durable log — retains for 7-14 days regardless of consumers |
| **RabbitMQ** | Exchange + queue + binding | AMQP — exchange routes by key into bound queues |
| **RabbitMQ** | MQTT plugin | Maps MQTT topics to AMQP routing keys (dot↔slash) |

---

## 2 · Artemis addresses (host Docker, port 61616)

These are the JMS topics on the TMS-side broker. Open
http://localhost:8161/console → addresses to see them.

| Address | Direction | What flows here | Created by |
|---|---|---|---|
| `TMS.PISInfo` | TMS → SCADA forward | PIS info XML from TMS publisher | TMS publisher (auto-creates on first publish) |
| `RCS.E2K.TMS.TrafficReportClient` | TMS → SCADA forward | Traffic reports from TMS | TMS publisher |
| `TSInfo` | TMS → SCADA forward | TS info from TMS | TMS publisher |
| `RCS.E2K.TMS.RouteInfo` | TMS → SCADA forward | Route info from TMS | TMS publisher |
| `SCADA.TMS.Alarms` | SCADA → TMS reverse | RSAE alarms from SCADA | scada-artemis-sink connector |
| `DLQ` | system | Default Artemis DLQ | Artemis built-in |
| `DLQ.kafka-bridge` | system | Camel route exception DLQ | bridge `onException` handler |
| `ExpiryQueue` | system | Expired messages | Artemis built-in |

> **Why most addresses look empty in the console:** they're **multicast**.
> Without a subscribed queue, Artemis routes the message in then drops it.
> See Section 6 to add viewer queues that retain messages for browsing.

---

## 3 · Kafka topics (in-cluster, port 9092)

Browse all of these in **Kafdrop** — http://localhost:9000.

| Topic | Partitions | Retention | What flows here |
|---|---|---|---|
| `tms.raw` | 3 | 7 days | XML from `tms-artemis-source*` connectors (forward in) |
| `tms.scada.encrypted` | 3 | 7 days | AES-encrypted JSON from bridge (forward out) |
| `scada.tms.raw` | 3 | 7 days | Raw JSON from `scada-rabbitmq-source` (reverse in) |
| `scada.tms.processed` | 3 | 7 days | Processed JSON from bridge (reverse out) |
| `scada.tms.alarms.state` | 3 | compacted | Latest alarm state per `alarmId` (snapshot replay) |
| `dlq.connect.tms-artemis-source` | 1 | 14 days | DLQ — failed records from any of 4 source connectors |
| `dlq.connect.tms-rabbitmq-sink` | 1 | 14 days | DLQ — failed publishes to RabbitMQ |
| `dlq.connect.scada-rabbitmq-source` | 1 | 14 days | DLQ — failed reads from RabbitMQ |
| `dlq.connect.scada-artemis-sink` | 1 | 14 days | DLQ — failed publishes back to Artemis |

All created by the `bootstrap-kafka-topics` Job (see
`bootstrap/k8s/10-kafka-topics-job.yaml`).

---

## 4 · RabbitMQ exchanges, queues & bindings (scada namespace, port 5672)

Open http://localhost:15672 (thiru/password) → Queues / Exchanges tabs.

### Exchanges
| Exchange | Type | Source of |
|---|---|---|
| `amq.topic` | topic (built-in) | All MQTT-bridged + Camel-published messages |

### Queues
| Queue | Bound to | Routing key | Drained by |
|---|---|---|---|
| `scada.tms.alarms.queue` | `amq.topic` | `scada.tms.alarms` | scada-rabbitmq-source connector |
| `scada.monitor.queue` | `amq.topic` | `tms.scada.pas` | bridge in-app monitor (if enabled) |
| `mqtt-subscription-scada-sim-ScateXqos0` | `amq.topic` | `tms.scada.pas` | scada-api MQTT subscriber (auto-managed by MQTT plugin) |

### MQTT topic → AMQP routing key translation (RabbitMQ MQTT plugin)
| MQTT topic | AMQP routing key |
|---|---|
| `scada/tms/alarms` | `scada.tms.alarms` |
| `tms/scada/pas` | `tms.scada.pas` |

(The plugin replaces `/` with `.` and uses `amq.topic` as the exchange.)

The `scada.tms.alarms.queue` is declared by the
`bootstrap-rabbitmq-queue` Job (or by `start.sh` running rabbitmqadmin
inside the rabbitmq pod).

---

## 5 · The 7 Kafka Connect connectors

| Connector | From | To | Purpose |
|---|---|---|---|
| `tms-artemis-source` | Artemis `TMS.PISInfo` | Kafka `tms.raw` | Forward source 1 |
| `tms-artemis-source-trafficreport` | Artemis `RCS.E2K.TMS.TrafficReportClient` | Kafka `tms.raw` | Forward source 2 |
| `tms-artemis-source-tsinfo` | Artemis `TSInfo` | Kafka `tms.raw` | Forward source 3 |
| `tms-artemis-source-routeinfo` | Artemis `RCS.E2K.TMS.RouteInfo` | Kafka `tms.raw` | Forward source 4 |
| `tms-rabbitmq-sink` | Kafka `tms.scada.encrypted` | RabbitMQ `amq.topic`/`tms.scada.pas` | Forward sink |
| `scada-rabbitmq-source` | RabbitMQ queue `scada.tms.alarms.queue` | Kafka `scada.tms.raw` | Reverse source |
| `scada-artemis-sink` | Kafka `scada.tms.processed` | Artemis `SCADA.TMS.Alarms` | Reverse sink |

Check status:
```bash
kubectl -n pinkline exec deploy/kafka-connect -- \
  curl -s http://localhost:8083/connectors?expand=status \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state, tasks:[.value.status.tasks[].state]}'
```

---

## 6 · Manual viewer queues for Artemis (one-time setup)

Artemis multicast topics drop messages when no subscriber is connected.
To **browse messages in the Artemis console**, create a durable queue
under each address. Once created, the queue retains every future message
routed to that address.

> ⚠ Run these **once per fresh Artemis container**. They persist across
> Artemis restarts but are lost if the container is recreated (e.g.
> `docker compose down`).

### 6.1 · The 5 queues to create

| Queue name | Address | What you'll see when you browse |
|---|---|---|
| `scada-tms-viewer` | `SCADA.TMS.Alarms` | RSAE alarms (UpdateAlarm, KeepAlive, SendAllAlarms) coming back to TMS |
| `tms-pisinfo-viewer` | `TMS.PISInfo` | PIS XML being sent from TMS publishers |
| `trafficreport-viewer` | `RCS.E2K.TMS.TrafficReportClient` | Traffic report XML |
| `tsinfo-viewer` | `TSInfo` | TS info XML |
| `routeinfo-viewer` | `RCS.E2K.TMS.RouteInfo` | Route info XML |

### 6.2 · Create commands (Git Bash on Windows)

```bash
# 1. SCADA → TMS reverse alarms (most useful — proves SCADA→TMS works)
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue create \
  --name scada-tms-viewer --address SCADA.TMS.Alarms \
  --durable --multicast --auto-create-address \
  --user admin --password admin --silent

# 2. TMS → SCADA forward (PIS)
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue create \
  --name tms-pisinfo-viewer --address TMS.PISInfo \
  --durable --multicast --auto-create-address \
  --user admin --password admin --silent

# 3. TMS → SCADA forward (traffic reports)
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue create \
  --name trafficreport-viewer --address RCS.E2K.TMS.TrafficReportClient \
  --durable --multicast --auto-create-address \
  --user admin --password admin --silent

# 4. TMS → SCADA forward (TS info)
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue create \
  --name tsinfo-viewer --address TSInfo \
  --durable --multicast --auto-create-address \
  --user admin --password admin --silent

# 5. TMS → SCADA forward (route info)
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue create \
  --name routeinfo-viewer --address RCS.E2K.TMS.RouteInfo \
  --durable --multicast --auto-create-address \
  --user admin --password admin --silent
```

> **Why `MSYS_NO_PATHCONV=1`?** Git Bash on Windows mangles `/var/lib/...`
> into a Windows path. The prefix disables that conversion. In PowerShell
> or on Linux/Mac, drop the prefix.

### 6.3 · PowerShell equivalent

```powershell
$queues = @(
  @{name='scada-tms-viewer';     address='SCADA.TMS.Alarms'},
  @{name='tms-pisinfo-viewer';   address='TMS.PISInfo'},
  @{name='trafficreport-viewer'; address='RCS.E2K.TMS.TrafficReportClient'},
  @{name='tsinfo-viewer';        address='TSInfo'},
  @{name='routeinfo-viewer';     address='RCS.E2K.TMS.RouteInfo'}
)
foreach ($q in $queues) {
  docker exec artemis /var/lib/artemis-instance/bin/artemis queue create `
    --name $q.name --address $q.address `
    --durable --multicast --auto-create-address `
    --user admin --password admin --silent
}
```

### 6.4 · Verify the queues exist and are filling

```bash
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue stat \
  --user admin --password admin --url tcp://localhost:61616 \
  | grep -E "NAME|viewer"
```

Expected output (after a few seconds of SCADA running):
```
| NAME                  | ADDRESS                          | MESSAGE_COUNT | MESSAGES_ADDED |
| scada-tms-viewer      | SCADA.TMS.Alarms                 | 17            | 17             |
| tms-pisinfo-viewer    | TMS.PISInfo                      | 9             | 9              |
| trafficreport-viewer  | RCS.E2K.TMS.TrafficReportClient  | 0             | 0              |
| tsinfo-viewer         | TSInfo                           | 0             | 0              |
| routeinfo-viewer      | RCS.E2K.TMS.RouteInfo            | 0             | 0              |
```

(`trafficreport`, `tsinfo`, `routeinfo` show 0 unless TMS is actively
publishing to those addresses.)

### 6.5 · Browse messages in the Artemis console

1. Open http://localhost:8161/console (admin/admin)
2. Tree → `0.0.0.0` → `addresses` → expand the address
3. → `queues` → click the viewer queue (e.g. `scada-tms-viewer`)
4. → **More ▾** → **Browse**
5. Each row is one message — click to see the full body

---

## 7 · Maintenance

### 7.1 · Viewer queues grow forever

Since nobody consumes them, messages accumulate indefinitely. Two options:

#### Purge (keep the queue, drop the messages)
```bash
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue purge \
  --name scada-tms-viewer --user admin --password admin
```

#### Delete and recreate
```bash
# Delete
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue delete \
  --name scada-tms-viewer --user admin --password admin

# Then re-run the create command from 6.2
```

### 7.2 · After Artemis container is recreated

If you ran `docker compose down` and brought Artemis back up, the queues
are gone — re-run all 5 create commands.

### 7.3 · List ALL queues (not just viewers)

```bash
MSYS_NO_PATHCONV=1 docker exec artemis \
  /var/lib/artemis-instance/bin/artemis queue stat \
  --user admin --password admin --url tcp://localhost:61616
```

Look for:
- Random UUID names (e.g. `d228db79-...`) under `TMS.PISInfo` etc. —
  those are auto-created by Connect's JMS subscriber. **Don't delete them.**
- The `viewer` queues you created (durable, listed above).
- `DLQ`, `DLQ.kafka-bridge`, `ExpiryQueue` — Artemis system queues.
- `activemq.management.*` — Artemis JMX management; ignore.

---

## 8 · Quick reference — the message-flow audit checklist

When you want to verify "is data really flowing?":

| Check | Where | What to look for |
|---|---|---|
| TMS → Artemis forward in | Artemis console → `tms-pisinfo-viewer` (after creating) | New rows appearing |
| Kafka tms.raw | Kafdrop → `tms.raw` | Increasing offsets |
| Bridge encrypted output | Kafdrop → `tms.scada.encrypted` | Same growth as `tms.raw` |
| RabbitMQ delivery | RabbitMQ admin → Exchanges → `amq.topic` | "Message rate in" non-zero |
| MQTT live | MQTT Explorer → `tms/scada/pas` | New payloads every few sec |
| SCADA decryption | http://localhost:8091 → TMS→SCADA pane | Decoded JSON cards |
| SCADA → MQTT publish | MQTT Explorer → `scada/tms/alarms` | Messages every 10s |
| RabbitMQ queue | RabbitMQ admin → `scada.tms.alarms.queue` | 1 consumer, msgs flowing |
| Kafka scada.tms.raw | Kafdrop → `scada.tms.raw` | Increasing offsets |
| Kafka scada.tms.processed | Kafdrop → `scada.tms.processed` | Same growth |
| Artemis SCADA.TMS.Alarms | Artemis console → `scada-tms-viewer` (after creating) | New rows appearing |

If all 11 are flowing, your end-to-end pipeline is healthy.
