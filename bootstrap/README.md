# bootstrap/ — pre-create Kafka topics + RabbitMQ queue

Two k8s Jobs that establish the topic + queue topology the Connect
connectors and Streams app expect. Both are idempotent — re-run any
time after editing the spec.

## When to run

* **First-time deploy** to a fresh cluster (before applying `connect/`)
* **After adding a new topic** to `kafka-topics-spec` ConfigMap
* **After Phase 5 cutover** removes the Camel bridge — the bridge used
  to declare the RabbitMQ queue at startup; this Job replaces that.

Auto-create-topics on the broker (`auto.create.topics.enable=true`)
*also* makes things work, but with broker defaults — usually 1
partition, 7-day retention. The Job here gives intended partition
counts + retention so production capacity matches design.

## What it creates

### Kafka topics (8)

| Topic | Partitions | Replication | Retention |
|---|---|---|---|
| `tms.raw` | 3 | 1 | 7d |
| `tms.scada.encrypted` | 3 | 1 | 7d |
| `scada.tms.raw` | 3 | 1 | 7d |
| `scada.tms.processed` | 3 | 1 | 7d |
| `dlq.connect.tms-artemis-source` | 1 | 1 | 14d |
| `dlq.connect.tms-rabbitmq-sink` | 1 | 1 | 14d |
| `dlq.connect.scada-rabbitmq-source` | 1 | 1 | 14d |
| `dlq.connect.scada-artemis-sink` | 1 | 1 | 14d |

`_connect-configs/_offsets/_status` are NOT in this list — Connect
creates its own internal topics on first startup. Edit
`bootstrap/k8s/10-kafka-topics-job.yaml` if you'd rather pre-create those too.

### RabbitMQ queue + binding

* Queue `scada.tms.alarms.queue` (durable, not auto-delete)
* Bound to exchange `amq.topic` with routing key `scada.tms.alarms`
* RabbitMQ's MQTT plugin maps MQTT topic `scada/tms/alarms` to this
  exact routing key, so SCADA's MQTT publish lands here automatically.

## Run

```bash
# 1. Make sure the connect-secret is applied first — the RMQ Job pulls
#    RABBITMQ_USER/PASS from it.
kubectl apply -f connect/k8s/10-secret.yaml

# 2. Run both bootstrap Jobs
kubectl apply -f bootstrap/k8s/10-kafka-topics-job.yaml
kubectl apply -f bootstrap/k8s/20-rabbitmq-queue-job.yaml

# 3. Watch them
kubectl -n pinkline logs -f job/bootstrap-kafka-topics
kubectl -n pinkline logs -f job/bootstrap-rabbitmq-queue
```

Healthy output:

```
→ tms.raw (partitions=3, rf=1, retention=604800000ms)
  OK
→ tms.scada.encrypted (partitions=3, rf=1, retention=604800000ms)
  OK
...
All topics ready.
```

```
→ declaring queue scada.tms.alarms.queue in vhost /
  HTTP 201
  queue OK
→ binding scada.tms.alarms.queue to amq.topic with key scada.tms.alarms
  HTTP 201
  binding OK
```

## Re-run after editing

```bash
# Edit the topic list:
vi bootstrap/k8s/10-kafka-topics-job.yaml   # change topics-spec.txt

# Re-run (Job objects are immutable — must delete the old one first):
kubectl -n pinkline delete job bootstrap-kafka-topics --ignore-not-found
kubectl apply -f bootstrap/k8s/10-kafka-topics-job.yaml
```

## Pointing at *your* Kafka / RabbitMQ

Both Jobs default to the addresses for the two-cluster dev topology:

```yaml
KAFKA_BOOTSTRAP: "kafka-service.pinkline.svc.cluster.local:9092"  # in-cluster TMS Kafka
RMQ_HOST:        "10.4.0.25"                                       # SCADA VM (cross-cluster)
RMQ_MGMT_PORT:   "31567"                                           # NodePort on SCADA VM
```

If your topology differs (different IPs, in-cluster RabbitMQ, etc.), edit
the `env:` section of each Job before applying:

```bash
# Example: point at a different Kafka cluster
sed 's|kafka-service.pinkline.svc.cluster.local:9092|kafka.shared.svc:9092|' \
  bootstrap/k8s/10-kafka-topics-job.yaml | kubectl apply -f -
```

## Troubleshooting

**`kafka-topics: Connection refused`** in the Kafka Job log
→ Wrong `KAFKA_BOOTSTRAP` value or Kafka not reachable from the Job pod.
   Test from any pod in the same namespace:
   `kubectl -n pinkline run -it --rm probe --image=busybox --restart=Never -- nc -zv kafka-service 9092`

**`HTTP 401 Unauthorized`** in the RMQ Job log
→ The `connect-secret` either doesn't exist yet or its
   `RABBITMQ_USER` / `RABBITMQ_PASS` keys don't match the broker's auth.

**Topic exists with wrong partition count**
→ `--if-not-exists` skips topics that already exist. To change
   partitions on an existing topic, use `kafka-topics --alter
   --partitions N` (NB: never decreases). To rebuild from scratch:
   `kafka-topics --delete --topic <name>` first, then re-run the Job.

**Queue exists but binding doesn't**
→ The Job declares both. If just the binding is missing
   (e.g. RabbitMQ was reset but the queue persisted), the binding step
   creates it idempotently — re-run is safe.

## Files

```
bootstrap/
  README.md                          — this file
  k8s/
    10-kafka-topics-job.yaml         — ConfigMap (topic spec) + Job
    20-rabbitmq-queue-job.yaml       — Job (uses connect-secret for auth)
```
