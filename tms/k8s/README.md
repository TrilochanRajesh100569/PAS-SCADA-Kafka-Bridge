# TMS VM — Kubernetes deployment

Runs on the **internal VM** / TMS-side cluster. Contains Artemis, Zookeeper, Kafka, and the PAS-SCADA-Kafka-Bridge.

## File order (apply in this order)

| # | File | Purpose |
|---|---|---|
| 00 | `00-namespace.yaml` | creates `pinkline` namespace |
| 10 | `10-artemis.yaml` | Artemis broker + PVC + Service + Secret |
| 20 | `20-zookeeper.yaml` | Zookeeper for Kafka |
| 30 | `30-kafka.yaml` | Kafka broker + Service |
| 40 | `configmap.yaml` | Bridge config (hosts/ports/pipeline) |
| 50 | `secret.yaml` | Bridge secrets (credentials + AES key) |
| 60 | `deployment.yaml` | Bridge Deployment + Service |

## Every configurable value

### `configmap.yaml`

| Key | Default | Change when |
|---|---|---|
| `SPRING_PROFILES_ACTIVE` | `prod` | Never — always prod in K8s |
| `ARTEMIS_HOST` | `artemis-service` | Artemis outside cluster |
| `ARTEMIS_PORT` | `61616` | Non-standard OpenWire port |
| `KAFKA_HOST` | `kafka-service` | Kafka outside cluster |
| `KAFKA_PORT` | `9092` | Non-standard listener |
| `RABBITMQ_HOST` | **`EXTERNAL-VM-IP-OR-DNS`** | **ALWAYS — set to external VM IP/DNS** |
| `RABBITMQ_PORT` | `30672` (NodePort) | `5672` if external uses LoadBalancer |
| `MQTT_HOST` | **`EXTERNAL-VM-IP-OR-DNS`** | **ALWAYS — same IP as RABBITMQ_HOST** |
| `MQTT_PORT` | `31883` (NodePort) | `1883` if LoadBalancer |
| `BRIDGE_PIPELINE` | `kafka,rabbitmq,mqtt` | Change sink chain |
| `BRIDGE_MONITOR_ENABLED` | `true` | `false` in prod for perf |
| `BRIDGE_MONITOR_FROM_EXCHANGE` | `amq.topic` | Different exchange |
| `BRIDGE_MONITOR_ROUTING_KEY` | `tms.scada.pas` | Different routing key |
| `BRIDGE_MONITOR_QUEUE` | `scada.monitor.queue` | Different queue name |

### `secret.yaml` (stringData — replace all `REPLACE` values)

| Key | Notes |
|---|---|
| `ARTEMIS_USER` | Artemis broker user — must match `artemis-secret` |
| `ARTEMIS_PASS` | Artemis broker password |
| `RABBITMQ_USER` | **Must match `external-scada/k8s/20-rabbitmq-secret.yaml` RABBITMQ_DEFAULT_USER** |
| `RABBITMQ_PASS` | **Must match external-scada secret** |
| `MQTT_USER` | **Same as RABBITMQ_USER** (RabbitMQ MQTT plugin shares users) |
| `MQTT_PASS` | **Same as RABBITMQ_PASS** |
| `SCADA_AES_KEY` | Base64-encoded 32-byte AES-256 key — share with SCADA decryption side |

### `artemis-secret` (inside `10-artemis.yaml`)

| Key | Default |
|---|---|
| `ARTEMIS_USER` | `pasbridge` |
| `ARTEMIS_PASSWORD` | `testpass123` (**CHANGE IN PROD**) |

### Image versions (edit in YAML)

| File | Image | Version |
|---|---|---|
| `10-artemis.yaml` | `apache/activemq-artemis` | `2.31.2` |
| `20-zookeeper.yaml` | `confluentinc/cp-zookeeper` | `7.5.0` |
| `30-kafka.yaml` | `confluentinc/cp-kafka` | `7.5.0` |
| `deployment.yaml` | `pinkline/pas-scada-bridge` | `latest` (change to pinned tag) |

### Storage sizing

| PVC | Default | Where |
|---|---|---|
| `artemis-data` | `5Gi` | `10-artemis.yaml` |
| (no kafka PVC yet — Kafka uses emptyDir; add PVC for prod) | — | — |

### Resource requests/limits (edit per file)

Defaults sized for dev. For prod, increase memory/CPU.

## Deploy

```bash
# Set your context to the TMS VM cluster
kubectl config use-context <tms-vm-context>

# Apply in order
kubectl apply -f 00-namespace.yaml
kubectl apply -f 10-artemis.yaml
kubectl apply -f 20-zookeeper.yaml
kubectl apply -f 30-kafka.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f deployment.yaml

# Check
kubectl -n pinkline get pods -w
kubectl -n pinkline logs -f deploy/pas-scada-bridge
```

## Verify

```bash
# Port-forward the bridge and check health
kubectl -n pinkline port-forward svc/pas-scada-bridge 8085:8085
curl http://localhost:8085/actuator/health
curl http://localhost:8085/actuator/camelroutes
```

## Cross-VM connectivity check

Bridge must reach external RabbitMQ on `$RABBITMQ_HOST:$RABBITMQ_PORT`.
From inside the bridge pod:
```bash
kubectl -n pinkline exec -it deploy/pas-scada-bridge -- \
  sh -c "nc -zv $RABBITMQ_HOST $RABBITMQ_PORT"
```
