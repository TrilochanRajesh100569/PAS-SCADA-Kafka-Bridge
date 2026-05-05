# External SCADA VM — Kubernetes deployment

Runs on the **external VM** / SCADA-side cluster. Contains RabbitMQ (with MQTT plugin).

## File order (apply in this order)

| # | File | Purpose |
|---|---|---|
| 00 | `00-namespace.yaml` | creates `scada` namespace |
| 10 | `10-rabbitmq-configmap.yaml` | `enabled_plugins` + `rabbitmq.conf` |
| 20 | `20-rabbitmq-secret.yaml` | default user/password/erlang cookie |
| 30 | `30-rabbitmq-pvc.yaml` | 5Gi PVC for queue state |
| 40 | `40-rabbitmq-deployment.yaml` | RabbitMQ Deployment |
| 50 | `50-rabbitmq-service.yaml` | internal ClusterIP + external NodePort |

## Every configurable value

### `20-rabbitmq-secret.yaml`

| Key | Default | Must match with |
|---|---|---|
| `RABBITMQ_DEFAULT_USER` | `thiru` | **`tms/k8s/secret.yaml` RABBITMQ_USER + MQTT_USER** |
| `RABBITMQ_DEFAULT_PASS` | `password` | **`tms/k8s/secret.yaml` RABBITMQ_PASS + MQTT_PASS** |
| `RABBITMQ_ERLANG_COOKIE` | `SWQOKODSQALRPCLNMEQG` | Only matters if clustering |

### `10-rabbitmq-configmap.yaml` — `rabbitmq.conf`

| Setting | Default | Change when |
|---|---|---|
| `listeners.tcp.default` | `5672` | Use non-standard AMQP port |
| `management.tcp.port` | `15672` | Non-standard admin port |
| `mqtt.listeners.tcp.default` | `1883` | Non-standard MQTT port |
| `web_mqtt.tcp.port` | `15675` | Non-standard WS-MQTT port |
| `mqtt.default_user` | `thiru` | **Must match secret RABBITMQ_DEFAULT_USER** |
| `mqtt.default_pass` | `password` | **Must match secret RABBITMQ_DEFAULT_PASS** |
| `mqtt.allow_anonymous` | `false` | `true` for open dev environments only |
| `mqtt.vhost` | `/` | Isolate via separate vhost |
| `mqtt.exchange` | `amq.topic` | Different default exchange |
| `heartbeat` | `30` | Tune for flaky networks |
| `disk_free_limit.absolute` | `500MB` | Raise for busy brokers |

### `50-rabbitmq-service.yaml` — NodePorts

**These must match `tms/k8s/configmap.yaml` RABBITMQ_PORT / MQTT_PORT.**

| Service port | NodePort | Used by |
|---|---|---|
| AMQP 5672 | **30672** | TMS bridge (cross-VM publish) |
| MQTT 1883 | **31883** | External MQTT clients (optional) |
| HTTP 15672 | **31567** | Admin UI |
| WS-MQTT 15675 | **31568** | Browser WS-MQTT clients |

Switch to `LoadBalancer` if you have a cloud LB (see comments in file).

### `30-rabbitmq-pvc.yaml`

| Field | Default | Change when |
|---|---|---|
| `storage` | `5Gi` | Heavy queues / long retention |
| `storageClassName` | *(cluster default)* | Pin to a specific class |

### Image version

| File | Image | Version |
|---|---|---|
| `40-rabbitmq-deployment.yaml` | `rabbitmq` | `3.12-management` |

### Resource requests/limits (edit in `40-rabbitmq-deployment.yaml`)

Defaults: 512Mi request / 1Gi limit. Increase for heavy MQTT fan-out.

## Deploy

```bash
# Set your context to the SCADA / external VM cluster
kubectl config use-context <external-vm-context>

# Apply in order
kubectl apply -f 00-namespace.yaml
kubectl apply -f 10-rabbitmq-configmap.yaml
kubectl apply -f 20-rabbitmq-secret.yaml
kubectl apply -f 30-rabbitmq-pvc.yaml
kubectl apply -f 40-rabbitmq-deployment.yaml
kubectl apply -f 50-rabbitmq-service.yaml

# Wait for ready
kubectl -n scada wait --for=condition=ready pod -l app=rabbitmq --timeout=180s
```

## One-time queue setup (after RabbitMQ is up)

The bridge monitor route expects `scada.monitor.queue` to exist:

```bash
EXTERNAL_VM=<external-vm-ip>
curl -u thiru:password -X PUT \
  http://$EXTERNAL_VM:31567/api/queues/%2F/scada.monitor.queue \
  -H "Content-Type: application/json" \
  -d '{"durable":true,"auto_delete":false}'

curl -u thiru:password -X POST \
  http://$EXTERNAL_VM:31567/api/bindings/%2F/e/amq.topic/q/scada.monitor.queue \
  -H "Content-Type: application/json" \
  -d '{"routing_key":"tms.scada.pas"}'
```

## Verify

```bash
# Admin UI from your laptop
open http://<external-vm-ip>:31567

# Check plugins enabled
kubectl -n scada exec -it deploy/rabbitmq -- rabbitmq-plugins list -e
# Expected: rabbitmq_management, rabbitmq_mqtt, rabbitmq_web_mqtt

# Check listeners
kubectl -n scada exec -it deploy/rabbitmq -- rabbitmq-diagnostics listeners
# Expected: amqp on 5672, mqtt on 1883, http on 15672, http/web-mqtt on 15675
```

## Firewall on the external VM (NodePort mode)

Allow from the TMS VM's IP only:

```bash
# ufw example
sudo ufw allow from <TMS_VM_IP> to any port 30672 proto tcp comment 'AMQP to bridge'
sudo ufw allow from <TMS_VM_IP> to any port 31883 proto tcp comment 'MQTT (optional)'
```
