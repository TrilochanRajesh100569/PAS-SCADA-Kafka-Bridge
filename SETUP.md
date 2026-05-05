# Setup — bring the whole stack up on a fresh PC

End-to-end install for someone who's never touched this repo before. If you
just want to run it again on the same PC, skip to [`./start.sh`](#one-time-setup).

## What you'll have when this is done

| Where | What |
|---|---|
| Docker (host) | Artemis broker — TMS-side JMS broker on `61616`, console on `8161` |
| minikube `pinkline` namespace | Zookeeper · Kafka · Kafdrop · Kafka Connect (7 connectors) · pas-scada-bridge · pas-scada-monitor · pas-scada-demo |
| minikube `scada` namespace | RabbitMQ (+ MQTT plugin) · scada-api (mock SCADA simulator) |

Connect-mode is on, with split encryption: forward (TMS → SCADA) AES-256-GCM,
reverse (SCADA → TMS) plain JSON. See [`CLIENT-REQUEST.md`](./CLIENT-REQUEST.md)
for the full architecture.

## Prerequisites

| Tool | Why | How to install |
|---|---|---|
| **Docker Desktop** | Runs Artemis on the host + minikube's container engine | https://docker.com/products/docker-desktop |
| **minikube** | Local Kubernetes | `winget install minikube` (Windows) · `brew install minikube` (Mac) · [docs](https://minikube.sigs.k8s.io/docs/start/) (Linux) |
| **kubectl** | Talks to minikube | `winget install Kubernetes.kubectl` · `brew install kubectl` · [docs](https://kubernetes.io/docs/tasks/tools/) |
| **git** | Clone the repos | https://git-scm.com |
| **bash** | Run `start.sh` | Built-in on Mac/Linux; on Windows use Git Bash or WSL |

Resources Docker Desktop needs (Settings → Resources):

- **CPU** ≥ 4
- **Memory** ≥ 8 GB
- **Disk** ≥ 30 GB free

Without these the Spring Boot bridge will OOM-kill, Kafka will fail leader
election, and minikube image loads will run out of space.

## One-time setup

```bash
# 1. Clone both repos side-by-side
mkdir -p ~/code && cd ~/code
git clone https://github.com/thirunavukkarasuthangaraj/POC-Camel-Routes.git PAS-SCADA-Kafka-Bridge
git clone <messaging-infra-repo-url> messaging-infra        # provides host-side Artemis docker-compose

# 2. Tell start.sh where messaging-infra lives if it's NOT at /d/pinkline/code/messaging-infra
export MESSAGING_INFRA="$HOME/code/messaging-infra"          # adjust to your path
# (on Windows Git Bash: /c/Users/you/code/messaging-infra)

# 3. Run
cd PAS-SCADA-Kafka-Bridge
chmod +x start.sh
./start.sh
```

That's it. `start.sh` is idempotent — safe to re-run after any failure or
crash; it converges to the desired state.

## What `start.sh` does, in order

1. Starts minikube (`--cpus=4 --memory=6144 --driver=docker`) if not already running.
2. Brings up Artemis on the host via `docker compose up -d` from `$MESSAGING_INFRA`.
3. Builds five images from source (cached after first run):
   - `pinkline/pas-scada-connect` (Kafka Connect + Camel kamelets)
   - `pinkline/pas-scada-bridge`  (Spring Boot + Camel)
   - `ghcr.io/thirunavukkarasuthangaraj/pas-scada-api` (Python Flask SCADA mock)
   - `pinkline/pas-scada-monitor` (FastAPI health monitor)
   - `pinkline/pas-scada-demo`    (FastAPI customer demo)
4. **Hard-replaces** locally-built images inside minikube via
   `minikube ssh -- docker rmi -f <img>` then `minikube image load <img>`.
   (Necessary because `minikube image load --overwrite` is unreliable across versions.)
5. Applies every k8s manifest:
   - `tms/k8s/` — Zookeeper, Kafka, Kafdrop, bridge ConfigMap+Secret+Deployment
   - `external-scada/k8s/` — RabbitMQ + scada-api
   - `connect/k8s/` — Kafka Connect deployment + connector configs ConfigMap
   - `monitor/k8s/` — FastAPI monitor + 19 probe definitions (incl. all 7 connector statuses)
   - `demo/k8s/` — Customer demo app
6. Patches probe timings + `imagePullPolicy=IfNotPresent` so the slow Spring
   Boot startup (~100 s) doesn't crash-loop.
7. Rolls all locally-built deployments so they pick up freshly-loaded images.
8. Bootstraps Kafka topics (one-shot Job).
9. Declares the SCADA → TMS RabbitMQ queue + binding via management API.
10. Applies Connect, waits for REST API, registers all 7 connectors.
11. Prints final pod listing + access URLs.

Total time on a cold first run: **8–12 minutes** (mostly image builds + base
image downloads + Spring Boot startup). Subsequent runs: **~2 minutes**.

## After it finishes — opening the UIs

`start.sh` prints the port-forward commands. Open separate terminals for
each service you want in the browser:

```bash
# All in pinkline namespace
kubectl -n pinkline port-forward svc/pas-scada-monitor 8080:8080  &
kubectl -n pinkline port-forward svc/pas-scada-demo    8090:8090  &
kubectl -n pinkline port-forward svc/pas-scada-bridge  8085:8085  &
kubectl -n pinkline port-forward svc/kafdrop           9000:9000  &
kubectl -n pinkline port-forward svc/kafka-connect     8083:8083  &

# In scada namespace
kubectl -n scada port-forward svc/scada-api-internal   8091:8091  &
kubectl -n scada port-forward svc/rabbitmq-internal    1883:1883 15672:15672  &

# Artemis console runs on host Docker — already on http://localhost:8161
```

| URL | What you see | Login |
|---|---|---|
| http://localhost:8080 | **Health monitor** — 19-probe live status with audible alarm on DOWN | — |
| http://localhost:8090 | **Customer demo** — live message table + flow diagram | — |
| http://localhost:8091 | **SCADA simulator** — TMS↔SCADA traffic, manual publish, timer controls | — |
| http://localhost:9000 | **Kafdrop** — Kafka topic browser | — |
| http://localhost:8161/console | **Artemis** — TMS broker, browse `SCADA.TMS.Alarms` queue | admin / admin |
| http://localhost:15672 | **RabbitMQ** — exchanges, queues, MQTT plugin status | thiru / password |
| http://localhost:8083/connectors?expand=status | **Kafka Connect REST** — all 7 connectors RUNNING | — |
| http://localhost:8085/actuator/health | **Bridge health** — Spring Boot health endpoint | — |

## Verify forward + reverse paths

```bash
# Forward path: publish a test XML to Artemis TMS.PISInfo
kubectl apply -f test-publish.yaml
# Then watch http://localhost:8091 (SCADA dashboard) → "TMS → SCADA" panel.
# Decoded JSON should appear within ~3 seconds.

# Reverse path: SCADA auto-publishes UpdateAlarm every 10s.
# Browse the destination queue in Artemis console:
#   addresses → SCADA.TMS.Alarms → queues → scada-tms-viewer → More ▾ → Browse
# Message count grows continuously.

# Health monitor counts (should all be UP / 19/19):
curl -s http://localhost:8080/state | jq '[.[] | .current_state] | group_by(.) | map({state: .[0], count: length})'
```

## Audible alarm on outage

The monitor at http://localhost:8080 plays a 3-tone descending alarm
(880→740→600 Hz) every 4 seconds whenever any of the 19 probes goes DOWN.

1. Open http://localhost:8080
2. Click `○ Sound off` (top-right) — single confirmation chirp confirms audio works
3. Trigger a fake outage:
   ```bash
   kubectl -n scada scale deploy/scada-api --replicas=0
   ```
4. After ~45 s (3 × 15 s probe cycles) the `scada-api` and `scada-api-mqtt-link`
   cards turn red, banner flashes red, tab title becomes
   `(2 DOWN) PAS-SCADA`, alarm starts beeping.
5. Restore: `kubectl -n scada scale deploy/scada-api --replicas=1` — alarm
   auto-stops ~30 s later when both probes hit 2 consecutive successes.

> **Browser autoplay rule:** the click in step 2 is required — browsers block
> sound until a user gesture. Once clicked, the setting persists across page
> reloads via `localStorage`.

## Common gotchas

| Symptom | Cause / fix |
|---|---|
| `host.minikube.internal` not reachable from inside the cluster | Minikube driver mismatch. Ensure docker driver: `minikube start --driver=docker`. The `hyperv` and `kvm` drivers don't expose this hostname reliably. |
| Kafka pod `CrashLoopBackOff` with `InconsistentClusterIdException` | After `minikube delete && minikube start`, the on-disk Kafka data dir survives because minikube's hostpath provisioner reuses it. Wipe per `CLIENT-REQUEST.md` (one-shot busybox pod that `rm -rf /data/*` on the `kafka-data` PVC). |
| Bridge stuck in CrashLoopBackOff for ~3 minutes after first apply | Spring Boot + Camel boot takes ~100 s; default probes kill it before it's ready. `start.sh` patches probes to `initialDelaySeconds: 180` — just wait. |
| Demo / monitor still shows old CSS after redeploy | Browser cache. Hard-refresh with **Ctrl+F5** (Cmd+Shift+R on Mac). |
| Port-forward `connection refused` | Pod was rolled and the kubectl port-forward died. Re-run the port-forward command. |
| `(2 DOWN)` for `scada-api*` alarms but you didn't kill anything | Probe interval is now **15 s** (was 30 s). The `scada-api` pod restart in step 7 of `start.sh` triggers a transient DOWN that recovers in ~30 s. Wait it out. |

## Tear down

```bash
kubectl delete ns pinkline scada
docker compose -f $MESSAGING_INFRA/docker-compose.yml down
minikube stop                # keep state — fast restart later
# OR
minikube delete              # free the disk — full re-bootstrap next time
```

## Where to look when something's wrong

```bash
# Pod logs (most common starting point)
kubectl -n pinkline logs -f deploy/pas-scada-bridge
kubectl -n pinkline logs -f deploy/kafka-connect
kubectl -n pinkline logs -f deploy/pas-scada-monitor
kubectl -n scada    logs -f deploy/scada-api

# Connector status (any of the 7)
kubectl -n pinkline run -i --rm vc --image=curlimages/curl:8.10.1 \
  --restart=Never -- curl -sS http://kafka-connect.pinkline:8083/connectors?expand=status \
  | jq '.[] | {name: .status.name, state: .status.connector.state, tasks: [.status.tasks[].state]}'

# Topic offsets (proves data is flowing)
kubectl -n pinkline exec deploy/kafka -- bash -c '
  for t in tms.raw tms.scada.encrypted scada.tms.raw scada.tms.processed; do
    end=$(kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-service:9092 \
          --topic $t --time -1 2>/dev/null | awk -F: "{sum+=\$3} END{print sum}")
    echo "$t: $end records"
  done'

# Artemis-side message count
docker exec artemis sh -c "/var/lib/artemis-instance/bin/artemis queue stat \
  --user admin --password admin --url tcp://localhost:61616" \
  | grep -E 'NAME|TMS|SCADA'
```

## Production deployment (real 2-server setup)

The minikube setup collapses everything onto one cluster. For real deployment
on **Server 1 (TMS)** + **Server 2 (SCADA)** use the base manifests instead
of the `overlay-minikube.yaml`:

- **Server 1:** apply `tms/k8s/` (use `configmap.yaml` not `overlay-minikube.yaml`),
  `connect/k8s/`, `bootstrap/k8s/`. Update `RABBITMQ_HOST` / `MQTT_HOST` in
  `configmap.yaml` to Server 2's IP. Update connector configs in
  `connect/k8s/20-configmap.yaml` to point Artemis at Server 1's local broker
  and RabbitMQ at Server 2's NodePort `30672`.
- **Server 2:** apply `external-scada/k8s/`. Open firewall `30672/tcp` from
  Server 1 IP only.
- **Monitor + demo** can sit on Server 1, Server 2, or a third operator host —
  they only need network access to probe URLs.

See `CLIENT-REQUEST.md` for the full topology diagram.
