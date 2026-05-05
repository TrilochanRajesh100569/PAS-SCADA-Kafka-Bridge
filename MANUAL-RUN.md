# Manual Run — step-by-step with status checks + fixes

Use this when you don't want to run `start.sh` blindly — you bring up
each component, verify it, then move on. Every step has:

1. **Run** — what to execute
2. **Check** — how to confirm it worked
3. **If it fails** — most common errors and the fix

If a step's check fails, **stop and fix it** before moving on. Skipping
a broken step cascades into confusing failures later.

> Companion docs: `FRESH-PC-SETUP.md` (install prerequisites first),
> `START-COMMANDS.md` (per-service one-shot commands),
> `CLIENT-REQUEST.md` (architecture + what client asked for).

---

## ⚠️ Read this first — Windows / fresh-PC notes

These are issues found by walking the steps end-to-end on a fresh
Windows 11 install (validated 2026-05-05). Knowing them up front
saves time:

1. **Run the steps in the order shown (0 → 9).** This file used to have
   the bridge before the SCADA namespace — that order doesn't work
   because the bridge connects to RabbitMQ on startup and crashes if
   it doesn't exist yet. The order has been fixed in this file. Just
   run them top-to-bottom.

2. **Use Git Bash, not WSL bash.** On Windows the default `bash.exe`
   can resolve to `C:\Windows\System32\bash.exe` (the WSL launcher),
   which doesn't see `/d/pinkline/...` from your host disk. Open
   **Git Bash** explicitly (`C:\Program Files\Git\bin\bash.exe` or
   `%LOCALAPPDATA%\Programs\Git\bin\bash.exe`) — or use the PowerShell
   equivalents shown in each step's "If it fails" table.

3. **`jq` is required** (used in Steps 7 and 9). Install with
   `winget install jqlang.jq` or `choco install jq`. `FRESH-PC-SETUP.md`
   doesn't list it yet — install it before you start.

4. **Don't paste multi-line `kubectl patch -p '…'` JSON into PowerShell.**
   PowerShell mangles the JSON across lines and the API server rejects
   it as "the request is invalid". Either run the patch from Git Bash,
   or save the JSON to a file and use `--patch-file <path>`. Each step
   that needs a patch shows both forms.

5. **Antivirus may block `kubectl run --rm <curl pod>`.** Some AVs
   (seen with K7 Total Security flagging spawned PowerShell as
   "Suspicious Program ID 41030") return `EPERM uv_spawn powershell.exe`
   when `kubectl` tries to spawn the curl pod. Step 5 shows a
   `rabbitmqadmin`-based fallback that runs entirely inside the
   RabbitMQ pod and avoids this.

---

## Step 0 · Prerequisites (once per shell session)

### Run — Git Bash
```bash
cd /d/pinkline/PAS-SCADA-Kafka-Bridge
export MESSAGING_INFRA="/d/pinkline/messaging-infra"
minikube start --cpus=4 --memory=6144 --driver=docker
```

### Run — PowerShell equivalent
```powershell
cd D:\pinkline\PAS-SCADA-Kafka-Bridge
$env:MESSAGING_INFRA = "D:\pinkline\messaging-infra"
minikube start --cpus=4 --memory=6144 --driver=docker
```

### Check
```bash
kubectl get nodes        # expect: STATUS = Ready
docker ps                # expect: command works (daemon running)
echo $MESSAGING_INFRA    # expect: path printed, not empty
ls $MESSAGING_INFRA/docker-compose.yml  # expect: file exists
```

### If it fails
| Symptom | Fix |
|---|---|
| `docker: command not found` / daemon error | Start Docker Desktop (whale icon in tray). Wait until it shows "running". |
| `minikube start` warns **"You cannot change the memory/CPUs for an existing minikube cluster"** | Cosmetic — using existing cluster's settings. To apply new sizes: `minikube delete` first, then `minikube start --cpus=4 --memory=6144 --driver=docker`. |
| `minikube start` hangs / errors | `minikube delete` then retry. If still failing, check VT-x/virtualization is enabled in BIOS. |
| `kubectl get nodes` → `Unable to connect` | `minikube status`. If stopped → `minikube start`. If context wrong → `kubectl config use-context minikube`. |
| `kubectl` warning **"version 1.30.x may have incompatibilities with Kubernetes 1.35.x"** | Cosmetic for most operations. To silence: install matching kubectl, or use `minikube kubectl -- <cmd>`. |
| **Default `bash.exe` doesn't recognize `/d/pinkline/...`** (looks for it in WSL filesystem) | You're running WSL bash, not Git Bash. Open **Git Bash** (`C:\Program Files\Git\bin\bash.exe` or `%LOCALAPPDATA%\Programs\Git\bin\bash.exe`) and re-run. Or use the PowerShell form above. |
| `$MESSAGING_INFRA` empty in a new terminal | You opened a new shell — re-run the `export` (Git Bash) or `$env:` (PowerShell) line. |
| Path doesn't exist | You haven't cloned `messaging-infra` yet. Get it from the same place as this repo, place at `/d/pinkline/messaging-infra`. |
| `jq: command not found` (used in Steps 7 and 9) | `winget install jqlang.jq` or `choco install jq`. Restart your shell. |

---

## Step 1 · Artemis (Docker on host)

### Run
```bash
docker compose -f $MESSAGING_INFRA/docker-compose.yml up -d
```

### Check
```bash
docker ps --filter name=artemis
# expect: STATUS = Up (healthy or starting)

curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8161
# expect: 200, 302, or 303
```
Open http://localhost:8161/console — login `admin` / `admin`. Should load.

### If it fails
| Symptom | Fix |
|---|---|
| Port 8161 / 61616 already in use | Old Artemis still running. `docker ps -a --filter name=artemis` → `docker rm -f artemis`. Or another app is bound to that port. **Common on Windows: a `rail-artemis` (or similar) container from a different project — `docker stop rail-artemis`.** Check with `docker ps --format '{{.Names}}\t{{.Ports}}'` or `netstat -ano \| findstr 8161`. |
| Container says "unhealthy" but console works | Cosmetic — broker is fine. Move on. |
| Console returns nothing / refused | `docker logs artemis --tail 50` — usually a port conflict. Free the port and retry. |
| **PowerShell prints "RemoteException / NativeCommandError" during `docker compose up`** | Cosmetic. Docker writes layer-pull progress to stderr and PowerShell flags any stderr as an error record. The container still starts — verify with `docker ps --filter name=artemis`. |
| Compose file not found | `$MESSAGING_INFRA` wrong. Verify with `ls $MESSAGING_INFRA` (PowerShell: `Test-Path $env:MESSAGING_INFRA\docker-compose.yml`). |

---

## Step 2 · Build + load images into minikube

> **First-run cost: 10–20 min.** The bridge is a Maven build (~5 min)
> and `connect` pulls a ~900 MB Confluent base image. Subsequent runs
> use Docker's layer cache and finish in 1–2 min.

### Run
```bash
docker build -t pinkline/pas-scada-bridge:latest tms/
docker build -t pinkline/pas-scada-connect:latest connect/
docker build -t ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest external-scada/scada-api/
docker build -t pinkline/pas-scada-monitor:latest monitor/
docker build -t pinkline/pas-scada-demo:1.0.0 demo/

for img in pinkline/pas-scada-bridge:latest pinkline/pas-scada-connect:latest \
           ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest \
           pinkline/pas-scada-monitor:latest pinkline/pas-scada-demo:1.0.0; do
  minikube ssh -- "docker rmi -f $img" 2>/dev/null
  minikube image load $img
done
```

### Check
```bash
minikube ssh -- "docker images" | grep -E "pas-scada"
# expect: 5 images (bridge, connect, scada-api, monitor, demo)
```

### If it fails
| Symptom | Fix |
|---|---|
| Bridge build fails: `mvn` errors / Java version | Need Java 17 + Maven on host, OR use the Docker-based build (the bridge Dockerfile already builds inside Docker — make sure your Docker has internet for Maven downloads on first run). |
| `pull access denied` on a base image | Docker Hub rate limit or no internet. `docker login`, retry, or wait. |
| `minikube image load` very slow (~2 min/image) | **Normal.** It saves the image as a tarball, streams over SSH into the minikube VM, and loads it. The Confluent `connect` image is ~900 MB and takes the longest. |
| Old code still running in pod after rebuild | The `docker rmi -f` line inside the loop didn't take. Run it manually: `minikube ssh -- "docker rmi -f <image>"`, then re-load and `kubectl rollout restart deploy/<name>`. |
| `no space left on device` in minikube | `minikube ssh -- "docker system prune -af"`. If still full, `minikube delete` and start over. |

---

## Step 3 · Namespace + Zookeeper + Kafka + Kafdrop

### Run
```bash
kubectl apply -f tms/k8s/00-namespace.yaml
kubectl apply -f tms/k8s/20-zookeeper.yaml
kubectl apply -f tms/k8s/30-kafka.yaml
kubectl apply -f tms/k8s/40-kafdrop.yaml
```

### Check (give it 60–90s)
```bash
kubectl -n pinkline get pods
# expect: zookeeper, kafka, kafdrop all 1/1 Running

kubectl -n pinkline wait --for=condition=ready pod -l app=kafka --timeout=180s
# expect: condition met
```

### If it fails
| Symptom | Fix |
|---|---|
| Kafka `CrashLoopBackOff` with `InconsistentClusterIdException` | Stale PVC data. Run: `kubectl -n pinkline scale deploy kafka --replicas=0` then `minikube ssh -- "sudo rm -rf /tmp/hostpath-provisioner/pinkline/kafka-data/*"` then `kubectl -n pinkline scale deploy kafka --replicas=1`. |
| Kafka `CrashLoopBackOff` complaining about Zookeeper | Zookeeper not ready yet. `kubectl -n pinkline logs deploy/zookeeper --tail 50`. Wait, or restart Kafka after ZK is up: `kubectl -n pinkline rollout restart deploy/kafka`. |
| All pods `Pending` | minikube out of resources. `kubectl -n pinkline describe pod <name>` will say `Insufficient cpu/memory`. Increase: `minikube stop && minikube start --cpus=6 --memory=8192`. |
| `ImagePullBackOff` on Confluent images | No internet from minikube. Test: `minikube ssh -- "curl -s https://hub.docker.com"`. Fix DNS / firewall. |
| Pods stuck `ContainerCreating` for 3+ min | Probably still pulling Confluent images (~750 MB each). `kubectl -n pinkline describe pod <name>` will show `Pulling image`. Wait. |

---

## Step 4 · Bootstrap Kafka topics

### Run
```bash
kubectl -n pinkline delete job bootstrap-kafka-topics --ignore-not-found
kubectl apply -f bootstrap/k8s/10-kafka-topics-job.yaml
kubectl -n pinkline wait --for=condition=complete job/bootstrap-kafka-topics --timeout=120s
```

### Check
```bash
kubectl -n pinkline exec deploy/kafka -- kafka-topics \
  --bootstrap-server kafka-service:9092 --list
# expect: tms.raw, tms.scada.encrypted, scada.tms.raw, scada.tms.processed, plus internals
```

### If it fails
| Symptom | Fix |
|---|---|
| Job never completes | Kafka isn't ready. Re-check Step 3. Then delete + reapply this job. |
| Topics list missing one of the four | Re-run the job: `kubectl -n pinkline delete job bootstrap-kafka-topics; kubectl apply -f bootstrap/k8s/10-kafka-topics-job.yaml`. |

---

## Step 5 · SCADA namespace (RabbitMQ + scada-api)

> **Why this comes before the bridge:** the bridge connects to
> `rabbitmq-internal.scada.svc.cluster.local` on startup. If RabbitMQ
> doesn't exist yet, the bridge throws `UnknownHostException`, fails
> its health probe, and `CrashLoopBackOff`s. Bring SCADA up first.

### Run
```bash
for f in 00-namespace.yaml 10-rabbitmq-configmap.yaml 20-rabbitmq-secret.yaml \
         30-rabbitmq-pvc.yaml 40-rabbitmq-deployment.yaml 50-rabbitmq-service.yaml \
         60-scada-api-secret.yaml 70-scada-api-deployment.yaml; do
  kubectl apply -f external-scada/k8s/$f
done

# Patch scada-api imagePullPolicy so it uses the locally-loaded image
kubectl -n scada patch deploy scada-api --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'

kubectl -n scada wait --for=condition=ready pod -l app=rabbitmq --timeout=180s
```

**PowerShell users — the inline `-p` JSON above will fail.** Save it to a file:
```powershell
'[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' | Out-File -Encoding ascii $env:TEMP\scada-api-patch.json
kubectl -n scada patch deploy scada-api --type=json --patch-file $env:TEMP\scada-api-patch.json
```

Declare the alarm queue + binding (use **one** of the two methods below):

**Method A — `rabbitmqadmin` inside the pod (recommended; works everywhere):**
```bash
kubectl -n scada exec deploy/rabbitmq -- rabbitmqadmin \
  --username=thiru --password=password \
  declare queue name=scada.tms.alarms.queue durable=true auto_delete=false

kubectl -n scada exec deploy/rabbitmq -- rabbitmqadmin \
  --username=thiru --password=password \
  declare binding source=amq.topic destination=scada.tms.alarms.queue routing_key=scada.tms.alarms
```

**Method B — curl pod (Git Bash only; may be blocked by AV):**
```bash
kubectl -n scada run rmq-bind-$$ --rm -i --restart=Never \
  --image=curlimages/curl:8.10.1 -- sh -c '
    AUTH="-u thiru:password"
    BASE="http://rabbitmq-internal:15672/api"
    curl -fsS $AUTH -X PUT -H "Content-Type: application/json" \
      --data "{\"durable\":true,\"auto_delete\":false}" \
      "$BASE/queues/%2F/scada.tms.alarms.queue" >/dev/null
    curl -fsS $AUTH -X POST -H "Content-Type: application/json" \
      --data "{\"routing_key\":\"scada.tms.alarms\"}" \
      "$BASE/bindings/%2F/e/amq.topic/q/scada.tms.alarms.queue" >/dev/null
    echo OK'
```

Wait for scada-api:
```bash
kubectl -n scada rollout status deploy/scada-api --timeout=120s
```

### Check
```bash
kubectl -n scada get pods
# expect: rabbitmq + scada-api both 1/1 Running

kubectl -n scada port-forward svc/rabbitmq-internal 15672:15672 1883:1883 &
kubectl -n scada port-forward svc/scada-api-internal 8091:8091 &

curl -s localhost:8091/api/status
# expect: JSON; mqtt_connected = true
```
Open http://localhost:15672 (thiru/password) → Queues tab → `scada.tms.alarms.queue` should be listed.

### If it fails
| Symptom | Fix |
|---|---|
| RabbitMQ pod stuck Pending | PVC issue. `kubectl -n scada describe pvc`. Often: `minikube addons enable storage-provisioner`. |
| **RabbitMQ pod restarts once with "Liveness probe failed: rabbitmq-diagnostics -q ping timed out after 15s"** | Cosmetic on slow PCs — RabbitMQ takes >15s to become diagnosable on first boot. Pod stabilizes after one restart. Move on if it ends up `1/1 Running`. |
| scada-api logs: MQTT auth failure (`rc=4`) | Wrong creds. Confirm `external-scada/k8s/60-scada-api-secret.yaml` has `MQTT_USER=thiru` / `MQTT_PASS=password` (NOT `admin/admin`). Reapply, restart deploy. |
| `mqtt_connected: false` in `/api/status` | RabbitMQ MQTT plugin not enabled. `kubectl -n scada exec deploy/rabbitmq -- rabbitmq-plugins list \| grep mqtt`. Should show `[E*]`. If not: `kubectl -n scada exec deploy/rabbitmq -- rabbitmq-plugins enable rabbitmq_mqtt`. |
| **Method B curl pod errors with `EPERM uv_spawn powershell.exe`** | Antivirus blocked it (seen with K7 Total Security flagging the spawned PS as "Suspicious Program ID 41030"). Use Method A (`rabbitmqadmin` inside the pod) instead — it runs entirely inside Kubernetes and avoids spawning a host process. |
| Queue declare command errors (`getaddrinfo` / connection refused) | RabbitMQ management API not ready. Wait 20s, retry. |
| `kubectl patch` fails with **"the request is invalid: the server rejected our request"** | PowerShell mangled the inline `-p '...'` JSON. Use the `--patch-file` form shown above, or run from Git Bash. |
| `/api/status` says connected but `/api/received` empty | tms-rabbitmq-sink not running OR queue binding missing. Check Step 7 connector status + RabbitMQ Queues tab. |

---

## Step 6 · Bridge (Spring Boot — slow boot, be patient)

### Run
```bash
kubectl apply -f tms/k8s/overlay-minikube.yaml
kubectl apply -f tms/k8s/deployment.yaml

kubectl -n pinkline patch deploy pas-scada-bridge --type=json -p='[
  {"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"},
  {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/initialDelaySeconds","value":180},
  {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/failureThreshold","value":5},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/initialDelaySeconds","value":120},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/failureThreshold","value":10}]'

kubectl -n pinkline rollout status deploy/pas-scada-bridge --timeout=300s
```

**PowerShell users — save the patch JSON to a file** (the inline form fails):
```powershell
@'
[
  {"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"},
  {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/initialDelaySeconds","value":180},
  {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/failureThreshold","value":5},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/initialDelaySeconds","value":120},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/failureThreshold","value":10}
]
'@ | Out-File -Encoding ascii $env:TEMP\bridge-patch.json
kubectl -n pinkline patch deploy pas-scada-bridge --type=json --patch-file $env:TEMP\bridge-patch.json
kubectl -n pinkline rollout status deploy/pas-scada-bridge --timeout=300s
```

### Check (~3 min cold start)
```bash
kubectl -n pinkline get pods -l app=pas-scada-bridge
# expect: 1/1 Running

kubectl -n pinkline port-forward svc/pas-scada-bridge 8085:8085 &
curl -s localhost:8085/actuator/health
# expect: {"status":"UP"}
```

### If it fails
| Symptom | Fix |
|---|---|
| Pod `0/1 Running` for 2–3 min | **Normal.** Spring Boot + Camel takes ~100s. Wait. |
| `CrashLoopBackOff`, logs show **`UnknownHostException: rabbitmq-internal.scada.svc.cluster.local`** | You skipped Step 5. Run Step 5 first, then `kubectl -n pinkline rollout restart deploy/pas-scada-bridge`. |
| `CrashLoopBackOff`, logs show "liveness probe failed" | The patch above didn't apply. Re-run the `kubectl patch` command. Confirm: `kubectl -n pinkline get deploy pas-scada-bridge -o yaml \| grep -A2 livenessProbe`. |
| **`kubectl patch` fails with "the request is invalid: the server rejected our request"** | PowerShell mangled the inline `-p '...'` JSON. Use the `--patch-file` form shown above, or run from Git Bash. |
| `CrashLoopBackOff`, logs show Artemis connection error | Artemis on host isn't reachable from minikube. Test: `minikube ssh -- "nc -vz host.minikube.internal 61616"`. If fails → Docker Desktop network glitch, restart Docker Desktop. |
| `ImagePullBackOff` | Image wasn't loaded. Re-do Step 2 for `pas-scada-bridge`. |
| Health says `OUT_OF_SERVICE` | Logs: `kubectl -n pinkline logs deploy/pas-scada-bridge --tail 100`. Usually Artemis or Kafka unreachable. |

---

## Step 7 · Kafka Connect + register all 7 connectors

### Run
```bash
kubectl apply -f connect/k8s/10-secret.yaml
kubectl apply -f connect/k8s/20-configmap.yaml
kubectl apply -f connect/k8s/30-deployment.yaml
kubectl -n pinkline rollout status deploy/kafka-connect --timeout=300s

kubectl -n pinkline delete job register-connectors --ignore-not-found
kubectl apply -f connect/k8s/40-job-register.yaml
kubectl -n pinkline wait --for=condition=complete job/register-connectors --timeout=120s
```

### Check
```bash
kubectl -n pinkline port-forward svc/kafka-connect 8083:8083 &
curl -s localhost:8083/connectors
# expect 7 names:
#   tms-artemis-source, tms-artemis-source-trafficreport,
#   tms-artemis-source-tsinfo, tms-artemis-source-routeinfo,
#   tms-rabbitmq-sink, scada-rabbitmq-source, scada-artemis-sink

curl -s "localhost:8083/connectors?expand=status" \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state}'
# expect: every state = "RUNNING"
```

### If it fails
| Symptom | Fix |
|---|---|
| Connect pod `CrashLoopBackOff` | Logs: `kubectl -n pinkline logs deploy/kafka-connect --tail 100`. Usually Kafka not ready — re-check Step 3. |
| Only 4 connectors registered (instead of 7) | Old configmap. `kubectl apply -f connect/k8s/20-configmap.yaml`, then re-run register job. |
| Connector state = `FAILED` | `curl -s localhost:8083/connectors/<name>/status \| jq` for the trace. Common: Artemis/RabbitMQ creds wrong, or RabbitMQ queue missing (re-do Step 5 then `restart`: `curl -X POST localhost:8083/connectors/<name>/restart`). |
| `scada-rabbitmq-source` produces literal `[B@<hash>` | Old configmap with `StringConverter`. Confirm `value.converter: ByteArrayConverter` in `connect/k8s/20-configmap.yaml`, reapply, restart connector. |
| Register job completes but no connectors | Check job logs: `kubectl -n pinkline logs job/register-connectors`. Usually Connect REST not ready — delete job + reapply after 30s. |

---

## Step 8 · Monitor + Demo (optional but client-requested)

### Run
```bash
kubectl apply -f monitor/k8s/30-pvc.yaml
kubectl apply -f monitor/k8s/20-secret.yaml
kubectl apply -f monitor/k8s/overlay-minikube.yaml
kubectl apply -f monitor/k8s/40-deployment.yaml
kubectl -n pinkline patch deploy pas-scada-monitor --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'
kubectl -n pinkline rollout status deploy/pas-scada-monitor --timeout=120s

kubectl apply -f demo/k8s/10-configmap.yaml
kubectl apply -f demo/k8s/20-deployment.yaml
kubectl -n pinkline patch deploy pas-scada-demo --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'
kubectl -n pinkline rollout status deploy/pas-scada-demo --timeout=120s

kubectl -n pinkline port-forward svc/pas-scada-monitor 8080:8080 &
kubectl -n pinkline port-forward svc/pas-scada-demo    8090:8090 &
```

**PowerShell users — same `--patch-file` workaround as Step 5/6** for the
two `kubectl patch` lines (the inline `-p` JSON will fail).

### Check
```bash
curl -s localhost:8080/healthz
# expect: {"status":"ok"}

curl -s -o /dev/null -w "%{http_code}\n" localhost:8090
# expect: 200
```
Open http://localhost:8080 — after ~30s the dashboard should show 19 components mostly green.

### If it fails
| Symptom | Fix |
|---|---|
| Monitor shows everything DOWN | Port-forwards from earlier steps died. Re-run Step 5 / 6 / 7 port-forward commands. The monitor probes `localhost`-ish targets via in-cluster service names — pod-to-pod traffic, not host port-forwards. So if it's red but the actual services are up, restart the monitor deploy: `kubectl -n pinkline rollout restart deploy/pas-scada-monitor`. |
| Demo shows old data / old UI | Rogue host docker container shadowing the kubectl port-forward. `docker ps \| grep -E "pas-scada-(api\|monitor\|demo)"`. Kill any matches: `docker rm -f <name>`. |

---

## Step 9 · End-to-end data flow test

### Forward (TMS → SCADA, encrypted)
```bash
kubectl apply -f test-publish.yaml
sleep 5
curl -s localhost:8091/api/received | jq '.[0].decoded'
# expect: {"messageType":"TMS_PAS_UPDATE", ...}
```

### Reverse (SCADA → TMS, plain JSON) — SCADA auto-publishes alarms
```bash
kubectl exec -n pinkline deploy/kafka -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 --topic scada.tms.processed \
  --partition 2 --offset earliest --max-messages 1 --timeout-ms 8000
# expect: {"CreatorId":"ScateX","Type":"UpdateAlarm",...}

docker exec artemis sh -c "/var/lib/artemis-instance/bin/artemis queue stat \
  --user admin --password admin --url tcp://localhost:61616" | grep SCADA.TMS.Alarms
# expect: MESSAGE_COUNT > 0 and growing
```

### If it fails
| Symptom | Fix |
|---|---|
| Forward: `/api/received` empty | (a) `tms-artemis-source` not RUNNING (Step 7), (b) `tms-rabbitmq-sink` not RUNNING, (c) RabbitMQ binding missing (Step 5), (d) bridge not encrypting → check bridge logs for "EncryptProcessor". |
| Forward: arrives but stays as XML / undecoded | Bridge transform not running. `kubectl -n pinkline logs deploy/pas-scada-bridge \| grep -i error`. |
| Reverse: `scada.tms.processed` empty | `scada-rabbitmq-source` failed (most common: ByteArrayConverter not set) or scada-api not publishing (`/api/status` mqtt_connected=false). |
| Reverse: Kafka has data but Artemis count = 0 | `scada-artemis-sink` failed. `curl -s localhost:8083/connectors/scada-artemis-sink/status`. Often Artemis creds wrong. |

---

## Quick troubleshooting reference

### Show me everything's state at once
```bash
kubectl -n pinkline get pods
kubectl -n scada get pods
docker ps --filter name=artemis
curl -s localhost:8083/connectors?expand=status \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state}'
curl -s localhost:8085/actuator/health
curl -s localhost:8091/api/status
```

### Restart one thing without redoing everything
```bash
# Bridge after Java change
docker build -t pinkline/pas-scada-bridge:latest tms/
minikube ssh -- "docker rmi -f pinkline/pas-scada-bridge:latest"
minikube image load pinkline/pas-scada-bridge:latest
kubectl -n pinkline rollout restart deploy/pas-scada-bridge

# scada-api after app.py / dashboard.html change
docker build -t ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest external-scada/scada-api/
minikube ssh -- "docker rmi -f ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest"
minikube image load ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest
kubectl -n scada rollout restart deploy/scada-api

# Connector config change (no rebuild needed)
kubectl apply -f connect/k8s/20-configmap.yaml
kubectl -n pinkline delete job register-connectors --ignore-not-found
kubectl apply -f connect/k8s/40-job-register.yaml
```

### Dead Letter Queue (DLQ) — when messages fail

Every connector is configured with `errors.tolerance=all` + a DLQ topic, so
a single bad message never stops the pipeline — it gets routed to a
`dlq.connect.<connector-name>` Kafka topic instead. Configured in
`connect/k8s/20-configmap.yaml`:

| Source connector | DLQ topic |
|---|---|
| `tms-artemis-source` (+ `-trafficreport`, `-tsinfo`, `-routeinfo`) | `dlq.connect.tms-artemis-source` |
| `tms-rabbitmq-sink` | `dlq.connect.tms-rabbitmq-sink` |
| `scada-rabbitmq-source` | `dlq.connect.scada-rabbitmq-source` |
| `scada-artemis-sink` | `dlq.connect.scada-artemis-sink` |

Retry behaviour: `errors.retry.timeout=600000` (10 min) with backoff up to
30 s. Only after 10 min of retries does a message go to DLQ.

#### Check if any DLQ has messages

```bash
# Quick: list DLQ topics + sizes
kubectl -n pinkline exec deploy/kafka -- bash -c "
  for t in dlq.connect.tms-artemis-source dlq.connect.tms-rabbitmq-sink \
           dlq.connect.scada-rabbitmq-source dlq.connect.scada-artemis-sink; do
    echo -n \"\$t: \"
    kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list kafka-service:9092 --topic \$t 2>/dev/null \
      | awk -F: '{s+=\$3} END{print s+0}'
  done"
# expect: all four = 0 in a healthy system
```

Or open Kafdrop http://localhost:9000 → look for `dlq.connect.*` topics.
Non-zero message count = something failed.

#### Read what's in the DLQ

```bash
kubectl -n pinkline exec deploy/kafka -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic dlq.connect.tms-artemis-source \
  --from-beginning --max-messages 5 \
  --property print.headers=true --property print.value=true --timeout-ms 5000
```
The headers (`__connect.errors.*`) tell you why it failed — exception
class, stack trace, original topic, original partition.

#### Common reasons messages land in DLQ

| Cause | Fix |
|---|---|
| Schema/converter mismatch (e.g. `StringConverter` reading `byte[]`) | Fix `value.converter` in configmap, reapply, restart connector. |
| Encrypted payload but decryption disabled (or vice-versa) | Verify `BRIDGE_REVERSE_KAFKA_ENCRYPT_ENABLED` matches what the publisher does. |
| Downstream broker (Artemis / RabbitMQ) unreachable for >10 min | Restore connectivity, then **replay** (see below). |
| Malformed payload from upstream | Inspect, fix upstream, then either replay or accept the loss. |

#### Replay messages from DLQ back into the original topic

There's no auto-replay. Manual procedure:
```bash
# 1. Dump DLQ to a file on the kafka pod
kubectl -n pinkline exec deploy/kafka -- bash -c "
  kafka-console-consumer --bootstrap-server kafka-service:9092 \
    --topic dlq.connect.tms-artemis-source \
    --from-beginning --timeout-ms 5000 > /tmp/replay.txt"

# 2. Pipe back into the original topic (after you've fixed the root cause)
kubectl -n pinkline exec deploy/kafka -- bash -c "
  cat /tmp/replay.txt | kafka-console-producer \
    --bootstrap-server kafka-service:9092 --topic tms.raw"

# 3. Once confirmed processed, purge the DLQ
kubectl -n pinkline exec deploy/kafka -- kafka-topics \
  --bootstrap-server kafka-service:9092 --delete \
  --topic dlq.connect.tms-artemis-source
# (will auto-recreate next time a message fails)
```

#### Restart a connector after fixing the root cause

If a connector hit `FAILED` state (vs just dead-lettering individual
records), restart it instead of rebuilding everything:
```bash
curl -X POST localhost:8083/connectors/<connector-name>/restart
curl -s localhost:8083/connectors/<connector-name>/status | jq
# expect: state = RUNNING, all tasks RUNNING
```

---

### Stop / wipe
```bash
# Stop one component, keep state
kubectl -n pinkline scale deploy <name> --replicas=0

# Stop everything, keep state
docker compose -f $MESSAGING_INFRA/docker-compose.yml down
minikube stop

# Wipe everything
kubectl delete ns pinkline scada
docker compose -f $MESSAGING_INFRA/docker-compose.yml down
minikube delete
```

---

## URL master list

| URL | What | Login |
|---|---|---|
| http://localhost:8161/console | Artemis | admin/admin |
| http://localhost:8085/actuator/health | Bridge health | — |
| http://localhost:8085/api/messages | Bridge in-app monitor | — |
| http://localhost:9000 | Kafdrop | — |
| http://localhost:8083/connectors?expand=status | Connect REST | — |
| http://localhost:8091 | SCADA Simulator | — |
| http://localhost:15672 | RabbitMQ admin | thiru/password |
| http://localhost:8080 | Health monitor | — |
| http://localhost:8090 | Demo (data table) | — |
| http://localhost:8090/flow | Demo (flow diagram) | — |
