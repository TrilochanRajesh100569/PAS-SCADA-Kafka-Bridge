# Demo Scenarios — Failure, Alarm, Auto-recovery, Zero-loss

Live-demo runbook for the TMS ↔ SCADA bridge. Designed to walk a
client through **every component** and show:

1. **Stop it** — the audible alarm fires from the Health Monitor
2. **Auto-create** — Kubernetes rebuilds the pod automatically
3. **Alarm clears** — monitor recovers, sound stops
4. **Zero data loss** — every message that arrived is still in the system

5 single-component scenarios + 3 combo scenarios + 1 real-world scenario,
~25 minutes total. Each scenario is ~2-3 minutes.

> **Companion docs (in `docs/`):**
> - `WORKFLOW.md` — how data flows between components
> - `MANUAL-RUN.md` §9 → "Dead Letter Queue (DLQ)" — operational runbook
> - `MORNING-START.md` — daily restart routine

---

## Pre-flight (5 minutes before the demo)

### 1 · Stack health check

```bash
kubectl -n pinkline get pods
kubectl -n scada    get pods
docker ps --filter name=artemis
```

Every pod must be `1/1 Running`. Artemis container `Up`. If anything is
wrong, fix it before going on stage (`docs/MORNING-START.md`).

### 2 · All port-forwards live

```bash
for p in 8080 8083 8085 8090 8091 9000 15672 8161; do
  echo -n "$p: "; curl -s -o /dev/null -w "%{http_code}\n" http://localhost:$p
done
```

All must return `200` (or `302` for Artemis console / `401` for kafka-connect REST).

### 3 · Open these tabs in your browser, in this order

| Tab | URL | Purpose |
|---|---|---|
| 1 | http://localhost:8080 | **Health Monitor** — the star of the demo |
| 2 | http://localhost:8091 | SCADA dashboard — message flow visualisation |
| 3 | http://localhost:9000 | Kafdrop — Kafka topic counts |
| 4 | http://localhost:8161/console | Artemis console (admin/admin) |
| 5 | http://localhost:15672 | RabbitMQ admin (thiru/password) |

### 4 · Arm the alarm — CRITICAL FIRST STEP

On the **Health Monitor tab** (http://localhost:8080):

1. Find the **"Sound off"** pill in the top-right corner
2. **Click it once** — it turns green and reads **"Sound on"**
3. You'll hear a single confirmation beep (660 Hz)

> Browsers block audio autoplay. The arming click is required. Without
> it, the alarm cannot make sound. **Do not skip this.**

The armed state persists in localStorage — won't reset between scenarios.

### 5 · Side-screen monitor (recommended)

Second terminal, runs the whole demo:

```bash
watch -n 3 '
echo "=== POD STATE ==="
kubectl -n pinkline get pods --no-headers | awk "{printf \"%-30s %s\n\", \$1, \$3}"
kubectl -n scada    get pods --no-headers | awk "{printf \"%-30s %s\n\", \$1, \$3}"
echo
echo "=== MAIN TOPIC COUNTS ==="
kubectl -n pinkline exec deploy/kafka -- bash -c "
for t in tms.raw tms.scada.encrypted scada.tms.raw scada.tms.processed; do
  echo -n \"\$t: \"
  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-service:9092 --topic \$t 2>/dev/null | awk -F: \"{s+=\\\$3} END{print s+0}\"
done"
echo
echo "=== DLQ COUNTS ==="
kubectl -n pinkline exec deploy/kafka -- bash -c "
for t in dlq.connect.tms-artemis-source dlq.connect.tms-rabbitmq-sink dlq.connect.scada-rabbitmq-source dlq.connect.scada-artemis-sink; do
  echo -n \"\$t: \"
  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-service:9092 --topic \$t 2>/dev/null | awk -F: \"{s+=\\\$3} END{print s+0}\"
done"
'
```

Topic counts move live as you trigger scenarios — very visual proof of
no data loss.

### 6 · Save the existing 227-message DLQ backup

```bash
kubectl -n pinkline exec deploy/kafka -- bash -c "
  kafka-console-consumer --bootstrap-server kafka-service:9092 \
    --topic dlq.connect.scada-artemis-sink --from-beginning --timeout-ms 8000" \
  > demo-dlq-backup.txt
wc -l demo-dlq-backup.txt
```

This snapshot is your replay material for Scenario 9 (the real production incident).

---

## Component → behaviour matrix (read this once, refer back during demo)

| Component | Stop method | Auto-recovers? | Alarm fires? (which checks) | Data at risk? | Recovery time |
|---|---|---|---|---|---|
| **pas-scada-bridge** | `scale --replicas=0` | ✅ k8s rebuilds | ✅ `pas-scada-bridge` | ❌ none — Artemis durable | ~90 s |
| **kafka** | `scale --replicas=0` | ✅ k8s + PVC | ✅ `kafka` (+ cascading: bridge, connect) | ❌ none — PVC persists | ~60 s |
| **zookeeper** | `scale --replicas=0` | ✅ k8s + PVC | ✅ `zookeeper`, then kafka | ❌ none | ~60 s |
| **kafka-connect** | `scale --replicas=0` | ✅ k8s | ⚠ no direct probe — connectors stop | ❌ none — Kafka offsets retained | ~90 s |
| **rabbitmq** | `scale --replicas=0` | ✅ k8s + PVC | ✅ 3 checks: `rabbitmq-mgmt`, `rabbitmq-amqp`, `rabbitmq-mqtt` | ❌ none for 10 min, then DLQ | ~60 s |
| **scada-api** | `scale --replicas=0` | ✅ k8s | ✅ `scada-api` + `scada-api-mqtt` | ❌ none | ~30 s |
| **kafdrop** | `delete pod` | ✅ k8s | ❌ no probe — UI only | ❌ none — UI only | ~20 s |
| **pas-scada-monitor** | `delete pod` | ✅ k8s | special: dashboard goes offline | ❌ none — state on PVC | ~20 s |
| **pas-scada-demo** | `delete pod` | ✅ k8s | ❌ no probe — UI only | ❌ none — UI only | ~20 s |
| **Artemis (host docker)** | `docker stop artemis` | ❌ manual | ✅ `artemis-openwire` + `artemis-console` | ❌ none for 10 min, then DLQ | ~10 s after restart |
| **minikube** | `minikube stop` | ❌ manual | ✅ everything red | ❌ none — PVCs persist | ~90 s after restart |

---

# Scenario 1 · Bridge crash + recovery

**Length:** 2 min · **What it proves:** pod-level resilience, no data loss across a Spring Boot restart.

**Say:** "The bridge is the central JVM that translates between TMS XML and SCADA JSON. What happens if it crashes?"

```bash
# Stop the bridge
kubectl -n pinkline scale deploy/pas-scada-bridge --replicas=0
```

**Watch on monitor dashboard:**
1. Within 30 seconds, `pas-scada-bridge` tile turns **red (DOWN)**
2. **Alarm sounds** — three descending tones, repeating every 4 s
3. Side-screen shows `pas-scada-bridge   0/1`

**Bring back:**
```bash
kubectl -n pinkline scale deploy/pas-scada-bridge --replicas=1
kubectl -n pinkline rollout status deploy/pas-scada-bridge --timeout=180s
```

**Watch:**
1. Pod becomes `1/1 Running` (~90 s — Spring Boot cold start)
2. Monitor tile turns **green (UP)**
3. **Alarm stops automatically** — silence
4. Side-screen: `tms.scada.encrypted` topic count **incremented** during the outage (TMS messages buffered in Artemis, drained on bridge reconnect)

**Closing line:** "Pod died, k8s built a new one, Artemis held the messages, every one was processed. Zero data loss."

---

# Scenario 2 · Kafka crash + recovery (the central nervous system)

**Length:** 2-3 min · **What it proves:** brokers survive restarts; PVC keeps every byte.

**Say:** "Kafka is the durable backbone of the pipeline — every message is persisted here for 7 days. What if it dies?"

```bash
kubectl -n pinkline scale deploy/kafka --replicas=0
```

**Watch:**
1. Within 30 s, monitor goes red across **multiple tiles** — `kafka`, then `pas-scada-bridge` (consumes from Kafka), then connectors fail
2. **Alarm sounds**
3. Side-screen: every `pinkline` consumer pod starts logging connection errors (don't show logs unless asked — keep it visual)

**Bring back:**
```bash
kubectl -n pinkline scale deploy/kafka --replicas=1
kubectl -n pinkline wait --for=condition=ready pod -l app=kafka --timeout=120s
```

**Watch:**
1. Kafka comes up, pulls cluster ID from PVC
2. Bridge auto-reconnects (Spring's Camel auto-retry)
3. Connectors auto-resume (Connect framework handles it)
4. All tiles green, alarm clears
5. Topic counts **identical** to pre-outage snapshot (no messages were even produced during the gap, since producers blocked)

**Closing line:** "The PVC remembered every offset. The clients remembered where they were. The system rebuilt itself."

---

# Scenario 3 · RabbitMQ crash + recovery (3 alarms at once)

**Length:** 2-3 min · **What it proves:** the monitor flags every protocol the broker exposes.

**Say:** "RabbitMQ exposes management HTTP, AMQP, and MQTT. The monitor watches all three independently."

```bash
kubectl -n scada scale deploy/rabbitmq --replicas=0
```

**Watch:**
1. Within 30 s, **THREE tiles turn red**: `rabbitmq-mgmt`, `rabbitmq-amqp`, `rabbitmq-mqtt`
2. **Alarm sounds** — and stays sounding through the whole RabbitMQ outage
3. SCADA dashboard at :8091 shows `mqtt_connected: false`
4. Side-screen: `scada.tms.raw` topic stops growing (no SCADA messages reach Kafka)

**While DOWN, send a few alarms** from the SCADA dashboard's "Send once" button. They'll fail to publish — perfect to demonstrate:

```bash
# Watch the connectors retrying (NOT giving up):
kubectl -n pinkline logs deploy/kafka-connect --tail 30 | grep -iE "rabbit|retry"
```

**Bring back:**
```bash
kubectl -n scada scale deploy/rabbitmq --replicas=1
kubectl -n scada wait --for=condition=ready pod -l app=rabbitmq --timeout=180s
```

**Watch:**
1. All three rabbitmq-* tiles turn green
2. Alarm clears
3. Pending SCADA messages flow through to Kafka — `scada.tms.raw` count jumps
4. **Important:** alarms sent during outage now appear in the dashboard `sent` counter

**Closing line:** "Three protocols, three independent health checks, all of them tracked, all of them recovered. And every message we tried to send during the outage was delivered after recovery."

---

# Scenario 4 · scada-api crash + recovery

**Length:** 1-2 min · **What it proves:** The SCADA simulator self-heals; in-memory state recovers from k8s state.

**Say:** "The SCADA simulator is what publishes alarms back to TMS. If it dies, what happens?"

```bash
kubectl -n scada scale deploy/scada-api --replicas=0
```

**Watch:**
1. `scada-api` tile + `scada-api-mqtt` tile both red
2. **Alarm sounds**
3. SCADA dashboard tab refuses to load (port-forward target gone)

**Bring back:**
```bash
kubectl -n scada scale deploy/scada-api --replicas=1
kubectl -n scada rollout status deploy/scada-api --timeout=120s
```

**Watch:**
1. ~30 seconds, both tiles green
2. Alarm clears
3. Refresh SCADA dashboard tab — auto-publisher running again

**Caveat:** the SCADA dashboard's port-forward dies when the pod restarts. If it shows "site can't be reached", restart that one port-forward:
```bash
nohup kubectl -n scada port-forward svc/scada-api-internal 8091:8091 >.port-forwards/scada-api.log 2>&1 &
```

**Closing line:** "30 seconds from kill to fully restored. No data lost — the alarm history lives in TMS via Kafka, not in scada-api memory."

---

# Scenario 5 · Artemis crash + manual recovery (host docker)

**Length:** 2 min · **What it proves:** Even a host-level service outside Kubernetes is monitored end-to-end.

**Say:** "Artemis is the central JMS broker that connects to TMS. It runs as a Docker container outside the Kubernetes cluster. The monitor watches it anyway."

```bash
docker stop artemis
```

**Watch:**
1. Within 30 s, **two tiles** red: `artemis-openwire` (port 61616) + `artemis-console` (port 8161)
2. **Alarm sounds**
3. Bridge starts logging Camel connection errors (don't dwell — visual is enough)
4. Side-screen: `tms.raw` topic stops growing if anyone was publishing TMS messages

**Bring back (manual — Artemis isn't k8s-managed):**
```bash
docker start artemis
```

**Watch:**
1. ~10 seconds, both tiles green
2. Alarm clears
3. Bridge reconnects (Camel auto-retry, no human action needed)
4. TMS message flow resumes automatically

**Closing line:** "Even a host-level Docker container outside k8s is monitored. The monitor doesn't care where things live — it cares that they're reachable."

---

# Scenario 6 · Monitor itself dies (the meta scenario)

**Length:** 1-2 min · **What it proves:** Even the watcher is watched (by k8s).

**Say:** "What about the monitor itself? If it dies, who watches the watcher? Answer: Kubernetes."

```bash
kubectl -n pinkline delete pod -l app=pas-scada-monitor
```

**Watch:**
1. Monitor browser tab goes blank / "site can't be reached" within seconds
2. Side-screen: `pas-scada-monitor   0/1   ContainerCreating`
3. ~20 s later: `pas-scada-monitor   1/1   Running`
4. Refresh the monitor tab — **all state restored** (consecutive_passes, last_state_change, etc., all persist on the 100 Mi PVC)
5. Alarm setting also persists (localStorage in the browser)

**Closing line:** "k8s is the safety net for the safety net. State on PVC means even the monitor's memory of what's healthy survives a restart."

---

# Scenario 7 · COMBO — Bridge + RabbitMQ both down

**Length:** 3 min · **What it proves:** Multiple simultaneous failures; cascading alarm; both recoveries.

**Say:** "Real outages rarely happen one component at a time. Watch what a multi-component failure looks like."

```bash
kubectl -n pinkline scale deploy/pas-scada-bridge --replicas=0
kubectl -n scada    scale deploy/rabbitmq        --replicas=0
```

**Watch:**
1. **4 tiles** red: `pas-scada-bridge`, `rabbitmq-mgmt`, `rabbitmq-amqp`, `rabbitmq-mqtt`
2. **Alarm sounds** (continues until LAST tile recovers)
3. The DOWN email (if SMTP configured) lists all 4 in a single alert — **no email spam, one consolidated message**

**Bring back (notice: order matters — RabbitMQ first, then bridge):**
```bash
kubectl -n scada    scale deploy/rabbitmq        --replicas=1
kubectl -n scada    wait --for=condition=ready pod -l app=rabbitmq --timeout=180s
kubectl -n pinkline scale deploy/pas-scada-bridge --replicas=1
kubectl -n pinkline rollout status deploy/pas-scada-bridge --timeout=180s
```

**Watch:**
1. RabbitMQ tiles green first (~30 s), but `pas-scada-bridge` still red — alarm continues
2. Bridge tile green ~90 s later — alarm finally stops
3. RECOVERY email lists each component with its individual downtime ("down 2m 14s")
4. Topic counts on side-screen all consistent — no gaps

**Closing line:** "Recovery is graceful. Alerts are intelligent. We get one email when things break, one email when they recover, with downtime per component."

---

# Scenario 8 · COMBO — Total cluster restart (the big one)

**Length:** 5-7 min · **What it proves:** Everything survives a complete shutdown.

**Say:** "The ultimate test: turn the whole cluster off. Then back on. Did anything break?"

**Snapshot all topic counts BEFORE:**
```bash
kubectl -n pinkline exec deploy/kafka -- bash -c "
for t in tms.raw tms.scada.encrypted scada.tms.raw scada.tms.processed; do
  echo -n \"\$t: \"
  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-service:9092 --topic \$t 2>/dev/null | awk -F: '{s+=\$3} END{print s+0}'
done"
```

**Note these numbers down on a piece of paper.**

**Stop everything:**
```bash
# This persists state to disk
& $env:USERPROFILE\minikube.exe stop      # PowerShell — minikube can't be stopped via Git Bash sometimes
docker stop artemis
```

**Watch:**
1. Every browser tab fails — **screen full of broken tabs**
2. Side-screen monitor: `kubectl: Unable to connect to server`

**(Optional dramatic pause)**

**Bring it all back:**
```powershell
& $env:USERPROFILE\minikube.exe start
docker start artemis
```

Wait ~3 minutes for all pods to settle. Then:

```bash
kubectl -n pinkline get pods
kubectl -n scada    get pods
docker ps --filter name=artemis
```

Re-start the port-forwards (see Pre-flight Step 2). **All browser tabs work again — same data, same counts, same alarms.**

**Snapshot topic counts AFTER:**
```bash
kubectl -n pinkline exec deploy/kafka -- bash -c "
for t in tms.raw tms.scada.encrypted scada.tms.raw scada.tms.processed; do
  echo -n \"\$t: \"
  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-service:9092 --topic \$t 2>/dev/null | awk -F: '{s+=\$3} END{print s+0}'
done"
```

**Show the client:** the AFTER counts ≥ BEFORE counts (they may have grown if anything queued during shutdown). **Not a single message was lost.**

**Closing line:** "The whole cluster restarted from cold. Every single message persisted. Persistent volumes, durable topics, durable JMS. This is what production-grade messaging looks like."

---

# Scenario 9 · Real production incident (the 227 messages)

**Length:** 3 min · **What it proves:** This isn't theoretical — it's already happened.

**Say:** "Earlier today we hit a real bug in this very session. Watch what the system did."

**Show the count:**
```bash
kubectl -n pinkline exec deploy/kafka -- bash -c "
  kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list kafka-service:9092 --topic dlq.connect.scada-artemis-sink 2>/dev/null \
  | awk -F: '{s+=\$3} END{print \"DLQ count:\", s+0}'"
```

**Read one message + its failure headers:**
```bash
kubectl -n pinkline exec deploy/kafka -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic dlq.connect.scada-artemis-sink \
  --from-beginning --max-messages 1 \
  --property print.headers=true --timeout-ms 5000
```

**Point at:**
- `__connect.errors.exception.class.name = ActiveMQSecurityException`
- `__connect.errors.exception.message = AMQ229031: Unable to validate user`
- `__connect.errors.topic` + `partition` + `offset` (exact provenance)

**Replay** (note Artemis `messagesAdded` count BEFORE):

```bash
kubectl -n pinkline exec deploy/kafka -- bash -c "
  kafka-console-consumer --bootstrap-server kafka-service:9092 \
    --topic dlq.connect.scada-artemis-sink --from-beginning --timeout-ms 8000 \
  | kafka-console-producer --bootstrap-server kafka-service:9092 \
    --topic scada.tms.processed"
```

In Artemis console (http://localhost:8161), `SCADA.TMS.Alarms.messagesAdded` jumps by **227**.

**Closing line:** "A bug, a 10-minute retry per message, then dead-lettered with full audit trail. We fixed the bug. We replayed every message with one command. Zero data loss in a real-world fault."

---

# Closing summary card — read this to the client

| What we showed | Where |
|---|---|
| **Audible alarm fires the moment any component fails** | All single-component scenarios |
| **k8s automatically replaces failed pods** | Scenarios 1, 2, 3, 4, 6 |
| **Multiple failures detected and alarmed independently** | Scenarios 3, 7 |
| **Host-level services (outside k8s) also monitored** | Scenario 5 |
| **The monitor itself is watched by k8s** | Scenario 6 |
| **Total cluster restart preserves every byte** | Scenario 8 |
| **Real production incidents auditable + replayable** | Scenario 9 |
| **No alert spam — one email per state transition** | Scenario 7 (consolidated alert) |

---

# Recovery — if things go sideways mid-demo

| Symptom | One-line fix |
|---|---|
| Side-screen monitor frozen | Ctrl + C, re-run watch command |
| `kubectl` returns `TLS handshake timeout` | minikube hung — `docs/MORNING-START.md` "Minikube ran out of RAM" |
| SCADA dashboard shows old UI | Ctrl + F5; if still old → `UI_ONLY=1 ./start.sh` |
| Port-forward died | `pkill -f "kubectl.*port-forward"` then re-run port-forwards from `docs/MORNING-START.md` Step 5 |
| Pod stuck `0/1` after a scenario | `kubectl -n <ns> rollout restart deploy/<name>` |
| Alarm doesn't sound | You forgot Pre-flight Step 4. Click "Sound off" pill on http://localhost:8080. |
| Want to replay 227 DLQ messages but topic was deleted | `cat demo-dlq-backup.txt \| kubectl -n pinkline exec -i deploy/kafka -- kafka-console-producer --bootstrap-server kafka-service:9092 --topic scada.tms.processed` |
| Demo just plain crashed | `./start.sh` re-runs idempotently. ~3 minutes from running stack. |

---

# Post-demo cleanup

```bash
# Optional: clear DLQs for a clean state next time
for t in dlq.connect.tms-artemis-source dlq.connect.tms-rabbitmq-sink \
         dlq.connect.scada-rabbitmq-source dlq.connect.scada-artemis-sink; do
  kubectl -n pinkline exec deploy/kafka -- kafka-topics \
    --bootstrap-server kafka-service:9092 --delete --topic $t
done

# Optional: stop port-forwards
pkill -f "kubectl.*port-forward"

# Optional: stop the cluster (state preserved)
& $env:USERPROFILE\minikube.exe stop
docker stop artemis
```
