# Fresh PC Setup — PAS-SCADA-Kafka-Bridge

Single source of truth for handing this project off to another Windows PC.
Covers first-time bring-up, daily restart, and troubleshooting.

---

## 0 · Folder layout (what you should have received)

You should have received **two folders**. Place them side-by-side under
`D:\pinkline\` (or any directory — just keep them together):

```
D:\pinkline\
  ├── messaging-infra\           ← Artemis docker-compose (separate repo)
  └── PAS-SCADA-Kafka-Bridge\    ← this repo (everything else)
```

> If your folders are at a different path, adjust `MESSAGING_INFRA` in
> Step 3 accordingly.

> **Important:** Artemis runs as a **Docker container on the host** (not
> inside Kubernetes). It will **not** appear in `kubectl get pods -n pinkline`.
> Verify it with `docker ps --filter name=artemis` instead.

---

## 1 · Prerequisites (must be installed first)

| Tool | Why | Verify |
|---|---|---|
| Docker Desktop | Runs Artemis + builds all images | `docker ps` |
| minikube | Local Kubernetes cluster | `minikube version` |
| kubectl | Talks to minikube | `kubectl version --client` |
| Git for Windows (Git Bash) | Runs `start.sh` (bash script) | `bash --version` |
| jq | Used by some troubleshooting commands | `jq --version` (`winget install jqlang.jq`) |
| Java 17 + Maven | Only if rebuilding bridge from source | `mvn -v` |

Docker Desktop resources (Settings → Resources):

- **CPU** ≥ 4
- **Memory** ≥ 8 GB
- **Disk** ≥ 30 GB free

Without these, the Spring Boot bridge will OOM-kill, Kafka will fail
leader election, and minikube image loads will run out of space.

**Before continuing:** make sure **Docker Desktop is running** (whale icon
in system tray, not greyed out).

---

## 2 · Open Git Bash in the project

```bash
cd /d/pinkline/PAS-SCADA-Kafka-Bridge
```

(In Git Bash, Windows drives are `/c`, `/d`, etc. Use forward slashes.)

> **Use Git Bash, not WSL bash.** `C:\Windows\System32\bash.exe` is the
> WSL launcher and can't see `/d/pinkline/...` from your host disk. Open
> Git Bash explicitly: `C:\Program Files\Git\bin\bash.exe`.

---

## 3 · Tell start.sh where Artemis lives

```bash
export MESSAGING_INFRA="/d/pinkline/messaging-infra"
```

This points `start.sh` at the docker-compose file that boots Artemis.
You only need to do this **once per Git Bash session** — opening a new
terminal requires re-running this `export`.

> Want it permanent? Add the same line to `~/.bashrc`.

---

## 4 · Run start.sh (first time)

```bash
./start.sh
```

**What it does, automatically, in order:**

1. **Section 0** — removes any leftover host docker containers
   (`pas-scada-api`, `pas-scada-monitor`, `pas-scada-demo`,
   `external-scada-scada-api`) that would shadow the kubectl port-forwards.
2. **minikube start** — boots the local k8s cluster (~1 min first time).
3. **Artemis up** — `docker compose up -d` from `$MESSAGING_INFRA`.
4. **Builds 5 images** — bridge (Java/Maven), connect, scada-api, monitor,
   demo. **First run takes 5–10 min** (Maven downloads, Docker layers).
5. **Loads images into minikube** so pods can pull them.
6. **Applies all k8s manifests** in correct dependency order
   (namespaces → zookeeper → kafka → kafdrop → bridge → connect →
   scada-api → rabbitmq → monitor → demo).
7. **Runs bootstrap jobs** — creates Kafka topics + RabbitMQ queue.
8. **Registers Kafka Connect connectors** — all 7 source/sink connectors.
9. **Waits for every deployment to become Ready.**
10. **Starts port-forwards** for all dashboards.

**Total time on a fresh PC: ~10–15 minutes for first run.**
Subsequent runs (when images already exist) take ~2 minutes.

If you ever see an error mid-way, just re-run `./start.sh` — it's
idempotent and converges to the right state.

---

## 5 · Verify everything is up

Open these URLs in Chrome — **all should respond with content**:

| URL | What you should see | Login |
|---|---|---|
| http://localhost:8080 | Monitor dashboard — 19 components, mostly green | — |
| http://localhost:8091 | SCADA simulator (TMS ↔ SCADA dashboard) | — |
| http://localhost:8090 | Demo data table | — |
| http://localhost:8090/flow | Live flow diagram with animated arrows | — |
| http://localhost:8085/actuator/health | `{"status":"UP"}` from the bridge | — |
| http://localhost:8085/api/messages | JSON array of recent messages | — |
| http://localhost:9000 | Kafdrop — list of Kafka topics | — |
| http://localhost:8161/console | Artemis console | admin / admin |
| http://localhost:15672 | RabbitMQ admin | thiru / password |
| http://localhost:8083/connectors?expand=status | Kafka Connect REST — all 7 connectors RUNNING | — |

If the Monitor at 8080 shows everything green after ~3 minutes, the
system is healthy end-to-end.

---

## 6 · Drive the system

### See SCADA → TMS messages flowing
Open http://localhost:8091. The right pane "SCADA → TMS" auto-publishes
UpdateAlarm / KeepAlive / SendAllAlarms / GetAllAlarms every 10 / 30 /
60 / 120 seconds (visible counters at the top).

### See TMS → SCADA messages flowing
On the same page, scroll to **MANUAL PUBLISH** → second row labelled
**TMS →**. Choose a topic, set interval (e.g. 3 seconds), click **Start**.
The left pane **TMS → SCADA** will start filling with decoded JSON.

### Browse messages in Artemis
Open http://localhost:8161/console → expand `0.0.0.0` → `addresses`.
TMS topics appear as multicast addresses. Their queues are drained
instantly by Kafka Connect (consumer count = 1 means subscribed).

### Browse messages in Kafdrop
Open http://localhost:9000 → click any topic (`tms.raw`,
`tms.scada.encrypted`, `scada.tms.alarms`, etc.) → "View Messages"
→ "View Messages" again → see message bodies.

---

## 7 · Daily restart (next-day run)

This document covers **first-time setup only**. After you've successfully
run `start.sh` once, the daily PC-reboot routine is much shorter and lives
in its own file:

> **See [MORNING-START.md](./MORNING-START.md)** for the daily startup steps.

You don't re-run `start.sh` every day — minikube + Docker survive PC
shutdown, so the morning routine is just: start Docker Desktop → start
minikube → wait for pods → start port-forwards.

---

## 8 · Stopping / cleanup

### Stop port-forwards (end of day)
```powershell
Get-Process powershell | Where-Object { $_.MainWindowTitle -like '*kubectl*' } | Stop-Process
```

### Stop one service (preserves state)
```bash
kubectl -n pinkline scale deploy pas-scada-bridge --replicas=0
```
Restart with `--replicas=1`.

### Stop everything (preserve state)
```bash
docker compose -f "$MESSAGING_INFRA/docker-compose.yml" down
minikube stop
```

### Wipe everything (destroys all data)
```bash
minikube delete
```
Re-running `start.sh` after a `minikube delete` rebuilds everything from scratch.

---

## 9 · Common gotchas

### Dashboard at 8091/8080/8090 shows old UI even after rebuilding
A leftover host docker container (`pas-scada-api`, `pas-scada-monitor`,
`pas-scada-demo`, or `external-scada-scada-api`) is bound to the same
port and shadowing the kubectl port-forward — so your browser hits the
old container, not the freshly-deployed pod. `start.sh` Section 0 already
handles this, but if you spun up something manually with `docker run`:

```powershell
docker ps | findstr /i "pas-scada external-scada"
docker rm -f pas-scada-api pas-scada-monitor pas-scada-demo external-scada-scada-api
```

Then re-run the relevant port-forward from Step 7.5.

> **Canary check:** the SCADA dashboard prints `[BUILD-MARKER-A1]` near
> the title (see `external-scada/scada-api/static/dashboard.html:363`).
> If the marker doesn't match what's in your source, the running container
> is stale.

### "Site can't be reached" on a dashboard
A port-forward died. Re-run Step 7.5 (or the single failing line from it).

### Pod stuck `0/1 Running` for 5+ min
```bash
kubectl -n pinkline rollout restart deploy/<name>
```

### Bridge in `CrashLoopBackOff`
Bridge needs RabbitMQ. Check rabbitmq pod is `1/1 Running` first
(`kubectl get pods -n scada`). Then restart bridge.

### Artemis container missing
```powershell
& 'C:\Program Files\Docker\Docker\resources\bin\docker.exe' start artemis
```
If the container doesn't exist at all (`docker ps -a --filter name=artemis`
shows nothing), `messaging-infra` was never brought up. From Git Bash:
```bash
export MESSAGING_INFRA="/d/pinkline/messaging-infra"
docker compose -f $MESSAGING_INFRA/docker-compose.yml up -d
```

### Artemis container shows "unhealthy"
Cosmetic — the broker is fine. Verify with `curl -s http://localhost:8161/console`.

### Kafka pod in `Error` with `InconsistentClusterIdException`
Stale Kafka data in minikube hostpath. Wipe it:
```bash
kubectl -n pinkline scale deploy kafka --replicas=0
minikube ssh -- "sudo rm -rf /tmp/hostpath-provisioner/pinkline/kafka-data/*"
kubectl -n pinkline scale deploy kafka --replicas=1
```
With the PVC fix in `tms/k8s/20-zookeeper.yaml` this should not recur.

### Bridge probe failing (Spring Boot slow startup)
The bridge needs ~3 minutes to come up on a cold start. If it's stuck
in `0/1 Running` after 5 minutes:
```bash
kubectl -n pinkline logs deploy/pas-scada-bridge --tail 50
```

### Port-forward log shows "connection refused"
The pod restarted while the forward was open. Close that minimized window
and re-run that single line from Step 7.5.

### Everything is broken — nuke and restart
```powershell
& $env:USERPROFILE\minikube.exe stop
& $env:USERPROFILE\minikube.exe start
./start.sh   # from Git Bash
```

---

## 10 · Troubleshooting reference

### Show everything's state at once
```bash
kubectl -n pinkline get pods
kubectl -n scada get pods
docker ps --filter name=artemis
curl -s localhost:8083/connectors?expand=status \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state}'
curl -s localhost:8085/actuator/health
curl -s localhost:8091/api/status
```

### Restart one piece after a code change

```bash
# After Java change → bridge
docker build -t pinkline/pas-scada-bridge:latest tms/
minikube ssh -- "docker rmi -f pinkline/pas-scada-bridge:latest"
minikube image load pinkline/pas-scada-bridge:latest
kubectl -n pinkline rollout restart deploy/pas-scada-bridge

# After scada-api/app.py or dashboard.html change
docker build -t ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest external-scada/scada-api/
minikube ssh -- "docker rmi -f ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest"
minikube image load ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest
kubectl -n scada rollout restart deploy/scada-api

# After monitor.py change
docker build -t pinkline/pas-scada-monitor:latest monitor/
minikube ssh -- "docker rmi -f pinkline/pas-scada-monitor:latest"
minikube image load pinkline/pas-scada-monitor:latest
kubectl -n pinkline rollout restart deploy/pas-scada-monitor

# After demo template change
docker build -t pinkline/pas-scada-demo:1.0.0 demo/
minikube ssh -- "docker rmi -f pinkline/pas-scada-demo:1.0.0"
minikube image load pinkline/pas-scada-demo:1.0.0
kubectl -n pinkline rollout restart deploy/pas-scada-demo

# After Connect connector config change (no rebuild needed)
kubectl apply -f connect/k8s/20-configmap.yaml
kubectl -n pinkline delete job register-connectors --ignore-not-found
kubectl apply -f connect/k8s/40-job-register.yaml
```

### Pod logs (most common starting point)
```bash
kubectl -n pinkline logs -f deploy/pas-scada-bridge
kubectl -n pinkline logs -f deploy/kafka-connect
kubectl -n pinkline logs -f deploy/pas-scada-monitor
kubectl -n scada    logs -f deploy/scada-api
```

### Forward + reverse path test
```bash
# Forward: publish a test XML to Artemis TMS.PISInfo
kubectl apply -f test-publish.yaml
# Watch http://localhost:8091 → "TMS → SCADA" panel — JSON appears in ~3s

# Reverse: SCADA auto-publishes UpdateAlarm every 10s.
# Browse Artemis console → addresses → SCADA.TMS.Alarms → queues → Browse
# Message count grows continuously.
```

---

## 11 · Folder map

| Folder | Purpose |
|---|---|
| `tms/` | Java/Spring Boot bridge (encrypts XML→JSON, AES-256-GCM) |
| `external-scada/scada-api/` | Python SCADA simulator + dashboard |
| `monitor/` | Python health probe dashboard |
| `demo/` | Python customer-facing demo UI |
| `connect/` | Kafka Connect Dockerfile + connector configs |
| `bootstrap/` | One-shot Jobs: Kafka topics + RabbitMQ queues |
| `cicd/`, `docs/` | CI/CD configs, architecture diagrams |

Each folder is self-contained: own Dockerfile, own k8s/ manifests.
Components communicate over the network only (Kafka, Artemis, RabbitMQ,
HTTP REST), so any subset can run independently.

---

## 12 · Need more detail?

- **Daily restart after PC reboot**: see [`MORNING-START.md`](./MORNING-START.md)
- **Step-by-step manual bring-up** (no `start.sh`, do it yourself with status checks at every step): see [`MANUAL-RUN.md`](./MANUAL-RUN.md)
- **Per-service start / restart commands** (bring up just one piece): see [`START-COMMANDS.md`](./START-COMMANDS.md)
- **Architecture / why each piece exists**: see [`README.md`](./README.md) and [`CLIENT-REQUEST.md`](./CLIENT-REQUEST.md)
- **Bridge internals (Camel routes, encryption)**: see [`tms/README.md`](./tms/README.md)
- **Connector configs**: see [`connect/README.md`](./connect/README.md)
- **VM / production deployment** (different topic, not for fresh PC dev):
  see `VM-DEPLOY.md`, `TMS-LINUX-VM.md`, `MONITOR-VM.md`, `SCADA-WINDOWS-VM.md`

---

That's it. If `start.sh` finishes without errors and Monitor at
http://localhost:8080 shows everything green, you're done.
