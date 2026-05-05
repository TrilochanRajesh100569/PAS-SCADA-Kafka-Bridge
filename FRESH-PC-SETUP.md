# Fresh PC Setup — PAS-SCADA-Kafka-Bridge

Run-from-scratch guide for a Windows PC where someone has been handed both
project folders and just wants to get the system running.

---

## 0 · Folder layout (what you should have received)

You should have received **two folders**. Place them side-by-side under
`D:\pinkline\` (or any directory — just keep them together):

```
D:\pinkline\
  ├── messaging-infra\           ← Artemis docker-compose
  └── PAS-SCADA-Kafka-Bridge\    ← this repo (everything else)
```

> If your folders are at a different path, adjust the `MESSAGING_INFRA`
> variable in step 3 accordingly.

---

## 1 · Prerequisites (must be installed first)

| Tool | Why | Check it works |
|---|---|---|
| Docker Desktop | Runs Artemis + builds all images | `docker ps` |
| minikube | Local Kubernetes cluster | `minikube version` |
| kubectl | Talks to minikube | `kubectl version --client` |
| Git for Windows (Git Bash) | Runs `start.sh` (bash script) | `bash --version` |
| Java 17 + Maven | Builds the bridge | `mvn -v` (only needed if rebuilding bridge from source) |

**Before continuing:** make sure **Docker Desktop is running** (whale icon
in system tray, not greyed out).

---

## 2 · Open Git Bash in the project

```bash
cd /d/pinkline/PAS-SCADA-Kafka-Bridge
```

(In Git Bash, Windows drives are `/c`, `/d`, etc. Use forward slashes.)

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

## 4 · Run start.sh

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

Open these URLs in Chrome — **all should respond with content (not "site
can't be reached")**:

| URL | What you should see |
|---|---|
| http://localhost:8080 | Monitor dashboard — 19 components, mostly green |
| http://localhost:8091 | SCADA simulator (TMS ↔ SCADA dashboard) |
| http://localhost:8090 | Demo data table |
| http://localhost:8090/flow | Live flow diagram with animated arrows |
| http://localhost:8085/actuator/health | `{"status":"UP"}` from the bridge |
| http://localhost:8085/api/messages | JSON array of recent messages |
| http://localhost:9000 | Kafdrop — list of Kafka topics |
| http://localhost:8161/console | Artemis console (login: `admin` / `admin`) |
| http://localhost:15672 | RabbitMQ admin (login: `thiru` / `password`) |

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

## 7 · Stopping / cleanup

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

## 8 · Common gotchas

### "Site can't be reached" on a dashboard
A port-forward died. Re-run:
```bash
./start.sh
```

### Dashboard shows old UI even after rebuilding
A leftover host docker container (`pas-scada-api` etc.) is shadowing
the kubectl port-forward. `start.sh`'s Section 0 already handles this,
but if you spin up something manually with `docker run`, kill it:
```bash
docker rm -f pas-scada-api pas-scada-monitor pas-scada-demo
```

### Kafka in CrashLoopBackOff with "InconsistentClusterIdException"
Stale Kafka data in minikube hostpath. Wipe it:
```bash
kubectl -n pinkline scale deploy kafka --replicas=0
minikube ssh -- "sudo rm -rf /tmp/hostpath-provisioner/pinkline/kafka-data/*"
kubectl -n pinkline scale deploy kafka --replicas=1
```
Then re-run `start.sh` to redo the topic bootstrap.

### Bridge probe failing (Spring Boot slow startup)
The bridge needs ~3 minutes to come up on a cold start. If it's stuck
in `0/1 Running` after 5 minutes, check logs:
```bash
kubectl -n pinkline logs deploy/pas-scada-bridge --tail 50
```

### Artemis container shows "unhealthy"
Cosmetic — the broker is fine. Verify with:
```bash
curl -s http://localhost:8161/console
```
Should return HTML.

---

## 9 · What's where (folder map)

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

## 10 · Need more detail on a specific component?

- **Per-service start commands** (run just one piece without the full
  `start.sh`): see `START-COMMANDS.md`
- **Architecture / why each piece exists**: see `README.md`
- **Bridge internals (Camel routes, encryption)**: see `tms/README.md`
- **Connector configs**: see `connect/README.md`
- **Demo UI**: see `demo/README.md`

---

That's it. If `start.sh` finishes without errors and Monitor at
http://localhost:8080 shows everything green, you're done.
