# Production deployment — cloud server with client's Artemis

How to run this stack on a cloud server where the client already has
Artemis running. Same `start.sh`, different env vars.

> **For first-time bring-up on YOUR laptop**, see [`FRESH-PC-SETUP.md`](./FRESH-PC-SETUP.md).
> **For VM-based prod with multi-server topology**, see [`VM-DEPLOY.md`](./VM-DEPLOY.md).
> This file covers the **single-cloud-server + remote Artemis** case.

---

## ⚡ Quick start — deploy in 6 steps

Skip ahead to detailed sections below for verification, troubleshooting,
and rotation. This is the minimum to get running.

### Step 1 — Install prerequisites on the cloud server (one time)

```
docker · minikube (or k8s) · kubectl · bash · git · jq
```

### Step 2 — Clone the repo

```bash
git clone <repo-url> /opt/PAS-SCADA-Kafka-Bridge
cd /opt/PAS-SCADA-Kafka-Bridge
```

### Step 3 — Verify the cluster can reach the client's Artemis

```bash
# Replace with the client-given Artemis host + port
kubectl run -i --rm tcptest --image=busybox --restart=Never -- \
  sh -c "nc -vz <client-artemis-host> 61616"
# Expected: ... open
```

If this fails, fix network/firewall/VPN BEFORE going further.

### Step 4 — Create your env file (one time)

```bash
cp .env.template /home/ops/.env.prod
chmod 600 /home/ops/.env.prod
nano /home/ops/.env.prod
```

In the editor, set at minimum:
```bash
SKIP_ARTEMIS=1
ARTEMIS_BROKER_URL=tcp://<client-artemis-host>:61616
ARTEMIS_USER=<client-given-username>
ARTEMIS_PASSWORD=<client-given-password>
SCADA_AES_KEY=<run: openssl rand -base64 32>
RABBITMQ_USER=<your-rmq-user>
RABBITMQ_PASS=<your-rmq-password>
MQTT_USER=<your-mqtt-user>
MQTT_PASS=<your-mqtt-password>
```

### Step 5 — Source and deploy

```bash
source /home/ops/.env.prod
./start.sh
```

Wait ~10–15 min on first run, ~2 min on subsequent runs.

### Step 6 — Verify

```bash
kubectl -n pinkline exec deploy/kafka-connect -- \
  curl -s http://localhost:8083/connectors?expand=status \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state}'
```

Expected: 7 connectors, all `RUNNING`. If any `FAILED` → see [Section 7](#7--common-prod-issues).

### Subsequent deploys (after first time)

Just steps 5 + 6:
```bash
source /home/ops/.env.prod
./start.sh
```

`start.sh` is idempotent. Re-run anytime to apply config changes.

---

## 1 · What changes vs dev

| Concern | Dev (your PC) | Prod (cloud server) |
|---|---|---|
| Artemis | Local Docker via `messaging-infra` | Client's existing broker on a remote host |
| Artemis credentials | `pasbridge` / `testpass123` | Real client-given username/password |
| Bridge `ARTEMIS_HOST` | `host.minikube.internal` | Client broker's hostname/IP |
| AES key | Hardcoded dev key | Rotated, never in git |
| Source of values | Hardcoded defaults in `start.sh` | `/home/ops/.env.prod` (gitignored) |
| `start.sh` Section 2 (Artemis docker compose) | runs | **skipped** via `SKIP_ARTEMIS=1` |
| `start.sh` Section 11c (viewer queues) | runs | **skipped** (no local artemis container) |

Everything else (Kafka, Connect, RabbitMQ, bridge, monitor, demo, port-forwards) runs identically.

---

## 2 · Prerequisites on the cloud server

Same toolchain as a dev PC, all installed once:

| Tool | Why | Verify |
|---|---|---|
| Docker | Builds images and runs containers | `docker ps` |
| minikube (or real k8s) | Hosts the cluster | `minikube version` |
| kubectl | Talks to the cluster | `kubectl version --client` |
| bash + Git | Run `start.sh` | `bash --version`, `git --version` |
| jq | Diagnostic commands | `jq --version` |
| Network reachability to client's Artemis on port 61616 | Required | `nc -vz <client-host> 61616` |

If you're on a real k8s cluster (not minikube), substitute equivalent commands.
The script checks for minikube; you may need to skip Section 1 manually.

---

## 3 · One-time setup — create `.env.prod`

Copy the template, restrict permissions, fill in real values:

```bash
cd /path/to/PAS-SCADA-Kafka-Bridge
cp .env.template /home/ops/.env.prod
chmod 600 /home/ops/.env.prod
nano /home/ops/.env.prod
```

### Minimum required overrides for prod

```bash
# Tell start.sh to skip the local Artemis docker compose
SKIP_ARTEMIS=1

# Client's Artemis — get these from the client / DevOps
ARTEMIS_BROKER_URL=tcp://10.0.0.5:61616
ARTEMIS_HOST=10.0.0.5
ARTEMIS_PORT=61616
ARTEMIS_USER=<client-given-username>
ARTEMIS_PASSWORD=<client-given-password>

# AES encryption key — generate ONCE, share with the SCADA-API operator,
# store offline. NEVER reuse the dev key in prod.
SCADA_AES_KEY=<output of `openssl rand -base64 32`>

# RabbitMQ + MQTT credentials (depends on your prod RabbitMQ deployment)
RABBITMQ_USER=<your-rmq-user>
RABBITMQ_PASS=<your-rmq-password>
MQTT_USER=<your-mqtt-user>
MQTT_PASS=<your-mqtt-password>
```

> The full `.env.template` shows every key with dev defaults pre-filled.
> You only need to override what's actually different.

### `.gitignore` already protects this

`.env.prod`, `.env.local`, `.env.dev`, `.env.staging`, `.env.*.local`, `*.env`
are all in `.gitignore`. Real credentials cannot accidentally land in git.

---

## 4 · Verify network reachability BEFORE deploying

Confirm the cluster can actually see the client's Artemis. Without this,
all credential debugging is wasted time.

```bash
kubectl run -i --rm tcptest --image=busybox --restart=Never -- \
  sh -c "nc -vz 10.0.0.5 61616"
# Expected output: ... open
```

If it fails:
- **Network policy** — k8s NetworkPolicy blocking egress
- **Firewall** — cloud security group / VPC firewall blocking outbound 61616
- **VPN** — site-to-site VPN to client network down
- **DNS** — using hostname instead of IP, no DNS resolution

Fix the network FIRST. No amount of password fiddling helps if the TCP
connection can't even open.

---

## 5 · Deploy

```bash
source /home/ops/.env.prod
./start.sh
```

`start.sh` auto-detects prod mode (because `SKIP_ARTEMIS=1` or
`ARTEMIS_BROKER_URL` is non-default) and:

1. Skips Section 2 (`docker compose up artemis`)
2. Patches `bridge-config` ConfigMap with `ARTEMIS_HOST`/`ARTEMIS_PORT` parsed from `ARTEMIS_BROKER_URL`
3. Regenerates `bridge-secret`, `connect-secret`, `scada-api-secret` from your env vars
4. Substitutes the broker URL into the Connect connector configmap (5 places via sed)
5. Skips Section 11c (viewer queues — `docker exec artemis` won't work)
6. Prints a prod-mode banner with the effective `ARTEMIS_BROKER_URL` and a `nc -vz` reachability check

Total time: same as dev (~10–15 min first run, ~2 min on re-runs).

---

## 6 · Verify the deploy worked

### 6.1 · All connectors RUNNING (most important)

```bash
kubectl -n pinkline exec deploy/kafka-connect -- \
  curl -s http://localhost:8083/connectors?expand=status \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state, tasks:[.value.status.tasks[].state]}'
```

Expected: 7 entries, all `RUNNING`. If any `FAILED`:
```bash
kubectl -n pinkline exec deploy/kafka-connect -- \
  curl -s http://localhost:8083/connectors/<name>/status | jq
```

The most common prod failures:
- `JMSSecurityException` — bad ARTEMIS_USER/PASSWORD
- `ConnectException: Connection refused` — bad ARTEMIS_BROKER_URL or network
- `No address associated with hostname` — DNS issue

### 6.2 · Bridge connected

```bash
kubectl -n pinkline logs deploy/pas-scada-bridge --tail 50 \
  | grep -iE "artemis|kafka source|reverse"
```

Expected: `← Kafka source [tms.raw]` and `→ Kafka [tms.scada.encrypted]` lines repeating. No connection errors.

### 6.3 · Data flowing end-to-end

```bash
kubectl -n pinkline exec deploy/kafka -- bash -c "
for t in tms.raw tms.scada.encrypted scada.tms.raw scada.tms.processed; do
  echo -n \"\$t: \"
  kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-service:9092 --topic \$t 2>/dev/null \
    | awk -F: '{s+=\$3} END{print s+0}'
done"
```

If the client is sending TMS data, `tms.raw` and `tms.scada.encrypted` should
have offsets growing. If SCADA is sending alarms, `scada.tms.raw` and
`scada.tms.processed` should grow too.

### 6.4 · Health monitor

Forward localhost:8080 and open in browser:
```bash
kubectl -n pinkline port-forward svc/pas-scada-monitor 8080:8080 &
```

The 19 probes should mostly turn green within ~3 minutes.

---

## 7 · Common prod issues

| Symptom | Cause | Fix |
|---|---|---|
| Connectors `FAILED` with `JMSSecurityException` | Bad `ARTEMIS_USER`/`ARTEMIS_PASSWORD` | Confirm the values in `.env.prod` match what the client gave you. Re-run `source ~/.env.prod && ./start.sh` |
| Connectors `FAILED` with `Connection refused` / `Connection timeout` | Network — Artemis unreachable | Run the `nc -vz` check from Section 4. Fix firewall / VPN / NetworkPolicy |
| Connectors `FAILED` with `UnknownHostException` | DNS — hostname doesn't resolve from inside the cluster | Use the IP directly in `ARTEMIS_BROKER_URL` instead of a hostname |
| Bridge logs `Failed to encrypt: AES-256-GCM` | Bad `SCADA_AES_KEY` (not 32 bytes after base64-decode) | Regenerate with `openssl rand -base64 32`, set in `.env.prod`, re-deploy |
| SCADA-API `decrypt_fail` counter increasing | `SCADA_AES_KEY` mismatch between bridge and scada-api | start.sh keeps these in sync IF both come from the same env var. Confirm both `bridge-secret.SCADA_AES_KEY` and `scada-api-secret.SCADA_AES_KEY` are identical |
| Connector configmap still has `host.minikube.internal` | start.sh's sed didn't fire because `ARTEMIS_BROKER_URL` is the dev default | Confirm `echo $ARTEMIS_BROKER_URL` shows the prod value before running start.sh |
| `kubectl: not found` | minikube/kubectl not on PATH for `start.sh` shell | Open a new shell, verify `which kubectl docker minikube` all return paths |

---

## 8 · Credential rotation

When the client rotates the Artemis password (or you rotate AES key):

```bash
# 1. Edit .env.prod with new value
nano /home/ops/.env.prod

# 2. Re-source and re-deploy
source /home/ops/.env.prod
./start.sh
```

`start.sh` overwrites the secrets via `kubectl create secret --dry-run | apply`
on every run, so the new credentials are applied. The deployment then needs
to restart pods to pick up the new secret values:

```bash
kubectl -n pinkline rollout restart deploy/pas-scada-bridge
kubectl -n pinkline rollout restart deploy/kafka-connect
kubectl -n scada    rollout restart deploy/scada-api
```

`start.sh` already triggers the bridge + scada-api rollout restart in
Section 8. Connect needs a manual restart for new connector credentials.

---

## 9 · Rollback / undeploy

```bash
# Stop port-forwards
./stop.sh

# Stop pods, keep state
./stop.sh --pods --minikube

# Full wipe (destroys all data)
./stop.sh --wipe
```

The `--wipe` will NOT touch the client's Artemis (you don't own it).
Only your minikube cluster + RabbitMQ + Kafka data are removed.

---

## 10 · Going beyond — when `.env.prod` isn't enough

The `.env.prod` flow is fine for early-stage prod and a single cloud server.
As you scale, replace it with one of:

| Option | What it gives you | Setup cost |
|---|---|---|
| **Sealed Secrets** | Encrypted secrets in git, GitOps-friendly | 1 day |
| **External Secrets Operator** + AWS Secrets Manager | Auto-fetched from cloud secret manager, audit logs, rotation | 1–2 days |
| **HashiCorp Vault** | Multi-cloud, dynamic credentials, fine-grained policy | 2–5 days |
| **CI/CD pipeline injection** | Secrets stay in CI vault, never on dev machines | 1 day if CI exists |

The `start.sh` interface stays the same — only the *source* of env vars
changes. Your operators learn the same `source <something> && ./start.sh`
muscle memory.

---

## Quick reference

```bash
# First time on this cloud server
cp .env.template /home/ops/.env.prod
chmod 600 /home/ops/.env.prod
nano /home/ops/.env.prod          # fill in real values

# Every deploy
source /home/ops/.env.prod
./start.sh

# Verify
kubectl -n pinkline exec deploy/kafka-connect -- \
  curl -s http://localhost:8083/connectors?expand=status \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state}'

# Rotate creds
nano /home/ops/.env.prod          # change values
source /home/ops/.env.prod
./start.sh
kubectl -n pinkline rollout restart deploy/kafka-connect
```
