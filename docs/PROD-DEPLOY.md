# Production deployment — Kubernetes cluster + client's Artemis

How to run this stack on a real Kubernetes cluster (no minikube) where the
client already has Artemis running. Uses `prodstart.sh`, not `start.sh`.

> **For first-time bring-up on YOUR laptop**, see [`FRESH-PC-SETUP.md`](./FRESH-PC-SETUP.md).
> **For VM-based prod with multi-server topology**, see [`VM-DEPLOY.md`](./VM-DEPLOY.md).
> This file covers the **real-Kubernetes + remote Artemis + no LoadBalancer** case.

---

## ⚡ Quick start — deploy in 7 steps

Skip ahead to detailed sections below for verification, troubleshooting,
and rotation. This is the minimum to get running.

### Step 1 — Install prerequisites on the cloud server (one time)

```
docker · kubectl · bash · git · jq
```

No minikube. The cluster is wherever your `kubectl` already points
(EKS, AKS, GKE, k3s, kubeadm, on-prem — doesn't matter).

### Step 2 — Clone the repo

```bash
git clone <repo-url> /opt/PAS-SCADA-Kafka-Bridge
cd /opt/PAS-SCADA-Kafka-Bridge
```

### Step 3 — Confirm `kubectl` points at the right cluster

```bash
kubectl config current-context
kubectl cluster-info
kubectl get nodes
```

If you have multiple clusters in your kubeconfig, set `KUBE_CONTEXT`
in `.env.prod` so `prodstart.sh` switches automatically.

### Step 4 — Verify the cluster can reach the client's Artemis

```bash
# Replace with the client-given Artemis host + port
kubectl run -i --rm tcptest --image=busybox --restart=Never -- \
  sh -c "nc -vz <client-artemis-host> 61616"
# Expected: ... open
```

If this fails, fix network/firewall/VPN BEFORE going further.

### Step 5 — Push images to a registry the cluster can pull from

The deployment YAMLs reference `pinkline/*` and
`ghcr.io/thirunavukkarasuthangaraj/pas-scada-api`. **A real cluster
cannot pull `pinkline/*` from Docker Hub** — you must mirror these into
a registry your cluster has credentials for, and update the YAMLs.

Easiest path: use GHCR / ECR / GCR / a private registry.

```bash
# Build + tag + push (one time, or whenever images change)
export IMAGE_REGISTRY=ghcr.io/your-org   # whatever registry your cluster pulls from

BUILD_IMAGES=1 ./prodstart.sh            # builds + pushes, then deploys
# (If you only want to build+push without deploying, run the docker build
#  + push commands manually — see Section 5 below.)
```

After the first push, update the deployment YAMLs to reference
`$IMAGE_REGISTRY/pas-scada-bridge:latest` etc. (see Section 5.2).
Subsequent deploys can run with `BUILD_IMAGES=0` (the default).

### Step 6 — Create your env file (one time)

```bash
cp .env.template /home/ops/.env.prod
chmod 600 /home/ops/.env.prod
nano /home/ops/.env.prod
```

In the editor, set at minimum:
```bash
ARTEMIS_BROKER_URL=tcp://<client-artemis-host>:61616
ARTEMIS_USER=<client-given-username>
ARTEMIS_PASSWORD=<client-given-password>
SCADA_AES_KEY=<run: openssl rand -base64 32>
RABBITMQ_USER=<your-rmq-user>
RABBITMQ_PASS=<your-rmq-password>
MQTT_USER=<your-mqtt-user>
MQTT_PASS=<your-mqtt-password>

# Optional
KUBE_CONTEXT=prod-cluster                # if kubeconfig has multiple contexts
PUBLIC_HOST=10.0.0.42                    # server's external IP for the URL list
```

Note: `SKIP_ARTEMIS=1` is **not needed** with `prodstart.sh` — the script
never starts a local Artemis. It always assumes the client's broker is
remote.

### Step 7 — Source and deploy

```bash
source /home/ops/.env.prod
./prodstart.sh
```

Wait ~5–10 min on first run, ~2 min on subsequent runs.

### Verify

```bash
kubectl -n pinkline exec deploy/kafka-connect -- \
  curl -s http://localhost:8083/connectors?expand=status \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state}'
```

Expected: 7 connectors, all `RUNNING`. If any `FAILED` → see [Section 7](#7--common-prod-issues).

### Subsequent deploys (after first time)

```bash
source /home/ops/.env.prod
./prodstart.sh
```

`prodstart.sh` is idempotent. Re-run anytime to apply config changes.

---

## 1 · What changes vs dev

| Concern | Dev (`start.sh` on your PC) | Prod (`prodstart.sh` on cloud) |
|---|---|---|
| Cluster | Local minikube | Real K8s (your `kubectl` context) |
| Artemis | Local Docker via `messaging-infra` | Client's existing broker on a remote host |
| Artemis credentials | `admin` / `admin` | Real client-given username/password |
| Bridge `ARTEMIS_HOST` | `host.minikube.internal` | Client broker's hostname/IP |
| AES key | Hardcoded dev key | Rotated, never in git |
| Source of values | Hardcoded defaults in `start.sh` | `/home/ops/.env.prod` (gitignored) |
| Image distribution | `minikube image load` (local) | Registry pull (`docker push` then `imagePullPolicy: Always`) |
| Service exposure | `kubectl port-forward 127.0.0.1` | `kubectl port-forward --address 0.0.0.0` (no LoadBalancer) |
| Local Artemis docker compose | runs | never runs (script doesn't reference it) |
| Viewer queues (`docker exec artemis`) | runs | skipped — broker is remote |

Everything else (Kafka, Connect, RabbitMQ, bridge, monitor, demo) runs identically.

---

## 2 · Prerequisites on the cloud server

| Tool | Why | Verify |
|---|---|---|
| Docker | Builds images locally before pushing | `docker ps` |
| kubectl | Talks to your prod cluster | `kubectl version --client` |
| bash + Git | Run `prodstart.sh` | `bash --version`, `git --version` |
| jq | Diagnostic commands | `jq --version` |
| Network reachability to client's Artemis on port 61616 | Required | `nc -vz <client-host> 61616` |
| Push access to a container registry the cluster can pull from | Required | `docker login <registry>` |

> **No minikube needed.** `prodstart.sh` uses whatever cluster
> `kubectl config current-context` points at.

---

## 3 · One-time setup — create `.env.prod`

```bash
cd /path/to/PAS-SCADA-Kafka-Bridge
cp .env.template /home/ops/.env.prod
chmod 600 /home/ops/.env.prod
nano /home/ops/.env.prod
```

### Minimum required overrides for prod

```bash
# Client's Artemis — get these from the client / DevOps
ARTEMIS_BROKER_URL=tcp://10.0.0.5:61616
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

# Optional but recommended
KUBE_CONTEXT=prod-cluster                 # if you have multiple kubeconfig contexts
PUBLIC_HOST=10.0.0.42                     # server external IP (for the URL list)
IMAGE_REGISTRY=ghcr.io/your-org           # only needed when BUILD_IMAGES=1
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

## 5 · Image distribution (the hard part of moving off minikube)

In dev, `start.sh` runs `minikube image load` to put locally-built images
inside the minikube node. **A real K8s cluster has no such trick** — every
node pulls images from a registry over the network.

### 5.1 · Two ways to handle it

| Approach | When | How |
|---|---|---|
| **CI builds + pushes** | You have CI (GitHub Actions, GitLab CI, etc.) | CI runs `docker build` + `docker push` on every commit. `prodstart.sh` runs with `BUILD_IMAGES=0` (default). |
| **`prodstart.sh` builds + pushes** | No CI, single-operator deploy | Set `IMAGE_REGISTRY=...` and `BUILD_IMAGES=1` before running. |

### 5.2 · Update deployment YAMLs to point at your registry

The YAMLs ship with `pinkline/*` and
`ghcr.io/thirunavukkarasuthangaraj/pas-scada-api` tags. After you push to
your own registry, edit these files to match:

| File | Image references |
|---|---|
| `tms/k8s/deployment.yaml` | `pinkline/pas-scada-bridge:latest` |
| `connect/k8s/30-deployment.yaml` | `pinkline/pas-scada-connect:latest` |
| `monitor/k8s/40-deployment.yaml` | `pinkline/pas-scada-monitor:latest` |
| `demo/k8s/20-deployment.yaml` | `pinkline/pas-scada-demo:1.0.0` |
| `external-scada/k8s/70-scada-api-deployment.yaml` | `ghcr.io/thirunavukkarasuthangaraj/pas-scada-api:latest` |

Find-and-replace each with `$IMAGE_REGISTRY/pas-scada-bridge:latest` etc.
**Do this once**; commit the change to a `prod` branch (or use Kustomize
overlays — see Section 10).

### 5.3 · Cluster needs registry credentials

If your registry is private:

```bash
kubectl -n pinkline create secret docker-registry regcred \
  --docker-server=ghcr.io \
  --docker-username=<your-user> \
  --docker-password=<token-with-read:packages> \
  --docker-email=<your-email>

kubectl -n scada create secret docker-registry regcred \
  --docker-server=ghcr.io \
  --docker-username=<your-user> \
  --docker-password=<token-with-read:packages> \
  --docker-email=<your-email>
```

Then add `imagePullSecrets: [{name: regcred}]` under `spec.template.spec`
in each deployment YAML. Public registries (Docker Hub anonymous,
public ECR) don't need this.

---

## 6 · Deploy

```bash
source /home/ops/.env.prod
./prodstart.sh
```

`prodstart.sh`:

1. Validates required env vars (`ARTEMIS_BROKER_URL`, `ARTEMIS_USER`, `ARTEMIS_PASSWORD`)
2. Switches kubectl context if `KUBE_CONTEXT` is set, then sanity-checks `cluster-info`
3. (If `BUILD_IMAGES=1`) builds images, tags for `$IMAGE_REGISTRY`, pushes
4. Applies all manifests (`tms/k8s`, `external-scada/k8s`, `connect/k8s`, `monitor/k8s`, `demo/k8s`)
5. Patches `bridge-config` with the prod `ARTEMIS_HOST` / `ARTEMIS_PORT`
6. Generates `bridge-secret`, `connect-secret`, `scada-api-secret` from env vars
7. Substitutes `ARTEMIS_BROKER_URL` into the Connect connector configmap (5 places via sed)
8. Sets `imagePullPolicy=Always` on app deployments (so registry pulls roll)
9. Bootstraps Kafka topics, declares the RabbitMQ queue + binding, registers connectors
10. Starts background port-forwards bound to `0.0.0.0` so the server's external IP works
11. Prints a URL list using `$PUBLIC_HOST` (defaults to `localhost`)

What it does NOT do (vs `start.sh`):
- No `minikube start` / `minikube image load`
- No `docker compose up artemis`
- No host-side container cleanup (`pas-scada-api`, `external-scada-scada-api`, etc.)
- No Artemis viewer-queue creation (`docker exec artemis ...`)

Total time: ~5–10 min first run (mostly Kafka / Connect rollout), ~2 min on re-runs.

---

## 7 · Verify the deploy worked

### 7.1 · All connectors RUNNING (most important)

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

### 7.2 · Bridge connected

```bash
kubectl -n pinkline logs deploy/pas-scada-bridge --tail 50 \
  | grep -iE "artemis|kafka source|reverse"
```

Expected: `← Kafka source [tms.raw]` and `→ Kafka [tms.scada.encrypted]` lines repeating. No connection errors.

### 7.3 · Data flowing end-to-end

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

### 7.4 · Health monitor

`prodstart.sh` already starts the port-forward bound to `0.0.0.0`, so
just open `http://<server-ip>:8080` from your laptop. The 19 probes
should mostly turn green within ~3 minutes.

---

## 8 · Common prod issues

| Symptom | Cause | Fix |
|---|---|---|
| `ImagePullBackOff` on bridge / connect / monitor / demo / scada-api | Cluster cannot reach the registry, or YAMLs still reference `pinkline/*` (which doesn't exist publicly) | Update YAMLs per Section 5.2; add `imagePullSecrets` per Section 5.3 |
| Connectors `FAILED` with `JMSSecurityException` | Bad `ARTEMIS_USER`/`ARTEMIS_PASSWORD` | Confirm the values in `.env.prod` match what the client gave you. Re-run `source ~/.env.prod && ./prodstart.sh` |
| Connectors `FAILED` with `Connection refused` / `Connection timeout` | Network — Artemis unreachable | Run the `nc -vz` check from Section 4. Fix firewall / VPN / NetworkPolicy |
| Connectors `FAILED` with `UnknownHostException` | DNS — hostname doesn't resolve from inside the cluster | Use the IP directly in `ARTEMIS_BROKER_URL` instead of a hostname |
| Bridge logs `Failed to encrypt: AES-256-GCM` | Bad `SCADA_AES_KEY` (not 32 bytes after base64-decode) | Regenerate with `openssl rand -base64 32`, set in `.env.prod`, re-deploy |
| SCADA-API `decrypt_fail` counter increasing | `SCADA_AES_KEY` mismatch between bridge and scada-api | `prodstart.sh` keeps these in sync IF both come from the same env var. Confirm both `bridge-secret.SCADA_AES_KEY` and `scada-api-secret.SCADA_AES_KEY` are identical |
| Connector configmap still has `host.minikube.internal` | sed didn't fire because `ARTEMIS_BROKER_URL` empty | `prodstart.sh` exits early if `ARTEMIS_BROKER_URL` is missing. If the configmap is wrong, confirm `echo $ARTEMIS_BROKER_URL` shows the prod value before running |
| `kubectl: not found` | kubectl not on PATH for `prodstart.sh` shell | Open a new shell, verify `which kubectl docker` both return paths |
| `prodstart.sh: Failed to switch kubectl context` | `KUBE_CONTEXT` doesn't exist in your kubeconfig | `kubectl config get-contexts` to list real names; fix `.env.prod` |
| Browsers can't reach `http://<server-ip>:8080` | Cloud security group / firewall blocks the port; or port-forward bound to 127.0.0.1 only | `prodstart.sh` binds `--address 0.0.0.0`. Open the port in your cloud SG. For real prod use NodePort or Ingress (Section 10). |

---

## 9 · Credential rotation

When the client rotates the Artemis password (or you rotate AES key):

```bash
# 1. Edit .env.prod with new value
nano /home/ops/.env.prod

# 2. Re-source and re-deploy
source /home/ops/.env.prod
./prodstart.sh
```

`prodstart.sh` overwrites the secrets via `kubectl create secret --dry-run | apply`
on every run, so the new credentials are applied. The deployment then needs
to restart pods to pick up the new secret values:

```bash
kubectl -n pinkline rollout restart deploy/pas-scada-bridge
kubectl -n pinkline rollout restart deploy/kafka-connect
kubectl -n scada    rollout restart deploy/scada-api
```

`prodstart.sh` already triggers the bridge + scada-api + connect rollout
restart on each run, so the bare re-deploy is enough.

---

## 10 · Going beyond — when `.env.prod` + port-forwards aren't enough

The `.env.prod` flow + `prodstart.sh` is fine for early-stage prod and a
single cloud server. As you scale, replace each piece:

### 10.1 · Secret management

| Option | What it gives you | Setup cost |
|---|---|---|
| **Sealed Secrets** | Encrypted secrets in git, GitOps-friendly | 1 day |
| **External Secrets Operator** + AWS Secrets Manager / GCP Secret Manager | Auto-fetched from cloud secret manager, audit logs, rotation | 1–2 days |
| **HashiCorp Vault** | Multi-cloud, dynamic credentials, fine-grained policy | 2–5 days |

### 10.2 · Image management

| Option | What it gives you | Setup cost |
|---|---|---|
| **CI builds + pushes on commit** | No more local docker on the server | 1 day if CI exists |
| **Kustomize overlays** | One YAML set, per-env image rewrites without sed | 1 day |
| **Helm chart** | Templated values, versioned releases, easy rollback | 2–3 days |

### 10.3 · Service exposure

| Option | What it gives you | Setup cost |
|---|---|---|
| **NodePort services** | Same as port-forwards but without the long-running process | 30 minutes |
| **Ingress controller (nginx, Traefik)** | One ingress IP, hostname routing, HTTPS via cert-manager | 1 day |
| **LoadBalancer** | Cloud-managed external IP per service | depends on cloud provider |

The `prodstart.sh` interface stays the same — only the *source* of env
vars and the *destination* of traffic change. Operators learn the same
`source <something> && ./prodstart.sh` muscle memory.

---

## 11 · Rollback / undeploy

```bash
# Stop port-forwards (leaves pods + data alone)
./stop.sh

# Stop pods, keep PVC data
./stop.sh --pods

# Full namespace wipe (destroys all data — RabbitMQ + Kafka + Zookeeper PVCs)
kubectl delete ns pinkline scada
```

The wipe will **NOT** touch the client's Artemis (you don't own it). Only
your in-cluster state (RabbitMQ messages, Kafka topics, Zookeeper data,
monitor history) is removed.

> Note: `stop.sh --minikube` and `stop.sh --wipe` use `minikube` commands
> and don't apply here. On a real cluster, `kubectl delete ns` is the
> equivalent.

---

## Quick reference

```bash
# First time on this cloud server
cp .env.template /home/ops/.env.prod
chmod 600 /home/ops/.env.prod
nano /home/ops/.env.prod                # fill in real values
# (one time) update deployment YAMLs to point at your registry

# Every deploy
source /home/ops/.env.prod
./prodstart.sh

# Verify
kubectl -n pinkline exec deploy/kafka-connect -- \
  curl -s http://localhost:8083/connectors?expand=status \
  | jq 'to_entries[] | {name:.key, state:.value.status.connector.state}'

# Rotate creds
nano /home/ops/.env.prod                # change values
source /home/ops/.env.prod
./prodstart.sh
```
