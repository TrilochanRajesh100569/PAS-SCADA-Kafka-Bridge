# Monitor VM — runbook

Self-contained install + ops guide for the **Monitor VM** — a separate
machine (Linux or Windows) that runs **only** the `pas-scada-monitor`
health dashboard. It probes the TMS VM and SCADA VM over HTTP and shows
a 19-component up/down board at port 8080.

**Why a separate VM?** Monitor is observability. If you put it on the
TMS VM, a TMS outage takes the dashboard down too — exactly when you
need it most. Keep it off the data path.

The TMS and SCADA components live on **separate VMs** — see
`TMS-LINUX-VM.md` and `SCADA-WINDOWS-VM.md`. Architecture overview in
`VM-DEPLOY.md`.

---

## 1 · Before you start — values to gather

| Variable | What | Example |
|---|---|---|
| `TMS_HOST` | DNS / IP of the TMS Linux VM | `tms-host.internal` or `10.20.0.41` |
| `SCADA_HOST` | DNS / IP of the SCADA Windows VM | `scada-host.internal` or `10.20.0.42` |

The Monitor VM needs **outbound HTTP** access to:

| To | Port | Why |
|---|---|---|
| `TMS_HOST` | 8085 | Bridge `/actuator/health` probe |
| `TMS_HOST` | 8083 | Kafka Connect REST `/connectors?expand=status` probe |
| `TMS_HOST` | 9000 | Kafdrop reachability (optional) |
| `TMS_HOST` | 8161 | Artemis console reachability (optional) |
| `SCADA_HOST` | 15672 | RabbitMQ admin API probe |
| `SCADA_HOST` | 8091 | scada-api `/api/status` probe |

Sanity-check these reach from this VM **before installing**:
```bash
# Linux
curl -s -o /dev/null -w '%{http_code}\n' http://$TMS_HOST:8085/actuator/health
curl -s -o /dev/null -w '%{http_code}\n' http://$SCADA_HOST:8091/api/status
```
```powershell
# Windows
Invoke-WebRequest http://$env:TMS_HOST:8085/actuator/health -UseBasicParsing | Select StatusCode
Invoke-WebRequest http://$env:SCADA_HOST:8091/api/status -UseBasicParsing | Select StatusCode
```
If those fail, fix the network / firewalls on TMS VM and SCADA VM
**first** — Monitor will be all red otherwise.

---

## 2 · Install prerequisites

### Option A — Linux Monitor VM (recommended)

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin git curl
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
# log out, log back in
docker version
docker compose version
```

### Option B — Windows Monitor VM

- Install **Docker Desktop for Windows** (WSL 2 backend).
- Install **Git for Windows**.
- Verify in PowerShell:
  ```powershell
  docker version
  docker compose version
  ```

The rest of this doc shows Linux commands by default. Windows
equivalents are noted where they diverge.

---

## 3 · Get the code

You only need the `monitor/` folder. Cloning the whole repo is simplest:

**Linux:**
```bash
sudo mkdir -p /opt/pinkline
sudo chown $USER:$USER /opt/pinkline
cd /opt/pinkline
git clone <PAS-SCADA-Kafka-Bridge repo URL> PAS-SCADA-Kafka-Bridge
```

**Windows:**
```powershell
mkdir C:\pinkline -Force | Out-Null
cd C:\pinkline
git clone <PAS-SCADA-Kafka-Bridge repo URL> PAS-SCADA-Kafka-Bridge
```

---

## 4 · Build the monitor image

**Linux:**
```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge
docker build -t pinkline/pas-scada-monitor:latest monitor/
```

**Windows:**
```powershell
cd C:\pinkline\PAS-SCADA-Kafka-Bridge
docker build -t pinkline/pas-scada-monitor:latest monitor\
```

Builds in ~1 min (Python container).

> **Alternative — pull from a registry instead:**
> `docker pull <registry>/pinkline/pas-scada-monitor:latest`

---

## 5 · Create the Monitor compose file

Save as `deploy/monitor-vm/docker-compose.yml` under your project root:

**Linux:** `/opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/monitor-vm/docker-compose.yml`
**Windows:** `C:\pinkline\PAS-SCADA-Kafka-Bridge\deploy\monitor-vm\docker-compose.yml`

```yaml
version: "3.8"
volumes:
  monitor-state:

services:
  monitor:
    image: pinkline/pas-scada-monitor:latest
    environment:
      # Targets the monitor probes — full URLs of services on other VMs
      BRIDGE_URL:        http://${TMS_HOST}:8085
      KAFKA_CONNECT_URL: http://${TMS_HOST}:8083
      KAFDROP_URL:       http://${TMS_HOST}:9000
      ARTEMIS_URL:       http://${TMS_HOST}:8161
      RABBITMQ_URL:      http://${SCADA_HOST}:15672
      SCADA_API_URL:     http://${SCADA_HOST}:8091
      # Optional creds (only needed if monitor authenticates to admin APIs)
      ARTEMIS_USER:      ${ARTEMIS_USER:-admin}
      ARTEMIS_PASS:      ${ARTEMIS_PASS:-admin}
      RABBITMQ_USER:     ${RABBITMQ_USER:-thiru}
      RABBITMQ_PASS:     ${RABBITMQ_PASS}
    ports:
      - "8080:8080"
    volumes:
      - monitor-state:/var/lib/monitor
    restart: unless-stopped
```

> **Cross-check the env keys** with the existing
> `monitor/k8s/20-secret.yaml` and `monitor/k8s/40-deployment.yaml`. If
> the image expects different names (e.g. `BRIDGE_HEALTH_URL` instead
> of `BRIDGE_URL`), use the names from those YAMLs.

Create `deploy/monitor-vm/.env`:

**Linux:**
```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/monitor-vm
cat > .env <<EOF
TMS_HOST=tms-host.internal
SCADA_HOST=scada-host.internal
ARTEMIS_USER=admin
ARTEMIS_PASS=<strong-password-here>
RABBITMQ_USER=thiru
RABBITMQ_PASS=<strong-password-here>
EOF
chmod 600 .env
```

**Windows:**
```powershell
cd C:\pinkline\PAS-SCADA-Kafka-Bridge\deploy\monitor-vm
Set-Content -Path .env -Encoding ascii -Value @"
TMS_HOST=tms-host.internal
SCADA_HOST=scada-host.internal
ARTEMIS_USER=admin
ARTEMIS_PASS=<strong-password-here>
RABBITMQ_USER=thiru
RABBITMQ_PASS=<strong-password-here>
"@
icacls .env /inheritance:r /grant:r "$env:USERNAME:(R,W)"
```

---

## 6 · Start the Monitor

**Linux:**
```bash
cd /opt/pinkline/PAS-SCADA-Kafka-Bridge/deploy/monitor-vm
docker compose up -d
docker compose ps
docker compose logs -f monitor       # watch first 30s of probes
# Ctrl-C the logs once probes are running
```

**Windows:** same commands in `C:\pinkline\...\deploy\monitor-vm`.

---

## 7 · Verify

```bash
# Health endpoint
curl -s http://localhost:8080/healthz       # {"status":"ok"}

# Live probe state (JSON)
curl -s http://localhost:8080/state | jq    # all probes + their last result
```

Open in browser: **http://localhost:8080** (or
`http://<MONITOR_VM_IP>:8080` from your ops workstation).

After ~30s, the dashboard should show 19 components — most or all green.

What "green" means:
- **TMS-side tiles** (Bridge, Kafdrop, Connect, Artemis) — TMS VM is up
  and reachable from this VM.
- **SCADA-side tiles** (RabbitMQ, scada-api) — SCADA VM is up and
  reachable from this VM.
- **Connector-state tiles** (7 of them) — pulled via Connect REST;
  green means the connector is `RUNNING`.

If a tile is red, click for details. The diagnosis is almost always one of:
1. Network / firewall — Monitor VM can't reach the target host:port.
2. Wrong env value — `BRIDGE_URL` etc. point at the wrong host.
3. The probed service is genuinely down — fix it on its own VM.

---

## 8 · Stop / restart

```bash
cd <deploy/monitor-vm path>
docker compose stop                  # pause
docker compose down                  # stop + remove containers (preserves state volume)
docker compose up -d                 # restart
docker compose down -v               # full wipe (drops monitor-state volume)
```

---

## 9 · Common issues

| Symptom | Fix |
|---|---|
| All tiles red | Network. From inside the monitor container: `docker compose exec monitor curl -v http://${TMS_HOST}:8085/actuator/health`. If it fails → DNS / firewall / TMS VM down. |
| Some TMS tiles green, all SCADA tiles red | Outbound from Monitor VM to `<SCADA_HOST>:5672/15672/8091` blocked. Open SCADA VM's Windows firewall (see `SCADA-WINDOWS-VM.md` §5) for `<MONITOR_VM_IP>` as `RemoteAddress`. |
| Some SCADA tiles green, all TMS tiles red | Outbound from Monitor VM to TMS VM blocked. Check TMS VM's iptables / ufw rules. On TMS VM: `sudo ufw allow from <MONITOR_VM_IP> to any port 8085 proto tcp` (and 8083). |
| Dashboard loads but probe count is wrong (e.g. 5/19) | `BRIDGE_URL` / `RABBITMQ_URL` / etc. env vars not set. Check `docker compose exec monitor env \| grep -E '_URL$'`. Fix `.env`, restart. |
| Connect-state tiles all red even though TMS reaches them fine via 8083 | Monitor expects to GET `<KAFKA_CONNECT_URL>/connectors?expand=status`. Make sure no proxy or firewall strips the `?expand=status` query string. |
| Memory keeps growing | The state volume (`monitor-state`) accumulates probe history. Periodic `docker compose down -v && up -d` clears it (cheap — Monitor has no important persistent state). |
| `8080` won't load from your ops workstation | Monitor VM's own firewall blocks inbound 8080. Linux: `sudo ufw allow from <ops-subnet> to any port 8080 proto tcp`. Windows: `New-NetFirewallRule -DisplayName "Monitor 8080" -Direction Inbound -Protocol TCP -LocalPort 8080 -RemoteAddress <ops-subnet> -Action Allow`. |

---

## 10 · Hardening notes (for prod)

- The Monitor probes admin APIs. The creds in `.env` are real
  RabbitMQ / Artemis admin credentials. Keep `.env` `chmod 600`
  (Linux) or restricted ACL (Windows).
- The Monitor dashboard at 8080 is unauthenticated. If exposed beyond
  your ops subnet, put it behind a reverse proxy with auth (nginx
  basic-auth, OAuth2 proxy, etc.).
- Tiles only check up/down — not throughput, latency, or error rate.
  For real metrics, add Prometheus + Grafana or similar. The Monitor
  is a quick at-a-glance board, not a full observability solution.
