# SCADA Windows VM — runbook

Self-contained install + ops guide for the **SCADA Windows VM**. Runs:
RabbitMQ (with MQTT plugin enabled) and the `scada-api` Python service.

The TMS and Monitor components live on **separate VMs** — see
`TMS-LINUX-VM.md` and `MONITOR-VM.md`. Architecture overview in
`VM-DEPLOY.md`.

---

## 1 · Before you start — values to gather

| Variable | What | Example |
|---|---|---|
| `TMS_VM_IP` | DNS name or IP of the TMS Linux VM | `tms-host.internal` or `10.20.0.41` |
| `MONITOR_VM_IP` | DNS / IP of the Monitor VM (so it can probe us) | `monitor-host.internal` or `10.20.0.43` |
| `RABBITMQ_USER` | RabbitMQ user the TMS VM bridge will connect with | `thiru` |
| `RABBITMQ_PASS` | RabbitMQ password (must match what TMS VM sends) | (strong password) |

These need to **match** the values configured on the TMS VM
(`SCADA_RABBITMQ_USER` / `SCADA_RABBITMQ_PASS` in TMS-LINUX-VM.md §6).

---

## 2 · Install prerequisites

| Tool | Source | Why |
|---|---|---|
| **Docker Desktop for Windows** | https://docs.docker.com/desktop/install/windows-install/ | Runs RabbitMQ + scada-api as containers. Use the **WSL 2 backend** (default on modern Windows). |
| **Git for Windows** | https://git-scm.com/download/win | Clone the repo. Includes Git Bash. |

After install:
1. Launch Docker Desktop and wait for the whale icon to say "Running".
2. Open **PowerShell** and verify:
```powershell
docker version
docker compose version
```

---

## 3 · Get the code

You only need the `external-scada/` folder, but cloning the whole repo
is simplest:

```powershell
mkdir C:\pinkline -Force | Out-Null
cd C:\pinkline
git clone <PAS-SCADA-Kafka-Bridge repo URL> PAS-SCADA-Kafka-Bridge
```

---

## 4 · Build the scada-api image

Builds locally on this VM (Python container, ~1 min):

```powershell
cd C:\pinkline\PAS-SCADA-Kafka-Bridge
docker build -t pinkline/pas-scada-api:latest external-scada\scada-api\
```

> **Alternative — pull from a registry instead of building:**
> ```powershell
> docker pull <registry>/pinkline/pas-scada-api:latest
> docker tag  <registry>/pinkline/pas-scada-api:latest pinkline/pas-scada-api:latest
> ```

---

## 5 · Open the Windows firewall

The TMS VM needs inbound TCP on **5672** (AMQP). The Monitor VM needs
inbound TCP on **15672** (RabbitMQ admin) and **8091** (scada-api).

Open **PowerShell as Administrator** and run:

```powershell
# AMQP from TMS VM (data path — required)
New-NetFirewallRule -DisplayName "RabbitMQ AMQP from TMS" `
  -Direction Inbound -Protocol TCP -LocalPort 5672 `
  -RemoteAddress <TMS_VM_IP> -Action Allow

# RabbitMQ admin from Monitor VM (probe — required)
New-NetFirewallRule -DisplayName "RabbitMQ admin from Monitor" `
  -Direction Inbound -Protocol TCP -LocalPort 15672 `
  -RemoteAddress <MONITOR_VM_IP> -Action Allow

# scada-api dashboard from Monitor VM + ops workstations
New-NetFirewallRule -DisplayName "scada-api dashboard" `
  -Direction Inbound -Protocol TCP -LocalPort 8091 `
  -RemoteAddress <MONITOR_VM_IP>,<your-ops-subnet> -Action Allow
```

> Replace `<TMS_VM_IP>`, `<MONITOR_VM_IP>`, `<your-ops-subnet>` with
> real values. For ops use `Any` for `-RemoteAddress` only on a trusted
> internal network.

---

## 6 · Create the SCADA compose file

Save as `C:\pinkline\PAS-SCADA-Kafka-Bridge\deploy\scada-vm\docker-compose.yml`:

```yaml
version: "3.8"
networks:
  scada:
    driver: bridge
volumes:
  rabbitmq-data:

services:
  rabbitmq:
    image: rabbitmq:3.12-management
    hostname: rabbitmq
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASS}
      # Enable MQTT plugin so scada-api can publish via MQTT 1883
      RABBITMQ_PLUGINS: rabbitmq_management,rabbitmq_mqtt
    ports:
      - "5672:5672"             # AMQP — exposed to TMS VM
      - "15672:15672"           # admin UI — exposed to Monitor VM
      - "127.0.0.1:1883:1883"   # MQTT — bound to localhost only (scada-api uses it)
    volumes:
      - rabbitmq-data:/var/lib/rabbitmq
    networks: [scada]
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 30s
      timeout: 30s
      retries: 5
      start_period: 60s

  scada-api:
    image: pinkline/pas-scada-api:latest
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      MQTT_HOST: rabbitmq
      MQTT_PORT: "1883"
      MQTT_USER: ${RABBITMQ_USER}
      MQTT_PASS: ${RABBITMQ_PASS}
      AMQP_HOST: rabbitmq
      AMQP_USER: ${RABBITMQ_USER}
      AMQP_PASS: ${RABBITMQ_PASS}
    ports:
      - "8091:8091"             # dashboard / API — exposed to Monitor + ops
    networks: [scada]
    restart: unless-stopped
```

Create `deploy\scada-vm\.env`:
```powershell
cd C:\pinkline\PAS-SCADA-Kafka-Bridge\deploy\scada-vm
Set-Content -Path .env -Encoding ascii -Value @"
RABBITMQ_USER=thiru
RABBITMQ_PASS=<strong-password-here>
"@
icacls .env /inheritance:r /grant:r "$env:USERNAME:(R,W)"
```

> **Cross-check env keys** with `external-scada/k8s/60-scada-api-secret.yaml`
> and `external-scada/k8s/70-scada-api-deployment.yaml`. If the scada-api
> image expects different names (e.g. `MQTT_BROKER_HOST` instead of
> `MQTT_HOST`), use the names from those YAMLs.

---

## 7 · Start the SCADA stack

```powershell
cd C:\pinkline\PAS-SCADA-Kafka-Bridge\deploy\scada-vm
docker compose up -d
docker compose ps
```

Wait ~60s for RabbitMQ first-boot, then verify the MQTT plugin is on:
```powershell
docker compose exec rabbitmq rabbitmq-plugins list
# expect: rabbitmq_mqtt        [E*] (enabled)
# expect: rabbitmq_management  [E*] (enabled)
```

---

## 8 · Declare the alarm queue + binding

One-shot, idempotent:

```powershell
docker compose exec rabbitmq rabbitmqadmin --username=$env:RABBITMQ_USER --password=$env:RABBITMQ_PASS `
  declare queue name=scada.tms.alarms.queue durable=true auto_delete=false

docker compose exec rabbitmq rabbitmqadmin --username=$env:RABBITMQ_USER --password=$env:RABBITMQ_PASS `
  declare binding source=amq.topic destination=scada.tms.alarms.queue routing_key=scada.tms.alarms
```

If `$env:RABBITMQ_USER` isn't set in your shell, substitute the literal
values from your `.env`.

> **Why `rabbitmqadmin` and not the curl-pod approach from `MANUAL-RUN.md`?**
> `rabbitmqadmin` runs entirely inside the RabbitMQ container and avoids
> the antivirus-vs-spawned-PowerShell issue we hit on Windows during
> dev (e.g. K7 Total Security flagging spawned shells as suspicious).

---

## 9 · Verify

```powershell
# 1. RabbitMQ ports are listening on the right interfaces
docker compose ps                                     # rabbitmq + scada-api Up

# 2. scada-api is connected to RabbitMQ over MQTT
Invoke-RestMethod http://localhost:8091/api/status
# expect: mqtt_connected = true

# 3. Queue + binding exist
$cred = New-Object System.Management.Automation.PSCredential($env:RABBITMQ_USER, (ConvertTo-SecureString $env:RABBITMQ_PASS -AsPlainText -Force))
Invoke-RestMethod -Uri http://localhost:15672/api/queues/%2F -Credential $cred `
  | Select-Object name,messages
# expect: scada.tms.alarms.queue listed

# 4. Cross-VM port exposed (run from TMS VM, not here)
#    On TMS VM: nc -vz <SCADA_VM_IP> 5672    # expect: succeeded
```

Open in browser:
- `http://localhost:15672` — RabbitMQ admin (login with `RABBITMQ_USER`/`RABBITMQ_PASS`).
  Queues tab → `scada.tms.alarms.queue` should be listed.
- `http://localhost:8091` — scada-api dashboard. The right pane
  "SCADA → TMS" auto-publishes UpdateAlarm / KeepAlive every 10–120s.

---

## 10 · Stop / restart / wipe

```powershell
cd C:\pinkline\PAS-SCADA-Kafka-Bridge\deploy\scada-vm

# Stop one
docker compose stop scada-api

# Restart one (e.g. after rebuild)
docker compose up -d --force-recreate --no-deps scada-api

# Stop everything (preserve queue data)
docker compose down

# Stop AND wipe RabbitMQ data — DESTRUCTIVE (loses queues + msgs)
docker compose down -v
```

---

## 11 · Common issues

| Symptom | Fix |
|---|---|
| `docker compose up` says `port is already allocated` for 5672 / 15672 | Another RabbitMQ already running. `docker ps` to find it; stop or remove. |
| `mqtt_connected: false` in `/api/status` | MQTT plugin not enabled. `docker compose exec rabbitmq rabbitmq-plugins enable rabbitmq_mqtt`, then restart scada-api: `docker compose restart scada-api`. |
| RabbitMQ pod restarts with **"Liveness probe failed: rabbitmq-diagnostics -q ping timed out after 15s"** on first boot | Cosmetic — RabbitMQ takes >15s to become diagnosable on first boot. The `start_period: 60s` in the healthcheck above is the fix; the pod stabilizes after one restart. Move on if it ends up healthy. |
| scada-api logs `MQTT auth failure (rc=4)` | Wrong creds. Make sure `MQTT_USER`/`MQTT_PASS` (or whatever keys the image uses) match `RABBITMQ_USER`/`RABBITMQ_PASS`. |
| TMS VM can't connect to 5672 (timeout) | Windows firewall is blocking. Re-check the `New-NetFirewallRule` in §5. Test from TMS VM: `nc -vz <SCADA_VM_IP> 5672`. |
| TMS VM gets `ACCESS_REFUSED` connecting to 5672 | Creds mismatch. The TMS VM's `.env` must use the same `RABBITMQ_USER`/`PASS` as set here. |
| Monitor VM can't probe `15672` | Firewall blocks. Re-check `New-NetFirewallRule` for 15672 in §5. |
| Queue declare fails with antivirus error (`EPERM uv_spawn`) | Some AVs block `kubectl run --rm` curl pods. Method shown above (`rabbitmqadmin` inside the container) avoids that — it doesn't spawn anything host-side. |
| 8091 dashboard shows old UI after rebuild | Old Docker container shadowing. `docker ps` for any rogue `pas-scada-api` outside this compose stack and `docker rm -f` it. |
