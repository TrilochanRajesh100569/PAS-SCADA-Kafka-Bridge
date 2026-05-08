# Morning start — bring the system back up after shutdown

Daily startup routine. Open this file every morning, copy-paste each block in order.

> **First time on this PC?** Run [FRESH-PC-SETUP.md](./FRESH-PC-SETUP.md)
> instead — that does the full bring-up via `start.sh`. This file is for
> day-2-onwards, when minikube + Docker images already exist.

---

## Step 1 · Wait for Docker Desktop

After login, look for the **whale icon** in the system tray (bottom-right
of taskbar). Wait until it stops animating and shows "Docker Desktop is
running" on hover. Usually 30 seconds.

If it doesn't auto-start, open it from the Start menu and wait.

---

## Step 2 · Start minikube

Open **PowerShell** and run:

```powershell
& $env:USERPROFILE\minikube.exe start
```

Wait until you see:
```
Done! kubectl is now configured to use "minikube" cluster
```
~1 minute.

---

## Step 3 · Make sure Artemis is up

Artemis runs as a host Docker container (not a k8s pod), so it needs a
separate check:

```powershell
docker ps --filter name=artemis
```

Expected: one row with `STATUS = Up`. If it's missing or stopped:

```powershell
& 'C:\Program Files\Docker\Docker\resources\bin\docker.exe' start artemis
```

If the container doesn't exist at all (`docker ps -a --filter name=artemis`
returns nothing), `messaging-infra` was never brought up on this PC — go
back to [FRESH-PC-SETUP.md](./FRESH-PC-SETUP.md) Step 4.

---

## Step 4 · Wait for pods (~2 minutes)

Run this and confirm all pods say `1/1 Running`:

```powershell
kubectl get pods -n pinkline
kubectl get pods -n scada
```

If some say `0/1 Running` or `ContainerCreating`, wait 30 seconds and
re-run. The **bridge takes ~100 seconds** to fully boot — be patient.

Re-run until everything shows `1/1 Running`.

---

## Step 5 · Start port-forwards (so browser works)

Copy this **whole block** into PowerShell and press Enter:

```powershell
$svcs = @(
  @{ns='pinkline'; svc='pas-scada-monitor';   ports='8080:8080'},
  @{ns='pinkline'; svc='pas-scada-demo';      ports='8090:8090'},
  @{ns='pinkline'; svc='pas-scada-bridge';    ports='8085:8085'},
  @{ns='pinkline'; svc='kafdrop';             ports='9000:9000'},
  @{ns='pinkline'; svc='kafka-connect';       ports='8083:8083'},
  @{ns='scada';    svc='rabbitmq-internal';   ports='15672:15672'},
  @{ns='scada';    svc='rabbitmq-internal';   ports='1883:1883'},
  @{ns='scada';    svc='scada-api-internal';  ports='8091:8091'}
)
foreach ($s in $svcs) {
  Start-Process powershell -ArgumentList '-NoExit','-Command',"kubectl -n $($s.ns) port-forward svc/$($s.svc) $($s.ports)" -WindowStyle Minimized
}
```

8 minimized PowerShell windows will appear in your taskbar. **Don't
close them** — each one holds a port-forward.

---

## Step 6 · Open in browser

| URL | What |
|---|---|
| http://localhost:8080 | Health monitor (start here) |
| http://localhost:8091 | SCADA simulator |
| http://localhost:8090 | Demo (data table) |
| http://localhost:9000 | Kafdrop (Kafka topics) |
| http://localhost:15672 | RabbitMQ admin (login: `thiru` / `password`) |
| http://localhost:8161/console | Artemis console (login: `admin` / `admin`) |

Wait ~30 seconds after opening http://localhost:8080 — most tiles
should be green.

---

## When you're done for the day

Stop the port-forwards:

```powershell
Get-Process powershell | Where-Object { $_.MainWindowTitle -like '*kubectl*' } | Stop-Process
```

Then shut down Windows normally. minikube + Docker survive shutdown
fine — your pods, topics, queues, and connectors all come back next
morning when you re-run Step 2.

---

## If something is broken in the morning

| Problem | Fix |
|---|---|
| Some pod stuck `0/1 Running` for 5+ min | `kubectl -n pinkline rollout restart deploy/<name>` |
| Bridge in `CrashLoopBackOff` | Bridge needs RabbitMQ. Check rabbitmq pod is `1/1 Running` first. Then restart bridge. |
| Browser says "site can't be reached" on localhost:8080 etc. | Port-forwards died. Re-run Step 5. |
| Port-forward log shows "connection refused" | The pod restarted while the forward was open. Close that minimized window and re-run that single line from Step 5. |
| Artemis container missing | `& 'C:\Program Files\Docker\Docker\resources\bin\docker.exe' start artemis` |
| **Dashboard shows old UI / wrong build marker** | Leftover host docker container shadowing the port-forward. See section below. |
| Kafka pod in `Error` with `InconsistentClusterIdException` | See **Kafka cluster ID mismatch** below. |
| Everything is wrong, nuke and restart | `& $env:USERPROFILE\minikube.exe stop` then start again |

---

## Old UI on a dashboard (8080 / 8090 / 8091)

If a dashboard shows the wrong layout or stale data even after a fresh
build, a host Docker container is bound to that port and shadowing the
kubectl port-forward — your browser hits the old container instead of
the freshly-deployed pod.

Check:
```powershell
docker ps | findstr /i "pas-scada external-scada"
```

Kill any matches:
```powershell
docker rm -f pas-scada-api pas-scada-monitor pas-scada-demo external-scada-scada-api
```

Then re-run the relevant port-forward line from Step 5.

**Canary check:** the SCADA dashboard at http://localhost:8091 prints a
build marker like `[BUILD-MARKER-A1]` near the title. Compare against
`external-scada/scada-api/static/dashboard.html:363` — if they don't
match, the running container is stale.

---

## Kafka cluster ID mismatch (one-time recovery)

If the kafka pod logs show `InconsistentClusterIdException`, kafka and
zookeeper disagree on which cluster they belong to. With the PVC fix in
`tms/k8s/20-zookeeper.yaml` this should not recur, but if you hit it
once (e.g. on an older deployment), wipe kafka's stored cluster ID:

```powershell
kubectl -n pinkline scale deploy kafka --replicas=0
kubectl -n pinkline scale deploy kafka-connect --replicas=0
kubectl -n pinkline scale deploy pas-scada-bridge --replicas=0
kubectl -n pinkline run kafka-wipe --rm -i --restart=Never --image=busybox `
  --overrides='{\"spec\":{\"containers\":[{\"name\":\"w\",\"image\":\"busybox\",\"command\":[\"sh\",\"-c\",\"rm -rf /data/* /data/.[!.]* 2>/dev/null; echo done\"],\"volumeMounts\":[{\"name\":\"d\",\"mountPath\":\"/data\"}]}],\"volumes\":[{\"name\":\"d\",\"persistentVolumeClaim\":{\"claimName\":\"kafka-data\"}}]}}'
kubectl -n pinkline scale deploy kafka --replicas=1
# wait ~60s for kafka to be 1/1 Running, then:
kubectl -n pinkline scale deploy kafka-connect --replicas=1
kubectl -n pinkline scale deploy pas-scada-bridge --replicas=1
kubectl -n pinkline delete job kafka-connect-register --ignore-not-found
kubectl apply -f connect/k8s/40-job-register.yaml
```

Topic data is wiped, but topics auto-recreate and the bridge republishes
fresh data from RabbitMQ. Safe for dev.
