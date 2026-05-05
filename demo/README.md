# PAS-SCADA Live Demo

A FastAPI app for showing the pipeline to a customer. Two pages, real-time
data, dark theme to match the rest of the docs.

## Pages

| URL | What it shows |
|---|---|
| `/`     | **🩺 Monitor** — green/red component cards (UP/DOWN/last-error). Polls `/api/monitor` every 3s. In live mode this proxies the existing `monitor/` app's `/state`. In synthetic mode the demo simulates outages (one component briefly DOWN every ~30s) so the demo isn't static. |
| `/flow` | **⚡ Live Flow** — animated architecture (TMS → Artemis → Bridge → Kafka → RabbitMQ → SCADA). One packet flies through per real message; per-node counters; "last message" detail panel below. |

The Monitor tab does **not** duplicate the existing `monitor/` app — it
just fetches its `/state` and renders it in the demo's theme so the
customer sees one cohesive UI.

## Two modes

| `MODE=` | Behaviour | When to use |
|---|---|---|
| `synthetic` (default) | App generates realistic fake messages every ~2.5s + simulates a component going DOWN every ~30s on the Monitor tab | Rehearsing, offline laptop demo, no bridge available |
| `live` | Flow tab proxies bridge's `/api/messages/stream` SSE; Monitor tab fetches the real `monitor/` app's `/state` | Real customer demo where bridge + monitor are reachable |

In `live` mode set:
* `BRIDGE_URL=http://<bridge-host>:8085` (default `http://localhost:8085`)
* `MONITOR_URL=http://<monitor-host>:8080` (default `http://pas-scada-monitor.pinkline.svc.cluster.local:8080`)

## Run locally — synthetic mode (always works)

```bash
cd demo
pip install -r requirements.txt
python app.py
# open http://localhost:8090/
```

You should see:
* The header pill says `🟣 SYNTHETIC`
* Within ~3s the table fills with messages and the counters tick
* `/flow` shows packets flying through the architecture every couple of seconds

## Run locally — live mode against a running bridge

```bash
MODE=live BRIDGE_URL=http://10.4.0.23:8085 python app.py
```

Header pill switches to `🔴 LIVE` and the connection bridge URL is shown.

## Build the image

```bash
cd demo
docker build -t pinkline/pas-scada-demo:1.0.0 .
docker push pinkline/pas-scada-demo:1.0.0
```

## Deploy to k8s

```bash
kubectl apply -f demo/k8s/
kubectl -n pinkline port-forward svc/pas-scada-demo 8090:8090
# open http://localhost:8090/
```

Switch to live mode by editing the ConfigMap:

```yaml
data:
  MODE:       "live"
  BRIDGE_URL: "http://pas-scada-bridge.pinkline.svc.cluster.local:8085"
```

…then `kubectl rollout restart deploy/pas-scada-demo`.

## Files

```
demo/
  app.py                  ← FastAPI app (single file)
  requirements.txt        ← fastapi, uvicorn, httpx, jinja2
  Dockerfile
  README.md               ← this file
  templates/
    base.html             ← shared layout: nav, theme, status pill
    messages.html         ← Page 1 — Live Data table
    flow.html             ← Page 2 — Flow Diagram animation
  static/
    style.css             ← shared dark theme
  k8s/
    10-configmap.yaml     ← MODE + BRIDGE_URL
    20-deployment.yaml    ← Deployment + Service (ClusterIP :8090)
```

## How it works

```
            ┌──────────────────────────────────────────┐
  browser ◀─┤  GET /        (Live Data page, HTML)      │
  browser ◀─┤  GET /flow    (Flow Diagram page, HTML)   │
  browser ◀─┤  GET /api/stream  (SSE — feeds both pages)│
  browser   │  GET /api/recent  (last 100 messages JSON)│
  browser   │  GET /healthz                              │
            └──────────────────┬───────────────────────┘
                               │
                  bus (in-memory pub/sub, last 100 msgs)
                               ▲
                               │ publish
                ┌──────────────┴──────────────┐
                │                              │
   synthetic_loop()                live_loop()
   • emits fake ICD JSON           • streams from bridge
     every 2-3s                       /api/messages/stream
   • realistic shapes:                via httpx
     ATRTimeTable / Arrival /
     Departure / RouteInfo
```

## Customer-demo tips

* **Open `/flow` first** — the animation is the visual hook.
* Switch to `/` to show the actual JSON content. Click any row to expand.
* In the room, mention each color = a different message type. The legend is in the per-type stat cards on the Live Data page.
* If the bridge link drops mid-demo, the demo app keeps running — synthetic mode can be flipped on by toggling `MODE` in the env. (Currently requires a restart; future tweak: hot-toggle endpoint.)
