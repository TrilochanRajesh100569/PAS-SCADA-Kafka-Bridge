"""PAS-SCADA Live Demo — FastAPI app with two pages.

Pages:
  /          → Live Data table (real-time messages, auto-update via SSE)
  /flow      → Live Flow diagram (animated architecture, packets fly through)

Modes (set via MODE env var):
  synthetic  → app generates fake messages every 2–3s. Always works offline.
  live       → app proxies the bridge's /api/messages/stream SSE.
               Set BRIDGE_URL=http://<bridge-host>:8085 to point at the real one.

Both pages subscribe to the same /api/stream SSE on this app.
"""

import asyncio
import collections
import json
import logging
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ── Config ────────────────────────────────────────────────────────

MODE        = os.getenv("MODE", "synthetic").lower()          # "synthetic" or "live"
BRIDGE_URL  = os.getenv("BRIDGE_URL", "http://localhost:8085").rstrip("/")
MONITOR_URL = os.getenv("MONITOR_URL",
              "http://pas-scada-monitor.pinkline.svc.cluster.local:8080").rstrip("/")
PORT        = int(os.getenv("PORT", "8090"))
INTERVAL    = float(os.getenv("SYNTH_INTERVAL_S", "2.5"))     # synthetic msg rate
HISTORY     = int(os.getenv("HISTORY", "100"))                 # last-N kept for replay

logging.basicConfig(level="INFO", format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("demo")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ── Message bus (shared between producer + SSE subscribers) ──────

class Bus:
    """Tiny in-memory pub/sub. Holds last N messages for replay on connect."""

    def __init__(self, max_size: int = 100):
        self.history: collections.deque = collections.deque(maxlen=max_size)
        self.subscribers: list[asyncio.Queue] = []
        self.id = 0

    def publish(self, topic: str, body: dict):
        self.id += 1
        msg = {
            "id": self.id,
            "receivedAt": now_iso(),
            "topic": topic,
            "json": json.dumps(body, separators=(",", ":")),
        }
        self.history.append(msg)
        for q in list(self.subscribers):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                pass
        return msg

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self.subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        try:
            self.subscribers.remove(q)
        except ValueError:
            pass


bus = Bus(max_size=HISTORY)


# ── Synthetic message generator ──────────────────────────────────
# Realistic shapes matching the ICD JSON envelope produced by the bridge's
# XmlToJsonProcessor — same fields, same names, varied content.

PLATFORMS = ["PL2201", "PL2202", "PL2203", "PL2204", "PL2205", "PL3101", "PL3102"]
TRAIN_GUIDS = [
    "dc-occ.eclrt-train-{}-guid".format(i) for i in range(6200, 6260)
]


def _envelope() -> dict:
    seq = random.randint(0, 255)
    return {
        "schemaVersion": "1.0",
        "messageType": "TMS_PAS_UPDATE",
        "timestamp": now_iso(),
        "header": {
            "server": "RCS.E2K.PIS",
            "version": "1.0",
            "health": 1,
            "healthSeq": seq,
            "statusUpdateAll": False,
        },
        "trains": [],
    }


def _make_atr_timetable() -> dict:
    e = _envelope()
    pl = random.choice(PLATFORMS)
    e["platformPredictions"] = [{
        "platformId": pl,
        "predictedTrains": [{
            "slot": 1,
            "trainId": random.choice(TRAIN_GUIDS),
            "arrivalTime": random.randint(30, 600),
            "departureTime": "{:02d}:{:02d}:00".format(random.randint(8, 22), random.randint(0, 59)),
            "status": 0,
            "destination": random.choice(PLATFORMS),
            "serviceState": 1,
            "runNumber": random.randint(100, 999),
        }],
    }]
    e["blockOccupancies"] = []
    e["gateCommands"] = []
    return e


def _make_arrival() -> dict:
    e = _envelope()
    pl = random.choice(PLATFORMS)
    e["platformPredictions"] = [{
        "platformId": pl,
        "predictedTrains": [{
            "slot": 1,
            "trainId": random.choice(TRAIN_GUIDS),
            "arrivalTime": 0,
            "departureTime": "",
            "status": 1,                       # at platform
            "destination": "",
            "serviceState": 1,
            "runNumber": random.randint(100, 999),
        }],
    }]
    e["blockOccupancies"] = []
    e["gateCommands"] = []
    return e


def _make_departure() -> dict:
    e = _envelope()
    pl = random.choice(PLATFORMS)
    e["platformPredictions"] = [{
        "platformId": pl,
        "predictedTrains": [{
            "slot": 1,
            "trainId": random.choice(TRAIN_GUIDS),
            "arrivalTime": 0,
            "departureTime": "{:02d}:{:02d}:00".format(random.randint(8, 22), random.randint(0, 59)),
            "status": 2,                       # departed
            "destination": "",
            "serviceState": 1,
            "runNumber": random.randint(100, 999),
        }],
    }]
    e["blockOccupancies"] = []
    e["gateCommands"] = []
    return e


def _make_route_info() -> dict:
    e = _envelope()
    e["platformPredictions"] = []
    e["blockOccupancies"] = []
    e["gateCommands"] = []
    e["routeInfo"] = {
        "trainId": str(random.randint(6200, 6260)),
        "trainGUID": random.choice(TRAIN_GUIDS),
        "route": random.sample(PLATFORMS, k=3),
    }
    return e


GENERATORS = [
    ("tms.scada.pas",                  "ATRTimeTable",    _make_atr_timetable, 0.5),
    ("tms.scada.pas",                  "SingleArrival",   _make_arrival,       0.2),
    ("tms.scada.pas",                  "SingleDeparture", _make_departure,     0.2),
    ("tms.scada.pas.routeinfo",        "RouteInfo",       _make_route_info,    0.1),
]


def _pick_generator():
    r = random.random()
    cum = 0.0
    for entry in GENERATORS:
        cum += entry[3]
        if r <= cum:
            return entry
    return GENERATORS[-1]


async def synthetic_loop():
    """Background task — emits a fake message every INTERVAL seconds."""
    log.info("synthetic mode: emitting fake messages every %.1fs", INTERVAL)
    while True:
        try:
            topic, _kind, fn, _w = _pick_generator()
            bus.publish(topic, fn())
        except Exception as e:
            log.exception("synthetic emit failed: %s", e)
        # Jitter ±20% so the demo doesn't look perfectly metronomic
        await asyncio.sleep(INTERVAL * random.uniform(0.8, 1.2))


# ── Synthetic monitor state (used by Tab 1 in synthetic mode) ──
# Same component list as monitor/k8s/10-configmap.yaml so the demo
# looks identical to the real monitor when comparing side-by-side.

SYNTH_COMPONENTS = [
    "pas-scada-bridge",
    "artemis-openwire",
    "artemis-console",
    "kafka",
    "zookeeper",
    "rabbitmq-mgmt",
    "rabbitmq-amqp",
    "rabbitmq-mqtt",
    "scada-api",
    "scada-api-mqtt-link",
]

# Mutable in-memory state (mirrors what monitor's /state returns)
synth_monitor_state: dict = {}


def _init_synth_monitor():
    base = now_iso()
    for name in SYNTH_COMPONENTS:
        synth_monitor_state[name] = {
            "current_state": "UP",
            "consecutive_passes": random.randint(40, 200),
            "consecutive_fails": 0,
            "last_ok_at": base,
            "last_fail_at": None,
            "last_error": None,
            "last_state_change": base,
            "first_fail_at": None,
        }


async def synth_monitor_loop():
    """Every ~25s pick a random component to flip DOWN for 20-45s.
    Keeps the demo dynamic instead of every card being green forever.
    """
    log.info("synthetic monitor: %d components, periodic outages", len(SYNTH_COMPONENTS))
    while True:
        await asyncio.sleep(random.uniform(20, 35))
        # Skip if something is already DOWN
        if any(s["current_state"] == "DOWN" for s in synth_monitor_state.values()):
            continue
        # Bring one component DOWN
        victim = random.choice(SYNTH_COMPONENTS)
        s = synth_monitor_state[victim]
        ts = now_iso()
        err = random.choice([
            "ConnectionError: timed out",
            "HTTP 503 (status=DOWN)",
            "TimeoutError: timed out",
        ])
        s.update({
            "current_state": "DOWN",
            "consecutive_fails": random.randint(3, 6),
            "consecutive_passes": 0,
            "last_fail_at": ts,
            "last_error": err,
            "last_state_change": ts,
            "first_fail_at": ts,
        })
        log.info("synthetic monitor: %s went DOWN", victim)
        # Down for a while, then recover
        await asyncio.sleep(random.uniform(20, 45))
        ts2 = now_iso()
        s.update({
            "current_state": "UP",
            "consecutive_fails": 0,
            "consecutive_passes": 1,
            "last_ok_at": ts2,
            "last_error": None,
            "last_state_change": ts2,
            "first_fail_at": None,
        })
        log.info("synthetic monitor: %s recovered", victim)


# ── Live mode — proxy bridge's SSE into our bus ──────────────────

async def live_loop():
    """Background task — subscribes to bridge SSE and re-publishes into our bus.

    Reconnects forever on errors so the demo survives a bridge restart.
    """
    url = f"{BRIDGE_URL}/api/messages/stream"
    log.info("live mode: subscribing to %s", url)
    while True:
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", url) as r:
                    if r.status_code != 200:
                        log.warning("bridge SSE returned HTTP %d — retrying in 5s", r.status_code)
                        await asyncio.sleep(5)
                        continue
                    log.info("bridge SSE connected")
                    data_lines: list[str] = []
                    async for line in r.aiter_lines():
                        # SSE frame format:  data: <json>   \n   <blank line>
                        if line.startswith("data:"):
                            data_lines.append(line[5:].lstrip())
                        elif line == "":
                            if data_lines:
                                payload = "\n".join(data_lines)
                                data_lines = []
                                try:
                                    msg = json.loads(payload)
                                    body = msg.get("json")
                                    if isinstance(body, str):
                                        try:
                                            body = json.loads(body)
                                        except Exception:
                                            body = {"raw": body}
                                    bus.publish(msg.get("topic", "unknown"), body or {})
                                except Exception as e:
                                    log.warning("bad SSE payload: %s", e)
        except Exception as e:
            log.warning("bridge SSE connection failed (%s) — retrying in 5s", e)
            await asyncio.sleep(5)


# ── App lifecycle ────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = []
    if MODE == "live":
        tasks.append(asyncio.create_task(live_loop()))
    else:
        tasks.append(asyncio.create_task(synthetic_loop()))
        _init_synth_monitor()
        tasks.append(asyncio.create_task(synth_monitor_loop()))
    log.info("PAS-SCADA Demo started — mode=%s port=%d", MODE, PORT)
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


app = FastAPI(title="PAS-SCADA Live Demo", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


# ── Pages ────────────────────────────────────────────────────────

@app.get("/")
async def page_monitor(request: Request):
    """Tab 1 — Component health monitor."""
    return templates.TemplateResponse("monitor.html", {
        "request": request,
        "mode": MODE,
        "source_url": MONITOR_URL if MODE == "live" else None,
    })


@app.get("/flow")
async def page_flow(request: Request):
    """Tab 2 — Live Flow diagram."""
    return templates.TemplateResponse("flow.html", {
        "request": request,
        "mode": MODE,
        "source_url": BRIDGE_URL if MODE == "live" else None,
    })


# ── API ──────────────────────────────────────────────────────────

@app.get("/api/monitor")
async def api_monitor():
    """Tab 1 data source — component health snapshot.

    Live mode  : fetches MONITOR_URL/state from the existing monitor pod
                 (no duplication — single source of truth).
    Synthetic  : returns the in-memory simulated state.
    """
    if MODE == "live":
        url = f"{MONITOR_URL}/state"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(url)
                if r.status_code == 200:
                    return JSONResponse(r.json())
                return JSONResponse(
                    {"error": f"monitor returned HTTP {r.status_code}"},
                    status_code=502,
                )
        except Exception as e:
            return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=502)
    return JSONResponse(synth_monitor_state)


@app.get("/api/recent")
async def api_recent():
    """Last N messages (for page load — replay before SSE catches up)."""
    return JSONResponse(list(bus.history))


@app.get("/api/stream")
async def api_stream():
    """Server-Sent Events stream — one message per `data:` frame."""
    q = bus.subscribe()

    async def gen():
        try:
            # Replay the last 20 messages so a fresh tab isn't empty
            for msg in list(bus.history)[-20:]:
                yield f"data: {json.dumps(msg)}\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=25)
                    yield f"data: {json.dumps(msg)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            bus.unsubscribe(q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "mode": MODE, "messages": bus.id}


# ── Entrypoint ───────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info", access_log=False)
