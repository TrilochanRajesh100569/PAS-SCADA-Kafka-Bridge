"""
Mock SCADA API — full simulator + dashboard.

Behaviour:
  * Receives encrypted TMS messages from MQTT topic tms/scada/pas,
    decrypts with SCADA_AES_KEY, stores for viewing.
  * Publishes UpdateAlarm messages on a configurable interval
    (simulates equipment state changes).
  * Publishes KeepAlive on a configurable interval
    (configurable at runtime via /config endpoint or dashboard).
  * Handles GetAllAlarms requests (responds with SendAllAlarms).
  * Exposes a visual HTML dashboard at /.

Endpoints:
  GET  /                 → HTML dashboard (live visual view)
  GET  /api/status       → connection + timer config + stats
  GET  /api/received     → last 100 decrypted messages from TMS
  GET  /api/sent         → last 100 messages SCADA published outbound
  GET  /api/keepalives   → KeepAlive history
  GET  /api/alarms       → current in-memory alarm state table
  GET  /api/stream       → SSE stream of all events (received + sent)
  POST /api/config       → update runtime config (keepalive interval etc.)
  POST /api/publish      → manually publish an alarm (body: {id, state})
  POST /api/pause        → pause KeepAlive (simulate connection cut)
  POST /api/resume       → resume KeepAlive
  POST /api/sendall      → trigger a SendAllAlarms broadcast

ENV vars:
  SCADA_AES_KEY   Base64 32-byte key for TMS decrypt
  MQTT_HOST       default rabbitmq
  MQTT_PORT       default 1883
  MQTT_USER       default thiru
  MQTT_PASS       default password
  MQTT_TOPIC_IN   default tms/scada/pas      (TMS→SCADA, encrypted)
  MQTT_TOPIC_OUT  default scada/tms/alarms   (SCADA→TMS, plain JSON,
                                              RSAE envelope — TMS team
                                              consumes via RabbitMQ AMQP
                                              on amq.topic routing key
                                              scada.tms.alarms)
  SCADA_CREATOR_ID default ScateX
"""
import base64
import binascii
import json
import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone
from queue import Queue, Empty

import paho.mqtt.client as mqtt
import requests
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from flask import Flask, Response, jsonify, request, send_from_directory


# ── Config (env + runtime-mutable) ─────────────────────────────────────
MQTT_HOST  = os.getenv("MQTT_HOST",  "10.4.0.10")
MQTT_PORT  = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER  = os.getenv("MQTT_USER",  "admin")
MQTT_PASS  = os.getenv("MQTT_PASS",  "admin")

# Inbound: TMS→SCADA (encrypted PIS/alarm data bridged from TMS)
TOPIC_IN   = os.getenv("MQTT_TOPIC_IN",  "tms/scada/pas")
# Outbound: SCADA→TMS — SINGLE topic carries all 4 RSAE message types.
# The `Type` field inside the JSON envelope distinguishes them:
#   UpdateAlarm / SendAllAlarms / GetAllAlarms / KeepAlive
# MQTT publish lands in RabbitMQ via the MQTT plugin automatically —
# AMQP subscribers see it on amq.topic with routing key scada.tms.alarms
# (plugin converts / to . on the way in).
TOPIC_OUT  = os.getenv("MQTT_TOPIC_OUT", "scada/tms/alarms")

CREATOR_ID = os.getenv("SCADA_CREATOR_ID", "ScateX")

AES_KEY_B64 = os.getenv("SCADA_AES_KEY", "k7Qh2NfT8vR0mC9aXy4pLwZbE3sG6uJtH1iKd5oArMw=")
AES_KEY = base64.b64decode(AES_KEY_B64) if AES_KEY_B64 else None

# Runtime-mutable config — EVERY value below is editable live via
# POST /api/config and takes effect immediately. Nothing is a restart
# requirement. Dashboard reflects changes via SSE.
runtime = {
    # ── Timer intervals ─────────────────────────
    "keepalive_seconds":        30,
    "auto_alarm_seconds":       10,     # 0 = disabled
    "auto_alarm_enabled":       True,
    "keepalive_paused":         False,
    "send_all_alarms_seconds":  60,     # 0 = disabled; auto-broadcast all alarms
    "send_all_alarms_enabled":  True,
    "get_all_alarms_seconds":   120,    # 0 = disabled; proactively ask TMS for all alarms
    "get_all_alarms_enabled":   True,

    # ── Identity & topics (mutable) ─────────────
    "creator_id":               CREATOR_ID,
    "topic_out":                TOPIC_OUT,
    "topic_in":                 TOPIC_IN,

    # ── Equipment list (mutable via /api/config) ─
    "equipment":                [],     # filled below after class defs

    # ── Runtime status (read-only from dashboard) ─
    "mqtt_connected":           False,
    "last_keepalive_sent":      None,
    "last_tms_received":        None,
    "last_alarm_sent":          None,
    "last_send_all_sent":       None,
    "last_get_all_sent":        None,
}

# ── In-memory stores ──────────────────────────────────────────────────
messages_in  = deque(maxlen=100)
messages_out = deque(maxlen=100)
keepalives   = deque(maxlen=50)
# Current alarm state table — key = alarm.Id, value = full alarm dict
alarm_db: dict[str, dict] = {}

stats = {
    "received":     0,
    "sent_alarm":   0,
    "sent_ka":      0,
    "sent_all":     0,
    "sent_get_all": 0,
    "decrypt_ok":   0,
    "decrypt_fail": 0,
    "dedup_skipped": 0,
}

# Last known state per alarm Id — used to drop unchanged repeats from
# the inbound MQTT stream (flood-control: snapshot replays after a
# reconnect can re-send the same state SCADA already has).
# value: (state, timestamp_str)
last_seen_alarm: dict[str, tuple[str, str]] = {}

sse_queues: list[Queue] = []

# Default equipment list — used to seed runtime["equipment"]
# Fully mutable via POST /api/config {"equipment": [...]}
DEFAULT_EQUIPMENT = [
    "ESC_ROL_1_23", "ESC_ROL_2_27", "ESC_ROL_3_15",
    "DOOR_PLT_2_3", "DOOR_PLT_1_1", "DOOR_STN_1_MAIN",
    "ELV_STN_4_01", "ELV_STN_2_03",
    "DNVEL01.ALC.ED06", "DNVEL02.ALC.ED06",
    "PUMP_DRY_1", "PUMP_DRY_2",
    "HVAC_PLT_1", "FIRE_DETECT_OFFICE_A",
    "CCTV_PLT_2_CAM5",
]
runtime["equipment"] = list(DEFAULT_EQUIPMENT)

# Rolling-window rate tracking (60-sec window for msgs/sec calc)
_rate_window_in  = deque(maxlen=600)   # timestamps of received msgs
_rate_window_out = deque(maxlen=600)   # timestamps of sent msgs

app = Flask(__name__, static_folder="static")
mqtt_client: mqtt.Client | None = None


# ── Helpers ───────────────────────────────────────────────────────────
def now_iso():
    return datetime.now(timezone.utc).isoformat()


def now_rsae():
    # RSAE spec timestamp format: YYYY-MM-DD HH:mm:ss.fff
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.") + \
           f"{datetime.now(timezone.utc).microsecond // 1000:03d}"


def broadcast_sse(event_type: str, payload: dict):
    msg = {"type": event_type, "payload": payload, "ts": now_iso()}
    for q in sse_queues[:]:
        try:
            q.put_nowait(msg)
        except Exception:
            pass


def encrypt_payload(plain_obj: dict) -> bytes:
    """AES-256-GCM encrypt + base64 encode — same wire format as decrypt_payload
    (12B IV + ciphertext + 16B GCM tag, base64 wrapped). Mirrors the bridge's
    EncryptProcessor so the reverse path can decrypt SCADA outbound."""
    plain = json.dumps(plain_obj).encode("utf-8")
    if AES_KEY is None:
        return plain
    iv = os.urandom(12)
    ciphertext = AESGCM(AES_KEY).encrypt(iv, plain, None)
    return base64.b64encode(iv + ciphertext)


def is_alarm_state_unchanged(alarm: dict) -> bool:
    """Return True if (Id, State) matches what we last saw with a non-newer
    timestamp. Used to silently drop replayed snapshot records that don't
    change state."""
    aid = alarm.get("Id")
    if not aid:
        return False
    state = str(alarm.get("State", ""))
    ts    = str(alarm.get("Timestamp", ""))
    seen  = last_seen_alarm.get(aid)
    if seen is None:
        return False
    seen_state, seen_ts = seen
    # Drop only when state is identical AND timestamp is not newer.
    return state == seen_state and ts <= seen_ts


def decrypt_payload(raw: bytes) -> dict:
    if AES_KEY is None:
        return {"raw_base64": base64.b64encode(raw).decode(),
                "note": "SCADA_AES_KEY not set — can't decrypt"}
    # Auto-detect base64-encoded payload (Connect pipeline) vs raw bytes
    # (legacy direct MQTT). Base64 contains only printable [A-Za-z0-9+/=].
    try:
        s = raw.decode("ascii")
        if all(c.isalnum() or c in "+/=\r\n" for c in s):
            raw = base64.b64decode(s, validate=True)
    except (UnicodeDecodeError, ValueError, binascii.Error):
        pass  # not base64, treat as raw bytes
    if len(raw) < 28:
        raise ValueError(f"payload too short: {len(raw)} bytes")
    iv, ciphertext = raw[:12], raw[12:]
    plain = AESGCM(AES_KEY).decrypt(iv, ciphertext, None)
    return json.loads(plain.decode("utf-8"))


# ── MQTT handlers ─────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    runtime["mqtt_connected"] = (rc == 0)
    if rc == 0:
        client.subscribe(TOPIC_IN)
        print(f"[scada-api] connected to {MQTT_HOST}:{MQTT_PORT}, sub '{TOPIC_IN}'", flush=True)
        broadcast_sse("connection", {"status": "connected"})
    else:
        print(f"[scada-api] MQTT connect failed rc={rc}", flush=True)
        broadcast_sse("connection", {"status": "failed", "rc": rc})


def on_disconnect(client, userdata, rc):
    runtime["mqtt_connected"] = False
    print(f"[scada-api] MQTT disconnected rc={rc}", flush=True)
    broadcast_sse("connection", {"status": "disconnected", "rc": rc})


def on_message(client, userdata, msg):
    stats["received"] += 1
    _rate_window_in.append(time.time())
    runtime["last_tms_received"] = now_iso()
    entry = {
        "id":         stats["received"],
        "receivedAt": now_iso(),
        "topic":      msg.topic,
        "payloadSize": len(msg.payload),
    }
    try:
        decoded = decrypt_payload(msg.payload)
        entry["decoded"] = decoded
        stats["decrypt_ok"] += 1

        # Flood-control dedup: if this is a replayed UpdateAlarm whose
        # state matches what we've already seen, drop silently.
        alarm = None
        if isinstance(decoded, dict):
            if decoded.get("Type") == "UpdateAlarm":
                alarm = decoded.get("Alarm")
            elif "envelope" in decoded:
                env = decoded.get("envelope") or {}
                if env.get("Type") == "UpdateAlarm":
                    alarm = env.get("Alarm")
        if alarm and is_alarm_state_unchanged(alarm):
            stats["dedup_skipped"] += 1
            entry["dedup_skipped"] = True
            return  # do not store, do not broadcast
        if alarm and alarm.get("Id"):
            last_seen_alarm[alarm["Id"]] = (
                str(alarm.get("State", "")),
                str(alarm.get("Timestamp", "")),
            )

        # If TMS just sent us a GetAllAlarms, respond with SendAllAlarms
        if isinstance(decoded, dict) and decoded.get("Type") == "GetAllAlarms":
            send_all_alarms()
    except Exception as e:
        entry["error"]      = str(e)
        entry["raw_base64"] = base64.b64encode(msg.payload).decode()
        stats["decrypt_fail"] += 1

    messages_in.appendleft(entry)
    broadcast_sse("received", entry)
    print(f"[scada-api] RX {msg.topic} ({len(msg.payload)}b): {entry.get('decoded') or entry.get('error')}", flush=True)


# ── SCADA publisher — the "sending" side ─────────────────────────────
# Outbound (SCADA→TMS) goes plain JSON when SCADA_OUTBOUND_ENCRYPT is
# unset/false. The bridge's bridge.reverse-kafka.encrypt-enabled=false
# matches this so the reverse path passes the bytes through unchanged.
SCADA_OUTBOUND_ENCRYPT = os.getenv("SCADA_OUTBOUND_ENCRYPT", "false").lower() == "true"

def publish(topic: str, envelope: dict, kind: str):
    if mqtt_client is None or not runtime["mqtt_connected"]:
        return False
    if SCADA_OUTBOUND_ENCRYPT:
        payload = encrypt_payload(envelope)
    else:
        payload = json.dumps(envelope).encode("utf-8")
    mqtt_client.publish(topic, payload, qos=0)
    _rate_window_out.append(time.time())
    record = {
        "id":        sum([stats["sent_alarm"], stats["sent_ka"], stats["sent_all"]]),
        "sentAt":    now_iso(),
        "topic":     topic,
        "kind":      kind,
        "envelope":  envelope,
    }
    messages_out.appendleft(record)
    broadcast_sse("sent", record)
    return True


def publish_update_alarm(alarm_id: str, state: str):
    alarm = {
        "Timestamp": now_rsae(),
        "Id":        alarm_id,
        "State":     str(state),
    }
    envelope = {
        "CreatorId": runtime["creator_id"],
        "Type":      "UpdateAlarm",
        "Timestamp": now_rsae(),
        "Alarm":     alarm,
    }
    alarm_db[alarm_id] = alarm
    if publish(runtime["topic_out"], envelope, "UpdateAlarm"):
        stats["sent_alarm"] += 1
        runtime["last_alarm_sent"] = now_iso()
        return True
    return False


def publish_keepalive():
    if runtime["keepalive_paused"]:
        return False
    envelope = {
        "CreatorId": runtime["creator_id"],
        "Type":      "KeepAlive",
        "Timestamp": now_rsae(),
    }
    ok = publish(runtime["topic_out"], envelope, "KeepAlive")
    if ok:
        stats["sent_ka"] += 1
        runtime["last_keepalive_sent"] = now_iso()
        keepalives.appendleft({"sentAt": now_iso(), "envelope": envelope})
    return ok


def send_all_alarms():
    envelope = {
        "CreatorId": runtime["creator_id"],
        "Type":      "SendAllAlarms",
        "Timestamp": now_rsae(),
        "Alarms":    list(alarm_db.values()),
    }
    ok = publish(runtime["topic_out"], envelope, "SendAllAlarms")
    if ok:
        stats["sent_all"] += 1
        runtime["last_send_all_sent"] = now_iso()
    return ok


def publish_get_all_alarms():
    """SCADA proactively requests full alarm state from TMS."""
    envelope = {
        "CreatorId": runtime["creator_id"],
        "Type":      "GetAllAlarms",
        "Timestamp": now_rsae(),
    }
    ok = publish(runtime["topic_out"], envelope, "GetAllAlarms")
    if ok:
        stats["sent_get_all"] += 1
        runtime["last_get_all_sent"] = now_iso()
    return ok


def compute_rate(window: "deque") -> float:
    """Messages per second over the last 60s, live computation."""
    now = time.time()
    recent = [t for t in window if now - t <= 60.0]
    return round(len(recent) / 60.0, 2)


# ── Timers — runtime-configurable intervals ──────────────────────────
def keepalive_timer():
    while True:
        time.sleep(1)
        if runtime["keepalive_paused"] or not runtime["mqtt_connected"]:
            continue
        last = runtime["last_keepalive_sent"]
        last_dt = datetime.fromisoformat(last) if last else None
        if (last_dt is None or
                (datetime.now(timezone.utc) - last_dt).total_seconds()
                >= runtime["keepalive_seconds"]):
            publish_keepalive()


def auto_alarm_timer():
    while True:
        time.sleep(1)
        if (not runtime["auto_alarm_enabled"] or
                not runtime["mqtt_connected"] or
                runtime["auto_alarm_seconds"] <= 0 or
                not runtime["equipment"]):
            continue
        # Sleep rest of the interval (picks up runtime changes on next tick)
        time.sleep(max(0, runtime["auto_alarm_seconds"] - 1))
        if (runtime["auto_alarm_enabled"] and
                runtime["mqtt_connected"] and
                runtime["equipment"]):
            eq = random.choice(runtime["equipment"])
            state = random.choice(["0", "1", "0", "0"])  # skewed toward OK
            publish_update_alarm(eq, state)


def send_all_alarms_timer():
    while True:
        time.sleep(1)
        if (not runtime["send_all_alarms_enabled"] or
                not runtime["mqtt_connected"] or
                runtime["send_all_alarms_seconds"] <= 0):
            continue
        time.sleep(max(0, runtime["send_all_alarms_seconds"] - 1))
        if (runtime["send_all_alarms_enabled"] and
                runtime["mqtt_connected"] and
                runtime["send_all_alarms_seconds"] > 0):
            send_all_alarms()


def get_all_alarms_timer():
    while True:
        time.sleep(1)
        if (not runtime["get_all_alarms_enabled"] or
                not runtime["mqtt_connected"] or
                runtime["get_all_alarms_seconds"] <= 0):
            continue
        time.sleep(max(0, runtime["get_all_alarms_seconds"] - 1))
        if (runtime["get_all_alarms_enabled"] and
                runtime["mqtt_connected"] and
                runtime["get_all_alarms_seconds"] > 0):
            publish_get_all_alarms()


def mqtt_loop():
    global mqtt_client
    while True:
        try:
            c = mqtt.Client(client_id=f"scada-sim-{CREATOR_ID}")
            c.username_pw_set(MQTT_USER, MQTT_PASS)
            c.on_connect    = on_connect
            c.on_disconnect = on_disconnect
            c.on_message    = on_message
            c.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            mqtt_client = c
            c.loop_forever()
        except Exception as e:
            runtime["mqtt_connected"] = False
            print(f"[scada-api] MQTT loop: {e} — retry 5s", flush=True)
            time.sleep(5)


# ── HTTP API ──────────────────────────────────────────────────────────
@app.route("/")
def index():
    # Disable HTTP caching for the dashboard shell so iterative UI
    # changes during a demo always render on next reload — Flask's
    # default static cache headers are aggressive (12-hour max-age)
    # which masks redeploys until the user opens DevTools.
    resp = send_from_directory("static", "dashboard.html")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/api/status")
def api_status():
    return jsonify({
        "creatorId":  runtime["creator_id"],
        "mqtt": {
            "host":     MQTT_HOST,
            "port":     MQTT_PORT,
            "topicIn":  runtime["topic_in"],
            "topicOut": runtime["topic_out"],
            "connected": runtime["mqtt_connected"],
        },
        "runtime":    runtime,
        "stats":      stats,
        "alarmCount": len(alarm_db),
    })


# ── TMS Publisher (for demo: synthesize TMS-side messages and POST them
# to the bridge's REST publish endpoint, which writes to Artemis) ──────
BRIDGE_URL = os.getenv("BRIDGE_URL",
            "http://pas-scada-bridge.pinkline.svc.cluster.local:8085").rstrip("/")

tms_pub = {
    "enabled":    False,
    "interval_s": 5,
    "topic":      "TMS.PISInfo",
    "sent":       0,
    "errors":     0,
    "last_at":    None,
    "last_error": None,
}
TMS_TOPICS = [
    "TMS.PISInfo",
    "RCS.E2K.TMS.TrafficReportClient",
    "TSInfo",
    "RCS.E2K.TMS.RouteInfo",
]

def _tms_xml(topic: str) -> str:
    """Generate REALISTIC XML matching the actual TMS schema each topic
    uses, so the bridge's XmlToJsonProcessor produces meaningful JSON
    that matches the production wire format.

    Schemas come from the Java POJOs in
    tms/src/main/java/com/pinkline/kafkabridge/model/."""
    now = datetime.now(timezone.utc)
    ts_compact   = now.strftime("%Y%m%dT%H%M%S")             # 20260430T143012
    ts_iso       = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")    # ISO8601
    seconds_today = now.hour * 3600 + now.minute * 60 + now.second
    train_id  = f"T{random.randint(1000, 9999)}"
    tt_guid   = f"{random.randint(100000, 999999)}-{random.randint(1000, 9999)}-{random.randint(100, 999)}"
    trip_no   = random.randint(100, 999)
    line_id   = random.randint(101, 109)
    platform  = random.randint(2200, 2299)
    arrival   = (seconds_today + 60) % 86400
    departure = arrival + 60

    # ── TMS.PISInfo: Passenger Info time table (ATRTimeTableMsg.V3) ──
    if topic == "TMS.PISInfo":
        return (
            f'<ATRTimeTable>'
            f'<dateTime>{ts_compact}</dateTime>'
            f'<Trains>'
            f'<Tg>'
            f'<Id>{trip_no}</Id>'
            f'<TTGUID>{tt_guid}</TTGUID>'
            f'<TripNo>{trip_no}</TripNo>'
            f'<CTD lpid="{line_id}" tn="{trip_no}"/>'
            f'<TmsPTI trguid="{tt_guid}" typeid="1" ttypeid="2"/>'
            f'<Evts F="3" Id="{platform}" As="{arrival}" Ds="{departure}"/>'
            f'<Evts F="3" Id="{platform + 1}" As="{arrival + 120}" Ds="{departure + 120}"/>'
            f'</Tg>'
            f'</Trains>'
            f'</ATRTimeTable>')

    # ── RCS.E2K.TMS.TrafficReportClient: alternates Arrival / Departure ──
    if topic == "RCS.E2K.TMS.TrafficReportClient":
        if random.random() < 0.5:
            return (
                f'<SingleArrival>'
                f'<Arr>'
                f'<train>'
                f'<TTGUID>{tt_guid}</TTGUID>'
                f'<id>{train_id}</id>'
                f'</train>'
                f'<loc><tmsid>{platform}</tmsid></loc>'
                f'<atimes><oTime>{ts_iso}</oTime></atimes>'
                f'</Arr>'
                f'</SingleArrival>')
        return (
            f'<SingleDeparture>'
            f'<Dep>'
            f'<train>'
            f'<TTGUID>{tt_guid}</TTGUID>'
            f'<id>{train_id}</id>'
            f'</train>'
            f'<loc><tmsid>{platform}</tmsid></loc>'
            f'<dtimes><oTime>{ts_iso}</oTime></dtimes>'
            f'</Dep>'
            f'</SingleDeparture>')

    # ── TSInfo: track section / signalling status (no Java POJO yet —
    #          treated as generic XML and converted via Jackson XmlMapper) ──
    if topic == "TSInfo":
        section_id = f"TS_{platform}_{random.randint(1, 9)}"
        state = random.choice(["OCCUPIED", "CLEAR", "FAULT", "RESERVED"])
        return (
            f'<TSInfo>'
            f'<dateTime>{ts_compact}</dateTime>'
            f'<Section id="{section_id}" state="{state}" '
            f'lineId="{line_id}" trainId="{train_id}"/>'
            f'</TSInfo>')

    # ── RCS.E2K.TMS.RouteInfo: route destinations for a train ──
    return (
        f'<routeinfo>'
        f'<TTGUID>{tt_guid}</TTGUID>'
        f'<trainid>{train_id}</trainid>'
        f'<dests>'
        f'<dest tmsid="{platform}"/>'
        f'<dest tmsid="{platform + 1}"/>'
        f'<dest tmsid="{platform + 2}"/>'
        f'</dests>'
        f'</routeinfo>')


def _tms_publish(topic: str, xml: str) -> None:
    url = f"{BRIDGE_URL}/api/tms-publish/{topic}"
    r = requests.post(url, data=xml.encode("utf-8"),
                      headers={"Content-Type": "application/xml"},
                      timeout=5)
    r.raise_for_status()
    tms_pub["sent"] += 1
    tms_pub["last_at"] = now_iso()
    tms_pub["last_error"] = None


def _tms_publish_loop():
    while True:
        if tms_pub["enabled"]:
            try:
                _tms_publish(tms_pub["topic"], _tms_xml(tms_pub["topic"]))
            except Exception as e:
                tms_pub["errors"] += 1
                tms_pub["last_error"] = f"{type(e).__name__}: {e}"
            time.sleep(max(1, int(tms_pub["interval_s"])))
        else:
            time.sleep(0.5)


threading.Thread(target=_tms_publish_loop, daemon=True, name="tms-publisher").start()


@app.route("/api/tms-publish", methods=["POST"])
def api_tms_publish_now():
    """Manual one-shot publish. Body: {"topic": "...", "xml": "..."}.
    Both fields optional — defaults to current toggle topic + synthesized XML."""
    body = request.get_json(silent=True) or {}
    topic = body.get("topic") or tms_pub["topic"]
    xml = body.get("xml") or _tms_xml(topic)
    if topic not in TMS_TOPICS:
        return jsonify({"ok": False, "error": "unknown topic", "allowed": TMS_TOPICS}), 400
    try:
        _tms_publish(topic, xml)
        return jsonify({"ok": True, "topic": topic, "sent": tms_pub["sent"]})
    except Exception as e:
        tms_pub["errors"] += 1
        tms_pub["last_error"] = f"{type(e).__name__}: {e}"
        return jsonify({"ok": False, "error": str(e)}), 502


@app.route("/api/tms-publish/config", methods=["POST"])
def api_tms_publish_config():
    """Update auto-publisher config. Body keys: enabled, interval_s, topic."""
    body = request.get_json(silent=True) or {}
    if "enabled" in body:
        tms_pub["enabled"] = bool(body["enabled"])
    if "interval_s" in body:
        try: tms_pub["interval_s"] = max(1, int(body["interval_s"]))
        except Exception: pass
    if "topic" in body and body["topic"] in TMS_TOPICS:
        tms_pub["topic"] = body["topic"]
    return jsonify(tms_pub)


@app.route("/api/tms-publish/state")
def api_tms_publish_state():
    return jsonify({**tms_pub, "topics": TMS_TOPICS, "bridge_url": BRIDGE_URL})


@app.route("/api/received")
def api_received():
    return jsonify(list(messages_in))


@app.route("/api/sent")
def api_sent():
    return jsonify(list(messages_out))


@app.route("/api/keepalives")
def api_keepalives():
    return jsonify(list(keepalives))


@app.route("/api/alarms")
def api_alarms():
    return jsonify([
        {"Id": k, **v} for k, v in sorted(alarm_db.items())
    ])


@app.route("/api/metrics")
def api_metrics():
    """Live throughput + latency metrics for the flow diagram."""
    last_in  = runtime.get("last_tms_received")
    last_out = runtime.get("last_keepalive_sent")
    silence_in  = (datetime.now(timezone.utc) - datetime.fromisoformat(last_in)).total_seconds() if last_in else None
    silence_out = (datetime.now(timezone.utc) - datetime.fromisoformat(last_out)).total_seconds() if last_out else None
    return jsonify({
        "rate_in_per_sec":   compute_rate(_rate_window_in),
        "rate_out_per_sec":  compute_rate(_rate_window_out),
        "silence_in_sec":    silence_in,
        "silence_out_sec":   silence_out,
        "total_received":    stats["received"],
        "total_sent":        stats["sent_alarm"] + stats["sent_ka"] + stats["sent_all"] + stats["sent_get_all"],
        "mqtt_connected":    runtime["mqtt_connected"],
    })


@app.route("/api/config", methods=["POST"])
def api_config():
    """Every runtime value configurable from dashboard. No restart needed."""
    body = request.get_json(force=True, silent=True) or {}

    # Numeric intervals
    for key in ("keepalive_seconds", "auto_alarm_seconds",
                "send_all_alarms_seconds", "get_all_alarms_seconds"):
        if key in body:
            try:
                runtime[key] = max(0, int(body[key]))
            except Exception:
                pass

    # Booleans
    for key in ("auto_alarm_enabled", "keepalive_paused",
                "send_all_alarms_enabled", "get_all_alarms_enabled"):
        if key in body:
            runtime[key] = bool(body[key])

    # Strings (identity + topics)
    for key in ("creator_id", "topic_out"):
        if key in body and isinstance(body[key], str) and body[key].strip():
            runtime[key] = body[key].strip()

    # topic_in change requires MQTT re-subscribe
    if "topic_in" in body and isinstance(body["topic_in"], str) and body["topic_in"].strip():
        old = runtime["topic_in"]
        new = body["topic_in"].strip()
        if new != old and mqtt_client and runtime["mqtt_connected"]:
            try:
                mqtt_client.unsubscribe(old)
                mqtt_client.subscribe(new)
                print(f"[scada-api] re-subscribed {old} → {new}", flush=True)
            except Exception as e:
                print(f"[scada-api] topic_in swap failed: {e}", flush=True)
        runtime["topic_in"] = new

    # Equipment list
    if "equipment" in body and isinstance(body["equipment"], list):
        runtime["equipment"] = [str(x).strip() for x in body["equipment"] if str(x).strip()]

    broadcast_sse("config", runtime)
    return jsonify(runtime)


@app.route("/api/publish", methods=["POST"])
def api_publish():
    body = request.get_json(force=True, silent=True) or {}
    alarm_id = body.get("id") or random.choice(DEFAULT_EQUIPMENT)
    state    = str(body.get("state", "1"))
    ok = publish_update_alarm(alarm_id, state)
    return jsonify({"ok": ok, "id": alarm_id, "state": state})


@app.route("/api/pause", methods=["POST"])
def api_pause():
    runtime["keepalive_paused"] = True
    broadcast_sse("config", runtime)
    return jsonify(runtime)


@app.route("/api/resume", methods=["POST"])
def api_resume():
    runtime["keepalive_paused"] = False
    broadcast_sse("config", runtime)
    return jsonify(runtime)


@app.route("/api/sendall", methods=["POST"])
def api_sendall():
    ok = send_all_alarms()
    return jsonify({"ok": ok, "alarmCount": len(alarm_db)})


@app.route("/api/getall", methods=["POST"])
def api_getall():
    ok = publish_get_all_alarms()
    return jsonify({"ok": ok})


@app.route("/api/stream")
def api_stream():
    def gen():
        q: Queue = Queue()
        sse_queues.append(q)
        try:
            yield f"data: {json.dumps({'type': 'hello', 'payload': {}, 'ts': now_iso()})}\n\n"
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield f"data: {json.dumps(msg)}\n\n"
                except Empty:
                    yield ": keepalive\n\n"
        finally:
            try:
                sse_queues.remove(q)
            except ValueError:
                pass

    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    threading.Thread(target=mqtt_loop,             daemon=True).start()
    threading.Thread(target=keepalive_timer,       daemon=True).start()
    threading.Thread(target=auto_alarm_timer,      daemon=True).start()
    threading.Thread(target=send_all_alarms_timer, daemon=True).start()
    threading.Thread(target=get_all_alarms_timer,  daemon=True).start()
    app.run(host="0.0.0.0", port=8091, threaded=True)
