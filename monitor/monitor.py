#!/usr/bin/env python3
# PAS-SCADA Health Monitor
# Probes every component in the bridge stack and emails on failure.
# State persisted to disk so restarts don't cause alert spam.
#
# HTTP layer: FastAPI + uvicorn (port 8080)
#   GET /healthz   liveness probe (always 200 once the app is up)
#   GET /readyz    readiness probe
#   GET /state     full probe state as JSON
#   GET /docs      Swagger UI for the above (FastAPI's built-in)

import json
import logging
import os
import smtplib
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import html
import requests
import uvicorn
import yaml
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse, JSONResponse

CONFIG_PATH = os.getenv("MONITOR_CONFIG", "/etc/monitor/config.yaml")
STATE_PATH  = Path(os.getenv("MONITOR_STATE", "/var/state/state.json"))
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
LOG_LEVEL   = os.getenv("LOG_LEVEL", "INFO")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_FROM = os.getenv("SMTP_FROM")
SMTP_TO   = [r.strip() for r in os.getenv("SMTP_TO", "").split(",") if r.strip()]
SMTP_TLS  = os.getenv("SMTP_TLS", "true").lower() == "true"
SMTP_SSL  = os.getenv("SMTP_SSL", "false").lower() == "true"
ALERT_PREFIX = os.getenv("ALERT_PREFIX", "[PAS-SCADA]")

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("monitor")


# ─── Time helpers ─────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def fmt_duration(start_iso: str, end_iso: str) -> str:
    secs = max(0, int((parse_iso(end_iso) - parse_iso(start_iso)).total_seconds()))
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m {secs % 60}s"
    h, rem = divmod(secs, 3600)
    return f"{h}h {rem // 60}m"


# ─── Probes ───────────────────────────────────────────────────────
# Each probe returns (ok: bool, message: str). Message is a short
# diagnostic that ends up in the alert email when it fails.

def _resolve_auth(check: dict):
    if check.get("auth") != "basic":
        return None
    user = os.getenv(check.get("username_env", ""), "")
    pwd  = os.getenv(check.get("password_env", ""), "")
    if not user:
        return None
    return (user, pwd)


def probe_http(check: dict):
    """Generic HTTP probe. Optional basic auth, status check, body match."""
    url     = check["url"]
    timeout = check.get("timeout_s", 5)
    expect  = check.get("expect_status", 200)
    contains = check.get("expect_body_contains")
    try:
        r = requests.get(url, timeout=timeout, auth=_resolve_auth(check), verify=False)
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
    if r.status_code != expect:
        return False, f"HTTP {r.status_code} (expected {expect})"
    if contains and contains not in r.text:
        return False, f"body missing '{contains}'"
    return True, f"HTTP {r.status_code}"


def probe_actuator(check: dict):
    """Spring Boot Actuator /actuator/health — reads JSON, checks status==UP.

    Reports which subcomponents are DOWN when the overall state is bad,
    so the alert email tells you *why* (e.g. kafka DOWN, rabbit UP).
    """
    url     = check["url"]
    timeout = check.get("timeout_s", 5)
    try:
        r = requests.get(url, timeout=timeout, auth=_resolve_auth(check))
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
    # 503 still has a valid body (Actuator returns DOWN with detail)
    if r.status_code not in (200, 503):
        return False, f"HTTP {r.status_code}"
    try:
        body = r.json()
    except Exception:
        return False, f"invalid JSON (HTTP {r.status_code})"
    status = body.get("status", "UNKNOWN")
    if status == "UP":
        return True, "UP"
    down = []
    for k, v in (body.get("components") or {}).items():
        sub = v.get("status") if isinstance(v, dict) else None
        if sub and sub != "UP":
            down.append(f"{k}={sub}")
    detail = ", ".join(down) if down else "no detail"
    return False, f"status={status} ({detail})"


def probe_tcp(check: dict):
    """TCP connect probe — for Kafka/Zookeeper/MQTT-only/AMQP-only."""
    host    = check["host"]
    port    = int(check["port"])
    timeout = check.get("timeout_s", 5)
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"connected {host}:{port}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def probe_json_field(check: dict):
    """GET URL, parse JSON, assert a dotted path equals expected.

    Used for SCADA API: /api/status returns {"mqtt":{"connected":true}}.
    Set field=mqtt.connected, equals=true to alert when MQTT detaches.
    """
    url     = check["url"]
    timeout = check.get("timeout_s", 5)
    field   = check["field"]
    expect  = check["equals"]
    try:
        r = requests.get(url, timeout=timeout, auth=_resolve_auth(check))
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
    if r.status_code != 200:
        return False, f"HTTP {r.status_code}"
    try:
        body = r.json()
    except Exception:
        return False, "invalid JSON"
    cursor = body
    for part in field.split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            return False, f"field '{field}' missing"
        cursor = cursor[part]
    if cursor != expect:
        return False, f"{field}={cursor!r} (expected {expect!r})"
    return True, f"{field}={cursor!r}"


PROBES = {
    "http":       probe_http,
    "actuator":   probe_actuator,
    "tcp":        probe_tcp,
    "json_field": probe_json_field,
}


# ─── State (consecutive fail/pass counters, persisted) ────────────

class State:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.data = self._load()

    def _load(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except Exception as e:
            log.warning("state load failed (%s) — starting fresh", e)
            return {}

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True))
        tmp.replace(self.path)

    def get(self, name: str) -> dict:
        with self.lock:
            return dict(self.data.get(name, {
                "consecutive_fails":  0,
                "consecutive_passes": 0,
                "current_state":      "UP",
                "last_state_change":  now_iso(),
                "last_ok_at":         None,
                "last_fail_at":       None,
                "last_error":         None,
            }))

    def update(self, name: str, fields: dict):
        with self.lock:
            current = self.data.setdefault(name, {})
            current.update(fields)
            self._save()

    def snapshot(self) -> dict:
        with self.lock:
            return json.loads(json.dumps(self.data))


# ─── Alerter (SMTP) ───────────────────────────────────────────────

class Alerter:
    def __init__(self):
        self.host = SMTP_HOST
        self.port = SMTP_PORT
        self.user = SMTP_USER
        self.password = SMTP_PASS
        self.sender = SMTP_FROM
        self.recipients = SMTP_TO
        self.tls = SMTP_TLS
        self.ssl = SMTP_SSL

    def configured(self) -> bool:
        return bool(self.host and self.recipients and self.sender)

    def send(self, subject: str, html: str, text: str):
        if not self.configured():
            log.warning("SMTP not configured — would send: %s", subject)
            return False
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg.attach(MIMEText(text, "plain", "utf-8"))
        msg.attach(MIMEText(html, "html", "utf-8"))
        try:
            if self.ssl:
                smtp = smtplib.SMTP_SSL(self.host, self.port, timeout=15)
            else:
                smtp = smtplib.SMTP(self.host, self.port, timeout=15)
            with smtp:
                smtp.ehlo()
                if self.tls and not self.ssl:
                    smtp.starttls()
                    smtp.ehlo()
                if self.user:
                    smtp.login(self.user, self.password)
                smtp.send_message(msg)
            log.info("EMAIL sent: %s", subject)
            return True
        except Exception as e:
            log.error("EMAIL send failed (%s): %s", type(e).__name__, e)
            return False


# ─── Email templates ──────────────────────────────────────────────

DOWN_HTML = """<!doctype html>
<html><body style="font-family:-apple-system,'Segoe UI',Roboto,sans-serif;max-width:640px;margin:0;padding:20px;background:#f7f7f7;color:#222;">
  <div style="background:#fff;border-left:5px solid #c0392b;padding:20px;border-radius:4px;">
    <h2 style="margin:0 0 12px 0;color:#c0392b;">Component DOWN: {name}</h2>
    <table style="width:100%;border-collapse:collapse;font-size:14px;">
      <tr><td style="padding:6px 0;color:#777;width:160px;">Type</td><td>{check_type}</td></tr>
      <tr><td style="padding:6px 0;color:#777;">Endpoint</td><td><code>{endpoint}</code></td></tr>
      <tr><td style="padding:6px 0;color:#777;">First failed at</td><td>{first_fail}</td></tr>
      <tr><td style="padding:6px 0;color:#777;">Consecutive fails</td><td>{fails}</td></tr>
      <tr><td style="padding:6px 0;color:#777;">Last error</td><td><code style="color:#c0392b;">{error}</code></td></tr>
    </table>
    <h3 style="margin:20px 0 6px 0;font-size:14px;color:#555;">Other components currently DOWN</h3>
    <ul style="margin:0;padding-left:20px;font-size:13px;">{others_down}</ul>
  </div>
  <p style="color:#999;font-size:11px;margin-top:16px;">
    Sent by <b>pas-scada-monitor</b> at {now}. No further alerts for this component until it recovers.
  </p>
</body></html>
"""

DOWN_TEXT = """Component DOWN: {name}

Type:              {check_type}
Endpoint:          {endpoint}
First failed at:   {first_fail}
Consecutive fails: {fails}
Last error:        {error}

Other components currently DOWN: {others_down_text}

Sent by pas-scada-monitor at {now}.
No further alerts for this component until it recovers.
"""

RECOVERED_HTML = """<!doctype html>
<html><body style="font-family:-apple-system,'Segoe UI',Roboto,sans-serif;max-width:640px;margin:0;padding:20px;background:#f7f7f7;color:#222;">
  <div style="background:#fff;border-left:5px solid #27ae60;padding:20px;border-radius:4px;">
    <h2 style="margin:0 0 12px 0;color:#27ae60;">Component RECOVERED: {name}</h2>
    <table style="width:100%;border-collapse:collapse;font-size:14px;">
      <tr><td style="padding:6px 0;color:#777;width:160px;">Endpoint</td><td><code>{endpoint}</code></td></tr>
      <tr><td style="padding:6px 0;color:#777;">Down since</td><td>{down_since}</td></tr>
      <tr><td style="padding:6px 0;color:#777;">Recovered at</td><td>{recovered_at}</td></tr>
      <tr><td style="padding:6px 0;color:#777;">Total downtime</td><td><b>{duration}</b></td></tr>
    </table>
  </div>
  <p style="color:#999;font-size:11px;margin-top:16px;">Sent by <b>pas-scada-monitor</b>.</p>
</body></html>
"""

RECOVERED_TEXT = """Component RECOVERED: {name}

Endpoint:        {endpoint}
Down since:      {down_since}
Recovered at:    {recovered_at}
Total downtime:  {duration}
"""


# ─── Engine ───────────────────────────────────────────────────────

def endpoint_label(check: dict) -> str:
    if "url" in check:
        return check["url"]
    if "host" in check and "port" in check:
        return f"{check['host']}:{check['port']}"
    return check.get("name", "?")


def others_down(state: State, exclude: str) -> list:
    snap = state.snapshot()
    return [name for name, s in snap.items()
            if name != exclude and s.get("current_state") == "DOWN"]


def send_down_alert(alerter: Alerter, check: dict, s: dict, state: State):
    others = others_down(state, check["name"])
    others_html = "<li>None</li>" if not others else "".join(
        f"<li><b>{n}</b></li>" for n in others)
    others_text = "none" if not others else ", ".join(others)
    ctx = {
        "name":         check["name"],
        "check_type":   check.get("type", "http"),
        "endpoint":     endpoint_label(check),
        "first_fail":   s.get("first_fail_at") or s.get("last_fail_at") or now_iso(),
        "fails":        s.get("consecutive_fails", 0),
        "error":        s.get("last_error", "") or "",
        "others_down":  others_html,
        "others_down_text": others_text,
        "now":          now_iso(),
    }
    subject = f"{ALERT_PREFIX} DOWN: {check['name']}"
    alerter.send(subject, DOWN_HTML.format(**ctx), DOWN_TEXT.format(**ctx))


def send_recovery_alert(alerter: Alerter, check: dict, s: dict):
    down_since = s.get("last_state_change", now_iso())
    recovered  = now_iso()
    duration   = fmt_duration(down_since, recovered)
    ctx = {
        "name":         check["name"],
        "endpoint":     endpoint_label(check),
        "down_since":   down_since,
        "recovered_at": recovered,
        "duration":     duration,
    }
    subject = f"{ALERT_PREFIX} RECOVERED: {check['name']} (down {duration})"
    alerter.send(subject, RECOVERED_HTML.format(**ctx), RECOVERED_TEXT.format(**ctx))


def evaluate(check: dict, ok: bool, msg: str, state: State, alerter: Alerter):
    name = check["name"]
    fail_th    = check.get("fail_threshold", 3)
    recover_th = check.get("recover_threshold", 1)

    s = state.get(name)
    new_state_change = None

    if ok:
        s["consecutive_fails"] = 0
        s["consecutive_passes"] = s.get("consecutive_passes", 0) + 1
        s["last_ok_at"] = now_iso()
        s["last_error"] = None
        s["first_fail_at"] = None
        if s["current_state"] == "DOWN" and s["consecutive_passes"] >= recover_th:
            send_recovery_alert(alerter, check, s)
            s["current_state"] = "UP"
            new_state_change = now_iso()
    else:
        # Capture *first* fail timestamp for the alert email — last_fail_at
        # updates every probe, but first_fail_at sticks until recovery.
        if s.get("consecutive_fails", 0) == 0:
            s["first_fail_at"] = now_iso()
        s["consecutive_passes"] = 0
        s["consecutive_fails"] = s.get("consecutive_fails", 0) + 1
        s["last_fail_at"] = now_iso()
        s["last_error"] = msg
        if s["current_state"] == "UP" and s["consecutive_fails"] >= fail_th:
            send_down_alert(alerter, check, s, state)
            s["current_state"] = "DOWN"
            new_state_change = now_iso()

    if new_state_change:
        s["last_state_change"] = new_state_change

    state.update(name, s)


def run_check(check: dict):
    probe_type = check.get("type", "http")
    fn = PROBES.get(probe_type)
    if fn is None:
        return False, f"unknown probe type: {probe_type}"
    try:
        return fn(check)
    except Exception as e:
        return False, f"probe crashed: {type(e).__name__}: {e}"


def probe_loop(state: State, alerter: Alerter, checks: list, interval: int,
               workers: int, stop: threading.Event):
    """Background thread: runs all probes every interval seconds until stop is set."""
    while not stop.is_set():
        cycle_start = time.time()
        log.debug("cycle start")
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="probe") as pool:
            futures = {pool.submit(run_check, c): c for c in checks}
            for fut in as_completed(futures):
                check = futures[fut]
                try:
                    ok, msg = fut.result()
                except Exception as e:
                    ok, msg = False, f"check crashed: {e}"
                level = logging.INFO if ok else logging.WARNING
                log.log(level, "[%s] %s — %s", "OK" if ok else "FAIL", check["name"], msg)
                evaluate(check, ok, msg, state, alerter)

        elapsed = time.time() - cycle_start
        sleep_for = max(1.0, interval - elapsed)
        # Sleep responsively so shutdown is fast.
        stop.wait(sleep_for)


# ─── FastAPI app ──────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: load config, start probe loop in a background thread.
    Shutdown: signal the thread to stop."""
    cfg_path = Path(CONFIG_PATH)
    if not cfg_path.exists():
        log.error("config not found at %s", CONFIG_PATH)
        sys.exit(1)
    cfg = yaml.safe_load(cfg_path.read_text()) or {}
    interval = int(cfg.get("interval_s", 30))
    workers  = int(cfg.get("workers", 8))
    checks   = cfg.get("checks", [])
    if not checks:
        log.error("no checks configured")
        sys.exit(1)
    log.info("loaded %d checks, interval=%ds, workers=%d", len(checks), interval, workers)

    state = State(STATE_PATH)
    alerter = Alerter()
    if not alerter.configured():
        log.warning("SMTP not configured — alerts will be logged only "
                    "(set SMTP_HOST/SMTP_FROM/SMTP_TO env vars)")

    app.state.shared_state = state
    app.state.alerter = alerter
    app.state.stop_event = threading.Event()

    t = threading.Thread(
        target=probe_loop,
        args=(state, alerter, checks, interval, workers, app.state.stop_event),
        daemon=True,
        name="probe-loop",
    )
    t.start()
    log.info("probe loop started — every %ds", interval)

    yield

    log.info("shutdown — stopping probe loop")
    app.state.stop_event.set()


app = FastAPI(
    title="PAS-SCADA Health Monitor",
    description="Health probes for the PAS-SCADA pipeline. Emails on threshold cross.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/healthz", response_class=Response, summary="Liveness probe (k8s)")
def healthz():
    return Response("OK\n", media_type="text/plain")


@app.get("/readyz", response_class=Response, summary="Readiness probe (k8s)")
def readyz():
    return Response("OK\n", media_type="text/plain")


@app.get("/state", summary="Full probe state — current_state per check")
def state_endpoint():
    """Returns one entry per check with last_ok_at, last_fail_at, consecutive
    counters, current state UP|DOWN, etc. Useful for ad-hoc dashboards."""
    return JSONResponse(app.state.shared_state.snapshot())


# ─── Visual dashboard ─────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>PAS-SCADA Status</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg: #ffffff;
      --bg-soft: #fafafa;
      --border: #e5e5e5;
      --border-strong: #d4d4d4;
      --text: #171717;
      --text-mute: #525252;
      --text-faint: #a3a3a3;
      --up: #15803d;
      --up-bg: #f0fdf4;
      --down: #b91c1c;
      --down-bg: #fef2f2;
      --accent: #1f1f1f;
    }

    @media (prefers-color-scheme: dark) {
      :root {
        --bg: #0b0b0b;
        --bg-soft: #131313;
        --border: #1f1f1f;
        --border-strong: #2a2a2a;
        --text: #ededed;
        --text-mute: #a3a3a3;
        --text-faint: #525252;
        --up: #4ade80;
        --up-bg: rgba(74, 222, 128, 0.06);
        --down: #f87171;
        --down-bg: rgba(248, 113, 113, 0.08);
        --accent: #ededed;
      }
    }

    body {
      font-family: 'IBM Plex Sans', system-ui, -apple-system, sans-serif;
      background: var(--bg);
      color: var(--text);
      font-size: 14px;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }

    main {
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 24px 64px;
    }

    /* ── Header ── */
    header {
      display: flex;
      align-items: center;
      gap: 16px;
      flex-wrap: wrap;
      padding-bottom: 24px;
      border-bottom: 1px solid var(--border);
    }
    .title {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .title-dot {
      width: 10px; height: 10px;
      border-radius: 50%;
      background: var(--up);
      flex-shrink: 0;
    }
    header.alert .title-dot { background: var(--down); }
    h1 {
      font-size: 17px;
      font-weight: 600;
      letter-spacing: -0.01em;
    }
    .header-meta {
      margin-left: auto;
      display: flex;
      align-items: center;
      gap: 24px;
      font-size: 13px;
      color: var(--text-mute);
    }
    .header-meta .clock {
      font-family: 'IBM Plex Mono', ui-monospace, monospace;
      font-variant-numeric: tabular-nums;
    }
    .alarm-btn {
      background: var(--bg);
      border: 1px solid var(--border-strong);
      color: var(--text);
      padding: 7px 14px;
      font-family: inherit;
      font-size: 13px;
      font-weight: 500;
      cursor: pointer;
      border-radius: 6px;
      transition: all 0.15s ease;
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .alarm-btn:hover { border-color: var(--text-mute); }
    .alarm-btn .led {
      width: 7px; height: 7px;
      border-radius: 50%;
      background: var(--text-faint);
    }
    .alarm-btn.armed { border-color: var(--up); color: var(--up); }
    .alarm-btn.armed .led {
      background: var(--up);
      box-shadow: 0 0 0 2px var(--up-bg);
    }

    /* ── Status banner ── */
    .status-row {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 14px 0;
      margin-bottom: 8px;
      font-size: 14px;
    }
    .status-row .label { color: var(--text-mute); }
    .status-row .value { color: var(--text); font-weight: 500; }
    .status-row.alert .value { color: var(--down); }

    /* ── Summary stats ── */
    .stats {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 0;
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
      margin-bottom: 32px;
      background: var(--bg-soft);
    }
    .stat {
      padding: 16px 20px;
      border-right: 1px solid var(--border);
    }
    .stat:last-child { border-right: none; }
    .stat-label {
      font-size: 11px;
      font-weight: 500;
      color: var(--text-mute);
      letter-spacing: 0.04em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }
    .stat-value {
      font-family: 'IBM Plex Mono', ui-monospace, monospace;
      font-size: 24px;
      font-weight: 500;
      letter-spacing: -0.02em;
      color: var(--text);
      font-variant-numeric: tabular-nums;
    }
    .stat-value.up { color: var(--up); }
    .stat-value.down { color: var(--down); }

    @media (max-width: 760px) {
      .stats { grid-template-columns: repeat(2, 1fr); }
      .stat:nth-child(2) { border-right: none; }
      .stat:nth-child(1), .stat:nth-child(2) { border-bottom: 1px solid var(--border); }
    }

    /* ── Sections ── */
    .section { margin-bottom: 32px; }
    .section-head {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      margin-bottom: 12px;
      padding-bottom: 8px;
      border-bottom: 1px solid var(--border);
    }
    .section-head h2 {
      font-size: 13px;
      font-weight: 600;
      color: var(--text);
      letter-spacing: -0.005em;
    }
    .section-head .summary {
      font-size: 12px;
      color: var(--text-mute);
      font-family: 'IBM Plex Mono', ui-monospace, monospace;
      font-variant-numeric: tabular-nums;
    }
    .section-head .summary.alert { color: var(--down); }

    /* ── Probe rows ── */
    .probes {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 1px;
      background: var(--border);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
    }
    .probe {
      background: var(--bg);
      padding: 12px 14px;
      transition: background 0.15s ease;
      position: relative;
    }
    .probe:hover { background: var(--bg-soft); }
    .probe[data-state="DOWN"] { background: var(--down-bg); }

    .probe[data-just-failed] { animation: flash-red 0.5s ease-out 4; }
    .probe[data-just-recovered] { animation: flash-green 1s ease-out 1; }
    @keyframes flash-red { 0%,100% { background: var(--down-bg); } 50% { background: rgba(248, 113, 113, 0.25); } }
    @keyframes flash-green { 0% { background: rgba(74, 222, 128, 0.18); } 100% { background: var(--bg); } }

    .probe-head {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 6px;
    }
    .probe-dot {
      width: 8px; height: 8px;
      border-radius: 50%;
      background: var(--up);
      flex-shrink: 0;
    }
    .probe[data-state="DOWN"] .probe-dot {
      background: var(--down);
    }
    .probe-name {
      font-size: 13px;
      font-weight: 500;
      flex: 1;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .probe-state {
      font-size: 11px;
      font-weight: 600;
      color: var(--up);
      letter-spacing: 0.02em;
    }
    .probe[data-state="DOWN"] .probe-state { color: var(--down); }

    .probe-meta {
      font-family: 'IBM Plex Mono', ui-monospace, monospace;
      font-size: 11px;
      color: var(--text-mute);
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      font-variant-numeric: tabular-nums;
    }
    .probe-meta .field { display: inline-flex; gap: 4px; }
    .probe-meta .field-label { color: var(--text-faint); }

    .probe-err {
      margin-top: 6px;
      font-family: 'IBM Plex Mono', ui-monospace, monospace;
      font-size: 11px;
      color: var(--down);
      word-break: break-word;
      padding: 6px 8px;
      background: var(--down-bg);
      border-radius: 4px;
    }

    /* ── Loading ── */
    .loading {
      grid-column: 1 / -1;
      padding: 60px 0;
      text-align: center;
      font-size: 13px;
      color: var(--text-mute);
    }

    /* ── Footer ── */
    footer {
      margin-top: 48px;
      padding-top: 16px;
      border-top: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 12px;
      color: var(--text-faint);
    }
    footer a {
      color: var(--text-mute);
      text-decoration: none;
      margin-left: 16px;
    }
    footer a:hover { color: var(--text); text-decoration: underline; }

    @media (max-width: 600px) {
      header { flex-direction: column; align-items: flex-start; }
      .header-meta { margin-left: 0; flex-wrap: wrap; gap: 12px; }
    }
  </style>
</head>
<body>
  <main>
    <header id="header">
      <div class="title">
        <span class="title-dot"></span>
        <h1>PAS-SCADA Status</h1>
      </div>
      <div class="header-meta">
        <span class="clock" id="clock">--:--:--</span>
        <button class="alarm-btn disarmed" id="alarm-toggle">
          <span class="led"></span>
          <span id="alarm-label">Sound off</span>
        </button>
      </div>
    </header>

    <div class="status-row" id="status-row">
      <span class="label">Status:</span>
      <span class="value" id="status-msg">Initialising…</span>
      <span class="label" id="ticker" style="margin-left:auto;font-size:12px;">— sync pending</span>
    </div>

    <div class="stats">
      <div class="stat">
        <div class="stat-label">Up</div>
        <div class="stat-value up" id="stat-up">—</div>
      </div>
      <div class="stat">
        <div class="stat-label">Down</div>
        <div class="stat-value down" id="stat-down">—</div>
      </div>
      <div class="stat">
        <div class="stat-label">Total components</div>
        <div class="stat-value" id="stat-total">—</div>
      </div>
      <div class="stat">
        <div class="stat-label">Probe interval</div>
        <div class="stat-value" id="stat-interval">15s</div>
      </div>
    </div>

    <div id="grid">
      <div class="loading">Loading probe state…</div>
    </div>

    <footer>
      <span>Refreshes every 5 seconds · alerts after 3 consecutive failures</span>
      <div>
        <a href="/state">JSON</a>
        <a href="/docs">API</a>
        <a href="/healthz">healthz</a>
      </div>
    </footer>
  </main>

  <script>
    const SECTIONS = [
      { id: 'pipeline',   title: 'TMS Pipeline',     match: n => /^(artemis-|kafka$|zookeeper$)/.test(n) },
      { id: 'bridge',     title: 'Bridge & Workers', match: n => /^(pas-scada-(bridge|demo|monitor)|kafdrop|kafka-connect)$/.test(n) },
      { id: 'connectors', title: 'Kafka Connect',    match: n => /^connector-/.test(n) },
      { id: 'scada',      title: 'SCADA',            match: n => /^(rabbitmq-|scada-api)/.test(n) },
    ];

    /* ── Audio (synthesized via Web Audio API) ── */
    let audioCtx = null, alarmInterval = null;
    let alarmArmed = localStorage.getItem('pas-alarm-armed') === '1';
    const ensureAudio = () => audioCtx ||= new (window.AudioContext || window.webkitAudioContext)();
    function beep(freq, duration, when = 0) {
      const ctx = ensureAudio();
      const osc = ctx.createOscillator(), gain = ctx.createGain();
      osc.type = 'sine'; osc.frequency.value = freq;
      const t0 = ctx.currentTime + when;
      gain.gain.setValueAtTime(0, t0);
      gain.gain.linearRampToValueAtTime(0.22, t0 + 0.02);
      gain.gain.linearRampToValueAtTime(0, t0 + duration);
      osc.connect(gain).connect(ctx.destination);
      osc.start(t0); osc.stop(t0 + duration);
    }
    const playAlarm = () => {
      if (!alarmArmed) return;
      beep(880, 0.18, 0); beep(740, 0.18, 0.22); beep(600, 0.28, 0.44);
    };
    const startAlarm = () => { stopAlarm(); playAlarm(); alarmInterval = setInterval(playAlarm, 4000); };
    const stopAlarm  = () => { if (alarmInterval) { clearInterval(alarmInterval); alarmInterval = null; } };

    const toggleBtn = document.getElementById('alarm-toggle');
    const toggleLabel = document.getElementById('alarm-label');
    function renderToggle() {
      if (alarmArmed) {
        toggleBtn.classList.add('armed'); toggleBtn.classList.remove('disarmed');
        toggleLabel.textContent = 'Sound on';
      } else {
        toggleBtn.classList.remove('armed'); toggleBtn.classList.add('disarmed');
        toggleLabel.textContent = 'Sound off';
      }
    }
    toggleBtn.addEventListener('click', () => {
      alarmArmed = !alarmArmed;
      localStorage.setItem('pas-alarm-armed', alarmArmed ? '1' : '0');
      renderToggle();
      if (alarmArmed) { ensureAudio(); beep(660, 0.12, 0); }
      else stopAlarm();
    });
    renderToggle();

    /* ── Clock ── */
    const clockEl = document.getElementById('clock');
    setInterval(() => clockEl.textContent = new Date().toTimeString().slice(0, 8), 1000);
    clockEl.textContent = new Date().toTimeString().slice(0, 8);

    /* ── Render ── */
    const grid = document.getElementById('grid');
    const header = document.getElementById('header');
    const statusRow = document.getElementById('status-row');
    const statusMsg = document.getElementById('status-msg');
    const ticker = document.getElementById('ticker');
    const statUp = document.getElementById('stat-up');
    const statDown = document.getElementById('stat-down');
    const statTotal = document.getElementById('stat-total');
    let lastStates = {}, firstRender = true, lastSync = Date.now();

    const escapeHtml = s => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const fmtTime = v => {
      if (!v) return '—';
      const m = String(v).match(/T(\d{2}:\d{2}:\d{2})/);
      return m ? m[1] : String(v).slice(0, 8);
    };

    function categorize(state) {
      const buckets = SECTIONS.map(s => ({ ...s, items: [] }));
      const orphans = [];
      Object.keys(state).sort().forEach(name => {
        const item = { name, ...state[name] };
        const target = buckets.find(b => b.match(name));
        if (target) target.items.push(item); else orphans.push(item);
      });
      if (orphans.length) buckets.push({ id: 'other', title: 'Other', items: orphans });
      return buckets;
    }

    function renderProbe(c, justFailed, justRecovered) {
      const errBlock = c.current_state === 'DOWN' && c.last_error
        ? `<div class="probe-err">${escapeHtml(c.last_error)}</div>` : '';
      return `
        <div class="probe" data-state="${c.current_state}" ${justFailed ? 'data-just-failed' : ''} ${justRecovered ? 'data-just-recovered' : ''}>
          <div class="probe-head">
            <span class="probe-dot"></span>
            <span class="probe-name">${escapeHtml(c.name)}</span>
            <span class="probe-state">${c.current_state}</span>
          </div>
          <div class="probe-meta">
            <span class="field"><span class="field-label">last ok</span>${fmtTime(c.last_ok_at)}</span>
            <span class="field"><span class="field-label">since</span>${fmtTime(c.last_state_change)}</span>
            <span class="field"><span class="field-label">streak</span>${c.consecutive_passes || 0}</span>
          </div>
          ${errBlock}
        </div>`;
    }

    function render(state) {
      const buckets = categorize(state);
      let html = '';
      for (const b of buckets) {
        if (!b.items.length) continue;
        const downs = b.items.filter(i => i.current_state === 'DOWN').length;
        const ups = b.items.length - downs;
        const summary = downs > 0
          ? `<span class="summary alert">${ups} up · ${downs} down</span>`
          : `<span class="summary">${ups} up</span>`;
        const probes = b.items.map(item => {
          const prev = lastStates[item.name];
          const justFailed   = !firstRender && prev && prev !== 'DOWN' && item.current_state === 'DOWN';
          const justRecovered = !firstRender && prev && prev !== 'UP'   && item.current_state === 'UP';
          return renderProbe(item, justFailed, justRecovered);
        }).join('');
        html += `
          <section class="section">
            <div class="section-head">
              <h2>${b.title}</h2>
              ${summary}
            </div>
            <div class="probes">${probes}</div>
          </section>`;
      }
      grid.innerHTML = html;
      firstRender = false;
    }

    function updateStats(state) {
      const items = Object.values(state);
      const total = items.length;
      const downs = items.filter(i => i.current_state === 'DOWN');
      const ups = total - downs.length;
      statUp.textContent = ups;
      statDown.textContent = downs.length;
      statTotal.textContent = total;

      if (downs.length === 0) {
        header.classList.remove('alert');
        statusRow.classList.remove('alert');
        statusMsg.textContent = 'All systems operational';
        document.title = 'PAS-SCADA · all up';
      } else {
        header.classList.add('alert');
        statusRow.classList.add('alert');
        statusMsg.textContent = downs.length === 1
          ? `1 system down — ${downs[0].name}`
          : `${downs.length} systems down — ${downs.map(d => d.name).join(', ')}`;
        document.title = `(${downs.length} DOWN) PAS-SCADA`;
      }
    }

    function detectAndAlert(state) {
      let newDownAppeared = false;
      const downSet = new Set();
      for (const name of Object.keys(state)) {
        const cur = state[name].current_state;
        const prev = lastStates[name];
        if (prev && prev !== 'DOWN' && cur === 'DOWN') newDownAppeared = true;
        if (cur === 'DOWN') downSet.add(name);
        lastStates[name] = cur;
      }
      if (downSet.size === 0) stopAlarm();
      else if (newDownAppeared) startAlarm();
    }

    async function poll() {
      try {
        const r = await fetch('/state', { cache: 'no-store' });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const state = await r.json();
        detectAndAlert(state);
        render(state);
        updateStats(state);
        lastSync = Date.now();
        ticker.textContent = '— synced just now';
      } catch (e) {
        ticker.textContent = '— sync failed: ' + e.message;
      }
    }
    setInterval(() => {
      const s = Math.floor((Date.now() - lastSync) / 1000);
      if (s >= 2) ticker.textContent = `— synced ${s}s ago`;
    }, 1000);
    poll();
    setInterval(poll, 5000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, summary="Visual dashboard")
def dashboard():
    """Status-page style live dashboard. JS polls /state every 5s,
    categorizes probes into 4 sections, plays an audible alarm on
    state-change UP→DOWN once the user clicks the Sound on button."""
    return HTMLResponse(DASHBOARD_HTML)


# ─── Entrypoint ───────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("PAS-SCADA Monitor starting (FastAPI on :%d)", HEALTH_PORT)
    try:
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=HEALTH_PORT,
            log_level=LOG_LEVEL.lower(),
            access_log=False,  # suppress noise — every k8s probe is a hit
        )
    except KeyboardInterrupt:
        log.info("shutdown requested")
        sys.exit(0)
