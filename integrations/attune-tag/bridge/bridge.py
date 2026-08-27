#!/usr/bin/env python3
"""
Attune Call Bridge — visitor-level call attribution we own end to end.

The problem: attributing a phone call to a household needs the *web visitor's*
IP, and the client's phone system may not expose one. Asking the vendor did not
work.

The insight: we do not need them to. Our own tag runs in the visitor's browser,
so we can see the visitor ourselves. This service leases a phone number to each
visitor, records the IP it saw them from, and later matches an inbound call back
to that lease. The phone system only has to tell us **which number was dialled
and when** — which every phone system on earth can do.

    browser ──POST /v1/session──> bridge        (bridge sees the visitor's IP,
       │                             │           leases a number, returns it)
       │  <──── number to display ───┘
       │
    visitor dials that number
       │
    phone system ──POST /v1/call──> bridge      (dialled number + time + call id)
                                      │
                                      └──> S2S conversion, carrying the IP
                                           we captured ourselves

Why this does not contradict "never proxy the pixel". The *pixel beacon* must
leave the visitor's browser, because the tracker infers the household IP from
the request's source address. The *S2S conversion endpoint* is a different
mechanism: it takes `ip` as an explicit parameter and is designed to be called
server-to-server. One infers the IP; the other is told it. Passing the correct
visitor IP from here is its intended use.

Deliberately stdlib-only so it runs anywhere with no install step. Traffic for a
single local advertiser is tiny; put it behind a real reverse proxy for TLS and
set ATTUNE_TRUSTED_PROXY_HOPS accordingly.
"""

import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --------------------------------------------------------------------------- config

def _env(name, default=None):
    return os.environ.get(name, default)


CONFIG = {
    # Numbers leased to visitors. All should forward to the client's main line.
    "pool": [n.strip() for n in (_env("ATTUNE_POOL", "") or "").split(",") if n.strip()],
    # Shown when the pool is exhausted. Unattributed, but the page still works.
    "fallback": _env("ATTUNE_FALLBACK_NUMBER", ""),
    "pixel_id": _env("ATTUNE_PIXEL_ID", ""),
    "s2s_url": _env("ATTUNE_S2S_URL", "https://t.vibe.co/s2s-conversion/events"),
    "lease_ttl": int(_env("ATTUNE_LEASE_TTL", "1800")),        # 30 min on-page hold
    "match_grace": int(_env("ATTUNE_MATCH_GRACE", "3600")),    # call may come later
    "db": _env("ATTUNE_DB", "bridge.db"),
    # Number of trusted proxies between the internet and this process.
    "proxy_hops": int(_env("ATTUNE_TRUSTED_PROXY_HOPS", "0")),
    # When true, log the conversion instead of sending it.
    "dry_run": (_env("ATTUNE_DRY_RUN", "0") == "1"),
    "origins": [o.strip() for o in (_env("ATTUNE_ALLOWED_ORIGINS", "*") or "").split(",") if o.strip()],
}

E164 = re.compile(r"^\+?[1-9]\d{7,14}$")


def digits(n):
    """Compare numbers by digits so +1-405-555-0101 and 4055550101 match."""
    d = re.sub(r"\D", "", n or "")
    return d[1:] if len(d) == 11 and d.startswith("1") else d


# --------------------------------------------------------------------------- storage

class Store:
    def __init__(self, path):
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS leases (
                id TEXT PRIMARY KEY,
                number TEXT NOT NULL,
                number_digits TEXT NOT NULL,
                ip TEXT NOT NULL,
                session TEXT,
                leased_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )""")
        self._db.execute("CREATE INDEX IF NOT EXISTS ix_lease_lookup ON leases(number_digits, leased_at)")
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS conversions (
                call_id TEXT PRIMARY KEY,
                lease_id TEXT,
                ip TEXT,
                sent_at INTEGER,
                ok INTEGER
            )""")
        self._db.commit()

    def lease(self, pool, ip, session, ttl):
        """Lease the least-recently-used free number. None if all are held."""
        now = int(time.time())
        with self._lock:
            held = {
                r[0] for r in self._db.execute(
                    "SELECT number_digits FROM leases WHERE expires_at > ?", (now,))
            }
            free = [n for n in pool if digits(n) not in held]
            if not free:
                return None
            # Least recently used, so a recycled number has maximum cool-down.
            def last_used(n):
                row = self._db.execute(
                    "SELECT MAX(leased_at) FROM leases WHERE number_digits = ?",
                    (digits(n),)).fetchone()
                return row[0] or 0
            number = min(free, key=last_used)
            lease_id = uuid.uuid4().hex
            self._db.execute(
                "INSERT INTO leases VALUES (?,?,?,?,?,?,?)",
                (lease_id, number, digits(number), ip, session, now, now + ttl))
            self._db.commit()
            return {"lease_id": lease_id, "number": number, "ip": ip,
                    "leased_at": now, "expires_at": now + ttl}

    def find_lease(self, number, call_ts, grace):
        """Most recent lease of `number` that plausibly produced a call at call_ts."""
        with self._lock:
            row = self._db.execute(
                """SELECT id, number, ip, leased_at, expires_at FROM leases
                   WHERE number_digits = ? AND leased_at <= ? AND ? <= expires_at + ?
                   ORDER BY leased_at DESC LIMIT 1""",
                (digits(number), call_ts, call_ts, grace)).fetchone()
        if not row:
            return None
        return {"lease_id": row[0], "number": row[1], "ip": row[2],
                "leased_at": row[3], "expires_at": row[4]}

    def already_sent(self, call_id):
        with self._lock:
            return self._db.execute(
                "SELECT 1 FROM conversions WHERE call_id = ?", (call_id,)).fetchone() is not None

    def record(self, call_id, lease_id, ip, ok):
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO conversions VALUES (?,?,?,?,?)",
                (call_id, lease_id, ip, int(time.time()), 1 if ok else 0))
            self._db.commit()


# --------------------------------------------------------------------------- s2s

def send_conversion(ip, call_id, ts_ms, cfg, opener=None):
    """Fire the server-to-server conversion. Returns True on 2xx."""
    if cfg["dry_run"]:
        sys.stderr.write(f"[bridge] DRY RUN conversion ip={ip} eid={call_id}\n")
        return True
    params = {
        "aid": cfg["pixel_id"],
        "a": "lead",
        "eid": call_id,
        "ip": ip,           # the visitor's IP, captured by us — never this server's
        "ts": str(ts_ms),
    }
    url = cfg["s2s_url"] + "?" + urllib.parse.urlencode(params)
    try:
        op = opener or urllib.request.urlopen
        with op(url, timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception as exc:                              # noqa: BLE001
        sys.stderr.write(f"[bridge] conversion failed: {exc}\n")
        return False


# --------------------------------------------------------------------------- http

def client_ip(handler, hops):
    """Resolve the real client IP given a known number of trusted proxies.

    Each proxy appends the address it received the request from, so the chain is
    [client, p1, ..., pN] plus our own peer address. With `hops` trusted proxies
    in front of us, the client sits `hops + 1` from the end. Getting this wrong
    silently attributes every conversion to a proxy, which is the exact failure
    this whole service exists to avoid — so it is explicit config, not a guess.
    """
    peer = handler.client_address[0]
    if hops <= 0:
        return peer
    xff = handler.headers.get("X-Forwarded-For", "")
    chain = [p.strip() for p in xff.split(",") if p.strip()] + [peer]
    idx = len(chain) - (hops + 1)
    return chain[idx] if idx >= 0 else chain[0]


class Handler(BaseHTTPRequestHandler):
    server_version = "AttuneBridge/1.0"
    cfg = CONFIG
    store = None

    def log_message(self, fmt, *args):
        sys.stderr.write("[bridge] %s\n" % (fmt % args))

    # ---- helpers
    def _cors(self):
        origin = self.headers.get("Origin", "")
        allowed = self.cfg["origins"]
        if "*" in allowed:
            self.send_header("Access-Control-Allow-Origin", "*")
        elif origin in allowed:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")

    def _json(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return {}
        if n <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8", "replace")) or {}
        except Exception:                                  # noqa: BLE001
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def do_GET(self):
        if urllib.parse.urlparse(self.path).path == "/health":
            return self._json(200, {"ok": True, "pool": len(self.cfg["pool"])})
        self._json(404, {"error": "not found"})

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/v1/session":
            return self._session()
        if path == "/v1/call":
            return self._call()
        self._json(404, {"error": "not found"})

    # ---- endpoints
    def _session(self):
        body = self._body()
        ip = client_ip(self, self.cfg["proxy_hops"])
        lease = self.store.lease(self.cfg["pool"], ip, body.get("session"), self.cfg["lease_ttl"])
        if not lease:
            # Pool exhausted. Serve the fallback so the page still shows a number;
            # that call simply will not be attributable. Never break the page.
            self.log_message("pool exhausted, serving fallback")
            return self._json(200, {"number": self.cfg["fallback"], "leased": False})
        return self._json(200, {
            "number": lease["number"],
            "leased": True,
            "lease_id": lease["lease_id"],
            "ttl": self.cfg["lease_ttl"],
        })

    def _call(self):
        body = self._body()
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        def field(*names):
            for n in names:
                if body.get(n):
                    return str(body[n])
                if qs.get(n):
                    return str(qs[n][0])
            return None

        number = field("dialed_number", "to", "tracking_number", "number")
        call_id = field("call_id", "id", "uuid", "sid")
        ts_raw = field("timestamp", "ts", "created_at")

        if not number:
            return self._json(400, {"error": "no dialled number in payload"})
        if not call_id:
            return self._json(400, {"error": "no call id in payload"})

        try:
            call_ts = int(float(ts_raw)) if ts_raw else int(time.time())
            if call_ts > 10_000_000_000:      # milliseconds
                call_ts //= 1000
        except (TypeError, ValueError):
            call_ts = int(time.time())

        if self.store.already_sent(call_id):
            return self._json(200, {"matched": True, "conversion_sent": False,
                                    "reason": "duplicate call id"})

        lease = self.store.find_lease(number, call_ts, self.cfg["match_grace"])
        if not lease:
            # No lease means we do not know who called. Do NOT invent an IP —
            # a wrong attribution is worse than a missing one.
            self.log_message("no lease for %s at %s; not attributing", number, call_ts)
            return self._json(200, {"matched": False, "conversion_sent": False,
                                    "reason": "no matching lease"})

        ok = send_conversion(lease["ip"], call_id, call_ts * 1000, self.cfg)
        self.store.record(call_id, lease["lease_id"], lease["ip"], ok)
        return self._json(200, {"matched": True, "conversion_sent": ok,
                                "lease_id": lease["lease_id"]})


def build(cfg=None, store=None):
    cfg = cfg or CONFIG
    Handler.cfg = cfg
    Handler.store = store or Store(cfg["db"])
    return Handler


def main():
    cfg = CONFIG
    problems = []
    if not cfg["pool"]:
        problems.append("ATTUNE_POOL is empty — no numbers to lease")
    if not cfg["pixel_id"] and not cfg["dry_run"]:
        problems.append("ATTUNE_PIXEL_ID is unset")
    for n in cfg["pool"]:
        if not E164.match(n.replace(" ", "").replace("-", "")):
            problems.append(f"pool number {n!r} does not look like E.164")
    if problems:
        for p in problems:
            sys.stderr.write(f"[bridge] config: {p}\n")
        if not cfg["dry_run"]:
            sys.exit(1)

    port = int(_env("PORT", "8088"))
    build(cfg)
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    sys.stderr.write(f"[bridge] listening on :{port} pool={len(cfg['pool'])} "
                     f"dry_run={cfg['dry_run']} proxy_hops={cfg['proxy_hops']}\n")
    srv.serve_forever()


if __name__ == "__main__":
    main()
