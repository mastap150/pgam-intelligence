#!/usr/bin/env python3
"""Tests for the Attune Call Bridge. Runs a real server on a real socket."""

import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bridge  # noqa: E402

SENT = []          # conversions the bridge tried to send
PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"PASS :: {name}")
    else:
        FAIL += 1
        print(f"FAIL :: {name}" + (f" :: {extra}" if extra else ""))


def fake_opener(url, timeout=None):
    """Capture the S2S call instead of firing it at the real endpoint."""
    SENT.append(url)

    class R:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return False
    return R()


def post(port, path, payload, token="s3cret"):
    url = f"http://127.0.0.1:{port}{path}"
    if token and path.startswith("/v1/call"):
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode({"token": token})
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def main():
    tmp = tempfile.mkdtemp(prefix="bridge-test-")
    cfg = dict(bridge.CONFIG)
    cfg.update({
        "pool": ["+14055550101", "+14055550102"],
        "fallback": "+14055559999",
        "pixel_id": "PIXTEST",
        "lease_ttl": 60,
        "match_grace": 120,
        "db": os.path.join(tmp, "t.db"),
        "proxy_hops": 0,
        "dry_run": False,
        "origins": ["*"],
        "call_token": "s3cret",
        "lease_retention_days": 30,
    })

    # Route conversions to the capture instead of the network.
    real_send = bridge.send_conversion
    bridge.send_conversion = lambda ip, cid, ts, c, opener=None: real_send(
        ip, cid, ts, c, opener=fake_opener)

    store = bridge.Store(cfg["db"])
    handler = bridge.build(cfg, store)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    time.sleep(0.2)

    # ---- leasing -------------------------------------------------------
    s, a = post(port, "/v1/session", {"session": "a"})
    check("session leases a number", s == 200 and a.get("leased") and a["number"] in cfg["pool"], str(a))

    s, b = post(port, "/v1/session", {"session": "b"})
    check("second visitor gets a DIFFERENT number",
          b.get("leased") and b["number"] != a["number"], f"{a.get('number')} vs {b.get('number')}")

    s, c = post(port, "/v1/session", {"session": "c"})
    check("pool exhausted -> fallback, page never breaks",
          c.get("leased") is False and c["number"] == cfg["fallback"], str(c))

    # ---- matching ------------------------------------------------------
    SENT.clear()
    now = int(time.time())
    s, r = post(port, "/v1/call", {"dialed_number": a["number"], "call_id": "call-1", "timestamp": now})
    check("call on a leased number is attributed", r.get("matched") and r.get("conversion_sent"), str(r))
    check("conversion carries the visitor IP, not the server's",
          len(SENT) == 1 and "ip=127.0.0.1" in SENT[0], SENT[0] if SENT else "none")
    check("conversion sent as a lead with the call id",
          SENT and "a=lead" in SENT[0] and "eid=call-1" in SENT[0], SENT[0] if SENT else "none")

    # number format shouldn't matter
    SENT.clear()
    s, r = post(port, "/v1/call", {"to": "(405) 555-0102", "call_id": "call-2", "timestamp": now})
    check("dialled number matched regardless of formatting",
          r.get("matched") and r.get("conversion_sent"), str(r))

    # ---- dedup ---------------------------------------------------------
    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": a["number"], "call_id": "call-1", "timestamp": now})
    check("duplicate call id is not sent twice",
          r.get("conversion_sent") is False and not SENT, str(r))

    # ---- never invent an IP -------------------------------------------
    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": "+14055557777", "call_id": "call-3", "timestamp": now})
    check("unknown number is NOT attributed rather than guessed",
          r.get("matched") is False and not SENT, str(r))

    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": a["number"], "call_id": "call-4",
                                   "timestamp": now - 99999})
    check("call long before the lease is not attributed", r.get("matched") is False, str(r))

    # ---- late call within grace ---------------------------------------
    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": a["number"], "call_id": "call-5",
                                   "timestamp": now + 90})
    check("call after lease expiry but inside grace still attributes",
          r.get("matched") and r.get("conversion_sent"), str(r))

    # ---- malformed input ----------------------------------------------
    s, r = post(port, "/v1/call", {"call_id": "x"})
    check("missing dialled number rejected", s == 400, f"{s} {r}")
    s, r = post(port, "/v1/call", {"dialed_number": a["number"]})
    check("missing call id rejected", s == 400, f"{s} {r}")

    # ---- millisecond timestamps ---------------------------------------
    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": b["number"], "call_id": "call-6",
                                   "timestamp": now * 1000})
    check("millisecond timestamps handled", r.get("matched"), str(r))

    # ---- proxy hop resolution -----------------------------------------
    class FakeHandler:
        client_address = ("10.0.0.1",)
        headers = {"X-Forwarded-For": "203.0.113.9, 70.41.3.18"}
    check("proxy_hops=0 uses the peer address",
          bridge.client_ip(FakeHandler, 0) == "10.0.0.1")
    check("proxy_hops=1 walks back one hop",
          bridge.client_ip(FakeHandler, 1) == "70.41.3.18",
          bridge.client_ip(FakeHandler, 1))
    check("proxy_hops=2 reaches the real client",
          bridge.client_ip(FakeHandler, 2) == "203.0.113.9",
          bridge.client_ip(FakeHandler, 2))
    check("more hops than the chain degrades to the leftmost entry",
          bridge.client_ip(FakeHandler, 9) == "203.0.113.9")

    # ---- auth ----------------------------------------------------------
    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": b["number"], "call_id": "no-auth",
                                   "timestamp": now}, token=None)
    check("call without a token is rejected", s == 401 and not SENT, f"{s} {r}")

    s, r = post(port, "/v1/call", {"dialed_number": b["number"], "call_id": "bad-auth",
                                   "timestamp": now}, token="wrong")
    check("call with a wrong token is rejected", s == 401 and not SENT, f"{s} {r}")

    # ---- Config A: vendor supplies the visitor IP ----------------------
    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": "+19995550000", "call_id": "cfgA-1",
                                   "timestamp": now, "visitor_ip": "8.8.8.8"})
    check("payload IP is used directly, no lease needed",
          r.get("source") == "payload_ip" and r.get("conversion_sent"), str(r))
    check("payload IP is the one sent onward",
          SENT and "ip=8.8.8.8" in SENT[0], SENT[0] if SENT else "none")

    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": "+19995550000", "call_id": "cfgA-2",
                                   "timestamp": now, "visitor_ip": "10.1.2.3"})
    check("private IP in payload is refused, not attributed",
          r.get("matched") is False and not SENT, str(r))

    SENT.clear()
    s, r = post(port, "/v1/call", {"dialed_number": a["number"], "call_id": "cfgA-3",
                                   "timestamp": now, "visitor_ip": "not-an-ip"})
    check("garbage IP falls back to the lease rather than failing",
          r.get("source") == "lease" and r.get("conversion_sent"), str(r))

    check("public IP validator accepts a routable address",
          bridge.usable_public_ip("8.8.8.8"))
    check("public IP validator rejects loopback, link-local and doc ranges",
          not bridge.usable_public_ip("127.0.0.1")
          and not bridge.usable_public_ip("169.254.1.1")
          and not bridge.usable_public_ip("203.0.113.55"))

    # ---- pruning -------------------------------------------------------
    check("prune keeps leases inside the retention window",
          store.prune(30) == 0)
    check("prune removes leases past the retention window",
          store.prune(-1) >= 1)

    # ---- health --------------------------------------------------------
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as r:
        check("health endpoint", r.status == 200 and json.loads(r.read())["ok"])

    srv.shutdown()
    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
