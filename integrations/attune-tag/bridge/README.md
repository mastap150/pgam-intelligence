# Attune Call Bridge

**Internal.** Visitor-level call attribution that does not depend on the client's
phone vendor supporting it.

## Why this exists

Attributing a phone call to a household needs the **web visitor's IP**. The
client uses smrtPhone, nobody there could tell us whether their DNI is
visitor-level or whether their webhook carries that IP, and their docs are
unreachable from our environment.

This removes the dependency. Our tag already runs in the visitor's browser, so
we can see the visitor ourselves. The bridge leases a phone number to each
visitor, records the IP it saw them from, and matches an inbound call back to
that lease. The phone system only has to report **which number was dialled and
when** — something every phone system can do, smrtPhone included.

```
browser ──POST /v1/session──> bridge      sees the visitor's IP, leases a
   │                            │         number, returns it
   │  <──── number to show ─────┘
   │
visitor dials that number
   │
phone system ──POST /v1/call──> bridge    dialled number + time + call id
                                  │
                                  └─────> S2S conversion, carrying the IP
                                          we captured ourselves
```

## This does not contradict "never proxy the pixel"

It looks like it does, so: the **pixel beacon** must leave the visitor's browser,
because the tracker infers the household IP from the request's source address.
Relay that and it records our server. The **S2S conversion endpoint** is a
different mechanism — it takes `ip` as an explicit parameter and is built to be
called server-to-server. One infers the IP; the other is told it. Passing the
real visitor IP from here is exactly its intended use.

## Run it

```sh
export ATTUNE_POOL="+14055550101,+14055550102,+14055550103,+14055550104"
export ATTUNE_FALLBACK_NUMBER="+14055559999"   # shown if the pool is exhausted
export ATTUNE_PIXEL_ID="XXXXXX"
export ATTUNE_TRUSTED_PROXY_HOPS=1             # see below — get this right
python3 bridge.py
```

`ATTUNE_DRY_RUN=1` logs conversions instead of sending them. Use it for the
first live shakedown.

## Wire up the tag

```html
<script async src="https://tag.pgammedia.com/attune.js"
        data-attune-id="XXXXXX"
        data-attune-bridge="https://bridge.pgammedia.com"></script>
```

Mark the numbers to swap with `data-attune-number` (or set your own selector
with `data-attune-number-selector`):

```html
<a data-attune-number href="tel:+14055550100">(405) 555-0100</a>
```

If the bridge is slow, down, or returns nothing usable, the tag leaves the
markup untouched. Blanking a client's phone number is far worse than losing
attribution, and there is a test for exactly that.

## Wire up the phone system

Point a call-completion webhook at `POST /v1/call`. Field names are flexible —
it accepts `dialed_number` / `to` / `tracking_number` / `number`, `call_id` /
`id` / `uuid` / `sid`, and `timestamp` / `ts` / `created_at`, in JSON body or
query string. Seconds and milliseconds both work.

No webhook? Poll the call log and POST the same three fields. That is the whole
requirement, and it is why this works with any phone system.

## The setting most likely to break it

`ATTUNE_TRUSTED_PROXY_HOPS` is the number of proxies between the internet and
this process. Get it wrong and every conversion is attributed to a proxy's IP —
the precise failure this service exists to avoid, and it fails *silently*.

Each proxy appends the address it received the request from, so the client sits
`hops + 1` from the end of `X-Forwarded-For` plus the peer address. Direct: `0`.
Behind one nginx or one CDN: `1`. Behind a CDN *and* a load balancer: `2`.

Verify it rather than assuming: hit `/v1/session` from a known external IP and
confirm the leased row records that IP, not a private one.

## Design decisions

- **Never invent an IP.** A call with no matching lease is logged and dropped.
  A wrong attribution is worse than a missing one.
- **Pool exhaustion serves the fallback number.** That call is unattributed, but
  the page always shows a working number.
- **Least-recently-used leasing**, so a recycled number gets maximum cool-down
  before it can be confused with a previous visitor.
- **Grace window** (`ATTUNE_MATCH_GRACE`, default 1h) — people browse, then call
  later. The lease stays matchable past its display expiry.
- **Dedup on call id**, so webhook retries do not double-count.
- **SQLite**, so leases survive a restart. Traffic here is tiny.

## Sizing the pool

A number must be held for the whole time a visitor might act on it
(`ATTUNE_LEASE_TTL`, default 30 min). So the pool needs to cover **concurrent**
visitors, not daily ones.

Roughly: `pool ≥ peak visitors per hour × (lease_ttl / 3600)`, plus headroom.
At 40 visitors in the busiest hour with a 30-minute lease, that is ~20 numbers.
Start smaller and watch for "pool exhausted" in the logs — every one of those is
an unattributed call, and the log line is the signal to buy more numbers.

## Tests

```sh
python3 test_bridge.py     # 29 checks, runs a real server on a real socket
../test/run.sh             # 22 browser checks, includes the number swap
```

Covered: leasing and uniqueness, pool exhaustion, matching across number
formats, dedup, refusal to attribute unknown numbers, timestamp handling in
seconds and milliseconds, proxy-hop resolution at every depth, auth rejection
with a missing and a wrong token, the payload-IP path including refusal of
private and malformed addresses, lease pruning in both directions, and the
bridge-down case where the page's own number must survive untouched.

## Serves both configs from one endpoint

`/v1/call` does not need to know in advance whether the phone vendor can supply
a visitor IP:

- **Payload carries a usable public IP** → used directly, no lease lookup.
  That is Config A.
- **It does not, or the IP is private / reserved / malformed** → falls back to
  matching the leased number. That is Config B.

So the endpoint can be wired up before the DNI tests come back, and the answer
changes nothing about the wiring.

A private or reserved address in the payload is refused rather than used. A
vendor sending its own server IP would otherwise put every call on one address
— garbage that looks exactly like success. Note `203.0.113.0/24` and the other
RFC 5737 documentation ranges count as non-routable here.

## Auth

`/v1/call` takes a shared secret, as `Authorization: Bearer <token>`,
`X-Attune-Token`, or `?token=` for senders that only let you configure a URL.
Compared in constant time. Generate with `openssl rand -hex 32`.

If `ATTUNE_CALL_TOKEN` is unset the endpoint is open and startup warns loudly.
Never leave it unset anywhere publicly reachable.

## Deploying

`attune-bridge.service`, `attune-bridge.env.example` and `nginx.conf.example`
are in this directory.

```sh
install -d /opt/attune-bridge && cp bridge.py /opt/attune-bridge/
cp attune-bridge.env.example /etc/attune-bridge.env   # fill in, chmod 600
cp attune-bridge.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now attune-bridge
```

The nginx example terminates TLS and is exactly one hop, so
`ATTUNE_TRUSTED_PROXY_HOPS=1`. Verify it before trusting the data: hit
`/v1/session` from a known external IP and confirm the stored lease carries
*that* address, not a private one.

Leases older than `ATTUNE_LEASE_RETENTION_DAYS` (default 30) are pruned at
startup and daily thereafter. Conversion rows are kept — they are the dedup
record and there is one small row per real call.

## Not done

- Never deployed or run against a real phone system
- `http.server` is adequate for one local advertiser but belongs behind the
  reverse proxy above for TLS and timeouts
