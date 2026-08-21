#!/usr/bin/env python3
"""
claude_usage.py — local Claude Code usage/cost visibility.

Reads the per-session transcripts Claude Code writes to
~/.claude/projects/<slug>/<session-uuid>.jsonl and aggregates the `usage`
block that every assistant turn carries. Nothing here calls an API, so it
works offline and costs nothing to run.

    python3 scripts/claude_usage.py                 # last 7 days
    python3 scripts/claude_usage.py --days 1        # today + yesterday
    python3 scripts/claude_usage.py --sessions      # per-session breakdown
    python3 scripts/claude_usage.py --runaway       # only the warnings
    python3 scripts/claude_usage.py --json          # machine-readable

WHAT THIS CAN AND CANNOT MEASURE
--------------------------------
CAN (authoritative, straight from the transcript):
  * input / output / cache-read / cache-write tokens per turn, session, day,
    project and model
  * the cache-TTL split (ephemeral_1h vs ephemeral_5m) — the single best
    early warning that context is being re-written rather than re-read
  * turn counts, session wall-clock, thinking tokens, repeated tool calls

CANNOT:
  * the billed dollar amount. Only Anthropic knows that. The `est_cost`
    column is list-price arithmetic (table below) and on long agentic
    sessions it has been observed to UNDERSTATE the platform's own
    cost figure by several times over — long-context (>200K) premium tiers
    and cache expiry are not modelled here. Treat est_cost as a relative
    signal for ranking sessions, never as an invoice.
  * usage from claude.ai chat, Claude Cowork, or cloud (claude.ai/code)
    sessions — those never write a local transcript. For cloud sessions ask
    a Claude Code session for `list_sessions`, which returns a real
    `cost_usd` per session.

Ground truth for money is console.anthropic.com → Usage / Billing.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import glob
import hashlib
import json
import os
import subprocess
import sys

# List prices, USD per 1M tokens. Source: Anthropic public pricing.
# Cache write = 1.25x input (5m TTL), cache read = 0.10x input.
# Update these when pricing changes; they are the only hardcoded rates.
PRICES = {
    "claude-opus-5":       {"in": 5.00, "out": 25.00},
    "claude-opus-4-8":     {"in": 5.00, "out": 25.00},
    "claude-opus-4-7":     {"in": 5.00, "out": 25.00},
    "claude-opus-4-6":     {"in": 5.00, "out": 25.00},
    "claude-fable-5":      {"in": 10.00, "out": 50.00},
    "claude-sonnet-5":     {"in": 3.00, "out": 15.00},
    "claude-sonnet-4-6":   {"in": 3.00, "out": 15.00},
    "claude-haiku-4-5":    {"in": 1.00, "out": 5.00},
}
CACHE_WRITE_MULT = 1.25
CACHE_READ_MULT = 0.10

# Thresholds for the runaway check. Deliberately loose — these are meant to
# catch the pathological case, not to nag during normal work.
WARN_SESSION_CACHE_READ = 20_000_000   # tokens of context replay in one session
WARN_CONTEXT_PER_TURN   = 400_000      # avg context re-ingested per turn
WARN_REPLAY_RATIO       = 150          # context tokens per output token
WARN_SESSION_HOURS      = 12           # wall-clock span of a single session
WARN_5M_CACHE_SHARE     = 0.50         # share of cache writes on the short TTL


def price(model: str, kind: str) -> float:
    if not model:
        return 0.0
    base = PRICES.get(model)
    if base is None:                      # tolerate suffixes like "[1m]"
        for k, v in PRICES.items():
            if model.startswith(k):
                base = v
                break
    if base is None:
        return 0.0
    return base["in"] if kind != "out" else base["out"]


def cost(model: str, inp: int, out: int, cr: int, cw: int) -> float:
    pin, pout = price(model, "in"), price(model, "out")
    return (
        inp * pin / 1e6
        + out * pout / 1e6
        + cr * pin * CACHE_READ_MULT / 1e6
        + cw * pin * CACHE_WRITE_MULT / 1e6
    )


class Acc:
    """Token accumulator."""
    __slots__ = ("inp", "out", "cr", "cw", "cw1h", "cw5m", "think", "turns",
                 "cost", "first", "last", "models")

    def __init__(self):
        self.inp = self.out = self.cr = self.cw = self.cw1h = self.cw5m = 0
        self.think = self.turns = 0
        self.cost = 0.0
        self.first = self.last = None
        self.models = collections.Counter()

    def add(self, model, u, ts):
        self.turns += 1
        self.models[model] += 1
        i = u.get("input_tokens", 0) or 0
        o = u.get("output_tokens", 0) or 0
        cr = u.get("cache_read_input_tokens", 0) or 0
        cw = u.get("cache_creation_input_tokens", 0) or 0
        self.inp += i; self.out += o; self.cr += cr; self.cw += cw
        cc = u.get("cache_creation") or {}
        self.cw1h += cc.get("ephemeral_1h_input_tokens", 0) or 0
        self.cw5m += cc.get("ephemeral_5m_input_tokens", 0) or 0
        self.think += (u.get("output_tokens_details") or {}).get("thinking_tokens", 0) or 0
        self.cost += cost(model, i, o, cr, cw)
        if ts:
            if self.first is None or ts < self.first:
                self.first = ts
            if self.last is None or ts > self.last:
                self.last = ts

    @property
    def context(self):
        return self.cr + self.cw

    @property
    def hours(self):
        if self.first and self.last:
            return (self.last - self.first).total_seconds() / 3600.0
        return 0.0

    @property
    def top_model(self):
        return self.models.most_common(1)[0][0] if self.models else "?"


def parse_ts(s):
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def scan(root, since):
    """Yield (project, session, model, usage, timestamp) per assistant turn."""
    seen_req = set()
    for path in glob.glob(os.path.join(root, "projects", "*", "*.jsonl")):
        project = os.path.basename(os.path.dirname(path))
        session = os.path.basename(path)[:-6]
        try:
            fh = open(path, errors="replace")
        except OSError:
            continue
        with fh:
            for line in fh:
                line = line.strip()
                if not line or '"usage"' not in line:
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                if d.get("type") != "assistant":
                    continue
                msg = d.get("message") or {}
                u = msg.get("usage")
                if not isinstance(u, dict):
                    continue
                # Claude Code can write the same assistant turn more than once
                # (streaming partials, replays). requestId dedupes it.
                rid = d.get("requestId")
                key = (session, rid)
                if rid and key in seen_req:
                    continue
                if rid:
                    seen_req.add(key)
                ts = parse_ts(d.get("timestamp"))
                if since and ts and ts.date() < since:
                    continue
                yield project, session, msg.get("model") or "?", u, ts


def scan_tool_calls(root, since):
    """Count repeated identical tool calls per session (duplicate-work signal)."""
    dupes = collections.Counter()
    for path in glob.glob(os.path.join(root, "projects", "*", "*.jsonl")):
        session = os.path.basename(path)[:-6]
        sigs = collections.Counter()
        try:
            fh = open(path, errors="replace")
        except OSError:
            continue
        with fh:
            for line in fh:
                if '"tool_use"' not in line:
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                ts = parse_ts(d.get("timestamp"))
                if since and ts and ts.date() < since:
                    continue
                for b in (d.get("message") or {}).get("content") or []:
                    if isinstance(b, dict) and b.get("type") == "tool_use":
                        sig = b.get("name", "") + "|" + json.dumps(
                            b.get("input"), sort_keys=True, default=str)
                        sigs[hashlib.sha1(sig.encode()).hexdigest()] += 1
        repeats = sum(c - 1 for c in sigs.values() if c > 1)
        if repeats:
            dupes[session] = repeats
    return dupes


def live_processes():
    try:
        out = subprocess.run(["ps", "ax", "-o", "pid=,etime=,args="],
                             capture_output=True, text=True, timeout=10).stdout
    except Exception:
        return []
    procs = []
    for line in out.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        pid, etime, args = parts
        base = args.split()[0].rsplit("/", 1)[-1] if args else ""
        if base == "claude" or "/claude " in args or args.endswith("/claude"):
            procs.append((pid, etime, args[:110]))
    return procs


def fmt(n):
    return f"{n:,}"


def bar(label, acc, width=38):
    return (f"  {label:<{width}} {acc.turns:>5} {fmt(acc.inp):>9} "
            f"{fmt(acc.out):>9} {fmt(acc.cr):>13} {fmt(acc.cw):>11} "
            f"${acc.cost:>9.2f}")


def header(width=38):
    return (f"  {'':<{width}} {'turns':>5} {'input':>9} {'output':>9} "
            f"{'cache read':>13} {'cache wr':>11} {'est cost':>10}")


def main():
    ap = argparse.ArgumentParser(description="Local Claude Code usage report.")
    ap.add_argument("--days", type=int, default=7, help="lookback window (default 7)")
    ap.add_argument("--root", default=os.path.expanduser("~/.claude"))
    ap.add_argument("--sessions", action="store_true", help="per-session table")
    ap.add_argument("--runaway", action="store_true", help="only the warnings")
    ap.add_argument("--json", action="store_true", dest="as_json")
    ap.add_argument("--no-cost", action="store_true", help="hide cost estimates")
    args = ap.parse_args()

    today = dt.datetime.now(dt.timezone.utc).date()
    since = today - dt.timedelta(days=args.days)

    by_day, by_proj, by_sess, by_model = ({} for _ in range(4))
    total = Acc()
    for project, session, model, u, ts in scan(args.root, since):
        total.add(model, u, ts)
        day = ts.date().isoformat() if ts else "unknown"
        for bucket, key in ((by_day, day), (by_proj, project),
                            (by_sess, session), (by_model, model)):
            bucket.setdefault(key, Acc()).add(model, u, ts)

    if not total.turns:
        print(f"No local transcripts with usage data under {args.root}/projects "
              f"in the last {args.days} day(s).")
        print("Cloud (claude.ai/code) and Cowork sessions do not write local "
              "transcripts — ask a session for `list_sessions` instead.")
        return 0

    dupes = scan_tool_calls(args.root, since)

    # ---- runaway checks -------------------------------------------------
    warnings = []
    for sid, a in by_sess.items():
        ctx_per_turn = a.context / a.turns if a.turns else 0
        replay = a.context / a.out if a.out else 0
        if a.cr >= WARN_SESSION_CACHE_READ:
            warnings.append((sid, "context-replay",
                             f"{fmt(a.cr)} cache-read tokens in one session"))
        if ctx_per_turn >= WARN_CONTEXT_PER_TURN:
            warnings.append((sid, "large-context",
                             f"{fmt(int(ctx_per_turn))} context tokens per turn"))
        if replay >= WARN_REPLAY_RATIO and a.out > 5000:
            warnings.append((sid, "low-yield",
                             f"{replay:.0f} context tokens per output token"))
        if a.hours >= WARN_SESSION_HOURS:
            warnings.append((sid, "long-lived",
                             f"session spans {a.hours:.1f}h — consider a fresh one"))
        if a.cw and (a.cw5m / a.cw) >= WARN_5M_CACHE_SHARE and a.cw > 200_000:
            warnings.append((sid, "short-cache-ttl",
                             f"{100*a.cw5m/a.cw:.0f}% of cache writes on the 5m TTL "
                             "— context is being rewritten, not reused"))
        if dupes.get(sid, 0) >= 25:
            warnings.append((sid, "duplicate-tool-calls",
                             f"{dupes[sid]} repeated identical tool calls"))

    procs = live_processes()

    if args.as_json:
        def dump(d):
            return {k: {"turns": v.turns, "input": v.inp, "output": v.out,
                        "cache_read": v.cr, "cache_write": v.cw,
                        "cache_write_1h": v.cw1h, "cache_write_5m": v.cw5m,
                        "thinking": v.think, "hours": round(v.hours, 2),
                        "est_cost_usd": round(v.cost, 4), "model": v.top_model}
                    for k, v in d.items()}
        print(json.dumps({
            "window_days": args.days, "generated_utc": dt.datetime.now(
                dt.timezone.utc).isoformat(),
            "total": dump({"all": total})["all"],
            "by_day": dump(by_day), "by_project": dump(by_proj),
            "by_session": dump(by_sess), "by_model": dump(by_model),
            "warnings": [{"session": s, "kind": k, "detail": d}
                         for s, k, d in warnings],
            "live_claude_processes": [
                {"pid": p, "elapsed": e, "cmd": c} for p, e, c in procs],
            "cost_caveat": "est_cost_usd is list-price arithmetic and can "
                           "materially understate billed cost on long "
                           "sessions. Authoritative: console.anthropic.com.",
        }, indent=2))
        return 0

    if args.runaway:
        if not warnings:
            print(f"No runaway signals in the last {args.days} day(s). "
                  f"{total.turns} turns across {len(by_sess)} session(s).")
        else:
            print(f"RUNAWAY SIGNALS ({len(warnings)}):\n")
            for sid, kind, detail in sorted(warnings, key=lambda w: w[1]):
                print(f"  [{kind:<21}] {sid[:8]}  {detail}")
        if procs:
            print(f"\nLive claude processes: {len(procs)}")
            for pid, etime, cmd in procs:
                print(f"  pid {pid:>7}  up {etime:>11}  {cmd}")
        return 0

    w = 38
    print(f"\nClaude Code local usage — last {args.days} day(s) "
          f"(to {today.isoformat()})")
    print("=" * 104)
    print(f"\nBY DAY\n{header(w)}")
    for day in sorted(by_day):
        print(bar(day, by_day[day], w))
    print("  " + "-" * 100)
    print(bar("TOTAL", total, w))

    print(f"\nBY PROJECT\n{header(w)}")
    for k, a in sorted(by_proj.items(), key=lambda kv: -kv[1].cost):
        print(bar(k[:w], a, w))

    print(f"\nBY MODEL\n{header(w)}")
    for k, a in sorted(by_model.items(), key=lambda kv: -kv[1].cost):
        print(bar(k[:w], a, w))

    if args.sessions:
        print(f"\nBY SESSION (most expensive first)\n{header(w)}")
        for k, a in sorted(by_sess.items(), key=lambda kv: -kv[1].cost):
            label = f"{k[:8]} {a.top_model[:18]} {a.hours:4.1f}h"
            print(bar(label, a, w))

    ctx = total.context
    print("\nWHERE THE TOKENS GO")
    print(f"  context re-ingestion (cache read + write) : {fmt(ctx)}"
          f"  ({100*ctx/(ctx+total.inp+total.out):.1f}% of all tokens)")
    print(f"  generated output                          : {fmt(total.out)}"
          f"  ({100*total.out/(ctx+total.inp+total.out):.1f}%)")
    if total.out:
        print(f"  context replayed per output token         : {ctx/total.out:.0f}x")
    if total.cw:
        print(f"  cache writes on 1h TTL / 5m TTL          : "
              f"{fmt(total.cw1h)} / {fmt(total.cw5m)}"
              f"  ({100*total.cw5m/total.cw:.0f}% short-TTL)")
    if total.think:
        print(f"  thinking tokens                          : {fmt(total.think)}")

    if warnings:
        print(f"\nRUNAWAY SIGNALS ({len(warnings)}) — run --runaway for just these")
        for sid, kind, detail in sorted(warnings, key=lambda x: x[1])[:12]:
            print(f"  [{kind:<21}] {sid[:8]}  {detail}")
        if len(warnings) > 12:
            print(f"  ... and {len(warnings)-12} more")

    print(f"\nLIVE CLAUDE PROCESSES: {len(procs)}")
    for pid, etime, cmd in procs:
        print(f"  pid {pid:>7}  up {etime:>11}  {cmd}")

    if not args.no_cost:
        print("\nest cost = list-price arithmetic on local transcripts only.")
        print("It EXCLUDES claude.ai chat, Cowork and cloud sessions, and has")
        print("been seen to understate billed cost on long agentic sessions.")
        print("Authoritative figures: console.anthropic.com -> Usage / Billing.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
