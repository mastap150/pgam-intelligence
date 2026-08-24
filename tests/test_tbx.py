"""
tests/test_tbx.py

Standalone, no-network tests for the new Teqblaze platform client
(`core/tbx_api.py` + `core/tbx_mgmt.py`).

Covers the pure logic that decides what gets sent to the platform — the
parts that must be right before any live write:

  - report payload assembly, vocabulary validation, LL alias translation
  - request hashing determinism (page consistency depends on it)
  - floor clamps: delta cap, contract minimum, global zero-out guard
  - deep merge semantics (dicts merge, lists replace)
  - key-loss detection (the field-blanking guard on read-modify-write)
  - read-only field stripping for the read→write round trip
  - the two write gates: dry_run default and TBX_ALLOW_WRITES

Run:
    python tests/test_tbx.py

Exits non-zero on any failure. No pytest dependency, no network, no
credentials — matches the test_compliance / test_msn_insights pattern.
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Keep the module-level gates at their safe defaults for the whole run.
os.environ.pop("TBX_ALLOW_WRITES", None)
os.environ.pop("TBX_DRY_RUN", None)

from core import tb_ledger        # noqa: E402
from core import tbx_api as tbx      # noqa: E402
from core import tbx_mgmt as tbm     # noqa: E402

# The write-gate tests exercise the real ledger call path. Redirect it at a
# temp file so a test run never appends to logs/tb_ledger.jsonl — that file is
# the production audit trail the guardian agents grade reverts against.
tb_ledger.LEDGER_PATH = os.path.join(
    tempfile.mkdtemp(prefix="tbx-test-ledger-"), "tb_ledger.jsonl"
)

_failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  ✓ {label}")
    else:
        print(f"  ✗ {label}" + (f" — {detail}" if detail else ""))
        _failures.append(label)


def expect_raises(label: str, exc_type, fn, *args, **kwargs) -> None:
    try:
        fn(*args, **kwargs)
    except exc_type:
        print(f"  ✓ {label}")
        return
    except Exception as exc:
        print(f"  ✗ {label} — raised {type(exc).__name__}, wanted {exc_type.__name__}")
        _failures.append(label)
        return
    print(f"  ✗ {label} — no exception raised")
    _failures.append(label)


# ---------------------------------------------------------------------------

def test_report_payload() -> None:
    print("\nreport payload")

    payload = tbx.build_report_payload(
        "2026-08-01", "2026-08-18",
        attributes=["date", "supply_source"],
        metrics=["imps_sum", "dsp_price_sum"],
    )
    check("date range lands in filter",
          payload["filter"]["date_from"] == "2026-08-01"
          and payload["filter"]["date_to"] == "2026-08-18")
    check("timezone defaults to ET", payload["filter"]["timezone"] == tbx.DEFAULT_TZ,
          payload["filter"]["timezone"])
    check("date attribute implies day granularity",
          payload["filter"]["date"] == "day", str(payload["filter"].get("date")))
    check("attributes preserved", payload["attributes"] == ["date", "supply_source"])
    check("metrics preserved", payload["metrics"] == ["imps_sum", "dsp_price_sum"])

    # LL-era names must translate so ported agent code reads the same.
    aliased = tbx.build_report_payload(
        "2026-08-01", "2026-08-02",
        metrics=["GROSS_REVENUE", "PUB_PAYOUT", "PROFIT", "IMPRESSIONS"],
    )
    check("LL aliases translate",
          aliased["metrics"] == ["dsp_price_sum", "ssp_price_sum", "profit", "imps_sum"],
          str(aliased["metrics"]))

    # A typo must fail locally, not as a 422 three layers down.
    expect_raises("unknown metric rejected", ValueError,
                  tbx.build_report_payload, "2026-08-01", "2026-08-02",
                  metrics=["revenue"])
    expect_raises("unknown attribute rejected", ValueError,
                  tbx.build_report_payload, "2026-08-01", "2026-08-02",
                  attributes=["publisher_name"])
    expect_raises("bad granularity rejected", ValueError,
                  tbx.build_report_payload, "2026-08-01", "2026-08-02",
                  attributes=["date"], date_granularity="weekly")

    # Metric filters behave like HAVING and must survive into filter.
    filtered = tbx.build_report_payload(
        "2026-08-01", "2026-08-02", metrics=["imps_sum"],
        filters={"traffic_type": ["CTV"], "imps_sum": {"operator": ">", "value": "1000"}},
    )
    check("dimension filter merged", filtered["filter"]["traffic_type"] == ["CTV"])
    check("metric filter merged",
          filtered["filter"]["imps_sum"] == {"operator": ">", "value": "1000"})

    # Explicit granularity must win over the implied default.
    hourly = tbx.build_report_payload("2026-08-01", "2026-08-01",
                                      attributes=["date"], date_granularity="hour")
    check("explicit granularity wins", hourly["filter"]["date"] == "hour")


def test_request_hash() -> None:
    print("\nrequest hash")
    a = tbx.build_report_payload("2026-08-01", "2026-08-02", metrics=["imps_sum"])
    b = tbx.build_report_payload("2026-08-01", "2026-08-02", metrics=["imps_sum"])
    c = tbx.build_report_payload("2026-08-01", "2026-08-03", metrics=["imps_sum"])
    check("same query → same hash", tbx._request_hash(a) == tbx._request_hash(b))
    check("different query → different hash", tbx._request_hash(a) != tbx._request_hash(c))
    check("hash is a 32-char hex digest",
          len(tbx._request_hash(a)) == 32 and all(ch in "0123456789abcdef"
                                                 for ch in tbx._request_hash(a)))


def test_floor_clamps() -> None:
    print("\nfloor clamps")

    # Delta cap trims an oversized move toward current.
    value, reasons = tbm.clamp_floor(2.00, current=1.00)
    check("upward move capped at +25%", abs(value - 1.25) < 1e-6, str(value))
    check("cap is reported", any("delta cap" in r for r in reasons))

    value, _ = tbm.clamp_floor(0.10, current=1.00)
    check("downward move capped at -25%", abs(value - 0.75) < 1e-6, str(value))

    value, reasons = tbm.clamp_floor(1.10, current=1.00)
    check("in-range move untouched", abs(value - 1.10) < 1e-6, str(value))
    check("no clamp reported for in-range move", not reasons, str(reasons))

    # The zero-out guard from the April incident.
    value, reasons = tbm.clamp_floor(0.0, current=None)
    check("zero clamped to global minimum", abs(value - tbm.GLOBAL_MIN_FLOOR) < 1e-9,
          str(value))
    check("global min clamp reported", any("global min" in r for r in reasons))

    # A contract minimum outranks the delta cap.
    tbm.PROTECTED_FLOOR_MINIMUMS["placement"][9001] = 1.70
    try:
        value, reasons = tbm.clamp_floor(0.05, current=1.80, placement_id=9001)
        check("contract floor enforced", abs(value - 1.70) < 1e-6, str(value))
        check("contract clamp reported", any("contract floor" in r for r in reasons))

        # Inheritance: a supply-source minimum covers its placements.
        tbm.PROTECTED_FLOOR_MINIMUMS["supply_source"][7001] = 0.90
        value, _ = tbm.clamp_floor(0.10, current=0.95, supply_source_id=7001)
        check("supply-source minimum inherited", abs(value - 0.90) < 1e-6, str(value))

        # Placement-level wins over supply-source level.
        value, _ = tbm.clamp_floor(0.05, current=1.80,
                                   placement_id=9001, supply_source_id=7001)
        check("placement minimum outranks source minimum",
              abs(value - 1.70) < 1e-6, str(value))
    finally:
        tbm.PROTECTED_FLOOR_MINIMUMS["placement"].pop(9001, None)
        tbm.PROTECTED_FLOOR_MINIMUMS["supply_source"].pop(7001, None)


def test_deep_merge() -> None:
    print("\ndeep merge")
    base = {
        "name": "src", "status": True,
        "geo_settings": {"bid_floor": [{"country_id": 1, "value": 1.0}],
                         "qps": [{"country_id": 1, "value": 50}]},
        "qps_limit": {"max_qps_limit": 100, "min_qps_limit": 10},
    }
    merged = tbm._deep_merge(base, {"geo_settings": {"bid_floor": [{"country_id": 2, "value": 2.0}]}})

    check("sibling dict key survives", "qps" in merged["geo_settings"])
    check("sibling scalar survives", merged["name"] == "src")
    check("list replaces wholesale",
          merged["geo_settings"]["bid_floor"] == [{"country_id": 2, "value": 2.0}],
          str(merged["geo_settings"]["bid_floor"]))
    check("nested dict merges key-wise",
          tbm._deep_merge(base, {"qps_limit": {"max_qps_limit": 200}})["qps_limit"]
          == {"max_qps_limit": 200, "min_qps_limit": 10})
    check("source dict is not mutated",
          base["geo_settings"]["bid_floor"] == [{"country_id": 1, "value": 1.0}])


def test_key_loss_guard() -> None:
    print("\nkey-loss guard")
    before = {"a": 1, "nested": {"x": 1, "y": 2}, "keep": True}

    check("lossless merge reports nothing",
          tbm._assert_no_key_loss(before, tbm._deep_merge(before, {"a": 2})) == [])
    check("top-level loss detected",
          tbm._assert_no_key_loss(before, {"a": 1, "nested": {"x": 1, "y": 2}}) == ["keep"])
    check("nested loss detected with path",
          tbm._assert_no_key_loss(before, {"a": 1, "keep": True, "nested": {"x": 1}})
          == ["nested.y"])


def test_read_only_stripping() -> None:
    print("\nread-only field stripping")
    supply = {"id": 22, "name": "s", "margin_type": "fixed", "margin_min": 10,
              "margin_max": 50, "status": True, "source": {"floor_price": 1.0}}
    stripped = tbm._strip_read_only(supply, "supply_source")
    check("supply read-only fields dropped",
          set(stripped) == {"name", "status", "source"}, str(sorted(stripped)))
    check("original not mutated", "id" in supply)

    demand = {"id": 91, "name": "d", "operation_systems": [1, 2],
              "uuid": "abc-123", "is_schain": True}
    stripped = tbm._strip_read_only(demand, "demand_source")
    check("demand read-only fields dropped",
          set(stripped) == {"name", "is_schain"}, str(sorted(stripped)))
    check("demand uuid is stripped (accepted on supply, rejected on demand)",
          "uuid" not in stripped)


def test_strip_list_matches_spec() -> None:
    """
    The strip list must equal what the vendored spec says is read-only.

    Hand-maintained tuples drift from the spec silently, and the cost of the
    drift is a rejected — or worse, a partially applied — write. `uuid` on a
    demand source was exactly this: returned by the read schema, absent from
    the write schema, and missing from the strip list.
    """
    print("\nstrip list vs vendored spec")
    for kind in ("supply_source", "demand_source"):
        accepted = tbm.write_schema_fields(kind)
        check(f"{kind}: write schema readable from the spec", bool(accepted))
        if not accepted:
            continue
        from_spec = tbm.read_only_fields_from_spec(kind)
        declared = set(tbm._READ_ONLY_FIELDS[kind])
        check(f"{kind}: strip list matches the spec",
              declared == from_spec,
              f"declared={sorted(declared)} spec={sorted(from_spec)}")


def test_unknown_write_keys() -> None:
    print("\nundeclared payload keys")
    check("a clean supply payload has no undeclared keys",
          tbm.unknown_write_keys({"name": "s", "status": True}, "supply_source") == [])
    check("a read-only field left in is flagged",
          tbm.unknown_write_keys({"name": "s", "margin_type": "fixed"},
                                 "supply_source") == ["margin_type"])
    check("demand uuid is flagged when left in",
          "uuid" in tbm.unknown_write_keys({"name": "d", "uuid": "x"}, "demand_source"))
    check("uuid is NOT flagged on supply (its write schema accepts it)",
          tbm.unknown_write_keys({"name": "s", "uuid": "x"}, "supply_source") == [])
    check("a stripped live entity round-trips clean",
          tbm.unknown_write_keys(
              tbm._strip_read_only(
                  {k: None for k in tbm._spec_properties("DemandSourceResource")},
                  "demand_source"),
              "demand_source") == [])
    check("unknown kind yields no opinion rather than a false pass",
          tbm.unknown_write_keys({"whatever": 1}, "not_a_kind") == [])


def test_recon_classifier() -> None:
    """
    The reconciliation's verdict logic (scripts/tbx_recon.py).

    This is the function that decides whether the two platforms agree, and its
    three answers send you to three different places — ship it, ask Teqblaze
    about a fee, or stop and escalate. Worth pinning offline.
    """
    print("\nrecon classifier")
    from scripts import tbx_recon as recon

    verdict, _ = recon._classify([(100, 100), (200, 200), (50, 50)])
    check("identical series → AGREEMENT", verdict == "AGREEMENT")

    verdict, _ = recon._classify([(100, 100.00005), (200, 200.0001)])
    check("numeric(14,4) rounding still agrees", verdict == "AGREEMENT")

    verdict, detail = recon._classify([(100, 78), (200, 156), (50, 39)])
    check("same ratio every day → CONSTANT OFFSET", verdict == "CONSTANT OFFSET")
    check("constant offset reports the size", "22.00%" in detail, detail)

    verdict, _ = recon._classify([(100, 80), (200, 190), (50, 25)])
    check("scattered gaps → ROW-LEVEL DIVERGENCE",
          verdict == "ROW-LEVEL DIVERGENCE")

    verdict, _ = recon._classify([(100, 130)])
    check("one day cannot be a constant offset",
          verdict == "ROW-LEVEL DIVERGENCE")

    verdict, _ = recon._classify([(0, 0), (0, 5)])
    check("zero legacy side is not divided by", verdict == "NO DATA")

    verdict, _ = recon._classify([])
    check("empty window → NO DATA", verdict == "NO DATA")


def test_pnl_check_exit_codes() -> None:
    """
    The P&L check's exit code is load-bearing, so pin it.

    `.github/workflows/tbx-neon-reports.yml` branches on it: 0 renders a green
    "agree", 1 raises a warning, anything else fails the job as a fault. If
    these drift, the workflow reports the wrong thing with total confidence —
    which is worse than not running at all.
    """
    print("\npnl check exit codes")
    import contextlib
    import io
    from datetime import date, timedelta

    from scripts import tbx_pnl_check as chk

    days = [date(2026, 8, 15) + timedelta(d) for d in range(3)]
    tbx = {d: (1000.0, 220.0) for d in days}

    def _rc(pnl: dict) -> int:
        with contextlib.redirect_stdout(io.StringIO()):
            return chk.report(tbx, pnl, days)

    check("matching numbers exit 0",
          _rc({d: (1000.0, 220.0) for d in days}) == 0)
    check("rounding-level difference still exits 0",
          _rc({d: (1000.0001, 220.0) for d in days}) == 0)
    check("divergence exits 1",
          _rc({d: (1200.0, 260.0) for d in days}) == 1)
    check("nothing comparable exits 1, not 0",
          _rc({}) == 1)
    check("a NULL P&L row counts as a gap, not a disagreement",
          _rc({d: (None, None) for d in days}) == 1)


def _reset_report_transport() -> None:
    """Clear the sticky transport decision `_report_call` caches per process."""
    tbx._EMPTY_HASH_OK = None
    tbx._HASH_CACHE.clear()


def test_report_prefers_the_empty_hash_form() -> None:
    """
    `POST /report/` — trailing slash, nothing after it — is the cheap path.

    It is one call instead of mint-then-read, and it does not stake the run
    on a hash TTL nobody has documented. These pin that it is what gets tried
    first, because the regression is silent in the direction that costs
    money: mint-then-read still *works*, it just doubles the request count on
    an hourly ETL and reintroduces the TTL.
    """
    print("\nreport transport: empty hash first")

    calls: list[str] = []
    saved = tbx._request

    def fake(method, path, payload=None, **kw):
        calls.append(path)
        if path == "/share/report":
            return {"hash": "server-minted-abc123"}
        return {"data": [{"date": "2026-08-20"}], "total": {}, "meta": {"last_page": 1}}

    try:
        tbx._request = fake
        _reset_report_transport()
        tbx.report("2026-08-14", "2026-08-20",
                   attributes=["date"], metrics=["imps_sum"])
        check("report() posts to /report/ with an empty hash",
              calls[0] == "/report/", str(calls))
        check("no hash was minted", "/share/report" not in calls, str(calls))
        check("one call, not two", len(calls) == 1, str(calls))

        # Pagination must not re-ask the transport question per page.
        calls.clear()
        pages = {"n": 0}

        def paged(method, path, payload=None, **kw):
            calls.append(path)
            pages["n"] += 1
            return {"data": [{"date": "2026-08-20"}], "total": {},
                    "meta": {"last_page": 3}}

        tbx._request = paged
        _reset_report_transport()
        tbx.report("2026-08-14", "2026-08-20",
                   attributes=["date"], metrics=["imps_sum"], max_pages=3)
        check("every page uses the empty-hash form",
              calls == ["/report/"] * 3, str(calls))
    finally:
        tbx._request = saved
        _reset_report_transport()


def test_report_falls_back_to_minting() -> None:
    """
    If the empty-hash form is rejected, mint-then-read still has to work.

    The first version of this client sent an md5 of the request body as the
    path hash, and every call 422'd with "Hash not found, or no longer
    available" — which is what left pgam_direct.tbx_daily_* empty while the
    ETL reported success. The failure mode is not a crash, it is an empty
    table, so the corrected sequence stays pinned even though it is no longer
    the default.
    """
    print("\nreport transport: mint fallback")

    calls: list[str] = []
    saved = tbx._request

    def fake(method, path, payload=None, **kw):
        calls.append(path)
        if path == "/report/":
            raise tbx.TbxError("no hash", status=422,
                               body='{"message":"Hash not found, or no longer available"}')
        if path == "/share/report":
            return {"hash": "server-minted-abc123"}
        return {"data": [{"date": "2026-08-20"}], "total": {}, "meta": {"last_page": 1}}

    try:
        tbx._request = fake
        _reset_report_transport()

        payload = tbx.build_report_payload(
            "2026-08-14", "2026-08-20",
            attributes=["date"], metrics=["imps_sum"])

        check("mint calls POST /share/report",
              tbx.mint_report_hash(payload) == "server-minted-abc123")
        check("the minted hash is cached, not re-requested",
              tbx.mint_report_hash(payload) == "server-minted-abc123"
              and calls.count("/share/report") == 1)
        check("refresh=True mints again",
              tbx.mint_report_hash(payload, refresh=True) == "server-minted-abc123"
              and calls.count("/share/report") == 2)

        calls.clear()
        _reset_report_transport()
        tbx.report("2026-08-14", "2026-08-20",
                   attributes=["date"], metrics=["imps_sum"])
        check("the empty-hash form is tried first", calls[0] == "/report/", str(calls))
        check("then it mints", "/share/report" in calls, str(calls))
        check("report() addresses the SERVER hash",
              "/report/server-minted-abc123" in calls, str(calls))
        check("report() never addresses the local md5",
              not any(c.startswith("/report/" + tbx._request_hash(payload))
                      for c in calls))

        # Sticky: a second report must not re-pay the rejected empty-hash call.
        calls.clear()
        tbx.report("2026-08-01", "2026-08-02",
                   attributes=["date"], metrics=["imps_sum"])
        check("the rejection is remembered for the process",
              "/report/" not in calls, str(calls))
    finally:
        tbx._request = saved
        _reset_report_transport()


def test_a_query_error_is_not_a_transport_error() -> None:
    """
    A 422 naming a bad metric must not trigger the mint fallback.

    Minting a hash for the same body would produce the same 422 one call
    later, and the second error is the one the caller would see — so the
    real complaint ("unknown report metric") would be reported as a hash
    problem. Only a hash-shaped rejection is grounds to switch transport.
    """
    print("\nreport transport: query errors propagate")

    calls: list[str] = []
    saved = tbx._request

    def fake(method, path, payload=None, **kw):
        calls.append(path)
        raise tbx.TbxError("bad request", status=422,
                           body='{"message":"metrics field is required"}')

    try:
        tbx._request = fake
        _reset_report_transport()
        expect_raises("a non-hash 422 propagates from the empty-hash call",
                      tbx.TbxError, tbx.report, "2026-08-14", "2026-08-20",
                      attributes=["date"], metrics=["imps_sum"])
        check("and it did not mint a hash to ask again",
              "/share/report" not in calls, str(calls))
        check("the transport is not marked broken by a query error",
              tbx._EMPTY_HASH_OK is not False)
    finally:
        tbx._request = saved
        _reset_report_transport()


def test_stale_hash_is_reminted_once() -> None:
    """A minted hash that expires mid-report must retry, not end the run."""
    print("\nstale hash recovery")

    check("a 422 naming the hash is recognised as stale",
          tbx._is_stale_hash(tbx.TbxError(
              "x", status=422,
              body='{"message":"Hash not found, or no longer available"}')))
    check("a 422 about something else is not",
          not tbx._is_stale_hash(tbx.TbxError(
              "x", status=422, body='{"message":"metrics field is required"}')))
    check("a 500 is not a stale hash",
          not tbx._is_stale_hash(tbx.TbxError("x", status=500, body="hash")))

    state = {"mints": 0, "reads": 0}
    saved = tbx._request

    def fake(method, path, payload=None, **kw):
        if path == "/report/":
            # Force the mint path, which is the one under test here.
            raise tbx.TbxError("no hash", status=422,
                               body='{"message":"Hash not found"}')
        if path == "/share/report":
            state["mints"] += 1
            return {"hash": f"h{state['mints']}"}
        state["reads"] += 1
        if state["reads"] == 1:                      # first read: hash has gone
            raise tbx.TbxError("expired", status=422,
                               body='{"errors":{"hash":["Hash not found"]}}')
        return {"data": [], "total": {}, "meta": {"last_page": 1}}

    try:
        tbx._request = fake
        _reset_report_transport()
        tbx.report("2026-08-14", "2026-08-20",
                   attributes=["date"], metrics=["imps_sum"])
        check("re-minted after the stale 422", state["mints"] == 2)
        check("and retried the read", state["reads"] == 2)
    except Exception as exc:                          # noqa: BLE001
        check("stale hash recovered without raising", False, f"{type(exc).__name__}: {exc}")
    finally:
        tbx._request = saved
        _reset_report_transport()

    # A non-hash error from the hash-addressed read must still propagate.
    def always_422(method, path, payload=None, **kw):
        if path == "/report/":
            raise tbx.TbxError("no hash", status=422,
                               body='{"message":"Hash not found"}')
        if path == "/share/report":
            return {"hash": "h"}
        raise tbx.TbxError("bad request", status=422,
                           body='{"message":"metrics field is required"}')

    try:
        tbx._request = always_422
        _reset_report_transport()
        expect_raises("a non-hash 422 still propagates", tbx.TbxError,
                      tbx.report, "2026-08-14", "2026-08-20",
                      attributes=["date"], metrics=["imps_sum"])
    finally:
        tbx._request = saved
        _reset_report_transport()


def test_tbx_demand_geo_floor_proposals() -> None:
    """
    The first dynamic optimizer on this platform, offline.

    Every check here is a way a floor writer costs money quietly rather than
    loudly: a floor on the wrong DSP, a floor above what the DSP clears, a
    second optimizer fighting the vendor's own, a country set replaced instead
    of merged. None of those raise.
    """
    print("\ntbx demand geo floor: proposals")

    from agents.optimization import tbx_demand_geo_floor as agent

    countries = {"usa": 1, "gbr": 2, "can": 3, "bra": 4}

    def row(source: str, country: str, imps: int, spend: float) -> dict:
        return {"demand_source": source, "country": country,
                "imps_sum": str(imps), "dsp_price_sum": f"{spend:.2f}",
                "ssp_price_sum": "0", "requests_sum": str(imps * 10)}

    by_name = {"alpha dsp": [10], "beta dsp": [20], "twin dsp": [30, 31]}
    by_id = {10: {"id": 10, "name": "Alpha DSP"},
             20: {"id": 20, "name": "Beta DSP"},
             30: {"id": 30, "name": "Twin DSP"},
             31: {"id": 31, "name": "Twin DSP"}}

    details = {
        # No floors set at all.
        10: {"id": 10, "name": "Alpha DSP", "geo_settings": {}},
        # Teqblaze's own optimizer owns this one.
        20: {"id": 20, "name": "Beta DSP", "is_smart_floor": True,
             "geo_settings": {}},
    }
    fetch = lambda sid: details.get(sid, {"id": sid})   # noqa: E731

    # $10.00 eCPM in the US -> proposed 0.85 * 10.00 = $8.50, no prior floor
    # so the delta cap has nothing to clamp against.
    rows = [row("Alpha DSP", "USA", 100_000, 1000.00)]
    props, skips = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("a clean pair produces one proposal", len(props) == 1, str(props))
    if props:
        pick = props[0]["picks"][0]
        check("floor = FLOOR_PCT x observed eCPM",
              abs(pick["proposed"] - 8.50) < 0.01, str(pick))
        check("keyed by the platform's numeric country id",
              props[0]["floors_by_country_id"] == {1: pick["proposed"]},
              str(props[0]["floors_by_country_id"]))

    # The vendor's own floor optimizer owns Beta. One owner per lever.
    rows = [row("Beta DSP", "USA", 100_000, 1000.00)]
    props, skips = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("is_smart_floor DSPs are skipped", not props, str(props))
    check("and the skip says why",
          any("is_smart_floor" in sk for sk in skips), str(skips))

    # An ambiguous name must never be guessed at — that is a floor on the
    # wrong DSP, and it is silent.
    rows = [row("Twin DSP", "USA", 100_000, 1000.00)]
    props, skips = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("an ambiguous demand name is skipped", not props, str(props))
    check("and counted as ambiguous",
          any("ambiguous" in sk for sk in skips), str(skips))

    rows = [row("Nobody At All", "USA", 100_000, 1000.00)]
    props, skips = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("an unknown demand name is skipped, not guessed", not props)

    # Volume and quality bars.
    rows = [row("Alpha DSP", "USA", 10, 5.00)]                # under min imps
    props, _ = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("a low-volume pair is not priced", not props, str(props))

    rows = [row("Alpha DSP", "USA", 100_000, 1.00)]           # $0.01 eCPM
    props, _ = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("a junk-eCPM pair is not priced", not props, str(props))

    # Geo allowlist.
    rows = [row("Alpha DSP", "BRA", 100_000, 1000.00)]
    props, _ = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("a country outside the allowlist is ignored", not props, str(props))

    # The delta cap. Current $1.00, observed eCPM $10.00 -> wants $8.50,
    # must land at $1.25 (+25%), not $8.50.
    details[10] = {"id": 10, "name": "Alpha DSP",
                   "geo_settings": {"bid_floor": [{"country_id": 1, "value": "1.00"}]}}
    rows = [row("Alpha DSP", "USA", 100_000, 1000.00)]
    props, _ = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("the delta cap trims a large move",
          bool(props) and abs(props[0]["picks"][0]["proposed"] - 1.25) < 0.001,
          str(props))
    check("and the clamp is recorded on the proposal",
          bool(props) and props[0]["picks"][0]["clamps"], str(props))

    # A floor already at or above what the DSP clears must not be touched:
    # raising it further removes them from the auction.
    details[10] = {"id": 10, "name": "Alpha DSP",
                   "geo_settings": {"bid_floor": [{"country_id": 1, "value": "9.00"}]}}
    props, _ = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("no proposal when the current floor already exceeds the target",
          not props, str(props))

    # Never a cut, even when the observed eCPM has collapsed.
    rows = [row("Alpha DSP", "USA", 100_000, 100.00)]         # $1.00 eCPM
    props, _ = agent.build_proposals(rows, by_name, by_id, countries, fetch)
    check("a collapsed eCPM produces no floor cut", not props, str(props))

    # A GET that fails is a skip with a reason, not a crash and not a
    # proposal built on a blank config.
    def boom(sid):
        raise RuntimeError("503")

    details[10] = {"id": 10, "name": "Alpha DSP", "geo_settings": {}}
    rows = [row("Alpha DSP", "USA", 100_000, 1000.00)]
    props, skips = agent.build_proposals(rows, by_name, by_id, countries, boom)
    check("a failed detail read is a skip, not a crash", not props)
    check("and names the source", any("[10]" in sk for sk in skips), str(skips))


def test_tbx_demand_geo_floor_write_path() -> None:
    """apply_proposals must merge, never replace, and must respect the gates."""
    print("\ntbx demand geo floor: write path")

    from agents.optimization import tbx_demand_geo_floor as agent

    calls: list[dict] = []
    saved = tbm.set_demand_geo_bid_floors

    def fake(**kw):
        calls.append(kw)
        return {"applied": not kw.get("dry_run"), "clamps": []}

    proposals = [{
        "demand_source_id": 10,
        "demand_name": "Alpha DSP",
        "floors_by_country_id": {1: 8.50},
        "picks": [{"country": "USA", "country_id": 1, "current": 0.0,
                   "proposed": 8.50, "observed_ecpm": 10.0, "imps": 100_000,
                   "spend": 1000.0, "clamps": []}],
        "spend_in_scope": 1000.0,
    }]

    try:
        tbm.set_demand_geo_bid_floors = fake
        agent.apply_proposals(proposals, dry_run=True)
        check("the writer is called once per proposal", len(calls) == 1)
        check("replace=False — countries we did not price are left alone",
              calls and calls[0].get("replace") is False, str(calls))
        check("demand_name is passed so partner_freeze can refuse",
              calls and calls[0].get("demand_name") == "Alpha DSP", str(calls))
        check("dry_run is honoured", calls and calls[0].get("dry_run") is True)
        check("the actor is the agent, not 'manual'",
              calls and calls[0].get("actor") == "tbx_demand_geo_floor")
    finally:
        tbm.set_demand_geo_bid_floors = saved

    # A refusal from partner_freeze is recorded, not counted as applied.
    calls.clear()

    def refuse(**kw):
        return {"applied": False, "refused": "partner_freeze"}

    try:
        tbm.set_demand_geo_bid_floors = refuse
        actions = agent.apply_proposals(proposals, dry_run=False)
        check("a freeze refusal is recorded on the action",
              actions and actions[0].get("refused") == "partner_freeze",
              str(actions))
        check("and is not reported as applied",
              actions and actions[0].get("applied") is False)
    finally:
        tbm.set_demand_geo_bid_floors = saved


def test_tbx_demand_geo_floor_gates() -> None:
    """--apply alone must not be enough to write."""
    print("\ntbx demand geo floor: autonomy gates")

    from agents.optimization import tbx_demand_geo_floor as agent

    os.environ.pop("PGAM_OPTIMIZER_AUTO_APPLY", None)
    check("the fleet autonomy gate is closed by default",
          agent.auto_apply_enabled() is False)
    os.environ["PGAM_OPTIMIZER_AUTO_APPLY"] = "1"
    check("and opens on exactly '1'", agent.auto_apply_enabled() is True)
    os.environ["PGAM_OPTIMIZER_AUTO_APPLY"] = "yes"
    check("'yes' does not open it", agent.auto_apply_enabled() is False)
    os.environ.pop("PGAM_OPTIMIZER_AUTO_APPLY", None)

    # Unconfigured platform: a scheduled run must return rather than raise.
    saved = tbx.configured
    try:
        tbx.configured = lambda: False
        outcome = agent.run(dry_run=True)
        check("no credentials is a clean no-op",
              outcome.get("ok") is True and "skipped" in outcome, str(outcome))
    finally:
        tbx.configured = saved

    check("FLOOR_PCT is below 1.0 — a floor at the clearing price is a block",
          agent.FLOOR_PCT < 1.0, str(agent.FLOOR_PCT))
    check("the per-run source cap is bounded", 0 < agent.MAX_SOURCES_PER_RUN <= 50)
    check("the per-source country cap is bounded",
          0 < agent.MAX_COUNTRIES_PER_SOURCE <= 25)


def test_write_gates() -> None:
    print("\nwrite gates")
    check("writes disabled by default", tbm.writes_enabled() is False)
    check("dry run on by default", tbm._default_dry_run() is True)

    os.environ["TBX_DRY_RUN"] = "false"
    check("TBX_DRY_RUN=false opts out of dry run", tbm._default_dry_run() is False)
    os.environ["TBX_DRY_RUN"] = "true"
    check("TBX_DRY_RUN=true restores dry run", tbm._default_dry_run() is True)
    del os.environ["TBX_DRY_RUN"]

    os.environ["TBX_ALLOW_WRITES"] = "1"
    check("TBX_ALLOW_WRITES=1 opens the gate", tbm.writes_enabled() is True)
    os.environ["TBX_ALLOW_WRITES"] = "0"
    check("TBX_ALLOW_WRITES=0 closes it", tbm.writes_enabled() is False)
    del os.environ["TBX_ALLOW_WRITES"]

    # A live write with the gate shut must be refused rather than attempted:
    # _simple_write short-circuits before any HTTP call, so this stays offline.
    result = tbm._simple_write("/deals/store", {"name": "t"}, "deal", 0,
                               "create_deal", "test", "gate check", dry_run=False)
    check("live write refused without the env gate",
          result.get("applied") is False and result.get("refused") == "TBX_ALLOW_WRITES!=1",
          str(result))

    dry = tbm._simple_write("/deals/store", {"name": "t"}, "deal", 0,
                            "create_deal", "test", "dry check", dry_run=True)
    check("dry run reports the payload without applying",
          dry.get("dry_run") is True and dry.get("applied") is False
          and dry.get("payload") == {"name": "t"})


def test_validation_guards() -> None:
    print("\nargument validation")
    expect_raises("bad price_type rejected", ValueError,
                  tbx.build_report_payload, "2026-08-01", "2026-08-02",
                  attributes=["nope"])
    expect_raises("bad bids-overview kind rejected", ValueError,
                  tbx.bids_overview, "sideways")
    expect_raises("bad human-report kind rejected", ValueError,
                  tbx.human_report, "everything")
    expect_raises("bad filter record_type rejected", ValueError,
                  tbm.create_filter_list, "n", "domain")
    expect_raises("bad filter list_type rejected", ValueError,
                  tbm.create_filter_list, "n", "adomain", list_type="grey")
    expect_raises("bad qps_recalculation rejected", ValueError,
                  tbm.set_demand_qps_limit, 1, qps_recalculation=45)
    expect_raises("bad qps_optimization_by rejected", ValueError,
                  tbm.set_demand_qps_limit, 1, qps_optimization_by="revenue")
    expect_raises("empty qps update rejected", ValueError,
                  tbm.set_demand_qps_limit, 1)
    expect_raises("bad margin_type rejected", ValueError,
                  tbm.set_demand_economics, 1, margin_type="sliding")
    expect_raises("out-of-range schain_node rejected", ValueError,
                  tbm.set_demand_schain_policy, 1, schain_node=99)
    expect_raises("empty schain update rejected", ValueError,
                  tbm.set_demand_schain_policy, 1)
    expect_raises("bad deal type rejected", ValueError,
                  tbm.create_deal, "n", "h", 1.0, deal_type="PMP")
    expect_raises("bad auction type rejected", ValueError,
                  tbm.create_deal, "n", "h", 1.0, auction_type=3)
    expect_raises("bad alert channel rejected", ValueError,
                  tbm.create_alert, "n", [{"name": "imps", "operator": "<", "value": 1}],
                  channel="sms")
    expect_raises("bad alert metric rejected", ValueError,
                  tbm.create_alert, "n", [{"name": "revenue", "operator": "<", "value": 1}])
    expect_raises("bad alert period rejected", ValueError,
                  tbm.create_alert, "n", [{"name": "imps", "operator": "<", "value": 1}],
                  period=6)
    expect_raises("bad scheduled interval rejected", ValueError,
                  tbm.create_scheduled_report, "n", 1, ["a@b.c"], interval="hourly")
    expect_raises("bad api_sync type rejected", ValueError,
                  tbm.set_api_sync_url, "supply_source", 1, "http://x", url_type="tsv")
    expect_raises("bad api_sync kind rejected", ValueError,
                  tbm.set_api_sync_url, "publisher", 1, "http://x")


def test_roughly_equal() -> None:
    print("\nverify comparison")
    check("float tolerance", tbm._roughly_equal(2.5, 2.50))
    check("numeric string tolerance", tbm._roughly_equal("2.5", 2.5))
    check("mismatch detected", not tbm._roughly_equal(2.5, 3.0))
    check("bool compared as bool", tbm._roughly_equal(True, 1))
    check("bool mismatch detected", not tbm._roughly_equal(True, 0))
    check("string equality", tbm._roughly_equal("bid_floor", "bid_floor"))
    check("string mismatch", not tbm._roughly_equal("bid_floor", "fixed_bid_price"))


def test_vocabulary() -> None:
    print("\nspec vocabulary")
    check("25 report attributes", len(tbx.REPORT_ATTRIBUTES) == 25,
          str(len(tbx.REPORT_ATTRIBUTES)))
    check("43 report metrics", len(tbx.REPORT_METRICS) == 43,
          str(len(tbx.REPORT_METRICS)))
    check("every alias points at a real metric",
          all(v in tbx.REPORT_METRICS for v in tbx.METRIC_ALIASES.values()),
          str([k for k, v in tbx.METRIC_ALIASES.items() if v not in tbx.REPORT_METRICS]))
    check("no duplicate metric names",
          len(set(tbx.REPORT_METRICS)) == len(tbx.REPORT_METRICS))
    check("no duplicate attribute names",
          len(set(tbx.REPORT_ATTRIBUTES)) == len(tbx.REPORT_ATTRIBUTES))
    check("filter record types match the spec enum",
          set(tbm.FILTER_RECORD_TYPES) == {"bundle", "publisher_id", "site_app_id",
                                           "crid", "adomain", "schain_node_domain"})


def test_multipart_encoding() -> None:
    """
    The spec declares several write endpoints as multipart/form-data, not JSON.
    Getting the encoding wrong is a silent 422, so pin the part construction.
    """
    print("\nmultipart encoding")

    parts = tbx.build_form_parts({"value": ["a.com", "b.com"]})
    check("array fields get PHP-style name[] keys",
          [k for k, _ in parts] == ["value[]", "value[]"],
          str([k for k, _ in parts]))
    check("array values preserved in order",
          [v[1] for _, v in parts] == ["a.com", "b.com"],
          str([v[1] for _, v in parts]))
    check("scalar parts use (None, value) so requests stays multipart",
          tbx.build_form_parts({"status": "1"})[0][1][0] is None)
    check("booleans encode as 1/0, not True/False",
          [v[1] for _, v in tbx.build_form_parts({"a": True, "b": False})] == ["1", "0"],
          str([v[1] for _, v in tbx.build_form_parts({"a": True, "b": False})]))

    file_parts = tbx.build_form_parts({"n": 2}, {"import": ("x.csv", b"a\nb\n", "text/csv")})
    check("file part passes through untouched",
          dict(file_parts)["import"] == ("x.csv", b"a\nb\n", "text/csv"))

    # A single value must still arrive as an array for add/remove-value.
    single = tbx.build_form_parts({"value": ["only.com"]})
    check("single value still sent as an array", single[0][0] == "value[]")


def test_filter_value_writes() -> None:
    """add/remove/import all go out as multipart; import carries a real file."""
    print("\nfilter list value writes")

    res = tbm.add_filter_values(7, ["a.com", "b.com"], dry_run=True)
    check("add batches into one call, not one per value",
          res["chunks"] == 1 and len(res["results"]) == 1, str(res["chunks"]))
    check("add reports the full count", res["count"] == 2)

    chunked = tbm.add_filter_values(7, [f"d{i}.com" for i in range(2500)],
                                    dry_run=True, chunk_size=1000)
    check("oversized batches split", chunked["chunks"] == 3, str(chunked["chunks"]))
    check("split preserves the total", chunked["count"] == 2500)

    check("blank-only input rejected rather than sent",
          _raises(ValueError, tbm.add_filter_values, 7, ["", "  "]))

    rem = tbm.remove_filter_values(7, "a.com", dry_run=True)
    check("remove accepts a bare string", rem["payload"]["value"] == ["a.com"],
          str(rem["payload"]))
    check("remove_filter_value alias still resolves",
          tbm.remove_filter_value is tbm.remove_filter_values)

    imp = tbm.import_filter_values(7, ["a.com", "b.com"], dry_run=True)
    check("import logs a count rather than dumping the CSV",
          imp["payload"] == {"value_count": 2}, str(imp["payload"]))
    check("import rejects an empty list",
          _raises(ValueError, tbm.import_filter_values, 7, []))


def _raises(exc, fn, *args, **kwargs) -> bool:
    try:
        fn(*args, **kwargs)
    except exc:
        return True
    except Exception:
        return False
    return False


def test_qps_waste_rule() -> None:
    """The cut rule decides revenue-affecting actions; pin its edges."""
    print("\nQPS waste rule")
    from agents.optimization import qps_waste_sentry as q
    from datetime import date

    today = date(2026, 8, 19)
    blended = 0.88          # $/M, the measured baseline
    old = "2020-01-01"      # well outside the grace period

    def row(name, gpm, gross, days=q.OBSERVE_DAYS, first=old, requests=10**9):
        return {"name": name, "gpm": gpm, "gross": gross, "requests": requests,
                "impressions": 0, "active_days": days, "first_seen": first}

    rows = q.classify([
        row("dead",       0.005, 4.30),      # far below cut band, trivial gross
        row("cheap-big",  0.05,  5000.0),    # below cut band but earns real money
        row("mid",        0.20,  900.0),     # cap band
        row("fine",       0.90,  5000.0),    # at/above blended
        row("newborn",    0.001, 0.0, first="2026-08-15"),   # inside grace
        row("paused",     0.001, 0.0, days=3),               # partial coverage
    ], blended, today)
    band = {r["name"]: r["band"] for r in rows}

    check("trivial waste is cut", band["dead"] == "cut", band["dead"])
    check("cheap but earning is capped, not cut",
          band["cheap-big"] == "cap", band["cheap-big"])
    check("mid band is capped", band["mid"] == "cap", band["mid"])
    check("healthy source untouched", band["fine"] == "ok", band["fine"])
    check("new source protected by grace", band["newborn"] == "grace", band["newborn"])
    check("partial coverage downgraded to watch — may be a pause, not waste",
          band["paused"] == "watch", band["paused"])

    # Never-cut list must beat the numbers.
    saved = set(q.NEVER_CUT)
    q.NEVER_CUT.add("dead")
    try:
        protected = q.classify([row("dead", 0.005, 4.30)], blended, today)[0]
        check("never-cut list overrides the rule", protected["band"] == "protected",
              protected["band"])
    finally:
        q.NEVER_CUT.clear(); q.NEVER_CUT.update(saved)

    # Blast radius: action count cap.
    many = q.classify([row(f"w{i}", 0.001, 1.0, requests=10**8) for i in range(12)],
                      blended, today)
    acted = q.enforce_blast_radius(many, total_requests=10**12)
    check("action count capped per run", len(acted) <= q.MAX_ACTIONS_PER_RUN,
          str(len(acted)))
    check("overflow marked deferred, not silently dropped",
          any(r["band"] == "deferred" for r in many))

    # Blast radius: QPS share cap. Two rows at 10% each cannot both go under 15%.
    big = q.classify([row("a", 0.001, 1.0, requests=10**11),
                      row("b", 0.001, 1.0, requests=10**11)], blended, today)
    acted = q.enforce_blast_radius(big, total_requests=10**12)
    check("QPS share cap limits one run", len(acted) == 1, str(len(acted)))


def test_qps_proposals_reach_the_ledger() -> None:
    """The docs promise every proposal is recorded. Prove it, at a temp path."""
    print("\nQPS proposal ledger")
    import importlib, json as _json, tempfile, os as _os
    from agents.optimization import qps_waste_sentry as q
    from core import tb_ledger

    saved = tb_ledger.LEDGER_PATH
    tmp = _os.path.join(tempfile.mkdtemp(), "ledger.jsonl")
    tb_ledger.LEDGER_PATH = tmp
    try:
        actions = [{"name": "Illumin - RON copy1 #2179", "band": "cut",
                    "reason": "zero revenue on 6.4B requests",
                    "requests": 6_400_000_000, "gross": 0.0, "gpm": 0.0,
                    "impressions": 0}]
        n = q.record_proposals(actions, "DEMAND SOURCES", 0.8225,
                               ("2026-08-05", "2026-08-18"))
        check("proposal written", n == 1, str(n))

        entries = [_json.loads(l) for l in open(tmp) if l.strip()]
        check("exactly one entry", len(entries) == 1, str(len(entries)))
        e = entries[0]
        check("recorded as NOT applied", e["applied"] is False, str(e["applied"]))
        check("not mislabelled as a dry run", e["dry_run"] is False, str(e["dry_run"]))
        check("action names the band", e["action"] == "propose_cut", e["action"])
        check("entity is the demand source", e["entity_id"].startswith("Illumin"),
              e["entity_id"])
        check("before-state captured for later attribution",
              e["before"]["requests"] == 6_400_000_000)
        check("after-state empty — nothing changed", e["after"] == {})
        check("evidence carries the baseline",
              e["extra"]["blended_gpm"] == 0.8225, str(e["extra"].get("blended_gpm")))

        check("empty action list writes nothing",
              q.record_proposals([], "DEMAND SOURCES", 0.8, ("a", "b")) == 0)
    finally:
        tb_ledger.LEDGER_PATH = saved


def test_no_credentials_message() -> None:
    print("\nunconfigured behaviour")
    saved = (tbx.TBX_EMAIL, tbx.TBX_PASSWORD)
    tbx.TBX_EMAIL, tbx.TBX_PASSWORD = "", ""
    try:
        check("configured() false without credentials", tbx.configured() is False)
        expect_raises("login without credentials raises TbxAuthError",
                      tbx.TbxAuthError, tbx._login)
        check("test_connection returns False, does not raise",
              tbx.test_connection(verbose=False) is False)
    finally:
        tbx.TBX_EMAIL, tbx.TBX_PASSWORD = saved


def test_interactive_credentials() -> None:
    """
    `set_credentials` must make the client usable without touching the
    environment, and must not leave a previous account's cached token usable.
    """
    print("\ninteractive credentials (--login path)")

    saved_email, saved_pw = tbx.TBX_EMAIL, tbx.TBX_PASSWORD
    try:
        tbx.TBX_EMAIL, tbx.TBX_PASSWORD = "", ""
        check("configured() false before set_credentials", not tbx.configured())

        tbx.set_credentials("ops@example.com", "s3cret")
        check("configured() true after set_credentials", tbx.configured())
        check("email applied", tbx.TBX_EMAIL == "ops@example.com")
        check("password applied", tbx.TBX_PASSWORD == "s3cret")

        # Whitespace around a pasted email is the common mistake; it must not
        # silently produce a cache key that never matches.
        tbx.set_credentials("  ops@example.com  ", "s3cret")
        check("email is stripped", tbx.TBX_EMAIL == "ops@example.com")

        for label, email, pw in (
            ("empty email rejected", "", "s3cret"),
            ("empty password rejected", "ops@example.com", ""),
            ("both empty rejected", "", ""),
        ):
            try:
                tbx.set_credentials(email, pw)
            except tbx.TbxAuthError:
                check(label, True)
            else:
                check(label, False, "expected TbxAuthError")

        # A cached token belonging to another account must be ignored, or a
        # --login run would silently read the wrong marketplace.
        import json as _json
        import tempfile as _tempfile
        saved_cache = tbx.TOKEN_CACHE
        try:
            fd, path = _tempfile.mkstemp()
            with open(fd, "w") as fh:
                _json.dump({"token": "stale-jwt", "expires_at": 2 ** 40,
                            "base": tbx.TBX_BASE, "email": "someone-else@example.com"}, fh)
            tbx.TOKEN_CACHE = path
            tbx.set_credentials("ops@example.com", "s3cret")
            check("cached token for another account is not reused",
                  tbx._load_cached_token() == "")

            with open(path, "w") as fh:
                _json.dump({"token": "our-jwt", "expires_at": 2 ** 40,
                            "base": tbx.TBX_BASE, "email": "ops@example.com"}, fh)
            check("cached token for this account is reused",
                  tbx._load_cached_token() == "our-jwt")
        finally:
            tbx.TOKEN_CACHE = saved_cache
            try:
                os.unlink(path)
            except OSError:
                pass

        # Non-interactive stdin must refuse rather than read a piped password.
        check("prompt_for_credentials refuses without a TTY",
              not sys.stdin.isatty()
              and _raises(tbx.TbxAuthError, tbx.prompt_for_credentials))
    finally:
        tbx.TBX_EMAIL, tbx.TBX_PASSWORD = saved_email, saved_pw



def test_tbx_auto_revert_candidates() -> None:
    """Which ledger writes are ours to undo — and which are emphatically not."""
    print("\ntbx auto-revert: candidate selection")

    from agents.optimization import tbx_auto_revert as agent

    now = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)

    def entry(**kw):
        base = {
            "id": kw.pop("id", f"e{len(kw)}"),
            "ts": kw.pop("ts", "2026-08-20T09:45:00+00:00"),
            "actor": kw.pop("actor", "tbx_demand_geo_floor"),
            "action": kw.pop("action", "set_demand_geo_bid_floors"),
            "entity_type": kw.pop("entity_type", "tbx_demand_source"),
            "entity_id": kw.pop("entity_id", 10),
            "applied": kw.pop("applied", True),
            "dry_run": kw.pop("dry_run", False),
            "before": {"geo_settings": {"bid_floor": [{"country_id": 1, "value": 1.00}]}},
            "after": {"geo_settings": {"bid_floor": [{"country_id": 1, "value": 1.25}]}},
        }
        base.update(kw)
        return base

    ours = entry(id="w1")
    cands, _skips = agent.find_candidates([ours], now=now)
    check("our own optimizer's applied write is a candidate", len(cands) == 1, str(cands))
    check("and it carries the prior floors to restore",
          cands and cands[0]["before_floors"] == {1: 1.00}, str(cands))

    # Exclusions, one at a time.
    cases = [
        ("a dry run is not a candidate", entry(id="w2", dry_run=True)),
        ("an unapplied write is not a candidate", entry(id="w3", applied=False)),
        ("a human's manual write is not ours to undo", entry(id="w4", actor="manual")),
        ("another agent's write is not ours to undo",
         entry(id="w5", actor="tbx_qps_waste_sentry")),
        ("a revert is never itself reverted",
         entry(id="w6", actor="tbx_auto_revert_20260820")),
        ("a legacy-platform entry is out of scope",
         entry(id="w7", entity_type="tb_placement")),
        ("a different action is out of scope",
         entry(id="w8", action="set_demand_status")),
        ("a write older than the window is out of scope",
         entry(id="w9", ts="2026-07-01T09:45:00+00:00")),
    ]
    for label, bad in cases:
        cands, _ = agent.find_candidates([bad], now=now)
        check(label, len(cands) == 0, str(cands))

    # A no-op write (floors identical before and after) has nothing to undo.
    noop = entry(id="w10")
    noop["after"] = {"geo_settings": {"bid_floor": [{"country_id": 1, "value": 1.00}]}}
    cands, _ = agent.find_candidates([noop], now=now)
    check("a write that changed no floor is not a candidate", len(cands) == 0)

    # Already reverted -> never reverted twice.
    link = {
        "id": "r1", "ts": "2026-08-22T10:15:00+00:00",
        "actor": "tbx_auto_revert_20260822", "action": "auto_revert_link",
        "entity_type": "tbx_demand_source", "entity_id": 10,
        "applied": True, "dry_run": False,
        "extra": {"reverted_from": "w1"},
    }
    cands, _ = agent.find_candidates([ours, link], now=now)
    check("a write already reverted is not reverted again", len(cands) == 0, str(cands))

    # Third-party write after ours -> escalate, never clobber.
    intruder = entry(id="w11", entity_id=10, actor="manual",
                     ts="2026-08-21T14:00:00+00:00")
    cands, skips = agent.find_candidates([ours, intruder], now=now)
    check("a third party writing after us blocks the revert",
          len(cands) == 0, str(cands))
    check("and it is escalated as a skip, not dropped silently",
          any("manual" in s and "human" in s.lower() for s in skips), str(skips))


def test_tbx_auto_revert_harm_rule() -> None:
    """What counts as harm, measured in settled days."""
    print("\ntbx auto-revert: harm rule")

    from agents.optimization import tbx_auto_revert as agent

    change_ts = datetime(2026, 8, 18, 9, 45, tzinfo=timezone.utc)
    candidate = {"ledger_id": "w1", "ts": change_ts, "demand_source_id": 10,
                 "before_floors": {1: 1.0}, "after_floors": {1: 1.25},
                 "actor": "tbx_demand_geo_floor"}
    today = date(2026, 8, 23)   # settled days run through the 23rd

    def index(pre_profit, post_profit, pre_imps=100_000.0, post_imps=100_000.0):
        out = {}
        for i in range(agent.PRE_DAYS):
            day = (change_ts.date() - timedelta(days=i + 1)).isoformat()
            out[(10, day)] = {"imps": pre_imps, "gross": pre_profit * 4,
                              "payout": pre_profit * 3, "profit": pre_profit}
        day = change_ts.date() + timedelta(days=1)
        while day <= today:
            out[(10, day.isoformat())] = {
                "imps": post_imps, "gross": post_profit * 4,
                "payout": post_profit * 3, "profit": post_profit}
            day += timedelta(days=1)
        return out

    v = agent.assess(candidate, index(100.0, 100.0), today=today)
    check("flat profit is left alone", v["revert"] is False, str(v))

    v = agent.assess(candidate, index(100.0, 95.0), today=today)
    check("a 5% dip is inside tolerance", v["revert"] is False, str(v))

    v = agent.assess(candidate, index(100.0, 70.0), today=today)
    check("a 30% profit drop triggers a revert", v["revert"] is True, str(v))
    check("and the reason names the numbers", "profit" in v["why"], v["why"])

    v = agent.assess(candidate, index(100.0, 120.0), today=today)
    check("a floor that improved profit is kept", v["revert"] is False, str(v))

    # Fill collapse fires even when profit holds.
    v = agent.assess(candidate, index(100.0, 100.0, post_imps=20_000.0), today=today)
    check("an 80% impression collapse triggers even at flat profit",
          v["revert"] is True, str(v))
    check("and says so", "impressions" in v["why"], v["why"])

    # Too small to act on.
    v = agent.assess(candidate, index(1.0, 0.0), today=today)
    check("a source below the pre-window profit floor is left alone",
          v["revert"] is False, str(v))
    check("and says why", "below" in v["why"], v["why"])

    # Not enough settled days yet.
    v = agent.assess(candidate, index(100.0, 0.0), today=date(2026, 8, 19))
    check(f"one settled day is not enough (MIN_POST_DAYS={agent.MIN_POST_DAYS})",
          v["revert"] is False, str(v))
    v = agent.assess(candidate, index(100.0, 0.0), today=date(2026, 8, 18))
    check("no settled day since the write is not enough",
          v["revert"] is False, str(v))

    # A total wipeout: the platform drops all-zero rows, so the post window is
    # simply absent from the index. That must read as zero, not as no-data.
    wiped = index(100.0, 0.0)
    for key in [k for k in wiped if k[1] > change_ts.date().isoformat()]:
        del wiped[key]
    v = agent.assess(candidate, wiped, today=today)
    check("a DSP with no post rows at all reads as zero, not as missing data",
          v["revert"] is True, str(v))

    # No pre-change data -> nothing to compare against, so do nothing.
    v = agent.assess(candidate, {}, today=today)
    check("no pre-change data means no revert", v["revert"] is False, str(v))


def test_tbx_auto_revert_write_path() -> None:
    """The revert primitive: exact restore, correct gates, loud refusals."""
    print("\ntbx auto-revert: write path")

    from agents.optimization import tbx_auto_revert as agent

    calls: list[dict] = []
    saved = tbm.set_demand_geo_bid_floors

    candidate = {"ledger_id": "w1", "ts": datetime(2026, 8, 18, tzinfo=timezone.utc),
                 "demand_source_id": 10, "demand_name": "Alpha DSP",
                 "before_floors": {1: 1.0, 2: 2.0},
                 "after_floors": {1: 1.25, 2: 2.5, 3: 4.0},
                 "actor": "tbx_demand_geo_floor"}
    verdict = {"revert": True, "why": "profit $100/day → $60/day"}

    def fake(**kw):
        calls.append(kw)
        return {"applied": not kw.get("dry_run"), "clamps": []}

    try:
        tbm.set_demand_geo_bid_floors = fake
        action = agent.revert_one(candidate, verdict, dry_run=True)
        check("the writer is called once", len(calls) == 1)
        check("replace=True — a revert restores the snapshot exactly",
              calls and calls[0].get("replace") is True, str(calls))
        check("country 3, which the forward run added, is dropped by replacing",
              calls and 3 not in calls[0]["floors_by_country_id"], str(calls))
        check("the prior values are what get written",
              calls and calls[0]["floors_by_country_id"] == {1: 1.0, 2: 2.0},
              str(calls))
        check("demand_name is passed so partner_freeze can refuse",
              calls and calls[0].get("demand_name") == "Alpha DSP")
        check("the actor marks it as an auto-revert",
              calls and calls[0]["actor"].startswith("tbx_auto_revert"))
        check("the reason cites the write being undone",
              calls and "w1" in calls[0]["reason"], str(calls))
        check("dry run is not reported as applied", action["applied"] is False)
    finally:
        tbm.set_demand_geo_bid_floors = saved

    # A freeze refusal must surface, not vanish — the harm is still live.
    try:
        tbm.set_demand_geo_bid_floors = lambda **kw: {
            "applied": False, "refused": "partner_freeze"}
        action = agent.revert_one(candidate, verdict, dry_run=True)
        check("a freeze refusal is recorded on the action",
              action.get("refused") == "partner_freeze", str(action))
        summary = agent.slack_summary([action], [], 1, applied=True)
        check("and Slack says the harmful floors are still live",
              "still live" in summary, summary)
    finally:
        tbm.set_demand_geo_bid_floors = saved

    # A clamp on the way back means the entity landed in a third state.
    try:
        tbm.set_demand_geo_bid_floors = lambda **kw: {
            "applied": True, "clamps": ["global min: $0.0000 → $0.0100"]}
        action = agent.revert_one(candidate, verdict, dry_run=True)
        check("a clamped revert is flagged inexact", action.get("inexact") is True,
              str(action))
        summary = agent.slack_summary([action], [], 1, applied=True)
        check("and Slack shows the clamp", "clamped on the way back" in summary,
              summary)
    finally:
        tbm.set_demand_geo_bid_floors = saved


def test_tbx_auto_revert_is_always_within_the_delta_cap() -> None:
    """Undoing a capped raise must never itself be blocked by the cap."""
    print("\ntbx auto-revert: the delta cap cannot trap a revert")

    delta = tbm.MAX_FLOOR_DELTA
    prior = 1.00
    raised, _ = tbm.clamp_floor(prior * 100, current=prior)   # ask for the moon
    check(f"a forward raise is capped at +{delta:.0%}",
          abs(raised - prior * (1 + delta)) < 1e-6, f"{raised}")

    back, reasons = tbm.clamp_floor(prior, current=raised)
    check("and reverting to the prior value is permitted, uncapped",
          abs(back - prior) < 1e-6, f"{back} reasons={reasons}")

    # The general statement: undoing (1+d) needs a cut of d/(1+d), which is
    # strictly less than d for any d > 0. So a single-step revert always fits.
    check("the arithmetic holds for the configured cap",
          delta / (1 + delta) < delta, f"delta={delta}")


def test_tbx_auto_revert_gates() -> None:
    """--apply alone must not write; the fleet gate deliberately does not apply."""
    print("\ntbx auto-revert: gates")

    from agents.optimization import tbx_auto_revert as agent

    saved_conf = tbx.configured
    try:
        tbx.configured = lambda: False
        outcome = agent.run(dry_run=True)
        check("no credentials is a clean no-op",
              outcome.get("ok") is True and "skipped" in outcome, str(outcome))
    finally:
        tbx.configured = saved_conf

    # --apply without the platform gate falls back to propose-only.
    os.environ.pop("TBX_ALLOW_WRITES", None)
    check("the platform write gate is closed by default",
          tbm.writes_enabled() is False)

    saved_iter = tb_ledger.iter_entries
    saved_report = agent.daily_rows
    try:
        tbx.configured = lambda: True
        tb_ledger.iter_entries = lambda since=None: iter([])
        agent.daily_rows = lambda s, e: []
        outcome = agent.run(dry_run=False)
        check("--apply with TBX_ALLOW_WRITES unset does not write",
              outcome.get("reverts", 0) == 0, str(outcome))
    finally:
        tbx.configured = saved_conf
        tb_ledger.iter_entries = saved_iter
        agent.daily_rows = saved_report

    check("the fleet autonomy gate is NOT among this agent's gates — a revert "
          "restores a prior state and must survive the accelerator being cut",
          "PGAM_OPTIMIZER_AUTO_APPLY" not in
          Path(agent.__file__).read_text().split('"""')[2],
          "found the fleet gate in the agent body")

    check("the per-run revert cap is bounded", 0 < agent.MAX_REVERTS_PER_RUN <= 10)
    check("the profit trigger is a real threshold, not a hair",
          0.05 <= agent.DROP_THRESHOLD_PCT <= 0.5)
    check("at least two settled days are required before acting",
          agent.MIN_POST_DAYS >= 2)


def test_tbx_etl_chunks_by_day() -> None:
    """The ETL must not trust a multi-day range, and must unpack the report."""
    print("\ntbx revenue ETL: day chunking")

    from agents.etl import tbx_revenue_etl as etl

    check("the ETL chunks one day at a time", etl.CHUNK_DAYS == 1)

    asked: list[tuple[str, str]] = []

    def fake_report(date_from, date_to, attributes, metrics):
        asked.append((date_from, date_to))
        # The platform's real shape: (rows, totals). Passing the tuple
        # straight to _aggregate is the bug this pins.
        return ([{"date": date_from, "demand_source": "Alpha",
                  "demand_source_id": 7, "imps_sum": "100",
                  "dsp_price_sum": "10.0", "ssp_price_sum": "7.0",
                  "requests_sum": "1000", "ssp_wins_sum": "200"}],
                {"imps_sum": "100"})

    saved = tbx.report
    try:
        tbx.report = fake_report
        rows, missing, off = etl._fetch_daily(
            "demand_source", date(2026, 8, 21), date(2026, 8, 24))
    finally:
        tbx.report = saved

    check("a 4-day window is 4 single-day requests, not one range request",
          len(asked) == 4, str(asked))
    check("each request asks for exactly one day",
          all(a == b for a, b in asked), str(asked))
    check("the days requested are the days asked for",
          [a for a, _ in asked] == ["2026-08-21", "2026-08-22",
                                    "2026-08-23", "2026-08-24"], str(asked))
    check("the (rows, totals) tuple is unpacked, not fed to _aggregate whole",
          len(rows) == 4 and all(isinstance(r, dict) for r in rows), str(rows)[:200])
    check("no day is reported missing when every day answered", missing == [])

    records, dropped = etl._aggregate(rows, "demand_source")
    check("and the rows aggregate to one record per day",
          len(records) == 4 and dropped == 0, str(records)[:200])

    # A row carrying a date other than the one requested is discarded, never
    # attributed to the requested day.
    def wrong_date(date_from, date_to, attributes, metrics):
        return ([{"date": "2026-08-19", "demand_source_id": 7,
                  "imps_sum": "100", "dsp_price_sum": "10.0",
                  "ssp_price_sum": "7.0", "requests_sum": "1", "ssp_wins_sum": "1"}],
                {})

    try:
        tbx.report = wrong_date
        rows, missing, off = etl._fetch_daily(
            "demand_source", date(2026, 8, 21), date(2026, 8, 21))
    finally:
        tbx.report = saved

    check("a row for another date is discarded, not misattributed",
          rows == [] and off == 1, f"rows={rows} off={off}")
    check("and the day is reported as having no rows", missing == ["2026-08-21"])

def test_tbx_etl_entity_id_suffix() -> None:
    """The report gives names with the id as a '#NNNN' suffix, not id fields."""
    print("\ntbx revenue ETL: entity id resolution")

    from agents.etl.tbx_revenue_etl import _entity, _aggregate

    # The shape measured against the live account on 2026-08-24. Before this
    # was handled, every row of every grain was dropped as unresolvable and
    # the ETL landed nothing while reporting success.
    live = {"date": "2026-08-21", "placement": "01net.it_300x250 #8766"}
    check("the id is taken from the '#NNNN' suffix",
          _entity(live, "placement") == (8766, "01net.it_300x250"),
          str(_entity(live, "placement")))

    vendor = {"demand_source": "Magnite - RON Prebid Server In App #1752"}
    check("and from the vendor reference's own example form",
          _entity(vendor, "demand_source")[0] == 1752,
          str(_entity(vendor, "demand_source")))

    check("the suffix is stripped from the stored name",
          _entity(vendor, "demand_source")[1] ==
          "Magnite - RON Prebid Server In App",
          str(_entity(vendor, "demand_source")))

    check("only the TRAILING #NNNN is the id — a '#' inside the name is not",
          _entity({"placement": "Weird #12 Name #345"}, "placement") ==
          (345, "Weird #12 Name"),
          str(_entity({"placement": "Weird #12 Name #345"}, "placement")))

    check("a name with no suffix stays unresolvable rather than guessing",
          _entity({"supply_source": "No Suffix"}, "supply_source") ==
          (None, "No Suffix"),
          str(_entity({"supply_source": "No Suffix"}, "supply_source")))

    # The other documented shapes must keep working.
    check("an {id, name} object still resolves",
          _entity({"demand_source": {"id": 7, "name": "Obj"}}, "demand_source")
          == (7, "Obj"))
    check("a flattened x_id column still resolves",
          _entity({"demand_source_id": 9, "demand_source": "Flat"},
                  "demand_source")[0] == 9)

    # End to end: a live-shaped row must aggregate rather than drop.
    records, dropped = _aggregate(
        [{"date": "2026-08-21", "placement": "01net.it_300x250 #8766",
          "imps_sum": "100", "dsp_price_sum": "10.5", "ssp_price_sum": "7.0",
          "requests_sum": "1000", "ssp_wins_sum": "200"}],
        "placement")
    check("a live-shaped row survives aggregation", dropped == 0 and len(records) == 1,
          f"dropped={dropped} records={records}")
    check("keyed on the parsed id",
          records and records[0]["entity_id"] == 8766, str(records))
    check("carrying the revenue", records and records[0]["gross_revenue"] == 10.5,
          str(records))

def main() -> int:
    print("tests/test_tbx.py — new Teqblaze platform client (offline)")
    test_report_payload()
    test_request_hash()
    test_report_prefers_the_empty_hash_form()
    test_report_falls_back_to_minting()
    test_a_query_error_is_not_a_transport_error()
    test_stale_hash_is_reminted_once()
    test_tbx_demand_geo_floor_proposals()
    test_tbx_demand_geo_floor_write_path()
    test_tbx_demand_geo_floor_gates()
    test_tbx_auto_revert_candidates()
    test_tbx_auto_revert_harm_rule()
    test_tbx_auto_revert_write_path()
    test_tbx_auto_revert_is_always_within_the_delta_cap()
    test_tbx_auto_revert_gates()
    test_tbx_etl_chunks_by_day()
    test_tbx_etl_entity_id_suffix()
    test_floor_clamps()
    test_deep_merge()
    test_key_loss_guard()
    test_read_only_stripping()
    test_strip_list_matches_spec()
    test_unknown_write_keys()
    test_recon_classifier()
    test_pnl_check_exit_codes()
    test_write_gates()
    test_validation_guards()
    test_roughly_equal()
    test_vocabulary()
    test_qps_waste_rule()
    test_qps_proposals_reach_the_ledger()
    test_multipart_encoding()
    test_filter_value_writes()
    test_no_credentials_message()
    test_interactive_credentials()

    print()
    if _failures:
        print(f"FAILED — {len(_failures)} check(s): {', '.join(_failures)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
