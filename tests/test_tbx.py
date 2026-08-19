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

    demand = {"id": 91, "name": "d", "operation_systems": [1, 2], "is_schain": True}
    stripped = tbm._strip_read_only(demand, "demand_source")
    check("demand read-only fields dropped",
          set(stripped) == {"name", "is_schain"}, str(sorted(stripped)))


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


def main() -> int:
    print("tests/test_tbx.py — new Teqblaze platform client (offline)")
    test_report_payload()
    test_request_hash()
    test_floor_clamps()
    test_deep_merge()
    test_key_loss_guard()
    test_read_only_stripping()
    test_write_gates()
    test_validation_guards()
    test_roughly_equal()
    test_vocabulary()
    test_qps_waste_rule()
    test_qps_proposals_reach_the_ledger()
    test_multipart_encoding()
    test_filter_value_writes()
    test_no_credentials_message()

    print()
    if _failures:
        print(f"FAILED — {len(_failures)} check(s): {', '.join(_failures)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
