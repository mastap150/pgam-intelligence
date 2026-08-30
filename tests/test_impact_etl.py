"""Checks for the impact.com affiliate leg — offline, no database, no API.

Covers the pure layer: field-name tolerance, date handling, the dedupe rule
between the two passes, and the probe's mapping audit. Everything here is the
part that decides whether a vendor row becomes a correct ledger row, which is
the part written without a live account and therefore the part worth pinning.

The DB-side behaviour (UPSERT, reversal correction through the views) is not
testable without Postgres and is exercised by the end-to-end run recorded in
docs/impact-affiliate-etl.md §"What has actually been tested".

    python3 tests/test_impact_etl.py
"""
from __future__ import annotations

import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from agents.etl import impact_revenue_etl as etl   # noqa: E402
from core import impact_api as imp                 # noqa: E402
import impact_probe as probe                       # noqa: E402

CHECKS = []


def check(name):
    def deco(fn):
        CHECKS.append((name, fn))
        return fn
    return deco


def action(**over) -> dict:
    """A minimal well-formed vendor action, overridable per check."""
    row = {
        "Id": "A1",
        "CampaignId": "901",
        "CampaignName": "Garmin US",
        "EventDate": "2026-08-20T14:03:00Z",
        "ModificationDate": "2026-08-21T09:00:00Z",
        "State": "APPROVED",
        "Payout": "12.50",
        "SaleAmount": "250.00",
        "Currency": "USD",
        "PayoutCurrency": "USD",
        "SubId1": "healthnation.com",
    }
    row.update(over)
    return row


# ---------------------------------------------------------------------------
# Scalar parsing
# ---------------------------------------------------------------------------

@check("payout strings parse, including currency symbols and thousands commas")
def _():
    assert etl._f("12.50") == 12.5
    assert etl._f("$1,234.56") == 1234.56
    assert etl._f(7) == 7.0


@check("unparseable and empty amounts are 0.0, not an exception")
def _():
    for junk in (None, "", "-", "n/a", {}):
        assert etl._f(junk) == 0.0


@check("a missing id is None, never 0 — 0 would group unrelated rows together")
def _():
    assert etl._i(None) is None
    assert etl._i("") is None
    assert etl._i("not-a-number") is None
    assert etl._i("901") == 901


@check("event date is the vendor's own date portion, with no timezone shift")
def _():
    # 23:30 in a negative-offset zone is still the 20th to impact.com, and
    # shifting it to UTC would move the revenue to the 21st and put every
    # per-day total permanently out of step with the vendor's dashboard.
    assert etl._day("2026-08-20T23:30:00-04:00") == date(2026, 8, 20)
    assert etl._day("2026-08-20") == date(2026, 8, 20)
    assert etl._day("") is None
    assert etl._day("not a date") is None


# ---------------------------------------------------------------------------
# normalize
# ---------------------------------------------------------------------------

@check("a well-formed action becomes a ledger record")
def _():
    rec = etl.normalize(action())
    assert rec["action_id"] == "A1"
    assert rec["campaign_id"] == 901
    assert rec["event_date"] == date(2026, 8, 20)
    assert rec["payout"] == 12.5
    assert rec["sale_amount"] == 250.0
    assert rec["sub_id1"] == "healthnation.com"


@check("a row with no action id is dropped — an UPSERT on a null key collapses rows")
def _():
    row = action()
    del row["Id"]
    assert etl.normalize(row) is None


@check("a row with no usable event date is dropped")
def _():
    assert etl.normalize(action(EventDate="")) is None
    assert etl.normalize(action(EventDate="garbage")) is None


@check("state is uppercased so the views' filters cannot miss a row")
def _():
    assert etl.normalize(action(State="Reversed"))["state"] == "REVERSED"
    assert etl.normalize(action(State="pending"))["state"] == "PENDING"


@check("alternate vendor spellings resolve through ACTION_FIELDS")
def _():
    row = {
        "ActionId": "B2",            # not "Id"
        "ActionDate": "2026-08-19",  # not "EventDate"
        "Commission": "4.25",        # not "Payout"
        "Status": "LOCKED",          # not "State"
        "ProgramId": "902",
    }
    rec = etl.normalize(row)
    assert rec is not None
    assert rec["action_id"] == "B2"
    assert rec["event_date"] == date(2026, 8, 19)
    assert rec["payout"] == 4.25
    assert rec["state"] == "LOCKED"
    assert rec["campaign_id"] == 902


@check("the whole vendor payload is kept in raw, so a bad mapping is a SQL fix")
def _():
    rec = etl.normalize(action(SomeCustomField="keep me"))
    assert "SomeCustomField" in rec["raw"]
    assert "keep me" in rec["raw"]


@check("an unknown logical field name raises rather than silently defaulting")
def _():
    try:
        imp.action_field(action(), "payuot")
    except KeyError:
        return
    raise AssertionError("a typo'd field name must not return the default")


# ---------------------------------------------------------------------------
# dedupe — the two passes overlap by design
# ---------------------------------------------------------------------------

@check("the same action from both passes collapses to one record")
def _():
    recs = [etl.normalize(action()), etl.normalize(action())]
    assert len(etl.dedupe(recs)) == 1


@check("the newest modification wins — a reversal must not lose to a stale copy")
def _():
    stale = etl.normalize(action(State="APPROVED",
                                 ModificationDate="2026-08-21T09:00:00Z"))
    fresh = etl.normalize(action(State="REVERSED", Payout="0.00",
                                 ModificationDate="2026-08-26T11:00:00Z"))
    # Order must not decide the outcome, so assert it both ways round.
    for pair in ((stale, fresh), (fresh, stale)):
        [winner] = etl.dedupe(list(pair))
        assert winner["state"] == "REVERSED", pair


@check("a record with no modification stamp never displaces one that has it")
def _():
    dated = etl.normalize(action(State="REVERSED",
                                 ModificationDate="2026-08-26T11:00:00Z"))
    undated = etl.normalize(action(State="APPROVED", ModificationDate=""))
    for pair in ((dated, undated), (undated, dated)):
        [winner] = etl.dedupe(list(pair))
        assert winner["state"] == "REVERSED", pair


@check("distinct actions are not collapsed")
def _():
    recs = [etl.normalize(action(Id="A1")), etl.normalize(action(Id="A2"))]
    assert len(etl.dedupe(recs)) == 2


# ---------------------------------------------------------------------------
# summarize
# ---------------------------------------------------------------------------

@check("summarize splits payout by state and by currency")
def _():
    recs = [
        etl.normalize(action(Id="A1", State="LOCKED", Payout="100")),
        etl.normalize(action(Id="A2", State="PENDING", Payout="10")),
        etl.normalize(action(Id="A3", State="LOCKED", Payout="5",
                             PayoutCurrency="GBP")),
    ]
    s = etl.summarize(recs)
    assert s["actions"] == 3
    assert s["by_state"]["LOCKED"] == 105.0
    assert s["by_state"]["PENDING"] == 10.0
    assert s["by_currency"] == {"USD": 110.0, "GBP": 5.0}


@check("a state the vendor did not send is reported as UNKNOWN, not dropped")
def _():
    rec = etl.normalize(action(State=""))
    s = etl.summarize([rec])
    assert s["by_state"] == {"UNKNOWN": 12.5}


@check("summarize on nothing does not divide by zero or raise")
def _():
    s = etl.summarize([])
    assert s["actions"] == 0 and s["earliest"] is None


# ---------------------------------------------------------------------------
# probe — the audit that stands in for a live account
# ---------------------------------------------------------------------------

@check("the probe reports all critical fields resolved on a good row")
def _():
    report = probe.check_field_mapping([action()])
    assert report["_missing_critical"] == []
    assert report["action_id"]["via"] == {"Id": 1}


@check("the probe names the critical field that would silently drop every row")
def _():
    row = action()
    del row["Payout"]
    del row["Id"]
    report = probe.check_field_mapping([row])
    assert set(report["_missing_critical"]) == {"action_id", "payout"}


@check("the probe surfaces vendor keys the mapping does not know about")
def _():
    report = probe.check_field_mapping([action(MysteryColumn="x")])
    assert "MysteryColumn" in report["_unmapped"]


@check("the probe flags a state the views' filters would not count")
def _():
    out = probe.check_states([action(State="TRIAL"), action(State="LOCKED")])
    assert out["unexpected"] == ["TRIAL"]


@check("the probe measures how much revenue is attributable to a property")
def _():
    rows = [action(Id="A1"), action(Id="A2", SubId1=""), action(Id="A3", SubId1="")]
    out = probe.check_subid_coverage(rows)
    assert out["with_sub_id1"] == 1 and out["pct"] == 33.3


# ---------------------------------------------------------------------------
# Report discovery — what the clicks/impressions grain is waiting on
# ---------------------------------------------------------------------------

@check("perf-report detection matches on id, name or description")
def _():
    assert imp.looks_like_perf_report({"Id": "mp_performance_by_day"})
    assert imp.looks_like_perf_report({"Id": "x", "Name": "Clicks by Campaign"})
    assert imp.looks_like_perf_report({"Id": "x", "Description": "daily traffic"})


@check("perf-report detection does not flag unrelated reports")
def _():
    for entry in ({"Id": "mp_payment_history", "Name": "Payment History"},
                  {"Id": "mp_geo", "Name": "Geo Breakdown"},
                  {"Id": "mp_tax_forms", "Name": "Tax Forms"}):
        assert not imp.looks_like_perf_report(entry), entry


class _FakeReports:
    """Stands in for imp.run_report: only one spelling is honoured."""

    def __init__(self, honoured=("START_DATE", "END_DATE"), rows=None):
        self.honoured, self.rows, self.calls = honoured, rows, []

    def __call__(self, report_id, params=None):
        params = params or {}
        self.calls.append((report_id, dict(params)))
        if all(k in params for k in self.honoured):
            return {"Records": self.rows if self.rows is not None else
                    [{"Date": "2026-08-25", "Clicks": "1420",
                      "Impressions": "98000"}]}
        raise imp.ImpactError("missing required parameter", status=400)


def _with_run_report(fake, fn):
    """Swap imp.run_report for the duration of fn, then put it back."""
    original = imp.run_report
    imp.run_report = fake
    try:
        return fn()
    finally:
        imp.run_report = original


@check("try_report reports which date-parameter spelling the account accepted")
def _():
    fake = _FakeReports()
    findings = _with_run_report(
        fake, lambda: imp.try_report("r1", "2026-08-01", "2026-08-26"))
    accepted = [f for f in findings if f["ok"]]
    assert len(accepted) == 1, [f["label"] for f in accepted]
    assert accepted[0]["label"] == "START_DATE/END_DATE"
    assert accepted[0]["row_keys"] == ["Clicks", "Date", "Impressions"]
    # every candidate is tried, so a rejection is a finding rather than a stop
    assert len(findings) == len(imp.REPORT_DATE_PARAM_CANDIDATES)


@check("try_report substitutes the real dates into the candidate templates")
def _():
    fake = _FakeReports()
    _with_run_report(fake, lambda: imp.try_report("r1", "2026-08-01", "2026-08-26"))
    sent = dict(fake.calls[0][1])
    assert sent == {"START_DATE": "2026-08-01", "END_DATE": "2026-08-26"}, sent


@check("try_report never raises on a report the account will not run")
def _():
    def refuse(report_id, params=None):
        raise imp.ImpactError("forbidden", status=403)
    findings = _with_run_report(
        refuse, lambda: imp.try_report("r1", "2026-08-01", "2026-08-26"))
    assert all(not f["ok"] for f in findings)
    assert all(f["status"] == 403 for f in findings)


@check("the handoff block carries only reports that returned usable rows")
def _():
    import argparse

    catalog = [{"Id": "good_performance_by_day", "Name": "Performance by Day"},
               {"Id": "empty_click_report", "Name": "Clicks by Campaign"}]
    original_catalog, original_meta = imp.report_catalog, imp.report_metadata
    imp.report_catalog = lambda: catalog
    imp.report_metadata = lambda rid: None

    def fake(report_id, params=None):
        if "START_DATE" not in (params or {}):
            raise imp.ImpactError("bad params", status=400)
        # the second report answers, but with nothing in it
        rows = ([{"Date": "2026-08-25", "Clicks": "5"}]
                if report_id == "good_performance_by_day" else [])
        return {"Records": rows}

    try:
        out = _with_run_report(fake, lambda: probe.audit_reports(
            argparse.Namespace(days=7, report_shape=None)))
    finally:
        imp.report_catalog, imp.report_metadata = original_catalog, original_meta

    assert out["usable"] == ["good_performance_by_day"], out["usable"]
    assert out["shapes"]["empty_click_report"]["row_keys"] == []


def main() -> int:
    failed = 0
    for name, fn in CHECKS:
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failed += 1
            print(f"  ✗ {name}\n      {e}")
    print(f"\n{len(CHECKS)} checks, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
