"""
tests/test_msn_insights.py

Standalone, no-network/no-DB sanity tests for the MSN insights stack.

Validates the pure-data layer of the puller + resolver against the real
DevTools fixtures Priyesh captured from Partner Hub on 2026-05-16:
  - realtime endpoint response (top 20 articles, recordCount=123)
  - daily-aggregate endpoint response (30 days of contentType=4 video)

Run:
    python tests/test_msn_insights.py

Exits non-zero on any failure. No pytest dependency to keep the repo
deploy-light.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make the repo root importable when run from anywhere.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from agents.enrichment.msn_doc_resolver import (
    _first_image_url,
)
from agents.etl.msn_insights_etl import _normalise_daily_row
from core.msn_partner_hub import _iso_z


# ---------------------------------------------------------------------------
# Fixtures — verbatim from the user's DevTools captures (2026-05-16).
# Note: trimmed where indicated to keep the file readable; coverage of the
# row-shape is identical with a smaller sample.
# ---------------------------------------------------------------------------

REALTIME_FIXTURE_JSON = """
{
    "recordList": [
        { "title": "Khamzat Chimaev's Next Move Causes Worry",
          "titleStatus": 1, "docID": "AA23fK6B", "readCount": 235 },
        { "title": "Ngannou Says Jones' Presence Adds Intrigue To His Fight",
          "titleStatus": 1, "docID": "AA23k9kT", "readCount": 165 },
        { "title": "Nate Diaz Vs. Mike Perry Predictions - 'By Submission'",
          "titleStatus": 1, "docID": "AA23fUHN", "readCount": 148 },
        { "title": "Boxing & UFC this weekend (15 May–17 May 2026)",
          "titleStatus": 1, "docID": "AA23fsFY", "readCount": 144 },
        { "title": "Benavidez Told To Challenge Usyk",
          "titleStatus": 1, "docID": "AA23kjPw", "readCount": 129 },
        { "title": "Francis Ngannou Reveals All On His UFC Departure",
          "titleStatus": 1, "docID": "AA23khOk", "readCount": 127 }
    ],
    "recordCount": 123
}
"""

# Daily aggregate fixture — first 3 records of the 30-day video stream.
DAILY_FIXTURE_JSON = """
{
    "recordList": [
        { "date": "2026-05-15", "contentType": 4, "impressionCount": 0,
          "readCount": 16, "saveCount": 0, "favouriteCount": 5,
          "forwardCount": 0, "uniqueUserCount": 10,
          "videoUniqueUserCount": 1947, "videoStartCount": 2408,
          "videoViewed25Count": 389, "videoViewed50Count": 261,
          "videoViewed75Count": 177, "videoViewed100Count": 142,
          "monetizableView": 803, "consumedSeconds": 29647,
          "dislikeCount": 2, "commentsCount": 0, "ctrClickCount": 0 },
        { "date": "2026-05-14", "contentType": 4, "impressionCount": 0,
          "readCount": 15, "saveCount": 0, "favouriteCount": 4,
          "forwardCount": 0, "uniqueUserCount": 12,
          "videoUniqueUserCount": 1929, "videoStartCount": 2403,
          "videoViewed25Count": 429, "videoViewed50Count": 263,
          "videoViewed75Count": 207, "videoViewed100Count": 165,
          "monetizableView": 858, "consumedSeconds": 32566,
          "dislikeCount": 2, "commentsCount": 1, "ctrClickCount": 0 },
        { "date": "2026-05-13", "contentType": 4, "readCount": 13,
          "videoStartCount": 1010 }
    ],
    "recordCount": 31
}
"""

# A representative MSN content-API JSON payload (verbatim shape from
# https://assets.msn.com/content/view/v2/Detail/en-us/{docID} captured
# 2026-05-31 for docID AA24kuRO). The resolver now hits this endpoint
# directly instead of scraping the SPA HTML page — sourceHref is the
# 1:1 boxingnews.com canonical we need.
MSN_DETAIL_FIXTURE = {
    "title": "Cain Velasquez reveals his one condition to fight again",
    "abstract": "Cain Velasquez has not closed the door on fighting again...",
    "sourceHref": "https://boxingnews.com/news/cain-velasquez-would-fight-again-on-one-condition/",
    "imageResources": [
        {
            "width": 834,
            "height": 722,
            "url": "https://img-s-msn-com.akamaized.net/tenant/amp/entityid/AA24kuRA.img",
            "title": "Cain Velasquez Reveals His One Condition To Fight Again",
        }
    ],
}

MSN_DETAIL_FIXTURE_MULTI_IMAGE = {
    "title": "Some Article",
    "sourceHref": "https://boxingnews.com/news/some-article/",
    "imageResources": [
        {"width": 200, "height": 200, "url": "https://img-s-msn-com/small.jpg"},
        {"width": 1200, "height": 800, "url": "https://img-s-msn-com/big.jpg"},
    ],
}

# 15-min traffic buckets fixture — captured 2026-05-16 from the
# Partner Hub Overview tab. Same /realtime path but called without
# $orderBy and with date=-1 → response is bucketed totals, not articles.
BUCKETS_FIXTURE_JSON = """
{
    "recordList": [
        { "date": "2026-05-16T23:30Z", "readCount": 33 },
        { "date": "2026-05-16T23:15Z", "readCount": 19 },
        { "date": "2026-05-16T23:00Z", "readCount": 26 },
        { "date": "2026-05-16T22:45Z", "readCount": 25 },
        { "date": "2026-05-16T22:30Z", "readCount": 19 }
    ],
    "recordCount": 95
}
"""

MSN_DETAIL_FIXTURE_NO_SOURCE = {
    "title": "Some Article",
    "imageResources": [
        {"width": 1, "height": 1, "url": "https://img-s-msn-com/img.jpg"}
    ],
}


# ---------------------------------------------------------------------------
# Tiny test runner. Each test is a function that raises on failure.
# ---------------------------------------------------------------------------

_FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  ✓ {name}")
    else:
        print(f"  ✗ {name}  {detail}")
        _FAILURES.append(f"{name}: {detail}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_iso_z_format() -> None:
    print("[test] _iso_z formats UTC datetimes as MSN expects")
    dt = datetime(2026, 5, 16, 20, 35, 0, tzinfo=timezone.utc)
    out = _iso_z(dt)
    check("formats UTC datetime", out == "2026-05-16T20:35Z", f"got {out!r}")

    # Naive datetimes should be treated as UTC.
    naive = datetime(2026, 5, 16, 20, 35, 0)
    out2 = _iso_z(naive)
    check("treats naive as UTC", out2 == "2026-05-16T20:35Z", f"got {out2!r}")


def test_normalise_daily_row() -> None:
    print("[test] _normalise_daily_row maps MSN aggregate row to schema cols")
    fixture = json.loads(DAILY_FIXTURE_JSON)
    rows = fixture["recordList"]

    row0 = _normalise_daily_row("AA1lKiff", rows[0])
    check("row0 returned", row0 is not None)
    assert row0 is not None  # for type narrowing
    check("partner_id propagated", row0["partner_id"] == "AA1lKiff")
    check("date propagated",       row0["report_date"] == "2026-05-15")
    check("content_type 4 = video", row0["content_type"] == 4)
    check("readCount mapped",      row0["read_count"] == 16)
    check("videoStartCount mapped", row0["video_start_count"] == 2408)
    check("monetizableView mapped", row0["monetizable_view"] == 803)
    check("consumed_seconds mapped", row0["consumed_seconds"] == 29647)

    # Sparse rows: missing fields default to 0, not None.
    row2 = _normalise_daily_row("AA1lKiff", rows[2])
    assert row2 is not None
    check("sparse: missing fields default to 0", row2["save_count"] == 0)
    check("sparse: present field kept",          row2["video_start_count"] == 1010)

    # Date-less rows are skipped.
    skipped = _normalise_daily_row("AA1lKiff", {"contentType": 4, "readCount": 5})
    check("rows without date are dropped", skipped is None)


def test_realtime_shape() -> None:
    print("[test] realtime fixture has the shape the puller expects")
    fixture = json.loads(REALTIME_FIXTURE_JSON)
    records = fixture["recordList"]
    record_count = fixture["recordCount"]
    check("recordList is a list", isinstance(records, list))
    check("recordCount is an int", isinstance(record_count, int))
    check("recordCount > len(records) for paginating fixture",
          record_count >= len(records))

    # The four fields the puller reads. We assert on the first record
    # only because the fixture is uniform — every record has the same shape.
    required = ("title", "titleStatus", "docID", "readCount")
    first = records[0]
    for f in required:
        check(f"record has {f}", f in first, f"missing in {first}")


def test_revenue_estimate() -> None:
    print("[test] $4 CPM math is what the dashboard will show")
    fixture = json.loads(REALTIME_FIXTURE_JSON)
    total_reads = sum(r["readCount"] for r in fixture["recordList"])
    # 6 records in the trimmed fixture: 235+165+148+144+129+127 = 948
    est = total_reads * 0.004
    check("sample est revenue", round(est, 4) == round(948 * 0.004, 4),
          f"total_reads={total_reads}")


def test_detail_fixture_has_sourceHref() -> None:
    print("[test] MSN content-API payload exposes sourceHref pointing at boxingnews")
    src = MSN_DETAIL_FIXTURE.get("sourceHref")
    check("sourceHref present", isinstance(src, str) and bool(src), f"got {src!r}")
    assert isinstance(src, str)
    check("sourceHref is boxingnews.com", "boxingnews.com" in src, f"got {src!r}")
    # Sanity: payload also carries the human title we'll cache as canonical_title
    title = MSN_DETAIL_FIXTURE.get("title")
    check("title present", isinstance(title, str) and bool(title))


def test_first_image_url_picks_largest() -> None:
    print("[test] _first_image_url picks the largest imageResources entry")
    url = _first_image_url(MSN_DETAIL_FIXTURE_MULTI_IMAGE)
    check("returns the 1200x800 image", url == "https://img-s-msn-com/big.jpg",
          f"got {url!r}")
    # Single-image payload still works
    one = _first_image_url(MSN_DETAIL_FIXTURE)
    check("single-image payload",
          one == "https://img-s-msn-com.akamaized.net/tenant/amp/entityid/AA24kuRA.img",
          f"got {one!r}")


def test_first_image_url_handles_missing() -> None:
    print("[test] _first_image_url returns None when imageResources is empty/absent")
    check("empty list -> None", _first_image_url({"imageResources": []}) is None)
    check("missing key -> None", _first_image_url({}) is None)


def test_bucket_fixture_shape() -> None:
    print("[test] buckets fixture has the shape the puller expects")
    fixture = json.loads(BUCKETS_FIXTURE_JSON)
    records = fixture["recordList"]
    check("recordList is a list", isinstance(records, list))
    check("recordCount is an int", isinstance(fixture["recordCount"], int))
    # Each bucket has `date` (ISO timestamp) and `readCount` only.
    first = records[0]
    check("bucket has date",      "date" in first)
    check("bucket has readCount", "readCount" in first)
    check("date is ISO-Z format", str(first["date"]).endswith("Z"))
    check("readCount is int",     isinstance(first["readCount"], int))

    # The sum is what we use to estimate 24h revenue. Sanity-check that
    # the math is intuitive.
    total = sum(int(r["readCount"]) for r in records)
    expected = 33 + 19 + 26 + 25 + 19
    check("sum is sane",  total == expected, f"got {total}")
    est = round(total * 0.004, 2)
    check("est revenue from 5 buckets ~= 5 × 0.004 × avg ~24 PV",
          est == round(expected * 0.004, 2),
          f"got {est}")


def main() -> int:
    print("\n========== MSN insights — fixture tests ==========\n")
    tests = [
        test_iso_z_format,
        test_normalise_daily_row,
        test_realtime_shape,
        test_revenue_estimate,
        test_detail_fixture_has_sourceHref,
        test_first_image_url_picks_largest,
        test_first_image_url_handles_missing,
        test_bucket_fixture_shape,
    ]
    for t in tests:
        t()
        print()
    print("=" * 52)
    if _FAILURES:
        print(f"FAILED — {len(_FAILURES)} assertion(s):")
        for f in _FAILURES:
            print(f"  - {f}")
        return 1
    print("ALL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
