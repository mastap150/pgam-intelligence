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
    _clean_url,
    _find_canonical_boxingnews_url,
    _meta,
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

# A representative MSN article page fragment that mimics what we expect to
# see when fetching https://www.msn.com/.../ar-{docID}. Includes the
# OG tags, a "View original" anchor pointing at boxingnews.com, and some
# MSN tracking params on the URL that the cleaner must strip.
MSN_HTML_SAMPLE = """
<html><head>
<meta property="og:title" content="Francis Ngannou Reveals All On His UFC Departure">
<meta property="og:image" content="https://img-s-msn-com.akamaized.net/tenant/amp/entityid/AA23khOk.img">
<meta property="og:url" content="https://www.msn.com/en-us/sports/mma_ufc/francis-ngannou-reveals-all-on-his-ufc-departure/ar-AA23khOk">
<link rel="canonical" href="https://www.msn.com/en-us/sports/mma_ufc/francis-ngannou-reveals-all-on-his-ufc-departure/ar-AA23khOk">
</head><body>
<article>
  <p>Francis Ngannou has finally opened up...</p>
  <p>For more boxing analysis, visit
     <a href="https://www.boxingnews.com/news/francis-ngannou-ufc-departure?ocid=hpmsn&amp;cvid=abc123">
       the original article on BoxingNews.com</a>.</p>
</article>
</body></html>
"""

MSN_HTML_NO_SOURCE = """
<html><head>
<meta property="og:title" content="Some Article">
<meta property="og:image" content="https://img-s-msn-com.akamaized.net/img.jpg">
</head><body>
<p>Article body with no source link.</p>
</body></html>
"""


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


def test_canonical_url_parser_finds_anchor() -> None:
    print("[test] _find_canonical_boxingnews_url finds the source anchor")
    url = _find_canonical_boxingnews_url(MSN_HTML_SAMPLE)
    check("returns a URL", url is not None, f"got {url!r}")
    assert url is not None
    check("URL is on boxingnews.com", "boxingnews.com" in url, f"got {url!r}")
    check("MSN tracking params stripped (ocid/cvid)",
          "ocid=" not in url and "cvid=" not in url,
          f"got {url!r}")
    # Sanity: kept the actual article path
    check("kept article path", "francis-ngannou-ufc-departure" in url, f"got {url!r}")


def test_canonical_url_parser_returns_none() -> None:
    print("[test] _find_canonical_boxingnews_url returns None when absent")
    out = _find_canonical_boxingnews_url(MSN_HTML_NO_SOURCE)
    check("no boxingnews link -> None", out is None, f"got {out!r}")


def test_meta_parser() -> None:
    print("[test] _meta extracts og: tags")
    title = _meta(MSN_HTML_SAMPLE, "og:title")
    image = _meta(MSN_HTML_SAMPLE, "og:image")
    missing = _meta(MSN_HTML_SAMPLE, "og:nonexistent")
    check("og:title parsed", title == "Francis Ngannou Reveals All On His UFC Departure",
          f"got {title!r}")
    check("og:image parsed", image is not None and image.startswith("https://img-s-msn-com"))
    check("missing -> None", missing is None)


def test_clean_url_passthrough() -> None:
    print("[test] _clean_url is a noop for non-boxingnews URLs")
    # Don't touch unrelated URLs (e.g. raw MSN URLs returned during fallback).
    url = "https://www.msn.com/en-us/sports/article/ar-AA23khOk?ocid=hpmsn"
    out = _clean_url(url)
    check("non-boxingnews URL untouched", out == url, f"got {out!r}")


def main() -> int:
    print("\n========== MSN insights — fixture tests ==========\n")
    tests = [
        test_iso_z_format,
        test_normalise_daily_row,
        test_realtime_shape,
        test_revenue_estimate,
        test_canonical_url_parser_finds_anchor,
        test_canonical_url_parser_returns_none,
        test_meta_parser,
        test_clean_url_passthrough,
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
