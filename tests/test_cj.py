"""Checks for the CJ affiliate client — offline, no network, no database.

Two things here matter more than the rest.

**Redaction.** CJ's documented 401 for a bad key is "Not Authenticated: xxxxxx"
where xxxxxx is the key you sent. Anything that logs that body verbatim writes
a live credential into Render's log stream. Several checks below assert the
token cannot survive into a message, an exception, or a stored body.

**XML record detection.** CJ answers XML on endpoints that ignore Accept, and
picking the wrong repeated element yields plausible rows of the wrong thing —
commissions where advertisers should be — with no error at all.

    python3 tests/test_cj.py
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import cj_api as cj    # noqa: E402

CHECKS = []


def check(name):
    def deco(fn):
        CHECKS.append((name, fn))
        return fn
    return deco


TOKEN = "pat_live_SUPERSECRET123"


def with_token(fn, token: str = TOKEN):
    """Run fn with a known token installed, then restore."""
    original = cj.CJ_TOKEN
    cj.CJ_TOKEN = token
    try:
        return fn()
    finally:
        cj.CJ_TOKEN = original


# One advertiser carrying THREE actions — the shape that breaks a
# count-based record heuristic, because <actions> has more children than
# <advertisers> does.
ONE_ADVERTISER_XML = """<?xml version="1.0" encoding="UTF-8"?>
<cj-api>
  <advertisers total-matched="1" records-returned="1" page-number="1">
    <advertiser>
      <advertiser-id>1702763</advertiser-id>
      <advertiser-name>Hotels.com</advertiser-name>
      <relationship-status>joined</relationship-status>
      <seven-day-epc>12.34</seven-day-epc>
      <primary-category>
        <parent>Travel</parent>
        <child>Hotels</child>
      </primary-category>
      <actions>
        <action><name>Sale</name><commission>4.00%</commission></action>
        <action><name>Lead</name><commission>2.00%</commission></action>
        <action><name>Click</name><commission>0.10</commission></action>
      </actions>
    </advertiser>
  </advertisers>
</cj-api>
"""

TWO_ADVERTISERS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<cj-api>
  <advertisers total-matched="2" records-returned="2" page-number="1">
    <advertiser>
      <advertiser-id>1702763</advertiser-id>
      <advertiser-name>Hotels.com</advertiser-name>
      <relationship-status>joined</relationship-status>
    </advertiser>
    <advertiser>
      <advertiser-id>4033857</advertiser-id>
      <advertiser-name>Marriott</advertiser-name>
      <relationship-status>notjoined</relationship-status>
    </advertiser>
  </advertisers>
</cj-api>
"""


# ---------------------------------------------------------------------------
# Redaction — the credential must not survive into anything loggable
# ---------------------------------------------------------------------------

@check("the token is stripped from CJ's own 'Not Authenticated' echo")
def _():
    body = f"Not Authenticated: {TOKEN}"
    out = with_token(lambda: cj._redact(body))
    assert TOKEN not in out, out
    assert "REDACTED" in out


@check("the echo is redacted even when the env token has since changed")
def _():
    # A rotated key still sitting in a log is still a disclosed key, so the
    # shape is matched independently of what is currently configured.
    out = with_token(lambda: cj._redact("Not Authenticated: some_other_key"),
                     token="")
    assert "some_other_key" not in out, out


@check("a token pasted into a query string is redacted too")
def _():
    for text in (f"https://x.api.cj.com/v3/lookup?key={TOKEN}&cid=1",
                 f"authorization={TOKEN}",
                 f"...&token={TOKEN}&..."):
        out = with_token(lambda t=text: cj._redact(t))
        assert TOKEN not in out, out


@check("CjError redacts both its message and its stored body")
def _():
    def build():
        return cj.CjError(f"failed with {TOKEN}", status=401,
                          body=f"Not Authenticated: {TOKEN}")
    exc = with_token(build)
    assert TOKEN not in str(exc), str(exc)
    assert TOKEN not in exc.body, exc.body


@check("redaction leaves an unrelated body untouched")
def _():
    body = "<cj-api><error>records-per-page too large</error></cj-api>"
    assert with_token(lambda: cj._redact(body)) == body


# ---------------------------------------------------------------------------
# The three 401s need three different fixes
# ---------------------------------------------------------------------------

@check("an empty 401 body points at the URL, not the credential")
def _():
    kind = cj._auth_error_kind("")
    assert "resource URL" in kind, kind
    assert "token" not in kind.split("—")[0]


@check("'You must specify a developer key.' points at a missing header")
def _():
    assert "no credential was sent" in cj._auth_error_kind(
        "You must specify a developer key.")


@check("'Not Authenticated' points at an invalid credential")
def _():
    assert "rejected" in cj._auth_error_kind(f"Not Authenticated: {TOKEN}")


@check("the auth-error verdict never leaks the echoed key")
def _():
    out = with_token(lambda: cj._auth_error_kind(f"Not Authenticated: {TOKEN}"))
    assert TOKEN not in out, out


# ---------------------------------------------------------------------------
# CJ's non-standard parameter encoding
# ---------------------------------------------------------------------------

@check("a space encodes to + and a literal + encodes to %2B, per CJ's rule")
def _():
    assert cj.encode_cj("credit card") == "credit+card"
    assert cj.encode_cj("5+ star") == "5%2B+star"
    # order matters: encoding the space first would then re-encode its own "+"
    assert cj.encode_cj("a + b") == "a+%2B+b"


@check("encoding leaves an ordinary value alone")
def _():
    assert cj.encode_cj("joined") == "joined"
    assert cj.encode_cj(101849129) == "101849129"


# ---------------------------------------------------------------------------
# XML flattening
# ---------------------------------------------------------------------------

@check("one advertiser with three actions parses as ONE advertiser record")
def _():
    rows = cj._xml_to_records(ONE_ADVERTISER_XML)
    assert len(rows) == 1, [r.get("advertiser-name") for r in rows]
    assert rows[0]["advertiser-name"] == "Hotels.com"


@check("many advertisers each parse as their own record")
def _():
    rows = cj._xml_to_records(TWO_ADVERTISERS_XML)
    assert len(rows) == 2
    assert [cj.advertiser_field(r, "name") for r in rows] == \
        ["Hotels.com", "Marriott"]


@check("a nested category is reachable by its compound key")
def _():
    [row] = cj._xml_to_records(ONE_ADVERTISER_XML)
    assert row["primary-category-parent"] == "Travel"
    assert cj.advertiser_field(row, "category") == "Travel"
    assert cj.advertiser_field(row, "subcategory") == "Hotels"


@check("wrapper attributes survive as fields")
def _():
    rows = cj._xml_to_records(TWO_ADVERTISERS_XML)
    assert all("advertiser-id" in r for r in rows)


@check("unparseable XML raises CjError rather than returning nothing")
def _():
    try:
        cj._xml_to_records("this is not xml at all")
    except cj.CjError:
        return
    raise AssertionError("a junk body must not parse as zero records")


@check("relationship status is read through the tolerance map")
def _():
    rows = cj._xml_to_records(TWO_ADVERTISERS_XML)
    assert cj.advertiser_field(rows[0], "relationship") == "joined"
    assert cj.advertiser_field(rows[1], "relationship") == "notjoined"


@check("an unknown logical field name raises rather than silently defaulting")
def _():
    try:
        cj.advertiser_field({"advertiser-name": "x"}, "advertsier_name")
    except KeyError:
        return
    raise AssertionError("a typo'd field name must not return the default")


@check("without paging attributes, a wrapper is descended, not treated as a record")
def _():
    # <cj-api> holding one <advertisers> is a wrapper. Treating it as the
    # record container yields exactly one row containing every advertiser's
    # fields mashed together — plausible-looking and completely wrong.
    xml = """<cj-api><advertisers>
      <advertiser><advertiser-id>1</advertiser-id><advertiser-name>A</advertiser-name></advertiser>
      <advertiser><advertiser-id>2</advertiser-id><advertiser-name>B</advertiser-name></advertiser>
    </advertisers></cj-api>"""
    rows = cj._xml_to_records(xml)
    assert len(rows) == 2, rows
    assert [r["advertiser-name"] for r in rows] == ["A", "B"]


@check("without paging attributes, a lone record is not mistaken for its children")
def _():
    xml = """<cj-api><advertisers><advertiser>
        <advertiser-id>1</advertiser-id>
        <advertiser-name>Solo</advertiser-name>
        <actions>
          <action><name>Sale</name></action>
          <action><name>Lead</name></action>
        </actions>
    </advertiser></advertisers></cj-api>"""
    rows = cj._xml_to_records(xml)
    assert len(rows) == 1, rows
    assert rows[0]["advertiser-name"] == "Solo"


@check("paging attributes win over structure when both are present")
def _():
    xml = """<cj-api><outer><advertisers records-returned="2">
      <advertiser><advertiser-name>A</advertiser-name></advertiser>
      <advertiser><advertiser-name>B</advertiser-name></advertiser>
    </advertisers></outer></cj-api>"""
    rows = cj._xml_to_records(xml)
    assert len(rows) == 2, rows


@check("a namespaced body parses the same as a bare one")
def _():
    xml = """<ns:cj-api xmlns:ns="http://api.cj.com"><ns:advertisers records-returned="1">
      <ns:advertiser><ns:advertiser-name>Hotels.com</ns:advertiser-name></ns:advertiser>
    </ns:advertisers></ns:cj-api>"""
    [row] = cj._xml_to_records(xml)
    assert cj.advertiser_field(row, "name") == "Hotels.com", row


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@check("missing_env names every absent variable, not just the first")
def _():
    saved = (cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID)
    cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID = "", "", ""
    try:
        assert cj.missing_env() == ["CJ_PERSONAL_ACCESS_TOKEN",
                                    "CJ_COMPANY_ID", "CJ_WEBSITE_ID"]
        assert not cj.configured()
    finally:
        cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID = saved


@check("test_connection reports missing config instead of raising")
def _():
    saved = (cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID)
    cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID = "", "", ""
    try:
        out = cj.test_connection()
        assert out["ok"] is False and "missing" in out["error"]
    finally:
        cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID = saved


@check("a read without credentials raises CjAuthError, not a bare request")
def _():
    saved = (cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID)
    cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID = "", "", ""
    try:
        cj.advertiser_lookup()
    except cj.CjAuthError:
        return
    finally:
        cj.CJ_TOKEN, cj.CJ_COMPANY_ID, cj.CJ_WEBSITE_ID = saved
    raise AssertionError("an unconfigured read must not reach the network")


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
