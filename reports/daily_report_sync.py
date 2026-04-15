#!/usr/bin/env python3
"""
PGAM Intelligence — Daily Report Sync
======================================
Fetches the daily CSV report from apex@sabiomobile.com, applies the PGAM
rate card, and writes two tabs to a shared Google Sheet:
  • PGAM View    — Date | Placement | Impressions | Gross Revenue | Media Cost | Profit
  • Partner View — Date | Placement | Impressions | Media Cost

Usage:
    python daily_report_sync.py              # process yesterday's report
    python daily_report_sync.py --date 2026-04-11  # process a specific date

Setup:
    See README section at the bottom of this file for credential setup.
"""

import argparse
import base64
import csv
import io
import os
import pickle
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ── load config from sibling config.py ───────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
import config as cfg

# Gmail + Sheets share a single OAuth2 token
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

BASE_DIR = Path(__file__).parent


# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────

def get_credentials() -> Credentials:
    """Return valid Google OAuth2 credentials, refreshing or re-authenticating
    as needed.  On first run this opens a browser window."""
    token_path = BASE_DIR / cfg.TOKEN_FILE
    creds_path = BASE_DIR / cfg.CREDENTIALS_FILE

    creds = None
    if token_path.exists():
        with open(token_path, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not creds_path.exists():
                sys.exit(
                    f"[ERROR] {creds_path} not found.\n"
                    "Download it from Google Cloud Console → APIs & Services → "
                    "Credentials → OAuth 2.0 Client IDs → Download JSON\n"
                    "and place it in the reports/ directory as 'credentials.json'."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, "wb") as f:
            pickle.dump(creds, f)

    return creds


# ─────────────────────────────────────────────────────────────────────────────
# Gmail — find and download the CSV attachment
# ─────────────────────────────────────────────────────────────────────────────

def find_report_message(gmail_svc, report_date: date) -> dict | None:
    """Search Gmail for the daily report for a given date.
    Returns the message dict or None if not found."""
    # Sabiomobile subject uses unpadded month/day: "2026-4-11"
    date_variants = [
        f"{report_date.year}-{report_date.month}-{report_date.day}",       # 2026-4-11
        report_date.strftime("%Y-%m-%d"),                                    # 2026-04-11
    ]
    query_parts = [
        f"from:{cfg.REPORT_SENDER}",
        f"to:{cfg.REPORT_TO}",
        f"subject:{cfg.REPORT_SUBJECT}",
    ]
    # Try each date format
    for dv in set(date_variants):
        query = " ".join(query_parts) + f" subject:{dv}"
        result = gmail_svc.users().messages().list(
            userId="me", q=query, maxResults=5
        ).execute()
        messages = result.get("messages", [])
        if messages:
            return messages[0]

    # Broader fallback — just sender + subject keyword + recent window
    query = " ".join(query_parts)
    result = gmail_svc.users().messages().list(
        userId="me", q=query, maxResults=10
    ).execute()
    messages = result.get("messages", [])

    # Filter by date in subject
    for msg_stub in messages:
        msg = gmail_svc.users().messages().get(
            userId="me", id=msg_stub["id"], format="metadata",
            metadataHeaders=["Subject", "Date"]
        ).execute()
        subject = next(
            (h["value"] for h in msg["payload"]["headers"] if h["name"] == "Subject"),
            ""
        )
        for dv in date_variants:
            if dv in subject:
                return msg_stub

    return None


def download_csv_attachment(gmail_svc, message_id: str) -> str:
    """Download the first .csv attachment from a message.
    Returns the decoded CSV text."""
    msg = gmail_svc.users().messages().get(
        userId="me", id=message_id, format="full"
    ).execute()

    def walk_parts(parts):
        for part in parts:
            if part.get("mimeType", "").startswith("multipart"):
                result = walk_parts(part.get("parts", []))
                if result is not None:
                    return result
            filename = part.get("filename", "")
            if filename.lower().endswith(".csv"):
                body = part.get("body", {})
                attachment_id = body.get("attachmentId")
                data = body.get("data")

                if attachment_id:
                    att = gmail_svc.users().messages().attachments().get(
                        userId="me", messageId=message_id, id=attachment_id
                    ).execute()
                    data = att["data"]

                if data:
                    decoded = base64.urlsafe_b64decode(data + "==")
                    return decoded.decode("utf-8", errors="replace")
        return None

    payload = msg.get("payload", {})
    parts = payload.get("parts", [payload])  # some messages are single-part
    result = walk_parts(parts)

    if result is None:
        raise ValueError(f"No .csv attachment found in message {message_id}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# CSV parsing
# ─────────────────────────────────────────────────────────────────────────────

def _find_col(headers: list[str], candidates: list[str]) -> str | None:
    """Case-insensitive column name lookup."""
    lower = {h.lower(): h for h in headers}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def parse_csv(raw_csv: str, report_date: date) -> list[dict]:
    """Parse the raw CSV text into a list of dicts with keys:
        date, placement_id, placement_name, impressions
    """
    reader = csv.DictReader(io.StringIO(raw_csv))
    headers = reader.fieldnames or []

    # ── resolve column names ──────────────────────────────────────────────────
    placement_col = cfg.PLACEMENT_COL or _find_col(
        headers, ["placement_id", "placement", "campaign_id", "campaign",
                  "ad_unit_id", "ad unit", "placementid", "site_id"]
    )
    impressions_col = cfg.IMPRESSIONS_COL or _find_col(
        headers, ["impressions", "imps", "imp", "views", "requests",
                  "total_impressions", "served_impressions"]
    )
    date_col = cfg.DATE_COL or _find_col(
        headers, ["date", "day", "report_date", "reportdate"]
    )

    if not placement_col:
        # Last-resort: find the column whose values match known placement IDs
        known_ids = set(cfg.RATE_CARD.keys())
        rows_sample = list(reader)
        for col in headers:
            vals = {str(r.get(col, "")).strip() for r in rows_sample}
            if vals & known_ids:
                placement_col = col
                break
        if not placement_col:
            print(f"[WARN] Could not auto-detect placement column. Headers: {headers}")
            print("       Set PLACEMENT_COL in config.py to the exact column name.")
        reader = csv.DictReader(io.StringIO(raw_csv))  # reset

    if not impressions_col:
        print(f"[WARN] Could not auto-detect impressions column. Headers: {headers}")
        print("       Set IMPRESSIONS_COL in config.py to the exact column name.")

    win_price_col = getattr(cfg, "WIN_PRICE_COL", None) or _find_col(
        headers, ["win price", "win_price", "winprice", "revenue", "spend"]
    )

    print(f"[INFO] Using columns: placement={placement_col!r}  "
          f"impressions={impressions_col!r}  win_price={win_price_col!r}  "
          f"date={date_col!r}")

    # ── accumulate impressions + win price by placement ───────────────────────
    # CSV Date format from Sabiomobile is MM/DD/YYYY e.g. "04/11/2026"
    totals: dict[str, dict] = {}
    for row in reader:
        pid = str(row.get(placement_col, "")).strip() if placement_col else ""
        if pid not in cfg.RATE_CARD:
            continue  # skip placements not in the rate card

        try:
            imps = int(str(row.get(impressions_col, "0")).replace(",", "").strip())
        except ValueError:
            imps = 0

        try:
            wp_raw = str(row.get(win_price_col, "0") if win_price_col else "0")
            win_price = float(wp_raw.replace("$", "").replace(",", "").strip())
        except ValueError:
            win_price = 0.0

        if pid not in totals:
            totals[pid] = {"impressions": 0, "win_price": 0.0}
        totals[pid]["impressions"] += imps
        totals[pid]["win_price"]   += win_price

    records = []
    for pid, agg in sorted(totals.items()):
        rate = cfg.RATE_CARD[pid]
        records.append({
            "date":             report_date.isoformat(),
            "placement_id":     pid,
            "placement_name":   rate["name"],
            "impressions":      agg["impressions"],
            "reported_revenue": round(agg["win_price"], 4),
            "cost_cpm":         rate["cost_cpm"],
        })

    if not records:
        reader2 = csv.DictReader(io.StringIO(raw_csv))
        if placement_col:
            found_ids = {str(r.get(placement_col, "")).strip() for r in reader2}
            print(f"[DEBUG] Placement IDs found in CSV: {sorted(found_ids)}")
            print(f"[DEBUG] Rate card keys: {list(cfg.RATE_CARD.keys())}")

    return records


# ─────────────────────────────────────────────────────────────────────────────
# Calculations
# ─────────────────────────────────────────────────────────────────────────────

def enrich(records: list[dict]) -> list[dict]:
    """Compute actual_gross, media_cost, and profit for each record.

    reported_revenue  = sum of Win Price from CSV
    actual_gross      = reported_revenue × (1 − PLATFORM_FEE)   [after 15%]
    media_cost        = impressions × cost_cpm / 1000            [fixed $4.50]
    profit            = actual_gross − media_cost
    """
    enriched = []
    for r in records:
        actual_gross = round(r["reported_revenue"] * (1 - cfg.PLATFORM_FEE), 2)
        media_cost   = round(r["impressions"] * r["cost_cpm"] / 1000, 2)
        profit       = round(actual_gross - media_cost, 2)
        enriched.append({**r,
            "actual_gross": actual_gross,
            "media_cost":   media_cost,
            "profit":       profit,
        })
    return enriched


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets helpers
# ─────────────────────────────────────────────────────────────────────────────

PGAM_HEADERS    = ["Date", "Placement ID", "Placement Name", "Impressions",
                   "Reported Revenue ($)", "Actual Gross ($)", "Media Cost ($)", "Profit ($)"]
PARTNER_HEADERS = ["Date", "Placement ID", "Placement Name", "Impressions",
                   "Media Cost ($)"]

# Legacy tab names written in the initial setup — removed when first MTD run happens
_LEGACY_TABS = ["PGAM View", "Partner View"]


def pgam_tab_name(d: date) -> str:
    """e.g. 'PGAM Apr 2026'  — internal MTD tab for the given month."""
    return f"PGAM {d.strftime('%b %Y')}"


def partner_tab_name(d: date) -> str:
    """e.g. 'Apr 2026'  — partner-facing MTD tab for the given month."""
    return d.strftime("%b %Y")


def _ensure_tab(spreadsheet, tab_name: str, headers: list[str]) -> gspread.Worksheet:
    """Return the worksheet, creating it with a header row if it doesn't exist."""
    try:
        ws = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=2000, cols=len(headers))
        ws.append_row(headers, value_input_option="USER_ENTERED")
        print(f"[INFO] Created tab '{tab_name}'")
    return ws


def _delete_legacy_tabs(spreadsheet):
    """Remove old fixed-name tabs from the initial setup (one-time cleanup)."""
    for name in _LEGACY_TABS:
        try:
            ws = spreadsheet.worksheet(name)
            spreadsheet.del_worksheet(ws)
            print(f"[INFO] Removed legacy tab '{name}'")
        except gspread.WorksheetNotFound:
            pass


def _upsert_rows(ws: gspread.Worksheet, new_rows: list[list], date_col_idx: int = 0):
    """Append rows for a given date, replacing any existing rows for that date.
    Preserves all other dates' rows — this is what gives us MTD accumulation."""
    if not new_rows:
        return

    report_date = new_rows[0][date_col_idx]
    all_values  = ws.get_all_values()

    # Find row indices (1-based) for this date (skip header row 1)
    existing_row_indices = [
        i + 1
        for i, row in enumerate(all_values)
        if i > 0 and len(row) > date_col_idx and row[date_col_idx] == str(report_date)
    ]

    # Delete in reverse to preserve indices
    for row_idx in sorted(existing_row_indices, reverse=True):
        ws.delete_rows(row_idx)

    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    print(f"[INFO] Wrote {len(new_rows)} rows to '{ws.title}' for {report_date}")


def _build_rows(records: list[dict], view: str) -> list[list]:
    """Build sheet rows + a daily TOTAL row. view='pgam' or 'partner'."""
    if view == "pgam":
        rows = [
            [r["date"], r["placement_id"], r["placement_name"], r["impressions"],
             r["reported_revenue"], r["actual_gross"], r["media_cost"], r["profit"]]
            for r in records
        ]
        rows.append([
            records[0]["date"], "TOTAL", "",
            sum(r["impressions"]            for r in records),
            round(sum(r["reported_revenue"] for r in records), 2),
            round(sum(r["actual_gross"]     for r in records), 2),
            round(sum(r["media_cost"]       for r in records), 2),
            round(sum(r["profit"]           for r in records), 2),
        ])
    else:  # partner
        rows = [
            [r["date"], r["placement_id"], r["placement_name"], r["impressions"],
             r["media_cost"]]
            for r in records
        ]
        rows.append([
            records[0]["date"], "TOTAL", "",
            sum(r["impressions"] for r in records),
            round(sum(r["media_cost"] for r in records), 2),
        ])
    return rows


def update_sheets(records: list[dict], report_date: date, creds: Credentials):
    gc = gspread.authorize(creds)

    pgam_tab    = pgam_tab_name(report_date)     # e.g. "PGAM Apr 2026"
    p_tab       = partner_tab_name(report_date)  # e.g. "Apr 2026"

    # ── PGAM internal sheet ───────────────────────────────────────────────────
    pgam_sheet = gc.open_by_key(cfg.SPREADSHEET_ID)
    _delete_legacy_tabs(pgam_sheet)

    pgam_ws    = _ensure_tab(pgam_sheet, pgam_tab, PGAM_HEADERS)
    partner_ws = _ensure_tab(pgam_sheet, p_tab,    PARTNER_HEADERS)

    _upsert_rows(pgam_ws,    _build_rows(records, "pgam"))
    _upsert_rows(partner_ws, _build_rows(records, "partner"))

    # ── Partner external sheet ────────────────────────────────────────────────
    partner_sheet = gc.open_by_key(cfg.PARTNER_SPREADSHEET_ID)
    ext_ws = _ensure_tab(partner_sheet, p_tab, PARTNER_HEADERS)
    _upsert_rows(ext_ws, _build_rows(records, "partner"))


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync Sabiomobile daily report to Google Sheets")
    parser.add_argument(
        "--date", type=str, default=None,
        help="Report date as YYYY-MM-DD (default: yesterday)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and print results without writing to Sheets"
    )
    parser.add_argument(
        "--retries", type=int, default=5,
        help="How many times to retry if email not found yet (default: 5)"
    )
    parser.add_argument(
        "--retry-interval", type=int, default=20,
        help="Minutes to wait between retries (default: 20)"
    )
    args = parser.parse_args()

    if args.date:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        report_date = date.today() - timedelta(days=1)

    print(f"[INFO] Processing report for {report_date}")

    creds = get_credentials()
    gmail = build("gmail", "v1", credentials=creds)

    # ── find the email (with retries for late delivery) ───────────────────────
    msg_stub = None
    for attempt in range(1, args.retries + 1):
        msg_stub = find_report_message(gmail, report_date)
        if msg_stub:
            break
        if attempt < args.retries:
            print(f"[INFO] Email not found yet (attempt {attempt}/{args.retries}). "
                  f"Waiting {args.retry_interval} min...")
            import time
            time.sleep(args.retry_interval * 60)
        else:
            sys.exit(
                f"[ERROR] No daily report found for {report_date} after {args.retries} attempts.\n"
                f"        Expected email from {cfg.REPORT_SENDER} with subject "
                f"containing '{cfg.REPORT_SUBJECT}' and date {report_date}."
            )
    print(f"[INFO] Found message: {msg_stub['id']}")

    # ── download CSV ──────────────────────────────────────────────────────────
    raw_csv = download_csv_attachment(gmail, msg_stub["id"])
    print(f"[INFO] Downloaded CSV ({len(raw_csv):,} bytes)")

    # ── parse + enrich ────────────────────────────────────────────────────────
    records = parse_csv(raw_csv, report_date)
    if not records:
        sys.exit(
            "[ERROR] No matching placements found in the CSV.\n"
            "        Check that the CSV contains placement IDs from the rate card: "
            + ", ".join(cfg.RATE_CARD.keys())
        )
    records = enrich(records)

    # ── print summary ─────────────────────────────────────────────────────────
    print(f"\n{'─'*80}")
    print(f"  {'Placement':<22} {'Imps':>10} {'Reported':>11} {'Actual Gross':>13} {'Cost':>9} {'Profit':>9}")
    print(f"{'─'*80}")
    for r in records:
        print(f"  {r['placement_name']:<22} {r['impressions']:>10,} "
              f"${r['reported_revenue']:>10,.4f} ${r['actual_gross']:>12,.2f} "
              f"${r['media_cost']:>8,.2f} ${r['profit']:>8,.2f}")
    print(f"{'─'*80}")
    print(f"  {'TOTAL':<22} {sum(r['impressions'] for r in records):>10,} "
          f"${sum(r['reported_revenue'] for r in records):>10,.4f} "
          f"${sum(r['actual_gross'] for r in records):>12,.2f} "
          f"${sum(r['media_cost'] for r in records):>8,.2f} "
          f"${sum(r['profit'] for r in records):>8,.2f}")
    print(f"{'─'*80}\n")

    if args.dry_run:
        print("[DRY RUN] Skipping Google Sheets update.")
        return

    # ── write to Sheets ───────────────────────────────────────────────────────
    update_sheets(records, report_date, creds)
    print("[INFO] Done.")


if __name__ == "__main__":
    main()


# =============================================================================
# SETUP GUIDE
# =============================================================================
#
# 1. GOOGLE CLOUD PROJECT
#    ─────────────────────
#    a. Go to https://console.cloud.google.com
#    b. Create a new project (or use an existing one)
#    c. Enable these two APIs:
#       • Gmail API
#       • Google Sheets API
#
# 2. OAUTH2 CREDENTIALS
#    ───────────────────
#    a. APIs & Services → Credentials → Create Credentials → OAuth client ID
#    b. Application type: Desktop app
#    c. Download the JSON file → rename to "credentials.json"
#    d. Place it in:  pgam-intelligence/reports/credentials.json
#    e. Add ppatel@pgammedia.com as a Test User under OAuth consent screen
#
# 3. GOOGLE SHEET
#    ─────────────
#    a. Create a new Google Sheet at https://sheets.google.com
#    b. Copy the ID from the URL (the long string between /d/ and /edit)
#    c. Paste it into config.py → SPREADSHEET_ID
#    d. Share the sheet with your Google account (the one you'll auth with)
#
# 4. INSTALL DEPENDENCIES
#    ─────────────────────
#    pip install -r requirements.txt
#
# 5. FIRST RUN (opens browser for auth)
#    ────────────────────────────────────
#    cd pgam-intelligence/reports
#    python daily_report_sync.py --date 2026-04-11 --dry-run
#
# 6. SCHEDULE DAILY (9 AM local time, processes yesterday's report)
#    ───────────────────────────────────────────────────────────────
#    Add to crontab:   crontab -e
#    0 9 * * * cd /Users/priyeshpatel/Desktop/pgam-intelligence/reports && \
#              /usr/bin/python3 daily_report_sync.py >> sync.log 2>&1
# =============================================================================
