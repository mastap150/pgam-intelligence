# =============================================================================
# PGAM Intelligence — Daily Report Config
# =============================================================================
# Edit these values as needed. Everything else is handled automatically.

# ── Rate Card ─────────────────────────────────────────────────────────────────
# Keys are placement IDs as they appear in the CSV.
# cost_cpm: fixed media cost you pay per 1,000 impressions ($4.50 for all)
# Gross revenue is derived from the CSV's Win Price column (see below).

RATE_CARD: dict[str, dict] = {
    "1379": {"name": "US Generic", "cost_cpm": 4.50},
    "1536": {"name": "UK",         "cost_cpm": 4.50},
    "1526": {"name": "US",         "cost_cpm": 4.50},
}

# ── Revenue Calculation ────────────────────────────────────────────────────────
# Reported Revenue  = sum of Win Price column from CSV
# Actual Gross      = Reported Revenue × (1 − PLATFORM_FEE)
# Media Cost        = Impressions × cost_cpm / 1000
# Profit            = Actual Gross − Media Cost

PLATFORM_FEE = 0.15   # 15% deducted from reported Win Price to get actual gross

# ── Google Sheets ─────────────────────────────────────────────────────────────
# PGAM internal sheet (all metrics — PGAM eyes only)
SPREADSHEET_ID = "1FOs0Abm0C686j5caqFtgnJ9667ahEEACq_xPEDcfeLI"

# Partner-facing sheet (impressions + media cost only — shared externally)
PARTNER_SPREADSHEET_ID = "1pEAK5suk9Nmaw8wbF6-utUYKjhhwAuT3qaNYMVoMT0A"

# Tab names are generated dynamically per month, e.g. "PGAM Apr 2026" / "Apr 2026"
# No static tab names needed.

# ── Gmail ─────────────────────────────────────────────────────────────────────
REPORT_SENDER  = "apex@sabiomobile.com"
REPORT_SUBJECT = "Daily Report"          # partial match is fine
REPORT_TO      = "ppatel@pgammedia.com"

# ── CSV column names ──────────────────────────────────────────────────────────
# If the script can't auto-detect columns, set these to the exact header names
# from the CSV (case-insensitive). Leave as None to use auto-detection.
PLACEMENT_COL   = "Placement Id"   # exact header from Sabiomobile CSV
IMPRESSIONS_COL = "Impressions"    # exact header from Sabiomobile CSV
WIN_PRICE_COL   = "Win Price"      # exact header; values like "$3.52800"
DATE_COL        = "Date"           # exact header; format in CSV is MM/DD/YYYY

# ── OAuth credentials file ────────────────────────────────────────────────────
# Download from Google Cloud Console → APIs & Services → Credentials
# Place next to this file and keep the filename below.
CREDENTIALS_FILE = "credentials.json"   # relative to this config file's dir
TOKEN_FILE       = "token.pickle"       # auto-created after first auth
