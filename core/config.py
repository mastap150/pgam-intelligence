import os
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# API credentials
# ---------------------------------------------------------------------------
TB_API_BASE_URL = os.environ.get("TB_API_BASE_URL", "http://stats.ortb.net/v1/stats")
TB_CLIENT_KEY   = os.environ.get("TB_CLIENT_KEY", "")
TB_SECRET_KEY   = os.environ.get("TB_SECRET_KEY", "")

# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------
SLACK_WEBHOOK  = os.environ.get("SLACK_WEBHOOK", "")
SENDGRID_KEY   = os.environ.get("SENDGRID_KEY", "")
SENDER_EMAIL   = os.environ.get("EMAIL_FROM", "")
RECIPIENTS     = [
    addr.strip()
    for addr in os.environ.get("EMAIL_TO", "").split(",")
    if addr.strip()
]

# ---------------------------------------------------------------------------
# Claude / Anthropic
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ---------------------------------------------------------------------------
# Alert & intelligence thresholds
# ---------------------------------------------------------------------------
THRESHOLDS = {
    "revenue_behind_pct":   40,       # % behind expected daily pace before alerting
    "min_revenue_for_alert": 100,     # minimum $ revenue before pace alerts fire
    "floor_raise_ratio":    2.0,      # CPM floor raise multiplier
    "floor_lower_ratio":    0.5,      # CPM floor lower multiplier
    "opp_fill_threshold":   0.0005,   # minimum opportunity fill rate
    "margin_floor":         20.0,     # minimum acceptable margin %
    "lookback_days":        7,        # days of history used for trend analysis
}
