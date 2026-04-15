"""
HealthNation Automation — Configuration
Copy this file's values into a .env file in this directory.
Never commit .env to version control.
"""
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"), override=True)

# ── Anthropic / Claude ────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = "claude-sonnet-4-6"   # Best quality-to-cost for long-form
CLAUDE_MAX_TOKENS = 8000

# ── WordPress REST API ────────────────────────────────────────────────────────
# Your WordPress site URL (no trailing slash)
WP_SITE_URL   = os.getenv("WP_SITE_URL", "https://healthnation.com")
# WordPress username (admin account)
WP_USERNAME   = os.getenv("WP_USERNAME", "")
# Application Password from: WP Admin → Users → Profile → Application Passwords
# Format: xxxx xxxx xxxx xxxx xxxx xxxx  (spaces are fine, requests handles it)
WP_APP_PASS   = os.getenv("WP_APP_PASS", "")

# ── Unsplash API ──────────────────────────────────────────────────────────────
# Free key from: https://unsplash.com/developers  (50 requests/hour free)
UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY", "")

# ── Publishing settings ───────────────────────────────────────────────────────
DEFAULT_POST_STATUS  = "publish"    # "draft" to review before publishing
ARTICLES_PER_DAY     = 2
DEFAULT_AUTHOR_ID    = 3            # WordPress user ID for the author
DEFAULT_REVIEWER     = "Editorial Team, HealthNation"
DEFAULT_REVIEWER_CREDENTIALS = "Science & Medical Review Team"

# ── Category slugs (must match WordPress category slugs) ─────────────────────
CATEGORIES = {
    "nutrition":     None,   # Will be fetched from WP at runtime
    "fitness":       None,
    "mental-health": None,
    "longevity":     None,
    "sleep":         None,
    "conditions":    None,
}

# ── Validation ────────────────────────────────────────────────────────────────
def validate():
    missing = []
    if not ANTHROPIC_API_KEY: missing.append("ANTHROPIC_API_KEY")
    if not WP_SITE_URL:       missing.append("WP_SITE_URL")
    if not WP_USERNAME:       missing.append("WP_USERNAME")
    if not WP_APP_PASS:       missing.append("WP_APP_PASS")
    if not UNSPLASH_ACCESS_KEY: missing.append("UNSPLASH_ACCESS_KEY")
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Create a .env file in this directory with these values."
        )

DOTENV_TEMPLATE = """
# HealthNation Automation — .env file
# Fill in your values and save as .env in the healthnation-automation/ folder

ANTHROPIC_API_KEY=your_claude_api_key_here
WP_SITE_URL=https://healthnation.com
WP_USERNAME=your_wordpress_admin_username
WP_APP_PASS=xxxx xxxx xxxx xxxx xxxx xxxx
UNSPLASH_ACCESS_KEY=your_unsplash_access_key_here
""".strip()
