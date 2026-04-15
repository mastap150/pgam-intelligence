"""
core/ui_nav.py

Canonical SSP UI navigation step templates.

Every agent that recommends a change appends one of these blocks so whoever
reads the Slack alert can execute it in under 2 minutes without looking
anything up.

All navigation paths are for ssp.pgammedia.com (Teqblaze SSP).
Update SSP_BASE if the domain changes.
"""

SSP_BASE = "ssp.pgammedia.com"


def floor_change(publisher: str, from_price: float, to_price: float) -> str:
    """
    Navigation steps to change a publisher's CPM floor price.

    Returns a Slack-markdown string starting with '→ *Execute:*'.
    """
    return (
        f"→ *Execute:* `{SSP_BASE}` → *Publishers* → search `{publisher}` "
        f"→ *Floor Prices* tab → set CPM floor `${from_price:.3f}` → `${to_price:.3f}` → *Save*"
    )


def demand_seat_add(publisher: str, demand_partner: str) -> str:
    """
    Navigation steps to add a demand partner seat to a publisher.
    """
    return (
        f"→ *Execute:* `{SSP_BASE}` → *Publishers* → search `{publisher}` "
        f"→ *Demand Partners* tab → *Add Seat* → select `{demand_partner}` → *Save*"
    )


def demand_seat_floor(publisher: str, demand_partner: str, from_price: float, to_price: float) -> str:
    """
    Navigation steps to change a floor price for a specific publisher × demand partner combo.
    """
    return (
        f"→ *Execute:* `{SSP_BASE}` → *Publishers* → search `{publisher}` "
        f"→ *Demand Partners* tab → click `{demand_partner}` → *Floor Price* "
        f"`${from_price:.3f}` → `${to_price:.3f}` → *Save*"
    )


def geo_target_add(demand_partner: str, country: str) -> str:
    """
    Navigation steps to add a country to a demand partner's geo targeting.
    """
    return (
        f"→ *Execute:* `{SSP_BASE}` → *Demand* → search `{demand_partner}` "
        f"→ *Targeting* tab → *Geography* → add `{country}` → *Save*"
    )


def publisher_demand_connect(publisher: str, demand_partner: str) -> str:
    """
    Navigation steps to connect a demand partner to a publisher (supply side).
    """
    return (
        f"→ *Execute:* `{SSP_BASE}` → *Demand* → search `{demand_partner}` "
        f"→ *Publishers* tab → *Add Publisher* → select `{publisher}` → *Save*"
    )


# ---------------------------------------------------------------------------
# System-prompt snippet
# ---------------------------------------------------------------------------
# Append this to any Claude system prompt where you want Claude to include
# navigation steps. Tells Claude the SSP base URL and the expected format.

NAV_INSTRUCTIONS = f"""
After each recommended action, append a single line formatted exactly like this:
→ *Execute:* `{SSP_BASE}` → [navigation path] → [field] `[old value]` → `[new value]` → *Save*

Navigation patterns:
  Floor price change:       `{SSP_BASE}` → *Publishers* → search `[publisher]` → *Floor Prices* tab → set CPM floor `$X.XXX` → `$Y.YYY` → *Save*
  Add demand partner seat:  `{SSP_BASE}` → *Publishers* → search `[publisher]` → *Demand Partners* tab → *Add Seat* → select `[DP name]` → *Save*
  DP-level floor:           `{SSP_BASE}` → *Publishers* → search `[publisher]` → *Demand Partners* tab → click `[DP name]` → *Floor Price* `$X.XXX` → `$Y.YYY` → *Save*
  Geo targeting:            `{SSP_BASE}` → *Demand* → search `[DP name]` → *Targeting* tab → *Geography* → add `[country]` → *Save*
  Connect DP to publisher:  `{SSP_BASE}` → *Demand* → search `[DP name]` → *Publishers* tab → *Add Publisher* → select `[publisher]` → *Save*

Always use the exact publisher and demand partner names from the data. Always include real dollar values.
""".strip()
