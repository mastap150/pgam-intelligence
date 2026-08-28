#!/usr/bin/env python3
"""
Turn a client config into a validated campaign payload.

Emits the exact JSON for `create_or_update_campaign`, having first checked it
against the platform rules that are easy to get wrong and only surface as a
failed API call — or worse, as a campaign that publishes and quietly
underperforms.

    scripts/build_campaign_payload.py clients/homebuyerforcash.json
    scripts/build_campaign_payload.py clients/homebuyerforcash.json --check

Exit 0 = payload on stdout, ready to fire. Exit 1 = problems on stderr, nothing
emitted. --check validates without printing the payload.

Rules enforced here, each learned from the platform docs or from an actual
rejection:

  * at most 2 demographic GROUPS (income + net worth uses the budget; adding
    language is rejected)
  * age bands must be a consecutive run, never hand-picked in isolation
  * optimisation goal type must be legal for the campaign goal
  * TRAFFIC/LEADS/SALES/RETARGETING are automatic-bidding only
  * frequency capping exists only on AWARENESS / ABM / APP_PROMOTION
  * attribution window is not configurable on AWARENESS
  * budget >= $50/day equivalent; lifetime budgets need an end date
  * dates in the future, end after start, within 5 years
  * campaign name 10-100 chars, no leading special character
  * county codes are 5-digit FIPS, ZIPs are 5-digit
  * Controls vs Suggestions are mutually exclusive per dimension
"""

import argparse
import json
import os
import re
import sys
from datetime import date, timedelta

# --- platform matrices -------------------------------------------------------

GOAL_OPTIMIZATION = {
    "AWARENESS":     {"COST_PER_UNIQUE_HOUSEHOLD", "CPM"},
    "TRAFFIC":       {"COST_PER_SESSION", "COST_PER_PURCHASE", "ROAS"},
    "LEADS":         {"COST_PER_LEAD"},
    "SALES":         {"COST_PER_PURCHASE", "ROAS"},
    "RETARGETING":   {"COST_PER_PURCHASE", "COST_PER_LEAD", "COST_PER_SESSION", "ROAS"},
    "INSTALL":       {"COST_PER_INSTALL", "COST_PER_PURCHASE", "ROAS"},
    "ABM":           {"COST_PER_UNIQUE_HOUSEHOLD", "COST_PER_LEAD"},
}

AUTOMATIC_ONLY = {"TRAFFIC", "LEADS", "SALES", "RETARGETING"}
FREQUENCY_CAP_OK = {"AWARENESS", "ABM", "INSTALL"}
ATTRIBUTION_FIXED = {"AWARENESS"}
SUGGESTIONS_OK = {"AWARENESS", "TRAFFIC", "LEADS", "SALES"}

AGE_ORDER = ["RANGE_18_20", "RANGE_21_24", "RANGE_25_34", "RANGE_35_44",
             "RANGE_45_54", "RANGE_55_64", "RANGE_65_MORE"]

# Demographic groups, for the max-2 rule.
DEMO_GROUP = [
    ("ESTIMATED_HOUSEHOLD_INCOME__", "income"),
    ("ESTIMATED_HOUSEHOLD_NET_WORTH__", "net worth"),
    ("EDUCATION__", "education"),
    ("CAREER__", "career"),
    ("ETHNICITY__", "ethnicity"),
    ("FAMILY_COMPOSITION__", "family composition"),
    ("LANGUAGE__", "language"),
    ("US_POLITICS__", "politics"),
]

FIPS = re.compile(r"^\d{5}$")
ZIP5 = re.compile(r"^\d{5}$")


def group_of(code):
    for prefix, name in DEMO_GROUP:
        if code.startswith(prefix):
            return name
    return "unknown"


# --- validation --------------------------------------------------------------

class Report:
    def __init__(self):
        self.errors, self.warnings = [], []

    def error(self, msg):
        self.errors.append(msg)

    def warn(self, msg):
        self.warnings.append(msg)

    def ok(self):
        return not self.errors


def require(cfg, path, rep, why=""):
    """Fetch a dotted path, recording an error when it is null or missing."""
    cur = cfg
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            rep.error(f"{path} is missing" + (f" — {why}" if why else ""))
            return None
        cur = cur[part]
    if cur is None or cur == "":
        rep.error(f"{path} is not set" + (f" — {why}" if why else ""))
        return None
    return cur


def validate(cfg, rep):
    goal = require(cfg, "campaign.goal", rep)
    opt_type = require(cfg, "campaign.optimization_goal_type", rep)

    require(cfg, "vibe.advertiser_id", rep,
            "create the advertiser in the dashboard first; there is no API for it")

    # The one value we are forbidden to invent.
    opt_value = cfg.get("campaign", {}).get("optimization_goal_value")
    if opt_value in (None, ""):
        rep.error("campaign.optimization_goal_value is not set — this must come "
                  "from the advertiser, never from a default or another client")
    elif not isinstance(opt_value, (int, float)) or opt_value <= 0:
        rep.error(f"campaign.optimization_goal_value must be a positive number, got {opt_value!r}")

    if goal and goal not in GOAL_OPTIMIZATION:
        rep.error(f"unknown campaign goal {goal!r}")
    elif goal and opt_type and opt_type not in GOAL_OPTIMIZATION[goal]:
        allowed = ", ".join(sorted(GOAL_OPTIMIZATION[goal]))
        rep.error(f"{opt_type} is not a valid optimisation goal for {goal} — allowed: {allowed}")

    name = cfg.get("campaign", {}).get("name", "")
    if not 10 <= len(name) <= 100:
        rep.error(f"campaign.name must be 10-100 characters, got {len(name)}")
    if name[:1] in "@#%!_-":
        rep.error("campaign.name must not start with a special character")

    if goal in ATTRIBUTION_FIXED and cfg["campaign"].get("attribution_window"):
        rep.warn(f"attribution window is not configurable on {goal}; it will be ignored")

    s = cfg.get("strategy", {})

    # Budget
    budget = s.get("budget_usd_per_day")
    btype = s.get("budget_type", "DAILY")
    if not isinstance(budget, (int, float)) or budget <= 0:
        rep.error("strategy.budget_usd_per_day must be a positive number")
    elif btype == "DAILY" and budget < 50:
        rep.error(f"daily budget must be at least $50, got ${budget}")
    if btype == "GLOBAL" and not s.get("ends_at"):
        rep.error("a lifetime budget requires strategy.ends_at")

    # Dates
    starts, ends = s.get("starts_at"), s.get("ends_at")
    if not starts:
        rep.error("strategy.starts_at is not set")
    else:
        try:
            sd = date.fromisoformat(starts)
            if sd < date.today():
                rep.error(f"strategy.starts_at {starts} is in the past")
            if sd > date.today() + timedelta(days=5 * 365):
                rep.error("strategy.starts_at is more than 5 years out")
            if ends:
                ed = date.fromisoformat(ends)
                if ed <= sd:
                    rep.error(f"strategy.ends_at {ends} must be after starts_at {starts}")
        except ValueError:
            rep.error("strategy dates must be YYYY-MM-DD")

    # Bidding
    mode = s.get("bidding_mode", "AUTOMATIC")
    if goal in AUTOMATIC_ONLY and mode != "AUTOMATIC":
        rep.error(f"{goal} supports automatic bidding only, got {mode}")
    if mode in ("MANUAL", "CAPPED_AUTOMATIC") and not s.get("bidding_cpm_usd"):
        rep.error(f"{mode} bidding requires strategy.bidding_cpm_usd")

    # Frequency capping
    if s.get("frequency_cap") and goal not in FREQUENCY_CAP_OK:
        rep.error(f"frequency capping is not available on {goal} — remove strategy.frequency_cap")

    # Geography
    counties = s.get("counties") or []
    for c in counties:
        code = c.get("fips") if isinstance(c, dict) else c
        if not FIPS.match(str(code or "")):
            rep.error(f"county code {code!r} is not a 5-digit FIPS code")
    for z in s.get("zips") or []:
        if not ZIP5.match(str(z)):
            rep.error(f"ZIP {z!r} is not 5 digits")
    if not counties and not s.get("zips") and not s.get("cities") and not s.get("regions"):
        rep.warn("no geography set — the campaign will run nationwide")

    # Age bands must be a consecutive run
    ages = s.get("age_ranges") or []
    bad = [a for a in ages if a not in AGE_ORDER]
    if bad:
        rep.error(f"unknown age ranges: {', '.join(bad)}")
    elif ages:
        idx = sorted(AGE_ORDER.index(a) for a in ages)
        if idx != list(range(idx[0], idx[0] + len(idx))):
            named = ", ".join(AGE_ORDER[i] for i in idx)
            rep.error("age ranges must be a consecutive run, not hand-picked bands: " + named)

    # Demographics: at most 2 groups
    demos = s.get("demographics") or []
    groups = sorted({group_of(d) for d in demos})
    if len(groups) > 2:
        rep.error(f"at most 2 demographic groups allowed, got {len(groups)}: {', '.join(groups)}")
    if "ethnicity" in groups:
        rep.error("ethnicity targeting must not be used on housing-adjacent advertising")

    # Controls vs Suggestions
    for dim in ("interest", "age", "demographic"):
        mode_key = f"{dim}_mode"
        m = s.get(mode_key)
        if m and m not in ("CONTROL", "SUGGESTION"):
            rep.error(f"strategy.{mode_key} must be CONTROL or SUGGESTION, got {m!r}")
        if m == "SUGGESTION" and goal not in SUGGESTIONS_OK:
            rep.error(f"{goal} supports Controls only — strategy.{mode_key} cannot be SUGGESTION")

    if not s.get("creative_ids"):
        rep.warn("no creative attached — the campaign can be saved as a draft but not published")

    return rep


# --- payload -----------------------------------------------------------------

def build(cfg):
    c, s = cfg["campaign"], cfg["strategy"]
    goal = c["goal"]

    strategy = {
        "name": s["name"],
        "budget": int(s["budget_usd_per_day"]),
        "budgetType": s.get("budget_type", "DAILY"),
        "startsAt": s["starts_at"],
        "biddingMode": s.get("bidding_mode", "AUTOMATIC"),
        "targetingCountyIncludedControls": [
            (x["fips"] if isinstance(x, dict) else x) for x in (s.get("counties") or [])
        ],
        "targetingTvInventoryTypeControl": "APPS_AND_CHANNELS",
        "targetingTvInventoryIdIncludedControls": [],
        "targetingTimeRangePresetControl": s.get("delivery", "ANY_TIME_ANY_DAY"),
        "targetingTimeRangeCustomControls": [],
    }
    if s.get("ends_at"):
        strategy["endsAt"] = s["ends_at"]
    if s.get("frequency_cap") and goal in FREQUENCY_CAP_OK:
        strategy["frequencyCapping"] = s["frequency_cap"]
    if s.get("creative_ids"):
        strategy["videoCreativeIds"] = s["creative_ids"]

    # Controls and Suggestions are mutually exclusive per dimension, so only
    # ever populate the side the mode names.
    for dim, mode_key, values_key, ctrl, sugg in (
        ("interest", "interest_mode", "interests",
         "targetingInterestControls", "targetingInterestSuggestions"),
        ("age", "age_mode", "age_ranges",
         "targetingAgeRangeControls", "targetingAgeRangeSuggestions"),
        ("demographic", "demographic_mode", "demographics",
         "targetingDemographicControls", "targetingDemographicSuggestions"),
    ):
        mode = s.get(mode_key, "SUGGESTION")
        values = s.get(values_key) or []
        key = {"interest": "targetingInterestMode", "age": "targetingAgeRangeMode",
               "demographic": "targetingDemographicMode"}[dim]
        strategy[key] = mode
        strategy[ctrl if mode == "CONTROL" else sugg] = values

    campaign = {
        "name": c["name"],
        "advertiserId": cfg["vibe"]["advertiser_id"],
        "goal": goal,
        "countries": c.get("countries", ["USA"]),
        "optimizationGoalType": c["optimization_goal_type"],
        "optimizationGoalValue": c["optimization_goal_value"],
        "active": True,
        "strategies": [strategy],
    }
    if goal not in ATTRIBUTION_FIXED and c.get("attribution_window"):
        campaign["attributionWindow"] = c["attribution_window"]
    return {"campaign": campaign}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config")
    ap.add_argument("--check", action="store_true", help="validate only, emit nothing")
    args = ap.parse_args()

    with open(args.config) as fh:
        cfg = json.load(fh)

    rep = validate(cfg, Report())

    for w in rep.warnings:
        sys.stderr.write(f"warning: {w}\n")
    for e in rep.errors:
        sys.stderr.write(f"ERROR:   {e}\n")

    if not rep.ok():
        sys.stderr.write(f"\n{len(rep.errors)} problem(s) — nothing emitted.\n")
        return 1

    if args.check:
        sys.stderr.write("config valid — ready to build\n")
        return 0

    try:
        json.dump(build(cfg), sys.stdout, indent=2)
        sys.stdout.write("\n")
        sys.stdout.flush()
    except BrokenPipeError:
        # Piping into head/less is normal usage. Point fd 1 at devnull so the
        # interpreter's shutdown flush cannot raise again, then exit quietly
        # instead of dumping a traceback over the user's terminal.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
