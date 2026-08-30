#!/usr/bin/env python3
"""Tests for the campaign payload builder — mostly that bad configs are caught."""

import copy
import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_campaign_payload as B  # noqa: E402

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"PASS :: {name}")
    else:
        FAIL += 1
        print(f"FAIL :: {name}" + (f" :: {extra}" if extra else ""))


def errors(cfg):
    return B.validate(cfg, B.Report()).errors


def has(cfg, fragment):
    return any(fragment.lower() in e.lower() for e in errors(cfg))


def base():
    """A config that should validate cleanly."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "clients", "homebuyerforcash.json")) as fh:
        cfg = json.load(fh)
    soon = date.today() + timedelta(days=7)
    cfg["vibe"]["advertiser_id"] = "11111111-1111-4111-8111-111111111111"
    cfg["campaign"]["optimization_goal_value"] = 2.5
    cfg["strategy"]["starts_at"] = soon.isoformat()
    cfg["strategy"]["ends_at"] = (soon + timedelta(days=30)).isoformat()
    cfg["strategy"]["creative_ids"] = ["22222222-2222-4222-8222-222222222222"]
    return cfg


def main():
    # ---- the shipped config is sound once the unset fields are filled ----
    check("filled-in reference config validates", not errors(base()), str(errors(base())))

    # ---- the fields we must never invent ----
    c = base(); c["campaign"]["optimization_goal_value"] = None
    check("unset optimisation value is refused", has(c, "must come from the advertiser"))

    c = base(); c["vibe"]["advertiser_id"] = None
    check("missing advertiser id is refused", has(c, "advertiser_id"))

    # ---- goal / optimisation matrix ----
    c = base(); c["campaign"]["optimization_goal_type"] = "COST_PER_LEAD"
    check("cost per lead rejected on a Traffic campaign", has(c, "not a valid optimisation goal"))

    c = base(); c["campaign"]["goal"] = "LEADS"; c["campaign"]["optimization_goal_type"] = "COST_PER_LEAD"
    check("cost per lead accepted on a Leads campaign", not errors(c), str(errors(c)))

    c = base(); c["campaign"]["goal"] = "AWARENESS"
    c["campaign"]["optimization_goal_type"] = "COST_PER_UNIQUE_HOUSEHOLD"
    check("awareness + cost per household is valid", not errors(c), str(errors(c)))

    # ---- the rule that actually bit us ----
    c = base(); c["strategy"]["demographics"] = c["strategy"]["demographics"] + ["LANGUAGE__ENGLISH"]
    check("a third demographic group is refused", has(c, "at most 2 demographic groups"))

    c = base(); c["strategy"]["demographics"] = ["ETHNICITY__CAUCASIAN"]
    check("ethnicity targeting is refused outright", has(c, "ethnicity"))

    # ---- age bands ----
    c = base(); c["strategy"]["age_ranges"] = ["RANGE_25_34", "RANGE_55_64"]
    check("non-consecutive age bands refused", has(c, "consecutive run"))

    c = base(); c["strategy"]["age_ranges"] = ["RANGE_35_44", "RANGE_45_54", "RANGE_55_64"]
    check("consecutive age bands accepted", not errors(c), str(errors(c)))

    c = base(); c["strategy"]["age_ranges"] = ["RANGE_99"]
    check("unknown age band refused", has(c, "unknown age ranges"))

    # ---- bidding and frequency capping ----
    c = base(); c["strategy"]["bidding_mode"] = "MANUAL"
    check("manual bidding refused on Traffic", has(c, "automatic bidding only"))

    c = base(); c["strategy"]["frequency_cap"] = "3_PER_DAY"
    check("frequency cap refused on Traffic", has(c, "frequency capping is not available"))

    c = base(); c["campaign"]["goal"] = "AWARENESS"
    c["campaign"]["optimization_goal_type"] = "COST_PER_UNIQUE_HOUSEHOLD"
    c["strategy"]["frequency_cap"] = "3_PER_DAY"
    check("frequency cap allowed on Awareness", not errors(c), str(errors(c)))

    # ---- Controls vs Suggestions ----
    c = base(); c["campaign"]["goal"] = "RETARGETING"
    c["campaign"]["optimization_goal_type"] = "COST_PER_SESSION"
    check("Suggestions refused on Retargeting", has(c, "Controls only"))

    # ---- budget and dates ----
    c = base(); c["strategy"]["budget_usd_per_day"] = 25
    check("sub-minimum daily budget refused", has(c, "at least $50"))

    c = base(); c["strategy"]["starts_at"] = "2020-01-01"
    check("start date in the past refused", has(c, "in the past"))

    c = base()
    c["strategy"]["ends_at"] = c["strategy"]["starts_at"]
    check("end date not after start refused", has(c, "must be after"))

    c = base(); c["strategy"]["budget_type"] = "GLOBAL"; c["strategy"]["ends_at"] = None
    check("lifetime budget without an end date refused", has(c, "requires strategy.ends_at"))

    # ---- geography ----
    c = base(); c["strategy"]["counties"] = [{"fips": "4010", "name": "bad"}]
    check("malformed FIPS refused", has(c, "5-digit FIPS"))

    # ---- name ----
    c = base(); c["campaign"]["name"] = "short"
    check("too-short campaign name refused", has(c, "10-100 characters"))

    c = base(); c["campaign"]["name"] = "-leading special char name here"
    check("leading special character refused", has(c, "special character"))

    # ---- payload shape ----
    payload = B.build(base())["campaign"]
    st = payload["strategies"][0]
    check("exactly one strategy emitted", len(payload["strategies"]) == 1)
    check("counties flattened to FIPS codes",
          st["targetingCountyIncludedControls"] == ["40109", "40027", "40017", "40125"],
          str(st["targetingCountyIncludedControls"]))
    check("suggestion mode populates the suggestions list only",
          st["targetingInterestSuggestions"] and st.get("targetingInterestControls") is None,
          f"ctrl={st.get('targetingInterestControls')}")

    c = base(); c["strategy"]["interest_mode"] = "CONTROL"
    st2 = B.build(c)["campaign"]["strategies"][0]
    check("control mode populates the controls list only",
          st2["targetingInterestControls"] and st2.get("targetingInterestSuggestions") is None,
          f"sugg={st2.get('targetingInterestSuggestions')}")

    check("attribution window carried on Traffic",
          B.build(base())["campaign"].get("attributionWindow") == "THIRTY_DAYS")

    c = base(); c["campaign"]["goal"] = "AWARENESS"
    c["campaign"]["optimization_goal_type"] = "COST_PER_UNIQUE_HOUSEHOLD"
    check("attribution window omitted on Awareness",
          "attributionWindow" not in B.build(c)["campaign"])

    # ---- warnings, not errors ----
    c = base(); c["strategy"]["creative_ids"] = []
    rep = B.validate(c, B.Report())
    check("missing creative warns but does not block a draft",
          rep.ok() and any("creative" in w for w in rep.warnings))

    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
