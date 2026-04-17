"""
intelligence/floor_model.py

Floor elasticity model — predicts clearing eCPM per (publisher, demand_partner,
country) segment, used as the primary signal for floor recommendations.

Problem statement
-----------------
Our pre-ML heuristic sets a new demand's floor at 40% of the demand partner's
30-day historical eCPM (averaged across publishers). That's a crude signal:
it ignores publisher-specific clearing behavior, country mix, and format.

This model uses a gradient-boosted regression (LightGBM) to predict clearing
eCPM at the (pub × demand × country) level, with quantile regression for
confidence bands. Recommended floor = predicted_p50 × 0.40, gated by the
confidence band.

Data pipeline
-------------
Source: LL GET stats API via core.api.fetch()
Dimensions: PUBLISHER, DEMAND_PARTNER, COUNTRY, DATE
Metrics:    WINS, BIDS, BID_REQUESTS, OPPORTUNITIES, GROSS_REVENUE, PUB_PAYOUT
Window:     30 days, with last 7 days held out for validation

Training row granularity
------------------------
One row per (pub_id, demand_partner, country, date). For inference we
aggregate the last 14 days to produce one prediction per (pub_id,
demand_partner, country).

Features
--------
Categorical (label-encoded):
  - pub_id
  - demand_partner
  - country (top 25 by volume, else 'OTHER')
  - format (ctv / interstitial / video / display / inapp — derived from
    publisher name tokens)
Numeric:
  - log_wins (log1p-transformed recent wins on same segment)
  - win_rate (wins / bids)
  - dow (day of week)

Target
------
  log_ecpm = log1p(GROSS_REVENUE / WINS * 1000)   (log for heavy-tailed dist)

Models
------
  median (quantile=0.5) → main prediction
  p10    (quantile=0.1) → pessimistic bound
  p90    (quantile=0.9) → optimistic bound
Confidence = narrowness of (p90 - p10) band relative to median.

Artifacts
---------
  logs/floor_model.pkl        — trained models + feature encoders
  logs/floor_predictions.json — per-segment predictions for lookup by the optimizer
  logs/floor_model_metrics.json — validation metrics from last training run

Entry points
------------
  train_and_predict()     — end-to-end: pull data, train, predict, save
  lookup_prediction(...)  — called by new_partner_optimizer at runtime
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
import warnings
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import joblib
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder

warnings.filterwarnings("ignore", category=UserWarning, module="lightgbm")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, ".env"), override=True)

from core.api import fetch
from core.ll_report import _sf
import core.ll_mgmt as llm

MODEL_PATH        = os.path.join(_REPO_ROOT, "logs", "floor_model.pkl")
PREDICTIONS_PATH  = os.path.join(_REPO_ROOT, "logs", "floor_predictions.json")
METRICS_PATH      = os.path.join(_REPO_ROOT, "logs", "floor_model_metrics.json")

# ─────────────────────────────────────────────────────────────────────────────
# Tuning
# ─────────────────────────────────────────────────────────────────────────────

TRAIN_DAYS = 30
HOLDOUT_DAYS = 7
MIN_WINS_PER_ROW = 5              # rows with < this many wins are noise
TOP_COUNTRIES_N = 25              # explicit encode; everything else → "OTHER"
FLOOR_RATIO = 0.40                # floor = predicted_eCPM × FLOOR_RATIO
CONFIDENCE_BAND_MAX = 1.2         # (p90-p10)/p50 > this → low confidence

FORMAT_PATTERNS = [
    ("ctv",          [r"\bctv\b", r"wurl", r"roku", r"future\s*today", r"ottera",
                      r"fuse\s*media", r"blue\s*ant", r"cox\s*media", r"lifevista",
                      r"quickcast", r"springserve"]),
    ("interstitial", [r"interstitial", r"\bintst\b"]),
    ("display",      [r"\d+x\d+", r"\bdisplay\b", r"\bbanner\b"]),
    ("video",        [r"\bvideo\b", r"\bolv\b", r"vast"]),
    ("inapp",        [r"in\s*[-_]?app", r"in_app", r"inapp"]),
]


def _infer_format(pub_name: str) -> str:
    t = (pub_name or "").lower()
    for fmt, pats in FORMAT_PATTERNS:
        for pat in pats:
            if re.search(pat, t):
                return fmt
    return "display"


# ─────────────────────────────────────────────────────────────────────────────
# Data
# ─────────────────────────────────────────────────────────────────────────────

def pull_training_data(days: int = TRAIN_DAYS) -> pd.DataFrame:
    """Fetch (pub × demand × country × date) 30-day slice, return DataFrame."""
    end = date.today()
    start = end - timedelta(days=days)
    print(f"[floor_model] pulling {days}d data ({start} → {end}) …", flush=True)
    rows = fetch(
        "PUBLISHER,DEMAND_PARTNER,COUNTRY,DATE",
        "WINS,BIDS,BID_REQUESTS,OPPORTUNITIES,GROSS_REVENUE,PUB_PAYOUT",
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
    )
    df = pd.DataFrame(rows)
    print(f"[floor_model]   {len(df)} raw rows", flush=True)
    if len(df) == 0:
        return df

    df["pub_id"] = df["PUBLISHER_ID"].apply(lambda v: int(_sf(v)))
    df["pub_name"] = df["PUBLISHER_NAME"].fillna("")
    df["demand_partner"] = df["DEMAND_PARTNER_NAME"].fillna("")
    df["country"] = df["COUNTRY"].fillna("")
    df["date"] = pd.to_datetime(df["DATE"], errors="coerce")
    for col in ("WINS", "BIDS", "BID_REQUESTS", "OPPORTUNITIES", "GROSS_REVENUE", "PUB_PAYOUT"):
        df[col] = df[col].apply(_sf).astype(float)

    # Drop rows with no wins (no signal)
    df = df[df["WINS"] >= MIN_WINS_PER_ROW].copy()
    df["ecpm"] = df["GROSS_REVENUE"] / df["WINS"] * 1000.0
    df["log_ecpm"] = np.log1p(df["ecpm"])
    df["wr"] = df["WINS"] / df["BIDS"].clip(lower=1)
    df["log_wins"] = np.log1p(df["WINS"])
    df["dow"] = df["date"].dt.dayofweek
    df["format"] = df["pub_name"].apply(_infer_format)

    # Margin as a feature — per-publisher 30-day rolling margin.
    # Gives the model a signal that "this publisher's economics are constrained"
    # and nudges predictions accordingly.
    pub_agg = df.groupby("pub_id").agg(
        _rev_sum=("GROSS_REVENUE", "sum"),
        _pay_sum=("PUB_PAYOUT", "sum"),
    )
    pub_agg["margin_30d"] = np.where(
        pub_agg["_rev_sum"] > 0,
        (pub_agg["_rev_sum"] - pub_agg["_pay_sum"]) / pub_agg["_rev_sum"] * 100.0,
        30.0,  # neutral default for pubs with no revenue
    )
    df = df.merge(pub_agg[["margin_30d"]], left_on="pub_id", right_index=True, how="left")
    df["margin_30d"] = df["margin_30d"].fillna(30.0)

    df = df[df["date"].notna()].copy()
    print(f"[floor_model]   {len(df)} rows after filtering (wins ≥ {MIN_WINS_PER_ROW}, ecpm finite)", flush=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Features
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Encoders:
    pub_id: LabelEncoder
    demand_partner: LabelEncoder
    country: LabelEncoder
    fmt: LabelEncoder
    top_countries: set

    def to_dict(self):
        return {
            "pub_id":          list(self.pub_id.classes_),
            "demand_partner":  list(self.demand_partner.classes_),
            "country":         list(self.country.classes_),
            "format":          list(self.fmt.classes_),
            "top_countries":   sorted(self.top_countries),
        }


def build_encoders(df: pd.DataFrame) -> Encoders:
    # Collapse countries below top-N into OTHER
    wins_by_country = df.groupby("country")["WINS"].sum().sort_values(ascending=False)
    top_countries = set(wins_by_country.head(TOP_COUNTRIES_N).index)

    df_enc = df.copy()
    df_enc.loc[~df_enc["country"].isin(top_countries), "country"] = "OTHER"

    pub_enc = LabelEncoder().fit(df_enc["pub_id"].astype(str))
    dem_enc = LabelEncoder().fit(df_enc["demand_partner"].astype(str))
    cty_enc = LabelEncoder().fit(df_enc["country"].astype(str))
    fmt_enc = LabelEncoder().fit(df_enc["format"].astype(str))
    return Encoders(pub_enc, dem_enc, cty_enc, fmt_enc, top_countries)


def apply_encoders(df: pd.DataFrame, enc: Encoders) -> pd.DataFrame:
    out = df.copy()
    out.loc[~out["country"].isin(enc.top_countries), "country"] = "OTHER"
    out["pub_id_enc"]         = enc.pub_id.transform(out["pub_id"].astype(str))
    out["demand_partner_enc"] = enc.demand_partner.transform(out["demand_partner"].astype(str))
    out["country_enc"]        = enc.country.transform(out["country"].astype(str))
    out["format_enc"]         = enc.fmt.transform(out["format"].astype(str))
    return out


FEATURE_COLS = [
    "pub_id_enc", "demand_partner_enc", "country_enc", "format_enc",
    "log_wins", "wr", "dow", "margin_30d",
]


# ─────────────────────────────────────────────────────────────────────────────
# Training
# ─────────────────────────────────────────────────────────────────────────────

def _train_quantile_model(X, y, quantile: float, categorical: list[int]) -> lgb.Booster:
    params = {
        "objective":     "quantile",
        "alpha":         quantile,
        "metric":        "quantile",
        "learning_rate": 0.05,
        "num_leaves":    31,
        "min_data_in_leaf": 20,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.8,
        "bagging_freq":  5,
        "verbose":       -1,
    }
    dset = lgb.Dataset(X, label=y, categorical_feature=categorical)
    return lgb.train(params, dset, num_boost_round=200)


def train_models(df_train: pd.DataFrame, enc: Encoders) -> dict:
    df = apply_encoders(df_train, enc)
    X = df[FEATURE_COLS].values
    y = df["log_ecpm"].values
    categorical_idx = [0, 1, 2, 3]   # first four feature columns are categorical

    models = {}
    for q, name in [(0.10, "p10"), (0.50, "p50"), (0.90, "p90")]:
        print(f"[floor_model]   training {name} quantile …", flush=True)
        models[name] = _train_quantile_model(X, y, q, categorical_idx)
    return models


def evaluate(models: dict, df_holdout: pd.DataFrame, enc: Encoders) -> dict:
    if len(df_holdout) == 0:
        return {"n_rows": 0}
    df = apply_encoders(df_holdout, enc)
    X = df[FEATURE_COLS].values
    y_true = df["ecpm"].values

    preds = {name: np.expm1(m.predict(X)) for name, m in models.items()}
    y_pred = preds["p50"]
    y_pred = np.maximum(y_pred, 0.01)
    ape = np.abs(y_pred - y_true) / np.maximum(y_true, 0.01)
    mape = float(np.median(ape))

    # Coverage — what fraction of truths fall inside the p10-p90 band?
    covered = ((y_true >= preds["p10"]) & (y_true <= preds["p90"])).mean()

    # Compare vs. heuristic (global demand_partner mean)
    baseline_ecpm = (
        df_holdout.groupby("demand_partner")["ecpm"].transform("mean").values
    )
    base_ape = np.abs(baseline_ecpm - y_true) / np.maximum(y_true, 0.01)
    baseline_mape = float(np.median(base_ape))

    return {
        "n_rows":        int(len(df_holdout)),
        "median_ape":    mape,
        "baseline_median_ape": baseline_mape,
        "lift_vs_baseline":    (baseline_mape - mape) / max(baseline_mape, 1e-6),
        "p10_p90_coverage":    float(covered),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Prediction surface
# ─────────────────────────────────────────────────────────────────────────────

def build_prediction_table(models: dict, enc: Encoders, df_all: pd.DataFrame) -> pd.DataFrame:
    """For every (pub_id, demand_partner, country) segment active in last 14d,
    predict eCPM using median features from that window."""
    cutoff = df_all["date"].max() - pd.Timedelta(days=14)
    recent = df_all[df_all["date"] >= cutoff].copy()

    # Aggregate — one row per segment
    seg = (
        recent.groupby(["pub_id", "pub_name", "demand_partner", "country", "format"])
              .agg(WINS=("WINS", "sum"),
                   BIDS=("BIDS", "sum"),
                   GROSS_REVENUE=("GROSS_REVENUE", "sum"),
                   margin_30d=("margin_30d", "mean"),
                   ecpm_actual=("ecpm", "mean"))
              .reset_index()
    )
    seg["log_wins"] = np.log1p(seg["WINS"])
    seg["wr"]      = seg["WINS"] / seg["BIDS"].clip(lower=1)
    seg["dow"]     = date.today().weekday()
    seg["log_ecpm"] = 0.0  # not used for prediction

    seg_enc = apply_encoders(seg, enc)
    X = seg_enc[FEATURE_COLS].values

    for name, m in models.items():
        seg_enc[f"pred_{name}"] = np.expm1(m.predict(X))

    seg_enc["predicted_ecpm"] = seg_enc["pred_p50"].clip(lower=0.01)
    seg_enc["predicted_p10"]  = seg_enc["pred_p10"].clip(lower=0.01)
    seg_enc["predicted_p90"]  = seg_enc["pred_p90"].clip(lower=0.01)
    seg_enc["band_width"]     = seg_enc["predicted_p90"] - seg_enc["predicted_p10"]
    seg_enc["relative_band"]  = seg_enc["band_width"] / seg_enc["predicted_ecpm"].clip(lower=0.01)
    seg_enc["confidence"]     = np.where(
        seg_enc["relative_band"] <= CONFIDENCE_BAND_MAX, "high", "low"
    )
    seg_enc["recommended_floor"] = (seg_enc["predicted_ecpm"] * FLOOR_RATIO).round(2)

    return seg_enc[[
        "pub_id", "pub_name", "demand_partner", "country", "format",
        "WINS", "ecpm_actual",
        "predicted_ecpm", "predicted_p10", "predicted_p90",
        "relative_band", "confidence", "recommended_floor",
    ]]


# ─────────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────────

def save_artifacts(models: dict, enc: Encoders, predictions: pd.DataFrame,
                   metrics: dict) -> None:
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump({"models": models, "encoders": enc.to_dict()}, MODEL_PATH)
    predictions.to_json(PREDICTIONS_PATH, orient="records", indent=2)
    with open(METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"[floor_model]   ✓ model   → {MODEL_PATH}")
    print(f"[floor_model]   ✓ preds   → {PREDICTIONS_PATH} ({len(predictions)} segments)")
    print(f"[floor_model]   ✓ metrics → {METRICS_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# Runtime lookup — called from new_partner_optimizer
# ─────────────────────────────────────────────────────────────────────────────

_PREDICTIONS_CACHE: Optional[dict] = None


def _load_predictions() -> dict:
    """Return a nested dict {pub_id: {demand_partner: {country: prediction_row}}}."""
    global _PREDICTIONS_CACHE
    if _PREDICTIONS_CACHE is not None:
        return _PREDICTIONS_CACHE
    if not os.path.exists(PREDICTIONS_PATH):
        _PREDICTIONS_CACHE = {}
        return _PREDICTIONS_CACHE
    try:
        with open(PREDICTIONS_PATH) as f:
            rows = json.load(f)
    except Exception:
        _PREDICTIONS_CACHE = {}
        return _PREDICTIONS_CACHE
    nested: dict = {}
    for r in rows:
        pid = int(r.get("pub_id", 0))
        dp = str(r.get("demand_partner", ""))
        cc = str(r.get("country", ""))
        nested.setdefault(pid, {}).setdefault(dp, {})[cc] = r
    _PREDICTIONS_CACHE = nested
    return nested


def lookup_prediction(pub_id: int, demand_partner: str,
                      country: str = "US") -> Optional[dict]:
    """Return the model's prediction for (pub_id, demand_partner, country).

    Falls back to same pub×demand across countries if the specific country
    isn't present. Returns None if no prediction exists or confidence is low.
    """
    preds = _load_predictions()
    pub_preds = preds.get(int(pub_id), {})
    dp_preds = pub_preds.get(demand_partner, {})
    if not dp_preds:
        return None
    # Try specific country, else US, else pick highest-volume country for that pair
    hit = dp_preds.get(country) or dp_preds.get("US")
    if hit is None:
        # Pick segment with most wins (the dominant geography)
        hit = max(dp_preds.values(), key=lambda r: r.get("WINS", 0))
    if hit.get("confidence") != "high":
        return None
    return hit


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def train_and_predict(days: int = TRAIN_DAYS, holdout_days: int = HOLDOUT_DAYS) -> dict:
    print(f"\n{'='*70}")
    print(f"  Floor Elasticity Model — Train & Predict")
    print(f"{'='*70}\n")

    df = pull_training_data(days)
    if df.empty:
        print("[floor_model] no data — aborting")
        return {}

    cutoff = df["date"].max() - pd.Timedelta(days=holdout_days)
    df_train   = df[df["date"] <  cutoff].copy()
    df_holdout = df[df["date"] >= cutoff].copy()
    print(f"[floor_model]   train rows: {len(df_train):,}   holdout rows: {len(df_holdout):,}")

    enc = build_encoders(df)
    print(f"[floor_model]   {len(enc.pub_id.classes_)} pubs, "
          f"{len(enc.demand_partner.classes_)} demand partners, "
          f"{len(enc.country.classes_)} countries (top {TOP_COUNTRIES_N} + OTHER)")

    models = train_models(df_train, enc)

    print("\n[floor_model] evaluating …")
    metrics = evaluate(models, df_holdout, enc)
    for k, v in metrics.items():
        print(f"    {k}: {v}")

    print("\n[floor_model] generating per-segment predictions …")
    predictions = build_prediction_table(models, enc, df)

    high_conf = (predictions["confidence"] == "high").sum()
    print(f"[floor_model]   {len(predictions)} segments  ({high_conf} high-confidence)")

    # Quick summary by format
    print("\n[floor_model] predicted eCPM by format (high-confidence only):")
    hc = predictions[predictions["confidence"] == "high"]
    if len(hc) > 0:
        summary = hc.groupby("format").agg(
            n=("predicted_ecpm", "count"),
            mean_ecpm=("predicted_ecpm", "mean"),
            mean_floor=("recommended_floor", "mean"),
        ).round(3)
        print(summary.to_string())

    save_artifacts(models, enc, predictions, metrics)
    return metrics


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--days",    type=int, default=TRAIN_DAYS)
    p.add_argument("--holdout", type=int, default=HOLDOUT_DAYS)
    args = p.parse_args()
    train_and_predict(days=args.days, holdout_days=args.holdout)
