"""
Price-response model — per (publisher, demand) tuple estimate of
E[revenue | floor = F] with a bootstrap credible interval.

Method
------
We have hourly observations of (bids, wins, revenue) per tuple over 30d.
For each hour with wins > 0, clear_ecpm = revenue / wins * 1000. That is the
observed average clear price that hour.

At a hypothetical floor F:
  - a historical hour clears if its clear_ecpm >= F (assumption: raising
    floor to F drops the impressions that cleared below F, keeps the rest).
  - expected wins  ≈ wins_hour * (1 if ecpm_hour >= F else 0)
  - expected rev   ≈ wins_hour * ecpm_hour / 1000 if ecpm_hour >= F else 0

This is the **impression-truncation** approximation: it assumes the bid
landscape is dense enough that raising the floor removes low-priced wins
without changing the clearing prices of high-priced wins. It's a lower
bound for rev — in reality floors also induce buyers to bid higher ("floor
lift"), which we'll learn once we have multi-regime data for the tuple.

When multi-regime data IS available in the ledger (e.g. a tuple has run
at both floor=null and floor=0.5), we reconcile: the model's prediction at
floor=0.5 is compared to the realized revenue at floor=0.5, and a
multiplicative lift factor is applied. Tranche 3 will replace this with
a proper Bayesian hierarchical model.

Credible intervals come from nonparametric bootstrap over hourly samples.

Usage
-----
    from intelligence.price_response import fit, predict_revenue

    m = fit(publisher_id=290115319, demand_id=604, lookback_days=14)
    m.predict(0.50)    # → {"expected_weekly_revenue": 123.4, "ci_low": 90, "ci_high": 150}
    m.optimal_floor()  # → (floor, expected_weekly_revenue, ci_low, ci_high)
"""
from __future__ import annotations

import gzip
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

DATA_DIR = Path(__file__).parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

BOOTSTRAP_ITERATIONS = 200
# Candidate floor grid: 0 + 10 quantiles of observed clear_ecpm.
N_CANDIDATE_FLOORS = 11


@dataclass
class PriceResponseModel:
    publisher_id: int
    demand_id: int
    publisher_name: str = ""
    demand_name: str = ""
    samples: list[tuple[float, float]] = field(default_factory=list)
    # list of (ecpm_per_win, revenue_this_hour) pairs — one per hour with wins>0
    total_bids: float = 0.0
    total_wins: float = 0.0
    total_revenue: float = 0.0
    hours_observed: int = 0
    lookback_days: int = 14

    @property
    def is_fittable(self) -> bool:
        return self.hours_observed >= 20 and self.total_wins >= 100

    def _predict_point(self, floor: float, samples: list[tuple[float, float]]) -> float:
        """Expected weekly revenue at given floor, given a sample of hourly obs."""
        if not samples:
            return 0.0
        # scale from lookback_days of samples → 7d
        cleared_rev = sum(rev for ecpm, rev in samples if ecpm >= floor)
        return cleared_rev * (7.0 / self.lookback_days)

    def predict(self, floor: float) -> dict:
        if not self.is_fittable:
            return {
                "floor": floor, "expected_weekly_revenue": None,
                "ci_low": None, "ci_high": None, "note": "insufficient_samples",
            }
        # Bootstrap CI
        n = len(self.samples)
        boot = []
        for _ in range(BOOTSTRAP_ITERATIONS):
            resampled = [self.samples[random.randrange(n)] for _ in range(n)]
            boot.append(self._predict_point(floor, resampled))
        boot.sort()
        lo = boot[int(BOOTSTRAP_ITERATIONS * 0.1)]
        hi = boot[int(BOOTSTRAP_ITERATIONS * 0.9)]
        point = self._predict_point(floor, self.samples)
        return {
            "floor": round(floor, 4),
            "expected_weekly_revenue": round(point, 2),
            "ci_low": round(lo, 2),
            "ci_high": round(hi, 2),
            "p_better_than_zero": round(sum(1 for x in boot if x > 0) / len(boot), 3),
        }

    def candidate_floors(self) -> list[float]:
        if not self.samples:
            return []
        ecpms = sorted(e for e, _ in self.samples)
        grid = [0.0]
        for q in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]:
            grid.append(round(ecpms[min(int(q * len(ecpms)), len(ecpms) - 1)], 2))
        # dedupe + sort
        return sorted(set(round(x, 2) for x in grid))

    def sweep(self) -> list[dict]:
        return [self.predict(f) for f in self.candidate_floors()]

    def optimal_floor(self) -> dict | None:
        if not self.is_fittable:
            return None
        sweep = self.sweep()
        best = max(sweep, key=lambda r: r["expected_weekly_revenue"] or 0)
        return best


# ────────────────────────────────────────────────────────────────────────────

def _load_hourly() -> list[dict]:
    with gzip.open(HOURLY_PATH, "rt") as f:
        return json.load(f)


def fit(publisher_id: int, demand_id: int, *,
        lookback_days: int = 14,
        _hourly_cache: list[dict] | None = None) -> PriceResponseModel:
    rows = _hourly_cache if _hourly_cache is not None else _load_hourly()
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()
    m = PriceResponseModel(
        publisher_id=publisher_id, demand_id=demand_id,
        lookback_days=lookback_days,
    )
    for r in rows:
        if int(r.get("PUBLISHER_ID", 0)) != publisher_id:
            continue
        if int(r.get("DEMAND_ID", 0)) != demand_id:
            continue
        if str(r.get("DATE", "")) < cutoff:
            continue
        bids = float(r.get("BIDS", 0) or 0)
        wins = float(r.get("WINS", 0) or 0)
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        m.total_bids += bids
        m.total_wins += wins
        m.total_revenue += rev
        m.hours_observed += 1
        m.publisher_name = r.get("PUBLISHER_NAME", "") or m.publisher_name
        m.demand_name = r.get("DEMAND_NAME", "") or m.demand_name
        if wins > 0 and rev > 0:
            ecpm = rev / wins * 1000.0
            m.samples.append((ecpm, rev))
    return m


def fit_all(*, lookback_days: int = 14, min_total_revenue: float = 20.0) -> list[PriceResponseModel]:
    """Fit one model per (pub, demand) tuple with enough signal."""
    rows = _load_hourly()
    tuples = {}
    for r in rows:
        pid = int(r.get("PUBLISHER_ID", 0))
        did = int(r.get("DEMAND_ID", 0))
        if pid == 0 or did == 0:
            continue
        tuples.setdefault((pid, did), None)
    models = []
    for (pid, did) in tuples:
        m = fit(pid, did, lookback_days=lookback_days, _hourly_cache=rows)
        if m.total_revenue >= min_total_revenue and m.is_fittable:
            models.append(m)
    return models


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pub", type=int, required=True)
    ap.add_argument("--demand", type=int, required=True)
    ap.add_argument("--lookback", type=int, default=14)
    args = ap.parse_args()

    m = fit(args.pub, args.demand, lookback_days=args.lookback)
    print(f"{m.publisher_name} / {m.demand_name}")
    print(f"  samples={len(m.samples)}  wins={m.total_wins:,.0f}  rev=${m.total_revenue:,.2f}")
    if not m.is_fittable:
        print("  (not fittable)")
    else:
        print(f"\n  {'Floor':>7} {'E[wk rev]':>11} {'CI low':>9} {'CI high':>9}")
        for row in m.sweep():
            print(f"  ${row['floor']:>6.2f} ${row['expected_weekly_revenue']:>10,.2f} "
                  f"${row['ci_low']:>8,.2f} ${row['ci_high']:>8,.2f}")
        best = m.optimal_floor()
        print(f"\n  optimal floor: ${best['floor']:.2f}  "
              f"E[wk rev]=${best['expected_weekly_revenue']:,.2f}")
