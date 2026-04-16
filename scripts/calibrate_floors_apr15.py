"""
scripts/calibrate_floors_apr15.py

Manual floor calibration — Apr 15, 2026.

Applies strategic per-demand-partner floors based on 7-day avg bid analysis.
Uses force=True to bypass the 25% step cap (initial conservative floors were
set without demand data; now we have it).

Smaato - Magnite:
  Pubmatic  $0.20 → $0.35  (avg bid $0.471, filter junk)
  Magnite   $0.20 → $0.75  (avg bid $1.734)
  Xandr     $0.20 → $1.50  (avg bid $9.521, 29.9% WR) [demand: Xandr - Smaato 9 Dots US West]

Illumin Display & Video:
  Verve     $0.25 → $0.25  (no change — $2,170/week at $0.333 avg, preserve)
  Unruly    $0.25 → $0.80  (avg bid $1.655)
  Stirista  $0.25 → $1.50  (avg bid $3.535, 55.2% WR — biggest opportunity)
  Magnite   $0.25 → $0.50  (avg bid $0.832)
  Sovrn     $0.25 → $0.30  (avg bid $0.405)
  Xandr     $0.25 → $1.50  (avg bid $4.827) [demand: Xandr - Illumin 9 Dots]
  Pubmatic  no change       (avg bid $0.294, too close to floor)

Illumin In App:
  Magnite   $0.20 → $0.75  (avg bid $3.782)
  Pubmatic  $0.20 → $0.30  (avg bid $0.401)
  Unruly    $0.20 → $0.35  (avg bid $0.533)
  Illumin   $0.20 → $0.50  (avg bid $1.060)
  Xandr     $0.20 → $1.00  (avg bid $4.550) [demand: Xandr - Illumin 9 Dots]
  Kueez     $0.20 → $0.50  (avg bid $1.291, 22.2% WR)

Smaato - Interstitial:
  Disable all demand seats ($0.02 revenue in 7 days — broken integration)
"""

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, '.env'))

from scripts.pilot_actions import apply_floor_change, disable_demand_seat

DRY_RUN = False  # set True to preview only

REASON = "Manual calibration Apr-15: per-demand floors based on 7d avg bid analysis"


def _change(pub: str, demand: str, new_floor: float):
    try:
        r = apply_floor_change(
            publisher_name=pub,
            demand_name=demand,
            new_floor=new_floor,
            reason=REASON,
            dry_run=DRY_RUN,
            force=True,
        )
        status = "✓ APPLIED" if r.get("applied") else ("DRY_RUN" if DRY_RUN else "✗ NOT APPLIED")
        print(f"  {status}  {pub!r} / {demand!r}  {r.get('old_floor', '?')} → {new_floor}")
    except Exception as e:
        print(f"  ✗ ERROR  {pub!r} / {demand!r}: {e}")


def _disable(pub: str, demand: str):
    try:
        r = disable_demand_seat(
            publisher_name=pub,
            demand_name=demand,
            reason="Smaato Interstitial disabled — $0.02 revenue in 7d, broken integration",
            dry_run=DRY_RUN,
        )
        status = "✓ DISABLED" if r.get("applied") else ("DRY_RUN" if DRY_RUN else "✗ NOT APPLIED")
        print(f"  {status}  {pub!r} / {demand!r}")
    except Exception as e:
        print(f"  ✗ ERROR  {pub!r} / {demand!r}: {e}")


def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"\n=== Floor Calibration Apr-15 [{mode}] ===\n")

    # ── Smaato - Magnite ───────────────────────────────────────────────────
    print("Smaato - Magnite:")
    _change("Smaato - Magnite ",  "Pubmatic",   0.35)
    _change("Smaato - Magnite ",  "Magnite",    0.75)
    _change("Smaato - Magnite ",  "Xandr - Smaato", 1.50)
    # Sovrn left at $0.20 — already set, no meaningful revenue anyway

    # ── Illumin Display & Video ────────────────────────────────────────────
    print("\nIllumin Display & Video:")
    # Verve: no change (avg bid $0.333, main revenue source — keep $0.25)
    _change("Illumin Display & Video", "Unruly",    0.80)
    _change("Illumin Display & Video", "Stirista",  1.50)
    _change("Illumin Display & Video", "Magnite",   0.50)
    _change("Illumin Display & Video", "Sovrn",     0.30)
    _change("Illumin Display & Video", "Xandr - Illumin", 1.50)
    # Pubmatic: no change (avg bid $0.294, already near floor)

    # ── Illumin In App ─────────────────────────────────────────────────────
    print("\nIllumin In App:")
    _change("Illumin In App", "Magnite",   0.75)
    _change("Illumin In App", "Pubmatic",  0.30)
    _change("Illumin In App", "Unruly",    0.35)
    _change("Illumin In App", "Illumin",   0.50)
    _change("Illumin In App", "Xandr - Illumin", 1.00)
    _change("Illumin In App", "Kueez",     0.50)

    # ── Illumin In App - TEST (same floors) ────────────────────────────────
    print("\nIllumin In App - TEST:")
    _change("Illumin In App - TEST", "Magnite",    0.75)
    _change("Illumin In App - TEST", "Pubmatic",   0.30)
    _change("Illumin In App - TEST", "Unruly",     0.35)
    _change("Illumin In App - TEST", "Illumin",    0.50)
    _change("Illumin In App - TEST", "Xandr - Illumin", 1.00)
    _change("Illumin In App - TEST", "Kueez",      0.50)

    # ── Smaato - Interstitial: disable all demand seats ────────────────────
    print("\nSmaato - Interstitial (disabling all — $0.02/week revenue):")
    for demand in ["Unruly", "Pubmatic", "Sovrn", "Illumin", "Magnite"]:
        _disable("Smaato - Interstitial ", demand)

    print(f"\n=== Calibration complete [{mode}] ===\n")


if __name__ == "__main__":
    main()
