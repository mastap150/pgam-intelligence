"""
scripts/train_floor_model.py

Weekly retrain of the floor elasticity model. Wrapper around
intelligence.floor_model.train_and_predict so the scheduler can pick it up
via `_import("scripts.train_floor_model")`.

Also posts a Slack summary of the retrain outcome so model drift is visible.
"""

from __future__ import annotations

import json
import os
import sys
import traceback

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, ".env"), override=True)


def run() -> None:
    """Scheduler entry point — retrain model + post Slack summary."""
    try:
        from intelligence.floor_model import train_and_predict
        metrics = train_and_predict()
    except Exception:
        err = traceback.format_exc()
        print(f"[train_floor_model] FAILED:\n{err}")
        try:
            from core.slack import post_message
            post_message(f"🤖 *Floor Model Retrain* — ❌ failed\n```{err[-800:]}```")
        except Exception:
            pass
        return

    try:
        from core.slack import post_message
        msg = [
            "🤖 *Floor Model Retrain* — ✓ complete",
            f"  holdout rows:       {metrics.get('n_rows', 0):,}",
            f"  median APE:         {metrics.get('median_ape', 0)*100:.1f}%",
            f"  baseline median APE: {metrics.get('baseline_median_ape', 0)*100:.1f}%",
            f"  lift vs baseline:   {metrics.get('lift_vs_baseline', 0)*100:.1f}%",
            f"  p10–p90 coverage:   {metrics.get('p10_p90_coverage', 0)*100:.1f}%",
        ]
        post_message("\n".join(msg))
    except Exception:
        pass


if __name__ == "__main__":
    run()
