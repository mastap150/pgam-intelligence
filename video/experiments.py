"""
video/experiments.py — Experimentation Engine (spec §27).

Structured A/B tests over ONE controlled variable (hook, length, voice,
price-first vs destination-first, map vs no map, CTA vs no CTA). Assignment
is explicit — videos are attached to an arm at production time — and the
conclusion uses Welch's t-test on video scores so we state confidence rather
than eyeballing means. No multi-attribute shotgun tests.
"""

import math

from video import settings
from video.models import Experiment, new_id
from video.store import store

ALLOWED_VARIABLES = [
    "hook", "length", "voice", "opening_order", "map", "cta", "visual_style",
]


def create(name: str, variable: str, variant_a: dict, variant_b: dict) -> dict:
    if variable not in ALLOWED_VARIABLES:
        raise ValueError(f"variable must be one of {ALLOWED_VARIABLES}")
    exp = Experiment(id=new_id("exp"), name=name, variable=variable,
                     variant_a=variant_a, variant_b=variant_b)
    return store().put("experiments", exp.to_record())


def attach_video(experiment_id: str, video_id: str, arm: str) -> dict:
    s = store()
    exp = s.get("experiments", experiment_id)
    if not exp:
        raise KeyError(experiment_id)
    key = {"a": "video_ids_a", "b": "video_ids_b"}[arm.lower()]
    if video_id not in exp[key]:
        exp[key].append(video_id)
    return s.put("experiments", exp)


def _welch(a: list[float], b: list[float]) -> tuple[float, float]:
    """(t_statistic, approx_p_two_sided). Normal approximation on the t stat —
    good enough for a go/no-go at these sample sizes; sample-size floors do
    the real guarding."""
    na, nb = len(a), len(b)
    ma, mb = sum(a) / na, sum(b) / nb
    va = sum((x - ma) ** 2 for x in a) / max(1, na - 1)
    vb = sum((x - mb) ** 2 for x in b) / max(1, nb - 1)
    se = math.sqrt(va / na + vb / nb)
    if se == 0:
        return 0.0, 1.0
    t = (ma - mb) / se
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(t) / math.sqrt(2))))
    return t, p


def conclude(experiment_id: str, min_per_arm: int = 5) -> dict:
    """Evaluate an experiment. Concludes only with enough sample per arm;
    otherwise records status=running with what's known so far."""
    s = store()
    exp = s.get("experiments", experiment_id)
    if not exp:
        raise KeyError(experiment_id)

    def arm_scores(ids):
        return [v["video_score"] for vid in ids
                if (v := s.get("videos", vid)) and v.get("video_score")]

    a, b = arm_scores(exp["video_ids_a"]), arm_scores(exp["video_ids_b"])
    result = {"n_a": len(a), "n_b": len(b)}
    if len(a) >= min_per_arm and len(b) >= min_per_arm:
        t, p = _welch(a, b)
        result.update({
            "mean_a": round(sum(a) / len(a), 1), "mean_b": round(sum(b) / len(b), 1),
            "t": round(t, 2), "p": round(p, 4),
            "winner": ("a" if t > 0 else "b") if p < 0.10 else "inconclusive",
            "confidence": "high" if p < 0.05 else ("moderate" if p < 0.10 else "low"),
        })
        exp["status"] = "concluded"
        settings.log("experiments",
                     f"{exp['name']}: winner={result['winner']} p={result['p']}")
    else:
        result["note"] = f"need ≥{min_per_arm} scored videos per arm"
    exp["result"] = result
    return s.put("experiments", exp)
