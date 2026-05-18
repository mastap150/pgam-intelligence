"""
agents/compliance/scoring.py

Per-publisher daily compliance score derivation.

Score is 100 minus severity-weighted open findings, floored at 0.
Weights are intentionally simple and human-readable:

    critical: -25     (4+ critical findings → score = 0)
    high:     -10
    medium:   -3
    info:     -1

Sentinel rows (publisher_key LIKE '_ssp:%') are SSP-level audits, not
publisher compliance — excluded.

Output: one row per active publisher per as_of date, upserted into
compliance_publisher_scores_daily. Dashboard reads the most recent
as_of for the "publisher risk ranking" table and the trailing 30 days
for trend lines.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from core.neon import connect


SEVERITY_WEIGHTS: dict[str, int] = {
    "critical": 25,
    "high":     10,
    "medium":   3,
    "info":     1,
}


# Pull open finding counts per active publisher; LEFT JOIN so publishers
# with zero findings still get a 100 score row written for the day.
_SCORE_SQL = """
WITH agg AS (
    SELECT
        publisher_key,
        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS open_critical,
        SUM(CASE WHEN severity = 'high'     THEN 1 ELSE 0 END) AS open_high,
        SUM(CASE WHEN severity = 'medium'   THEN 1 ELSE 0 END) AS open_medium,
        SUM(CASE WHEN severity = 'info'     THEN 1 ELSE 0 END) AS open_info
    FROM pgam_direct.compliance_findings
    WHERE status = 'open'
      AND publisher_key NOT LIKE '\\_%' ESCAPE '\\'
    GROUP BY publisher_key
)
SELECT
    cp.publisher_key,
    COALESCE(a.open_critical, 0) AS open_critical,
    COALESCE(a.open_high,     0) AS open_high,
    COALESCE(a.open_medium,   0) AS open_medium,
    COALESCE(a.open_info,     0) AS open_info
FROM pgam_direct.compliance_publishers cp
LEFT JOIN agg a ON a.publisher_key = cp.publisher_key
WHERE cp.is_active = TRUE;
"""


_UPSERT_SQL = """
INSERT INTO pgam_direct.compliance_publisher_scores_daily
    (publisher_key, as_of, compliance_score,
     open_critical, open_high, open_medium, open_info, computed_at)
VALUES
    (%(publisher_key)s, %(as_of)s, %(compliance_score)s,
     %(open_critical)s, %(open_high)s, %(open_medium)s, %(open_info)s, now())
ON CONFLICT (publisher_key, as_of) DO UPDATE SET
    compliance_score = EXCLUDED.compliance_score,
    open_critical    = EXCLUDED.open_critical,
    open_high        = EXCLUDED.open_high,
    open_medium      = EXCLUDED.open_medium,
    open_info        = EXCLUDED.open_info,
    computed_at      = now();
"""


@dataclass(frozen=True)
class ScoreRow:
    publisher_key: str
    compliance_score: float
    open_critical: int
    open_high: int
    open_medium: int
    open_info: int


def compute_score(critical: int, high: int, medium: int, info: int) -> float:
    penalty = (
        SEVERITY_WEIGHTS["critical"] * critical
        + SEVERITY_WEIGHTS["high"]   * high
        + SEVERITY_WEIGHTS["medium"] * medium
        + SEVERITY_WEIGHTS["info"]   * info
    )
    return float(max(0, 100 - penalty))


@dataclass(frozen=True)
class ScoreSummary:
    rows_written: int
    avg_score: float
    publishers_below_75: int


def refresh_publisher_scores(as_of: date | None = None) -> ScoreSummary:
    """Compute and UPSERT scores for every active publisher for `as_of` (default today)."""
    as_of = as_of or date.today()

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_SCORE_SQL)
            rows = [
                ScoreRow(
                    publisher_key=r[0],
                    open_critical=int(r[1] or 0),
                    open_high=int(r[2] or 0),
                    open_medium=int(r[3] or 0),
                    open_info=int(r[4] or 0),
                    compliance_score=compute_score(
                        int(r[1] or 0), int(r[2] or 0), int(r[3] or 0), int(r[4] or 0),
                    ),
                )
                for r in cur.fetchall()
            ]

    if not rows:
        return ScoreSummary(rows_written=0, avg_score=0.0, publishers_below_75=0)

    payload = [
        {
            "publisher_key":    r.publisher_key,
            "as_of":            as_of,
            "compliance_score": r.compliance_score,
            "open_critical":    r.open_critical,
            "open_high":        r.open_high,
            "open_medium":      r.open_medium,
            "open_info":        r.open_info,
        }
        for r in rows
    ]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL, payload)
        conn.commit()

    avg = sum(r.compliance_score for r in rows) / len(rows)
    below_75 = sum(1 for r in rows if r.compliance_score < 75)
    return ScoreSummary(
        rows_written=len(rows),
        avg_score=round(avg, 2),
        publishers_below_75=below_75,
    )
