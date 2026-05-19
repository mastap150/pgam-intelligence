"""
agents/compliance/findings.py

Neon UPSERT + auto-resolve helpers for compliance findings.

Idempotency contract
--------------------
- A finding is uniquely keyed by (publisher_key, check_id, fingerprint).
- Re-observing a finding refreshes last_observed_at + flips status back
  to 'open' if it had been auto-resolved (a regression).
- Findings present last run but absent this run are auto-resolved
  for the publishers we actually scanned this run — we never resolve
  a finding for a publisher whose ads.txt was unreachable, because
  "I couldn't see it" is not the same as "it's fixed".
"""
from __future__ import annotations

import json
from collections.abc import Iterable

from core.neon import connect

from agents.compliance.validators.adstxt_universal import Finding


_UPSERT_SQL = """
INSERT INTO pgam_direct.compliance_findings
    (publisher_key, category, check_id, severity, fingerprint, detail,
     first_observed_at, last_observed_at, status)
VALUES
    (%(publisher_key)s, %(category)s, %(check_id)s, %(severity)s,
     %(fingerprint)s, %(detail)s::jsonb,
     now(), now(), 'open')
ON CONFLICT (publisher_key, check_id, fingerprint) DO UPDATE SET
    severity         = EXCLUDED.severity,
    detail           = EXCLUDED.detail,
    last_observed_at = now(),
    status           = CASE
                          WHEN pgam_direct.compliance_findings.status = 'suppressed'
                            THEN 'suppressed'
                          ELSE 'open'
                       END,
    resolved_at      = NULL
RETURNING (xmax = 0) AS inserted;
"""

# Auto-resolve any finding for a scanned publisher that didn't fire this
# run. We resolve only within the (publisher_key, observed-this-run check_ids)
# set so we never blanket-clear a publisher whose entire scan failed.
_RESOLVE_SQL = """
UPDATE pgam_direct.compliance_findings
SET status = 'resolved',
    resolved_at = now()
WHERE publisher_key = ANY(%(scanned)s)
  AND status = 'open'
  AND (publisher_key, check_id, fingerprint) NOT IN (
      SELECT * FROM unnest(%(seen_pubs)s::text[],
                           %(seen_checks)s::text[],
                           %(seen_fps)s::text[])
  )
RETURNING finding_id;
"""


def upsert_findings(findings: Iterable[Finding]) -> tuple[int, int]:
    """Upsert findings. Returns (opened_or_regressed, total_seen)."""
    rows = [
        {
            "publisher_key": f.publisher_key,
            "category":      f.category,
            "check_id":      f.check_id,
            "severity":      f.severity,
            "fingerprint":   f.fingerprint,
            "detail":        json.dumps(f.detail, sort_keys=True),
        }
        for f in findings
    ]
    if not rows:
        return 0, 0

    opened = 0
    with connect() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(_UPSERT_SQL, r)
                inserted = cur.fetchone()
                if inserted and inserted[0]:
                    opened += 1
        conn.commit()
    return opened, len(rows)


def resolve_cleared(
    scanned_publishers: list[str],
    seen: Iterable[tuple[str, str, str]],
) -> int:
    """Auto-resolve open findings that weren't re-observed for scanned pubs.

    `seen` is the iterable of (publisher_key, check_id, fingerprint)
    that DID fire this run.
    """
    seen_list = list(seen)
    if not scanned_publishers:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_RESOLVE_SQL, {
                "scanned":     scanned_publishers,
                "seen_pubs":   [s[0] for s in seen_list],
                "seen_checks": [s[1] for s in seen_list],
                "seen_fps":    [s[2] for s in seen_list],
            })
            resolved = cur.rowcount or 0
        conn.commit()
    return resolved
