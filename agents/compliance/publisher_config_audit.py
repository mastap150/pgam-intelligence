"""
agents/compliance/publisher_config_audit.py

Phase 4 — Tier A schain audit: verify that every ACTIVE publisher_config
row in pgam_direct's bidder-edge configuration has schain_asi set to
"pgamssp.com" (the canonical PGAM node ASI).

Background
----------
The bidder-edge service (services/bidder-edge) injects PGAM as the
terminal node in the outbound source.schain on every bid request. The
ASI value comes from:

    OurSchainByTenant[tenant_id] →
        ASI: getenvOr("PGAM_BOOTSTRAP_OUR_SCHAIN_ASI", "pgamssp.com")

…and is overridden per-publisher via pgam_direct.publisher_configs.schain_asi.
If a row in publisher_configs has schain_asi = NULL or a value other than
"pgamssp.com", every bid emitted for that publisher will carry the wrong
ASI in source.schain.nodes[] — meaning DSPs auditing supply paths through
PGAM won't see us in the chain even though the rest of the declaration
(ads.txt, sellers.json) is right.

This is the *configured intent* check. A separate Tier B
(agents/compliance/dynamic_schain.py + a new schain_asis column on
ClickHouse auction_events) would prove what's actually emitted, but
catching configured mismatches first is cheap and covers ~99 % of the
real failure mode.

Findings produced
-----------------
schain.publisher_config_asi_mismatch    CRITICAL
    Active publisher_config has schain_asi != "pgamssp.com".

schain.publisher_config_asi_null        CRITICAL
    Active publisher_config has schain_asi = NULL — bidder will fall
    through to PGAM_BOOTSTRAP_OUR_SCHAIN_ASI which IS "pgamssp.com" by
    default, but relying on the bootstrap default is fragile (anyone
    setting PGAM_BOOTSTRAP_OUR_SCHAIN_ASI in Vercel env to a different
    value silently breaks this publisher). Flag explicitly.

Sentinel publisher_key: `_pub_config:<id>` — keeps these findings
separate from LL-pub findings (`_ll_pub:`) and entity audits (`dom:` /
`app:`). Excluded from publisher compliance scoring.
"""
from __future__ import annotations

from dataclasses import dataclass

from core.neon import connect

from agents.compliance.validators.adstxt_universal import Finding


EXPECTED_ASI = "pgamssp.com"


_LOAD_SQL = """
SELECT id, name, status, tenant_id, schain_asi
FROM pgam_direct.publisher_configs
WHERE status = 'active';
"""


@dataclass(frozen=True)
class PublisherConfigStats:
    skipped_reason: str | None
    total_active: int
    correct_asi: int
    null_asi: int
    mismatch_asi: int
    findings_count: int


def _sentinel(pub_id: object) -> str:
    return f"_pub_config:{pub_id}"


def _make_mismatch(row: dict, observed: str) -> Finding:
    return Finding.make(
        publisher_key=_sentinel(row["id"]),
        check_id="schain.publisher_config_asi_mismatch",
        severity="critical",
        detail={
            "publisher_config_id":   row["id"],
            "publisher_name":        row.get("name"),
            "tenant_id":             row.get("tenant_id"),
            "expected_asi":          EXPECTED_ASI,
            "observed_asi":          observed,
            "fix":                   (
                "UPDATE pgam_direct.publisher_configs "
                f"SET schain_asi = '{EXPECTED_ASI}' "
                f"WHERE id = '{row['id']}';"
            ),
        },
        fingerprint_extra=str(observed),
    )


def _make_null(row: dict) -> Finding:
    return Finding.make(
        publisher_key=_sentinel(row["id"]),
        check_id="schain.publisher_config_asi_null",
        severity="critical",
        detail={
            "publisher_config_id": row["id"],
            "publisher_name":      row.get("name"),
            "tenant_id":           row.get("tenant_id"),
            "expected_asi":        EXPECTED_ASI,
            "fix": (
                "UPDATE pgam_direct.publisher_configs "
                f"SET schain_asi = '{EXPECTED_ASI}' "
                f"WHERE id = '{row['id']}';"
            ),
        },
    )


def run_publisher_config_schain_audit(
) -> tuple[PublisherConfigStats, list[Finding], list[str]]:
    """Query publisher_configs and emit findings for any wrong/null ASI.

    Degrades cleanly when the table doesn't exist or the bidder-edge
    isn't deployed yet — returns skipped_reason without raising.
    """
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_LOAD_SQL)
                cols = [c.name for c in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        return (
            PublisherConfigStats(
                skipped_reason=f"publisher_configs query failed: {exc}",
                total_active=0, correct_asi=0, null_asi=0,
                mismatch_asi=0, findings_count=0,
            ),
            [], [],
        )

    if not rows:
        return (
            PublisherConfigStats(
                skipped_reason="no active publisher_configs",
                total_active=0, correct_asi=0, null_asi=0,
                mismatch_asi=0, findings_count=0,
            ),
            [], [],
        )

    findings: list[Finding] = []
    correct = null_count = mismatch = 0
    sentinel_keys: list[str] = []
    for row in rows:
        sentinel_keys.append(_sentinel(row["id"]))
        observed = row.get("schain_asi")
        if observed is None or observed == "":
            null_count += 1
            findings.append(_make_null(row))
        elif observed.strip().lower() != EXPECTED_ASI:
            mismatch += 1
            findings.append(_make_mismatch(row, observed.strip()))
        else:
            correct += 1

    stats = PublisherConfigStats(
        skipped_reason=None,
        total_active=len(rows),
        correct_asi=correct,
        null_asi=null_count,
        mismatch_asi=mismatch,
        findings_count=len(findings),
    )
    return stats, findings, sentinel_keys
