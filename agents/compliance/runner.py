"""
agents/compliance/runner.py

Supply Compliance & Quality Intelligence agent — Phase 1 orchestrator.

Daily run:
  1. Fetch PGAM sellers.json -> rebuild compliance_publishers universe
  2. Ensure schema (idempotent CREATE TABLE IF NOT EXISTS)
  3. For each publisher: fetch ads.txt (+ app-ads.txt if present)
     and persist fetch metadata
  4. Validate the universal `pgamssp.com, <seller_id>, DIRECT` line
  5. UPSERT findings; auto-resolve clears
  6. Post a daily Slack digest (deduped per UTC date)
  7. Write a row to compliance_runs

Gating
------
PGAM_COMPLIANCE_ENABLED=1 must be set for the scheduler to fire this.
PGAM_COMPLIANCE_LIMIT=N to scan only the first N publishers (dev/staging).
PGAM_COMPLIANCE_APP_ADS_TXT=1 to also fetch and validate app-ads.txt.
PGAM_COMPLIANCE_RATE_HZ (default 2.0) requests per second across all hosts.
"""
from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(dotenv_path=str(_REPO_ROOT / ".env"), override=True)

from core.neon import connect  # noqa: E402

from agents.compliance.activity_filter import (  # noqa: E402
    load_active_publisher_keys,
    refresh_partner_activity,
)
from agents.compliance.audit_matrix import (  # noqa: E402
    build_audit_matrix,
    persist_matrix,
    rows_to_csv,
)
from agents.compliance.supply_path_audit import (  # noqa: E402
    build_supply_path_audit,
    persist_supply_path,
)
from agents.compliance.supply_partner_audit import (  # noqa: E402
    run_supply_partner_audit,
)
from agents.compliance.block_list import refresh_block_list  # noqa: E402
from agents.compliance.crawlers.adstxt import AdsTxtFetch, fetch_adstxt  # noqa: E402
from agents.compliance.demand_detector import run_demand_detector  # noqa: E402
from agents.compliance.dynamic_schain import run_dynamic_schain_audit  # noqa: E402
from agents.compliance.inventory_roundtrip_audit import (  # noqa: E402
    run_inventory_roundtrip_audit,
)
from agents.compliance.publisher_config_audit import (  # noqa: E402
    run_publisher_config_schain_audit,
)
from agents.compliance.crawlers.downstream_sellersjson import (  # noqa: E402
    fetch_downstream_sellers_json,
)
from agents.compliance.entity_audit import run_entity_audit  # noqa: E402
from agents.compliance.findings import resolve_cleared, upsert_findings  # noqa: E402
from agents.compliance.ll_bridge import run_bridge  # noqa: E402
from agents.compliance.observed_monetization import (  # noqa: E402
    load_observed_for_publishers,
    refresh_observed_monetization,
)
from agents.compliance.reporters.slack_digest import post_digest  # noqa: E402
from agents.compliance.schain_audit import run_schain_audit  # noqa: E402
from agents.compliance.scoring import refresh_publisher_scores  # noqa: E402
from agents.compliance.ssp_registry import PHASE_2_SSP_EXPECTATIONS  # noqa: E402
from agents.compliance.universe import (  # noqa: E402
    Publisher, build_full_registry, build_universe, sync_universe,
)
from agents.compliance.validators.adstxt_resellers import (  # noqa: E402
    validate_resellers_for_publisher,
)
from agents.compliance.validators.adstxt_universal import (  # noqa: E402
    Finding,
    validate_universal_direct,
)
from agents.compliance.validators.sellersjson_downstream import (  # noqa: E402
    validate_downstream_sellersjson,
)

ACTOR = "compliance_runner"
MIGRATION_PATHS = (
    _REPO_ROOT / "migrations" / "2026_05_17_compliance.sql",
    _REPO_ROOT / "migrations" / "2026_05_17_compliance_phase2.sql",
    _REPO_ROOT / "migrations" / "2026_05_17_compliance_phase3.sql",
    _REPO_ROOT / "migrations" / "2026_05_18_compliance_phase5.sql",
    _REPO_ROOT / "migrations" / "2026_05_18_compliance_partner_activity.sql",
    _REPO_ROOT / "migrations" / "2026_05_18_compliance_adstxt_cache.sql",
    _REPO_ROOT / "migrations" / "2026_05_28_compliance_ll_bridge_many_to_one.sql",
    _REPO_ROOT / "migrations" / "2026_05_28_compliance_observed_demands.sql",
    _REPO_ROOT / "migrations" / "2026_05_29_compliance_entity_ssp_audit.sql",
    _REPO_ROOT / "migrations" / "2026_05_29_compliance_supply_path_audit.sql",
    _REPO_ROOT / "migrations" / "2026_06_01_compliance_runs_ok_nullable.sql",
    _REPO_ROOT / "migrations" / "2026_06_02_compliance_path_block_list.sql",
    _REPO_ROOT / "migrations" / "2026_06_02_supply_path_schain_column.sql",
)


# ── Schema bootstrap ─────────────────────────────────────────────────────────


def _ensure_schema() -> None:
    """Run all compliance migrations idempotently, in order."""
    for path in MIGRATION_PATHS:
        if not path.exists():
            print(f"[{ACTOR}] WARNING: migration file missing: {path}")
            continue
        sql = path.read_text()
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()


# ── Crawl + persist fetch metadata ───────────────────────────────────────────


_FETCH_INSERT_SQL = """
INSERT INTO pgam_direct.compliance_adstxt_fetches
    (publisher_key, variant, fetched_at, http_status,
     body_sha256, line_count, error)
VALUES
    (%(publisher_key)s, %(variant)s, now(), %(http_status)s,
     %(body_sha256)s, %(line_count)s, %(error)s);
"""


def _persist_fetches(fetches: list[AdsTxtFetch]) -> None:
    if not fetches:
        return
    rows = [
        {
            "publisher_key": f.publisher_key,
            "variant":       f.variant,
            "http_status":   f.http_status,
            "body_sha256":   f.body_sha256,
            "line_count":    len(f.lines),
            "error":         f.error,
        }
        for f in fetches
    ]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_FETCH_INSERT_SQL, rows)
        conn.commit()


def _crawl_publisher(pub: Publisher, app_ads: bool) -> list[AdsTxtFetch]:
    """Fetch ads.txt (and optionally app-ads.txt) for a single publisher."""
    out: list[AdsTxtFetch] = [
        fetch_adstxt(pub.publisher_key, pub.domain, variant="ads.txt", use_cache=True),
    ]
    if app_ads:
        out.append(fetch_adstxt(pub.publisher_key, pub.domain, variant="app-ads.txt", use_cache=True))
    return out


def _crawl_all(
    publishers: list[Publisher],
    *,
    app_ads: bool,
    rate_hz: float,
    workers: int = 6,
) -> list[AdsTxtFetch]:
    """Crawl publishers in parallel with a global rate cap."""
    if not publishers:
        return []
    min_interval = 1.0 / max(rate_hz, 0.1)
    next_slot = [time.monotonic()]

    def _gated(pub: Publisher) -> list[AdsTxtFetch]:
        wait_until = next_slot[0]
        now = time.monotonic()
        if now < wait_until:
            time.sleep(wait_until - now)
        next_slot[0] = max(time.monotonic(), wait_until) + min_interval
        return _crawl_publisher(pub, app_ads=app_ads)

    results: list[AdsTxtFetch] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_gated, p): p for p in publishers}
        for fut in as_completed(futures):
            try:
                results.extend(fut.result())
            except Exception as exc:
                pub = futures[fut]
                print(f"[{ACTOR}] crawl error pub={pub.publisher_key}: {exc}")
    return results


# ── Validation pass ──────────────────────────────────────────────────────────


def _validate_all(
    publishers: list[Publisher],
    fetches: list[AdsTxtFetch],
    *,
    app_ads: bool,
    enable_resellers: bool,
    active_keys: set[str] | None = None,
) -> list[Finding]:
    """Phase 1 + Phase 2 validation.

    `active_keys`: if provided, restrict validation to these publisher_keys.
    The runner populates this with the set of partners showing trailing-7d
    revenue activity on LL — partners outside the set are skipped to avoid
    firing critical alerts on stale sellers.json entries. If None, all
    publishers are validated (legacy behaviour).
    """
    pub_by_key: dict[str, Publisher] = {p.publisher_key: p for p in publishers}
    fetch_by_pub: dict[str, dict[str, AdsTxtFetch]] = {}
    for f in fetches:
        fetch_by_pub.setdefault(f.publisher_key, {})[f.variant] = f

    observed_by_pub: dict = {}
    if enable_resellers and fetch_by_pub:
        observed_by_pub = load_observed_for_publishers(list(fetch_by_pub.keys()))

    findings: list[Finding] = []
    for pub_key, variants in fetch_by_pub.items():
        pub = pub_by_key.get(pub_key)
        if pub is None:
            continue
        # Activity gate — partners with no trailing-7d LL revenue are skipped.
        # See agents/compliance/activity_filter.py.
        if active_keys is not None and pub_key not in active_keys:
            continue
        ads = variants.get("ads.txt")
        if ads is not None:
            findings.extend(validate_universal_direct(pub_key, pub.seller_id, ads))
            if enable_resellers:
                obs = observed_by_pub.get(pub_key, [])
                if obs:
                    findings.extend(validate_resellers_for_publisher(pub_key, ads, obs))
        if app_ads:
            aa = variants.get("app-ads.txt")
            if aa is not None and aa.http_status == 200:
                findings.extend(validate_universal_direct(pub_key, pub.seller_id, aa))
                # Reseller validation against app-ads.txt mirrors ads.txt;
                # only run when the file actually carries content.
                if enable_resellers:
                    obs = observed_by_pub.get(pub_key, [])
                    if obs:
                        findings.extend(validate_resellers_for_publisher(pub_key, aa, obs))
    return findings


# ── Downstream sellers.json audit (Phase 3) ──────────────────────────────────


_DOWNSTREAM_FETCH_INSERT_SQL = """
INSERT INTO pgam_direct.compliance_downstream_sellersjson_fetches
    (ssp_key, url, fetched_at, http_status, body_sha256,
     seller_count, pgam_seat_found, error)
VALUES
    (%(ssp_key)s, %(url)s, now(), %(http_status)s, %(body_sha256)s,
     %(seller_count)s, %(pgam_seat_found)s, %(error)s);
"""


def _audit_downstream_ssps() -> tuple[list[Finding], list[str]]:
    """Fetch + validate each SSP's sellers.json. Persists fetch metadata.

    Returns (findings, sentinel_publisher_keys). Sentinel keys are the
    `_ssp:<key>` strings used for auto-resolve so cleared findings flip
    to resolved on the next clean run.
    """
    all_findings: list[Finding] = []
    sentinel_keys: list[str] = []
    fetch_rows: list[dict] = []

    for exp in PHASE_2_SSP_EXPECTATIONS:
        sentinel_keys.append(f"_ssp:{exp.ssp_key}")
        try:
            df = fetch_downstream_sellers_json(exp)
        except Exception as exc:
            print(f"[{ACTOR}] downstream sellers.json fetch failed for "
                  f"{exp.ssp_key}: {exc}")
            continue

        pgam_seat_found: bool | None = None
        if df.ok:
            pgam_seat_found = any(
                str(s.get("seller_id") or "") == str(exp.account_id)
                for s in df.sellers
            )

        fetch_rows.append({
            "ssp_key":         exp.ssp_key,
            "url":             df.url,
            "http_status":     df.http_status,
            "body_sha256":     df.body_sha256,
            "seller_count":    df.seller_count,
            "pgam_seat_found": pgam_seat_found,
            "error":           df.error,
        })

        all_findings.extend(validate_downstream_sellersjson(exp, df))

    if fetch_rows:
        try:
            with connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany(_DOWNSTREAM_FETCH_INSERT_SQL, fetch_rows)
                conn.commit()
        except Exception as exc:
            print(f"[{ACTOR}] downstream fetch persistence failed: {exc}")

    return all_findings, sentinel_keys


# ── Run log ──────────────────────────────────────────────────────────────────


_RUN_INSERT_SQL = """
INSERT INTO pgam_direct.compliance_runs
    (started_at, finished_at, publishers_scanned, adstxt_fetched,
     findings_opened, findings_resolved, ok, error)
VALUES
    (%(started_at)s, now(), %(publishers_scanned)s, %(adstxt_fetched)s,
     %(findings_opened)s, %(findings_resolved)s, %(ok)s, %(error)s)
RETURNING run_id;
"""


def _write_run_log(payload: dict) -> int | None:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_RUN_INSERT_SQL, payload)
                row = cur.fetchone()
            conn.commit()
        return row[0] if row else None
    except Exception as exc:
        print(f"[{ACTOR}] run log write failed: {exc}")
        return None


# ── Entry point ──────────────────────────────────────────────────────────────


_RUN_START_INSERT_SQL = """
-- Insert a tombstone row at the BEGINNING of the run so even an
-- OOM-killed worker leaves evidence in compliance_runs. The
-- scheduler's catch-up cooldown reads from started_at — without this
-- start-row, a process killed mid-run looks identical to "no run
-- attempted today" and the catch-up restart-loops forever.
INSERT INTO pgam_direct.compliance_runs
    (started_at, finished_at, publishers_scanned, adstxt_fetched,
     findings_opened, findings_resolved, ok, error)
VALUES
    (%(started_at)s, NULL, 0, 0, 0, 0, NULL, NULL)
RETURNING run_id;
"""

_RUN_FINALIZE_SQL = """
UPDATE pgam_direct.compliance_runs
SET finished_at        = now(),
    publishers_scanned = %(publishers_scanned)s,
    adstxt_fetched     = %(adstxt_fetched)s,
    findings_opened    = %(findings_opened)s,
    findings_resolved  = %(findings_resolved)s,
    ok                 = %(ok)s,
    error              = %(error)s
WHERE run_id = %(run_id)s;
"""


def _insert_run_start(started_at) -> int | None:
    """Tombstone row at run start. Defensive — never raises."""
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_RUN_START_INSERT_SQL, {"started_at": started_at})
                row = cur.fetchone()
            conn.commit()
        return row[0] if row else None
    except Exception as exc:
        print(f"[{ACTOR}] run-start log failed (non-fatal): {exc}")
        return None


def _finalize_run_log(run_id: int | None, payload: dict) -> None:
    """Update the tombstone with final summary. Falls back to a fresh
    INSERT if the start-row write failed (run_id is None)."""
    if run_id is None:
        _write_run_log(payload)
        return
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_RUN_FINALIZE_SQL, {**payload, "run_id": run_id})
            conn.commit()
    except Exception as exc:
        print(f"[{ACTOR}] run-finalize failed (non-fatal): {exc}")


_IDEMPOTENCE_SQL = """
-- Decide whether to run *now* based on today's compliance_runs state.
-- Two skip conditions:
--   done       = any run today already finished with ok=TRUE → no work
--   in_flight  = a run started in the last 15 min with ok IS NULL →
--                another worker is probably still going; let it finish
-- A NULL row >15 min old is treated as a dead zombie (likely OOM-killed
-- on Render) and a new attempt is allowed. This is what enables retry-
-- until-success: the scheduler can fire compliance_runner at multiple
-- times in the morning (08:00 / 08:30 / 09:00 / …) and each tick that
-- finds NO live run + NO success will start a fresh attempt.
SELECT
    BOOL_OR(ok IS TRUE)                                          AS done,
    BOOL_OR(ok IS NULL AND started_at >= now() - interval '15 minutes') AS in_flight
FROM pgam_direct.compliance_runs
WHERE started_at::date = current_date;
"""


def _should_skip_today() -> str | None:
    """Return a reason string ('already_succeeded' / 'another_in_flight')
    or None to proceed. Safe to call from any cron tick — turns repeated
    firings into a single audited run plus harmless no-ops."""
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(_IDEMPOTENCE_SQL)
            row = cur.fetchone() or (False, False)
        done, in_flight = bool(row[0]), bool(row[1])
        if done:      return "already_succeeded"
        if in_flight: return "another_in_flight"
        return None
    except Exception as exc:
        # Don't gate retries on a DB hiccup — let the run try.
        print(f"[{ACTOR}] idempotence check failed (proceeding): {exc}")
        return None


def run() -> dict:
    """Scheduler entry. Returns a summary dict."""
    # Multi-fire idempotence: scheduler.py registers this at several
    # morning times (08:00 / 08:30 / 09:00 / 09:30 / 10:00 ET) so a
    # mid-run OOM on the first attempt automatically gets retried 30
    # min later. The runner itself decides whether each tick should do
    # real work or no-op. See _should_skip_today docstring.
    skip = _should_skip_today()
    if skip:
        print(f"[{ACTOR}] skip ({skip}) — another tick will handle it")
        return {"ok": True, "skipped": skip,
                "publishers_scanned": 0, "findings_opened": 0,
                "findings_resolved": 0}

    started_at = datetime.now(timezone.utc)
    print(f"[{ACTOR}] start {started_at.isoformat()}")
    # Tombstone insert — must succeed before heavy phases so concurrent
    # ticks see "another in flight" and skip cleanly, and so a worker
    # killed mid-run still leaves evidence in compliance_runs.
    _run_id = _insert_run_start(started_at)

    limit = int(os.environ.get("PGAM_COMPLIANCE_LIMIT") or 0) or None
    app_ads = os.environ.get("PGAM_COMPLIANCE_APP_ADS_TXT") == "1"
    rate_hz = float(os.environ.get("PGAM_COMPLIANCE_RATE_HZ") or 2.0)
    # Phase 2 reseller validation defaults ON; flip to "0" to skip if
    # ll_daily_partner_revenue is stale or LL bridge matching is being tuned.
    enable_resellers = os.environ.get("PGAM_COMPLIANCE_RESELLERS", "1") != "0"
    # Phase 3 downstream sellers.json audit; defaults ON.
    enable_downstream = os.environ.get("PGAM_COMPLIANCE_DOWNSTREAM", "1") != "0"
    # Phase 3 publisher scoring; defaults ON.
    enable_scoring = os.environ.get("PGAM_COMPLIANCE_SCORING", "1") != "0"
    # Phase 4 schain static audit; defaults ON. Degrades cleanly without LL creds.
    enable_schain = os.environ.get("PGAM_COMPLIANCE_SCHAIN", "1") != "0"
    # Phase 4 dynamic schain audit (reads pgam_direct.compliance_schain_emissions_24h
    # populated by pgam-direct/web's /api/cron/schain-rollup). Defaults ON;
    # cleanly skips if the source table/view doesn't exist yet.
    enable_schain_dynamic = os.environ.get("PGAM_COMPLIANCE_SCHAIN_DYNAMIC", "1") != "0"
    # Phase 5 per-entity audit (LL "Suppliers" view granularity). Defaults ON.
    # Defaults to top 200 entities — override with PGAM_COMPLIANCE_PHASE5_TOP_N.
    enable_phase5 = os.environ.get("PGAM_COMPLIANCE_PHASE5", "1") != "0"

    summary: dict = {
        "started_at": started_at,
        "publishers_scanned": 0,
        "adstxt_fetched": 0,
        "ll_bridge_matched": 0,
        "ll_bridge_unmatched": 0,
        "observed_ssp_rows": 0,
        "ssps_audited": 0,
        "partners_active_recent": 0,
        "partners_inactive_recent": 0,
        "partners_unbridged": 0,
        "schain_demands_audited": 0,
        "schain_publishers_audited": 0,
        "dynamic_schain_publishers": 0,
        "dynamic_schain_findings": 0,
        "publisher_config_total": 0,
        "publisher_config_correct": 0,
        "publisher_config_findings": 0,
        "roundtrip_entities": 0,
        "roundtrip_declared_direct": 0,
        "roundtrip_declared_via_ssp": 0,
        "roundtrip_unbridged": 0,
        "roundtrip_undeclared": 0,
        "roundtrip_rev_at_risk_7d": 0.0,
        "demand_total_seen": 0,
        "demand_new": 0,
        "demand_unmapped": 0,
        "phase5_entities_audited": 0,
        "phase5_domains": 0,
        "phase5_apps": 0,
        "phase5_apps_unresolved": 0,
        "scores_written": 0,
        "avg_score": 0.0,
        "findings_opened": 0,
        "findings_resolved": 0,
        "ok": False,
        "error": None,
    }

    try:
        _ensure_schema()

        # Sync the FULL sellers.json registry (PUBLISHER + BOTH +
        # INTERMEDIARY) into compliance_publishers — INTERMEDIARY rows
        # are what the ll_bridge needs to map LL supply partners
        # (Start.io, Smaato, BidMachine, …) for Phase 6's round-trip.
        full_registry = build_full_registry()
        upserted, deactivated = sync_universe(full_registry)
        print(f"[{ACTOR}] sellers.json registry upserted={upserted} "
              f"deactivated={deactivated} (full set incl. INTERMEDIARY)")

        # Phase 1's ads.txt crawl loop operates on the publisher-like
        # subset only — we don't crawl Start.io / Smaato / etc.'s
        # ads.txt; we crawl the apps + domains flowing through them.
        publishers = [p for p in full_registry if p.seller_type in ("PUBLISHER","BOTH")]
        if limit:
            publishers = publishers[:limit]

        fetches = _crawl_all(publishers, app_ads=app_ads, rate_hz=rate_hz)
        _persist_fetches(fetches)
        print(f"[{ACTOR}] crawled {len(fetches)} files across {len(publishers)} pubs")

        # Phase 2: bridge LL publishers → sellers.json domains, then derive
        # observed (publisher × ssp) monetization. Both feed the conditional
        # reseller validator. Each block is independently fault-tolerant —
        # if LL revenue data isn't available, the universal Phase 1 check
        # still runs.
        if enable_resellers:
            try:
                bridge_stats = run_bridge()
                summary["ll_bridge_matched"]   = bridge_stats.matched
                summary["ll_bridge_unmatched"] = bridge_stats.unmatched
                print(
                    f"[{ACTOR}] ll_bridge matched={bridge_stats.matched}/"
                    f"{bridge_stats.ll_publishers_seen} "
                    f"methods={bridge_stats.method_counts}"
                )
            except Exception as exc:
                print(f"[{ACTOR}] ll_bridge failed (non-fatal): {exc}")

            try:
                obs_stats = refresh_observed_monetization()
                summary["observed_ssp_rows"] = obs_stats.observed_rows
                print(
                    f"[{ACTOR}] observed_monetization rows={obs_stats.observed_rows} "
                    f"pubs={obs_stats.unique_publishers} "
                    f"ssps={obs_stats.unique_ssps} "
                    f"unclassified_demands={obs_stats.unclassified_demands}"
                )
            except Exception as exc:
                print(f"[{ACTOR}] observed_monetization refresh failed (non-fatal): {exc}")

        # Activity gate — restrict Phase 1 audits to partners currently
        # earning revenue on LL. Runs AFTER the bridge so we can join via
        # compliance_publishers.ll_publisher_id. If activity refresh fails
        # we leave active_keys=None which falls back to the legacy "audit
        # everything" behavior — better signal than going silent.
        active_keys: set[str] | None = None
        try:
            act_stats = refresh_partner_activity()
            active_keys = load_active_publisher_keys()
            summary["partners_active_recent"]   = act_stats.active
            summary["partners_inactive_recent"] = act_stats.inactive
            summary["partners_unbridged"]       = act_stats.unbridged
            print(
                f"[{ACTOR}] partner_activity active={act_stats.active}/"
                f"{act_stats.total} (inactive={act_stats.inactive}, "
                f"unbridged={act_stats.unbridged}, "
                f"revenue_7d=${act_stats.total_revenue_7d:,.0f})"
            )
        except Exception as exc:
            print(f"[{ACTOR}] partner activity refresh failed (non-fatal): {exc}")

        findings = _validate_all(publishers, fetches,
                                 app_ads=app_ads,
                                 enable_resellers=enable_resellers,
                                 active_keys=active_keys)

        # Phase 3: downstream sellers.json audit. Each SSP-level finding
        # uses a sentinel publisher_key '_ssp:<key>' so the upsert
        # pipeline handles them uniformly. Scoring excludes sentinels.
        ssp_sentinel_keys: list[str] = []
        if enable_downstream:
            ssp_findings, ssp_sentinel_keys = _audit_downstream_ssps()
            findings.extend(ssp_findings)
            summary["ssps_audited"] = len(ssp_sentinel_keys)

        # Supply-partner sellers.json audit — mirror of Phase 3 but
        # pointed at the LL supply partners (Smaato, BidMachine, etc.)
        # instead of the demand SSPs. Verifies each partner declares
        # our PGAM seat correctly in their own sellers.json — without
        # this, demand-side audit chains break and DSPs reject bids.
        supply_partner_sentinel_keys: list[str] = []
        if os.environ.get("PGAM_COMPLIANCE_SUPPLY_PARTNER_AUDIT", "1") != "0":
            try:
                sp_audit = run_supply_partner_audit()
                findings.extend(sp_audit.findings)
                supply_partner_sentinel_keys = sp_audit.sentinel_keys
                summary["supply_partners_audited"] = sp_audit.partners_audited
                summary["supply_partner_findings"] = len(sp_audit.findings)
                print(
                    f"[{ACTOR}] supply_partner_audit partners="
                    f"{sp_audit.partners_audited} "
                    f"findings={len(sp_audit.findings)}"
                )
            except Exception as exc:
                print(f"[{ACTOR}] supply_partner audit failed (non-fatal): {exc}")

        # Phase 5: per-entity audit, scoped to the LL "Suppliers" view.
        # Universe = every (app, domain) flowing through each ACTIVE supply
        # partner in LL (Start.IO, Smaato, BidMachine, ...). For each entity:
        # tiered universal-DIRECT-line check + per-entity conditional reseller
        # lines. Read-only — no writes to LL.
        # Pre-Phase-5 dev_domain backfill — resolves any unknown
        # bundle-to-developer-domain mappings so Phase 5 can fetch
        # their app-ads.txt this run (instead of skipping them as
        # unresolved). Uses the full play_store_resolver cascade
        # (heuristic → iTunes → Play Store scrape → fallback).
        if os.environ.get("PGAM_COMPLIANCE_DEV_DOMAIN_BACKFILL", "1") != "0":
            try:
                from agents.enrichment.dev_domain_backfill import (
                    resolve_top_unresolved_bundles,
                )
                bf = resolve_top_unresolved_bundles(top_n=30)
                summary["dev_domain_candidates"] = bf.candidates_seen
                summary["dev_domain_resolved"]   = bf.resolved
                summary["dev_domain_unresolved"] = bf.unresolved
                print(
                    f"[{ACTOR}] dev_domain_backfill candidates="
                    f"{bf.candidates_seen} attempted={bf.attempted} "
                    f"resolved={bf.resolved} unresolved={bf.unresolved}"
                )
            except Exception as exc:
                print(f"[{ACTOR}] dev_domain_backfill failed (non-fatal): {exc}")

        phase5_sentinel_keys: list[str] = []
        p5_for_matrix = None   # held for the audit matrix step below
        if enable_phase5:
            try:
                p5 = run_entity_audit(rate_hz=rate_hz)
                if p5.skipped_reason:
                    print(f"[{ACTOR}] phase5 skipped: {p5.skipped_reason}")
                else:
                    findings.extend(p5.findings)
                    phase5_sentinel_keys = p5.sentinel_keys
                    p5_for_matrix = p5
                    summary["phase5_entities_audited"] = p5.universe_stats.top_n_selected
                    summary["phase5_domains"]          = p5.universe_stats.domains_in_universe
                    summary["phase5_apps"]             = p5.universe_stats.apps_in_universe
                    summary["phase5_apps_unresolved"]  = p5.universe_stats.apps_unresolved
                    summary["phase5_supply_partners"]  = len(p5.supply_partners or [])
                    print(
                        f"[{ACTOR}] phase5 partners={len(p5.supply_partners or [])} "
                        f"entities={p5.universe_stats.top_n_selected} "
                        f"domains={p5.universe_stats.domains_in_universe} "
                        f"apps={p5.universe_stats.apps_in_universe} "
                        f"apps_unresolved={p5.universe_stats.apps_unresolved} "
                        f"findings={len(p5.findings)}"
                    )
            except Exception as exc:
                print(f"[{ACTOR}] phase5 entity audit failed (non-fatal): {exc}")

        # ── Audit matrix — per-(entity × SSP) compliance grid ─────────
        # Source of truth for "what was audited and is it compliant",
        # complementing compliance_findings (which only carries failures).
        # Persists to pgam_direct.compliance_entity_ssp_audit and writes
        # the daily CSV under data/compliance_matrix_<date>.csv. The
        # digest pulls the daily KPI row from
        # compliance_audit_summary_daily.
        if p5_for_matrix is not None:
            try:
                # Build the supply-path audit FIRST so the per-entity
                # path classification (pgam_direct vs via_partner) and
                # the path-aware B-layer verdict are available when
                # the per-(entity × SSP) matrix gets classified below.
                # Otherwise the matrix's DIRECT-only PGAM check
                # mislabels every via_partner entity as critical (it
                # has a RESELLER line, not a DIRECT line — both are
                # valid for different paths).
                sp_rows = []
                sp_summary = None
                supply_path_by_entity: dict = {}
                try:
                    sp_rows, sp_summary = build_supply_path_audit(
                        entities=p5_for_matrix.entities,
                        fetches_by_entity=p5_for_matrix.fetches_by_entity,
                        pgam_seat_registry=p5_for_matrix.pgam_seat_registry,
                    )
                    persist_supply_path(sp_rows)
                    supply_path_by_entity = {r.entity_key: r for r in sp_rows}
                    summary["supply_path_rows"]              = sp_summary.total_rows
                    summary["supply_path_pgam_direct"]       = sp_summary.pgam_direct_rows
                    summary["supply_path_via_partner"]       = sp_summary.via_partner_rows
                    summary["supply_path_unknown"]           = sp_summary.unknown_rows
                    summary["supply_path_revenue_audited"]   = sp_summary.revenue_audited_usd
                    summary["supply_path_revenue_compliant"] = sp_summary.revenue_compliant_usd
                    summary["supply_path_revenue_at_risk"]   = sp_summary.revenue_at_risk_usd
                    summary["supply_path_compliance_pct"]    = sp_summary.compliance_pct
                    summary["supply_path_critical"]          = sp_summary.critical_rows
                    summary["supply_path_warning"]           = sp_summary.warning_rows
                    summary["supply_path_healthy"]           = sp_summary.healthy_rows
                    print(
                        f"[{ACTOR}] supply_path rows={sp_summary.total_rows} "
                        f"compliant={sp_summary.compliance_pct}% "
                        f"$compliant={sp_summary.revenue_compliant_usd:,.0f} "
                        f"$at_risk={sp_summary.revenue_at_risk_usd:,.0f} "
                        f"pgam_direct={sp_summary.pgam_direct_rows} "
                        f"via_partner={sp_summary.via_partner_rows} "
                        f"unknown={sp_summary.unknown_rows}"
                    )
                except Exception as exc:
                    print(f"[{ACTOR}] supply_path audit failed (non-fatal): {exc}")

                rows, mtx_summary = build_audit_matrix(
                    entities=p5_for_matrix.entities,
                    fetches_by_entity=p5_for_matrix.fetches_by_entity,
                    pgam_seat_registry=p5_for_matrix.pgam_seat_registry,
                    supply_path_by_entity=supply_path_by_entity,
                )
                persisted = persist_matrix(rows)
                # Daily aggregate row for the digest + dashboard.
                from datetime import date as _date
                with connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO pgam_direct.compliance_audit_summary_daily
                                (as_of, total_rows, domains_audited, apps_audited,
                                 ssps_audited, revenue_audited_usd,
                                 revenue_compliant_usd, revenue_non_compliant_usd,
                                 compliance_pct, critical_rows, warning_rows,
                                 healthy_rows, below_threshold_rows)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (as_of) DO UPDATE SET
                                total_rows=EXCLUDED.total_rows,
                                domains_audited=EXCLUDED.domains_audited,
                                apps_audited=EXCLUDED.apps_audited,
                                ssps_audited=EXCLUDED.ssps_audited,
                                revenue_audited_usd=EXCLUDED.revenue_audited_usd,
                                revenue_compliant_usd=EXCLUDED.revenue_compliant_usd,
                                revenue_non_compliant_usd=EXCLUDED.revenue_non_compliant_usd,
                                compliance_pct=EXCLUDED.compliance_pct,
                                critical_rows=EXCLUDED.critical_rows,
                                warning_rows=EXCLUDED.warning_rows,
                                healthy_rows=EXCLUDED.healthy_rows,
                                below_threshold_rows=EXCLUDED.below_threshold_rows,
                                computed_at=now();
                            """,
                            (
                                _date.today(),
                                mtx_summary.total_rows,
                                mtx_summary.domains_audited,
                                mtx_summary.apps_audited,
                                mtx_summary.ssps_audited,
                                mtx_summary.revenue_audited_usd,
                                mtx_summary.revenue_compliant_usd,
                                mtx_summary.revenue_non_compliant_usd,
                                mtx_summary.compliance_pct,
                                mtx_summary.critical_rows,
                                mtx_summary.warning_rows,
                                mtx_summary.healthy_rows,
                                mtx_summary.below_threshold_rows,
                            ),
                        )
                    conn.commit()
                # Daily CSV next to the agent's other data files.
                csv_path = _REPO_ROOT / "data" / f"compliance_matrix_{_date.today().isoformat()}.csv"
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                csv_path.write_text(rows_to_csv(rows))
                summary["audit_matrix_rows"]        = mtx_summary.total_rows
                summary["audit_matrix_compliant_pct"] = mtx_summary.compliance_pct
                summary["audit_matrix_revenue_audited"]   = mtx_summary.revenue_audited_usd
                summary["audit_matrix_revenue_compliant"] = mtx_summary.revenue_compliant_usd
                summary["audit_matrix_revenue_at_risk"]   = mtx_summary.revenue_non_compliant_usd
                summary["audit_matrix_critical"] = mtx_summary.critical_rows
                summary["audit_matrix_warning"]  = mtx_summary.warning_rows
                summary["audit_matrix_healthy"]  = mtx_summary.healthy_rows
                summary["audit_matrix_ssps"]     = mtx_summary.ssps_audited
                summary["audit_matrix_csv_path"] = str(csv_path)

                # ── Block-list maintenance (Stage 1: queue only) ────
                # For each non-compliant (entity × supply_partner) path
                # with revenue ≥ $50/7d, upsert a row into
                # compliance_path_block_list. Auto-release rows whose
                # audit is now healthy. Stage 3 (PGAM bidder-edge filter
                # in pgam-direct/web) reads from this table to apply
                # per-path enforcement at request time. The flip from
                # 'pending_review' to 'active' is a manual ops action
                # (Stage 2, not built yet).
                try:
                    bl = refresh_block_list()
                    summary["block_list_pending"]       = bl.pending_review
                    summary["block_list_active"]        = bl.active
                    summary["block_list_inserted"]      = bl.rows_inserted
                    summary["block_list_auto_released"] = bl.rows_auto_released
                    summary["block_list_expired"]       = bl.rows_expired
                    print(
                        f"[{ACTOR}] block_list pending={bl.pending_review} "
                        f"active={bl.active} new={bl.rows_inserted} "
                        f"auto_released={bl.rows_auto_released} "
                        f"expired={bl.rows_expired}"
                    )
                except Exception as exc:
                    print(f"[{ACTOR}] block_list refresh failed (non-fatal): {exc}")
                # Per-entity verdicts (separate from per-row): needed by the
                # digest for honest "X of Y entities need attention" headers.
                _entities_with_issues: set[str] = set()
                _entities_all_clean: set[str] = set()
                _all_entities: set[str] = set()
                for r in rows:
                    _all_entities.add(r.entity_key)
                    if r.status != "healthy":
                        _entities_with_issues.add(r.entity_key)
                _entities_all_clean = _all_entities - _entities_with_issues
                summary["audit_entities_total"]        = len(_all_entities)
                summary["audit_entities_with_issues"]  = len(_entities_with_issues)
                summary["audit_entities_fully_clean"]  = len(_entities_all_clean)
                print(
                    f"[{ACTOR}] audit_matrix rows={mtx_summary.total_rows} "
                    f"compliant={mtx_summary.compliance_pct}% "
                    f"$compliant={mtx_summary.revenue_compliant_usd:,.0f} "
                    f"$at_risk={mtx_summary.revenue_non_compliant_usd:,.0f} "
                    f"crit={mtx_summary.critical_rows} "
                    f"warn={mtx_summary.warning_rows} "
                    f"healthy={mtx_summary.healthy_rows} "
                    f"csv={csv_path.name}"
                )
            except Exception as exc:
                print(f"[{ACTOR}] audit_matrix failed (non-fatal): {exc}")

            # ── Memory hygiene ────────────────────────────────────────
            # Phase 5 + audit_matrix + supply_path together hold ~50
            # entities × parsed ads.txt content (some files have 10K+
            # lines) AND the in-flight row lists. The 512MB Render
            # budget gets tight by this point. Worst-case OOM kills
            # happen during the heavier inventory_roundtrip pull that
            # comes next (73K LL stats rows). Drop the Phase 5 holdings
            # here so roundtrip has a clean working set.
            try:
                import gc as _gc
                # EntityAuditResult is frozen but its list/dict attrs
                # are mutable — clear in-place to drop refs to the
                # parsed ads.txt content without rebinding the frozen
                # attribute.
                if p5_for_matrix is not None:
                    p5_for_matrix.fetches_by_entity.clear()
                    p5_for_matrix.fetches.clear()
                    p5_for_matrix.entities.clear()
                if "rows" in locals():
                    rows = None
                if "sp_rows" in locals():
                    sp_rows = None
                _gc.collect()
                print(f"[{ACTOR}] mem hygiene: released Phase 5 fetches "
                      f"+ matrix/sp rows before roundtrip")
            except Exception as _exc:
                print(f"[{ACTOR}] mem hygiene cleanup (non-fatal): {_exc}")

        # Phase 4: static schain audit on LL demands + publishers. Reports
        # any drift the optimization-side auto-fixer didn't catch (rev-threshold
        # backlog, manual UI toggles, etc). Read-only — does not write to LL.
        schain_sentinel_keys: list[str] = []
        if enable_schain:
            try:
                sch = run_schain_audit()
                if sch.skipped_reason:
                    print(f"[{ACTOR}] schain audit skipped: {sch.skipped_reason}")
                else:
                    findings.extend(sch.findings)
                    schain_sentinel_keys = sch.sentinel_keys
                    summary["schain_demands_audited"]   = sch.demands_audited
                    summary["schain_publishers_audited"] = sch.publishers_audited
                    print(
                        f"[{ACTOR}] schain audit demands={sch.demands_audited} "
                        f"publishers={sch.publishers_audited} "
                        f"findings={len(sch.findings)}"
                    )
            except Exception as exc:
                print(f"[{ACTOR}] schain audit failed (non-fatal): {exc}")

        # Phase 4 — DYNAMIC schain audit. Reads
        # pgam_direct.compliance_schain_emissions_24h, populated hourly
        # by pgam-direct/web's /api/cron/schain-rollup
        # (ClickHouse auction_events → Postgres). Complements the static
        # audit by flagging real emitted-schain anomalies, not just
        # misconfigured demand flags.
        dynamic_schain_sentinel_keys: list[str] = []
        if enable_schain_dynamic:
            try:
                d_stats, d_findings, d_keys = run_dynamic_schain_audit()
                if d_stats.skipped_reason:
                    print(f"[{ACTOR}] dynamic schain skipped: {d_stats.skipped_reason}")
                else:
                    findings.extend(d_findings)
                    dynamic_schain_sentinel_keys = d_keys
                    summary["dynamic_schain_publishers"] = d_stats.publishers_seen
                    summary["dynamic_schain_findings"]   = d_stats.findings_count
                    print(
                        f"[{ACTOR}] dynamic schain "
                        f"publishers={d_stats.publishers_seen} "
                        f"findings={d_stats.findings_count}"
                    )
            except Exception as exc:
                print(f"[{ACTOR}] dynamic schain failed (non-fatal): {exc}")

        # Phase 4 — Tier A: verify every active publisher_config has
        # schain_asi = 'pgamssp.com' so the bidder-edge injects our
        # canonical node ASI on every emitted bid. Catches misconfig at
        # the source-of-truth layer (the bidder polls publisher_configs
        # ~60s); complements the LL-side static audit which validates
        # supplyChainEnabled on demands.
        pub_config_sentinel_keys: list[str] = []
        if enable_schain:
            try:
                pc_stats, pc_findings, pc_keys = run_publisher_config_schain_audit()
                if pc_stats.skipped_reason:
                    print(f"[{ACTOR}] publisher_config schain skipped: "
                          f"{pc_stats.skipped_reason}")
                else:
                    findings.extend(pc_findings)
                    pub_config_sentinel_keys = pc_keys
                    summary["publisher_config_total"]    = pc_stats.total_active
                    summary["publisher_config_correct"]  = pc_stats.correct_asi
                    summary["publisher_config_findings"] = pc_stats.findings_count
                    print(
                        f"[{ACTOR}] publisher_config schain "
                        f"total={pc_stats.total_active} "
                        f"correct={pc_stats.correct_asi} "
                        f"null={pc_stats.null_asi} "
                        f"mismatch={pc_stats.mismatch_asi}"
                    )
            except Exception as exc:
                print(f"[{ACTOR}] publisher_config schain failed (non-fatal): {exc}")

        # Phase 7 — new-demand + unmapped-SSP detection. Tracks every
        # LL demand_partner name observed; flags ones never seen before
        # and ones that don't classify to any SSP in ssp_registry
        # (i.e. silent gaps in the per-SSP reseller-line check). Auto-
        # resolves as the registry is updated to cover them.
        demand_sentinel_keys: list[str] = []
        if os.environ.get("PGAM_COMPLIANCE_DEMAND_DETECTOR", "1") != "0":
            try:
                d_stats, d_findings, d_keys = run_demand_detector()
                if d_stats.skipped_reason:
                    print(f"[{ACTOR}] demand_detector skipped: "
                          f"{d_stats.skipped_reason}")
                else:
                    findings.extend(d_findings)
                    demand_sentinel_keys = d_keys
                    summary["demand_total_seen"]    = d_stats.total_demands_seen
                    summary["demand_new"]           = d_stats.new_demands
                    summary["demand_unmapped"]      = d_stats.unmapped_demands
                    print(
                        f"[{ACTOR}] demand_detector seen={d_stats.total_demands_seen} "
                        f"new={d_stats.new_demands} "
                        f"unmapped={d_stats.unmapped_demands} "
                        f"findings={d_stats.findings_count}"
                    )
            except Exception as exc:
                print(f"[{ACTOR}] demand_detector failed (non-fatal): {exc}")

        # Phase 6 — sellers.json revenue round-trip. For every entity
        # earning trailing-7d revenue, verify it's covered by either a
        # direct PUBLISHER entry in PGAM sellers.json (entity's domain
        # is declared) or an INTERMEDIARY supply partner that IS
        # declared (Start.IO, Smaato, etc.). Flags revenue earned on
        # inventory we haven't declared — the gap DSPs catch when
        # auditing our supply paths. Vivek-inspired port.
        roundtrip_sentinel_keys: list[str] = []
        if os.environ.get("PGAM_COMPLIANCE_ROUNDTRIP", "1") != "0":
            try:
                rt = run_inventory_roundtrip_audit()
                if rt.skipped_reason:
                    print(f"[{ACTOR}] roundtrip skipped: {rt.skipped_reason}")
                elif rt.stats is not None:
                    findings.extend(rt.findings)
                    roundtrip_sentinel_keys = rt.sentinel_keys
                    summary["roundtrip_entities"]         = rt.stats.entities_seen
                    summary["roundtrip_declared_direct"]  = rt.stats.declared_direct
                    summary["roundtrip_declared_via_ssp"] = rt.stats.declared_intermediary
                    summary["roundtrip_unbridged"]        = rt.stats.unbridged_partner
                    summary["roundtrip_undeclared"]       = rt.stats.undeclared
                    summary["roundtrip_rev_at_risk_7d"]   = rt.stats.revenue_at_risk_7d
                    print(
                        f"[{ACTOR}] roundtrip "
                        f"declared={rt.stats.declared_direct}+{rt.stats.declared_intermediary} "
                        f"unbridged={rt.stats.unbridged_partner} "
                        f"undeclared={rt.stats.undeclared} "
                        f"at_risk=${rt.stats.revenue_at_risk_7d:,.0f}/7d"
                    )
            except Exception as exc:
                print(f"[{ACTOR}] roundtrip failed (non-fatal): {exc}")

        opened, total = upsert_findings(findings)
        print(f"[{ACTOR}] findings: total={total} newly_opened={opened}")

        seen = [(f.publisher_key, f.check_id, f.fingerprint) for f in findings]
        # Only auto-resolve for publishers whose ads.txt actually returned 200.
        # An unreachable file is "I don't know" — don't infer "fixed".
        # SSP + schain sentinels auto-resolve when the next audit clears them.
        reachable_pubs = sorted({
            f.publisher_key for f in fetches
            if f.variant == "ads.txt" and f.http_status == 200
        })
        resolvable = (
            reachable_pubs
            + ssp_sentinel_keys
            + supply_partner_sentinel_keys
            + schain_sentinel_keys
            + dynamic_schain_sentinel_keys
            + pub_config_sentinel_keys
            + phase5_sentinel_keys
            + roundtrip_sentinel_keys
            + demand_sentinel_keys
        )
        resolved = resolve_cleared(resolvable, seen)
        print(f"[{ACTOR}] auto-resolved {resolved} previously-open findings")

        # Phase 3: per-publisher score, AFTER findings have been upserted.
        if enable_scoring:
            try:
                score_stats = refresh_publisher_scores()
                summary["scores_written"] = score_stats.rows_written
                summary["avg_score"]      = score_stats.avg_score
                summary["avg_score_active"] = score_stats.avg_score_active
                summary["active_publishers"] = score_stats.active_count
                summary["publishers_below_75"] = score_stats.publishers_below_75
                summary["publishers_below_75_active"] = (
                    score_stats.publishers_below_75_active
                )
                print(
                    f"[{ACTOR}] scores written={score_stats.rows_written} "
                    f"avg_all={score_stats.avg_score} "
                    f"avg_active={score_stats.avg_score_active} "
                    f"(active n={score_stats.active_count}) "
                    f"below_75_active={score_stats.publishers_below_75_active}"
                )
            except Exception as exc:
                print(f"[{ACTOR}] scoring failed (non-fatal): {exc}")

        summary.update({
            "publishers_scanned": len(publishers),
            "adstxt_fetched":     len(fetches),
            "findings_opened":    opened,
            "findings_resolved":  resolved,
            "ok":                 True,
        })

        try:
            post_digest(summary)
        except Exception as exc:
            print(f"[{ACTOR}] Slack digest failed (non-fatal): {exc}")

    except Exception as exc:
        summary["error"] = str(exc)
        print(f"[{ACTOR}] FAILED: {exc}")

    _finalize_run_log(_run_id, summary)
    print(
        f"[{ACTOR}] done ok={summary['ok']} "
        f"pubs={summary['publishers_scanned']} "
        f"opened={summary['findings_opened']} "
        f"resolved={summary['findings_resolved']}"
    )
    return summary


def run_fallback_digest() -> dict:
    """Last-resort delivery so #compliance always has a daily message.

    Wired into scheduler.py at 10:30 ET (30 min after the last retry
    window). If by then today's audit hasn't succeeded AND no digest
    has gone out, this posts the most recent available snapshot with
    an explicit banner so the operator knows the audit failed but
    isn't left wondering whether the cron itself fired.

    No-ops if:
      • today's audit already finished ok=TRUE (the normal digest
        already posted from inside run()), OR
      • a digest with today's dedupe key was already sent (covers the
        case where a previous fallback call already delivered).
    """
    from agents.compliance.reporters import slack_digest as _sd
    from core import slack as _slack
    print(f"[{ACTOR}] fallback-digest check")

    # 1) Did today's audit succeed? If yes, the normal post happened.
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT BOOL_OR(ok IS TRUE) FROM pgam_direct.compliance_runs
                WHERE started_at::date = current_date
            """)
            done = bool((cur.fetchone() or (False,))[0])
        if done:
            print(f"[{ACTOR}] fallback skip: today's audit completed normally")
            return {"ok": True, "skipped": "audit_succeeded"}
    except Exception as exc:
        print(f"[{ACTOR}] fallback DB check failed (proceeding): {exc}")

    # 2) Was a digest already sent today (e.g. earlier fallback / manual fire)?
    # Uses the Neon-backed shared dedup so manual posts and scheduled fires
    # observe the same state across hosts.
    try:
        if _slack.already_sent_today_shared(_sd.DEDUPE_KEY):
            print(f"[{ACTOR}] fallback skip: digest dedupe key already set")
            return {"ok": True, "skipped": "digest_already_sent"}
    except Exception as exc:
        print(f"[{ACTOR}] fallback dedupe check failed (proceeding): {exc}")

    # 3) Find the most recent available snapshot.
    latest_as_of = None
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(as_of) FROM pgam_direct.compliance_entity_supply_path_audit
            """)
            latest_as_of = cur.fetchone()[0]
    except Exception as exc:
        print(f"[{ACTOR}] fallback latest-snapshot query failed: {exc}")

    if latest_as_of is None:
        print(f"[{ACTOR}] fallback abort: no audit snapshots anywhere in DB")
        return {"ok": False, "skipped": "no_snapshot"}

    # 4) Build the fallback digest. Instead of citing stale snapshot
    # data (which can be days out of date and may already be wrong —
    # e.g. com.block.juggle showed as misclassified Smaato/critical
    # on 2026-06-06 because the 6/2 snapshot was BEFORE the dev_domain
    # remap to hungrystudio.com), we pull TODAY's LL revenue live and
    # crawl ads.txt right now for the top earners. The fallback then
    # surfaces:
    #   • Yesterday's revenue by default; trailing-7d when fired on a
    #     Monday (operator's standing preference)
    #   • Per-entity: which exact pgamssp.com seat is missing for
    #     that publisher's supply path AND which demand-SSP lines are
    #     missing, with revenue attribution per (entity × SSP) pair
    #   • Note on snapshot freshness so the operator knows whether
    #     they're looking at fresh or fallback data
    from datetime import date as _date, timedelta as _td
    today = _date.today()
    is_monday = today.weekday() == 0
    period_start = (today - _td(days=7)) if is_monday else (today - _td(days=1))
    period_end   = today - _td(days=1)
    period_label = ("trailing-7d (Monday weekly view)" if is_monday
                    else f"yesterday ({period_end.isoformat()})")

    # Demand SSPs to check + the line on a publisher ads.txt that
    # authorizes them to resell PGAM inventory. None seat → just
    # check line presence (PubMatic, Sovrn use per-account seats we
    # haven't pinned in the registry).
    DEMAND_CHECKS = {
        "Rubicon":      ("rubiconproject.com",    "24852"),
        "Sharethrough": ("sharethrough.com",      "VQlYJeXR"),
        "Triplelift":   ("triplelift.com",        "14680"),
        "Loopme":       ("loopme.com",            "19940"),
        "Zeta":         ("zetaglobal.net",        "748"),
        "Appnexus":     ("appnexus.com",          "8106"),
        "Unruly":       ("video.unrulymedia.com", "5921144960123684292"),
        "Pubmatic":     ("pubmatic.com",          None),
        "Sovrn":        ("lijit.com",             None),
    }

    def _norm_host(h):
        h = (h or "").strip().lower()
        if h.startswith(("http://","https://")):
            h = h.split("://",1)[1]
        for sep in ("?","#","/"):
            if sep in h: h = h.split(sep,1)[0]
        if h.startswith("www."):
            h = h[4:]
        return h

    def _live_fetch(host):
        """Crawl both ads.txt and app-ads.txt; return parsed lines."""
        import urllib.request
        bodies = []
        for path in ("app-ads.txt", "ads.txt"):
            try:
                req = urllib.request.Request(
                    f"https://{host}/{path}",
                    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                          "AppleWebKit/605.1.15"})
                with urllib.request.urlopen(req, timeout=10) as r:
                    if r.status == 200:
                        b = r.read().decode("utf-8", errors="ignore")
                        if len(b) > 50: bodies.append(b)
            except Exception:
                pass
        if not bodies:
            return None
        lines = []
        for raw in "\n".join(bodies).splitlines():
            s = raw.split("#",1)[0].strip()
            if not s or ("=" in s and "," not in s):
                continue
            parts = [p.strip() for p in s.split(",")]
            if len(parts) >= 3:
                lines.append((parts[0].lower(), parts[1], parts[2].upper()))
        return lines

    # Pull LL revenue for the period
    try:
        from core.api import fetch as _ll_fetch
        live_rev = []
        for bd, k in (("DOMAIN,PUBLISHER,DEMAND_PARTNER","DOMAIN"),
                       ("BUNDLE,PUBLISHER,DEMAND_PARTNER","BUNDLE")):
            for r in _ll_fetch(bd, ["GROSS_REVENUE","IMPRESSIONS"],
                               period_start.isoformat(), period_end.isoformat()):
                val = str(r.get(k) or "").strip()
                if not val or val == "?": continue
                rev = float(r.get("GROSS_REVENUE") or 0)
                if rev <= 0: continue
                live_rev.append({
                    "kind": "domain" if k == "DOMAIN" else "app",
                    "value": val,
                    "supply_pub_id":   str(r.get("PUBLISHER_ID") or ""),
                    "supply_pub_name": str(r.get("PUBLISHER_NAME") or ""),
                    "demand_name":     str(r.get("DEMAND_PARTNER_NAME") or ""),
                    "rev": rev,
                })
    except Exception as exc:
        print(f"[{ACTOR}] fallback LL pull failed (using stale snapshot): {exc}")
        live_rev = []

    blocks: list[dict] = []

    if live_rev:
        # Aggregate by entity + load bridge + crawl ads.txt
        from collections import defaultdict
        agg = defaultdict(lambda: {"rev":0.0, "supply": defaultdict(float),
                                   "demand": defaultdict(float)})
        for r in live_rev:
            key = (r["kind"], r["value"])
            agg[key]["rev"] += r["rev"]
            agg[key]["supply"][r["supply_pub_id"]] += r["rev"]
            agg[key]["demand"][r["demand_name"]] += r["rev"]
        top = sorted(agg.items(), key=lambda kv: -kv[1]["rev"])[:10]

        # Resolve bundles → publisher domain via app_metadata
        bundles = {k[1] for k,_ in top if k[0]=="app"}
        bun_to_dom = {}
        if bundles:
            try:
                with connect() as conn, conn.cursor() as cur:
                    cur.execute("SELECT bundle_id, dev_domain FROM pgam_direct.app_metadata "
                                "WHERE bundle_id = ANY(%s) AND dev_domain IS NOT NULL",
                                (list(bundles),))
                    bun_to_dom = dict(cur.fetchall())
            except Exception:
                pass

        # Per-supply-partner expected pgamssp seat
        partner_seats = {}
        try:
            with connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT ll_publisher_id, seller_id, publisher_key "
                            "FROM pgam_direct.compliance_ll_partner_bridge")
                partner_seats = {r[0]: {"seat": r[1], "key": r[2]} for r in cur.fetchall()}
        except Exception:
            pass

        period_total = sum(info["rev"] for _, info in top)
        blocks.append({"type":"section","text":{"type":"mrkdwn","text":
            f":warning: *Compliance fallback — daily audit failed; live data instead*\n"
            f"_Today's audit didn't complete (latest snapshot {latest_as_of}, "
            f"{days_stale}d stale). Pulled fresh LL revenue + crawled ads.txt "
            f"in real-time for the top 10 earners._\n"
            f"_Period: *{period_label}*  ·  Top 10 revenue: *${period_total:,.0f}*_"
        }})

        live_cache = {}
        for (kind, val), info in top:
            host = val if kind=="domain" else bun_to_dom.get(val)
            host = _norm_host(host) if host else None
            top_pub = max(info["supply"].items(), key=lambda x: x[1])[0]
            ps = partner_seats.get(top_pub, {})
            expected_seat = ps.get("seat")
            partner_key   = ps.get("key", "<unbridged>")

            if host and host not in live_cache:
                live_cache[host] = _live_fetch(host)
            lines = live_cache.get(host)

            # PGAM seat line for the supply path
            if not host:
                pgam_status = f"❓ no audit_host resolved for `{val}`"
                sym = "❓"
            elif lines is None:
                pgam_status = f"❓ `{host}` ads.txt unreachable"
                sym = "❓"
            elif not expected_seat:
                pgam_status = (f"⚠️ supply path via `{top_pub}` not bridged "
                               "to a pgamssp seat — can't check Layer B")
                sym = "⚠️"
            else:
                pgam_lines = [l for l in lines if l[0]=="pgamssp.com"]
                has_seat = any(l[1]==expected_seat for l in pgam_lines)
                if has_seat:
                    pgam_status = (f"✅ `pgamssp.com, {expected_seat}, RESELLER` "
                                   f"present (for {partner_key} path)")
                    sym = "✅"
                else:
                    other = sorted({l[1] for l in pgam_lines})[:3]
                    extra = (f" — page has {len(pgam_lines)} other pgamssp seats "
                             f"({', '.join(other)})" if other else
                             " — no pgamssp lines on this page at all")
                    pgam_status = (f"🚨 *missing* `pgamssp.com, {expected_seat}, RESELLER` "
                                   f"for `{partner_key}` path{extra}")
                    sym = "🚨"

            # Demand SSP misses (only for SSPs that earned revenue this period)
            demand_gaps = []
            if lines is not None:
                for ssp_name, (dom, seat) in DEMAND_CHECKS.items():
                    earned = sum(rv for nm, rv in info["demand"].items()
                                 if ssp_name.lower() in (nm or "").lower())
                    if earned <= 0:
                        continue
                    ssp_lines = [l for l in lines if l[0]==dom]
                    has_line = bool(ssp_lines) and (seat is None or
                                                     any(l[1]==seat for l in ssp_lines))
                    if not has_line:
                        why = ("no line at all" if not ssp_lines else
                               f"line present but our seat {seat} not among "
                               f"{len(ssp_lines)} {dom} entries")
                        demand_gaps.append((ssp_name, earned, why))
            demand_gaps.sort(key=lambda x: -x[1])

            body = [
                f"{sym} *{val}* — *${info['rev']:,.0f} {period_label.split()[0]}* "
                f"via `{partner_key}` ({top_pub})",
                f"     audit host: `{host or '<unresolved>'}`",
                f"     PGAM line: {pgam_status}",
            ]
            if demand_gaps:
                body.append(f"     Demand SSP gaps (with attributed $):")
                for ssp_name, ssp_rev, why in demand_gaps[:6]:
                    body.append(f"       • `{ssp_name}` — *${ssp_rev:,.0f}* — {why}")
            elif lines is not None:
                body.append(f"     Demand SSP gaps: _none among active demands_")
            blocks.append({"type":"section","text":{"type":"mrkdwn","text":"\n".join(body)}})

        blocks.append({"type":"context","elements":[{"type":"mrkdwn","text":
            f"_Snapshot table still {days_stale}d stale (latest as_of {latest_as_of}). "
            f"3/4-node SSP detection needs the `compliance_schain_emissions_24h` "
            f"rollup view from pgam-direct/web — not built yet. Audit retry will "
            f"fire at next morning window._"
        }]})
    else:
        # Couldn't pull live LL data — minimal banner
        blocks.append({"type":"section","text":{"type":"mrkdwn","text":
            f":warning: *Compliance fallback — audit and LL pull both failed*\n"
            f"_Latest snapshot: *{latest_as_of}* ({days_stale}d stale). "
            f"Auto-retry will fire at next morning window._"
        }})

    webhook = os.environ.get("COMPLIANCE_SLACK_WEBHOOK", "").strip()
    try:
        if webhook:
            _sd._post_to_compliance_webhook(
                webhook, blocks, "Compliance fallback digest")
        else:
            _slack.send_blocks(blocks, text="Compliance fallback digest")
        _slack.mark_sent_shared(_sd.DEDUPE_KEY)
        print(f"[{ACTOR}] fallback digest posted "
              f"(stale snapshot {latest_as_of}, {days_stale}d behind)")
        return {"ok": True, "posted_fallback": True, "as_of": str(latest_as_of)}
    except Exception as exc:
        print(f"[{ACTOR}] fallback digest post FAILED: {exc}")
        return {"ok": False, "error": str(exc)}


if __name__ == "__main__":
    result = run()
    sys.exit(0 if result.get("ok") else 1)
