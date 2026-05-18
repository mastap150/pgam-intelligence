"""
agents/compliance/runner.py

Supply Compliance & Quality Intelligence agent — Phase 1 orchestrator.

Daily run:
  1. Fetch PGAM sellers.json -> rebuild compliance_publishers universe
  2. Ensure schema (idempotent CREATE TABLE IF NOT EXISTS)
  3. For each publisher: fetch ads.txt (+ app-ads.txt if present)
     and persist fetch metadata
  4. Validate the universal `pgammedia.com, <seller_id>, DIRECT` line
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

from agents.compliance.crawlers.adstxt import AdsTxtFetch, fetch_adstxt  # noqa: E402
from agents.compliance.crawlers.downstream_sellersjson import (  # noqa: E402
    fetch_downstream_sellers_json,
)
from agents.compliance.findings import resolve_cleared, upsert_findings  # noqa: E402
from agents.compliance.ll_bridge import run_bridge  # noqa: E402
from agents.compliance.observed_monetization import (  # noqa: E402
    load_observed_for_publishers,
    refresh_observed_monetization,
)
from agents.compliance.reporters.slack_digest import post_digest  # noqa: E402
from agents.compliance.scoring import refresh_publisher_scores  # noqa: E402
from agents.compliance.ssp_registry import PHASE_2_SSP_EXPECTATIONS  # noqa: E402
from agents.compliance.universe import Publisher, build_universe, sync_universe  # noqa: E402
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
        fetch_adstxt(pub.publisher_key, pub.domain, variant="ads.txt"),
    ]
    if app_ads:
        out.append(fetch_adstxt(pub.publisher_key, pub.domain, variant="app-ads.txt"))
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
) -> list[Finding]:
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


def run() -> dict:
    """Scheduler entry. Returns a summary dict."""
    started_at = datetime.now(timezone.utc)
    print(f"[{ACTOR}] start {started_at.isoformat()}")

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

    summary: dict = {
        "started_at": started_at,
        "publishers_scanned": 0,
        "adstxt_fetched": 0,
        "ll_bridge_matched": 0,
        "ll_bridge_unmatched": 0,
        "observed_ssp_rows": 0,
        "ssps_audited": 0,
        "scores_written": 0,
        "avg_score": 0.0,
        "findings_opened": 0,
        "findings_resolved": 0,
        "ok": False,
        "error": None,
    }

    try:
        _ensure_schema()

        publishers = build_universe()
        if limit:
            publishers = publishers[:limit]
        upserted, deactivated = sync_universe(publishers)
        print(f"[{ACTOR}] universe upserted={upserted} deactivated={deactivated}")

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

        findings = _validate_all(publishers, fetches,
                                 app_ads=app_ads,
                                 enable_resellers=enable_resellers)

        # Phase 3: downstream sellers.json audit. Each SSP-level finding
        # uses a sentinel publisher_key '_ssp:<key>' so the upsert
        # pipeline handles them uniformly. Scoring excludes sentinels.
        ssp_sentinel_keys: list[str] = []
        if enable_downstream:
            ssp_findings, ssp_sentinel_keys = _audit_downstream_ssps()
            findings.extend(ssp_findings)
            summary["ssps_audited"] = len(ssp_sentinel_keys)

        opened, total = upsert_findings(findings)
        print(f"[{ACTOR}] findings: total={total} newly_opened={opened}")

        seen = [(f.publisher_key, f.check_id, f.fingerprint) for f in findings]
        # Only auto-resolve for publishers whose ads.txt actually returned 200.
        # An unreachable file is "I don't know" — don't infer "fixed".
        # SSP sentinels auto-resolve when the next downstream audit clears them.
        reachable_pubs = sorted({
            f.publisher_key for f in fetches
            if f.variant == "ads.txt" and f.http_status == 200
        })
        resolved = resolve_cleared(reachable_pubs + ssp_sentinel_keys, seen)
        print(f"[{ACTOR}] auto-resolved {resolved} previously-open findings")

        # Phase 3: per-publisher score, AFTER findings have been upserted.
        if enable_scoring:
            try:
                score_stats = refresh_publisher_scores()
                summary["scores_written"] = score_stats.rows_written
                summary["avg_score"]      = score_stats.avg_score
                print(
                    f"[{ACTOR}] scores written={score_stats.rows_written} "
                    f"avg={score_stats.avg_score} "
                    f"below_75={score_stats.publishers_below_75}"
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

    _write_run_log(summary)
    print(
        f"[{ACTOR}] done ok={summary['ok']} "
        f"pubs={summary['publishers_scanned']} "
        f"opened={summary['findings_opened']} "
        f"resolved={summary['findings_resolved']}"
    )
    return summary


if __name__ == "__main__":
    result = run()
    sys.exit(0 if result.get("ok") else 1)
