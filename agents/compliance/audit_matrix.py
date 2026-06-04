"""
agents/compliance/audit_matrix.py

Per-(entity × SSP) compliance audit matrix.

Where the rest of the agent emits *findings* (one row per problem, only
when something is broken), this module emits an *audit row* per
(entity × SSP) pair regardless of compliance status. The matrix is the
source of truth for:

  - "what % of trailing-7d revenue is compliant"
  - "for foxsports.com, which of the 5 active SSPs are OK vs broken"
  - "give me a CSV of every transacting (domain, SSP) and its three Y/N flags"

The three explicit flags per row, all evaluated against the parsed
ads.txt content the Phase 5 crawler already pulled:

  1. pgam_direct_present     : the canonical `pgamssp.com, <id>, DIRECT`
                               line exists for ANY PGAM-owned seat
  2. ssp_line_present        : the canonical SSP reseller line exists
                               with the correct account_id
  3. sellers_json_match      : the SSP's PGAM-side seat exists in
                               PGAM sellers.json (validates the
                               *other* direction of the supply path:
                               downstream → us)

Three explicit status buckets:

  - critical  : revenue flowing but PGAM Direct OR sellers.json missing
  - warning   : revenue flowing with reseller-line mismatch only
                (incorrect seller_id, outdated entry, partial auth)
  - healthy   : all three flags green

The output is shaped to drop into a CSV with one row per (entity × SSP)
and a stable set of columns, exactly matching the brief.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date

from agents.compliance.crawlers.adstxt import AdsTxtFetch
from agents.compliance.entity_universe import Entity
from agents.compliance.ssp_registry import (
    SspExpectation,
    get_expectation,
)


# Below this floor entities are tracked in a separate low-priority
# bucket — they aren't ignored, but they don't drive Slack alerts.
DEFAULT_AUDIT_THRESHOLD_USD = 10.0


@dataclass
class AuditRow:
    # Identity
    entity_key:        str         # 'dom:foo.com' | 'app:com.bar.baz'
    kind:              str         # 'domain' | 'app'
    entity_value:      str         # 'foo.com' | 'com.bar.baz'
    audit_host:        str | None  # ads.txt host fetched
    ll_publisher_name: str | None  # which LL supply partner monetizes
    revenue_7d:        float
    ssp_key:           str
    ssp_partner_name:  str         # SSP human name (Rubicon, PubMatic…)
    ads_txt_url:       str | None

    # The three flags
    pgam_direct_present:    bool
    ssp_line_present:       bool
    sellers_json_match:     bool

    # Diagnostics
    pgam_seller_id_in_adstxt:    str | None   # what's on the line in ads.txt
    pgam_seller_id_expected:     str | None   # what should be (PGAM seller_id)
    ssp_seller_id_in_adstxt:     list[str] = field(default_factory=list)
    ssp_seller_id_expected:      str | None = None

    # Verdict
    status:             str = "healthy"           # critical | warning | healthy
    issues:             list[str] = field(default_factory=list)
    recommended_action: str = ""


@dataclass(frozen=True)
class MatrixSummary:
    total_rows:                int
    domains_audited:           int
    apps_audited:              int
    ssps_audited:              int
    revenue_audited_usd:       float
    revenue_compliant_usd:     float
    revenue_non_compliant_usd: float
    compliance_pct:            float
    critical_rows:             int
    warning_rows:              int
    healthy_rows:              int
    below_threshold_rows:      int       # tracked but not actioned


def _line_matches_domain(line, ssp_domain: str) -> bool:
    return getattr(line, "domain", "").lower() == ssp_domain.lower()


def _evaluate_pgam_direct(
    fetch: AdsTxtFetch,
    pgam_seat_registry: dict[str, dict],
    expected_seller_id: str | None,
) -> tuple[bool, str | None]:
    """Return (pgam_direct_present, seller_id_observed_on_line).

    "Present" means there's at least one `pgamssp.com, <PGAM-owned-id>, DIRECT`
    line. We don't require the seller_id to match `expected_seller_id`
    (that's a separate finding tier) — just that ANY PGAM-owned seat is
    declared as DIRECT.
    """
    if fetch is None or fetch.http_status != 200 or not fetch.lines:
        return False, None
    pgam_lines = [ln for ln in fetch.lines if _line_matches_domain(ln, "pgamssp.com")]
    if not pgam_lines:
        return False, None
    # Find a line that's both PGAM-owned and marked DIRECT.
    for ln in pgam_lines:
        if (
            ln.relationship == "DIRECT"
            and str(ln.account_id) in pgam_seat_registry
        ):
            return True, str(ln.account_id)
    # PGAM line(s) present but either wrong type or unknown seat.
    return False, str(pgam_lines[0].account_id)


def _evaluate_ssp_line(
    fetch: AdsTxtFetch,
    exp: SspExpectation,
) -> tuple[bool, list[str]]:
    """Return (ssp_line_present_with_correct_account_id, observed_account_ids).

    "Present" means there's a `<ssp_domain>, <expected_account_id>, RESELLER`
    line. Cert-authority mismatch alone counts as a soft pass (we surface
    it as a warning elsewhere, but the SSP IS authorized in ads.txt).
    """
    if fetch is None or fetch.http_status != 200 or not fetch.lines:
        return False, []
    ssp_lines = [ln for ln in fetch.lines if _line_matches_domain(ln, exp.ads_txt_domain)]
    if not ssp_lines:
        return False, []
    observed = sorted({str(ln.account_id) for ln in ssp_lines})
    matching = [
        ln for ln in ssp_lines
        if str(ln.account_id) == str(exp.account_id)
        and ln.relationship.upper() == exp.relationship.upper()
    ]
    return len(matching) > 0, observed


def _evaluate_sellers_json(
    exp: SspExpectation,
    pgam_seat_registry: dict[str, dict],
) -> bool:
    """Check that the SSP's downstream PGAM seat exists in PGAM sellers.json.

    The SspExpectation carries `pgam_seller_id_in_their_sellers_json` if
    we've cataloged it. Cross-direction validation: this is the OTHER
    direction of the supply path (downstream → us). Phase 3's
    downstream sellers.json audit already does the canonical check; we
    surface the result alongside the per-(entity × SSP) row so it's
    visible in the matrix.
    """
    target_seat = getattr(exp, "pgam_seller_id", None) or getattr(
        exp, "pgam_seller_id_in_their_sellers_json", None
    )
    if not target_seat:
        # Registry doesn't carry it; conservative default to True so we
        # don't mark every row warning just because we haven't cataloged
        # the PGAM-side seat. Phase 3 still raises a downstream finding.
        return True
    return str(target_seat) in pgam_seat_registry


def _classify(row: AuditRow,
              effective_pgam_present: bool | None = None,
              expected_pgam_line: str | None = None) -> tuple[str, list[str], str]:
    """Derive (status, issues, recommended_action) from the flags.

    ``effective_pgam_present`` overrides ``row.pgam_direct_present`` for
    the "is the PGAM line in place" determination. It exists because
    the matrix's DIRECT-only check is wrong for via_partner entities —
    those entities should have a partner-specific RESELLER line, not a
    DIRECT one. supply_path_audit knows the path kind per entity and
    computes the correct value; we pass it in here so the matrix's
    critical/warning/healthy bucketing matches reality.

    ``expected_pgam_line`` is the path-correct copy-paste line for the
    Slack action card ("Add `pgamssp.com, <seat>, RESELLER`" for via
    partners, "…DIRECT" for pgam_direct). Falls back to the matrix's
    DIRECT-shaped guess if not provided.
    """
    pgam_present = (
        effective_pgam_present
        if effective_pgam_present is not None
        else row.pgam_direct_present
    )
    issues: list[str] = []
    actions: list[str] = []

    if not pgam_present:
        issues.append("PGAM line missing for this supply path")
        if expected_pgam_line:
            actions.append(
                f"Add to {row.audit_host or 'publisher'} ads.txt: "
                f"`{expected_pgam_line}`"
            )
        else:
            seat_placeholder = row.pgam_seller_id_expected or "<publisher's PGAM seller_id>"
            actions.append(
                f"Add to {row.audit_host or 'publisher'} ads.txt: "
                f"`pgamssp.com, {seat_placeholder}, DIRECT`"
            )
    if not row.ssp_line_present:
        if row.ssp_seller_id_in_adstxt:
            issues.append(
                f"{row.ssp_partner_name} line present but wrong account_id "
                f"(found {', '.join(row.ssp_seller_id_in_adstxt[:3])}"
                f"{'…' if len(row.ssp_seller_id_in_adstxt) > 3 else ''}, "
                f"expect {row.ssp_seller_id_expected})"
            )
        else:
            issues.append(f"{row.ssp_partner_name} reseller line missing")
        if row.ssp_seller_id_expected:
            actions.append(
                f"Add/replace in {row.audit_host or 'publisher'} ads.txt the "
                f"canonical reseller line for {row.ssp_partner_name}"
            )
    if not row.sellers_json_match:
        issues.append(
            f"{row.ssp_partner_name} seat not declared in PGAM sellers.json"
        )
        actions.append(
            f"Add the {row.ssp_partner_name}-issued PGAM seat to "
            f"https://sellers.pgamssp.com/62ebe78298926f0faf3a822a/sellers.json"
        )

    if not pgam_present or not row.sellers_json_match:
        status = "critical"
    elif not row.ssp_line_present:
        status = "warning"
    else:
        status = "healthy"

    return status, issues, " ; ".join(actions)


def build_audit_matrix(
    entities: list[Entity],
    fetches_by_entity: dict[str, AdsTxtFetch | None],
    pgam_seat_registry: dict[str, dict],
    threshold_usd: float = DEFAULT_AUDIT_THRESHOLD_USD,
    supply_path_by_entity: dict[str, object] | None = None,
) -> tuple[list[AuditRow], MatrixSummary]:
    """Produce one AuditRow per (entity × active_ssp), plus the summary.

    Rows below `threshold_usd` revenue still come back (we want the
    record), but they're tagged 'below_threshold' in the issues list so
    callers can filter them out of the digest.

    ``supply_path_by_entity`` is an optional pre-computed lookup from
    ``build_supply_path_audit``. When supplied, the per-entity path
    classification (pgam_direct vs via_partner) drives the matrix's
    PGAM-line check — fixing a false-positive where via_partner
    entities with a correct RESELLER line were marked critical because
    the matrix's DIRECT-only check returned False. The lookup values
    are ``SupplyPathRow`` dataclasses (declared via ``object`` here to
    avoid a circular import).
    """
    supply_path_by_entity = supply_path_by_entity or {}
    rows: list[AuditRow] = []
    below_threshold = 0
    seen_ssps: set[str] = set()
    # For correct revenue accounting we have to attribute each entity's
    # revenue ONCE to the audit, not once per SSP row. Otherwise a
    # 5-SSP entity inflates the denominator 5×. Track per-entity
    # status: an entity counts as compliant iff EVERY SSP row is
    # healthy (the supply path is only as strong as its weakest leg).
    entity_status: dict[str, dict] = {}  # entity_key → {revenue, has_non_healthy}

    for entity in entities:
        if not entity.active_ssps:
            # No SSP observed monetizing — there's nothing to per-SSP
            # audit. Universe-level findings (Phase 5 universal-DIRECT)
            # still surface elsewhere.
            continue

        fetch = fetches_by_entity.get(entity.entity_key)
        url = None
        if fetch is not None:
            url = getattr(fetch, "url", None)

        # PGAM-Direct check is per-entity (not per-SSP). Compute once.
        pgam_present, pgam_observed = _evaluate_pgam_direct(
            fetch, pgam_seat_registry, entity.expected_seller_id,
        )

        entity_status[entity.entity_key] = {
            "revenue": entity.revenue_7d,
            "any_non_healthy": False,
        }

        for ssp_key in entity.active_ssps:
            seen_ssps.add(ssp_key)
            exp = get_expectation(ssp_key)
            if exp is None:
                continue  # Phase 7's unmapped-demand check covers this
            ssp_present, ssp_observed = _evaluate_ssp_line(fetch, exp)
            json_match = _evaluate_sellers_json(exp, pgam_seat_registry)

            row = AuditRow(
                entity_key=entity.entity_key,
                kind=entity.kind,
                entity_value=entity.entity_value,
                audit_host=entity.audit_host,
                ll_publisher_name=entity.ll_publisher_name,
                revenue_7d=round(entity.revenue_7d, 2),
                ssp_key=ssp_key,
                ssp_partner_name=getattr(exp, "display_name", None) or ssp_key.title(),
                ads_txt_url=url,
                pgam_direct_present=pgam_present,
                ssp_line_present=ssp_present,
                sellers_json_match=json_match,
                pgam_seller_id_in_adstxt=pgam_observed,
                pgam_seller_id_expected=entity.expected_seller_id,
                ssp_seller_id_in_adstxt=ssp_observed,
                ssp_seller_id_expected=str(exp.account_id),
            )
            # If supply_path_audit already evaluated this entity, use
            # its path-aware B-layer verdict instead of the matrix's
            # DIRECT-only check. Otherwise fall back to the raw flag.
            sp = supply_path_by_entity.get(entity.entity_key)
            effective_pgam = getattr(sp, "pgam_line_present_for_path", None) if sp else None
            expected_line = getattr(sp, "expected_pgam_line", None) if sp else None
            row.status, row.issues, row.recommended_action = _classify(
                row,
                effective_pgam_present=effective_pgam,
                expected_pgam_line=expected_line,
            )

            if entity.revenue_7d < threshold_usd:
                below_threshold += 1
                row.issues.insert(0, f"(below ${threshold_usd:.0f}/7d threshold)")

            if row.status != "healthy":
                entity_status[entity.entity_key]["any_non_healthy"] = True
            rows.append(row)

    # Revenue accounting: each entity contributes ONCE to the
    # denominator. An entity is "compliant" only if every SSP path
    # through it is healthy — the supply chain is only as strong as
    # its weakest leg.
    revenue_audited = sum(e["revenue"] for e in entity_status.values())
    revenue_compliant = sum(
        e["revenue"] for e in entity_status.values() if not e["any_non_healthy"]
    )
    revenue_non_compliant = revenue_audited - revenue_compliant

    domains = sum(1 for e in entities if e.kind == "domain" and e.active_ssps)
    apps = sum(1 for e in entities if e.kind == "app" and e.active_ssps)
    compliance_pct = (
        100.0 * revenue_compliant / revenue_audited
        if revenue_audited > 0 else 100.0
    )

    summary = MatrixSummary(
        total_rows=len(rows),
        domains_audited=domains,
        apps_audited=apps,
        ssps_audited=len(seen_ssps),
        revenue_audited_usd=round(revenue_audited, 2),
        revenue_compliant_usd=round(revenue_compliant, 2),
        revenue_non_compliant_usd=round(revenue_non_compliant, 2),
        compliance_pct=round(compliance_pct, 1),
        critical_rows=sum(1 for r in rows if r.status == "critical"),
        warning_rows=sum(1 for r in rows if r.status == "warning"),
        healthy_rows=sum(1 for r in rows if r.status == "healthy"),
        below_threshold_rows=below_threshold,
    )
    return rows, summary


# ── CSV serialization ───────────────────────────────────────────────────────

CSV_COLUMNS = [
    "as_of", "entity_key", "kind", "entity_value", "audit_host",
    "ll_publisher_name", "revenue_7d",
    "ssp_key", "ssp_partner_name", "ads_txt_url",
    "pgam_direct_present", "pgam_seller_id_in_adstxt", "pgam_seller_id_expected",
    "ssp_line_present", "ssp_seller_id_in_adstxt", "ssp_seller_id_expected",
    "sellers_json_match",
    "status", "issues", "recommended_action",
]


def rows_to_csv(rows: list[AuditRow], as_of: date | None = None) -> str:
    """Render the matrix as CSV (no file I/O — caller decides where it lands)."""
    import csv
    import io
    as_of = as_of or date.today()
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
    w.writeheader()
    for r in rows:
        d = asdict(r)
        d["as_of"] = as_of.isoformat()
        d["issues"] = " | ".join(r.issues)
        d["ssp_seller_id_in_adstxt"] = " | ".join(r.ssp_seller_id_in_adstxt)
        w.writerow({k: d.get(k, "") for k in CSV_COLUMNS})
    return buf.getvalue()


# ── Neon persistence ────────────────────────────────────────────────────────

_INSERT_SQL = """
INSERT INTO pgam_direct.compliance_entity_ssp_audit
    (as_of, entity_key, kind, entity_value, audit_host, ll_publisher_name,
     revenue_7d, ssp_key, ssp_partner_name, ads_txt_url,
     pgam_direct_present, pgam_seller_id_in_adstxt, pgam_seller_id_expected,
     ssp_line_present, ssp_seller_id_in_adstxt, ssp_seller_id_expected,
     sellers_json_match, status, issues, recommended_action, audited_at)
VALUES
    (%(as_of)s, %(entity_key)s, %(kind)s, %(entity_value)s, %(audit_host)s,
     %(ll_publisher_name)s, %(revenue_7d)s, %(ssp_key)s, %(ssp_partner_name)s,
     %(ads_txt_url)s, %(pgam_direct_present)s, %(pgam_seller_id_in_adstxt)s,
     %(pgam_seller_id_expected)s, %(ssp_line_present)s,
     %(ssp_seller_id_in_adstxt)s, %(ssp_seller_id_expected)s,
     %(sellers_json_match)s, %(status)s, %(issues)s, %(recommended_action)s,
     now())
ON CONFLICT (entity_key, ssp_key, as_of) DO UPDATE SET
    revenue_7d                = EXCLUDED.revenue_7d,
    audit_host                = EXCLUDED.audit_host,
    ll_publisher_name         = EXCLUDED.ll_publisher_name,
    ads_txt_url               = EXCLUDED.ads_txt_url,
    pgam_direct_present       = EXCLUDED.pgam_direct_present,
    pgam_seller_id_in_adstxt  = EXCLUDED.pgam_seller_id_in_adstxt,
    pgam_seller_id_expected   = EXCLUDED.pgam_seller_id_expected,
    ssp_line_present          = EXCLUDED.ssp_line_present,
    ssp_seller_id_in_adstxt   = EXCLUDED.ssp_seller_id_in_adstxt,
    ssp_seller_id_expected    = EXCLUDED.ssp_seller_id_expected,
    sellers_json_match        = EXCLUDED.sellers_json_match,
    status                    = EXCLUDED.status,
    issues                    = EXCLUDED.issues,
    recommended_action        = EXCLUDED.recommended_action,
    audited_at                = now();
"""


def persist_matrix(rows: list[AuditRow], as_of: date | None = None) -> int:
    """UPSERT the matrix into pgam_direct.compliance_entity_ssp_audit."""
    from core.neon import connect
    as_of = as_of or date.today()
    if not rows:
        return 0
    payload = []
    for r in rows:
        payload.append({
            "as_of":                     as_of,
            "entity_key":                r.entity_key,
            "kind":                      r.kind,
            "entity_value":              r.entity_value,
            "audit_host":                r.audit_host,
            "ll_publisher_name":         r.ll_publisher_name,
            "revenue_7d":                r.revenue_7d,
            "ssp_key":                   r.ssp_key,
            "ssp_partner_name":          r.ssp_partner_name,
            "ads_txt_url":               r.ads_txt_url,
            "pgam_direct_present":       r.pgam_direct_present,
            "pgam_seller_id_in_adstxt":  r.pgam_seller_id_in_adstxt,
            "pgam_seller_id_expected":   r.pgam_seller_id_expected,
            "ssp_line_present":          r.ssp_line_present,
            "ssp_seller_id_in_adstxt":   list(r.ssp_seller_id_in_adstxt),
            "ssp_seller_id_expected":    r.ssp_seller_id_expected,
            "sellers_json_match":        r.sellers_json_match,
            "status":                    r.status,
            "issues":                    list(r.issues),
            "recommended_action":        r.recommended_action,
        })
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_INSERT_SQL, payload)
        conn.commit()
    return len(payload)
