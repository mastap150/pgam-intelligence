"""
agents/compliance/supply_path_audit.py

Per-entity SUPPLY-PATH audit. Asks the correct compliance question:

   "For this domain/app, which LL supply partner is bringing the
    inventory to us, and is that supply path properly declared in the
    publisher's ads.txt + in our + the partner's sellers.json files?"

Distinct from the demand-side audit (`audit_matrix.py`), which asks
"which SSPs are buying through us" — that's revenue visibility, not
compliance. Demand-side SSPs like Sharethrough buyers don't need to
appear in a publisher's ads.txt; the publisher only needs to authorize
its supply partner.

Hybrid model — every entity falls into one of two paths:

  pgam_direct
    The entity's domain is in OUR sellers.json as a PUBLISHER. We're
    its direct supply partner. Expected ads.txt line:
        pgamssp.com, <entity-specific PGAM seller_id>, DIRECT

  via_partner
    The entity is brought to us by an LL supply partner (Smaato,
    BidMachine, Start.IO, etc.) that's listed as INTERMEDIARY in our
    sellers.json. The entity's relationship is with that partner, not
    with us. Expected ads.txt lines:
        <partner_domain>, <publisher's seat with partner>, DIRECT
        pgamssp.com, <partner's PGAM seat in our sellers.json>, RESELLER

  unknown
    Couldn't classify — usually means the LL supply partner isn't
    bridged to a sellers.json entry yet. Flagged for the bridge gap
    report, not for ads.txt fixes.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date

from agents.compliance.crawlers.adstxt import AdsTxtFetch
from agents.compliance.entity_universe import Entity


# Below this revenue floor, entities are tracked but not flagged loud.
DEFAULT_AUDIT_THRESHOLD_USD = 10.0


@dataclass
class SupplyPathRow:
    # Identity
    entity_key:                str          # 'dom:<>' | 'app:<>'
    kind:                      str          # 'domain' | 'app'
    entity_value:              str
    audit_host:                str | None
    revenue_7d:                float

    # Path classification
    path_kind:                 str          # 'pgam_direct' | 'via_partner' | 'unknown'
    ll_publisher_id:           str | None
    ll_publisher_name:         str | None
    supply_partner_key:        str | None   # e.g. 'smaato.com' (compliance_publishers.publisher_key)
    supply_partner_domain:     str | None   # ads.txt-relevant domain, e.g. 'smaato.com'
    supply_partner_pgam_seat:  str | None   # PGAM seat the partner holds in our sellers.json

    # The compliance flags
    supply_partner_line_present:    bool    # for via_partner: is partner's domain on publisher ads.txt?
    pgam_line_present_for_path:     bool    # DIRECT for pgam_direct, RESELLER (with partner's seat) for via_partner
    sellers_json_partner_declared:  bool    # our sellers.json declares this partner

    # Diagnostics
    expected_pgam_line:        str | None
    observed_pgam_seats:       list[str] = field(default_factory=list)
    observed_partner_seats:    list[str] = field(default_factory=list)

    # Layer 5 — emitted schain validation per pair. Populated from the
    # compliance_schain_emissions_24h ClickHouse rollup (built in
    # pgam-direct/web; not yet live). When the source view is absent,
    # all three fields stay None and the rollup is treated as "unknown".
    schain_emitted_ok:         bool | None = None
    schain_emissions_24h:      int | None = None
    schain_incomplete_rate:    float | None = None
    schain_hop_violation_rate: float | None = None

    # Verdict
    status:                    str = "healthy"   # critical | warning | healthy | unknown
    issues:                    list[str] = field(default_factory=list)
    recommended_action:        str = ""


@dataclass(frozen=True)
class SupplyPathSummary:
    total_rows:                  int
    pgam_direct_rows:            int
    via_partner_rows:            int
    unknown_rows:                int
    revenue_audited_usd:         float
    revenue_compliant_usd:       float
    revenue_at_risk_usd:         float
    compliance_pct:              float
    critical_rows:               int
    warning_rows:                int
    healthy_rows:                int


# ── PGAM seat registry helpers ──────────────────────────────────────────────


def _line_matches_domain(line, domain: str) -> bool:
    return getattr(line, "domain", "").lower() == (domain or "").lower()


def _pgam_lines(fetch: AdsTxtFetch | None) -> list:
    if fetch is None or fetch.http_status != 200 or not fetch.lines:
        return []
    return [ln for ln in fetch.lines if _line_matches_domain(ln, "pgamssp.com")]


def _observed_partner_seats(fetch: AdsTxtFetch | None,
                            partner_domain: str | None) -> list[str]:
    if not partner_domain or fetch is None or fetch.http_status != 200:
        return []
    return sorted({
        str(ln.account_id) for ln in (fetch.lines or [])
        if _line_matches_domain(ln, partner_domain)
    })


def _evaluate_pgam_direct(fetch: AdsTxtFetch | None,
                          pgam_seat_registry: dict[str, dict],
                          expected_seat: str | None) -> tuple[bool, list[str]]:
    """For pgam_direct path: pgamssp.com line present with a PGAM-owned
    seat marked DIRECT. Returns (present, observed_seat_ids)."""
    lines = _pgam_lines(fetch)
    observed = sorted({str(ln.account_id) for ln in lines})
    for ln in lines:
        if (ln.relationship == "DIRECT"
                and str(ln.account_id) in pgam_seat_registry):
            return True, observed
    return False, observed


def _evaluate_pgam_reseller(fetch: AdsTxtFetch | None,
                            expected_partner_seat: str) -> tuple[bool, list[str]]:
    """For via_partner path: pgamssp.com line present with the partner's
    specific PGAM seat marked RESELLER. Returns (present, observed_seat_ids)."""
    lines = _pgam_lines(fetch)
    observed = sorted({str(ln.account_id) for ln in lines})
    for ln in lines:
        if (str(ln.account_id) == str(expected_partner_seat)
                and ln.relationship == "RESELLER"):
            return True, observed
    return False, observed


def _classify(row: SupplyPathRow) -> tuple[str, list[str], str]:
    """Derive status + issues + recommended_action."""
    issues: list[str] = []
    actions: list[str] = []

    if row.path_kind == "unknown":
        issues.append(
            f"LL supply partner {row.ll_publisher_name!r} not bridged "
            "to any sellers.json entry"
        )
        actions.append(
            "Bridge the LL supply partner in compliance_ll_partner_bridge "
            "so we know which ads.txt domain + PGAM seat to expect."
        )
        return "warning", issues, " ; ".join(actions)

    if row.path_kind == "pgam_direct":
        if not row.pgam_line_present_for_path:
            issues.append("PGAM Direct line missing or wrong seat")
            actions.append(
                f"Add to {row.audit_host or 'publisher'} ads.txt: "
                f"`pgamssp.com, <entity's PGAM seller_id>, DIRECT`"
            )
        if not row.sellers_json_partner_declared:
            # For pgam_direct, this means our sellers.json doesn't have
            # an entry for the entity's domain. Less common but possible.
            issues.append("Entity not declared in our sellers.json as PUBLISHER")
            actions.append(
                "Add entity to https://sellers.pgamssp.com/...sellers.json"
            )

    elif row.path_kind == "via_partner":
        if not row.supply_partner_line_present:
            issues.append(
                f"Supply partner ({row.supply_partner_domain}) line missing "
                "from publisher ads.txt"
            )
            actions.append(
                f"Get publisher to add: "
                f"`{row.supply_partner_domain}, <their seat with partner>, DIRECT`"
            )
        if not row.pgam_line_present_for_path:
            if row.observed_pgam_seats:
                issues.append(
                    f"PGAM line present but wrong seat — "
                    f"found {', '.join(row.observed_pgam_seats[:3])} "
                    f"(expected {row.supply_partner_pgam_seat} for {row.supply_partner_key})"
                )
            else:
                issues.append("PGAM RESELLER line missing (downstream of supply partner)")
            actions.append(
                f"Add to {row.audit_host or 'publisher'} ads.txt: "
                f"`pgamssp.com, {row.supply_partner_pgam_seat}, RESELLER`"
            )
        if not row.sellers_json_partner_declared:
            issues.append(
                f"Supply partner ({row.supply_partner_key}) not declared "
                "in our sellers.json as INTERMEDIARY"
            )
            actions.append(
                f"Add {row.supply_partner_key} to PGAM sellers.json"
            )

    # Status from flags
    if row.path_kind == "via_partner":
        if (not row.supply_partner_line_present
                or not row.pgam_line_present_for_path):
            status = "critical"
        elif not row.sellers_json_partner_declared:
            status = "warning"
        else:
            status = "healthy"
    elif row.path_kind == "pgam_direct":
        if not row.pgam_line_present_for_path:
            status = "critical"
        elif not row.sellers_json_partner_declared:
            status = "warning"
        else:
            status = "healthy"
    else:
        status = "warning"

    return status, issues, " ; ".join(actions)


# ── Builders for the partner-lookup tables ──────────────────────────────────


def _build_ll_partner_lookup() -> dict[str, dict]:
    """Map ll_publisher_id → {publisher_key, domain, seller_id, seller_type}.

    Joins compliance_ll_partner_bridge → compliance_publishers so the
    supply-path audit knows which ads.txt domain + PGAM seat to expect
    for each LL supply partner.
    """
    from core.neon import connect
    out: dict[str, dict] = {}
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.ll_publisher_id, cp.publisher_key, cp.domain,
                       cp.seller_id, cp.seller_type
                FROM pgam_direct.compliance_ll_partner_bridge b
                JOIN pgam_direct.compliance_publishers cp
                  ON cp.publisher_key = b.publisher_key
                WHERE cp.is_active = TRUE;
            """)
            for ll_id, pub_key, domain, seller_id, seller_type in cur.fetchall():
                if not ll_id:
                    continue
                out[str(ll_id)] = {
                    "publisher_key": pub_key,
                    "domain":        domain,
                    "seller_id":     seller_id,
                    "seller_type":   (seller_type or "").upper(),
                }
    return out


def _build_pgam_direct_publisher_lookup() -> dict[str, dict]:
    """Domains in our sellers.json declared as PUBLISHER (or BOTH).

    Used to identify entities that are direct PGAM publishers vs ones
    that flow through an INTERMEDIARY supply partner.
    """
    from core.neon import connect
    out: dict[str, dict] = {}
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT publisher_key, domain, seller_id, seller_type
                FROM pgam_direct.compliance_publishers
                WHERE is_active = TRUE
                  AND seller_type IN ('PUBLISHER', 'BOTH');
            """)
            for pub_key, domain, seller_id, seller_type in cur.fetchall():
                if not domain:
                    continue
                out[domain.lower()] = {
                    "publisher_key": pub_key,
                    "seller_id":     seller_id,
                    "seller_type":   (seller_type or "").upper(),
                }
    return out


def _load_schain_emissions_24h() -> dict[tuple[str, str], dict]:
    """Load per-(publisher_id, supply_partner) emission rollup if the
    ClickHouse → Postgres view exists. Returns empty dict if absent;
    caller treats missing as 'unknown'."""
    from core.neon import connect
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT publisher_id, supply_partner,
                       emissions, incomplete_rate, hop_violation_rate
                FROM pgam_direct.compliance_schain_emissions_24h
                WHERE emissions > 0;
            """)
            cols = [c.name for c in cur.description]
            out = {}
            for r in cur.fetchall():
                row = dict(zip(cols, r))
                key = (str(row.get("publisher_id") or ""),
                       str(row.get("supply_partner") or "").lower())
                out[key] = row
            return out
    except Exception:
        # View doesn't exist yet — treat as unknown for every pair.
        return {}


def _evaluate_schain(
    ll_publisher_id: str | None,
    supply_partner_key: str | None,
    emissions_map: dict[tuple[str, str], dict],
) -> tuple[bool | None, int | None, float | None, float | None]:
    """Layer 5: per-(entity-LL-pub × partner) schain emission verdict.

    Returns (ok, emissions, incomplete_rate, hop_violation_rate). ok is:
      TRUE  — emissions seen and complete=1 + hops<=2 across the window
      FALSE — emissions seen but schain validation failing materially
      NULL  — no emission data available (ClickHouse view absent or
              no bid traffic in trailing 24h)
    """
    if not ll_publisher_id or not supply_partner_key:
        return None, None, None, None
    key = (str(ll_publisher_id), supply_partner_key.lower())
    row = emissions_map.get(key)
    if row is None:
        return None, None, None, None
    emissions = int(row.get("emissions") or 0)
    inc_rate = float(row.get("incomplete_rate") or 0)
    hop_rate = float(row.get("hop_violation_rate") or 0)
    # Use the same thresholds as the standalone dynamic_schain validator.
    # Threshold: any incomplete > 0 or hop_violation > 0 counts as not OK.
    ok = (inc_rate == 0 and hop_rate == 0)
    return ok, emissions, inc_rate, hop_rate


def build_supply_path_audit(
    entities: list[Entity],
    fetches_by_entity: dict[str, AdsTxtFetch | None],
    pgam_seat_registry: dict[str, dict],
    threshold_usd: float = DEFAULT_AUDIT_THRESHOLD_USD,
) -> tuple[list[SupplyPathRow], SupplyPathSummary]:
    """Build the per-entity supply-path audit + summary KPIs.

    Each entity gets ONE row (per-entity, not per-SSP) representing the
    primary supply path bringing its inventory to us.
    """
    ll_lookup = _build_ll_partner_lookup()
    pgam_direct = _build_pgam_direct_publisher_lookup()
    # Layer 5: pre-load the schain emissions map once. Empty if the
    # ClickHouse → Postgres rollup view isn't built yet — every row
    # then gets schain_emitted_ok=NULL (unknown).
    emissions_map = _load_schain_emissions_24h()

    rows: list[SupplyPathRow] = []
    rev_audited = 0.0
    rev_compliant = 0.0

    for entity in entities:
        fetch = fetches_by_entity.get(entity.entity_key)
        normalized_domain = (entity.entity_value or "").lower().strip()
        # For app entities the entity_value is a bundle — they're
        # never pgam_direct (publishers register by domain, not by
        # bundle), so app entities always classify as via_partner
        # (or unknown if unbridged).
        is_app = entity.kind == "app"
        path_kind: str
        sp_key: str | None = None
        sp_domain: str | None = None
        sp_pgam_seat: str | None = None
        sellers_json_partner_declared: bool = False

        if not is_app and normalized_domain in pgam_direct:
            path_kind = "pgam_direct"
            sp_key = pgam_direct[normalized_domain]["publisher_key"]
            sp_domain = "pgamssp.com"
            sp_pgam_seat = pgam_direct[normalized_domain]["seller_id"]
            sellers_json_partner_declared = True
        else:
            partner = ll_lookup.get(str(entity.ll_publisher_id or ""))
            if partner and partner.get("domain"):
                path_kind = "via_partner"
                sp_key = partner["publisher_key"]
                sp_domain = partner["domain"]
                sp_pgam_seat = partner["seller_id"]
                sellers_json_partner_declared = True
            else:
                path_kind = "unknown"

        # Evaluate the path-specific PGAM line presence.
        if path_kind == "pgam_direct":
            pgam_ok, pgam_obs = _evaluate_pgam_direct(
                fetch, pgam_seat_registry, sp_pgam_seat,
            )
        elif path_kind == "via_partner" and sp_pgam_seat:
            pgam_ok, pgam_obs = _evaluate_pgam_reseller(fetch, sp_pgam_seat)
        else:
            pgam_ok, pgam_obs = False, [
                str(ln.account_id) for ln in _pgam_lines(fetch)
            ]

        # For via_partner, also check the partner's own domain line.
        partner_obs = _observed_partner_seats(fetch, sp_domain) \
            if (path_kind == "via_partner" and sp_domain) else []
        partner_line_present = (
            len(partner_obs) > 0 if path_kind == "via_partner" else True
        )

        expected_pgam = None
        if path_kind == "pgam_direct":
            expected_pgam = f"pgamssp.com, {sp_pgam_seat}, DIRECT"
        elif path_kind == "via_partner" and sp_pgam_seat:
            expected_pgam = f"pgamssp.com, {sp_pgam_seat}, RESELLER"

        # Layer 5 lookup — emitted schain validation per pair.
        schain_ok, schain_em, schain_inc, schain_hop = _evaluate_schain(
            entity.ll_publisher_id, sp_key, emissions_map,
        )

        row = SupplyPathRow(
            entity_key=entity.entity_key,
            kind=entity.kind,
            entity_value=entity.entity_value,
            audit_host=entity.audit_host,
            revenue_7d=round(entity.revenue_7d, 2),
            path_kind=path_kind,
            ll_publisher_id=entity.ll_publisher_id,
            ll_publisher_name=entity.ll_publisher_name,
            supply_partner_key=sp_key,
            supply_partner_domain=sp_domain,
            supply_partner_pgam_seat=sp_pgam_seat,
            supply_partner_line_present=partner_line_present,
            pgam_line_present_for_path=pgam_ok,
            sellers_json_partner_declared=sellers_json_partner_declared,
            expected_pgam_line=expected_pgam,
            observed_pgam_seats=pgam_obs,
            observed_partner_seats=partner_obs,
            schain_emitted_ok=schain_ok,
            schain_emissions_24h=schain_em,
            schain_incomplete_rate=schain_inc,
            schain_hop_violation_rate=schain_hop,
        )
        row.status, row.issues, row.recommended_action = _classify(row)

        if entity.revenue_7d < threshold_usd:
            row.issues.insert(0, f"(below ${threshold_usd:.0f}/7d threshold)")

        rows.append(row)
        rev_audited += entity.revenue_7d
        if row.status == "healthy":
            rev_compliant += entity.revenue_7d

    rev_at_risk = rev_audited - rev_compliant
    pct = 100.0 * rev_compliant / rev_audited if rev_audited > 0 else 100.0

    summary = SupplyPathSummary(
        total_rows=len(rows),
        pgam_direct_rows=sum(1 for r in rows if r.path_kind == "pgam_direct"),
        via_partner_rows=sum(1 for r in rows if r.path_kind == "via_partner"),
        unknown_rows=sum(1 for r in rows if r.path_kind == "unknown"),
        revenue_audited_usd=round(rev_audited, 2),
        revenue_compliant_usd=round(rev_compliant, 2),
        revenue_at_risk_usd=round(rev_at_risk, 2),
        compliance_pct=round(pct, 1),
        critical_rows=sum(1 for r in rows if r.status == "critical"),
        warning_rows=sum(1 for r in rows if r.status == "warning"),
        healthy_rows=sum(1 for r in rows if r.status == "healthy"),
    )
    return rows, summary


# ── Neon persistence ────────────────────────────────────────────────────────


_UPSERT_SQL = """
INSERT INTO pgam_direct.compliance_entity_supply_path_audit
    (as_of, entity_key, kind, entity_value, audit_host, revenue_7d,
     path_kind, ll_publisher_id, ll_publisher_name,
     supply_partner_key, supply_partner_domain, supply_partner_pgam_seat,
     supply_partner_line_present, pgam_line_present_for_path,
     sellers_json_partner_declared,
     expected_pgam_line, observed_pgam_seats, observed_partner_seats,
     schain_emitted_ok, schain_emissions_24h,
     schain_incomplete_rate, schain_hop_violation_rate,
     status, issues, recommended_action, audited_at)
VALUES
    (%(as_of)s, %(entity_key)s, %(kind)s, %(entity_value)s, %(audit_host)s,
     %(revenue_7d)s, %(path_kind)s, %(ll_publisher_id)s, %(ll_publisher_name)s,
     %(supply_partner_key)s, %(supply_partner_domain)s, %(supply_partner_pgam_seat)s,
     %(supply_partner_line_present)s, %(pgam_line_present_for_path)s,
     %(sellers_json_partner_declared)s,
     %(expected_pgam_line)s, %(observed_pgam_seats)s, %(observed_partner_seats)s,
     %(schain_emitted_ok)s, %(schain_emissions_24h)s,
     %(schain_incomplete_rate)s, %(schain_hop_violation_rate)s,
     %(status)s, %(issues)s, %(recommended_action)s, now())
ON CONFLICT (entity_key, as_of) DO UPDATE SET
    revenue_7d                     = EXCLUDED.revenue_7d,
    path_kind                      = EXCLUDED.path_kind,
    ll_publisher_id                = EXCLUDED.ll_publisher_id,
    ll_publisher_name              = EXCLUDED.ll_publisher_name,
    supply_partner_key             = EXCLUDED.supply_partner_key,
    supply_partner_domain          = EXCLUDED.supply_partner_domain,
    supply_partner_pgam_seat       = EXCLUDED.supply_partner_pgam_seat,
    supply_partner_line_present    = EXCLUDED.supply_partner_line_present,
    pgam_line_present_for_path     = EXCLUDED.pgam_line_present_for_path,
    sellers_json_partner_declared  = EXCLUDED.sellers_json_partner_declared,
    expected_pgam_line             = EXCLUDED.expected_pgam_line,
    observed_pgam_seats            = EXCLUDED.observed_pgam_seats,
    observed_partner_seats         = EXCLUDED.observed_partner_seats,
    schain_emitted_ok              = EXCLUDED.schain_emitted_ok,
    schain_emissions_24h           = EXCLUDED.schain_emissions_24h,
    schain_incomplete_rate         = EXCLUDED.schain_incomplete_rate,
    schain_hop_violation_rate      = EXCLUDED.schain_hop_violation_rate,
    status                         = EXCLUDED.status,
    issues                         = EXCLUDED.issues,
    recommended_action             = EXCLUDED.recommended_action,
    audited_at                     = now();
"""


def persist_supply_path(rows: list[SupplyPathRow], as_of: date | None = None) -> int:
    """UPSERT the rows into pgam_direct.compliance_entity_supply_path_audit."""
    from core.neon import connect
    if not rows:
        return 0
    as_of = as_of or date.today()
    payload = []
    for r in rows:
        payload.append({
            "as_of":                          as_of,
            "entity_key":                     r.entity_key,
            "kind":                           r.kind,
            "entity_value":                   r.entity_value,
            "audit_host":                     r.audit_host,
            "revenue_7d":                     r.revenue_7d,
            "path_kind":                      r.path_kind,
            "ll_publisher_id":                r.ll_publisher_id,
            "ll_publisher_name":              r.ll_publisher_name,
            "supply_partner_key":             r.supply_partner_key,
            "supply_partner_domain":          r.supply_partner_domain,
            "supply_partner_pgam_seat":       r.supply_partner_pgam_seat,
            "supply_partner_line_present":    r.supply_partner_line_present,
            "pgam_line_present_for_path":     r.pgam_line_present_for_path,
            "sellers_json_partner_declared":  r.sellers_json_partner_declared,
            "expected_pgam_line":             r.expected_pgam_line,
            "observed_pgam_seats":            list(r.observed_pgam_seats),
            "observed_partner_seats":         list(r.observed_partner_seats),
            "schain_emitted_ok":              r.schain_emitted_ok,
            "schain_emissions_24h":           r.schain_emissions_24h,
            "schain_incomplete_rate":         r.schain_incomplete_rate,
            "schain_hop_violation_rate":      r.schain_hop_violation_rate,
            "status":                         r.status,
            "issues":                         list(r.issues),
            "recommended_action":             r.recommended_action,
        })
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL, payload)
        conn.commit()
    return len(payload)
