"""
agents/compliance/publisher_chain_audit.py

The corrected Layer C check, applied across every active supply partner.

Background (operator clarification 2026-06-11): the existing
`sellers_json_partner_declared` flag was a placeholder — hard-coded
True whenever a bridge entry existed, so it never actually validated
anything. The real chain-of-custody requirement per ads.txt/sellers.json
standards is:

  App publisher  → declared as PUBLISHER (or BOTH) in
                   the supply partner's sellers.json
  Supply partner → declared as INTERMEDIARY in our sellers.json
  PGAM           → buyer

Without the publisher's direct seller record in the partner's
sellers.json, the chain is incomplete and buyers can't verify the
inventory's provenance. This agent runs that check for every active
(entity × supply_partner) pair daily.

Behavior:
  1. For each active supply partner with a known sellers.json URL,
     fetch + parse it (with retries + UA spoof for sites that block
     bots). Try both `https://<domain>/sellers.json` and
     `https://www.<domain>/sellers.json` because some partners
     (Start.io) only serve at the www subdomain.
  2. Build a `compliance_partner_sellers_index` snapshot: one row per
     (partner_key, publisher_domain, seller_id) tuple.
  3. For each row in today's `compliance_entity_supply_path_audit`,
     resolve the publisher's audit_host, look it up in the partner's
     index, and update three new columns:
       publisher_declared_in_partner_sj BOOL
       partner_sellers_json_seller_id   TEXT
       partner_sellers_json_seller_type TEXT

Idempotent. Runs daily after the main compliance runner persists
supply_path_audit. Wired into scheduler.py at :47 past every hour
alongside the other compliance jobs — sellers.json doesn't change
hourly so the once-a-day-effective rerun is cheap.
"""
from __future__ import annotations

import json
import os
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

from core.neon import connect


ACTOR = "publisher_chain_audit"
TIMEOUT_SEC = 20
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"


def normalize_domain(d: str) -> str:
    d = (d or "").strip().lower()
    if d.startswith(("http://", "https://")):
        d = d.split("://", 1)[1]
    for sep in ("?", "#", "/"):
        if sep in d:
            d = d.split(sep, 1)[0]
    if d.startswith("www."):
        d = d[4:]
    return d


def _fetch_sellers_json(partner_domain: str) -> dict | None:
    """Try both bare-domain and www-prefixed URLs. Some partners
    (Start.io) only serve at www; some only at bare. Return the parsed
    dict on first success; None if both fail."""
    for host_variant in (partner_domain, "www." + partner_domain):
        url = f"https://{host_variant}/sellers.json"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as r:
                if r.status != 200:
                    continue
                body = r.read().decode("utf-8", errors="ignore")
            data = json.loads(body)
            if isinstance(data, dict) and "sellers" in data:
                return data
        except Exception:
            continue
    return None


def _refresh_partner_index(cur, partner_key: str, partner_domain: str) -> int:
    """Fetch a single partner's sellers.json + persist its (domain,
    seller_type) tuples. Returns row count written."""
    data = _fetch_sellers_json(partner_domain)
    if not data:
        print(f"[{ACTOR}] sellers.json fetch FAILED for {partner_key}")
        return 0
    sellers = data.get("sellers") or []
    today_d = date.today()
    rows = []
    seen_keys = set()
    for s in sellers:
        d = normalize_domain(s.get("domain", ""))
        sid = (s.get("seller_id") or "").strip()
        styp = (s.get("seller_type") or "").upper()
        if not d or not sid:
            continue
        # Some files have duplicate (domain, seller_id) — dedup by PK.
        key = (partner_key, d, sid)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        rows.append((partner_key, d, sid, styp, s.get("name", ""), today_d))
    if not rows:
        return 0
    # Clear yesterday's index for this partner so we don't leave stale
    # rows when a partner drops a publisher.
    cur.execute(
        "DELETE FROM pgam_direct.compliance_partner_sellers_index "
        "WHERE partner_key = %s", (partner_key,))
    cur.executemany(
        "INSERT INTO pgam_direct.compliance_partner_sellers_index "
        "(partner_key, publisher_domain, seller_id, seller_type, "
        " seller_name, snapshot_date) VALUES (%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING",
        rows)
    return len(rows)


def _refresh_all_partner_indexes() -> dict[str, int]:
    """Pull active supply partners from compliance_publishers, fetch
    each one's sellers.json in parallel (network-bound, light)."""
    with connect() as c, c.cursor() as cur:
        cur.execute("""
            SELECT publisher_key, domain FROM pgam_direct.compliance_publishers
            WHERE seller_type IN ('INTERMEDIARY','BOTH') AND is_active = TRUE
              AND ll_publisher_id IS NOT NULL AND domain IS NOT NULL
        """)
        partners = [(r[0], r[1]) for r in cur.fetchall()]

    results: dict[str, int] = {}

    def _do(pk_dom):
        pk, dom = pk_dom
        with connect() as c, c.cursor() as cur:
            n = _refresh_partner_index(cur, pk, dom)
            c.commit()
        return pk, n

    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(_do, p): p for p in partners}
        for f in as_completed(futs):
            try:
                pk, n = f.result()
                results[pk] = n
            except Exception as exc:
                pk = futs[f][0]
                print(f"[{ACTOR}] {pk}: {exc}")
                results[pk] = 0
    return results


def _update_supply_path_chain_columns(as_of: date | None = None) -> dict:
    """Join today's supply_path_audit against the freshly-refreshed
    sellers index and populate the three new columns.

    Strategy: pull all candidate (entity, partner, audit_host) tuples
    where partner has a non-empty index. Look up host in the index.
    Update column. Idempotent."""
    as_of = as_of or date.today()
    with connect() as c, c.cursor() as cur:
        # Load the freshly-built index into memory: partner → host → {seller_id, seller_type}
        cur.execute("""
            SELECT partner_key, publisher_domain, seller_id, seller_type
            FROM pgam_direct.compliance_partner_sellers_index
        """)
        idx: dict[str, dict[str, tuple[str, str]]] = defaultdict(dict)
        for pk, d, sid, styp in cur.fetchall():
            # When multiple sellers for the same (partner, domain),
            # prefer PUBLISHER > BOTH > anything-else so the column
            # reflects the strongest declaration.
            existing = idx[pk].get(d)
            if existing is None or _rank(styp) > _rank(existing[1]):
                idx[pk][d] = (sid, styp)

        # Pull the latest snapshot rows we need to update
        cur.execute("""
            SELECT entity_key, supply_partner_key, audit_host, entity_value, kind
            FROM pgam_direct.compliance_entity_supply_path_audit
            WHERE as_of = %s
        """, (as_of,))
        rows = cur.fetchall()
        updates = []
        no_partner_index = 0
        no_host = 0
        decl_pub = 0
        decl_wrong = 0
        not_declared = 0
        for ek, sp_key, host, ev, kind in rows:
            partner_idx = idx.get(sp_key)
            if partner_idx is None:
                no_partner_index += 1
                continue
            host_norm = normalize_domain(host or (ev if kind == "domain" else ""))
            if not host_norm:
                no_host += 1
                updates.append((ek, sp_key, None, None, None))
                continue
            hit = partner_idx.get(host_norm)
            if hit and hit[1] in ("PUBLISHER", "BOTH"):
                decl_pub += 1
                updates.append((ek, sp_key, True, hit[0], hit[1]))
            elif hit:
                decl_wrong += 1
                updates.append((ek, sp_key, False, hit[0], hit[1]))
            else:
                not_declared += 1
                updates.append((ek, sp_key, False, None, None))

        cur.executemany("""
            UPDATE pgam_direct.compliance_entity_supply_path_audit
            SET publisher_declared_in_partner_sj = %s,
                partner_sellers_json_seller_id   = %s,
                partner_sellers_json_seller_type = %s
            WHERE entity_key = %s AND supply_partner_key = %s AND as_of = %s
        """, [(d, sid, styp, ek, pk, as_of) for ek, pk, d, sid, styp in updates])
        c.commit()
    return {
        "rows_examined": len(rows),
        "declared_publisher_or_both": decl_pub,
        "listed_but_wrong_type":      decl_wrong,
        "not_declared_at_all":        not_declared,
        "missing_partner_index":      no_partner_index,
        "missing_host":               no_host,
    }


_RANK = {"PUBLISHER": 3, "BOTH": 2, "INTERMEDIARY": 1, "": 0}
def _rank(t): return _RANK.get((t or "").upper(), 0)


def run() -> dict:
    started = datetime.now(timezone.utc)
    print(f"[{ACTOR}] start  refreshing supply-partner sellers.json indexes")
    fetch_results = _refresh_all_partner_indexes()
    total_sellers = sum(fetch_results.values())
    print(f"[{ACTOR}] sellers.json fetched for {len(fetch_results)} partners "
          f"({total_sellers:,} seller rows indexed)")
    update_stats = _update_supply_path_chain_columns()
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"[{ACTOR}] done  rows={update_stats['rows_examined']}  "
          f"declared={update_stats['declared_publisher_or_both']}  "
          f"wrong_type={update_stats['listed_but_wrong_type']}  "
          f"not_declared={update_stats['not_declared_at_all']}  "
          f"elapsed={elapsed:.1f}s")
    return {
        "ok": True,
        "partners_fetched": fetch_results,
        **update_stats,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
