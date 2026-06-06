"""Backfill compliance_ll_partner_bridge for all earning LL pubs.

For each LL publisher_id that earned revenue yesterday but isn't already
in compliance_ll_partner_bridge, match by name substring against
compliance_publishers.publisher_key and INSERT the bridge row.

Conservative: only matches with a confident name signal. Logs ambiguous
cases for manual review.
"""
import os, sys
WT = "/Users/priyeshpatel/Desktop/pgam-intelligence/.claude/worktrees/reverent-leavitt-f122dc"
os.chdir(WT)
sys.path.insert(0, WT)

from core.api import fetch as ll_fetch
from core.neon import connect
from datetime import date, timedelta


# Explicit name → publisher_key map. Captures the LL UI labels we see.
NAME_TO_KEY = [
    # (lowercase fragment in LL name, publisher_key, partner display name)
    ("zmaticoo",        "zmaticoo.com"),
    ("cas.ai",          "cas.ai"),
    ("pubrev",          "pubrev.us"),    # all PubRev+ LL pubs map to pubrev.us seat
    ("dexerto",         "dexerto.com"),
    ("start.io",        "start.io"),
    ("startio",         "start.io"),
    ("blueseax",        "blueseasx.com"),
    ("illumin",         "admanmedia.com"),  # Illumin seat lives on admanmedia.com
    ("admanmedia",      "admanmedia.com"),
    ("adman media",     "admanmedia.com"),
    ("pubnative",       "pubnative.net"),
    ("verve",           "pubnative.net"),  # Verve is the Pubnative brand name
    ("bidmachine",      "bidmachine.io"),
    ("algorix",         "algorix.co"),
    ("smaato",          "smaato.com"),
    ("dailymotion",     "dailymotion.com"),
    ("geeksforgeeks",   "geeksforgeeks.org"),
    ("videoelephant",   "videoelephant.com"),
    ("adapex",          "adapex.io"),
]


def match_partner(ll_name):
    n = (ll_name or "").lower()
    for frag, key in NAME_TO_KEY:
        if frag in n:
            return key
    return None


def main():
    y = date.today() - timedelta(days=1)

    # Pull LL pubs that earned yesterday
    rows = ll_fetch("PUBLISHER", ["GROSS_REVENUE","IMPRESSIONS"],
                    y.isoformat(), y.isoformat())
    earning = {str(r.get('PUBLISHER_ID')): {
                  "name": str(r.get('PUBLISHER_NAME') or ''),
                  "rev":  float(r.get('GROSS_REVENUE') or 0)}
               for r in rows if float(r.get('GROSS_REVENUE') or 0) > 0}
    print(f"LL pubs earning yesterday: {len(earning)} (total ${sum(e['rev'] for e in earning.values()):,.2f})")

    with connect() as c, c.cursor() as cur:
        # Currently bridged
        cur.execute("SELECT ll_publisher_id FROM pgam_direct.compliance_ll_partner_bridge")
        already = {r[0] for r in cur.fetchall()}
        # Available partner keys + their seats/types
        cur.execute("""SELECT publisher_key, seller_id, seller_type
            FROM pgam_direct.compliance_publishers WHERE is_active=TRUE""")
        partners = {r[0]: {"seat": r[1], "type": r[2]} for r in cur.fetchall()}

        new_rows = []
        ambiguous = []
        for pid, info in earning.items():
            if pid in already:
                continue
            key = match_partner(info["name"])
            if not key:
                ambiguous.append((pid, info["name"], info["rev"]))
                continue
            if key not in partners:
                print(f"  WARN: matched {pid}/{info['name']!r} → {key} but {key} not in compliance_publishers")
                continue
            new_rows.append((pid, key, partners[key]["type"], partners[key]["seat"],
                              info["name"], "name_explicit_map", 0.95))

        if not new_rows:
            print("\nNo new bridge rows to write.")
        else:
            print(f"\n=== {len(new_rows)} new bridge rows ===")
            for r in sorted(new_rows, key=lambda x: -earning[x[0]]['rev']):
                print(f"  {r[0]:>12} → {r[1]:25} (${earning[r[0]]['rev']:,.2f} yesterday)")
            for r in new_rows:
                cur.execute("""INSERT INTO pgam_direct.compliance_ll_partner_bridge
                    (ll_publisher_id, publisher_key, seller_type, seller_id,
                     ll_publisher_name, bridge_method, bridge_score, bridged_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (ll_publisher_id) DO NOTHING""", r)
            c.commit()
            print(f"\nWrote {len(new_rows)} new bridge rows.")

        if ambiguous:
            print(f"\n=== {len(ambiguous)} unmatched LL pubs (need manual review) ===")
            for pid, name, rev in sorted(ambiguous, key=lambda x: -x[2]):
                print(f"  {pid:>12} {name:50} ${rev:>9,.2f}")


if __name__ == "__main__":
    main()
