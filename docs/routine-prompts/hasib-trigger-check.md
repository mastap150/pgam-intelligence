# Routine prompt — Hasib trigger check (weekly MSN cohort monitor)

Replacement prompt for the routine that currently inlines two Neon connection
strings. See `docs/runbook-neon-credential-rotation.md` for why, and for the
ordering — this prompt is step 7, after rotation.

**Routine settings to preserve when recreating:**

| Setting | Value |
|---|---|
| Name | `Hasib trigger check — weekly MSN cohort monitor` |
| Cron | `0 18 * * 0` (UTC) |
| Model | `claude-sonnet-5` |
| Allowed tools | `Bash`, `Read` |
| Source | `mastap150/pgam-intelligence` |
| Environment | `PGAM` (`env_0112tdyC54U8EXEqtMucGBbB`) |

Both `PGAM_DIRECT_DATABASE_URL` and `BOXINGNEWS_DATABASE_URL` must be set in
that environment's **Environment variables** box before the routine will run.
Prefer the read-only Neon role — this monitor only reads.

---

## Prompt

```text
Run the Hasib-reduction trigger check for boxingnews.com and report the verdict.
The script compares AI-rotation cohort vs Hasib cohort MSN performance
week-over-week and evaluates whether Priyesh should cut Hasib's $11/article ×
5/day contract.

CREDENTIAL RULE — read this first. This routine needs two Neon connection
strings. They must come from the environment. Never paste a connection string
into this prompt, a reply, a commit, or a file: routine prompts are stored
server-side and echoed back in full by list_triggers, so anything inlined here
is readable by anyone who can list this account's routines. Refer to the
credentials only by env-var name.

Steps:

1. From the pgam-intelligence repo root (this is your cwd), install psycopg3:
   pip install --quiet 'psycopg[binary]'

2. Confirm both DSNs are present in the environment. This prints names and
   set/MISSING only — it must never print a value:

   for v in PGAM_DIRECT_DATABASE_URL BOXINGNEWS_DATABASE_URL; do
     if [ -n "${!v}" ]; then echo "$v: set"; else echo "$v: MISSING"; fi
   done

   If either is MISSING, STOP. Do not run the script, do not try to
   reconstruct or guess a DSN, and do not look for one in the repo (secrets
   are never committed here). Reply saying which variable is missing and that
   it needs to be added to the PGAM cloud environment's "Environment
   variables" box. Then end the run.

3. Run the script:
   python3 scripts/hasib_trigger_check.py --weeks 6

   It reads both DSNs from the environment itself. If it exits with the "env
   vars are required" error, treat that the same as step 2's MISSING case.

4. Read the output. It prints a 6-week table (AI arts / AI msn / AI avg PV /
   AI est. rev / Hasib arts / Hasib avg PV / Hasib est. rev) followed by a
   TRIGGER line: KEEP_5, CUT_TO_3, or CUT_TO_2.

5. Reply with a short update (under 200 words). Cover:
   (a) The trigger verdict, verbatim from the script output.
   (b) Last week's AI avg reads/ingested vs the 90 threshold, and AI weekly
       est. rev vs the $350 threshold. State the gap in absolute terms.
   (c) Notable trend vs the prior week — is AI avg PV rising, flat, or
       falling? Same for AI est. rev. Two consecutive weeks matter for the
       trigger, so a flat/falling second week is the important signal.
   (d) If the verdict flipped from KEEP_5 to CUT_TO_3 or CUT_TO_2, flag it
       prominently at the top and quote the specific numbers that fired the
       rule.

   Never include a connection string, password, host, or any part of a DSN in
   the reply.

Context for interpretation:
- Rule: CUT_TO_3 when AI avg reads/ingested >= 90 for 2 consecutive weeks AND
  AI weekly est. rev >= $350. CUT_TO_2 at >= 120.
- Hasib contract = $11/article × 5/day ~= $1,650/mo.
- The last row of the table is often partial (articles <5 days old under-count
  peak reads). If the most recent week's AI on_msn count is <60% of the prior
  week's, note that the numbers will firm up over the next few days.
- Base rev estimate uses $4 CPM × 1.6 real-payout multiplier.

Do NOT auto-cut anything. This is a monitor.
```

## What changed from the original

- The two `export …='postgresql://…'` lines are gone. `hasib_trigger_check.py`
  already reads both DSNs from the environment (`scripts/hasib_trigger_check.py:248`)
  and errors clearly when they are absent, so the exports were never needed —
  they only duplicated what the script does.
- Added a preflight that reports set/MISSING **without printing values**, and a
  hard stop instead of a half-run against one database.
- Added an explicit instruction never to echo a DSN back in the reply.
- Steps 4–5 and the interpretation notes are unchanged from the original.
