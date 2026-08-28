# Two Neon connection strings were stored in a Routine prompt

**Found:** 2026-08-25, while running `list_triggers` before creating a
PR check-in (which `CLAUDE.md` requires, for unrelated reasons).
**Exposed from:** 2026-07-27, when the Routine was created.
**Status:** removal is prepared here; **rotation has not happened yet**
and is the only step that actually closes this.

---

## What was exposed

The weekly `Hasib trigger check` Routine
(`trig_01KfQFfT93WTsqjEifmtnwfr`, created via the HTTP API, enabled,
firing Sundays at 18:00 UTC) carries a prompt that begins with two
literal `export` lines. Between them they contain:

| Variable | Database | Credential in prompt |
|---|---|---|
| `PGAM_DIRECT_DATABASE_URL` | `pgam_direct` on Neon (`ep-small-math-…`) | role + password |
| `BOXINGNEWS_DATABASE_URL` | boxingnews on Neon (`ep-delicate-star-…`) | role + password |

Both are the `neondb_owner` role — full owner rights on their database,
not a scoped reader.

The script itself was never the problem. `scripts/hasib_trigger_check.py`
has always read both values from `os.environ` and has never contained a
credential. The exposure is entirely in the **stored prompt**.

## Why this is a real exposure and not a tidiness issue

`CLAUDE.md` already carries the rule this broke:

> **Never inline a connection string in a routine prompt.** Routine
> prompts are stored server-side and echoed back in full by
> `list_triggers`. Read credentials from the environment instead. A
> routine created through the web UI cannot be edited from a session,
> so a secret pasted into one has to be rotated to be removed — moving
> it is not enough.

That is exactly what happened here, and the discovery demonstrated the
mechanism: a routine `list_triggers` call, made for an unrelated
reason, printed both passwords in full into a session transcript. Any
`list_triggers` call by anyone on the account does the same, and has
been doing so for four weeks.

So the blast radius is not "one prompt". It is: every transcript that
ever listed triggers, plus the control-plane record itself.

## Why it happened — the part worth fixing

Nobody ignored the rule for convenience. The script's own USAGE block
gave exactly one recipe:

```
export $(grep -E '^(PGAM_DIRECT_DATABASE_URL|BOXINGNEWS_DATABASE_URL)=' \
  ~/Desktop/pgam-intelligence/.env | xargs)
```

That path exists on one laptop. A cloud Routine cannot satisfy it, and
the documentation offered no cloud alternative — so the only way to
make the weekly monitor run was to supply the values some other way,
and the prompt was the nearest place to put them. **The instructions
left no compliant path.**

That is fixed in this change: the USAGE block and the script's
missing-variable error now name both the local and the cloud path, and
say plainly not to paste a connection string into a prompt.

---

## Remediation

### 1. Rotate both passwords — required, not optional

Removing the prompt text does **not** un-expose a stored secret.

In the Neon console, for each project:

- the `pgam_direct` project (endpoint `ep-small-math-…`) — reset the
  `neondb_owner` password.
- the boxingnews project (endpoint `ep-delicate-star-…`) — reset the
  `neondb_owner` password.

Endpoint IDs are deliberately abbreviated throughout this file: **this
repository is public**, and the host half of a DSN is precisely the
part that rotation does *not* change. Both projects are unambiguous
from the prefixes above in the Neon console.

Then update every consumer. Grep before you rotate, so nothing is
missed:

```bash
grep -rn "PGAM_DIRECT_DATABASE_URL\|BOXINGNEWS_DATABASE_URL" \
  --include='*.py' --include='*.yml' --include='*.ts' . | grep -v '\.git/'
```

Known consumers at time of writing: the local `.env`, the Render worker
environment, and the GitHub Actions secrets used by the scheduled
workflows.

**Not the Routine.** It is the thing being remediated — pasting a fresh
connection string back into its prompt would recreate the exposure with
a new password. The Routine gets its values from the environment
instead; that is step 3, and step 4 rewrites the prompt to stop
carrying them at all.

### 2. Prefer a read-only role over rotating owner in place

`hasib_trigger_check.py` issues **two SELECTs** and writes nothing.
Handing a weekly monitor owner credentials is more rights than the job
has ever needed.

Note the two databases use **different schemas**, so the grants are not
symmetric. Granting only `public` on the `pgam_direct` side — the
obvious-looking default — fails at the first query with `permission
denied for schema pgam_direct`, and an operator mid-incident will
reasonably conclude the read-only role "doesn't work" and go back to
`neondb_owner`, which defeats this step entirely.

```sql
-- ── pgam_direct database ──
-- Reads pgam_direct.msn_article_peak JOIN pgam_direct.msn_article_meta
CREATE ROLE hasib_monitor WITH LOGIN PASSWORD '<generated>';
GRANT CONNECT ON DATABASE neondb TO hasib_monitor;
GRANT USAGE ON SCHEMA pgam_direct TO hasib_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA pgam_direct TO hasib_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgam_direct
  GRANT SELECT ON TABLES TO hasib_monitor;
```

```sql
-- ── boxingnews database ──
-- Reads public.articles
CREATE ROLE hasib_monitor WITH LOGIN PASSWORD '<generated>';
GRANT CONNECT ON DATABASE neondb TO hasib_monitor;
GRANT USAGE ON SCHEMA public TO hasib_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO hasib_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO hasib_monitor;
```

Verify before repointing the Routine — a role that can log in but not
read is the failure mode this note exists to prevent:

```bash
PGAM_DIRECT_DATABASE_URL='<new monitor dsn>' \
BOXINGNEWS_DATABASE_URL='<new monitor dsn>' \
  python3 scripts/hasib_trigger_check.py --weeks 2
```

Point the Routine at `hasib_monitor`. If it leaks again, the loss is a
read of two reporting tables rather than owner on both databases.

### 3. Provision the values — with eyes open

> The script's USAGE block already describes the cloud path in the
> present tense ("injected by the environment at session start"). That
> is the intended end state, not today's: until this step is ticked,
> a cloud run exits with the missing-variable message naming which one.


`CLAUDE.md` is explicit that a cloud environment is **not** a secrets
store:

> Anyone who uses the environment can read the values, and cloud
> environments have no dedicated secrets store, so don't add API keys
> or other credentials.

So this is a judgement call, not a clean fix. For a **read-only
monitor role scoped to two reporting tables**, putting the DSN in the
environment config is a defensible trade: it is the only way an
unattended weekly Routine can run at all, and the blast radius is a
scoped reader. For the `neondb_owner` credentials it is not
defensible, which is why step 2 comes first.

Set them at claude.ai/code → the environment selector (`PGAM`) → gear
→ **Environment variables**. Values are copied **once at session
start**, so only sessions begun afterwards see them.

### 4. Replace the Routine's prompt

The current prompt's first two steps are the `pip install` and the two
`export` lines. The replacement below drops the exports entirely. It is
otherwise the same job, and the reporting instructions are unchanged.

A Routine created through the web UI cannot be edited from a session,
but this one was created via the HTTP API, so `update_trigger` can
rewrite its prompt in place — which preserves its run history. Do that
**after** rotating, not before: rewriting the prompt first would remove
the evidence of which credentials need rotating.

<details>
<summary>Replacement prompt (no credentials)</summary>

```text
Run the Hasib-reduction trigger check for boxingnews.com and report the verdict.
The script compares AI-rotation cohort vs Hasib cohort MSN performance
week-over-week and evaluates whether Priyesh should cut Hasib's
$11/article x 5/day contract.

Steps:

1. From the pgam-intelligence repo root (this is your cwd), install psycopg3:
   pip install --quiet 'psycopg[binary]'

2. Run the script. PGAM_DIRECT_DATABASE_URL and BOXINGNEWS_DATABASE_URL are
   provided by the environment — do NOT export them here, and do not paste a
   connection string into this prompt or any command. If either is missing the
   script exits with a message naming which one; report that and stop.

   python3 scripts/hasib_trigger_check.py --weeks 6

3. Read the output. It prints a 6-week table (AI arts / AI msn / AI avg PV /
   AI est. rev / Hasib arts / Hasib avg PV / Hasib est. rev) followed by a
   TRIGGER line: KEEP_5, CUT_TO_3, or CUT_TO_2.

4. Reply with a short update (under 200 words). Cover:
   (a) The trigger verdict, verbatim from the script output.
   (b) Last week's AI avg reads/ingested vs the 90 threshold, and AI weekly
       est. rev vs the $350 threshold. State the gap in absolute terms.
   (c) Notable trend vs the prior week — is AI avg PV rising, flat, or falling?
       Same for AI est. rev. Two consecutive weeks matter for the trigger, so a
       flat/falling second week is the important signal.
   (d) If the verdict flipped from KEEP_5 to CUT_TO_3 or CUT_TO_2, flag it
       prominently at the top and quote the specific numbers that fired the rule.

Context for interpretation:
- Rule: CUT_TO_3 when AI avg reads/ingested >= 90 for 2 consecutive weeks AND
  AI weekly est. rev >= $350. CUT_TO_2 at >= 120.
- Hasib contract = $11/article x 5/day ~ $1,650/mo.
- The last row of the table is often partial (articles <5 days old under-count
  peak reads). If the most recent week's AI on_msn count is <60% of the prior
  week's, note that the numbers will firm up over the next few days.
- Base rev estimate uses $4 CPM x 1.6 real-payout multiplier.

Do NOT auto-cut anything. This is a monitor.
```

</details>

### 5. Verify

```
list_triggers → the prompt contains neither "postgres://" nor "postgresql://"
```

Both spellings matter: Postgres accepts either scheme and
`.env.example` documents these two variables with `postgres://`, so a
check for the longer form alone would pass a prompt that still holds a
live credential.

Then let one Sunday firing complete and confirm it still reports a
verdict.

---

## Checklist

- [ ] Rotate `neondb_owner` on `pgam_direct`
- [ ] Rotate `neondb_owner` on boxingnews
- [ ] Create scoped `hasib_monitor` read-only roles (recommended)
- [ ] Update `.env`, Render env, GitHub Actions secrets
- [ ] Set both vars on the `PGAM` cloud environment
- [ ] `update_trigger` the Routine prompt to the version above
- [ ] Confirm no `postgres://` **or** `postgresql://` in `list_triggers` output
- [ ] Confirm the next Sunday run reports a verdict

---

## The general lesson

The rule in `CLAUDE.md` was already correct and already written down.
It was broken anyway, because the documented workflow only covered the
laptop. A security rule with no compliant path for a real use case
does not prevent the behaviour — it just moves it somewhere nobody
looks.

When adding a rule of this kind, check that every place the job
actually runs has a way to obey it.
