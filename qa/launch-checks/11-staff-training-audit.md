# Staff training & operator readiness audit

**Question**: If Priyesh is unavailable for 48 hours mid-launch, can someone
else (Vivek, a contractor, an on-call person) keep the SSP running? What
docs exist, and what gaps will bite us?

## Existing training material in repo

| Doc | Lines | What it covers | Quality |
|---|---|---|---|
| `docs/HANDOFF.md` | 186 | High-level system map, repo layout, env-var pointers | OK |
| `docs/operator-playbook.md` | 423 | Day-2 ops: deploys, env management, incident triage | Good |
| `docs/dsp-onboarding.md` | 162 | How to add a new DSP | OK but theoretical (zero real onboardings done) |
| `docs/partner-onboarding-complete.md` | ? | Publisher onboarding via wizard | Drafty |
| `docs/prebid-integration.md` | ? | Adapter setup for partners | Partner-facing, not staff-facing |
| `docs/runbooks/*.md` | 22 files | Per-incident playbooks (alert-*, rotate-*, restore-*) | Templates exist; many never executed |
| `docs/runbooks/new-prebid-partner-email-template.md` | new | Email template for AM | Just added this round |
| `docs/secrets-explained.md` | ? | Where secrets live, how to rotate | OK |

## What's missing (gaps that block "anyone can run this")

### 1. No incident on-call rotation defined
- No PagerDuty / Opsgenie schedule
- Runbooks reference "on-call" but there is no on-call person
- **Fix before launch**: at minimum, write down "Priyesh is on-call. Backup is X.
  If both unreachable, escalate to Y. Phone numbers in 1Password vault `pgam-oncall`."

### 2. No "getting started" doc for a new operator
- No "Day 1: install these tools, get these credentials, read these 5 docs in order"
- HANDOFF.md is closer but assumes engineering background
- **Fix before launch**: 1-page `docs/onboarding-new-operator.md` that walks
  someone from clone → first deploy → first incident drill in <2h.

### 3. Runbook execution gap
- 22 runbooks exist; **most have never been executed end-to-end**.
- Confirmed-tested: `restore-from-backup.md` (now via 05-neon-pitr-drill.sh),
  `rollback-bidder.md` (cited in #7 drill), `rotate-dsp-credentials.md` (theoretical).
- Untested: `alert-*` (alerts aren't wired — see #8 audit), `engage-gdpr-eu-rep.md`,
  `wire-aws-secrets-manager.md`.
- **Fix before launch**: spot-check 3 critical runbooks with a stopwatch:
  rollback-bidder, restore-from-backup, rotate-dsp-credentials. Note actual
  steps that diverge from the doc.

### 4. No "what does normal look like" doc
- No baseline metrics doc — when Vivek looks at the dashboard, what's a
  healthy auction p95? What's a normal bid_request volume for Tuesday 3pm?
- **Fix before launch**: 1-page `docs/baselines.md` capturing post-launch-week
  baselines, updated weekly for first month.

### 5. Partner-AM (account management) playbook missing
- Email template exists (this round). But: no doc on
  - How to read partner-scoped reporting in the admin
  - How to investigate "partner says their numbers don't match ours"
  - How to handle a partner asking to change their rev-share %
  - How to off-board a partner cleanly
- **Fix before launch**: `docs/runbooks/partner-am-playbook.md` covering
  the above 4 scenarios.

### 6. No video walkthroughs or screencasts
- Text docs are dense. A 10-min screencast of "creating a new publisher
  end-to-end" would shortcut a lot of learning.
- **Optional**: not blocking, but record one before launch.

### 7. SQL cookbook missing
- Operators will need to query Neon for "what's this publisher's last bid?",
  "did this DSP fan out?", "why is this user locked out?"
- Currently no cheat sheet of common diagnostic queries.
- **Fix before launch**: `docs/sql-cookbook.md` with 10-15 read-only queries
  (publisher status, DSP fan-out for last hour, user lookup, financial-events
  for a date range).

## Pass condition

- [ ] On-call rotation written down (even if "Priyesh + backup")
- [ ] New-operator onboarding doc exists
- [ ] 3 critical runbooks spot-checked with timing
- [ ] Partner-AM playbook drafted
- [ ] SQL cookbook drafted

## Verdict

Training material is **decent for a one-person shop, insufficient for a team
launch**. If launch means "Priyesh + Vivek can both troubleshoot independently",
the 5 fixes above are blocking. If launch means "Priyesh alone", existing
docs are passable.
