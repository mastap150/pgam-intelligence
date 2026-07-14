# PGAM Onboarding

A new hire's first two weeks. Each item is a checkbox — mark it, move on. Role-specific tracks branch after week 1.

## Before day 1 (Priyesh sets up)

- [ ] 1Password vault access + shared vaults granted
- [ ] Slack invite sent
- [ ] Monday.com seat + board access
- [ ] GitHub org invite
- [ ] Trainual seat (once provisioned)
- [ ] Role-specific tool invites (see role tracks below)
- [ ] Laptop provisioned OR BYOD onboarding plan agreed
- [ ] First-week 1:1 scheduled with Priyesh

## Day 1 — orientation and non-negotiables

**Goal by end of day: know what PGAM does, know what not to break.**

- [ ] Read `training/00-company.md` — the two-stack rule, product map, commercial models
- [ ] Read `training/01-security-nonnegotiables.md` — every rule on this list matters
- [ ] 30-minute walkthrough with Priyesh: PGAM commercial model + the current top 3 priorities
- [ ] Get access to all your tools (finish the checklist above)
- [ ] Post a quick intro in the team Slack channel

**Do not touch prod anything on day 1.** Read, ask, take notes.

## Day 2–3 — get oriented in your area

- [ ] Read the playbook that matches your role (see role tracks below)
- [ ] Watch every Priority 1 Scribe recording in `training/99-scribe-shotlist.md`
- [ ] Do at least one Scribe-guided flow in the demo/dev environment for your role
- [ ] Set up your local dev environment if you're an engineer (see `06-engineering-playbook.md`)

## End of week 1 — shadow

- [ ] Sit through a real ops session — a real campaign build, a real floor review, a real partner escalation — with the owner explaining as they go
- [ ] Watch Priority 2 Scribes for your role
- [ ] Ask every question you're saving up in your notes; get it down to a short residual list before the 1:1

**Friday 1:1 with Priyesh — 30 min.** Bring your residual questions.

## Week 2 — do the work with a review gate

- [ ] Take ownership of a small, low-blast-radius task (Priyesh will assign)
- [ ] Every action goes through review before it hits prod / a partner
- [ ] Read `training/06-engineering-playbook.md` if you haven't — even non-engineers benefit from the repo map
- [ ] Watch Priority 3+ Scribes as tasks require them

**End of week 2:** you should be able to run one recurring task in your area independently, with checks (not approvals) from the owner.

---

## Role tracks

### Trafficker (DSP / SS ops)

**Playbooks:** 00, 01, 02, 03
**Scribes:** all Priority 1 + all Priority 3 + Priority 6
**Weekly ritual:** Monday morning — pacing check on all live campaigns; end of week — margin review
**Cannot touch without approval:** freeze list, campaign rates, agency-name-adjacent fields in SS

### Supply ops (LL / TB)

**Playbooks:** 00, 01, 04, 05
**Scribes:** all Priority 1 + all Priority 2 + Priority 6
**Weekly ritual:** daily target check ($10K/$15K), partner health scan, freeze list sanity check
**Cannot touch without approval:** contract floors (9 Dots $1.70+), freeze removals, BidMachine QPS

### Engineer

**Playbooks:** 00, 01, 06 + whichever product playbook (02 for DSP, 03 for marketplace, 04/05 for supply)
**Scribes:** all Priority 1 + Priority 4 + product-specific
**Weekly ritual:** PR review, Vercel deploy health, Neon bloat check
**Cannot touch without approval:** `main` force pushes, secret rotation, freeze removal, CODEOWNERS on P&L paths

### Content / MSN

**Playbooks:** 00, 01
**Scribes:** all Priority 1 + Priority 5
**Weekly ritual:** content queue review, MSN feed health, session bootstrap if needed
**Cannot touch without approval:** any account-level content platform settings, cross-property CMS changes

### Finance / Recon

**Playbooks:** 00, 01
**Scribes:** all Priority 1 + Priority 6
**Weekly ritual:** partner payout schedule, invoicing queue, month-close prep
**Cannot touch without approval:** commercial terms, partner billing entity changes, credit terms

---

## When to escalate to Priyesh (not later — immediately)

- Anything touching a partner relationship or partner-facing surface
- Any P&L question, contract term, or rate decision
- Any secret rotation or access grant/revoke
- Any freeze list change
- Any deploy to `main` that involves finance/margin/agency code paths
- Any unfamiliar file, branch, or config you're about to delete or overwrite
- Any incident (WP hack, DNS incident, credential leak, partner outage)

## When you don't need to escalate

- Reading anything
- Editing training docs and other markdown
- Running dry-run scheduler jobs
- Working in the demo env
- Local dev, tests, preview builds
- Filing your own tickets in Monday
