# Outbound SDR Agent — "Jordan Reilly"

Daily lead loader that pushes net-new ICP leads from **Apollo → HubSpot → Instantly**, sending from a non-Priyesh persona ("Jordan Reilly"). Replies route to Priyesh.

```
Apollo search ──▶ this agent ──▶ HubSpot pipeline 899621236
                       │
                       └──▶ Instantly campaign (Jordan Reilly inboxes)
```

---

## v1 scope

- Two segments, mirrored to the DSP rate-card SKUs:
  - `brand_awareness` — brand/growth marketers at mid-market consumer + B2B
  - `performance` — heads of acquisition at call-driven verticals
- Daily run at 09:00 ET (registered in `scheduler.py`).
- Dry-run by default. Nothing writes to HubSpot or Instantly until `SDR_DRY_RUN=false`.
- Daily cap per segment (default 25 leads/day) to keep Instantly inbox warmth healthy.
- Slack summary per run.

**Not in v1** (planned next phase):
- Reply classification + suggested-response generation
- Per-creative A/B on subject lines
- Domain-level dedupe (today we dedupe by email only)
- Per-vertical sequence selection inside `performance` segment

---

## One-time setup — checklist

### 1. Instantly inboxes

We're using the inboxes that already exist in your Instantly account. Make sure:

- [ ] At least 2 inboxes are warmed (warming score green) and assigned to the campaigns below.
- [ ] Inbox **display names** are set to `Jordan Reilly` (Instantly → Inbox → Edit → Sender Name).
- [ ] Inbox **signature** is updated to:
  ```
  Jordan
  PGAM Media
  ```
- [ ] Daily send limit per inbox ≤ 30 (Instantly default is fine).

### 2. Instantly campaigns

Create two campaigns (or repurpose existing if you already have them). Load the sequences from [`templates.py`](./templates.py):

- [ ] Campaign A — "PGAM DSP — Brand Awareness Outbound" → paste `BRAND_AWARENESS_SEQUENCE`
- [ ] Campaign B — "PGAM DSP — Performance / Call" → paste `PERFORMANCE_SEQUENCE`
- [ ] Each campaign: 4 steps, day offsets 0/3/7/14, weekdays-only, US business hours, plain text.
- [ ] Attach the Jordan-Reilly-display-name inboxes to both campaigns.
- [ ] Copy each campaign ID.

### 3. HubSpot

- [ ] Confirm pipeline `899621236` ("PGAM Cold Outbound (Apollo+Instantly)") exists and you have access.
- [ ] Decide the deal-stage ID for net-new outbound contacts. Default in code is empty (HubSpot will use pipeline default); set `HUBSPOT_DEAL_STAGE_NEW` if you want a specific stage.
- [ ] Confirm custom properties exist on contacts: `pgam_outbound_source`, `pgam_outbound_persona`. On deals: `pgam_outbound_segment`, `pgam_outbound_persona`. (Memory already notes `pgam_outbound_*` props — verify before first live run.)

### 4. Env vars

Add to `.env`:

```bash
# Apollo
APOLLO_API_KEY=...

# HubSpot
HUBSPOT_ACCESS_TOKEN=...
HUBSPOT_PIPELINE_ID=899621236
HUBSPOT_DEAL_STAGE_NEW=          # optional; leave empty for pipeline default

# Instantly
INSTANTLY_API_KEY=...
INSTANTLY_CAMPAIGN_BRAND_AWARENESS_ID=...
INSTANTLY_CAMPAIGN_PERFORMANCE_ID=...

# Agent controls
SDR_DRY_RUN=true                  # flip to false to send
SDR_DAILY_CAP_PER_SEGMENT=25      # leads per segment per day
```

---

## Running

**Dry-run (no writes):**
```bash
SDR_DRY_RUN=true python -c "from agents.outbound.sdr_agent import run; run()"
```

Logs to stdout + sends a Slack summary with what *would have* been pushed.

**Live:**
```bash
SDR_DRY_RUN=false python -c "from agents.outbound.sdr_agent import run; run()"
```

**Scheduled:** the agent is registered in `scheduler.py` for daily 09:00 ET. Just deploy.

---

## Tuning loop

After ~200 sends per segment:

1. Pull reply rate by **title** (HubSpot view, group by `jobtitle`). Cut titles with zero positive replies.
2. Pull bounce rate by **industry**. Tighten `q_organization_keyword_tags` in `icp.py`.
3. Pull positive-reply rate by **industry**. Add adjacent industries that are working.
4. A/B subject lines. The `subject_options` lists in `templates.py` are 2-3 variants each — Instantly will rotate.

Every change to ICP or copy → re-deploy `icp.py` / re-paste sequences in Instantly. Both are versioned here so a code review catches drift.

---

## Reply handling (today + planned)

**Today (v1):** Instantly has built-in reply detection. Configure each campaign's "reply handling" to:
- Mark "interested" replies as `Won` in Instantly.
- Auto-forward to `<your monitoring inbox>` (recommend: a `replies@` alias or shared inbox so Jordan-Reilly persona isn't tied to Priyesh's personal address).
- Priyesh replies *from his personal address* on warm replies, introducing himself as "the founder Jordan mentioned." Standard SDR→AE handoff pattern.

**Planned (v2):** a `reply_classifier.py` agent that:
- Polls Instantly's reply webhook
- Classifies (interested / not-now / OOO / wrong-contact / unsubscribe)
- For "interested": drafts a warm-handoff response from Priyesh and Slack-pings him for one-click approve
- For everything else: updates HubSpot deal stage automatically

---

## Files

| File | What it does |
|---|---|
| `sdr_agent.py` | Main daily loader. Apollo → HubSpot dedupe → HubSpot create → Instantly push → Slack. |
| `icp.py` | Apollo search filters per segment. Tune here. |
| `templates.py` | Versioned reference copy for the 4-touch sequences. Instantly is source of truth at send. |
| `README.md` | You are here. |

---

## Safety notes

- **Dry-run is the default.** First live run requires explicit env flip.
- **Daily cap per segment.** Hard stop at `SDR_DAILY_CAP_PER_SEGMENT` regardless of how many Apollo returns.
- **Per-lead try/except.** A single bad row never kills the run.
- **No founder inbox involved.** All sends originate from Instantly inboxes; nothing touches Priyesh's personal Gmail.
- **Domain reputation.** We're using existing Instantly inboxes (your decision). If reply quality is good but deliverability degrades, the migration path is a separate outbound domain — that's a 2-hour project, not a blocker today.
