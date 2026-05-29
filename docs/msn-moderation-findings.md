# MSN Partner Hub — Moderation State Capture Findings

**Status:** In progress (2026-05-29)
**Goal:** Decode MSN-side moderation state (rejected / under review / published) into the boxingnews `articles.msn_moderation_state` column so the feed can stop re-submitting rejected items and the dashboard can surface the new 60-day appeal deadline.

## What we know

| Endpoint | Returns | Per-doc? |
|---|---|---|
| `/msn/v0/pages/ugc/insights/content/realtime` | Articles MSN currently surfaces, with `titleStatus=1` for **all** captured rows (4,053 / 127 docs) | Yes, but only published items |
| `/msn/v0/pages/ugc/contents/report/partnerdocstats` | Aggregate: `contentSubmitted=714`, `contentPublished=696`, `contentRejected=17`, `contentPublishRate=97.48` | No — single brand-level row |
| `/msn/v0/pages/ugc/contents/report/partnerrejecteddocstats` | `docCount=17`, `failures=[]` (empty list — likely ingestion failures only, not moderation rejections) | No |
| `/msn/v0/pages/ugc/contents/report/partnerfeedstates` | Per-brand feed health rate (100%) | No |

**Bottom line:** every endpoint we've sniffed so far is aggregate. The per-doc rejected list — the thing we actually need to mirror — has not been fired by our scripted navigation.

## What we tried

1. **URL filter params** (`?status=rejected`, `?filter=rejected`, etc.) — SPA ignores them. `/partnerhub/content` redirects to `/partnerhub/home`.
2. **Text-based filter clicks** (`Rejected`, `Under review`, `Published`, `All`) — no matching DOM nodes on the home page.
3. **Home page DOM dump** — left nav has Home / Analytics / Monetization / Settings / Resources. **No "Moderation" submenu** despite earlier screenshot read (misread "Monetization" as "Moderation").
4. **"Resolve content issues" card → Download button** — couldn't find a clickable Download element via standard role/text selectors. The card text reads `"You've had 14 content issues over the last 30 days. Download report to review what needs to be updated."` — `Download` may be an inline link inside the body text, not a button.

## What's still unknown

- The SPA route for the rejected-articles list. Likely candidates not yet tried:
  - Clicking the "Resolve content issues" card itself (instead of looking for a Download button)
  - Clicking the brand-overview row for "Boxing News" (which may drill into per-brand content management)
  - Hitting `/partnerhub/home/contentissues` or similar URL directly
- The actual int enum used by MSN for moderation states. We've only ever seen `titleStatus=1` (published).
- Whether rejected items expose a `rejected_at` timestamp the dashboard can use for the 60-day appeal countdown.

## Artifacts

- `scripts/msn_moderation_capture_auto.py` — drives Partner Hub, captures all JSON XHRs
- `scripts/msn_partnerhub_dom_probe.py` — screenshot + clickable-elements dump
- `scripts/msn_content_issues_download.py` — attempts to trigger the "Download report" flow
- Latest captures in `~/.pgam/msn-session/`:
  - `moderation-capture-20260529T162333Z.jsonl` (74 XHRs, manual run aborted)
  - `moderation-capture-20260529T162855Z.jsonl` (126 XHRs, autonomous URL sweep)
  - `moderation-capture-20260529T163307Z.jsonl` (33 XHRs, autonomous w/ filter click attempts)
  - `probe/content-page.png` + `probe/content-elements.txt`
  - `probe/content-issues-xhr-20260529T163617Z.jsonl`

## Next step (recommended)

Visual driving. Either:
1. Open Partner Hub manually, take a fresh full-page screenshot, identify the exact CSS for the Download link inside the "Resolve content issues" card, then encode that into the script.
2. Click the "Resolve content issues" card *itself* (not the Download word inside it) to see if it routes to a Moderation/Issues SPA page that fires the per-doc XHR we need.
