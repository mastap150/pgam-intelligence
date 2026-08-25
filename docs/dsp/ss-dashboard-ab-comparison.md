# Self-serve dashboard — A/B comparison and recommendation

**Surfaces:** `/ss-dashboard` (A) · `/ss-dashboard1` (B) · `/ss-dashboard-concept` (proposed)
**Code:** `mastap150/pgam-dsp-dashboard` @ `d1e1631`
**Date:** 2026-08-25
**Report:** https://claude.ai/code/artifact/e05a1362-2de6-43aa-ad5f-fc34da118040
**PR:** mastap150/pgam-dsp-dashboard#550 (draft)

> Unlike the 2026-08-18 review, this one is from rendered screenshots, not source.
> `demo.dsp.pgammedia.com` is still blocked by the session egress proxy, so both
> routes were run locally (`next dev`, demo fixtures via `demo.localhost` + a
> dev-minted demo cookie) and captured with Playwright at 1440 / 1280 / 768.

---

## Verdict

**Dashboard B (`/ss-dashboard1`) is the base. It is not close — 75/100 against 40.**

B is a deliberate rebuild against the Q3 handoff and it has already fixed every
structural fault the 2026-08-18 review found in A: eight creation CTAs became two,
three stacked gradient surfaces became one, the fabricated sparklines are gone, and
every figure derives from the ledger. It reaches the first real number in ~270px
where A takes ~950.

**B is not finished.** It answers *what is running* and never *how is it trending*
or *what should I do next* — and leaves roughly half the fold empty doing it. At
1440×900 B's content stops around y=475.

`(attune)` is a full parallel surface, not one page: 12 mirrored `*1` routes. So
"adopt B" is a decision about the whole self-serve shell, not this screen.

## Scorecard

| Category | A | B |
|---|--:|--:|
| Overall visual design | 5 | 8 |
| Professional / enterprise feel | 4 | 8 |
| Typography | 5 | 8 |
| Information hierarchy | 3 | 8 |
| Navigation | 4 | 7 |
| Dashboard readability | 4 | 8 |
| Data visualization | 2 | 5 |
| Spacing / layout | 4 | 7 |
| Ease of use | 5 | 8 |
| Modern DSP feel | 4 | 8 |
| **Total** | **40** | **75** |

Data visualization is the category neither wins: A's sparklines are drawn
competently but are literal arrays; B has honest pacing bars and no trend chart.

## Keep from A

- The greeting ("Good afternoon, Demo") — B dropped warmth for "Your June" for nothing.
- The setup checklist with `2 / 4` progress.
- **Live activity** — "Call attributed to YouTube TV · $65 lead" is the most
  persuasive thing on either screen. Keep it, but as a full-width strip, not a
  second right rail.
- Wallet CTA within reach of the top.

## Keep from B

- KPI strip as **one card with hairline cells**, not four boxes in three shapes.
- **Footnoted tiles** — every number states its basis.
- **The flight schedule.** Campaigns on a real date axis with pacing bars. The single
  most credible component in the product.
- The derived sub-line, computed from the same aggregate as the tiles.
- Wallet balance persistent in the header.
- Two creation paths.

## Problems found in the browser

**A — ship blockers**
- Fabricated data: sparklines are `[40,55,50,70,62,82,100]` / `[30,48,58,54,74,86,100]`;
  trend pills are a hardcoded `▲8%` / `▲12%`.
- **"Calls & leads: 0" renders above a sparkline with bars in it.** Visible at 1440.
- The white-label preview chip overlaps the Active campaigns tile at 768.

**A — structural**
- Eight creation CTAs behind five routes; the footer tells the user the main path is
  the limited one.
- Nav overflow fires at every width — Tools and Help behind "More ▾" on a 27" monitor;
  four unlabeled icons at 768.
- Double-wrapped layout: group clamps 1200, page declares 1560.
- Truncated campaign names beside a **Pacing column that renders empty**.
- 5,486px tall at 768.

**B — unfinished**
- No trend anywhere; the range pills change four numbers and nothing else.
- Half the fold empty.
- **Spend missing from the KPI strip** — it appears only in a footnote under CPL.
- Attention is a bare `77`: no band, no components, no way in.
- No insights or recommendations.
- `+474%` off a near-zero base reads as broken.

**Both**
- Header collides at 768 — wordmark overlapped by the first nav item.
- "PREVIEW WHITE-LABEL" chip floats over content on every route.
- No campaign table with search / sort / pagination.

## Typography

Inter is correct — true tabular figures are what make a column of money legible.
The fault is discipline, not typeface: A uses nine arbitrary `text-[Npx]` values and
semibold on 11px micro-labels.

| Role | Spec |
|---|---|
| Page title | 32 / 600 / −0.021em |
| KPI value | 28 / 600 / −0.026em / tnum |
| Signature metric | 44 / 600 / −0.030em / tnum |
| Section heading | 15 / 600 |
| Body | 14 / 400 / 1.55 |
| Table data | 13 / 400, numbers tnum + right |
| Table row title | 14 / 500 |
| Secondary | 12 / 400 |
| Micro-label | 11 / 500 / 0.055em caps |
| Button / nav | 13 / 500–600 |

Three rules do most of the work: **never 700**; **never semibold below 12px**;
**`tabular-nums` on every figure**, not just tables.

## Recommended hierarchy

1. Greeting + derived sub-line
2. KPI strip — Spend · Calls & leads · Cost per lead · Attention · Wallet runway
3. Performance chart + Attention panel, side by side at ≥1100px
4. PGAM Insights
5. What's on air (B's flight schedule, unchanged)
6. Goal row

Do **not** ship a hardcoded "Performance is 14% above benchmark" in the greeting.
No benchmark exists in the ledger, and that is exactly the fault that makes A
untrustworthy — worse in a greeting, because it reads as a promise.

## Three directions

| | Scope | Cost | Leaves |
|---|---|---|---|
| **1 · Refined B** | Ship B, fill the fold, add Spend, set the type scale | ~1 day | No trend; Attention inert |
| **2 · Premium DSP** *(recommended)* | + performance chart, + real campaign table | ~3–4 days | Attention still a tile |
| **3 · PGAM Intelligence** | + Attention panel, + typed insights | ~1 week | Insight copy needs review |

They are strictly additive — one branch, not three. Stage: ship 1, then 2, then 3.

## Prioritised changes

**Must change**
1. Adopt B as the base.
2. Delete A's fabricated sparklines and trend pills.
3. Put Total spend back on the KPI strip.
4. Fix the 768px header collision.
5. Stop the white-label chip floating over content.
6. Fill the empty fold.

**Should change**
1. Performance chart with metric switching — the largest credibility gain available.
2. Attention panel — score, band, three components.
3. Insights panel — five signal types, all derivable today.
4. Rebuild the campaign table.
5. One status pill; add *Starts &lt;date&gt;* and *Out of funds*.
6. Write the type scale into `docs/design-system-ss.md` and hold it in review.

**Nice to have**
Live activity as a full-width strip · comparison-period overlay · dark mode ·
saved views · benchmark line on Attention once a real benchmark exists.

## Concept route

`/ss-dashboard-concept`, branch `claude/dsp-dashboard-ui-comparison-bsae62`.
Additive: one new file, no existing route / component / API / token / shared
stylesheet modified. `/ss-dashboard1` at 768px is byte-identical before and after.
Every figure, series point, delta and insight is derived from the ledger — the
`ledger-types.ts` no-display-constants rule holds.

Responsive ramp measured: 2 columns at 390px, 3 at 768 and 1024, 5 at 1440, no
horizontal overflow at any width.

### Reproducing the screenshots

The demo host is proxy-blocked, so render locally:

```bash
git clone --depth 1 https://github.com/mastap150/pgam-dsp-dashboard
cd pgam-dsp-dashboard && npm install
echo "127.0.0.1 demo.localhost" >> /etc/hosts
npx next dev -p 3011
```

`isDemoRequest` needs a `demo.*` host plus any `pgam_demo_auth` cookie; outside
production the token secret falls back to a dev-only string, so a valid cookie can be
minted locally with `signDemoToken` (`src/lib/demo/demo-token.ts`). Add `pgam_demo=1`
and browse `http://demo.localhost:3011/ss-dashboard1`. `?state=first-run` walks the
empty states.

Note: `next build` fails in a fresh clone with `No database connection string was
provided to neon()` at `/api/ctv/impression/log`. That reproduces on `main` and is a
missing `POSTGRES_URL`, not a code fault — any dummy value makes the build pass.
