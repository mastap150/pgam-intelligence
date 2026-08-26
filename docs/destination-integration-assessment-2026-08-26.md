# Destination.com — Integration Assessment

**2026-08-26.** Written against the brief "bring everything together into a
polished user journey rather than a collection of individual features."

I went looking for what makes the experience feel fragmented. It is not a
missing feature. It is two specific, measurable things, and both are
consolidation work rather than new building.

---

## Finding 1 — There are three different article experiences

`MigratedArticleTemplate` already contains almost everything the brief asks
for: `RelatedArticles`, `RelatedToolsForArticle`, `NewsletterForm`,
`LeadMagnetCTA`, `CreditCardCtaBlock`, `GlobalPartnersModule`,
`FlightsFromAnywhere`, `CompareStaysInline`, and `withCtaTracking`.

It is used by **275 of 732 guide pages — 38%.**

| Experience | Pages | Recirculation | Newsletter | Conversion |
|---|---|---|---|---|
| `MigratedArticleTemplate` | 275 (38%) | ✅ | ✅ | ✅ |
| Hand-rolled guide pages | 457 (62%) | inconsistent | inconsistent | 57% have *something* |
| **`/news/[slug]`** | all news | ❌ | ❌ | ❌ |

Every guide is a separate hand-authored `page.tsx`; there is no shared dynamic
route, so there is nothing enforcing consistency. A reader landing on one guide
gets related content, a newsletter prompt and a booking path. A reader landing
on the next guide may get none of it. **That inconsistency is the fragmentation
— readers experience it directly as "some pages are finished and some aren't."**

`/news/` is the worst case and the one currently taking the traffic: it uses
none of the shared template, and ships three links (Home, News,
editorial-standards) with no newsletter capture, no related content and no
booking path. The NewsBreak traffic in the audit lands there and leaves.

**The fix is migration, not construction.** The components exist and are
already wired for tracking. This is the single highest-leverage action
available, and it needs no new features at all.

### Flight search is the weakest seam

**0 of 732 guide pages** link to a `/flights/` route. Flight conversion exists
only as the `FlightsFromAnywhere` widget inside the shared template — so the
62% of guides not on that template have no flight path whatsoever, and news has
none either. The `/flights/[route]` templates the audit recommended linking to
are effectively orphaned from the content that should feed them.

---

## Finding 2 — Nine PRs have been open for two to four months

This is the other reason it reads as separate features: **the features are
built and unmerged.** Fourteen PRs are open on `destination-com`. They split
cleanly in two.

**Recent and mergeable — all clean against main:**

| PR | Branch | Age | State |
|---|---|---|---|
| 455 | `claude/news-opportunity-engine` | today | clean · typecheck, lint, 11/11 tests |
| 454 | `claude/news-image-rights` | today | clean · CI green |
| 453 | `claude/media-kit-enquiry-form` | 1d | clean |
| 452 | `claude/careers-apply-form` | 1d | clean, 5 behind |
| 446 | `claude/destination-newsletter-audit` | 2d | clean, 21 behind |

**Stale — every one of these is 87 commits behind main:**

| PR | Branch | Last commit | Ahead | What it is |
|---|---|---|---|---|
| 429 | `feat/cardratings-integration` | Aug 24 | 217 | Points/cards comparison + primer |
| 348 | `feat/hotels-com-registry-backfill` | Aug 15 | 127 | Hotels.com CJ feed → property IDs |
| 244 | `chore/expedia-guide-sweep` | Aug 5 | 42 | 412 legacy guides → correct CTAs |
| 241 | `chore/expedia-lob-commission-fix` | Aug 5 | 41 | Re-route uncommissioned LOBs |
| 137 | `feat/affiliate-earnings` | **Jun 11** | 35 | Daily Impact + Travelpayouts sync |
| 135 | `feat/travel-shop-deeplinks` | **Jun 9** | 31 | Travel Shop CTA on best-hotels pages |
| 132 | `feat/expedia-rapid-mapping` | **Jun 9** | 30 | Expedia Rapid property-id mapping |
| 131 | `feat/tm-surface-expansion` | **Jun 9** | 30 | Events rail + HotTickets placement |
| 129 | `fix/sweepstakes-resend-routing` | **May 18** | 30 | Sweepstakes → Resend welcome flow |
| 92 | `feat/deep-dive-essays` | **Apr 20** | 30 | Long-form essay program |

Read that list against the brief. **Affiliate integrations, flight/hotel
deeplinks, travel discovery, curation, monetization** — the exact workstreams
the brief wants unified are sitting in these branches. The product does not
feel joined up because more than half of it has never reached `main`.

At 87 commits behind, none of these will merge by fast-forward, and the two
Expedia branches (#241, #244) touch overlapping CTA logic, so they need
sequencing rather than parallel merges.

---

## The journey, and where it actually breaks

```
  discover ──▶ inspire ──▶ research ──▶ search ──▶ convert
   /news       /guides      /guides     /flights   affiliate
     │            │            │           │           │
     ▼            ▼            ▼           ▼           ▼
   BREAK       38% only     38% only   orphaned    unmerged
```

- **discover → inspire.** `/news/[slug]` has no related content and no path
  into guides. `/news` is not in the site navigation at all. This is the break
  that matters most, because it is where the NewsBreak and search traffic lands.
- **inspire → research.** Works on the 38% using the shared template;
  inconsistent on the rest.
- **research → search.** The weakest seam. No guide links to a `/flights/`
  route; flight conversion depends entirely on a widget most pages don't render.
- **search → convert.** Substantially built, substantially unmerged (#137,
  #135, #132, #241, #244, #348).

---

## Recommended order

Sequenced by risk and dependency, not by size.

**1 — Land the five clean PRs.** #455, #454, #453, #452, #446. All clean
against main, all verified. This is same-day work and it stops the backlog
growing.

**2 — Bring `/news/[slug]` onto the shared template.** Highest leverage per
hour in the whole plan: it closes the discover → inspire break on the pages
currently taking the traffic, and it is one template swap plus the article
redesign already specced in the content strategy. Do it after #454 lands so the
two do not collide on the same file.

**3 — Revive the monetization PRs, oldest-risk first.** Take them in dependency
order — #241 and #244 together (overlapping Expedia CTA logic), then #132 and
#348 (property-ID mapping), then #137 and #135. Each needs main merged in,
regenerated lockfiles and a real test pass; at 87 commits behind, treat each as
a fresh integration rather than a rebase. **Decide explicitly whether #92 (Apr)
and #129 (May) are still wanted** — four months of drift may cost more to
reconcile than to rewrite, and closing them is a legitimate outcome.

**4 — Migrate guides onto the shared template.** The 457 hand-rolled pages, in
batches, highest-traffic first. Mechanical and safe once the template is
settled; a good candidate for a dedicated session per batch.

**5 — Wire flight search into content.** Link `/flights/[route]` from news and
guides using the `country`/`region`/`tags` already extracted at publish time.

---

## What I could not assess

**"The recent Insite work."** I could not find it. `insite` appears nowhere in
`pgam-intelligence` or `destination-com`, there is no repository matching it on
the account, and no session by that name. Whatever it refers to was discussed
somewhere this session cannot see — a different session, a different repo, or a
different tool. **It is excluded from everything above**, and it needs a
pointer (repo, PR, or session) before it can be integrated.

**Live desktop/mobile testing.** `destination.com` is blocked by this
environment's egress proxy, so I could verify code and build but could not load
the real site, test responsive behaviour, or walk the journey as a user.
Everything above is from source, the repository's own history, and the build —
not from a browser. Vercel preview deployments on the open PRs are the practical
way to close that gap.

---

## An operational note

Every currently-running session on this account is reporting
`isUsingOverage: true` with requests being **rejected** at the five-hour limit.
`CLAUDE.md` is explicit that long-lived sessions, not scheduled jobs, are what
drives this, and that overage compounds itself. Six sessions have been working
adjacent pieces of this product in parallel today.

The consolidation above is partly a cost argument as well as a product one:
landing what exists is cheaper than rebuilding it, and one session working a
merge queue in order costs far less than several working the same ground at once.
