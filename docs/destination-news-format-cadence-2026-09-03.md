# Destination.com news — format, ads, cadence changes (2026-09-03)

Owner feedback: article formats still weak (example:
`/news/huakai-by-hawaiian-adds-free-bags-boosted-rewards`), no ads on news
pages, and "only a handful of pieces publish a day."

Implemented in `mastap150/destination-com` PR
[#585](https://github.com/mastap150/destination-com/pull/585)
(branch `claude/news-article-format-and-cadence`):

1. **Format** — the synthesis contract now requires the "What travelers need
   to know" key-facts block (audit 2026-08-25 §5 / fix #3, previously never
   landed), `<h2>` subheads every 200–300 words, tables/lists for enumerable
   facts, `<time datetime>` on dates, at 450–650 words. `.news-body` CSS
   renders it; legacy paragraph-only articles are unaffected.
2. **Ads** — `/news/[slug]` and `/news` now run the existing Aditude stack
   (they had zero units): in-content ads after paragraph 2 then every 4,
   never after the last, max 3, plus an end-of-story bottom leaderboard and
   an index top leaderboard. Placement rules are pure + tested
   (`src/lib/news-body.ts`). The dek → key-facts area stays ad-free
   (content strategy 2026-08-26 §9).
3. **Cadence** — `news-autopublish` raised from 2 to 4 runs/day (11:30,
   14:30, 17:30, 20:30 UTC); ceiling 6 → 12/day. All quality gates
   (originality, relevance, search floor, per-source cap) unchanged. The
   rescue pass is now capped **per day** instead of per run
   (`countRecentItems`), so more runs cannot multiply below-floor output.

## Standing decisions this supersedes / reaffirms

- The strategy docs' "cap ~3/day until an editor is staffed" throttle is
  superseded **at the owner's explicit request** (this session). What makes
  the raise defensible now vs. 2026-08-26: primary sources are live, the
  autopublish gates mechanize the originality test, and the weekly
  performance review is watching per-article numbers.
- **Reaffirmed:** per-article performance, not volume, authorizes the next
  raise (audit §8). If engagement per story falls as output rises, drop
  `DEFAULT_MAX_PER_RUN` or the run count back down. The throttle guard test
  in `news-throttle.test.ts` records this and blocks a silent raise past
  12/day.
- Existing articles keep their old bodies; only stories synthesized after
  merge carry the key-facts block.
