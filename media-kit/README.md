# destination.com media kit — placement and audience slides

Eight shareable JPGs (2400×1350) for advertiser conversations. Built for the
Expedia thread, but nothing in the generator is Expedia-specific beyond
`brand.prepared_for` in `config.json`.

```bash
pip install Pillow          # once
python3 media-kit/build.py  # writes media-kit/out/*.jpg
```

## What is safe to send today, and what is not

| Slide | State |
|---|---|
| `01-cover` | Needs data — carries four reach tiles |
| `02-desktop-in-article` | **Ready to send** |
| `03-where-to-stay-native` | **Ready to send** |
| `04-mobile-in-feed` | **Ready to send** |
| `05-newsletter-sponsorship` | Ready except the three list-metric fields |
| `06-audience-reach` | Needs data |
| `07-audience-demographics` | Needs data |
| `08-placement-specs` | **Ready to send** |

The placement slides are mockups of where units sit on the page — that is what
a placement diagram is, and each carries a footer saying so. The audience
slides are a different thing: they assert facts about real people. **No figure
in them is estimated, inferred, or filled in with a plausible-looking number.**
Every value is `null` in `config.json` until someone puts a measured one there.

While any figure on a data slide is still `null`:

- the value renders as a hatched **ADD DATA** chip, and
- the slide carries a red **TEMPLATE — figures not yet populated** banner.

Both disappear on their own once the config is complete. Do not work around
them by typing numbers into the HTML — edit `config.json` and re-run, so the
banner logic keeps telling the truth about what has been sourced.

## Where each figure comes from

**GA4** → Reports → User → User attributes → Demographic details

| Config field | GA4 dimension |
|---|---|
| `age[].pct` | Age |
| `gender[].pct` | Gender |
| `geography[].pct` | Country (Reports → User → Tech/Geo → Country) |
| `device[].pct` | Device category |
| `reach.monthly_readers` | Active users, last complete month |
| `reach.monthly_pageviews` | Views, same period |
| `reach.pages_per_session` | Views ÷ Sessions |
| `reach.avg_engaged_time_min` | Average engagement time per session |
| `reach.returning_reader_pct` | Returning users ÷ Total users |

GA4 reports age and gender **only for users with Google Signals active** — a
subset, not the whole audience. Read the coverage share off the report's
sampling note and put it in `demographics_source_note`, which prints in the
footer of both data slides. Quoting a demographic split without that caveat
overstates what the data supports, and an advertiser's analytics team will ask.

**ESP** (whatever sends The Dispatch) → `reach.newsletter_subscribers`,
`newsletter_open_rate_pct`, `newsletter_ctr_pct`. Use a trailing-3-month
average rather than the best single issue.

**Intent signals** are the strongest thing on the deck for a travel advertiser
and are not standard GA4 reports — build them as explorations:

| Config field | How |
|---|---|
| Sessions landing on a destination/accommodation guide | Landing page exploration filtered to guide path prefixes ÷ total sessions |
| Sessions reaching "Where to Stay" | Scroll or section-view event on the accommodation anchor ÷ sessions on guide pages |
| Average engaged time on guide pages | Engagement time filtered to the guide path prefix |
| Outbound booking-partner clicks | `affiliate_clicks` rows for the month (the `/api/go/*` bouncer already writes these) |

If GA4 demographic coverage turns out thin, lead the conversation with the
intent block instead. Behaviour at the point of decision is a better argument
to a travel advertiser than an age pyramid.

## Editing

- **Numbers** → `config.json`. Nothing else.
- **Placement copy, sizes, positions** → `config.json` → `placements[]`, which
  feeds both the per-placement slides and the summary table on slide 08.
- **Layout and styling** → `build.py`. Brand tokens (colours, type) sit in the
  `CSS` constant and match `destination-redesign-mockups/`.
- **A different advertiser** → change `brand.prepared_for` and `brand.date`.

Chart colours are the destination.com brand triple (`#1B6CA8`, `#C4703E`,
`#0D9B76`), validated all-pairs for colour-vision deficiency and for ≥3:1
contrast against the light surface. Every split-bar segment is direct-labelled
and legended, so identity is never carried by colour alone. If you change a
data colour, re-validate rather than eyeballing it.

## Notes

- Renders with `headless_shell`, not the full Chrome binary: `--window-size`
  maps 1:1 onto the viewport there, whereas Chrome reserves ~87px for browser
  chrome and silently crops the bottom of every slide. `build.py` asserts the
  capture dimensions so that failure mode cannot return quietly.
- Fonts (DM Sans, Playfair Display) are base64-embedded in
  `fonts/fonts-embedded.css` so builds do not depend on network access.
- `_work/` holds intermediate HTML and PNG and is gitignored; `out/` is
  committed so the JPGs are shareable without a build.

## Before this becomes a paid placement

`docs/expedia-affiliate-decision.md` records Expedia's affiliate compliance
terms, several of which bind a paid or sponsored relationship: "Expedia" may
not appear in ad copy without prior written approval, `www.expedia.com` may not
be used as a display URL, there is no direct linking to Expedia US from any
paid or sponsored listing, and no promotion via Twitter or Facebook without
written approval. Get the waiver in writing in the same thread rather than
after the fact.
