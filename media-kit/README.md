# destination.com media kit — placement and audience deck

Nine 16:9 slides for advertiser conversations, rendered as JPGs and assembled
into a PDF and a PPTX. Built for the Expedia thread, but nothing in the
generator is Expedia-specific beyond `brand.prepared_for` in `config.json`.

```bash
pip install Pillow python-pptx   # once
python3 media-kit/build.py       # writes media-kit/out/
```

Outputs in `media-kit/out/`:

| File | What it is |
|---|---|
| `NN-*.jpg` | One 2400×1350 JPG per slide, for pasting inline into email |
| `destination-com-media-kit.pdf` | The deck, 9 full-bleed pages — the usual thing to attach |
| `destination-com-media-kit.pptx` | Same slides in a 13.333×7.5in deck, one image per slide |

The PPTX carries the slides as images rather than native shapes. The layouts are
CSS-rendered, so converting them to PowerPoint shapes would lose fidelity and
gain nothing — edit `config.json` and re-run instead of editing slides by hand.

## The slides

| Slide | Content | Depends on data? |
|---|---|---|
| `01-cover` | Title, four reach tiles | Yes |
| `02-desktop-in-article` | 970×250 in a desktop guide page | No |
| `03-where-to-stay-native` | Sponsored card in the accommodation module | No |
| `04-mobile-in-feed` | 300×250 in the mobile article flow | No |
| `05-newsletter-sponsorship` | Sponsor banner at top of issue | List metrics only |
| `06-audience-reach` | Reach tiles, geography, engagement | Yes |
| `07-audience-demographics` | Age, gender, device, intent signals | Yes |
| `08-audience-segments` | Affinity/in-market segments, income deciles | Yes |
| `09-placement-specs` | All units in one table | No |

## `figures_basis` — read this before sending the deck

`config.json` carries a top-level `figures_basis`, and it governs how the
numbers are presented:

- **`"estimated"`** (current state) — every data slide carries an amber
  **ESTIMATES · PENDING VERIFICATION** pill in its header and prints
  `basis_note` in its footer. The audience figures currently in `config.json`
  are modelled, not measured: the 60,000 newsletter list size was supplied by
  PGAM, and everything else was derived from it and from travel-publisher
  benchmarks so the *shape* of the audience can be discussed.
- **`"measured"`** — the pill and the basis note disappear. Set this only once
  every figure has been replaced with a real GA4 or ESP number.

Do not set `figures_basis` to `"measured"` to tidy up the slides. The pill is
the only thing standing between a modelled number and an advertiser treating
it as a delivery commitment, and an analytics team that later reconciles
against real GA4 will find the gap.

Any figure left `null` renders as a hatched **ADD DATA** chip and puts a red
TEMPLATE banner on its slide, independent of `figures_basis`.

## Where each figure should come from

**GA4** → Reports → User → User attributes → Demographic details

| Config field | GA4 dimension |
|---|---|
| `age[].pct` | Age |
| `gender[].pct` | Gender |
| `income[].pct` | Household income (US deciles) |
| `segments[].pct` | Affinity categories + In-market segments |
| `geography[].pct` | Country (Reports → User → Tech/Geo → Country) |
| `device[].pct` | Device category |
| `reach.monthly_readers` | Active users, last complete month |
| `reach.monthly_pageviews` | Views, same period |
| `reach.pages_per_session` | Views ÷ Sessions |
| `reach.avg_engaged_time_min` | Average engagement time per session |
| `reach.returning_reader_pct` | Returning users ÷ Total users |

GA4 reports age, gender, income and affinity **only for users with Google
Signals active** — a subset, not the whole audience. Read the coverage share
off the report's sampling note and fold it into `basis_note`, which prints in
the footer of every data slide. Quoting a demographic split without that
caveat overstates what the data supports, and an advertiser's analytics team
will ask.

Segment shares overlap by design — one reader can sit in several — so they do
not sum to 100%. `segments_note` says so on the slide; keep that true if you
change the segment list.

**ESP** (whatever sends The Dispatch) → `reach.newsletter_subscribers`,
`newsletter_open_rate_pct`, `newsletter_ctr_pct`. Use a trailing-3-month
average rather than the best single issue.

**Intent signals** are the strongest thing in the deck for a travel advertiser
and are not standard GA4 reports — build them as explorations:

| Config field | How |
|---|---|
| Sessions landing on a destination/accommodation guide | Landing page exploration filtered to guide path prefixes ÷ total sessions |
| Sessions reaching "Where to Stay" | Scroll or section-view event on the accommodation anchor ÷ sessions on guide pages |
| Average engaged time on guide pages | Engagement time filtered to the guide path prefix |
| Outbound booking-partner clicks | `affiliate_clicks` rows for the month (the `/api/go/*` bouncer already writes these) |

If GA4 demographic coverage turns out thin, lead with the intent block instead.
Behaviour at the point of decision is a better argument to a travel advertiser
than an age pyramid.

## Editing

- **Numbers** → `config.json`. Nothing else.
- **Placement copy, sizes, positions** → `config.json` → `placements[]`, which
  feeds both the per-placement slides and the summary table on slide 09.
- **Layout and styling** → `build.py`. Brand tokens (colours, type) sit in the
  `CSS` constant and match `destination-redesign-mockups/`.
- **A different advertiser** → change `brand.prepared_for` and `brand.date`.

Chart colours are the destination.com brand triple (`#1B6CA8`, `#C4703E`,
`#0D9B76`), validated all-pairs for colour-vision deficiency and for ≥3:1
contrast against the light surface. Every split-bar segment is direct-labelled
and legended, so identity is never carried by colour alone; a segment too
narrow to hold its label drops the label rather than clipping it. If you change
a data colour, re-validate rather than eyeballing it.

## Notes

- Renders with `headless_shell`, not the full Chrome binary: `--window-size`
  maps 1:1 onto the viewport there, whereas Chrome reserves ~87px for browser
  chrome and silently crops the bottom of every slide. `build.py` asserts the
  capture dimensions so that failure mode cannot return quietly.
- The PDF is written at 150dpi, where 2400×1350 is exactly 16×9in — pages come
  out full-bleed with no scaling and no margin.
- Fonts (DM Sans, Playfair Display) are base64-embedded in
  `fonts/fonts-embedded.css` so builds do not depend on network access.
- `_work/` holds intermediate HTML and PNG and is gitignored; `out/` is
  committed so the deck is shareable without a build.

## Before this becomes a paid placement

`docs/expedia-affiliate-decision.md` records Expedia's affiliate compliance
terms, several of which bind a paid or sponsored relationship: "Expedia" may
not appear in ad copy without prior written approval, `www.expedia.com` may not
be used as a display URL, there is no direct linking to Expedia US from any
paid or sponsored listing, and no promotion via Twitter or Facebook without
written approval. Get the waiver in writing in the same thread rather than
after the fact.
