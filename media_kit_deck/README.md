# Destination.com Media Kit

Advertiser-facing media kit deck — 20 slides, 16:9.

```
npm install
node build.js      # writes Destination_com_Media_Kit.pptx
```

## Before sending externally

All audience figures live in the `N` object at the top of `build.js`. Only
`subscribers` is confirmed — everything marked `PLACEHOLDER` is an
industry-plausible default and must be replaced with real ESP / analytics
data before the deck goes to an advertiser:

| Field | Status |
|---|---|
| `subscribers` | Confirmed — 50,000+ |
| `openRate` | PLACEHOLDER — pull 90-day average from ESP |
| `ctr` | PLACEHOLDER — pull 90-day average from ESP |
| `sendsPerWeek` | PLACEHOLDER — confirm cadence |
| `monthlyReaders` | PLACEHOLDER — confirm from site analytics |
| `listGrowth` | PLACEHOLDER — confirm from ESP |
| `usShare` | PLACEHOLDER — confirm from ESP geo report |

Rate-card and package pricing (slides 15–16) is an opening position derived
from `08_monetization_strategy.md` scaled to a 50K list. Confirm against
what has actually closed before it goes out as a rate card.

## Structure

| # | Slide |
|---|---|
| 1 | Cover — "Reach travelers before they book." |
| 2 | The opportunity — travel advertising's timing problem |
| 3 | At a glance — audience numbers |
| 4 | Audience profile |
| 5 | Full travel journey — five stages |
| 6 | Divider — The Newsletter |
| 7 | Why the inbox outperforms |
| 8 | Newsletter sponsorship formats (6) |
| 9 | Segmentation & targeting |
| 10 | Seasonal moments |
| 11 | Divider — Destination.com |
| 12 | Site sponsorship formats (8) |
| 13 | Activation map — product by journey stage |
| 14 | Advertiser categories |
| 15 | Sample packages |
| 16 | Rate card |
| 17 | How a campaign runs |
| 18 | Measurement & brand safety |
| 19 | Why Destination.com |
| 20 | Contact |

## Design

Palette is pulled from the Destination.com site redesign
(`destination-redesign-mockups/homepage-premium.html`): ocean blue `#1B6CA8`,
teal `#0D9B76`, terracotta `#C4703E`, gold `#F4A124` on cream `#FAF7F2`.
Typography matches the PGAM house deck — Georgia headlines, Calibri body.

To preview: convert with LibreOffice and rasterize.

```
soffice --headless --convert-to pdf Destination_com_Media_Kit.pptx
pdftoppm -jpeg -r 52 Destination_com_Media_Kit.pdf slide
```
