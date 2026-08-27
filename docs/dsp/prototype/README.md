# Attune self-serve prototype — source

`attune-self-serve-prototype.html` is the built file. Open it in a browser;
it needs no server and no build step.

Published copy: https://claude.ai/code/artifact/3f4eded5-4205-4927-b47e-6133a6b06d00

## Why the source is here

The prototype was authored in five parts and concatenated. Editing the built
file directly works but is unpleasant — it is one 330 KB document. Edit the
parts and rebuild:

```sh
cat p_css.html p_shell.html p_pages2.html p_pages3.html p_js.html \
  > attune-self-serve-prototype.html
```

| file | what is in it |
|---|---|
| `p_css.html` | `<title>`, the Inter link, and the whole stylesheet including the token block |
| `p_shell.html` | prototype chrome, top bar, ⌘K palette, `<main>` open |
| `p_pages2.html` | campaigns, campaign detail, results, attention, grow, tools, wallet, help |
| `p_pages3.html` | creatives, settings, the five-step builder, integrations, footer, `</main>` |
| `p_js.html` | every behaviour, in one IIFE |

Concatenation order matters: `p_js.html` reads elements the page files define,
and `p_pages3.html` closes the `<main>` that `p_shell.html` opens.

## The rules the code holds itself to

Worth knowing before editing, because several of them look like styling and
are not:

- **One ledger.** `WK` in `p_js.html` is 24 weekly buckets and every figure on
  every screen is a sum over a slice of it. A week belongs to the month it
  *starts* in. Break either and the statements stop matching Results.
- **A dash is never a zero.** An unmeasured week is excluded from a cost-per-
  lead calculation, not counted as zero; a campaign with no cost per lead
  sorts to the bottom in both directions.
- **Share means share of the total**, not of the largest row.
- **Rate charts get a padded baseline**, labelled `not zero`.
- **Attention rolls up.** Every breakdown on Results is impression-weighted to
  exactly 77. If you change a row, re-derive the rest.
- **Design tokens only.** No raw hex outside the `:root` block; nine type
  sizes, four radii, three tracking values, one easing curve.
- **Logotypes are exempt from contrast**, everything else is AA or better.

## Checks

The Playwright scripts that verified it are not committed — they were session
scratch. What they asserted, if you rebuild them: no duplicate ids, no
unlabelled control, every `th` scoped, every decorative SVG hidden, no
horizontal overflow at 390 / 768 / 1024 / 1440, WCAG AA contrast on all
thirteen routes (logotypes excepted), no `aria-pressed` control that does
nothing, and focus trapped inside the ⌘K dialog.
