# Attune Tag — installation

*Client-facing. This document is safe to send to an advertiser as-is: it names
no vendor. For how the wrapper actually works and what it cannot hide, see
[`README.md`](./README.md) — internal only.*

---

Attune measures connected-TV campaigns by matching households, not clicks. A TV
ad plays in a household; when someone in that household later visits your site
on any device on the same Wi-Fi, the Attune Tag records the visit and credits it
to the campaign.

Nothing works without the tag. Install it before launch, not after.

## 1. Base tag — every page

Paste this into the `<head>` of **every page** on your site. Most platforms have
a single "header code" or "code injection" field that applies site-wide.

```html
<script async src="https://tag.pgammedia.com/attune.js"
        data-attune-id="YOUR-ID"></script>
```

We supply `YOUR-ID`. It is specific to your account.

> **Homepage only is the single most common mistake.** Enquiries that happen on
> any other page are then invisible, and the campaign optimises against
> incomplete data.

## 2. Enquiry tag — confirmation pages only

On the page a visitor lands on **after** submitting an enquiry form — the
thank-you or confirmation page — add:

```html
<script>attune('event', 'lead');</script>
```

Fire this only on confirmation. On a normal page it counts every visitor as an
enquiry and the campaign optimises toward nothing.

If your form submits without a page change, call `attune('event', 'lead')` in
your success handler instead.

## 3. Phone calls

Calls are the harder half of the measurement problem, because a phone call
leaves no trace in a browser. There are two layers:

**Automatic — click-to-call.** Any `<a href="tel:...">` link is tracked as an
enquiry with no work from you. This covers most mobile visitors. Make sure your
phone number is a real `tel:` link rather than plain text.

**Manual — everyone else.** A visitor who reads the number off the screen and
dials from a desk phone is invisible to any website tag. Capturing those needs a
call-tracking service with dynamic number insertion, which shows each visitor a
distinct number and ties the call back to their session. We will set this up
with you — it is worth doing, and it is the only honest way to count those
calls.

## 4. Platform notes

| Platform | Where the base tag goes |
|---|---|
| WordPress | Header/footer plugin (e.g. WPCode) → Header |
| Wix | Settings → Advanced → Custom Code → All Pages → Head |
| Squarespace | Settings → Advanced → Code Injection → Header *(Business plan or above)* |
| GoDaddy | Settings → Site Settings → Code Injection → Header |
| Go High Level | Sites → Websites → Head tracking code |
| Google Tag Manager | Custom HTML tag, trigger **All Pages**, then **Submit → Publish** |

Shopify checkout blocks third-party scripts — tell us if you sell through
Shopify and we will handle checkout separately.

Using Google Tag Manager? The tag will not go live until you click **Submit**
and then **Publish**. Saving is not publishing.

## 5. Check it worked

1. Open your site in Chrome.
2. Open DevTools (`F12` or `Cmd+Option+I`) → **Network** tab.
3. Reload. Filter for `attune`.
4. You should see the tag load without error.

Then visit a few pages and submit a test enquiry. Tell us when you have, and we
will confirm the events arrived on our side — usually within seconds.

## 6. What happens next

Once page views are flowing we can launch. Two things are worth knowing about
the timing:

- We need **at least 12 hours of live traffic** through the tag before the
  campaign can go live. Install early.
- Reporting is deliberately conservative. Attune only credits visits it can
  match to a household that saw your ad, so its numbers will read lower than
  Google Analytics' — GA counts every visit from every source. That gap is
  expected, and it is the honest direction to be wrong in.

Questions: your PGAM Media contact.
