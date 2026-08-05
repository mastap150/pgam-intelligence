# PGAM Contributor Agreement — Operator's Guide

This folder contains the freelance contributor agreement templates for PGAM Media LLC's editorial properties (**boxingnews.com**, **destination.com**, **healthnation.com**).

**Files:**

- `PGAM-Contributor-Agreement-Template.docx` — US contributor template (default; governing law: Florida)
- `PGAM-Contributor-Agreement-Template-International.docx` — variant for non-US contributors (Milagros-style; governing law: Florida with jurisdiction-consent + W-8BEN + OFAC clauses)
- `PGAM-Contributor-Agreement-Template.pdf` / `-International.pdf` — read-only renders for review

Rebuild both files from source with: `node /private/tmp/docx-build/build-contract.js` (or migrate the script into `scripts/` if you want it versioned in the repo).

---

## Why this exists

PGAM was on the receiving end of an image-licensing demand letter in August 2026 covering three images used by freelance contributors on boxingnews.com between 2023-11 and 2025-01. There was **zero contractual paperwork** with any of those writers — no work-for-hire clause, no warranty that submitted images were properly licensed, no indemnity. That made the exposure PGAM's problem when it should have been the writer's.

Sections **5** (Writer Warranties on Content and Images) and **7** (Indemnification) are the core protection. Everything else supports them.

---

## (a) Fields to fill in per writer

Before sending the file for signature, do a find-and-replace on these placeholders. All are wrapped in `[SQUARE BRACKETS]` and set in bold so they're easy to spot.

| Placeholder | What goes there | Example |
|---|---|---|
| `[EFFECTIVE DATE]` | Date the agreement takes effect | `August 5, 2026` |
| `[PGAM ADDRESS]` | Registered principal place of business for PGAM Media LLC | (your FL registered office / mailing address) |
| `[WRITER LEGAL NAME]` | Contributor's full legal name (not their byline) | `Hasib Rahman` |
| `[WRITER ADDRESS]` | Contributor's mailing address (or country of tax residence, on the international variant) | `123 Main St, City, State ZIP` |
| `[FLORIDA COUNTY, e.g., Miami-Dade County]` | The FL county whose courts are the exclusive venue | `Miami-Dade County` (or wherever PGAM is registered) |
| `[SIGNATORY NAME]` | Person signing for PGAM | `Priyesh Patel` |
| `[SIGNATORY TITLE, e.g., Managing Member]` | Their title at PGAM Media LLC | `Managing Member` |

For the **international variant only**, one additional placeholder in §1 needs filling: `[WRITER COUNTRY]` (Writer's country of tax residence — e.g., `Spain` for Milagros).

The **Article Rate ($11 USD per published Deliverable)** is baked into §3.1. To use a different rate for a specific writer, either edit §3.1 before sending, or attach a one-line signed assignment order overriding it — §3.1 already provides for written per-assignment overrides.

---

## (b) Clauses to have counsel review before first use

The whole document should get a lawyer's eyes at least once before it's used with a real contributor. Prioritize these:

1. **§4 (Work Made for Hire / Assignment).** Confirm the work-for-hire recital fits the actual editorial arrangement (commissioned pieces for a collective work). If any contributors are writing standalone longform that wouldn't qualify as work-for-hire under 17 U.S.C. § 101, the backstop assignment in §4.2 is doing the real work — counsel should confirm the language.
2. **§5 (Writer Warranties on Content and Images).** This is the central clause. Confirm it's enforceable in Florida, and verify the paparazzi / stock-wire list in §5.1(e) hasn't missed any agent that PGAM's properties actually attract claims from.
3. **§7 (Indemnification).** Cap-free indemnity favors PGAM heavily; counsel should decide whether that's likely to survive negotiation with pickier writers or whether a soft cap (e.g., 2× annual comp) is worth conceding to close deals.
4. **§7.3 (Settlements PGAM pays).** The "commercially reasonable" standard for a paid pre-litigation settlement is aggressive; some jurisdictions require independent judgment. Confirm.
5. **§9.3 (Immediate termination for cause).** Verify that Florida's implied covenant of good faith doesn't require a cure period for anything listed here.
6. **§10 (Independent Contractor).** Confirm the arrangement won't be reclassified as employment under FL law or the federal DOL 2024 rule (control, opportunity for profit/loss, investment, permanency, integral part of business, skill). At $11/article on a project-by-project basis with no exclusivity and no PGAM control over method, this should hold, but counsel should stress-test.
7. **International variant — §11.2 jurisdiction consent.** Enforceability against a Spain-resident freelancer relies on the Hague Convention on Choice of Court Agreements. Counsel should confirm the clause language is Hague-compliant.
8. **International variant — §11.6 GDPR data-protection nod.** The current language leans on "contract necessity" and "legitimate interest" as lawful bases. If PGAM plans to store EU-contributor personal data for more than a limited administrative window, or transfer it to third-party payment processors, counsel should decide whether Standard Contractual Clauses (SCCs) need to be attached as an addendum.

---

## (c) Suggested workflow

1. **Before the writer's first byline runs**, send the template through **DocuSign** or **HelloSign / Dropbox Sign**. The agreement is drafted to accept e-signature under the US E-SIGN Act (§11.9 US variant / §11.8 international variant).
2. **Do not accept a signed pitch, an assignment email, or a first draft as a substitute** for a signed contract. The demand-letter exposure is entirely upstream of the CMS — if there's no contract on file when the image goes live, PGAM has no indemnity to invoke later.
3. **For each assignment**, send a short written assignment order (Slack, email, or CMS field) noting the topic, deadline, and any per-assignment rate override. This satisfies §2 (Scope of Services) and §3.1 (per-assignment rate flexibility).
4. **On submission**, require the writer to fill in the CMS attribution field for every image with (i) source URL, (ii) license name, (iii) attribution string. This creates the documentary record §5.1(b)(iii) refers to and is the fastest way to shut down a demand letter that arrives later.
5. **On any incoming demand letter**, immediately (i) invoke §5.1(d) to require the writer to produce license documentation within 5 business days, and (ii) put the writer on written notice under §7.1. Keep those written records — they're what makes the indemnity enforceable when the writer inevitably says "I thought it was fine."
6. **Retain signed agreements indefinitely.** §5.2 makes the warranties survive termination indefinitely; §7.5 does the same for indemnification. A signed PDF from 2026 is what protects PGAM from a claim in 2029.
7. **Refresh the template annually**, or whenever counsel flags a change in law (17 U.S.C. amendments, DOL contractor-classification rules, FL statutory changes, GDPR updates for the international variant).

---

## Which template to use

| Contributor is… | Use |
|---|---|
| US resident and US citizen | **US template** |
| US resident, non-citizen (visa holder, green card) | **US template** (they're US-taxed) |
| Non-US resident, non-US citizen (e.g., Milagros in Spain) | **International template** |
| US resident but wants payment to a foreign entity | **International template** + confirm with counsel |

If in doubt, use the international variant — the additional clauses (jurisdiction consent, W-8BEN, OFAC, GDPR) don't hurt a US signer and close ambiguities.

---

## Not legal advice

These templates are a starting point. They have not been reviewed by counsel. Nothing in this README is legal advice. Before deploying with any writer, have a Florida-licensed attorney review the applicable template.
