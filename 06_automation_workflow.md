# AUTOMATED CONTENT SYSTEM — BOTH SITES
## Daily Article Generation, Image Creation & Publishing Workflow

---

## SYSTEM OVERVIEW

```
┌─────────────────────────────────────────────────────────────────┐
│                 DAILY CONTENT PIPELINE                          │
│                                                                 │
│  [Keyword Queue] → [AI Writer] → [AI Image] → [Review Gate]    │
│       ↓                ↓             ↓             ↓           │
│  Airtable DB      Claude API    Midjourney    Human Editor      │
│                        ↓             ↓             ↓           │
│                   [Draft Article] [Hero Image] [Approved]       │
│                                              ↓                  │
│                                    [Auto-Publish to CMS]        │
│                                    WordPress / Webflow          │
└─────────────────────────────────────────────────────────────────┘
```

**Daily throughput target:** 2 articles/day per site (14/week, ~60/month)
**Human time required:** 20–30 minutes/day review + light editing
**Automation platform:** n8n (self-hosted) or Make.com (cloud)

---

## TOOL STACK

| Function | Tool | Cost (approx) | Notes |
|----------|------|---------------|-------|
| Keyword tracking | Airtable | Free–$20/mo | Central database for all article data |
| AI writing | Claude API (claude-sonnet-4-6) | ~$0.50–$2.00/article | Best quality-to-cost for long-form |
| AI images | Midjourney (v6) | $30/mo | Photorealistic travel/health images |
| Image backup | DALL-E 3 via OpenAI API | ~$0.04/image | Use if Midjourney is unavailable |
| Automation | n8n (self-hosted) | ~$5/mo VPS | Full control, no per-task fees |
| Automation alt | Make.com | $9–$29/mo | Easier setup, operation limits apply |
| CMS | WordPress (REST API) | Existing hosting | Most flexible for SEO plugins |
| SEO plugin | Rank Math or Yoast | Free–$59/yr | Auto-injects schema, sitemaps |
| Image hosting | Cloudinary | Free–$89/mo | Auto-compress, CDN delivery |
| Review queue | Notion or Airtable | Free | Editorial calendar + status tracking |

---

## PHASE 1: SETUP (One-Time)

### Step 1: Build the Keyword Database (Airtable)
Create an Airtable base with these fields per article:

```
Fields:
- Article ID (auto)
- Site (destination.com / healthnation.com)
- Target Keyword (text)
- Secondary Keywords (text, comma-separated)
- Search Intent (select: Informational / Navigational / Transactional)
- Category (select)
- Suggested Title (text)
- Status (select: Queued / In Progress / Review / Approved / Published)
- Priority (1–3)
- Publish Date (date)
- WordPress Post ID (number, filled after publish)
- Author (text)
- Reviewer (text — for healthnation.com)
- Word Count Target (number)
- Notes (long text)
```

**Load your 100 article ideas** (50 per site from files 04 and 05) into Airtable as the initial queue.

### Step 2: Configure Your CMS
- Install WordPress REST API authentication (Application Passwords)
- Install Rank Math SEO plugin
- Create category taxonomy matching your site structure
- Set up featured image field in post schema
- Configure Cloudinary plugin for auto image optimization

### Step 3: Set Up n8n (or Make.com)
- Deploy n8n on a $5/mo VPS (DigitalOcean, Hetzner) or use n8n Cloud ($20/mo)
- Connect credentials: Airtable API, Anthropic API, WordPress REST API, Cloudinary, Midjourney (via unofficial API or proxy)

---

## PHASE 2: DAILY AUTOMATION WORKFLOW

### The n8n Flow — Node by Node

```
NODE 1: CRON TRIGGER
────────────────────
Schedule: Daily at 6:00 AM
Action: Fire workflow

NODE 2: FETCH TODAY'S ARTICLE FROM AIRTABLE
─────────────────────────────────────────────
Query: Status = "Queued", Priority = highest, limit 2
Output: Article record with keyword, title, category, site

NODE 3: GENERATE ARTICLE (Claude API)
──────────────────────────────────────
Input: Keyword, title, category, site, word count target
Model: claude-sonnet-4-6
System prompt: [See prompt templates below]
Output: Full markdown article (1,500–2,500 words)

NODE 4: PARSE AND STRUCTURE ARTICLE
────────────────────────────────────
Extract:
  - H1 (title)
  - Meta description (from intro or generate separately)
  - Article body (HTML)
  - FAQ section (for schema markup)
  - Key takeaways

NODE 5: GENERATE IMAGE PROMPT
──────────────────────────────
Input: Article title + category + site
Model: claude-haiku-4-5 (fast, cheap)
Output: Optimized image generation prompt

NODE 6: GENERATE HERO IMAGE
────────────────────────────
Input: Image prompt from Node 5
Service: Midjourney API / DALL-E 3 API
Parameters:
  - destination.com: photorealistic, editorial travel photography style
  - healthnation.com: clean, bright, medical-adjacent lifestyle photography
Output: Image URL

NODE 7: UPLOAD IMAGE TO CLOUDINARY
────────────────────────────────────
Input: Image URL
Output: Cloudinary CDN URL + optimized versions (WebP, AVIF)
Alt text: Auto-generated from article title

NODE 8: UPDATE AIRTABLE STATUS
────────────────────────────────
Status: "Queued" → "Review"
Add fields: Draft content, image URL, meta description

NODE 9: SEND TO REVIEW QUEUE
──────────────────────────────
Action: Email notification to editor with:
  - Article title
  - Link to Airtable record with draft
  - Direct link to approve/reject (webhook trigger)
  - Preview of first 300 words

[HUMAN REVIEW — 10–15 MINUTES]
────────────────────────────────
Editor reads draft, makes light edits in Airtable
Clicks "Approve" button (triggers webhook)
For healthnation.com: Medical reviewer checks claims

NODE 10: WEBHOOK TRIGGER (On Approval)
─────────────────────────────────────────
Condition: Status changed to "Approved" in Airtable
Fires: Publishing workflow

NODE 11: PUBLISH TO WORDPRESS
───────────────────────────────
WordPress REST API call:
  POST /wp-json/wp/v2/posts
  {
    title: H1,
    content: article HTML,
    status: "publish",
    categories: [mapped category ID],
    featured_media: [Cloudinary image ID],
    meta: {
      rank_math_focus_keyword: target_keyword,
      rank_math_description: meta_description,
      rank_math_title: seo_title
    }
  }

NODE 12: UPDATE AIRTABLE RECORD
──────────────────────────────────
Status: "Approved" → "Published"
Add: WordPress Post ID, Published URL, Published Date

NODE 13: DISTRIBUTE (Optional but recommended)
────────────────────────────────────────────────
Auto-post excerpt to:
  - Twitter/X API
  - Pinterest API (travel/health images perform well)
  - Newsletter queue (for weekly digest)
```

---

## AI WRITING PROMPT TEMPLATES

### destination.com — Destination Guide Prompt

```
SYSTEM:
You are a senior travel writer for a premium editorial travel site. You write in an authoritative, first-person-informed style — honest, detailed, and free of travel cliché. You have personally visited the places you write about (write as if you have). You never pad articles, never use filler phrases, and cite specific details that demonstrate on-the-ground knowledge.

USER:
Write a comprehensive travel guide article with the following parameters:

Target keyword: [KEYWORD]
Article title: [TITLE]
Category: [CATEGORY]
Target word count: [WORD_COUNT]
Secondary keywords to incorporate naturally: [SECONDARY_KEYWORDS]

Article requirements:
1. H1: Use the exact title provided
2. Include a compelling 2-sentence introduction before the first H2
3. Use the following H2 structure (adapt as needed):
   - Why Visit [Destination] Right Now
   - Best Time to Go
   - [Destination]-Specific Section 1
   - [Destination]-Specific Section 2
   - Where to Stay
   - Getting There and Around
   - Budget Breakdown
   - [FAQ with 4 questions]
4. Include 3 internal link placeholders formatted as: [INTERNAL LINK: topic]
5. Include 1 Points & Miles callout box formatted as: [MILES BOX: relevant airline/hotel program]
6. End with a "Bottom Line" section (2 sentences)
7. Write in markdown format
8. Avoid: "nestled", "vibrant", "hidden gem" (use "lesser-known"), "boasts", "breathtaking"
9. Target keyword must appear in: H1, first paragraph, one H2, meta description

Output format:
---
META_TITLE: [SEO title under 60 characters]
META_DESC: [Description 145-155 characters]
---
[ARTICLE IN MARKDOWN]
```

---

### healthnation.com — Health Article Prompt

```
SYSTEM:
You are a senior health and science writer for an evidence-based health publication. You have deep familiarity with nutritional science, exercise physiology, and medical literature. Every claim you make is backed by published research. You write in plain, direct English — no wellness jargon, no pseudoscience, no absolute health claims. You cite studies by author name and journal where possible. You include a medical review disclaimer.

USER:
Write a science-based health article with the following parameters:

Target keyword: [KEYWORD]
Article title: [TITLE]  
Category: [CATEGORY]
Target word count: [WORD_COUNT]
Secondary keywords: [SECONDARY_KEYWORDS]

Article requirements:
1. H1: Use the exact title
2. Add this block after H1:
   "Reviewed by: [REVIEWER PLACEHOLDER] · Last updated: [CURRENT_DATE] · [X] min read"
3. Add a "Key Takeaways" box with 4 bullet points immediately after the intro
4. Use this H2 structure:
   - What [Topic] Actually Means
   - What the Research Says
   - How to Apply This Practically
   - Common Mistakes
   - Expert Recommendations
   - FAQ (4 questions)
   - The Bottom Line
5. Include a "Table of Contents" with anchor links
6. Cite at least 3 research studies (use real study patterns: "A 2022 study published in [Journal] found...")
7. Include 3 internal link placeholders: [INTERNAL LINK: topic]
8. Include 1 "Related Tools" callout: [TOOL LINK: calculator/tool name]
9. End with: "This article is for informational purposes only. Consult a healthcare provider before making health decisions."
10. Write in markdown format
11. Never use: "superfoods", "toxins", "cleanse", "boost your immune system", "miracle", "cure"

Output format:
---
META_TITLE: [SEO title under 60 characters]
META_DESC: [Description 145-155 characters]
REVIEWER: [Recommended reviewer specialty]
---
[ARTICLE IN MARKDOWN]
```

---

### Image Generation Prompt Template

```
SYSTEM:
Given an article title and site, generate an image prompt for a high-quality editorial photograph.

For destination.com:
- Style: Cinematic travel photography, golden hour lighting, authentic local scene
- No text overlays, no collages
- Format: Horizontal (16:9), photorealistic

For healthnation.com:
- Style: Clean lifestyle photography, natural light, minimal props
- Medical-adjacent but not clinical
- Format: Horizontal (16:9), photorealistic

USER:
Site: [SITE]
Article title: [TITLE]
Category: [CATEGORY]

Generate a Midjourney prompt for the hero image.

Example output (destination.com):
"Cinematic wide-angle photograph of the Serengeti plains at golden hour, vast savanna with acacia trees and distant herd of wildebeest, warm amber light, photojournalism style, no people, sharp foreground grass, dreamy background, National Geographic quality --ar 16:9 --v 6"

Example output (healthnation.com):
"Clean lifestyle photography of a woman in her 30s preparing a colorful vegetable bowl in a bright modern kitchen, natural window light, neutral tones, no text, editorial magazine style, shallow depth of field --ar 16:9 --v 6"
```

---

## QUALITY GATES

### Automatic Checks (Pre-Review)
- Word count within ±10% of target
- Target keyword present in H1
- Meta description 145–160 characters
- Meta title under 65 characters
- Minimum 3 H2 sections present
- No instances of banned phrases (regex check)

### Human Review Checklist (10–15 min)
- [ ] Factual claims are accurate (spot-check 2–3)
- [ ] Internal link placeholders filled with real URLs
- [ ] Image is relevant and not AI-uncanny
- [ ] Tone matches site voice
- [ ] For healthnation.com: No absolute medical claims
- [ ] CTA placement looks natural

### healthnation.com Additional Gate
- Medical reviewer approval required before publish
- Set up a shared Notion or Google Doc for reviewer comments
- SLA: reviewer approves within 24 hours or article queued to next day

---

## EDITORIAL CALENDAR SETUP

### Weekly Cadence

| Day | Action |
|-----|--------|
| Monday | Run automation for 2 destination.com articles |
| Tuesday | Run automation for 2 healthnation.com articles |
| Wednesday | Run automation for 2 destination.com articles |
| Thursday | Run automation for 2 healthnation.com articles |
| Friday | Run automation for 2 destination.com articles |
| Saturday | Review queue catch-up, internal linking pass |
| Sunday | Keyword research, queue next week's articles in Airtable |

**Output: ~10 articles/week across both sites (~520/year)**

---

## SCALING PATH

| Phase | Articles/day | Human time/day | Monthly cost |
|-------|-------------|----------------|-------------|
| Launch | 2 total | 30 min | ~$80 |
| Growth | 4 total | 45 min | ~$160 |
| Scale | 6–8 total | 60 min + 1 part-time editor | ~$350 |

At scale, consider:
- Second AI writer for first drafts (GPT-4o as backup)
- Freelance editor @ 30 min/article at $15–25/hr
- Automated internal link insertion tool (Link Whisper plugin for WordPress)
- Automated social distribution to Pinterest, Twitter, newsletter

---

## COST ESTIMATE (LAUNCH PHASE)

| Item | Monthly Cost |
|------|-------------|
| Claude API (60 articles × $1.50 avg) | $90 |
| Midjourney | $30 |
| n8n VPS | $5 |
| Airtable | Free (under 1,000 records) |
| Cloudinary | Free (under 25GB) |
| WordPress hosting | $20–$50 (existing) |
| **Total** | **~$145–175/month** |

At 10 articles/week, cost per article ≈ $3.50–$4.50.
