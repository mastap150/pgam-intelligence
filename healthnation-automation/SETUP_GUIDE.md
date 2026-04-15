# HealthNation — Complete Setup & Deployment Guide
## WordPress + Theme + Automation System

---

## OVERVIEW: WHAT WE'RE BUILDING

```
[InfinityFree / Custom Host]          [Your Mac / Any Server]
        │                                       │
  WordPress + HealthNation Theme    ◄──────  Python Automation
  (PHP, MySQL, REST API)                    (Claude API + Unsplash)
        │                                       │
  healthnation.com                    Runs daily via cron
  (your domain)                       Publishes 2 articles/day
```

---

## STEP 1 — GET FREE HOSTING (InfinityFree)

InfinityFree gives you free PHP + MySQL hosting that fully supports WordPress.

1. Go to **infinityfree.com** → click **Sign Up Free**
2. Create an account with your email
3. Click **Create Account** → choose any subdomain (e.g. `healthnation`) or skip — you'll connect your real domain in Step 3
4. Note your **cPanel URL**, **username**, and **password**

> **Alternative: already have hosting?** Skip to Step 2 — just use your existing PHP/MySQL host.

> **Vercel option:** Vercel doesn't run PHP natively. For Vercel, the cleanest path is a **headless WordPress** setup: WordPress backend on InfinityFree (API only), Next.js frontend on Vercel. This is more complex — get the basic WordPress running first, then we can migrate the frontend to Vercel.

---

## STEP 2 — INSTALL WORDPRESS

### On InfinityFree (1-click):
1. Log in to InfinityFree → **Control Panel**
2. Find **Softaculous** or **WordPress** installer
3. Click **Install WordPress**
4. Fill in:
   - **Site name:** HealthNation
   - **Site description:** Evidence-based health guidance
   - **Admin username:** (choose something other than "admin")
   - **Admin password:** (strong password — save it)
   - **Admin email:** your email
5. Click **Install** — takes ~2 minutes
6. Note your WordPress admin URL: `http://yoursite.infinityfree.net/wp-admin`

### On any cPanel host:
Same process — look for **Softaculous** in cPanel → WordPress → Install.

### Manual install (if no Softaculous):
1. Download WordPress from wordpress.org
2. Upload via FTP to `public_html/`
3. Create a MySQL database in cPanel
4. Visit your domain → follow the 5-minute WordPress setup

---

## STEP 3 — CONNECT YOUR DOMAIN (healthnation.com)

1. Log in to wherever you bought **healthnation.com** (GoDaddy, Namecheap, etc.)
2. Find **DNS Settings** or **Nameservers**
3. Change nameservers to your host's nameservers (InfinityFree provides these in your account)
4. Or: Add an **A Record** pointing `@` to your host's IP address
5. DNS propagates in 1–24 hours

---

## STEP 4 — INSTALL THE HEALTHNATION THEME

1. **Zip the theme folder:**
   - On your Mac: right-click the `healthnation-wordpress-theme/` folder → Compress
   - This creates `healthnation-wordpress-theme.zip`

2. **Upload to WordPress:**
   - WordPress Admin → **Appearance** → **Themes** → **Add New** → **Upload Theme**
   - Select `healthnation-wordpress-theme.zip` → click **Install Now**
   - Click **Activate**

3. **Install required plugins:**
   Go to **Plugins** → **Add New** and install:
   - **Rank Math SEO** (free) — handles meta titles, schema, sitemaps
   - **WP Super Cache** (free) — page caching for performance
   - **Smush** (free) — image compression
   - **Classic Editor** (optional) — if you prefer classic editor

4. **Create categories:** Go to **Posts** → **Categories** and create:
   - nutrition
   - fitness
   - mental-health
   - longevity
   - sleep
   - conditions

5. **Set up navigation:** Go to **Appearance** → **Menus** → create a menu with your categories → assign to **Primary Navigation**

6. **Set homepage:** Go to **Settings** → **Reading** → select **A static page** → set Front page to your homepage (create a page called "Home" first if needed, or leave blank — the `front-page.php` template loads automatically)

---

## STEP 5 — GET YOUR API KEYS

### A. Claude API Key (Anthropic)
1. Go to **console.anthropic.com**
2. Sign in → **API Keys** → **Create Key**
3. Copy and save securely — you won't see it again

### B. WordPress Application Password
1. WordPress Admin → **Users** → **Profile**
2. Scroll to **Application Passwords**
3. Name: `HealthNation Automation`
4. Click **Add New Application Password**
5. Copy the password (format: `xxxx xxxx xxxx xxxx xxxx xxxx`)

### C. Unsplash API Key (Free)
1. Go to **unsplash.com/developers**
2. Click **Register as a developer** → **Your apps** → **New Application**
3. App name: `HealthNation`
4. Copy your **Access Key**
5. Free tier: 50 requests/hour (plenty for 2 articles/day)

---

## STEP 6 — CONFIGURE THE AUTOMATION

1. **Navigate to the automation folder:**
   ```bash
   cd /Users/priyeshpatel/Desktop/pgam-intelligence/healthnation-automation
   ```

2. **Create your .env file:**
   ```bash
   cat > .env << 'EOF'
   ANTHROPIC_API_KEY=your_claude_api_key_here
   WP_SITE_URL=https://healthnation.com
   WP_USERNAME=your_wordpress_admin_username
   WP_APP_PASS=xxxx xxxx xxxx xxxx xxxx xxxx
   UNSPLASH_ACCESS_KEY=your_unsplash_access_key_here
   EOF
   ```

3. **Set up Python environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate          # Mac/Linux
   # venv\Scripts\activate           # Windows
   pip install -r requirements.txt
   ```

4. **Test the connection:**
   ```bash
   python -c "
   import config; config.validate()
   from wordpress_publisher import init; init()
   print('✓ All connections working')
   "
   ```

---

## STEP 7 — POPULATE YOUR SITE (50 ARTICLES)

Run the bulk populator to seed your site with all 50 articles:

```bash
# First, do a dry run to check everything works
python bulk_populate.py --dry-run --limit 3

# If dry run looks good, publish priority-1 articles first (top 10)
python bulk_populate.py --priority 1

# Then run all remaining articles (takes ~45-60 minutes)
python bulk_populate.py --resume
```

**What this does:**
- Generates each article with Claude API (~30 seconds each)
- Fetches a matching Unsplash photo
- Uploads photo to WordPress Media Library
- Publishes article with all SEO meta, reviewer data, key takeaways
- Saves progress so you can resume if interrupted

**Cost estimate:** 50 articles × ~$1.50 avg Claude cost = **~$75 total**

---

## STEP 8 — SET UP DAILY AUTOMATION

### Option A: Mac cron (simplest — runs while your Mac is on)

```bash
# Open crontab editor
crontab -e

# Add this line (publishes 2 articles at 8:00 AM daily):
0 8 * * * /Users/priyeshpatel/Desktop/pgam-intelligence/healthnation-automation/venv/bin/python /Users/priyeshpatel/Desktop/pgam-intelligence/healthnation-automation/daily_runner.py >> /Users/priyeshpatel/Desktop/pgam-intelligence/healthnation-automation/logs/cron.log 2>&1
```

### Option B: Run as a background process (Mac stays on)
```bash
# Run in background, schedules itself at 8 AM daily
python daily_runner.py --schedule --time "08:00" &
```

### Option C: DigitalOcean / any Linux server ($4/month)
```bash
# On the server, same crontab setup:
crontab -e
# Add:
0 8 * * * /path/to/venv/bin/python /path/to/daily_runner.py >> /path/to/logs/cron.log 2>&1
```

### Option D: GitHub Actions (free, cloud-based)
Create `.github/workflows/daily_publish.yml`:
```yaml
name: Daily Article Publisher
on:
  schedule:
    - cron: '0 8 * * *'   # 8:00 AM UTC daily
  workflow_dispatch:        # Also allows manual trigger

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r healthnation-automation/requirements.txt
      - run: python healthnation-automation/daily_runner.py
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          WP_SITE_URL:       ${{ secrets.WP_SITE_URL }}
          WP_USERNAME:       ${{ secrets.WP_USERNAME }}
          WP_APP_PASS:       ${{ secrets.WP_APP_PASS }}
          UNSPLASH_ACCESS_KEY: ${{ secrets.UNSPLASH_ACCESS_KEY }}
```
Add your secrets in GitHub repo → Settings → Secrets. **This is the cleanest free cloud option.**

---

## STEP 9 — VERCEL FRONTEND (Optional Upgrade)

If you want the Next.js/Vercel frontend for better performance:

1. WordPress stays as a **headless CMS** on your host (no theme needed — just the REST API)
2. We build a Next.js app that fetches from `healthnation.com/wp-json/wp/v2/posts`
3. Deploy the Next.js app to Vercel (free tier)
4. Point `healthnation.com` DNS to Vercel

This gives you: WordPress content management + Vercel's CDN edge performance + free hosting.

> Let me know when you want to do this step — it's a separate build.

---

## MONITORING & MAINTENANCE

### Check automation is running:
```bash
tail -f /Users/priyeshpatel/Desktop/pgam-intelligence/healthnation-automation/logs/daily_runner.log
```

### Check published articles count:
```bash
python -c "
import json
from pathlib import Path
d = json.load(open(Path('healthnation-automation/published.json')))
print(f'Published: {len(d[\"published\"])} articles')
"
```

### Add new topics to the queue:
Edit `topics_queue.json` — append new topic objects. The daily runner will pick them up automatically.

### Update a published article:
Use WordPress admin → Posts → find and edit directly, or add an `update_article.py` script later.

---

## FILE STRUCTURE SUMMARY

```
healthnation-wordpress-theme/   ← Upload this to WordPress
  style.css                     ← Theme declaration + all CSS
  functions.php                 ← Theme setup, REST API, helpers
  header.php                    ← Site header
  footer.php                    ← Site footer + mobile nav
  front-page.php                ← Homepage template
  single.php                    ← Article template
  index.php                     ← Archive/blog template
  assets/js/main.js             ← JavaScript (filters, TOC, newsletter)

healthnation-automation/        ← Run this locally or on a server
  .env                          ← YOUR SECRETS (never commit this)
  config.py                     ← Configuration loader
  generate_article.py           ← Claude API article generator
  unsplash_image.py             ← Unsplash fetch + WP image upload
  wordpress_publisher.py        ← WordPress REST API publisher
  bulk_populate.py              ← One-time 50-article seeder
  daily_runner.py               ← Daily cron automation
  topics_queue.json             ← 50 article topics (your queue)
  published.json                ← Auto-created: tracks what's published
  requirements.txt              ← Python dependencies
  logs/                         ← Auto-created: run logs
```

---

## COST SUMMARY

| Item | Cost |
|------|------|
| InfinityFree hosting | Free |
| healthnation.com domain | Already owned |
| WordPress | Free |
| HealthNation theme | Free (built above) |
| Unsplash API | Free (50 req/hour) |
| Initial 50 articles (Claude API) | ~$75 one-time |
| Daily 2 articles (Claude API) | ~$3/day = ~$90/month |
| Rank Math SEO plugin | Free |
| GitHub Actions automation | Free |
| **Total monthly ongoing** | **~$90/month** |

At 50K+ sessions/month with Mediavine ads at $25 RPM = **$1,250/month revenue**. ROI positive.

---

## QUESTIONS / NEXT STEPS

Once you've completed Steps 1–8:
1. Share the WordPress URL and I'll verify the theme is rendering correctly
2. We can set up Rank Math SEO configuration
3. We can build the Vercel/Next.js frontend upgrade
4. We can add destination.com with the same system
