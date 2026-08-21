import { chromium } from "playwright"
import fs from "node:fs"
import crypto from "node:crypto"
import path from "node:path"

const SECRET = process.env.DEMO_COOKIE_SECRET
if (!SECRET) throw new Error("set DEMO_COOKIE_SECRET (must match the app's .env.local)")
const expiresAt = Date.now() + 7 * 24 * 3600 * 1000
const payload = `v1.${expiresAt}`
const TOKEN = `${payload}.${crypto.createHmac("sha256", SECRET).update(payload).digest("hex")}`
const BASE = process.env.BASE_URL || "http://demo.localhost:3000"
const OUT = process.env.OUT_DIR || "./png"
fs.mkdirSync(OUT, { recursive: true })

const ROUTES = [
  ["/ss-dashboard", "Dashboard", "Core"],
  ["/onboarding", "Onboarding", "Core"],
  ["/ss-campaigns", "Campaigns — list", "Campaigns"],
  ["/ss-campaigns/new", "Campaign builder — new", "Campaigns"],
  ["/ss-campaigns/new-pro", "Campaign builder — pro", "Campaigns"],
  ["/ss-campaigns/quickstart", "Campaign quickstart", "Campaigns"],
  ["/ss-campaigns/url", "Campaign from URL", "Campaigns"],
  ["/ss-campaigns/request", "Campaign request", "Campaigns"],
  ["/ss-campaigns/ss-cmp-001", "Campaign detail", "Campaigns"],
  ["/ss-campaigns/ss-cmp-001/results", "Campaign detail — results", "Campaigns"],
  ["/ss-campaigns/ss-cmp-001/optimize", "Campaign detail — optimize", "Campaigns"],
  ["/ss-creatives", "Creatives — library", "Creatives"],
  ["/ss-creatives/ss-cr-001", "Creative detail", "Creatives"],
  ["/ss-creatives/studio", "Creative studio", "Creatives"],
  ["/ss-creatives/generate", "Creative generate (AI)", "Creatives"],
  ["/ss-creatives/video", "Creative video", "Creatives"],
  ["/ss-creatives/video-ai", "Creative video AI", "Creatives"],
  ["/ss-creatives/compliance", "Creative compliance", "Creatives"],
  ["/ss-results", "Results", "Measurement"],
  ["/ss-reporting", "Reporting", "Measurement"],
  ["/ss-measurement", "Measurement", "Measurement"],
  ["/ss-attention", "Attention", "Measurement"],
  ["/ss-incrementality", "Incrementality", "Measurement"],
  ["/ss-call-attribution", "Call attribution", "Measurement"],
  ["/ss-forecast", "Forecast", "Planning"],
  ["/ss-media-planner", "Media planner", "Planning"],
  ["/ss-audiences", "Audiences", "Planning"],
  ["/ss-marketplace", "Marketplace", "Planning"],
  ["/ss-optimization", "Optimization", "Planning"],
  ["/ss-templates", "Templates", "Planning"],
  ["/ss-grow", "Grow", "Growth"],
  ["/ss-capabilities", "Capabilities (Tools)", "Growth"],
  ["/ss-integrations", "Integrations", "Growth"],
  ["/ss-learn", "Learn", "Growth"],
  ["/ss-help", "Help", "Growth"],
  ["/ss-book-a-call", "Book a call", "Growth"],
  ["/ss-billing", "Billing", "Account"],
  ["/ss-settings", "Settings", "Account"],
  ["/ss-settings/pixels", "Settings — pixels", "Account"],
  ["/ss-settings/guide", "Settings — guide", "Account"],
]

const slug = (p) => p.replace(/^\//, "").replace(/\//g, "__") || "root"

const browser = await chromium.launch({
  executablePath: process.env.CHROMIUM_PATH || undefined,
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
})
const ctx = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 2,
  ignoreHTTPSErrors: true,
})
await ctx.addCookies([
  { name: "pgam_demo_auth", value: TOKEN, domain: "demo.localhost", path: "/" },
  { name: "pgam_demo", value: "1", domain: "demo.localhost", path: "/" },
])
// Freeze animations so shots are deterministic
await ctx.addInitScript(() => {
  const css = "*,*::before,*::after{animation-duration:0s!important;animation-delay:0s!important;transition-duration:0s!important;transition-delay:0s!important}"
  document.addEventListener("DOMContentLoaded", () => {
    const s = document.createElement("style"); s.textContent = css; document.head.appendChild(s)
  })
})

const results = []
for (const [route, title, section] of ROUTES) {
  const page = await ctx.newPage()
  const errors = []
  page.on("pageerror", (e) => errors.push(String(e.message).slice(0, 200)))
  let status = null, finalUrl = null, file = null, note = ""
  try {
    const resp = await page.goto(BASE + route, { waitUntil: "domcontentloaded", timeout: 45000 })
    status = resp ? resp.status() : null
    try { await page.waitForLoadState("networkidle", { timeout: 12000 }) } catch {}
    await page.waitForTimeout(2500)
    // scroll through to trigger lazy/in-view content, then back to top
    await page.evaluate(async () => {
      const h = document.body.scrollHeight
      for (let y = 0; y < h; y += 600) { window.scrollTo(0, y); await new Promise(r => setTimeout(r, 60)) }
      window.scrollTo(0, 0)
    })
    await page.waitForTimeout(1200)
    await page.addStyleTag({ content: "nextjs-portal{display:none!important}" }).catch(()=>{})
    const bodyText = await page.evaluate(() => document.body.innerText || "")
    if (/not found|don't have access|Something went wrong/i.test(bodyText.slice(0, 800))) note = (note ? note + "; " : "") + "EMPTY/NOT-FOUND STATE"
    if (bodyText.trim().length < 400) note = (note ? note + "; " : "") + "very little text rendered"
    finalUrl = page.url()
    if (new URL(finalUrl).pathname !== route) note = `redirected to ${new URL(finalUrl).pathname}`
    file = path.join(OUT, `${slug(route)}.png`)
    await page.screenshot({ path: file, fullPage: true })
  } catch (e) {
    note = `CAPTURE ERROR: ${String(e.message).slice(0, 160)}`
  }
  const size = file && fs.existsSync(file) ? fs.statSync(file).size : 0
  results.push({ route, title, section, status, finalUrl, file, note, errors: errors.slice(0, 3), size })
  console.log(`${status ?? "---"}  ${route.padEnd(34)} ${(size/1024).toFixed(0).padStart(6)}KB  ${note}`)
  await page.close()
}

fs.writeFileSync(path.join(OUT, "..", "results.json"), JSON.stringify(results, null, 2))
await browser.close()
console.log(`\nDONE ${results.filter(r => r.size > 0).length}/${ROUTES.length} captured`)
