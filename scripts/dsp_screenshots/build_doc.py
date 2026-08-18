import json, base64, os, html
from collections import OrderedDict

R = json.load(open("./results.json"))
def b64(p):
    with open(p, "rb") as f: return base64.b64encode(f.read()).decode()

SECTIONS = OrderedDict()
for r in R:
    SECTIONS.setdefault(r["section"], []).append(r)

SECTION_NOTE = {
    "Core": "Entry points. Everything below is reached from here.",
    "Campaigns": "Five separate creation paths converge on one campaign object — the biggest redundancy in the product.",
    "Creatives": "Asset library plus four generation/QA tools that each own a full page.",
    "Measurement": "Six reporting surfaces with heavy overlap in what they show.",
    "Planning": "Pre-flight tools: forecasting, audience selection, inventory.",
    "Growth": "Education, integrations and sales-assist surfaces.",
    "Account": "Settings, billing and pixel setup.",
}

def slug(s): return s.lower().replace(" ", "-").replace("—","-").replace("(","").replace(")","")

nav, contact, body = [], [], []
n = 0
for sec, items in SECTIONS.items():
    nav.append(f'<div class="nav-sec"><span class="nav-sec-t">{html.escape(sec)}</span><span class="nav-sec-n">{len(items)}</span></div><ul class="nav-list">')
    for r in items:
        n += 1
        sid = f"s{n}"
        flag = " ⚑" if r.get("note") else ""
        nav.append(f'<li><a href="#{sid}"><span class="nav-i">{n:02d}</span>{html.escape(r["title"])}{flag}</a></li>')
        contact.append(
            f'<a class="cs" href="#{sid}"><span class="cs-img"><img loading="lazy" src="data:image/jpeg;base64,{b64(r["thumb"])}" alt="{html.escape(r["title"])}"></span>'
            f'<span class="cs-m"><span class="cs-i">{n:02d}</span><span class="cs-t">{html.escape(r["title"])}</span></span></a>')
        px = r.get("px", [0,0])
        note = f'<p class="flag">⚑ {html.escape(r["note"])}</p>' if r.get("note") else ""
        body.append(f"""<article class="screen" id="{sid}">
  <header class="s-head">
    <div class="s-id"><span class="s-num">{n:02d}</span><span class="s-sec">{html.escape(sec)}</span></div>
    <h3>{html.escape(r["title"])}</h3>
    <p class="route"><code>{html.escape(r["route"])}</code></p>
    <p class="dims">{px[0]//2} × {px[1]//2} css px · full page</p>
    {note}
  </header>
  <div class="shot"><img loading="lazy" src="data:image/jpeg;base64,{b64(r["web"])}" alt="{html.escape(r["title"])} screen"></div>
</article>""")
    nav.append("</ul>")

sec_heads = {}
cur = None
out_body = []
i = 0
for sec, items in SECTIONS.items():
    out_body.append(f'<section class="sec" id="sec-{slug(sec)}"><div class="sec-head"><h2>{html.escape(sec)}</h2>'
                    f'<p>{html.escape(SECTION_NOTE.get(sec,""))}</p><span class="sec-count">{len(items)} screens</span></div>')
    for _ in items:
        out_body.append(body[i]); i += 1
    out_body.append("</section>")

flagged = [r for r in R if r.get("note")]

HTML = f"""<title>Attune Screen Inventory</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Archivo:wght@600;700&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap">
<style>
:root{{
  --ground:#FBFBFD; --surface:#FFFFFF; --surface-2:#F4F5F9;
  --ink:#12161F; --ink-2:#5B6478; --ink-3:#8B93A5;
  --line:#E4E7EE; --line-2:#EFF1F6;
  --accent:#2B4FD8; --accent-soft:#EAEEFC;
  --flag:#B54708; --flag-soft:#FDF3EA;
  --shadow:0 1px 2px rgba(18,22,31,.05),0 8px 24px -12px rgba(18,22,31,.12);
}}
@media (prefers-color-scheme:dark){{
  :root:not([data-theme="light"]){{
    --ground:#0E1116; --surface:#161B24; --surface-2:#1D2430;
    --ink:#EEF1F6; --ink-2:#9AA4B6; --ink-3:#6D7789;
    --line:#262E3B; --line-2:#1E2532;
    --accent:#7C97F5; --accent-soft:#1B2440;
    --flag:#E8A365; --flag-soft:#2A1F14;
    --shadow:0 1px 2px rgba(0,0,0,.4),0 8px 24px -12px rgba(0,0,0,.6);
  }}
}}
:root[data-theme="dark"]{{
  --ground:#0E1116; --surface:#161B24; --surface-2:#1D2430;
  --ink:#EEF1F6; --ink-2:#9AA4B6; --ink-3:#6D7789;
  --line:#262E3B; --line-2:#1E2532;
  --accent:#7C97F5; --accent-soft:#1B2440;
  --flag:#E8A365; --flag-soft:#2A1F14;
  --shadow:0 1px 2px rgba(0,0,0,.4),0 8px 24px -12px rgba(0,0,0,.6);
}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--ground);color:var(--ink);
  font-family:"IBM Plex Sans",ui-sans-serif,system-ui,sans-serif;font-size:15px;line-height:1.6;
  -webkit-font-smoothing:antialiased}}
h1,h2,h3{{font-family:Archivo,"IBM Plex Sans",sans-serif;font-weight:700;letter-spacing:-.02em;text-wrap:balance;margin:0}}
code{{font-family:"IBM Plex Mono",ui-monospace,monospace}}
a{{color:inherit}}
:focus-visible{{outline:2px solid var(--accent);outline-offset:2px;border-radius:3px}}

.wrap{{display:grid;grid-template-columns:250px minmax(0,1fr);gap:48px;
  max-width:1360px;margin:0 auto;padding:0 32px 96px}}
@media(max-width:1000px){{.wrap{{grid-template-columns:1fr;gap:0;padding:0 20px 64px}} .rail{{display:none}}}}

/* ---- masthead ---- */
.mast{{border-bottom:1px solid var(--line);background:var(--surface);margin-bottom:48px}}
.mast-in{{max-width:1360px;margin:0 auto;padding:56px 32px 44px}}
.eyebrow{{font-size:11px;font-weight:600;letter-spacing:.14em;text-transform:uppercase;color:var(--accent);margin:0 0 14px}}
.mast h1{{font-size:clamp(34px,5vw,54px);line-height:1.04;max-width:16ch}}
.lede{{color:var(--ink-2);max-width:62ch;margin:18px 0 0;font-size:16.5px}}
.facts{{display:flex;flex-wrap:wrap;gap:0;margin:36px 0 0;border-top:1px solid var(--line-2);padding-top:20px}}
.fact{{padding-right:40px;margin-right:40px;border-right:1px solid var(--line-2)}}
.fact:last-child{{border-right:0;margin-right:0;padding-right:0}}
.fact b{{display:block;font-family:Archivo,sans-serif;font-size:26px;font-weight:700;
  font-variant-numeric:tabular-nums;line-height:1.1}}
.fact span{{font-size:12px;color:var(--ink-3);letter-spacing:.04em}}

/* ---- rail ---- */
.rail{{position:sticky;top:0;align-self:start;max-height:100vh;overflow-y:auto;
  padding:8px 0 40px;font-size:13.5px}}
.nav-sec{{display:flex;justify-content:space-between;align-items:baseline;
  margin:24px 0 8px;padding-bottom:6px;border-bottom:1px solid var(--line)}}
.nav-sec-t{{font-size:11px;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:var(--ink)}}
.nav-sec-n{{font-size:11px;color:var(--ink-3);font-variant-numeric:tabular-nums}}
.nav-list{{list-style:none;margin:0;padding:0}}
.nav-list a{{display:flex;gap:9px;text-decoration:none;color:var(--ink-2);padding:3.5px 0;line-height:1.35}}
.nav-list a:hover{{color:var(--accent)}}
.nav-i{{color:var(--ink-3);font-family:"IBM Plex Mono",monospace;font-size:11px;
  font-variant-numeric:tabular-nums;padding-top:2px;flex:none}}

/* ---- contact sheet ---- */
.cs-wrap{{margin:0 0 72px}}
.block-h{{display:flex;align-items:baseline;gap:14px;margin:0 0 20px;
  padding-bottom:12px;border-bottom:1px solid var(--line)}}
.block-h h2{{font-size:19px}}
.block-h p{{margin:0;color:var(--ink-3);font-size:13px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:18px}}
.cs{{text-decoration:none;display:block}}
.cs-img{{display:block;border:1px solid var(--line);border-radius:7px;overflow:hidden;
  background:var(--surface);box-shadow:var(--shadow);transition:border-color .15s,transform .15s}}
.cs:hover .cs-img{{border-color:var(--accent);transform:translateY(-2px)}}
.cs-img img{{display:block;width:100%;height:auto}}
.cs-m{{display:flex;gap:7px;margin-top:8px;align-items:baseline}}
.cs-i{{font-family:"IBM Plex Mono",monospace;font-size:10.5px;color:var(--ink-3);flex:none}}
.cs-t{{font-size:12.5px;color:var(--ink-2);line-height:1.3}}

/* ---- sections ---- */
.sec{{margin:0 0 20px}}
.sec-head{{position:relative;margin:64px 0 28px;padding:0 0 14px;border-bottom:2px solid var(--ink)}}
.sec-head h2{{font-size:27px}}
.sec-head p{{margin:6px 0 0;color:var(--ink-2);max-width:60ch;font-size:14px}}
.sec-count{{position:absolute;right:0;top:4px;font-size:11px;color:var(--ink-3);
  letter-spacing:.08em;text-transform:uppercase}}

.screen{{margin:0 0 52px;scroll-margin-top:16px}}
.s-head{{display:grid;gap:2px;margin:0 0 14px}}
.s-id{{display:flex;align-items:center;gap:10px;margin-bottom:3px}}
.s-num{{font-family:"IBM Plex Mono",monospace;font-size:12px;color:var(--accent);font-weight:500}}
.s-sec{{font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--ink-3)}}
.s-head h3{{font-size:20px}}
.route code{{font-size:12.5px;color:var(--ink-2);background:var(--surface-2);
  border:1px solid var(--line-2);padding:2px 7px;border-radius:4px}}
.route{{margin:5px 0 0}}
.dims{{margin:6px 0 0;font-size:11.5px;color:var(--ink-3);font-variant-numeric:tabular-nums}}
.flag{{margin:9px 0 0;font-size:12.5px;color:var(--flag);background:var(--flag-soft);
  border:1px solid color-mix(in srgb,var(--flag) 25%,transparent);
  padding:6px 11px;border-radius:5px;display:inline-block}}
.shot{{border:1px solid var(--line);border-radius:9px;overflow:hidden;
  background:var(--surface);box-shadow:var(--shadow)}}
.shot img{{display:block;width:100%;height:auto}}

.note-box{{background:var(--surface);border:1px solid var(--line);border-radius:9px;
  padding:22px 24px;margin:0 0 60px}}
.note-box h2{{font-size:16px;margin-bottom:10px}}
.note-box ul{{margin:0;padding-left:19px;color:var(--ink-2);font-size:14px}}
.note-box li{{margin:5px 0}}
.note-box strong{{color:var(--ink)}}
@media (prefers-reduced-motion:reduce){{*{{transition:none!important}}}}
</style>

<header class="mast"><div class="mast-in">
  <p class="eyebrow">Redesign handoff · captured {len(R)} of {len(R)} routes</p>
  <h1>Attune Screen Inventory</h1>
  <p class="lede">Every page in the self-serve DSP at <code>demo.dsp.pgammedia.com</code>, captured
  full-page against demo fixture data. Grouped by the product's real information architecture so the
  overlaps between surfaces are visible at a glance.</p>
  <div class="facts">
    <div class="fact"><b>{len(R)}</b><span>screens</span></div>
    <div class="fact"><b>{len(SECTIONS)}</b><span>product areas</span></div>
    <div class="fact"><b>1440×900</b><span>viewport @2x</span></div>
    <div class="fact"><b>{len(flagged)}</b><span>no-data states</span></div>
  </div>
</div></header>

<div class="wrap">
  <nav class="rail" aria-label="Screen index">{''.join(nav)}</nav>
  <main>
    <div class="note-box">
      <h2>How to read this</h2>
      <ul>
        <li>Screens are <strong>full-page captures</strong> — the whole scroll height, not just the fold.</li>
        <li>Data is the product's own <strong>demo fixtures</strong>, so numbers are representative but fictional.</li>
        <li><strong>⚑ flags</strong> mark screens that render an empty or not-found state because demo mode has no fixture behind them. The layout is real; the content is not.</li>
        <li>Sizes are CSS pixels. Source PNGs are 2× that, supplied alongside this document.</li>
      </ul>
    </div>

    <section class="cs-wrap">
      <div class="block-h"><h2>All screens</h2><p>Top of each page — click to jump</p></div>
      <div class="grid">{''.join(contact)}</div>
    </section>

    {''.join(out_body)}
  </main>
</div>"""

open("./attune-screen-inventory.html", "w").write(HTML)
print("wrote", os.path.getsize("./attune-screen-inventory.html")/1024/1024, "MB")
