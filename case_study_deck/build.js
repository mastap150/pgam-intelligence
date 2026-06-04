// PGAM Case Study Deck — Amazon Business OLV
const pptxgen = require("pptxgenjs");

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE"; // 13.333 x 7.5
const SW = 13.333, SH = 7.5;
pres.author = "PGAM Media";
pres.title = "PGAM Case Study — Amazon Business OLV";
pres.company = "PGAM Media";

// Palette — Midnight Executive (navy + ice + gold accent)
const C = {
  navy: "0B1E3F",
  navyDeep: "071530",
  ice: "CADCFC",
  iceSoft: "E6EEFC",
  bg: "F7F9FC",
  white: "FFFFFF",
  gold: "F2A900",
  goldSoft: "FCE9B5",
  text: "0B1E3F",
  muted: "64748B",
  divider: "D5DCE8",
  pos: "1F9D55",
};

const F = { head: "Georgia", body: "Calibri" };

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------
function addFooter(slide, page, total) {
  slide.addShape(pres.shapes.LINE, {
    x: 0.5, y: SH - 0.45, w: SW - 1.0, h: 0,
    line: { color: C.divider, width: 0.75 },
  });
  slide.addText("PGAM MEDIA  ·  Case Study  ·  Amazon Business OLV", {
    x: 0.5, y: SH - 0.36, w: 8, h: 0.3,
    fontFace: F.body, fontSize: 9, color: C.muted, charSpacing: 2,
  });
  slide.addText(`${page} / ${total}`, {
    x: SW - 1.5, y: SH - 0.36, w: 1.0, h: 0.3,
    fontFace: F.body, fontSize: 9, color: C.muted, align: "right",
  });
}

function sectionTitle(slide, kicker, title) {
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 0.55, w: 0.55, h: 0.04,
    fill: { color: C.gold }, line: { type: "none" },
  });
  slide.addText(kicker, {
    x: 1.2, y: 0.4, w: 8, h: 0.35,
    fontFace: F.body, fontSize: 10, color: C.gold,
    bold: true, charSpacing: 4, margin: 0,
  });
  slide.addText(title, {
    x: 0.5, y: 0.8, w: SW - 1.0, h: 0.7,
    fontFace: F.head, fontSize: 30, color: C.navy,
    bold: true, margin: 0,
  });
}

const TOTAL = 11;

// ================================================================
// SLIDE 1 — Cover
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.navyDeep };

  // Decorative right-side panel
  s.addShape(pres.shapes.RECTANGLE, {
    x: SW - 4.5, y: 0, w: 4.5, h: SH,
    fill: { color: C.navy }, line: { type: "none" },
  });
  // Gold accent strip
  s.addShape(pres.shapes.RECTANGLE, {
    x: SW - 4.5, y: 0, w: 0.08, h: SH,
    fill: { color: C.gold }, line: { type: "none" },
  });

  // Kicker
  s.addText("PGAM MEDIA  ·  CTV + OLV DSP", {
    x: 0.8, y: 1.6, w: 8, h: 0.4,
    fontFace: F.body, fontSize: 11, color: C.gold,
    bold: true, charSpacing: 6,
  });

  // Title
  s.addText("Brand-Safe OLV at\nPremium Scale", {
    x: 0.8, y: 2.1, w: 8.5, h: 2.2,
    fontFace: F.head, fontSize: 54, color: C.white,
    bold: true, lineSpacingMultiple: 1.05,
  });

  // Subtitle
  s.addText(
    "How PGAM delivered 3.9M premium impressions and a 66.5% video " +
    "completion rate for Amazon Business — above the IAB OLV benchmark.",
    {
      x: 0.8, y: 4.5, w: 8.0, h: 1.4,
      fontFace: F.body, fontSize: 16, color: C.ice,
      lineSpacingMultiple: 1.35,
    }
  );

  // Right-panel content
  s.addText("CASE STUDY", {
    x: SW - 4.2, y: 1.6, w: 3.7, h: 0.35,
    fontFace: F.body, fontSize: 10, color: C.gold,
    bold: true, charSpacing: 8,
  });
  s.addText("Amazon Business", {
    x: SW - 4.2, y: 2.0, w: 3.7, h: 0.6,
    fontFace: F.head, fontSize: 26, color: C.white, bold: true,
  });
  s.addText("Q2 2026  ·  Online Video", {
    x: SW - 4.2, y: 2.6, w: 3.7, h: 0.35,
    fontFace: F.body, fontSize: 13, color: C.ice,
  });

  // Quick fact cards
  const facts = [
    ["3.9M", "Premium\nimpressions"],
    ["66.5%", "Video\ncompletion rate"],
    ["48", "Vetted\npublishers"],
  ];
  facts.forEach((f, i) => {
    const y = 3.5 + i * 1.05;
    s.addShape(pres.shapes.RECTANGLE, {
      x: SW - 4.2, y, w: 3.7, h: 0.9,
      fill: { color: C.navyDeep }, line: { color: C.gold, width: 0.5 },
    });
    s.addText(f[0], {
      x: SW - 4.1, y: y + 0.1, w: 1.6, h: 0.75,
      fontFace: F.head, fontSize: 28, color: C.gold, bold: true, valign: "middle",
    });
    s.addText(f[1], {
      x: SW - 2.4, y: y + 0.1, w: 1.9, h: 0.75,
      fontFace: F.body, fontSize: 11, color: C.ice, valign: "middle",
    });
  });

  // Bottom signature
  s.addText("pgammedia.com  ·  Prepared June 2026", {
    x: 0.8, y: SH - 0.7, w: 8, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.muted, charSpacing: 3,
  });
}

// ================================================================
// SLIDE 2 — Executive Summary (big stats)
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.bg };
  sectionTitle(s, "EXECUTIVE SUMMARY", "Live results, 22 days into flight");

  // 4-stat grid
  const stats = [
    { v: "3.94M", l: "Impressions delivered", sub: "Premium OLV inventory" },
    { v: "66.5%", l: "Video completion rate", sub: "vs ~64% IAB OLV benchmark" },
    { v: "60.5", l: "PGAM Attention score", sub: "Proprietary, TFN-calibrated" },
    { v: "48", l: "Distinct publishers", sub: "100% domain-allowlisted" },
  ];
  const startX = 0.5, startY = 1.8, cardW = 3.0, cardH = 2.1, gap = 0.13;
  stats.forEach((st, i) => {
    const x = startX + i * (cardW + gap);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: startY, w: cardW, h: cardH,
      fill: { color: C.white },
      line: { color: C.divider, width: 0.5 },
      shadow: { type: "outer", color: "000000", blur: 8, offset: 2, angle: 90, opacity: 0.06 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: startY, w: cardW, h: 0.06,
      fill: { color: C.gold }, line: { type: "none" },
    });
    s.addText(st.v, {
      x: x + 0.2, y: startY + 0.25, w: cardW - 0.4, h: 0.95,
      fontFace: F.head, fontSize: 44, color: C.navy, bold: true, margin: 0,
    });
    s.addText(st.l, {
      x: x + 0.2, y: startY + 1.25, w: cardW - 0.4, h: 0.4,
      fontFace: F.body, fontSize: 13, color: C.navy, bold: true, margin: 0,
    });
    s.addText(st.sub, {
      x: x + 0.2, y: startY + 1.62, w: cardW - 0.4, h: 0.4,
      fontFace: F.body, fontSize: 10, color: C.muted, margin: 0,
    });
  });

  // Lower commentary band
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 4.3, w: SW - 1.0, h: 2.5,
    fill: { color: C.navy }, line: { type: "none" },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 4.3, w: 0.1, h: 2.5,
    fill: { color: C.gold }, line: { type: "none" },
  });
  s.addText("THE READ", {
    x: 0.85, y: 4.45, w: 4, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.gold,
    bold: true, charSpacing: 6,
  });
  s.addText(
    "Amazon Business' Q2 OLV campaign is pacing on plan and over-indexing on " +
    "completion. Completion rate climbed from the low 60s in week one to the " +
    "low 70s by week three as PGAM's optimization moved spend toward " +
    "high-attention placements. Forty-eight allowlisted publishers, zero " +
    "MFA leakage, and a full-funnel mid-funnel layer launched on June 3.",
    {
      x: 0.85, y: 4.8, w: SW - 1.7, h: 1.9,
      fontFace: F.body, fontSize: 14, color: C.white,
      lineSpacingMultiple: 1.4,
    }
  );

  addFooter(s, 2, TOTAL);
}

// ================================================================
// SLIDE 3 — The Brand & The Ask
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.bg };
  sectionTitle(s, "THE BRIEF", "The brand, the audience, the ask");

  // Left: brand block
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 1.8, w: 5.5, h: 4.9,
    fill: { color: C.white }, line: { color: C.divider, width: 0.5 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 1.8, w: 0.1, h: 4.9,
    fill: { color: C.navy }, line: { type: "none" },
  });
  s.addText("THE BRAND", {
    x: 0.85, y: 2.0, w: 4, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 6,
  });
  s.addText("Amazon Business", {
    x: 0.85, y: 2.35, w: 5, h: 0.6,
    fontFace: F.head, fontSize: 26, color: C.navy, bold: true,
  });
  s.addText(
    "Amazon's B2B procurement platform serving 6M+ business customers — " +
    "from sole proprietors to Fortune 500 buyers. Category leader competing " +
    "for share of corporate purchasing wallets.",
    {
      x: 0.85, y: 3.05, w: 5.0, h: 1.5,
      fontFace: F.body, fontSize: 13, color: C.text, lineSpacingMultiple: 1.35,
    }
  );

  s.addText("AUDIENCE", {
    x: 0.85, y: 4.6, w: 4, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 6,
  });
  s.addText(
    "SMB owners, office managers, and procurement decision-makers — " +
    "engaged on business, finance, and tech editorial.",
    {
      x: 0.85, y: 4.95, w: 5.0, h: 1.5,
      fontFace: F.body, fontSize: 13, color: C.text, lineSpacingMultiple: 1.35,
    }
  );

  // Right: the ask block
  s.addShape(pres.shapes.RECTANGLE, {
    x: 6.3, y: 1.8, w: 6.5, h: 4.9,
    fill: { color: C.navy }, line: { type: "none" },
  });
  s.addText("THE ASK", {
    x: 6.55, y: 2.0, w: 4, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 6,
  });
  s.addText("Brand-safe OLV at scale", {
    x: 6.55, y: 2.35, w: 6, h: 0.6,
    fontFace: F.head, fontSize: 24, color: C.white, bold: true,
  });

  const asks = [
    ["Drive completion, not clicks", "Prioritize VTR and viewable completion over CPC noise."],
    ["Premium business editorial", "Adjacent to business, finance, and decision-maker content."],
    ["Full transparency", "Every domain disclosed. No MFA, no laundered supply."],
    ["Set up full-funnel", "Upper-funnel OLV today; mid-funnel layer next."],
  ];
  asks.forEach((a, i) => {
    const y = 3.15 + i * 0.85;
    s.addShape(pres.shapes.OVAL, {
      x: 6.55, y: y + 0.05, w: 0.18, h: 0.18,
      fill: { color: C.gold }, line: { type: "none" },
    });
    s.addText(a[0], {
      x: 6.85, y, w: 5.8, h: 0.3,
      fontFace: F.body, fontSize: 13, color: C.white, bold: true, margin: 0,
    });
    s.addText(a[1], {
      x: 6.85, y: y + 0.3, w: 5.8, h: 0.5,
      fontFace: F.body, fontSize: 11, color: C.ice, margin: 0, lineSpacingMultiple: 1.25,
    });
  });

  addFooter(s, 3, TOTAL);
}

// ================================================================
// SLIDE 4 — The PGAM Approach (4 pillars)
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.bg };
  sectionTitle(s, "THE APPROACH", "Four pillars behind the result");

  const pillars = [
    { n: "01", t: "Curated premium supply",
      d: "Direct ClearLine seats into business and finance editorial. Domain allowlists, not contextual scrapes. No MFA, no incentivized traffic." },
    { n: "02", t: "PGAM Attention Engine",
      d: "Proprietary attention scoring on every impression — calibrated against real tracked-phone-number outcomes, not vendor black boxes." },
    { n: "03", t: "Full-funnel attribution",
      d: "TFN-matched call attribution, CAPI server-to-server, and household-identity bridge. Measure what the campaign actually drove." },
    { n: "04", t: "Margin-free transparency",
      d: "You see the publishers, the rates, the viewability, the attention. No supply-path arbitrage. Every dollar accountable." },
  ];

  const cardW = 3.0, cardH = 4.3, startX = 0.5, gap = 0.13, startY = 1.9;
  pillars.forEach((p, i) => {
    const x = startX + i * (cardW + gap);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: startY, w: cardW, h: cardH,
      fill: { color: C.white }, line: { color: C.divider, width: 0.5 },
      shadow: { type: "outer", color: "000000", blur: 8, offset: 2, angle: 90, opacity: 0.06 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: startY, w: cardW, h: 1.3,
      fill: { color: C.navy }, line: { type: "none" },
    });
    s.addText(p.n, {
      x: x + 0.2, y: startY + 0.15, w: 1.5, h: 0.5,
      fontFace: F.head, fontSize: 24, color: C.gold, bold: true, margin: 0,
    });
    s.addText(p.t, {
      x: x + 0.2, y: startY + 0.65, w: cardW - 0.4, h: 0.6,
      fontFace: F.head, fontSize: 15, color: C.white, bold: true, margin: 0,
    });
    s.addText(p.d, {
      x: x + 0.2, y: startY + 1.5, w: cardW - 0.4, h: cardH - 1.6,
      fontFace: F.body, fontSize: 12, color: C.text,
      margin: 0, lineSpacingMultiple: 1.4, valign: "top",
    });
  });

  addFooter(s, 4, TOTAL);
}

// ================================================================
// SLIDE 5 — Results vs Benchmark (comparison chart)
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.bg };
  sectionTitle(s, "PERFORMANCE", "Measured against industry benchmarks");

  // Bar chart: VTR comparison
  const chartData = [
    {
      name: "PGAM – Amazon Q2",
      labels: ["Completion rate", "Attention score (PGAM)", "Allowlisted supply"],
      values: [66.5, 60.5, 100],
    },
    {
      name: "IAB OLV benchmark",
      labels: ["Completion rate", "Attention score (PGAM)", "Allowlisted supply"],
      values: [64.0, 0, 0],
    },
  ];

  s.addChart(pres.charts.BAR, chartData, {
    x: 0.5, y: 1.9, w: 8.2, h: 4.8,
    barDir: "col",
    chartColors: [C.gold, C.ice],
    chartArea: { fill: { color: C.white } },
    catAxisLabelColor: C.text,
    catAxisLabelFontSize: 11,
    valAxisLabelColor: C.muted,
    valAxisLabelFontSize: 9,
    valGridLine: { color: C.divider, size: 0.5 },
    catGridLine: { style: "none" },
    showValue: true,
    dataLabelPosition: "outEnd",
    dataLabelColor: C.navy,
    dataLabelFontSize: 11,
    dataLabelFontBold: true,
    showLegend: true,
    legendPos: "b",
    legendColor: C.text,
    legendFontSize: 11,
    showTitle: false,
    barGapWidthPct: 80,
  });

  // Right callout column
  s.addShape(pres.shapes.RECTANGLE, {
    x: 8.95, y: 1.9, w: 3.85, h: 4.8,
    fill: { color: C.navy }, line: { type: "none" },
  });
  s.addText("READ THIS WAY", {
    x: 9.15, y: 2.05, w: 3.5, h: 0.3,
    fontFace: F.body, fontSize: 9, color: C.gold, bold: true, charSpacing: 5,
  });

  const notes = [
    ["+2.5 pp", "above IAB OLV completion benchmark"],
    ["60.5", "average PGAM Attention score (0–100)"],
    ["100%", "supply on advertiser-approved allowlist"],
  ];
  notes.forEach((n, i) => {
    const y = 2.55 + i * 1.35;
    s.addText(n[0], {
      x: 9.15, y, w: 3.5, h: 0.6,
      fontFace: F.head, fontSize: 28, color: C.gold, bold: true, margin: 0,
    });
    s.addText(n[1], {
      x: 9.15, y: y + 0.65, w: 3.5, h: 0.6,
      fontFace: F.body, fontSize: 11, color: C.white, margin: 0, lineSpacingMultiple: 1.3,
    });
  });

  // Source line
  s.addText("IAB OLV benchmark: industry average 62–68% completion for in-stream desktop+CTV video.", {
    x: 0.5, y: 6.85, w: SW - 1.0, h: 0.25,
    fontFace: F.body, fontSize: 9, color: C.muted, italic: true,
  });

  addFooter(s, 5, TOTAL);
}

// ================================================================
// SLIDE 6 — Daily delivery + optimization
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.bg };
  sectionTitle(s, "OPTIMIZATION", "Completion rate climbed as PGAM tuned supply");

  // Daily data (May 13 – June 3)
  const dailyVTR = [
    67.2, 64.6, 62.4, 62.3, 64.3, 66.0, 69.4, 61.0, 63.4, 65.0, 63.6,
    63.1, 63.2, 65.0, 65.6, 64.9, 69.4, 73.2, 72.2, 71.1, 67.4, 69.3,
  ];
  const dayLabels = [
    "5/13","","","5/16","","","5/19","","","5/22","","","5/25","","","5/28","","","5/31","","","6/3",
  ];

  s.addChart(pres.charts.LINE, [{
    name: "Daily completion rate (%)",
    labels: dayLabels,
    values: dailyVTR,
  }], {
    x: 0.5, y: 1.9, w: 8.2, h: 4.8,
    chartColors: [C.gold],
    chartArea: { fill: { color: C.white } },
    lineSize: 3,
    lineSmooth: true,
    lineDataSymbol: "circle",
    lineDataSymbolSize: 8,
    lineDataSymbolLineColor: C.gold,
    catAxisLabelColor: C.muted,
    catAxisLabelFontSize: 9,
    valAxisLabelColor: C.muted,
    valAxisLabelFontSize: 9,
    valGridLine: { color: C.divider, size: 0.5 },
    catGridLine: { style: "none" },
    valAxisMinVal: 55,
    valAxisMaxVal: 80,
    showLegend: false,
    showTitle: false,
  });

  // Right column: insights
  s.addShape(pres.shapes.RECTANGLE, {
    x: 8.95, y: 1.9, w: 3.85, h: 4.8,
    fill: { color: C.white }, line: { color: C.divider, width: 0.5 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 8.95, y: 1.9, w: 0.1, h: 4.8,
    fill: { color: C.gold }, line: { type: "none" },
  });
  s.addText("WEEK-OVER-WEEK", {
    x: 9.2, y: 2.05, w: 3.5, h: 0.3,
    fontFace: F.body, fontSize: 9, color: C.gold, bold: true, charSpacing: 5,
  });
  s.addText("Completion rate moved up as low-attention publishers were de-weighted.", {
    x: 9.2, y: 2.4, w: 3.5, h: 0.9,
    fontFace: F.body, fontSize: 12, color: C.text, lineSpacingMultiple: 1.35,
  });

  const trend = [
    ["Week 1", "~64.6%"],
    ["Week 2", "~64.8%"],
    ["Week 3", "~70.6%"],
  ];
  trend.forEach((t, i) => {
    const y = 3.5 + i * 0.65;
    s.addText(t[0], {
      x: 9.2, y, w: 1.5, h: 0.4,
      fontFace: F.body, fontSize: 12, color: C.muted, margin: 0,
    });
    s.addText(t[1], {
      x: 10.7, y, w: 2.0, h: 0.4,
      fontFace: F.head, fontSize: 16, color: C.navy, bold: true, margin: 0, align: "right",
    });
  });

  s.addShape(pres.shapes.LINE, {
    x: 9.2, y: 5.55, w: 3.4, h: 0,
    line: { color: C.divider, width: 0.75 },
  });
  s.addText("Δ +6.0 pp", {
    x: 9.2, y: 5.7, w: 3.5, h: 0.5,
    fontFace: F.head, fontSize: 20, color: C.pos, bold: true, margin: 0,
  });
  s.addText("Week 1 → Week 3 completion lift", {
    x: 9.2, y: 6.2, w: 3.5, h: 0.4,
    fontFace: F.body, fontSize: 10, color: C.muted, margin: 0,
  });

  addFooter(s, 6, TOTAL);
}

// ================================================================
// SLIDE 7 — Premium supply quality
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.bg };
  sectionTitle(s, "SUPPLY QUALITY", "Top 8 publishers — real domains, real attention");

  // Table data
  const hdrOpts = {
    bold: true, color: C.white, fill: { color: C.navy },
    fontFace: F.body, fontSize: 11, valign: "middle", align: "left",
  };
  const hdrOptsR = { ...hdrOpts, align: "right" };

  const rows = [
    ["Publisher", "Impressions", "Completion", "Attention", "Spend"],
    ["gizmodo.com", "2,263,711", "68.9%", "69.7", "$15,144"],
    ["forbes.com", "600,096", "41.1%", "40.1", "$3,966"],
    ["neoseeker.com", "430,611", "81.7%", "81.4", "$2,902"],
    ["stocktwits.com", "319,256", "75.5%", "81.3", "$2,001"],
    ["stockcharts.com", "153,247", "72.6%", "71.8", "$1,020"],
    ["interestingengineering.com", "92,464", "53.9%", "49.7", "$625"],
    ["barchart.com", "23,611", "93.4%", "93.6", "$151"],
    ["ibtimes.com", "19,642", "79.1%", "77.8", "$131"],
  ];

  const table = rows.map((r, ri) => {
    if (ri === 0) {
      return r.map((c, ci) => ({ text: c, options: ci === 0 ? hdrOpts : hdrOptsR }));
    }
    const zebra = ri % 2 === 0 ? C.iceSoft : C.white;
    return r.map((c, ci) => ({
      text: c,
      options: {
        color: C.text, fill: { color: zebra },
        fontFace: F.body, fontSize: 11, valign: "middle",
        align: ci === 0 ? "left" : "right",
        bold: ci === 0,
      },
    }));
  });

  s.addTable(table, {
    x: 0.5, y: 1.9, w: 8.5, colW: [2.6, 1.6, 1.4, 1.3, 1.6],
    rowH: 0.42, border: { pt: 0.5, color: C.divider },
  });

  // Right summary panel
  s.addShape(pres.shapes.RECTANGLE, {
    x: 9.3, y: 1.9, w: 3.5, h: 4.8,
    fill: { color: C.navy }, line: { type: "none" },
  });
  s.addText("40 MORE PUBLISHERS", {
    x: 9.5, y: 2.05, w: 3.2, h: 0.35,
    fontFace: F.body, fontSize: 9, color: C.gold, bold: true, charSpacing: 5,
  });
  s.addText("48 total", {
    x: 9.5, y: 2.5, w: 3.2, h: 0.6,
    fontFace: F.head, fontSize: 26, color: C.white, bold: true,
  });
  s.addText(
    "Long tail spans business news, finance, and engineering editorial. " +
    "Every domain disclosed at the impression level — no laundered supply paths.",
    {
      x: 9.5, y: 3.2, w: 3.2, h: 2.2,
      fontFace: F.body, fontSize: 11, color: C.ice, lineSpacingMultiple: 1.35,
    }
  );
  s.addShape(pres.shapes.LINE, {
    x: 9.5, y: 5.4, w: 3.0, h: 0,
    line: { color: C.gold, width: 1 },
  });
  s.addText("Zero MFA. Zero subdomain spoofing.", {
    x: 9.5, y: 5.55, w: 3.2, h: 0.8,
    fontFace: F.body, fontSize: 11, color: C.white, italic: true, lineSpacingMultiple: 1.35,
  });

  addFooter(s, 7, TOTAL);
}

// ================================================================
// SLIDE 8 — PGAM Attention Engine
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.bg };
  sectionTitle(s, "THE DIFFERENTIATOR", "PGAM Attention Engine");

  // Left: explanation
  s.addText(
    "Attention isn't a vendor score we license. We score every impression " +
    "ourselves — using ad-server signals (viewability, completion, audibility, " +
    "device class) calibrated against real outcomes from tracked phone numbers " +
    "on advertiser landing pages.",
    {
      x: 0.5, y: 1.9, w: 6.5, h: 1.8,
      fontFace: F.body, fontSize: 14, color: C.text, lineSpacingMultiple: 1.4,
    }
  );

  s.addText("WHY IT MATTERS", {
    x: 0.5, y: 3.85, w: 6, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 5,
  });

  const points = [
    "Optimizes toward what actually moves the business, not vanity metrics",
    "Independent of any single SSP — we own the scoring stack end-to-end",
    "TFN-calibrated so the score correlates with real human response",
    "Runs in parallel with Athena 'PGAM Attention' for cross-validation",
  ];
  points.forEach((p, i) => {
    const y = 4.25 + i * 0.55;
    s.addShape(pres.shapes.OVAL, {
      x: 0.5, y: y + 0.07, w: 0.16, h: 0.16,
      fill: { color: C.gold }, line: { type: "none" },
    });
    s.addText(p, {
      x: 0.8, y, w: 6.3, h: 0.5,
      fontFace: F.body, fontSize: 12, color: C.text, margin: 0, lineSpacingMultiple: 1.3,
    });
  });

  // Right: formula card
  s.addShape(pres.shapes.RECTANGLE, {
    x: 7.6, y: 1.9, w: 5.2, h: 4.8,
    fill: { color: C.navy }, line: { type: "none" },
  });
  s.addText("SCORE = (B + D) × C", {
    x: 7.8, y: 2.15, w: 4.8, h: 0.5,
    fontFace: F.head, fontSize: 22, color: C.gold, bold: true,
  });
  s.addText("Postgres-native v1 scorer", {
    x: 7.8, y: 2.7, w: 4.8, h: 0.35,
    fontFace: F.body, fontSize: 11, color: C.ice, charSpacing: 2,
  });

  const formula = [
    ["B", "Behavioral", "Viewability + audible-completion lift"],
    ["D", "Device", "Premium device class + connection quality"],
    ["C", "Calibration", "TFN-matched human-response factor"],
  ];
  formula.forEach((f, i) => {
    const y = 3.4 + i * 1.0;
    s.addShape(pres.shapes.OVAL, {
      x: 7.8, y, w: 0.6, h: 0.6,
      fill: { color: C.gold }, line: { type: "none" },
    });
    s.addText(f[0], {
      x: 7.8, y, w: 0.6, h: 0.6,
      fontFace: F.head, fontSize: 22, color: C.navy, bold: true, align: "center", valign: "middle", margin: 0,
    });
    s.addText(f[1], {
      x: 8.55, y: y + 0.02, w: 4, h: 0.3,
      fontFace: F.body, fontSize: 13, color: C.white, bold: true, margin: 0,
    });
    s.addText(f[2], {
      x: 8.55, y: y + 0.32, w: 4, h: 0.5,
      fontFace: F.body, fontSize: 10, color: C.ice, margin: 0, lineSpacingMultiple: 1.3,
    });
  });

  addFooter(s, 8, TOTAL);
}

// ================================================================
// SLIDE 9 — Full-funnel architecture
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.bg };
  sectionTitle(s, "FULL FUNNEL", "Brand awareness layered with mid-funnel intent");

  // Funnel: 3 stacked horizontal cards
  const funnels = [
    {
      tier: "UPPER FUNNEL",
      title: "Q2 OLV — Brand awareness",
      status: "LIVE  ·  $65.8K  ·  May 13 → Jun 7",
      detail: "3.94M impressions delivered, 66.5% completion rate, 48 premium publishers.",
      color: C.gold,
      w: 11.0,
    },
    {
      tier: "MID FUNNEL",
      title: "Q2 Mid-Funnel — Consideration",
      status: "LIVE  ·  $30K  ·  Jun 3 → Jul 27",
      detail: "Retargets upper-funnel completers + lookalike business decision-makers.",
      color: C.ice,
      w: 9.0,
    },
    {
      tier: "LOWER FUNNEL",
      title: "Conversion / call attribution",
      status: "MEASUREMENT LIVE  ·  TFN + CAPI",
      detail: "PGAM CTV Attribution Stack matches landing-page calls and CAPI events back to impression.",
      color: C.iceSoft,
      w: 7.0,
    },
  ];

  funnels.forEach((f, i) => {
    const y = 1.9 + i * 1.55;
    const x = (SW - f.w) / 2;
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w: f.w, h: 1.35,
      fill: { color: C.white }, line: { color: C.divider, width: 0.5 },
      shadow: { type: "outer", color: "000000", blur: 8, offset: 2, angle: 90, opacity: 0.06 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w: 0.12, h: 1.35,
      fill: { color: f.color }, line: { type: "none" },
    });
    s.addText(f.tier, {
      x: x + 0.3, y: y + 0.12, w: 3, h: 0.3,
      fontFace: F.body, fontSize: 9, color: C.gold, bold: true, charSpacing: 5, margin: 0,
    });
    s.addText(f.title, {
      x: x + 0.3, y: y + 0.4, w: f.w - 0.5, h: 0.45,
      fontFace: F.head, fontSize: 17, color: C.navy, bold: true, margin: 0,
    });
    s.addText(f.detail, {
      x: x + 0.3, y: y + 0.82, w: f.w - 4.3, h: 0.5,
      fontFace: F.body, fontSize: 11, color: C.text, margin: 0, lineSpacingMultiple: 1.3,
    });
    s.addText(f.status, {
      x: x + f.w - 4.0, y: y + 0.85, w: 3.7, h: 0.4,
      fontFace: F.body, fontSize: 10, color: C.muted, italic: true,
      margin: 0, align: "right",
    });
  });

  // Bottom note
  s.addText(
    "Full-funnel architecture means every upper-funnel completion has somewhere to go. " +
    "Mid-funnel launched June 3 to absorb the audience PGAM just paid to reach.",
    {
      x: 0.5, y: 6.65, w: SW - 1.0, h: 0.4,
      fontFace: F.body, fontSize: 11, color: C.muted, align: "center", italic: true,
    }
  );

  addFooter(s, 9, TOTAL);
}

// ================================================================
// SLIDE 10 — Brand safety & why PGAM
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.navyDeep };

  // Title
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 0.55, w: 0.55, h: 0.04,
    fill: { color: C.gold }, line: { type: "none" },
  });
  s.addText("WHY PGAM", {
    x: 1.2, y: 0.4, w: 8, h: 0.35,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 4, margin: 0,
  });
  s.addText("What you get that you don't anywhere else", {
    x: 0.5, y: 0.8, w: SW - 1.0, h: 0.7,
    fontFace: F.head, fontSize: 30, color: C.white, bold: true, margin: 0,
  });

  // 6 reasons in 2x3 grid
  const reasons = [
    { t: "Direct ClearLine supply", d: "PGAM holds the seat. No reseller hops. Lower cost, full transparency." },
    { t: "Own attention stack", d: "Scoring built in-house. Calibrated against real call outcomes, not vendor scores." },
    { t: "Full attribution wired", d: "TFN-matched calls, CAPI server-to-server, household identity bridge — shipped." },
    { t: "Brand safety by allowlist", d: "Domain allowlists, not contextual scrapes. Every publisher disclosed at impression level." },
    { t: "Margin transparency", d: "You see media cost vs gross. No supply-path arbitrage hidden in your CPM." },
    { t: "Pace + optimize in flight", d: "Buyer agent watches pacing, frequency, attention. Levers move daily, not monthly." },
  ];
  const startX = 0.5, startY = 1.85, cw = 4.05, ch = 2.4, gx = 0.2, gy = 0.25;
  reasons.forEach((r, i) => {
    const col = i % 3, row = Math.floor(i / 3);
    const x = startX + col * (cw + gx);
    const y = startY + row * (ch + gy);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w: cw, h: ch,
      fill: { color: C.navy }, line: { color: C.gold, width: 0.5 },
    });
    s.addShape(pres.shapes.OVAL, {
      x: x + 0.3, y: y + 0.3, w: 0.55, h: 0.55,
      fill: { color: C.gold }, line: { type: "none" },
    });
    s.addText(String(i + 1), {
      x: x + 0.3, y: y + 0.3, w: 0.55, h: 0.55,
      fontFace: F.head, fontSize: 20, color: C.navy, bold: true,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(r.t, {
      x: x + 0.3, y: y + 1.0, w: cw - 0.5, h: 0.5,
      fontFace: F.head, fontSize: 15, color: C.white, bold: true, margin: 0,
    });
    s.addText(r.d, {
      x: x + 0.3, y: y + 1.5, w: cw - 0.5, h: ch - 1.6,
      fontFace: F.body, fontSize: 11, color: C.ice, margin: 0, lineSpacingMultiple: 1.35, valign: "top",
    });
  });

  // Footer (dark variant)
  s.addText("PGAM MEDIA  ·  Case Study  ·  Amazon Business OLV", {
    x: 0.5, y: SH - 0.36, w: 8, h: 0.3,
    fontFace: F.body, fontSize: 9, color: C.ice, charSpacing: 2,
  });
  s.addText(`10 / ${TOTAL}`, {
    x: SW - 1.5, y: SH - 0.36, w: 1.0, h: 0.3,
    fontFace: F.body, fontSize: 9, color: C.ice, align: "right",
  });
}

// ================================================================
// SLIDE 11 — CTA / Contact
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.navyDeep };

  // Left accent panel
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.25, h: SH,
    fill: { color: C.gold }, line: { type: "none" },
  });

  s.addText("LET'S BUILD YOURS", {
    x: 1.0, y: 1.6, w: 11, h: 0.4,
    fontFace: F.body, fontSize: 12, color: C.gold, bold: true, charSpacing: 6,
  });

  s.addText("Premium OLV.\nReal attention.\nFull transparency.", {
    x: 1.0, y: 2.1, w: 12, h: 2.6,
    fontFace: F.head, fontSize: 54, color: C.white, bold: true, lineSpacingMultiple: 1.05,
  });

  s.addText(
    "If you run a brand-awareness or full-funnel video budget and you're tired " +
    "of vendor black boxes and laundered supply, we should talk.",
    {
      x: 1.0, y: 4.85, w: 10.5, h: 1.2,
      fontFace: F.body, fontSize: 17, color: C.ice, lineSpacingMultiple: 1.35,
    }
  );

  // Contact card
  s.addShape(pres.shapes.RECTANGLE, {
    x: 1.0, y: 6.2, w: 11.3, h: 0.85,
    fill: { color: C.navy }, line: { color: C.gold, width: 0.5 },
  });
  s.addText("Priyesh Patel  ·  PGAM Media", {
    x: 1.2, y: 6.3, w: 5, h: 0.35,
    fontFace: F.body, fontSize: 13, color: C.white, bold: true, margin: 0,
  });
  s.addText("priyesh@pgammedia.com  ·  pgammedia.com", {
    x: 1.2, y: 6.65, w: 7, h: 0.3,
    fontFace: F.body, fontSize: 11, color: C.ice, margin: 0,
  });
  s.addText(`${TOTAL} / ${TOTAL}`, {
    x: SW - 2.2, y: 6.55, w: 1.0, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.ice, align: "right",
  });
}

// ----------------------------------------------------------------
pres.writeFile({ fileName: "PGAM_Case_Study_Amazon_Business_OLV.pptx" })
  .then(f => console.log("Wrote:", f));
