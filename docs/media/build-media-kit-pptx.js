/**
 * Destination.com advertiser media kit — .pptx generator.
 *
 * Companion to docs/media/destination-com-media-kit.html; same content,
 * same palette, as a sendable deck. Run: npm install pptxgenjs && node this.
 *
 * Newsletter figures and all segment splits are internal projections, not
 * measured — see the footnote on the final slide.
 */
const pptxgen = require("pptxgenjs");

/* ── Palette — destination.com brand, navy-dominant for print/send ── */
const NAVY      = "1A2634";
const NAVY_DEEP = "16202C";
const SAND      = "F5F0E8";
const SAND_DK   = "EDE5D6";
const PAPER     = "FAF7F2";
const TERRA     = "C4622D";
const TERRA_LT  = "E08048";
const TERRA_DK  = "A0471C";
const GOLD      = "D4A843";
const GOLD_DK   = "8A6620";
const INK       = "1A1714";
const INK_MID   = "3D3530";
const INK_SOFT  = "6B5F58";
const D_INK     = "F2EDE4";
const D_SOFT    = "9AA9B8";

/* ── Type — safe-list faces so QA overflow checks are trustworthy ── */
const DISPLAY = "Century Schoolbook";
const BODY    = "Calibri";

const W = 13.333, H = 7.5, M = 0.62;

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE";
pres.author = "PGAM Media";
pres.company = "Destination.com";
pres.title = "Destination.com Media Kit";

/* ── Motif: numbered terracotta disc + uppercase section label.
      Repeats on every slide. No accent bars anywhere. ── */
function sectionHead(slide, num, label, onDark) {
  slide.addShape(pres.ShapeType.ellipse, {
    x: M, y: M, w: 0.36, h: 0.36,
    fill: { color: onDark ? TERRA_LT : TERRA_DK }, line: { type: "none" },
  });
  slide.addText(num, {
    x: M, y: M, w: 0.36, h: 0.36,
    fontFace: BODY, fontSize: 12, bold: true,
    color: onDark ? NAVY : "FFFFFF", align: "center", valign: "middle", margin: 0,
  });
  slide.addText(label.toUpperCase(), {
    x: M + 0.52, y: M, w: 7.5, h: 0.36,
    fontFace: BODY, fontSize: 11, bold: true, charSpacing: 2.4,
    color: onDark ? GOLD : GOLD_DK, valign: "middle", margin: 0,
  });
}

function title(slide, text, onDark, y) {
  slide.addText(text, {
    x: M, y: y || 1.20, w: 10.2, h: 1.30,
    fontFace: DISPLAY, fontSize: 33, bold: true,
    color: onDark ? D_INK : INK, valign: "top", margin: 0, lineSpacing: 36,
  });
}

function lede(slide, text, onDark, y, w) {
  slide.addText(text, {
    x: M, y: y || 2.54, w: w || 11.9, h: 0.86,
    fontFace: BODY, fontSize: 14.5,
    color: onDark ? D_SOFT : INK_MID, valign: "top", margin: 0, lineSpacing: 21,
  });
}

/* ══════════════ 1 · COVER ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: NAVY };

  // Compass motif — concentric rings, low contrast, bleeding off the right edge.
  [3.5, 2.7, 1.9, 1.1].forEach((r) => {
    s.addShape(pres.ShapeType.ellipse, {
      x: 10.5 - r, y: 3.75 - r, w: r * 2, h: r * 2,
      // pptxgenjs ignores fill:{type:"none"} and emits a SOLID fill, so paint the
      // ring interiors with the slide ground; drawn largest-first, each smaller
      // disc masks the previous interior and only the strokes remain visible.
      fill: { color: NAVY }, line: { color: TERRA_LT, width: 0.9, transparency: 60 },
    });
  });
  s.addShape(pres.ShapeType.ellipse, {
    x: 10.36, y: 3.61, w: 0.28, h: 0.28,
    fill: { color: GOLD }, line: { type: "none" },
  });

  s.addText("ADVERTISING & PARTNERSHIPS  ·  2026", {
    x: M, y: 1.5, w: 8, h: 0.3,
    fontFace: BODY, fontSize: 11.5, bold: true, charSpacing: 2.6, color: GOLD, margin: 0,
  });
  s.addText(
    [
      { text: "Destination", options: { color: D_INK } },
      { text: ".", options: { color: TERRA_LT } },
      { text: "com", options: { color: D_INK } },
    ],
    { x: M - 0.05, y: 2.0, w: 9.2, h: 1.3, fontFace: DISPLAY, fontSize: 60, bold: true, margin: 0 }
  );
  s.addText(
    "Guides written by travelers who have actually been there —\nand an audience that books what we recommend.",
    { x: M, y: 3.42, w: 7.4, h: 1.0, fontFace: DISPLAY, fontSize: 16.5,
      italic: true, color: D_INK, margin: 0, lineSpacing: 26 }
  );

  const facts = [
    ["CATEGORY", "Travel · Points & Miles"],
    ["OWNER", "PGAM Media, owned & operated"],
    ["NEWSLETTER", "60,000 subscribers"],
    ["ENGAGEMENT", "50%+ open  ·  10% click"],
    ["SURFACES", "Web · Email · iOS & Android"],
  ];
  let fy = 4.72;
  facts.forEach(([k, v]) => {
    s.addShape(pres.ShapeType.line, {
      x: M, y: fy, w: 7.4, h: 0,
      line: { color: D_INK, width: 0.6, transparency: 80 },
    });
    s.addText(k, { x: M, y: fy + 0.06, w: 2.3, h: 0.3, fontFace: BODY, fontSize: 9.5,
      bold: true, charSpacing: 1.8, color: D_SOFT, margin: 0, valign: "middle" });
    s.addText(v, { x: M + 2.35, y: fy + 0.06, w: 5.0, h: 0.3, fontFace: BODY, fontSize: 11.5,
      color: D_INK, margin: 0, valign: "middle" });
    fy += 0.46;
  });

  s.addNotes("Cover. Lead with the audience quality argument, not the size. The claim line is the editorial promise that makes the clicks worth buying.");
}

/* ══════════════ 2 · THE PROPERTY ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: PAPER };
  sectionHead(s, "1", "The property");
  title(s, "A travel publisher that owns its own demand stack.");
  lede(s, "Destination.com is an owned-and-operated property of PGAM Media, an ad tech company that runs its own SSP. No reseller between you and the inventory, no opaque supply chain, and a plan that spans editorial, email, and commerce in one conversation.", false, 2.54, 11.9);

  const blocks = [
    ["A high-ticket category",
     "One reader decision is worth thousands in flights, hotels, tours, insurance, and cards. Travel intent monetises at a multiple of general interest."],
    ["A points & miles pillar",
     "Award travel is the highest-value vertical in the category, and it is a first-class section here rather than a footnote. The audience is self-selected and financially engaged."],
    ["A closed loop",
     "Site, newsletter, and app share one identity layer, so a campaign can start in editorial and finish in the inbox against the same segment."],
  ];
  let x = M;
  const cw = (12.09 - 0.6) / 3;
  blocks.forEach(([h, b], i) => {
    s.addShape(pres.ShapeType.roundRect, {
      x, y: 3.5, w: cw, h: 2.28, rectRadius: 0.04,
      fill: { color: SAND }, line: { type: "none" },
    });
    s.addShape(pres.ShapeType.ellipse, {
      x: x + 0.3, y: 3.78, w: 0.3, h: 0.3,
      fill: { color: TERRA_DK }, line: { type: "none" },
    });
    s.addText(String(i + 1), { x: x + 0.3, y: 3.78, w: 0.3, h: 0.3, fontFace: BODY,
      fontSize: 10.5, bold: true, color: "FFFFFF", align: "center", valign: "middle", margin: 0 });
    s.addText(h, { x: x + 0.3, y: 4.2, w: cw - 0.6, h: 0.34, fontFace: DISPLAY,
      fontSize: 15, bold: true, color: INK, margin: 0 });
    s.addText(b, { x: x + 0.3, y: 4.6, w: cw - 0.6, h: 1.0, fontFace: BODY,
      fontSize: 11.5, color: INK_SOFT, margin: 0, lineSpacing: 16 });
    x += cw + 0.3;
  });

  s.addText("No paid rankings, ever. Sponsorship buys placement and attention — never a position in a ranking.", {
    x: M, y: 6.16, w: 11.9, h: 0.34, fontFace: DISPLAY, fontSize: 13.5, italic: true,
    color: TERRA_DK, margin: 0,
  });
  s.addNotes("The 'owns its own demand stack' line is the differentiator against every other travel content site a buyer is comparing us to.");
}

/* ══════════════ 3 · THE WEBSITE ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: PAPER };
  sectionHead(s, "2", "The website");
  title(s, "Four pillars, six regions, one honest voice.");
  lede(s, "A hub-and-spoke library: region hubs feed country guides, country guides feed city and experience guides, and every guide routes back to the planning and points content that converts. You buy into that structure — a region, an experience type, a decision moment — not a run-of-network blur.", false, 2.54, 11.9);

  const pillars = [
    ["Destinations", "Europe, Asia, the Americas, Africa, the Pacific, the Middle East — region hubs down to city level, with freshness dates on every node."],
    ["Experiences", "Adventure, food & drink, culture & art, wellness. Readers filter by how they travel, not only where — which is how brands want to target."],
    ["Travel Guides", "Visas, best-time-to-visit, insurance, packing, safety, airports, booking windows. Highest intent on the site, and the last thing read before booking."],
    ["Points & Miles", "Card bonuses, transfer sweet spots, hotel redemptions, tracked weekly. Premium CPMs and the strongest card-acquisition audience we have."],
  ];
  const pw = (12.09 - 0.75) / 4;
  let px = M;
  pillars.forEach(([h, b], i) => {
    s.addShape(pres.ShapeType.roundRect, {
      x: px, y: 3.46, w: pw, h: 2.42, rectRadius: 0.04,
      fill: { color: i === 3 ? NAVY : SAND }, line: { type: "none" },
    });
    s.addText("PILLAR " + (i + 1), { x: px + 0.26, y: 3.68, w: pw - 0.5, h: 0.24,
      fontFace: BODY, fontSize: 9, bold: true, charSpacing: 1.8,
      color: i === 3 ? GOLD : GOLD_DK, margin: 0 });
    s.addText(h, { x: px + 0.26, y: 3.98, w: pw - 0.5, h: 0.32, fontFace: DISPLAY,
      fontSize: 15, bold: true, color: i === 3 ? D_INK : INK, margin: 0 });
    s.addText(b, { x: px + 0.26, y: 4.38, w: pw - 0.5, h: 1.3, fontFace: BODY,
      fontSize: 11, color: i === 3 ? D_SOFT : INK_SOFT, margin: 0, lineSpacing: 15.5 });
    px += pw + 0.25;
  });

  s.addText("Full traffic, geography, and viewability reporting is shared on request under NDA.", {
    x: M, y: 6.16, w: 11.9, h: 0.32, fontFace: BODY, fontSize: 11.5, italic: true,
    color: INK_SOFT, margin: 0,
  });
  s.addNotes("Do not quote traffic figures from this slide. The NDA line is deliberate — it moves the conversation to a call.");
}

/* ══════════════ 4 · BEYOND THE LIBRARY ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: SAND };
  sectionHead(s, "3", "The website");
  title(s, "Three surfaces past the guides.");

  const items = [
    ["AI Trip Planner", "Readers enter dates, budget, party, and trip type, and get a day-by-day itinerary with a costed breakdown plus live hotel and flight options. Every plan is a declared intent — destination, dates, budget band — and sponsorable as such. The highest-intent inventory on the property."],
    ["Native app · iOS & Android", "Guides, saved trips, and planner access behind a sign-in, giving a persistent identity and a push channel alongside email."],
    ["Live commerce rails", "Hotels, vacation rentals, packages, cars, tours, and activities all route through live affiliate integrations with Expedia Group, Viator, and GetYourGuide. Booking behaviour is measured, not assumed."],
  ];
  let y = 2.68;
  items.forEach(([h, b], i) => {
    s.addShape(pres.ShapeType.ellipse, {
      x: M, y: y + 0.04, w: 0.42, h: 0.42,
      fill: { color: i === 0 ? TERRA_DK : NAVY }, line: { type: "none" },
    });
    s.addText(String(i + 1), { x: M, y: y + 0.04, w: 0.42, h: 0.42, fontFace: BODY,
      fontSize: 13, bold: true, color: i === 0 ? "FFFFFF" : GOLD,
      align: "center", valign: "middle", margin: 0 });
    s.addText(h, { x: M + 0.66, y: y, w: 4.1, h: 0.4, fontFace: DISPLAY,
      fontSize: 17, bold: true, color: INK, margin: 0 });
    s.addText(b, { x: M + 4.9, y: y - 0.02, w: 7.24, h: 1.1, fontFace: BODY,
      fontSize: 12.5, color: INK_MID, margin: 0, lineSpacing: 18 });
    y += 1.42;
  });

  s.addNotes("Lead the planner for performance buyers, the app for reach buyers, the commerce rails for affiliate and OTA conversations.");
}

/* ══════════════ 5 · THE NEWSLETTER ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: NAVY };
  sectionHead(s, "4", "The newsletter", true);
  title(s, "60,000 travelers. Half of them open it. A tenth of them click.", true);
  lede(s, "One flagship issue every Tuesday, plus a Friday award-travel drop to the points and miles segment. The list is grown from editorial, not bought or co-registered — which is why the engagement holds where most travel lists sag.", true, 2.54, 11.9);

  const stats = [
    ["SUBSCRIBERS", "60,000", "Opt-in, single-source, grown from on-site editorial."],
    ["OPEN RATE", "50%+", "~30,000 opens per issue. Travel lists this size typically run 28–38%."],
    ["CLICK RATE", "10%", "~6,000 clicks per issue on the full list — a click-to-open near 20%."],
  ];
  const sw = (12.09 - 0.6) / 3;
  let sx = M;
  stats.forEach(([label, fig, note]) => {
    s.addShape(pres.ShapeType.roundRect, {
      x: sx, y: 3.48, w: sw, h: 1.96, rectRadius: 0.04,
      fill: { color: NAVY_DEEP }, line: { type: "none" },
    });
    s.addText(label, { x: sx + 0.3, y: 3.70, w: sw - 0.6, h: 0.24, fontFace: BODY,
      fontSize: 9.5, bold: true, charSpacing: 2, color: GOLD, margin: 0 });
    s.addText(fig, { x: sx + 0.3, y: 3.96, w: sw - 0.6, h: 0.76, fontFace: DISPLAY,
      fontSize: 44, bold: true, color: D_INK, margin: 0 });
    s.addText(note, { x: sx + 0.3, y: 4.74, w: sw - 0.6, h: 0.6, fontFace: BODY,
      fontSize: 10.5, color: D_SOFT, margin: 0, lineSpacing: 14.5 });
    sx += sw + 0.3;
  });

  s.addShape(pres.ShapeType.roundRect, {
    x: M, y: 5.60, w: 12.09, h: 0.72, rectRadius: 0.03,
    fill: { color: NAVY_DEEP }, line: { color: TERRA_LT, width: 0.75, transparency: 55 },
  });
  s.addText(
    [
      { text: "What a $2,500 primary sponsorship works out to:", options: { color: D_SOFT } },
      { text: "\u00A0\u00A0\u00A0$42\u00A0CPM delivered", options: { color: D_INK, bold: true } },
      { text: "\u00A0\u00A0·\u00A0\u00A0", options: { color: GOLD } },
      { text: "$83\u00A0CPM on opens", options: { color: D_INK, bold: true } },
      { text: "\u00A0\u00A0·\u00A0\u00A0", options: { color: GOLD } },
      { text: "$0.42\u00A0per click", options: { color: TERRA_LT, bold: true } },
    ],
    { x: M + 0.3, y: 5.60, w: 11.5, h: 0.72, fontFace: BODY, fontSize: 13,
      valign: "middle", margin: 0 }
  );

  s.addText("One primary sponsor and one native dispatch per issue — never more.", {
    x: M, y: 6.52, w: 11.9, h: 0.3, fontFace: BODY, fontSize: 11.5, italic: true,
    color: D_SOFT, margin: 0,
  });
  s.addNotes("The $0.42 per click is the strongest number in the deck. Lead with it against any performance buyer.");
}

/* ══════════════ 6 · SEGMENTATION — INTEREST ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: PAPER };
  sectionHead(s, "5", "Segmentation");
  title(s, "You are not buying 60,000 people. You are buying the right 9,000.");
  lede(s, "Every subscriber declares interests and regions at signup, and every click refines them. Segments are addressable individually, combinable, and available for standalone sends from 5,000 names up.", false, 2.54, 11.9);

  s.addChart(
    pres.ChartType.bar,
    [{
      name: "Share of list",
      labels: ["Points & miles", "Adventure & outdoors", "Beach & islands",
               "Food & wine", "Culture & city breaks", "Luxury & slow travel",
               "Budget & backpacking"],
      values: [22, 18, 16, 13, 12, 10, 9],
    }],
    {
      x: M, y: 3.50, w: 7.3, h: 3.46,
      barDir: "bar", barGrouping: "clustered", barGapWidthPct: 42,
      chartColors: [TERRA],
      showTitle: false, showLegend: false,
      showValue: true, dataLabelPosition: "outEnd",
      dataLabelFormatCode: '0"%"', dataLabelFontFace: BODY,
      dataLabelFontSize: 11, dataLabelColor: INK,
      catAxisLabelFontFace: BODY, catAxisLabelFontSize: 11.5, catAxisLabelColor: INK_MID,
      catAxisLineShow: false, catGridLine: { style: "none" },
      valAxisHidden: true, valGridLine: { style: "none" },
      valAxisMaxVal: 26,
      plotArea: { fill: { color: PAPER } }, chartArea: { fill: { color: PAPER } },
    }
  );

  const counts = [
    ["Points & miles", "13,200"], ["Adventure & outdoors", "10,800"],
    ["Beach & islands", "9,600"], ["Food & wine", "7,800"],
    ["Culture & city breaks", "7,200"], ["Luxury & slow travel", "6,000"],
    ["Budget & backpacking", "5,400"],
  ];
  s.addShape(pres.ShapeType.roundRect, {
    x: 8.28, y: 3.50, w: 4.43, h: 3.46, rectRadius: 0.03,
    fill: { color: SAND }, line: { type: "none" },
  });
  s.addText("SUBSCRIBERS PER SEGMENT", {
    x: 8.58, y: 3.70, w: 3.9, h: 0.26, fontFace: BODY, fontSize: 9.5,
    bold: true, charSpacing: 2, color: GOLD_DK, margin: 0,
  });
  let cy = 4.04;
  counts.forEach(([k, v], i) => {
    s.addText(k, { x: 8.58, y: cy, w: 2.7, h: 0.28, fontFace: BODY, fontSize: 11.5,
      color: i === 0 ? INK : INK_MID, bold: i === 0, margin: 0, valign: "middle" });
    s.addText(v, { x: 11.3, y: cy, w: 1.16, h: 0.28, fontFace: BODY, fontSize: 11.5,
      color: i === 0 ? TERRA_DK : INK, bold: i === 0, align: "right", margin: 0, valign: "middle" });
    cy += 0.405;
  });

  s.addNotes("Points & miles is the premium slot — 13,200 financially engaged names, and the segment card issuers pay most for.");
}

/* ══════════════ 7 · SEGMENTATION — THE OTHER CUTS ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: SAND };
  sectionHead(s, "5", "Segmentation");
  title(s, "Four more ways to cut the list — layered, or bought on their own.");

  /* Trip stage — stat row */
  s.addText("BY TRIP STAGE — BEHAVIOURAL", {
    x: M, y: 2.76, w: 11.9, h: 0.26, fontFace: BODY, fontSize: 9.5, bold: true,
    charSpacing: 2, color: GOLD_DK, margin: 0,
  });
  const stages = [
    ["45%", "Dreaming", "no dates yet"],
    ["32%", "Planning", "booking in 30–90 days"],
    ["15%", "Booking", "in-market now"],
    ["8%", "Just back", "reviewing, resharing"],
  ];
  const tw = (12.09 - 0.9) / 4;
  let tx = M;
  stages.forEach(([fig, name, note], i) => {
    s.addShape(pres.ShapeType.roundRect, {
      x: tx, y: 3.08, w: tw, h: 1.32, rectRadius: 0.03,
      fill: { color: i === 1 || i === 2 ? NAVY : PAPER }, line: { type: "none" },
    });
    const hot = i === 1 || i === 2;
    s.addText(fig, { x: tx + 0.24, y: 3.22, w: tw - 0.48, h: 0.54, fontFace: DISPLAY,
      fontSize: 28, bold: true, color: hot ? GOLD : INK, margin: 0 });
    s.addText(name, { x: tx + 0.24, y: 3.76, w: tw - 0.48, h: 0.26, fontFace: BODY,
      fontSize: 12.5, bold: true, color: hot ? D_INK : INK, margin: 0 });
    s.addText(note, { x: tx + 0.24, y: 4.02, w: tw - 0.48, h: 0.26, fontFace: BODY,
      fontSize: 10.5, color: hot ? D_SOFT : INK_SOFT, margin: 0 });
    tx += tw + 0.3;
  });

  /* Region intent — chart */
  s.addText("BY REGION INTENT", {
    x: M, y: 4.66, w: 5.6, h: 0.26, fontFace: BODY, fontSize: 9.5, bold: true,
    charSpacing: 2, color: GOLD_DK, margin: 0,
  });
  s.addChart(
    pres.ChartType.bar,
    [{
      name: "Region intent",
      labels: ["Europe", "Asia", "Americas", "Africa", "Middle East & Pacific", "Open to anywhere"],
      values: [34, 22, 19, 11, 8, 6],
    }],
    {
      x: M - 0.1, y: 4.90, w: 5.9, h: 2.30,
      barDir: "bar", barGrouping: "clustered", barGapWidthPct: 40,
      chartColors: [GOLD_DK],
      showTitle: false, showLegend: false,
      showValue: true, dataLabelPosition: "outEnd",
      dataLabelFormatCode: '0"%"', dataLabelFontFace: BODY,
      dataLabelFontSize: 10.5, dataLabelColor: INK,
      catAxisLabelFontFace: BODY, catAxisLabelFontSize: 10.5, catAxisLabelColor: INK_MID,
      catAxisLineShow: false, catGridLine: { style: "none" },
      valAxisHidden: true, valGridLine: { style: "none" }, valAxisMaxVal: 40,
      plotArea: { fill: { color: SAND } }, chartArea: { fill: { color: SAND } },
    }
  );

  /* Engagement + geo */
  s.addText("BY ENGAGEMENT", {
    x: 6.9, y: 4.66, w: 5.6, h: 0.26, fontFace: BODY, fontSize: 9.5, bold: true,
    charSpacing: 2, color: GOLD_DK, margin: 0,
  });
  s.addText(
    [
      { text: "Opens every issue 28%", options: { bullet: true, breakLine: true, bold: true, color: INK } },
      { text: "Most issues 34%", options: { bullet: true, breakLine: true } },
      { text: "Occasional 26%", options: { bullet: true, breakLine: true } },
      { text: "Re-engagement 12%", options: { bullet: true, breakLine: true } },
      { text: "A frequency-capped campaign can buy the top two tiers only.", options: { italic: true, color: INK_SOFT } },
    ],
    { x: 6.9, y: 4.96, w: 5.6, h: 1.2, fontFace: BODY, fontSize: 11.5,
      color: INK_MID, margin: 0, paraSpaceAfter: 4 }
  );
  s.addText("BY GEOGRAPHY", {
    x: 6.9, y: 6.20, w: 5.6, h: 0.26, fontFace: BODY, fontSize: 9.5, bold: true,
    charSpacing: 2, color: GOLD_DK, margin: 0,
  });
  s.addText("United States 58%  ·  United Kingdom 12%  ·  European Union 10%  ·  Canada 8%  ·  Australia & New Zealand 7%  ·  rest of world 5%.  About two thirds of opens are on mobile.", {
    x: 6.9, y: 6.48, w: 5.6, h: 0.74, fontFace: BODY, fontSize: 11.5,
    color: INK_MID, margin: 0, lineSpacing: 16,
  });

  s.addNotes("Trip stage is the cut most buyers have never been offered. Planning plus booking is 47% of the list — that is the performance audience.");
}

/* ══════════════ 8 · NEWSLETTER RATES ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: PAPER };
  sectionHead(s, "6", "Inventory & rates");
  title(s, "Newsletter placements, and what each one costs.");
  s.addText("Rate card 2026, before volume and annual terms.", {
    x: M, y: 2.54, w: 11.9, h: 0.3, fontFace: BODY, fontSize: 13,
    color: INK_SOFT, italic: true, margin: 0,
  });

  const head = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 9.5, bold: true,
    charSpacing: 1.8, color: GOLD_DK, fill: { color: PAPER }, valign: "bottom" } });
  const unit = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 12, bold: true, color: INK } });
  const spec = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 11, color: INK_SOFT } });
  const rate = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 12, bold: true,
    color: TERRA_DK, align: "right" } });

  s.addTable(
    [
      [head("PLACEMENT"), head("WHAT IT IS"), head("RATE")],
      [unit("Primary sponsorship"), spec("Top-of-issue billboard, logo lockup, 40 words, one link. One per issue."), rate("$2,500 / issue")],
      [unit("Native dispatch"), spec("In-body sponsored block — image, 80–100 words in house voice, labelled. One per issue."), rate("$1,500 / issue")],
      [unit("Text link block"), spec("Three-line classified in the resources footer."), rate("$600 / issue")],
      [unit("Segment-targeted send"), spec("Your creative to one declared segment. 5,000-name minimum."), rate("$900 / 10k")],
      [unit("Points & Miles solo send"), spec("Standalone email to the award-travel segment (13,200). Card and financial services."), rate("$2,000")],
      [unit("Dedicated solo send"), spec("Standalone email, full list, single advertiser."), rate("$4,500")],
      [unit("Presenting sponsor"), spec("Primary slot in all 13 issues of a quarter, plus one solo send."), rate("$26,000 / quarter")],
    ],
    {
      x: M, y: 3.02, w: 12.09, colW: [3.0, 6.59, 2.5],
      border: { type: "solid", pt: 0.5, color: SAND_DK },
      rowH: 0.48, valign: "middle", margin: [0.06, 0.14, 0.06, 0.14],
      fill: { color: PAPER },
    }
  );
  s.addNotes("The presenting sponsor package is the one to push — it is 13 issues of continuity and about 20% off the à la carte primary rate.");
}

/* ══════════════ 9 · SITE & COMMERCE RATES ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: PAPER };
  sectionHead(s, "6", "Inventory & rates");
  title(s, "Site display, content ownership, and commerce.");
  s.addText("Programmatic buyers can transact the display inventory directly through PGAM's own seats as a private marketplace deal.", {
    x: M, y: 2.54, w: 11.9, h: 0.3, fontFace: BODY, fontSize: 13,
    color: INK_SOFT, italic: true, margin: 0,
  });

  const head = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 9.5, bold: true,
    charSpacing: 1.8, color: GOLD_DK, fill: { color: PAPER }, valign: "bottom" } });
  const grp = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 9.5, bold: true,
    charSpacing: 1.8, color: "FFFFFF", fill: { color: NAVY } } });
  const blank = () => ({ text: "", options: { fill: { color: NAVY } } });
  const unit = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 11.5, bold: true, color: INK } });
  const spec = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 10.5, color: INK_SOFT } });
  const rate = (t) => ({ text: t, options: { fontFace: BODY, fontSize: 11.5, bold: true,
    color: TERRA_DK, align: "right" } });

  s.addTable(
    [
      [head("PLACEMENT"), head("WHAT IT IS"), head("RATE")],
      [grp("DISPLAY"), blank(), blank()],
      [unit("Run of site"), spec("Responsive leaderboard and in-content 300×250. Max three in-content units per article."), rate("$8–12 CPM")],
      [unit("High impact"), spec("Sticky 300×600 half-page, desktop sidebar. Highest viewability on the property."), rate("$15–22 CPM")],
      [unit("Mobile anchor"), spec("320×50 sticky, five-second delay to protect the reading experience."), rate("$10–14 CPM")],
      [unit("Trip Planner results"), spec("Sponsored slot inside a generated itinerary — destination, dates, and budget declared."), rate("$18–25 CPM")],
      [grp("CONTENT & OWNERSHIP"), blank(), blank()],
      [unit("Sponsored guide"), spec("1,500+ words to house editorial standard, labelled, evergreen, internally linked."), rate("$1,500–3,500")],
      [unit("Region hub sponsorship"), spec("Presenting brand on a region hub and its guides for 30 days."), rate("$3,000 / month")],
      [unit("Homepage takeover"), spec("Hero surround and trust-bar placement, 24 hours, one advertiser."), rate("$2,500 / day")],
      [grp("COMMERCE"), blank(), blank()],
      [unit("Commerce partnership"), spec("Preferred placement in booking CTAs, comparison tables, and planner output."), rate("Custom")],
      [unit("Launch package"), spec("Four newsletter primaries, one region hub month, one sponsored guide. Value $16,500."), rate("$14,000")],
    ],
    {
      x: M, y: 3.02, w: 12.09, colW: [2.9, 6.69, 2.5],
      border: { type: "solid", pt: 0.5, color: SAND_DK },
      rowH: 0.295, valign: "middle", margin: [0.04, 0.14, 0.04, 0.14],
      fill: { color: PAPER },
    }
  );
  s.addNotes("Trip Planner results is the premium CPM. If a buyer balks at it, that is the slot to demo live rather than discount.");
}

/* ══════════════ 10 · STANDARDS ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: SAND };
  sectionHead(s, "7", "Standards");
  title(s, "Clean supply, clean pages, clean disclosure.");
  lede(s, "Most of what depresses performance in this category is self-inflicted: too many units, unverified supply, and disclosure treated as an afterthought. Here is what we hold to.", false, 2.54, 11.9);

  const rules = [
    ["Verified, direct supply", "Authorised seats are declared DIRECT in ads.txt and machine-checked every day. A missing or downgraded line pages an engineer the same morning."],
    ["Owned and operated", "One publisher, one domain, no arbitraged or resold traffic in the path. You know exactly what you bought."],
    ["Three units, hard cap", "No more than three in-content ads per article, none in the first 200 words, never two stacked. Density is capped to protect viewability."],
    ["No dark patterns", "No pop-ups, pop-unders, interstitials, or auto-playing video. Core Web Vitals are a revenue input here, not a compliance chore."],
    ["Labelled, always", "Sponsored labels above the fold, paid links marked, affiliate relationships disclosed to FTC standard on every page carrying them."],
    ["Editorial independence", "Sponsorship never buys a ranking, a rewrite, or the removal of a recommendation. That is why a click here is worth buying."],
  ];
  const rw = (12.09 - 0.6) / 3, rh = 1.62;
  rules.forEach(([h, b], i) => {
    const col = i % 3, row = Math.floor(i / 3);
    const rx = M + col * (rw + 0.3), ry = 3.50 + row * (rh + 0.26);
    s.addShape(pres.ShapeType.roundRect, {
      x: rx, y: ry, w: rw, h: rh, rectRadius: 0.03,
      fill: { color: PAPER }, line: { type: "none" },
    });
    s.addShape(pres.ShapeType.ellipse, {
      x: rx + 0.26, y: ry + 0.24, w: 0.26, h: 0.26,
      fill: { color: TERRA_DK }, line: { type: "none" },
    });
    s.addText(h, { x: rx + 0.62, y: ry + 0.2, w: rw - 0.88, h: 0.32, fontFace: DISPLAY,
      fontSize: 13.5, bold: true, color: INK, margin: 0, valign: "middle" });
    s.addText(b, { x: rx + 0.26, y: ry + 0.6, w: rw - 0.52, h: 0.9, fontFace: BODY,
      fontSize: 10.5, color: INK_SOFT, margin: 0, lineSpacing: 14.5 });
  });

  s.addNotes("The ads.txt verification point lands hardest with programmatic buyers — it is an automated daily check, not a policy statement.");
}

/* ══════════════ 11 · CLOSE ══════════════ */
{
  const s = pres.addSlide();
  s.background = { color: NAVY };
  sectionHead(s, "8", "Next", true);
  title(s, "Let's build the plan against a segment, not a guess.", true);

  const steps = [
    "Tell us the audience you actually want — a region, an interest, a trip stage, or a combination. We size it against the list and the site in a day.",
    "We come back with a costed plan, availability, and the traffic and viewability detail under NDA.",
    "Test on a single issue or a two-week flight. Full delivery, click, and CTR reporting within 72 hours of the last send.",
  ];
  let sy = 2.80;
  steps.forEach((t, i) => {
    s.addShape(pres.ShapeType.ellipse, {
      x: M, y: sy, w: 0.42, h: 0.42,
      fill: { color: TERRA_LT }, line: { type: "none" },
    });
    s.addText(String(i + 1), { x: M, y: sy, w: 0.42, h: 0.42, fontFace: BODY,
      fontSize: 13, bold: true, color: NAVY, align: "center", valign: "middle", margin: 0 });
    s.addText(t, { x: M + 0.68, y: sy - 0.04, w: 7.3, h: 0.9, fontFace: BODY,
      fontSize: 13, color: D_INK, margin: 0, lineSpacing: 19 });
    sy += 1.06;
  });

  s.addShape(pres.ShapeType.roundRect, {
    x: 8.6, y: 2.80, w: 4.11, h: 2.5, rectRadius: 0.03,
    fill: { color: NAVY_DEEP }, line: { type: "none" },
  });
  s.addText("GET IN TOUCH", { x: 8.9, y: 3.04, w: 3.5, h: 0.26, fontFace: BODY,
    fontSize: 9.5, bold: true, charSpacing: 2, color: GOLD, margin: 0 });
  s.addText("Destination.com", { x: 8.9, y: 3.36, w: 3.5, h: 0.36, fontFace: DISPLAY,
    fontSize: 19, bold: true, color: D_INK, margin: 0 });
  s.addText(
    [
      { text: "Advertising & Partnerships", options: { breakLine: true, color: D_SOFT } },
      { text: "PGAM Media", options: { breakLine: true, color: D_SOFT } },
      { text: "info@pgammedia.com", options: { breakLine: true, color: D_INK, bold: true,
          hyperlink: { url: "mailto:info@pgammedia.com?subject=Destination.com%20enquiry" } } },
      { text: "pgammedia.com", options: { color: D_SOFT } },
    ],
    { x: 8.9, y: 3.80, w: 3.5, h: 1.3, fontFace: BODY, fontSize: 12, margin: 0, lineSpacing: 19 }
  );

  s.addText("Newsletter subscriber counts, open and click rates, and all segment splits in this document are internal projections for planning purposes and are not independently audited. Site traffic, viewability, and geography detail are shared on request under NDA. Rate card pricing is indicative and excludes agency commission, volume, and annual commitment terms.", {
    x: M, y: 6.0, w: 12.09, h: 0.86, fontFace: BODY, fontSize: 9.5,
    color: D_SOFT, margin: 0, lineSpacing: 13.5,
  });

  s.addNotes("Close on the segment question, not on the rate card. The ask is 'which audience do you want', which is a question a buyer can answer on the call.");
}

const out = process.argv[2] || "destination-com-media-kit.pptx";
pres.writeFile({ fileName: out }).then(() => console.log("wrote " + out));
