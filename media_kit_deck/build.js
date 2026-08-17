// Destination.com Media Kit — advertiser-facing deck
const pptxgen = require("pptxgenjs");

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE"; // 13.333 x 7.5
const SW = 13.333, SH = 7.5;
pres.author = "PGAM Media";
pres.title = "Destination.com Media Kit";
pres.company = "PGAM Media";

// ----------------------------------------------------------------
// EDITABLE FIGURES
// Every number a seller might need to update lives here. Values marked
// PLACEHOLDER are industry-plausible defaults, not measured results —
// swap them for real ESP / analytics data before sending externally.
// ----------------------------------------------------------------
const N = {
  subscribers: "50,000+",     // confirmed
  openRate: "42%",            // PLACEHOLDER — pull from ESP 90-day average
  ctr: "3.8%",                // PLACEHOLDER — pull from ESP 90-day average
  sendsPerWeek: "3x",         // PLACEHOLDER — confirm cadence
  monthlyReaders: "180K+",    // PLACEHOLDER — confirm from site analytics
  listGrowth: "+8% MoM",      // PLACEHOLDER — confirm from ESP
  usShare: "68%",             // PLACEHOLDER — confirm from ESP geo report
  quarter: "Q4 2026",
};

// Palette — Destination.com brand (ocean blue / teal / terracotta / gold on cream)
const C = {
  ink: "0E2F42",        // deep navy — dark slide grounds
  inkDeep: "081F2E",
  blue: "1B6CA8",       // brand primary
  blueDeep: "0E4D6F",
  blueSoft: "D6E6F2",
  teal: "0D9B76",       // brand secondary
  terra: "C4703E",      // brand accent
  gold: "F4A124",
  goldSoft: "FDEBCB",
  cream: "FAF7F2",
  creamDark: "F0EBE3",
  white: "FFFFFF",
  text: "1A1A1A",
  muted: "6B7A85",
  divider: "E0DAD1",
};

const F = { head: "Georgia", body: "Calibri" };

const TOTAL = 20;
let page = 0;

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------
function addFooter(slide, dark) {
  page += 1;
  const line = dark ? C.blueDeep : C.divider;
  const ink = dark ? C.blueSoft : C.muted;
  slide.addShape(pres.shapes.LINE, {
    x: 0.5, y: SH - 0.5, w: SW - 1.0, h: 0,
    line: { color: line, width: 0.75 },
  });
  slide.addText("DESTINATION.COM  ·  MEDIA KIT  ·  " + N.quarter, {
    x: 0.5, y: SH - 0.41, w: 8, h: 0.3,
    fontFace: F.body, fontSize: 9, color: ink, charSpacing: 2, margin: 0,
  });
  slide.addText(`${page} / ${TOTAL}`, {
    x: SW - 1.5, y: SH - 0.41, w: 1.0, h: 0.3,
    fontFace: F.body, fontSize: 9, color: ink, align: "right", margin: 0,
  });
}

// Standard content slide: cream ground, kicker rule, headline, optional deck
function contentSlide(kicker, title, sub) {
  const s = pres.addSlide();
  s.background = { color: C.cream };
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 0.58, w: 0.55, h: 0.045,
    fill: { color: C.terra }, line: { type: "none" },
  });
  s.addText(kicker, {
    x: 1.2, y: 0.43, w: 9, h: 0.32,
    fontFace: F.body, fontSize: 10, color: C.terra,
    bold: true, charSpacing: 4, margin: 0,
  });
  s.addText(title, {
    x: 0.5, y: 0.82, w: SW - 1.0, h: 0.62,
    fontFace: F.head, fontSize: 29, color: C.ink, bold: true, margin: 0,
  });
  if (sub) {
    s.addText(sub, {
      x: 0.5, y: 1.44, w: SW - 1.6, h: 0.42,
      fontFace: F.body, fontSize: 13.5, color: C.muted, margin: 0,
    });
  }
  addFooter(s, false);
  return s;
}

// White card with a colored top rule
function card(s, x, y, w, h, accent) {
  s.addShape(pres.shapes.RECTANGLE, {
    x, y, w, h,
    fill: { color: C.white },
    line: { color: C.divider, width: 0.5 },
    shadow: { type: "outer", color: "000000", blur: 8, offset: 2, angle: 90, opacity: 0.06 },
  });
  if (accent) {
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w, h: 0.055,
      fill: { color: accent }, line: { type: "none" },
    });
  }
}

// Section divider on a dark ground
function divider(kicker, title, blurb) {
  const s = pres.addSlide();
  s.background = { color: C.ink };
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.18, h: SH,
    fill: { color: C.gold }, line: { type: "none" },
  });
  s.addText(kicker, {
    x: 1.1, y: 2.5, w: 9, h: 0.4,
    fontFace: F.body, fontSize: 11, color: C.gold, bold: true, charSpacing: 6, margin: 0,
  });
  s.addText(title, {
    x: 1.1, y: 2.95, w: 10.5, h: 1.3,
    fontFace: F.head, fontSize: 44, color: C.white, bold: true,
    lineSpacingMultiple: 1.05, margin: 0,
  });
  s.addText(blurb, {
    x: 1.1, y: 4.35, w: 9.5, h: 0.9,
    fontFace: F.body, fontSize: 15, color: C.blueSoft, lineSpacingMultiple: 1.3, margin: 0,
  });
  addFooter(s, true);
  return s;
}

// Simple table drawn as shapes — keeps typography under our control
function priceTable(s, x, y, w, rows, colW, headAccent) {
  const rowH = 0.42;
  rows.forEach((r, i) => {
    const ry = y + i * rowH;
    const isHead = i === 0;
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: ry, w, h: rowH,
      fill: { color: isHead ? headAccent : (i % 2 ? C.white : C.creamDark) },
      line: { type: "none" },
    });
    let cx = x + 0.16;
    r.forEach((cell, j) => {
      s.addText(cell, {
        x: cx, y: ry, w: colW[j] - 0.2, h: rowH,
        fontFace: F.body, fontSize: isHead ? 10 : 11.5,
        color: isHead ? C.white : C.text,
        bold: isHead || j === 0,
        charSpacing: isHead ? 1.5 : 0,
        align: j === 0 ? "left" : (j === r.length - 1 ? "right" : "left"),
        valign: "middle", margin: 0,
      });
      cx += colW[j];
    });
  });
}

// ================================================================
// 1 — Cover
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.inkDeep };

  s.addShape(pres.shapes.RECTANGLE, {
    x: SW - 4.6, y: 0, w: 4.6, h: SH,
    fill: { color: C.ink }, line: { type: "none" },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: SW - 4.6, y: 0, w: 0.08, h: SH,
    fill: { color: C.gold }, line: { type: "none" },
  });

  s.addText("DESTINATION.COM  ·  MEDIA KIT", {
    x: 0.85, y: 1.35, w: 8, h: 0.4,
    fontFace: F.body, fontSize: 11, color: C.gold, bold: true, charSpacing: 6, margin: 0,
  });
  s.addText("Reach travelers\nbefore they book.", {
    x: 0.85, y: 1.85, w: 8.0, h: 2.3,
    fontFace: F.head, fontSize: 52, color: C.white, bold: true,
    lineSpacingMultiple: 1.05, margin: 0,
  });
  s.addText(
    "Destination.com is a premium travel media and intent-driven audience " +
    "platform. Our readers arrive with a trip in mind — and a decision still open.",
    {
      x: 0.85, y: 4.35, w: 7.6, h: 1.3,
      fontFace: F.body, fontSize: 16, color: C.blueSoft, lineSpacingMultiple: 1.35, margin: 0,
    }
  );
  s.addText("Advertising & partnerships  ·  " + N.quarter, {
    x: 0.85, y: SH - 0.95, w: 8, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.muted, charSpacing: 3, margin: 0,
  });

  s.addText("THE AUDIENCE", {
    x: SW - 4.25, y: 1.35, w: 3.8, h: 0.35,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 8, margin: 0,
  });
  const facts = [
    [N.subscribers, "Newsletter subscribers"],
    [N.openRate, "Average open rate"],
    ["100%", "High travel intent"],
    ["5", "Journey stages covered"],
  ];
  facts.forEach((f, i) => {
    const y = 1.9 + i * 1.15;
    s.addShape(pres.shapes.RECTANGLE, {
      x: SW - 4.25, y, w: 3.8, h: 0.98,
      fill: { color: C.inkDeep }, line: { color: C.blueDeep, width: 0.75 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: SW - 4.25, y, w: 0.05, h: 0.98,
      fill: { color: C.gold }, line: { type: "none" },
    });
    s.addText(f[0], {
      x: SW - 4.05, y: y + 0.09, w: 1.75, h: 0.8,
      fontFace: F.head, fontSize: 26, color: C.gold, bold: true, valign: "middle", margin: 0,
    });
    s.addText(f[1], {
      x: SW - 2.25, y: y + 0.09, w: 1.75, h: 0.8,
      fontFace: F.body, fontSize: 11, color: C.blueSoft, valign: "middle", margin: 0,
    });
  });
  addFooter(s, true);
}

// ================================================================
// 2 — The opportunity
// ================================================================
{
  const s = contentSlide(
    "THE OPPORTUNITY",
    "Travel advertising's hardest problem is timing.",
    "Most media reaches people who might travel someday. We reach people planning a trip right now."
  );

  const cols = [
    {
      k: "THE PROBLEM",
      t: "Broad reach wastes budget",
      b: "Mass channels sell impressions to people with no trip on the calendar. " +
         "You pay for scale and inherit the waste.",
      c: C.terra,
    },
    {
      k: "THE SIGNAL",
      t: "Intent is observable",
      b: "Readers tell us where they're going and what stage they're at — through " +
         "what they open, click, read and search.",
      c: C.blue,
    },
    {
      k: "THE PLATFORM",
      t: "Destination.com sits in the gap",
      b: "An owned newsletter, an owned site, and first-party segments that let you " +
         "buy the moment instead of the demographic.",
      c: C.teal,
    },
  ];

  cols.forEach((col, i) => {
    const x = 0.5 + i * 4.2;
    card(s, x, 2.15, 3.93, 2.55, col.c);
    s.addText(col.k, {
      x: x + 0.28, y: 2.42, w: 3.4, h: 0.28,
      fontFace: F.body, fontSize: 9.5, color: col.c, bold: true, charSpacing: 3, margin: 0,
    });
    s.addText(col.t, {
      x: x + 0.28, y: 2.72, w: 3.4, h: 0.75,
      fontFace: F.head, fontSize: 17.5, color: C.ink, bold: true,
      lineSpacingMultiple: 1.1, margin: 0,
    });
    s.addText(col.b, {
      x: x + 0.28, y: 3.55, w: 3.4, h: 1.0,
      fontFace: F.body, fontSize: 12, color: C.muted, lineSpacingMultiple: 1.25, margin: 0,
    });
  });

  // Payoff band
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 5.05, w: SW - 1.0, h: 1.65,
    fill: { color: C.ink }, line: { type: "none" },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 5.05, w: 0.07, h: 1.65,
    fill: { color: C.gold }, line: { type: "none" },
  });
  s.addText("What that means for your media plan", {
    x: 0.85, y: 5.25, w: 6.5, h: 0.32,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 3, margin: 0,
  });
  s.addText(
    "Every impression on Destination.com is served to someone who chose to read about travel today. " +
    "You are not buying an audience that skews travel — you are buying the travel decision itself, " +
    "at the exact stage where your brand can still change the outcome.",
    {
      x: 0.85, y: 5.6, w: 11.4, h: 1.0,
      fontFace: F.body, fontSize: 14.5, color: C.white, lineSpacingMultiple: 1.3, margin: 0,
    }
  );
}

// ================================================================
// 3 — At a glance
// ================================================================
{
  const s = contentSlide(
    "AT A GLANCE",
    "Destination.com by the numbers",
    "A focused, permission-based travel audience — built for engagement, not for volume."
  );

  const stats = [
    { v: N.subscribers, l: "Newsletter subscribers", sub: "Opt-in, permission-based" },
    { v: N.openRate, l: "Average open rate", sub: "Well above publisher norms" },
    { v: N.ctr, l: "Average click rate", sub: "Readers act, not just scroll" },
    { v: N.monthlyReaders, l: "Monthly site readers", sub: "Destinations, guides, deals" },
  ];
  const cardW = 3.03, gap = 0.14;
  stats.forEach((st, i) => {
    const x = 0.5 + i * (cardW + gap);
    card(s, x, 2.1, cardW, 2.05, C.blue);
    s.addText(st.v, {
      x: x + 0.22, y: 2.32, w: cardW - 0.44, h: 0.9,
      fontFace: F.head, fontSize: 40, color: C.ink, bold: true, margin: 0,
    });
    s.addText(st.l, {
      x: x + 0.22, y: 3.26, w: cardW - 0.44, h: 0.35,
      fontFace: F.body, fontSize: 12.5, color: C.ink, bold: true, margin: 0,
    });
    s.addText(st.sub, {
      x: x + 0.22, y: 3.6, w: cardW - 0.44, h: 0.4,
      fontFace: F.body, fontSize: 10, color: C.muted, margin: 0,
    });
  });

  const strip = [
    [N.sendsPerWeek, "Sends per week"],
    [N.listGrowth, "List growth"],
    [N.usShare, "US-based readers"],
    ["12+", "Content categories"],
    ["1st-party", "Data, fully owned"],
  ];
  strip.forEach((t, i) => {
    const x = 0.5 + i * 2.51;
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: 4.4, w: 2.37, h: 1.0,
      fill: { color: C.white }, line: { color: C.divider, width: 0.5 },
    });
    s.addText(t[0], {
      x: x + 0.18, y: 4.52, w: 2.0, h: 0.42,
      fontFace: F.head, fontSize: 19, color: C.blue, bold: true, margin: 0,
    });
    s.addText(t[1], {
      x: x + 0.18, y: 4.94, w: 2.0, h: 0.32,
      fontFace: F.body, fontSize: 10.5, color: C.muted, margin: 0,
    });
  });

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 5.65, w: SW - 1.0, h: 1.05,
    fill: { color: C.goldSoft }, line: { type: "none" },
  });
  s.addText(
    "Our subscribers didn't stumble in. They asked for travel inspiration, deals and planning help — " +
    "which is why they open, click, and convert at rates a general-interest list can't reach.",
    {
      x: 0.85, y: 5.82, w: 11.4, h: 0.75,
      fontFace: F.body, fontSize: 13.5, color: C.ink, italic: true,
      lineSpacingMultiple: 1.25, margin: 0,
    }
  );
}

// ================================================================
// 4 — Audience profile
// ================================================================
{
  const s = contentSlide(
    "THE AUDIENCE",
    "Who reads Destination.com",
    "High-intent travelers actively researching where to go, where to stay, and how to pay for it."
  );

  const traits = [
    { t: "Actively planning", b: "Reading destination guides, comparing itineraries, and shortlisting trips — usually 30 to 120 days out from booking.", c: C.blue },
    { t: "Decision-stage, not idle", b: "Clicking through to hotels, flights, tours and deals. They treat our newsletter as a planning tool.", c: C.teal },
    { t: "Cross-category spenders", b: "One trip is a hotel, a flight, a card swipe, insurance, luggage, transfers, tickets and meals.", c: C.terra },
    { t: "Deal-aware, not deal-only", b: "They respond to value and price drops, but they buy on trust, editorial credibility and fit.", c: C.gold },
    { t: "Repeat travelers", b: "Multiple trips a year across beach, city, family and long-haul — so the relationship compounds.", c: C.blue },
    { t: "Mobile-first readers", b: "Opened on phones, in-market, often mid-research — the moment where recall becomes a click.", c: C.teal },
  ];

  traits.forEach((tr, i) => {
    const col = i % 3, row = Math.floor(i / 3);
    const x = 0.5 + col * 4.2;
    const y = 2.1 + row * 2.35;
    card(s, x, y, 3.93, 2.1, tr.c);
    s.addText(tr.t, {
      x: x + 0.26, y: y + 0.3, w: 3.45, h: 0.4,
      fontFace: F.head, fontSize: 16.5, color: C.ink, bold: true, margin: 0,
    });
    s.addText(tr.b, {
      x: x + 0.26, y: y + 0.78, w: 3.45, h: 1.15,
      fontFace: F.body, fontSize: 12, color: C.muted, lineSpacingMultiple: 1.3, margin: 0,
    });
  });
}

// ================================================================
// 5 — The travel journey
// ================================================================
{
  const s = contentSlide(
    "FULL-JOURNEY REACH",
    "Five stages. One audience. Every one of them buyable.",
    "Travel isn't a single decision — it's a sequence. We can reach the same reader at each step."
  );

  const stages = [
    { n: "01", t: "Inspiration", b: "\"Where should we go?\"", d: "Destination features, listicles, photo-led discovery, trending-now content.", c: C.blue },
    { n: "02", t: "Research", b: "\"Is this trip right for us?\"", d: "Guides, comparisons, best-time-to-visit, budget breakdowns, safety and visas.", c: C.teal },
    { n: "03", t: "Planning", b: "\"How do we make it work?\"", d: "Itineraries, packing, points & miles, insurance, logistics and timing.", c: C.terra },
    { n: "04", t: "Booking", b: "\"Where do I book it?\"", d: "Deal features, deep links, hotel and flight placements, limited-time offers.", c: C.gold },
    { n: "05", t: "Post-trip", b: "\"What's next?\"", d: "Loyalty, reviews, gear upgrades, next-destination nurture and re-engagement.", c: C.blue },
  ];

  // Connector rail
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.75, y: 2.72, w: SW - 1.5, h: 0.05,
    fill: { color: C.divider }, line: { type: "none" },
  });

  stages.forEach((st, i) => {
    const x = 0.5 + i * 2.51;
    // Stage node
    s.addShape(pres.shapes.OVAL, {
      x: x + 0.9, y: 2.45, w: 0.6, h: 0.6,
      fill: { color: st.c }, line: { color: C.cream, width: 2 },
    });
    s.addText(st.n, {
      x: x + 0.9, y: 2.45, w: 0.6, h: 0.6,
      fontFace: F.body, fontSize: 12, color: C.white, bold: true,
      align: "center", valign: "middle", margin: 0,
    });
    card(s, x, 3.3, 2.37, 3.05, null);
    s.addText(st.t, {
      x: x + 0.2, y: 3.52, w: 2.0, h: 0.38,
      fontFace: F.head, fontSize: 17, color: C.ink, bold: true, margin: 0,
    });
    s.addText(st.b, {
      x: x + 0.2, y: 3.92, w: 2.0, h: 0.6,
      fontFace: F.body, fontSize: 11.5, color: st.c, italic: true,
      lineSpacingMultiple: 1.2, margin: 0,
    });
    s.addShape(pres.shapes.LINE, {
      x: x + 0.2, y: 4.6, w: 1.0, h: 0,
      line: { color: C.divider, width: 1 },
    });
    s.addText(st.d, {
      x: x + 0.2, y: 4.72, w: 2.0, h: 1.45,
      fontFace: F.body, fontSize: 11, color: C.muted, lineSpacingMultiple: 1.25, margin: 0,
    });
  });

  s.addText(
    "Buy one stage to solve a specific problem — or sequence all five and own the trip from daydream to departure.",
    {
      x: 0.5, y: 6.5, w: SW - 1.0, h: 0.4,
      fontFace: F.body, fontSize: 13, color: C.ink, bold: true, align: "center", margin: 0,
    }
  );
}

// ================================================================
// 6 — Divider: Newsletter
// ================================================================
divider(
  "01  ·  THE FLAGSHIP",
  "The Newsletter",
  N.subscribers + " opt-in travel intenders, delivered straight to the inbox — the highest-attention " +
  "environment we own, and the one advertisers ask for first."
);

// ================================================================
// 7 — Newsletter overview
// ================================================================
{
  const s = contentSlide(
    "THE NEWSLETTER",
    "Why the inbox outperforms everything else",
    "No algorithm between you and the reader. No feed to scroll past. One send, one audience, full attention."
  );

  const points = [
    { t: "Owned, not rented", b: "We control the list, the data and the delivery. No platform can throttle your reach or change the rules mid-flight.", c: C.blue },
    { t: "Undivided attention", b: "An email is a single-column, one-thing-at-a-time environment. Your placement isn't competing with nine other units.", c: C.teal },
    { t: "Trusted context", b: "Readers subscribed for our recommendations. A brand featured alongside our editorial inherits that credibility.", c: C.terra },
    { t: "Measurable, fast", b: "Opens, clicks, click-through destinations and post-click behavior — reported per placement, per send.", c: C.gold },
  ];

  points.forEach((p, i) => {
    const y = 2.15 + i * 1.15;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: 7.4, h: 1.0,
      fill: { color: C.white }, line: { color: C.divider, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: 0.06, h: 1.0,
      fill: { color: p.c }, line: { type: "none" },
    });
    s.addText(p.t, {
      x: 0.78, y: y + 0.13, w: 6.9, h: 0.34,
      fontFace: F.head, fontSize: 16, color: C.ink, bold: true, margin: 0,
    });
    s.addText(p.b, {
      x: 0.78, y: y + 0.47, w: 6.9, h: 0.48,
      fontFace: F.body, fontSize: 11.5, color: C.muted, lineSpacingMultiple: 1.2, margin: 0,
    });
  });

  // Right rail — performance panel
  s.addShape(pres.shapes.RECTANGLE, {
    x: 8.2, y: 2.15, w: 4.63, h: 4.55,
    fill: { color: C.ink }, line: { type: "none" },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 8.2, y: 2.15, w: 4.63, h: 0.07,
    fill: { color: C.gold }, line: { type: "none" },
  });
  s.addText("PERFORMANCE SNAPSHOT", {
    x: 8.5, y: 2.45, w: 4.1, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 3, margin: 0,
  });
  const perf = [
    [N.openRate, "Average open rate"],
    [N.ctr, "Average click-through rate"],
    [N.sendsPerWeek, "Sends per week"],
    [N.listGrowth, "Subscriber growth"],
  ];
  perf.forEach((p, i) => {
    const y = 2.9 + i * 0.85;
    // Label above, value below — keeps long values like "+8% MoM" on one line
    s.addText(p[1], {
      x: 8.5, y, w: 4.05, h: 0.26,
      fontFace: F.body, fontSize: 10.5, color: C.blueSoft, margin: 0,
    });
    s.addText(p[0], {
      x: 8.5, y: y + 0.22, w: 4.05, h: 0.45,
      fontFace: F.head, fontSize: 26, color: C.white, bold: true, margin: 0,
    });
    if (i < 3) {
      s.addShape(pres.shapes.LINE, {
        x: 8.5, y: y + 0.72, w: 4.05, h: 0,
        line: { color: C.blueDeep, width: 0.75 },
      });
    }
  });
  s.addText("Reported per placement, per send.", {
    x: 8.5, y: 6.25, w: 4.1, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.muted, italic: true, margin: 0,
  });
}

// ================================================================
// 8 — Newsletter sponsorship opportunities
// ================================================================
{
  const s = contentSlide(
    "NEWSLETTER SPONSORSHIPS",
    "Six ways to show up in the inbox",
    "From full ownership of a send to a native mention inside the editorial our readers open for."
  );

  const units = [
    { t: "Dedicated Send", sov: "100% SOV", b: "The entire email is yours. Your creative, your message, your CTA — sent to the full list or a chosen segment.", c: C.blue },
    { t: "Sponsored Placement", sov: "TOP / MID SLOT", b: "A premium banner-and-copy unit in the flow of the newsletter, above or beside our lead story.", c: C.teal },
    { t: "Native Content", sov: "EDITORIAL VOICE", b: "Written in our voice, clearly labeled, and built to be read — not skipped. Highest engagement of any unit.", c: C.terra },
    { t: "Destination Spotlight", sov: "TOURISM BOARDS", b: "A full editorial feature on your destination: what to do, when to go, where to stay, why now.", c: C.gold },
    { t: "Travel Deal Feature", sov: "CONVERSION-LED", b: "Your offer in our deals module, framed as a genuine find. Built for click-through and booking.", c: C.blue },
    { t: "Segmented Campaign", sov: "PRECISION REACH", b: "Send only to the subset that matters — beach intenders, luxury travelers, a specific origin market.", c: C.teal },
  ];

  units.forEach((u, i) => {
    const col = i % 3, row = Math.floor(i / 3);
    const x = 0.5 + col * 4.2;
    const y = 2.1 + row * 2.35;
    card(s, x, y, 3.93, 2.1, u.c);
    s.addText(u.sov, {
      x: x + 0.26, y: y + 0.24, w: 3.45, h: 0.26,
      fontFace: F.body, fontSize: 8.5, color: u.c, bold: true, charSpacing: 2.5, margin: 0,
    });
    s.addText(u.t, {
      x: x + 0.26, y: y + 0.52, w: 3.45, h: 0.42,
      fontFace: F.head, fontSize: 17.5, color: C.ink, bold: true, margin: 0,
    });
    s.addText(u.b, {
      x: x + 0.26, y: y + 0.98, w: 3.45, h: 0.95,
      fontFace: F.body, fontSize: 11.5, color: C.muted, lineSpacingMultiple: 1.25, margin: 0,
    });
  });
}

// ================================================================
// 9 — Segmentation & targeting
// ================================================================
{
  const s = contentSlide(
    "TARGETING",
    "Segment the list. Buy only the travelers you want.",
    "Our first-party data lets you narrow by who they are, where they're going, and how close they are to booking."
  );

  const groups = [
    {
      h: "WHO THEY ARE", c: C.blue,
      items: ["Geography & origin market", "Family travel", "Luxury travel", "Budget travel", "Solo & couples", "Repeat / frequent travelers"],
    },
    {
      h: "WHERE THEY'RE GOING", c: C.teal,
      items: ["Destination interest", "Beach & island trips", "City breaks", "Cruises", "Adventure & outdoors", "Long-haul vs. domestic"],
    },
    {
      h: "WHAT THEY'RE BUYING", c: C.terra,
      items: ["Hotels & resorts", "Flights", "Experiences & tours", "Car rental & transfers", "Insurance & protection", "Points, miles & cards"],
    },
    {
      h: "WHEN THEY'RE BUYING", c: C.gold,
      items: ["Travel intent stage", "Seasonal moments", "Booking-window timing", "Deal responders", "Recent clickers", "Re-engagement audiences"],
    },
  ];

  groups.forEach((g, i) => {
    const x = 0.5 + i * 3.14;
    card(s, x, 2.1, 2.94, 4.05, g.c);
    s.addText(g.h, {
      x: x + 0.22, y: 2.35, w: 2.5, h: 0.3,
      fontFace: F.body, fontSize: 9.5, color: g.c, bold: true, charSpacing: 2.5, margin: 0,
    });
    s.addShape(pres.shapes.LINE, {
      x: x + 0.22, y: 2.72, w: 2.5, h: 0,
      line: { color: C.divider, width: 1 },
    });
    g.items.forEach((it, j) => {
      const y = 2.85 + j * 0.5;
      s.addShape(pres.shapes.OVAL, {
        x: x + 0.24, y: y + 0.14, w: 0.09, h: 0.09,
        fill: { color: g.c }, line: { type: "none" },
      });
      s.addText(it, {
        x: x + 0.45, y, w: 2.3, h: 0.4,
        fontFace: F.body, fontSize: 11.5, color: C.text, valign: "middle", margin: 0,
      });
    });
  });

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 6.32, w: SW - 1.0, h: 0.55,
    fill: { color: C.ink }, line: { type: "none" },
  });
  s.addText(
    "Layer segments freely — e.g. \"US families researching beach trips in the next 60 days\" — and we'll build the send around it.",
    {
      x: 0.5, y: 6.32, w: SW - 1.0, h: 0.55,
      fontFace: F.body, fontSize: 12.5, color: C.white, align: "center", valign: "middle", margin: 0,
    }
  );
}

// ================================================================
// 10 — Seasonal moments
// ================================================================
{
  const s = contentSlide(
    "SEASONAL MOMENTS",
    "Buy the moment demand is already spiking",
    "Travel demand is seasonal and predictable. These are the windows where intent spikes and creative works hardest."
  );

  const seasons = [
    { q: "Q1", t: "Winter Escape & New Year Planning", m: "JAN – MAR", b: "Sun-seeking, ski, spring break booking, resolution trips. Peak research volume of the year.", c: C.blue },
    { q: "Q2", t: "Summer Booking Season", m: "APR – JUN", b: "Europe, beach and family summer trips get locked in. Highest hotel and flight conversion window.", c: C.teal },
    { q: "Q3", t: "Peak Travel & Fall Shoulder", m: "JUL – SEP", b: "In-destination spend, experiences, and early shoulder-season and fall foliage planning.", c: C.terra },
    { q: "Q4", t: "Holiday Travel & Year-Ahead", m: "OCT – DEC", b: "Holiday flights, gifting, luggage and gear, plus next-year trip dreaming and deal hunting.", c: C.gold },
  ];

  seasons.forEach((sn, i) => {
    const y = 2.15 + i * 1.12;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: SW - 1.0, h: 0.98,
      fill: { color: i % 2 ? C.white : C.creamDark }, line: { color: C.divider, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: 0.9, h: 0.98,
      fill: { color: sn.c }, line: { type: "none" },
    });
    s.addText(sn.q, {
      x: 0.5, y, w: 0.9, h: 0.98,
      fontFace: F.head, fontSize: 24, color: C.white, bold: true,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(sn.t, {
      x: 1.65, y: y + 0.15, w: 5.4, h: 0.38,
      fontFace: F.head, fontSize: 17, color: C.ink, bold: true, margin: 0,
    });
    s.addText(sn.m, {
      x: 1.65, y: y + 0.55, w: 5.4, h: 0.3,
      fontFace: F.body, fontSize: 9.5, color: sn.c, bold: true, charSpacing: 2.5, margin: 0,
    });
    s.addText(sn.b, {
      x: 7.3, y: y + 0.18, w: 5.5, h: 0.65,
      fontFace: F.body, fontSize: 12, color: C.muted, lineSpacingMultiple: 1.2, valign: "middle", margin: 0,
    });
  });

  s.addText(
    "Seasonal packages are sold ahead of the window — the best inventory goes 8 to 12 weeks before the spike.",
    {
      x: 0.5, y: 6.75, w: SW - 1.0, h: 0.35,
      fontFace: F.body, fontSize: 12, color: C.ink, bold: true, align: "center", margin: 0,
    }
  );
}

// ================================================================
// 11 — Divider: The Site
// ================================================================
divider(
  "02  ·  THE PLATFORM",
  "Destination.com",
  "Guides, destination pages, deals and points coverage — an always-on environment where readers " +
  "research the trip they're about to take."
);

// ================================================================
// 12 — Site sponsorship (editorial + placement)
// ================================================================
{
  const s = contentSlide(
    "SITE SPONSORSHIPS",
    "Own the pages travelers actually research on",
    "Editorial, placement and category ownership across Destination.com — sold as brand-safe, clearly labeled partnerships."
  );

  const items = [
    { t: "Sponsored Articles", b: "A full feature produced by our editorial studio, optimized for search and built to live on the site long after the flight ends.", c: C.blue },
    { t: "Homepage Placements", b: "Hero and featured-module positions on the highest-traffic page we own. Ideal for launches and seasonal pushes.", c: C.teal },
    { t: "Category Sponsorships", b: "Own a vertical outright — Beaches, City Breaks, Points & Miles, Family Travel — with presence on every page in it.", c: C.terra },
    { t: "Display Advertising", b: "Standard IAB units across run-of-site or targeted category inventory, with viewability and brand-safety controls.", c: C.gold },
    { t: "Native Placements", b: "In-feed and in-article recommendation units that match our editorial design and read as part of the page.", c: C.blue },
    { t: "Affiliate & Deep-Link Integration", b: "Deep links straight to your booking flow, room type or offer page — placed where the reader is already comparing.", c: C.teal },
    { t: "Custom Content Hubs", b: "A branded destination within Destination.com: multiple articles, imagery, itineraries and offers under one URL.", c: C.terra },
    { t: "Newsletter-to-Site Retargeting", b: "Re-serve your message on-site to readers who clicked your newsletter placement. The full-funnel close.", c: C.gold },
  ];

  items.forEach((it, i) => {
    const col = i % 4, row = Math.floor(i / 4);
    const x = 0.5 + col * 3.14;
    const y = 2.1 + row * 2.32;
    card(s, x, y, 2.94, 2.08, it.c);
    s.addText(it.t, {
      x: x + 0.22, y: y + 0.28, w: 2.5, h: 0.7,
      fontFace: F.head, fontSize: 15, color: C.ink, bold: true,
      lineSpacingMultiple: 1.08, margin: 0,
    });
    s.addText(it.b, {
      x: x + 0.22, y: y + 1.0, w: 2.5, h: 1.0,
      fontFace: F.body, fontSize: 10.5, color: C.muted, lineSpacingMultiple: 1.25, margin: 0,
    });
  });
}

// ================================================================
// 13 — Full-funnel activation map
// ================================================================
{
  const s = contentSlide(
    "ACTIVATION MAP",
    "Which product does which job",
    "A quick reference for matching your objective to the right placement across the journey."
  );

  const rows = [
    ["JOURNEY STAGE", "READER MINDSET", "BEST-FIT PLACEMENTS", "WHAT IT DELIVERS"],
    ["Inspiration", "Where should we go?", "Destination Spotlight · Homepage Hero · Native", "Awareness & consideration"],
    ["Research", "Is this trip right for us?", "Sponsored Article · Category Sponsorship", "Credibility & shortlisting"],
    ["Planning", "How do we make it work?", "Newsletter Native · Custom Content Hub", "Preference & product education"],
    ["Booking", "Where do I book it?", "Travel Deal Feature · Deep Links · Dedicated Send", "Clicks, bookings, revenue"],
    ["Post-trip", "What's next?", "Segmented Campaign · Retargeting · Display", "Loyalty & repeat purchase"],
  ];

  const colW = [2.2, 2.8, 4.6, 2.4];
  const rowH = 0.72;
  rows.forEach((r, i) => {
    const y = 2.25 + i * rowH;
    const isHead = i === 0;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: SW - 1.0, h: rowH,
      fill: { color: isHead ? C.ink : (i % 2 ? C.white : C.creamDark) },
      line: { type: "none" },
    });
    if (!isHead) {
      s.addShape(pres.shapes.RECTANGLE, {
        x: 0.5, y, w: 0.05, h: rowH,
        fill: { color: [C.blue, C.teal, C.terra, C.gold, C.blue][i - 1] }, line: { type: "none" },
      });
    }
    let cx = 0.78;
    r.forEach((cell, j) => {
      s.addText(cell, {
        x: cx, y, w: colW[j] - 0.25, h: rowH,
        fontFace: isHead ? F.body : (j === 0 ? F.head : F.body),
        fontSize: isHead ? 9.5 : (j === 0 ? 14 : 11),
        color: isHead ? C.white : (j === 0 ? C.ink : C.text),
        bold: isHead || j === 0,
        italic: !isHead && j === 1,
        charSpacing: isHead ? 2 : 0,
        valign: "middle", margin: 0,
      });
      cx += colW[j];
    });
  });

  s.addText(
    "Most partners start with one stage and expand. The strongest results come from sequencing two or more.",
    {
      x: 0.5, y: 6.35, w: SW - 1.0, h: 0.4,
      fontFace: F.body, fontSize: 12.5, color: C.muted, italic: true, align: "center", margin: 0,
    }
  );
}

// ================================================================
// 14 — Advertiser categories
// ================================================================
{
  const s = contentSlide(
    "WHO WE WORK WITH",
    "Built for brands that need travelers, not eyeballs",
    "If your revenue depends on someone deciding to take a trip, this audience is your buying window."
  );

  const cats = [
    { t: "Hotels & Resorts", c: C.blue }, { t: "Airlines", c: C.teal },
    { t: "OTAs & Booking", c: C.terra }, { t: "Tourism Boards", c: C.gold },
    { t: "Credit Cards & Points", c: C.blue }, { t: "Cruise Lines", c: C.teal },
    { t: "Travel Insurance", c: C.terra }, { t: "Luggage & Gear", c: C.gold },
    { t: "Attractions & Tickets", c: C.blue }, { t: "Car Rental", c: C.teal },
    { t: "Restaurants & Dining", c: C.terra }, { t: "Local Experiences", c: C.gold },
  ];

  cats.forEach((ct, i) => {
    const col = i % 4, row = Math.floor(i / 4);
    const x = 0.5 + col * 3.14;
    const y = 2.15 + row * 1.15;
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w: 2.94, h: 1.0,
      fill: { color: C.white }, line: { color: C.divider, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w: 0.07, h: 1.0,
      fill: { color: ct.c }, line: { type: "none" },
    });
    s.addText(ct.t, {
      x: x + 0.3, y, w: 2.5, h: 1.0,
      fontFace: F.head, fontSize: 15, color: C.ink, bold: true, valign: "middle", margin: 0,
    });
  });

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 5.85, w: SW - 1.0, h: 0.9,
    fill: { color: C.goldSoft }, line: { type: "none" },
  });
  s.addText(
    "One trip creates a dozen purchases. Reaching the traveler once, early, puts your brand in front of " +
    "every decision that follows it.",
    {
      x: 0.85, y: 5.85, w: 11.6, h: 0.9,
      fontFace: F.body, fontSize: 13.5, color: C.ink, italic: true,
      valign: "middle", margin: 0,
    }
  );
}

// ================================================================
// 15 — Sample packages
// ================================================================
{
  const s = contentSlide(
    "SAMPLE PACKAGES",
    "Four ways in",
    "Indicative bundles — every package is built to the brief, and everything below can be mixed."
  );

  const packs = [
    {
      n: "DISCOVER", p: "$5,000", tag: "Test the audience",
      items: ["1 × Sponsored Placement", "1 × Travel Deal Feature", "1 × Sponsored Article", "Performance report"],
      c: C.blue, dark: false,
    },
    {
      n: "AMPLIFY", p: "$12,500", tag: "Build a season",
      items: ["1 × Dedicated Send", "2 × Newsletter Native", "1 × Homepage Placement", "2 × Sponsored Articles", "Segment targeting included"],
      c: C.teal, dark: false,
    },
    {
      n: "SIGNATURE", p: "$28,000", tag: "Own the category",
      items: ["2 × Dedicated Sends", "Destination Spotlight series", "Category Sponsorship (1 month)", "Custom Content Hub", "Newsletter-to-site retargeting", "Dedicated account team"],
      c: C.terra, dark: true,
    },
    {
      n: "ALWAYS-ON", p: "$8,000", unit: "/mo", tag: "Stay in market",
      items: ["Monthly native placements", "Run-of-site display", "Deep-link integration", "Quarterly content refresh", "6-month minimum"],
      c: C.gold, dark: false,
    },
  ];

  packs.forEach((pk, i) => {
    const x = 0.5 + i * 3.14;
    const bg = pk.dark ? C.ink : C.white;
    const titleColor = pk.dark ? C.white : C.ink;
    const bodyColor = pk.dark ? C.blueSoft : C.muted;

    s.addShape(pres.shapes.RECTANGLE, {
      x, y: 2.1, w: 2.94, h: 4.35,
      fill: { color: bg },
      line: { color: pk.dark ? C.gold : C.divider, width: pk.dark ? 1 : 0.5 },
      shadow: { type: "outer", color: "000000", blur: 8, offset: 2, angle: 90, opacity: 0.06 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: 2.1, w: 2.94, h: 0.07,
      fill: { color: pk.c }, line: { type: "none" },
    });

    s.addText(pk.n, {
      x: x + 0.22, y: 2.32, w: 2.5, h: 0.35,
      fontFace: F.body, fontSize: 11, color: pk.c, bold: true, charSpacing: 3.5, margin: 0,
    });
    // "from" sits on its own line so every price renders identically
    s.addText("from", {
      x: x + 0.22, y: 2.63, w: 2.5, h: 0.24,
      fontFace: F.body, fontSize: 10.5, color: bodyColor, margin: 0,
    });
    s.addText(
      [
        { text: pk.p, options: { fontSize: 26 } },
        { text: pk.unit || "", options: { fontSize: 13 } },
      ],
      {
        x: x + 0.22, y: 2.84, w: 2.5, h: 0.5,
        fontFace: F.head, color: titleColor, bold: true, margin: 0,
      }
    );
    s.addText(pk.tag, {
      x: x + 0.22, y: 3.3, w: 2.5, h: 0.3,
      fontFace: F.body, fontSize: 11, color: bodyColor, italic: true, margin: 0,
    });
    s.addShape(pres.shapes.LINE, {
      x: x + 0.22, y: 3.58, w: 2.5, h: 0,
      line: { color: pk.dark ? C.blueDeep : C.divider, width: 1 },
    });
    pk.items.forEach((it, j) => {
      const y = 3.7 + j * 0.46;
      s.addShape(pres.shapes.OVAL, {
        x: x + 0.24, y: y + 0.13, w: 0.08, h: 0.08,
        fill: { color: pk.c }, line: { type: "none" },
      });
      s.addText(it, {
        x: x + 0.44, y, w: 2.3, h: 0.42,
        fontFace: F.body, fontSize: 10.5, color: pk.dark ? C.white : C.text,
        valign: "middle", lineSpacingMultiple: 1.1, margin: 0,
      });
    });
  });

  s.addText(
    "Indicative pricing, net. Final rates depend on segment size, flight dates and production scope.",
    {
      x: 0.5, y: 6.6, w: SW - 1.0, h: 0.35,
      fontFace: F.body, fontSize: 10.5, color: C.muted, italic: true, align: "center", margin: 0,
    }
  );
}

// ================================================================
// 16 — Rate card
// ================================================================
{
  const s = contentSlide(
    "RATE CARD",
    "Indicative rates",
    "Net rates for planning purposes. Segmented and multi-flight buys are quoted on request."
  );

  s.addText("NEWSLETTER", {
    x: 0.5, y: 2.05, w: 6, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.blue, bold: true, charSpacing: 3, margin: 0,
  });
  priceTable(s, 0.5, 2.4, 6.2, [
    ["Placement", "Reach", "Rate"],
    ["Dedicated Send", "Full list", "$3,500"],
    ["Sponsored Placement — top", "Full list", "$2,000"],
    ["Sponsored Placement — mid", "Full list", "$1,200"],
    ["Destination Spotlight", "Full list", "$2,750"],
    ["Travel Deal Feature", "Full list", "$1,500"],
    ["Segmented Campaign", "10K min.", "from $950"],
  ], [3.3, 1.6, 1.3], C.blue);

  s.addText("DESTINATION.COM SITE", {
    x: 6.95, y: 2.05, w: 6, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.teal, bold: true, charSpacing: 3, margin: 0,
  });
  priceTable(s, 6.95, 2.4, 5.88, [
    ["Placement", "Term", "Rate"],
    ["Sponsored Article", "12 months live", "$3,000"],
    ["Homepage Hero", "1 week", "$2,500"],
    ["Category Sponsorship", "1 month", "$4,000"],
    ["Display / Native ROS", "CPM", "$18"],
    ["Custom Content Hub", "3 months", "$12,000+"],
    ["Newsletter→Site Retargeting", "Add-on", "$1,500"],
  ], [3.1, 1.5, 1.28], C.teal);

  // Terms band
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 5.5, w: SW - 1.0, h: 1.2,
    fill: { color: C.ink }, line: { type: "none" },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 5.5, w: 0.07, h: 1.2,
    fill: { color: C.gold }, line: { type: "none" },
  });
  const terms = [
    ["Volume", "10–20% off multi-flight and quarterly commitments"],
    ["Production", "Native and article creative produced in-house, included"],
    ["Exclusivity", "Category exclusivity available on request"],
  ];
  terms.forEach((t, i) => {
    const x = 0.85 + i * 4.0;
    s.addText(t[0].toUpperCase(), {
      x, y: 5.7, w: 3.7, h: 0.28,
      fontFace: F.body, fontSize: 9.5, color: C.gold, bold: true, charSpacing: 2.5, margin: 0,
    });
    s.addText(t[1], {
      x, y: 6.0, w: 3.7, h: 0.55,
      fontFace: F.body, fontSize: 11.5, color: C.white, lineSpacingMultiple: 1.2, margin: 0,
    });
  });
}

// ================================================================
// 17 — How a campaign runs
// ================================================================
{
  const s = contentSlide(
    "HOW IT WORKS",
    "From brief to live in two weeks",
    "A short, low-friction process — because travel windows don't wait for long approval cycles."
  );

  const steps = [
    { n: "01", t: "Brief", d: "WEEK 1", b: "You share the objective, target traveler and flight dates. We come back with a recommended segment and placement mix.", c: C.blue },
    { n: "02", t: "Plan & Book", d: "WEEK 1", b: "We confirm inventory, lock dates, and issue the IO. Segment sizes are confirmed before you commit.", c: C.teal },
    { n: "03", t: "Create", d: "WEEK 2", b: "Our studio produces native copy, article drafts and creative for your approval. One round of revisions included.", c: C.terra },
    { n: "04", t: "Launch", d: "WEEK 2", b: "Placements go live. Deep links and tracking are QA'd before send so nothing is lost at the click.", c: C.gold },
    { n: "05", t: "Report", d: "ONGOING", b: "Performance reporting per placement, plus a post-campaign readout with recommendations for the next flight.", c: C.blue },
  ];

  steps.forEach((st, i) => {
    const y = 2.15 + i * 0.9;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: SW - 1.0, h: 0.78,
      fill: { color: i % 2 ? C.white : C.creamDark }, line: { color: C.divider, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: 0.75, h: 0.78,
      fill: { color: st.c }, line: { type: "none" },
    });
    s.addText(st.n, {
      x: 0.5, y, w: 0.75, h: 0.78,
      fontFace: F.head, fontSize: 18, color: C.white, bold: true,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(st.t, {
      x: 1.5, y, w: 2.0, h: 0.78,
      fontFace: F.head, fontSize: 16, color: C.ink, bold: true, valign: "middle", margin: 0,
    });
    s.addText(st.d, {
      x: 3.4, y, w: 1.3, h: 0.78,
      fontFace: F.body, fontSize: 9.5, color: st.c, bold: true, charSpacing: 2,
      valign: "middle", margin: 0,
    });
    s.addText(st.b, {
      x: 4.85, y: y + 0.08, w: 7.9, h: 0.62,
      fontFace: F.body, fontSize: 11.5, color: C.muted,
      lineSpacingMultiple: 1.2, valign: "middle", margin: 0,
    });
  });

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 6.72, w: SW - 1.0, h: 0.0,
    fill: { color: C.divider }, line: { type: "none" },
  });
}

// ================================================================
// 18 — Measurement
// ================================================================
{
  const s = contentSlide(
    "MEASUREMENT",
    "What you get back",
    "Reporting that answers the only question that matters: did it move travelers toward booking?"
  );

  const metrics = [
    { h: "DELIVERY", c: C.blue, items: ["Sends and delivered volume", "Segment size confirmation", "Impressions by placement", "Viewability (display)"] },
    { h: "ENGAGEMENT", c: C.teal, items: ["Open rate vs. list benchmark", "Clicks and CTR by unit", "Article reads and dwell", "Scroll depth on hubs"] },
    { h: "OUTCOMES", c: C.terra, items: ["Click-through to your site", "Deep-link and affiliate clicks", "Conversions via your pixel", "Post-click behavior"] },
    { h: "LEARNING", c: C.gold, items: ["Best-performing segments", "Creative and subject-line reads", "Recommended next flight", "Full post-campaign readout"] },
  ];

  metrics.forEach((m, i) => {
    const x = 0.5 + i * 3.14;
    card(s, x, 2.1, 2.94, 3.3, m.c);
    s.addText(m.h, {
      x: x + 0.22, y: 2.35, w: 2.5, h: 0.3,
      fontFace: F.body, fontSize: 9.5, color: m.c, bold: true, charSpacing: 2.5, margin: 0,
    });
    s.addShape(pres.shapes.LINE, {
      x: x + 0.22, y: 2.72, w: 2.5, h: 0,
      line: { color: C.divider, width: 1 },
    });
    m.items.forEach((it, j) => {
      const y = 2.85 + j * 0.6;
      s.addShape(pres.shapes.OVAL, {
        x: x + 0.24, y: y + 0.2, w: 0.09, h: 0.09,
        fill: { color: m.c }, line: { type: "none" },
      });
      s.addText(it, {
        x: x + 0.45, y, w: 2.3, h: 0.5,
        fontFace: F.body, fontSize: 11, color: C.text,
        lineSpacingMultiple: 1.15, valign: "middle", margin: 0,
      });
    });
  });

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 5.62, w: SW - 1.0, h: 1.08,
    fill: { color: C.goldSoft }, line: { type: "none" },
  });
  s.addText("Brand safety & transparency", {
    x: 0.85, y: 5.78, w: 6, h: 0.3,
    fontFace: F.body, fontSize: 10, color: C.terra, bold: true, charSpacing: 2.5, margin: 0,
  });
  s.addText(
    "All sponsored content is clearly labeled, editorially reviewed, and produced to our own standards. " +
    "No cluttered placements, no misleading creative, no surprises for your brand or our readers.",
    {
      x: 0.85, y: 6.08, w: 11.6, h: 0.55,
      fontFace: F.body, fontSize: 12, color: C.ink, lineSpacingMultiple: 1.2, margin: 0,
    }
  );
}

// ================================================================
// 19 — Why Destination.com
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.ink };
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 0.58, w: 0.55, h: 0.045,
    fill: { color: C.gold }, line: { type: "none" },
  });
  s.addText("WHY DESTINATION.COM", {
    x: 1.2, y: 0.43, w: 9, h: 0.32,
    fontFace: F.body, fontSize: 10, color: C.gold, bold: true, charSpacing: 4, margin: 0,
  });
  s.addText("Five reasons this is a must-buy", {
    x: 0.5, y: 0.82, w: SW - 1.0, h: 0.62,
    fontFace: F.head, fontSize: 29, color: C.white, bold: true, margin: 0,
  });

  const reasons = [
    { n: "01", t: "Intent, not inference", b: "We don't model travel interest from browsing exhaust. Our readers came to us to plan a trip." },
    { n: "02", t: "First-party and owned", b: "The list, the site and the data are ours. No cookie deprecation risk, no platform dependency." },
    { n: "03", t: "Full-journey coverage", b: "Inspiration through post-trip, in one buy, against the same reader — sequenced, not scattered." },
    { n: "04", t: "Editorial credibility", b: "Native and sponsored work here because readers trust the recommendations around it." },
    { n: "05", t: "Built to be measured", b: "Per-placement reporting, deep-link tracking, and a readout that tells you what to buy next." },
  ];

  reasons.forEach((r, i) => {
    const y = 1.75 + i * 1.02;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: SW - 1.0, h: 0.9,
      fill: { color: C.inkDeep }, line: { type: "none" },
    });
    s.addText(r.n, {
      x: 0.75, y, w: 0.85, h: 0.9,
      fontFace: F.head, fontSize: 22, color: C.gold, bold: true, valign: "middle", margin: 0,
    });
    s.addText(r.t, {
      x: 1.75, y, w: 3.7, h: 0.9,
      fontFace: F.head, fontSize: 17, color: C.white, bold: true, valign: "middle", margin: 0,
    });
    s.addText(r.b, {
      x: 5.6, y, w: 7.2, h: 0.9,
      fontFace: F.body, fontSize: 12.5, color: C.blueSoft,
      lineSpacingMultiple: 1.2, valign: "middle", margin: 0,
    });
  });

  addFooter(s, true);
}

// ================================================================
// 20 — Contact
// ================================================================
{
  const s = pres.addSlide();
  s.background = { color: C.inkDeep };
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.18, h: SH,
    fill: { color: C.gold }, line: { type: "none" },
  });

  s.addText("LET'S BUILD IT", {
    x: 1.1, y: 1.5, w: 8, h: 0.4,
    fontFace: F.body, fontSize: 11, color: C.gold, bold: true, charSpacing: 6, margin: 0,
  });
  s.addText("Tell us who you want\nto reach.", {
    x: 1.1, y: 2.0, w: 10.5, h: 2.0,
    fontFace: F.head, fontSize: 46, color: C.white, bold: true,
    lineSpacingMultiple: 1.05, margin: 0,
  });
  s.addText(
    "Send us the destination, the traveler and the dates. We'll come back with a segment, " +
    "a placement plan and a rate — usually within two business days.",
    {
      x: 1.1, y: 4.1, w: 9.5, h: 1.0,
      fontFace: F.body, fontSize: 16, color: C.blueSoft, lineSpacingMultiple: 1.35, margin: 0,
    }
  );

  s.addShape(pres.shapes.RECTANGLE, {
    x: 1.1, y: 5.35, w: 10.5, h: 1.1,
    fill: { color: C.ink }, line: { color: C.gold, width: 0.75 },
  });
  s.addText("Advertising & Partnerships  ·  PGAM Media", {
    x: 1.4, y: 5.5, w: 7, h: 0.35,
    fontFace: F.body, fontSize: 13, color: C.white, bold: true, margin: 0,
  });
  s.addText("ppatel@pgammedia.com  ·  destination.com  ·  pgammedia.com", {
    x: 1.4, y: 5.88, w: 9, h: 0.35,
    fontFace: F.body, fontSize: 12, color: C.blueSoft, margin: 0,
  });

  page += 1;
  s.addText(`${page} / ${TOTAL}`, {
    x: SW - 1.5, y: SH - 0.41, w: 1.0, h: 0.3,
    fontFace: F.body, fontSize: 9, color: C.muted, align: "right", margin: 0,
  });
}

// ----------------------------------------------------------------
pres.writeFile({ fileName: "Destination_com_Media_Kit.pptx" })
  .then(f => console.log("Wrote:", f, "—", page, "slides"));
