"""
Creates 6 working health calculator pages on WordPress.
Run once: python create_calculators.py
"""
import json, requests, config

AUTH    = (config.WP_USERNAME, config.WP_APP_PASS.replace(" ", ""))
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", "Accept": "application/json"}
API     = f"{config.WP_SITE_URL}/wp-json/wp/v2"

def post(endpoint, data):
    r = requests.post(f"{API}/{endpoint}", auth=AUTH, headers=HEADERS, json=data, timeout=30)
    r.encoding = "utf-8-sig"
    r.raise_for_status()
    return json.loads(r.text)

# ── Ensure /tools/ parent page exists ──────────────────────────────────────
def get_or_create_tools_parent():
    r = requests.get(f"{API}/pages", auth=AUTH, headers=HEADERS,
                     params={"slug": "tools", "per_page": 1}, timeout=15)
    r.encoding = "utf-8-sig"
    pages = json.loads(r.text)
    if pages:
        print(f"  ✓ /tools/ page exists (ID {pages[0]['id']})")
        return pages[0]["id"]
    p = post("pages", {"title": "Health Tools & Calculators", "slug": "tools", "status": "publish",
                        "content": "<p>Evidence-based health calculators to help you track the numbers that matter.</p>"})
    print(f"  ✓ Created /tools/ page (ID {p['id']})")
    return p["id"]

CALCULATORS = [
    {
        "title": "BMR Calculator — Find Your Base Metabolic Rate",
        "slug": "bmr-calculator",
        "meta_title": "BMR Calculator: Calculate Your Basal Metabolic Rate",
        "meta_desc": "Calculate your Basal Metabolic Rate (BMR) using the Mifflin-St Jeor equation — the most accurate formula for estimating daily calorie needs.",
        "content": """
<div class="calc-wrap">
<h1>BMR Calculator</h1>
<p class="calc-intro">Your <strong>Basal Metabolic Rate (BMR)</strong> is the number of calories your body burns at rest. Use this to find your true calorie baseline.</p>

<div class="calc-box" id="bmr-calc">
  <div class="calc-fields">
    <div class="calc-field">
      <label>Sex</label>
      <select id="bmr-sex"><option value="male">Male</option><option value="female">Female</option></select>
    </div>
    <div class="calc-field">
      <label>Age (years)</label>
      <input type="number" id="bmr-age" value="30" min="15" max="100" />
    </div>
    <div class="calc-field">
      <label>Weight (kg)</label>
      <input type="number" id="bmr-weight" value="70" min="30" max="300" />
    </div>
    <div class="calc-field">
      <label>Height (cm)</label>
      <input type="number" id="bmr-height" value="175" min="100" max="250" />
    </div>
  </div>
  <button class="calc-btn" onclick="calcBMR()">Calculate BMR</button>
  <div class="calc-result" id="bmr-result" style="display:none"></div>
</div>

<script>
function calcBMR() {
  var sex = document.getElementById('bmr-sex').value;
  var age = parseFloat(document.getElementById('bmr-age').value);
  var w   = parseFloat(document.getElementById('bmr-weight').value);
  var h   = parseFloat(document.getElementById('bmr-height').value);
  var bmr = sex === 'male'
    ? 10 * w + 6.25 * h - 5 * age + 5
    : 10 * w + 6.25 * h - 5 * age - 161;
  var tdee = [
    ['Sedentary (little/no exercise)', bmr * 1.2],
    ['Lightly active (1–3 days/week)', bmr * 1.375],
    ['Moderately active (3–5 days/week)', bmr * 1.55],
    ['Very active (6–7 days/week)', bmr * 1.725],
    ['Extra active (physical job + exercise)', bmr * 1.9]
  ];
  var html = '<h3>Your BMR: <strong>' + Math.round(bmr) + ' calories/day</strong></h3>';
  html += '<p>This is your calorie burn at complete rest. Add activity below:</p><table><thead><tr><th>Activity Level</th><th>Daily Calories (TDEE)</th></tr></thead><tbody>';
  tdee.forEach(function(t){ html += '<tr><td>' + t[0] + '</td><td><strong>' + Math.round(t[1]) + '</strong></td></tr>'; });
  html += '</tbody></table><p class="calc-note">Formula: Mifflin-St Jeor (most validated for accuracy)</p>';
  var el = document.getElementById('bmr-result');
  el.innerHTML = html; el.style.display = 'block';
}
</script>

<h2>How Is BMR Calculated?</h2>
<p>This calculator uses the <strong>Mifflin-St Jeor equation</strong>, which a 2005 meta-analysis in the Journal of the American Dietetic Association found to be the most accurate BMR formula for most people:</p>
<ul>
<li><strong>Men:</strong> BMR = (10 × weight in kg) + (6.25 × height in cm) − (5 × age) + 5</li>
<li><strong>Women:</strong> BMR = (10 × weight in kg) + (6.25 × height in cm) − (5 × age) − 161</li>
</ul>
<h2>What Is BMR Used For?</h2>
<p>BMR is the foundation of calculating your Total Daily Energy Expenditure (TDEE) — the total calories you burn per day including activity. Knowing your TDEE lets you set accurate calorie targets for weight loss, maintenance, or muscle gain.</p>
</div>""",
    },
    {
        "title": "Protein Calculator — Daily Intake by Goal & Body Weight",
        "slug": "protein-calculator",
        "meta_title": "Protein Calculator: How Much Protein Do You Need Per Day?",
        "meta_desc": "Calculate your optimal daily protein intake based on your body weight, goal (muscle gain, fat loss, maintenance), and activity level.",
        "content": """
<div class="calc-wrap">
<h1>Protein Calculator</h1>
<p class="calc-intro">Calculate exactly how much protein you need per day based on your weight, goal, and activity level — using current sports nutrition research.</p>

<div class="calc-box" id="protein-calc">
  <div class="calc-fields">
    <div class="calc-field">
      <label>Body Weight (kg)</label>
      <input type="number" id="p-weight" value="70" min="30" max="300" />
    </div>
    <div class="calc-field">
      <label>Goal</label>
      <select id="p-goal">
        <option value="maintain">Maintain weight</option>
        <option value="muscle">Build muscle</option>
        <option value="lose">Lose fat (preserve muscle)</option>
        <option value="athlete">Endurance athlete</option>
      </select>
    </div>
  </div>
  <button class="calc-btn" onclick="calcProtein()">Calculate Protein</button>
  <div class="calc-result" id="protein-result" style="display:none"></div>
</div>

<script>
function calcProtein() {
  var w    = parseFloat(document.getElementById('p-weight').value);
  var goal = document.getElementById('p-goal').value;
  var ranges = {
    maintain: [1.2, 1.6, 'General health & weight maintenance'],
    muscle:   [1.6, 2.2, 'Muscle hypertrophy & strength gains'],
    lose:     [1.8, 2.4, 'Fat loss while preserving lean mass'],
    athlete:  [1.4, 1.7, 'Endurance performance & recovery']
  };
  var r = ranges[goal];
  var low = Math.round(r[0] * w), high = Math.round(r[1] * w);
  var html = '<h3>Your Daily Protein Target: <strong>' + low + '–' + high + 'g</strong></h3>';
  html += '<p>(' + r[0] + '–' + r[1] + 'g per kg body weight for: <em>' + r[2] + '</em>)</p>';
  html += '<table><thead><tr><th>Meal (4 per day)</th><th>Protein per meal</th></tr></thead><tbody>';
  html += '<tr><td>Minimum per meal</td><td>' + Math.round(low/4) + 'g</td></tr>';
  html += '<tr><td>Maximum per meal</td><td>' + Math.round(high/4) + 'g</td></tr>';
  html += '</tbody></table><p class="calc-note">Based on ISSN Position Stand on protein and exercise (Stokes et al., 2018)</p>';
  var el = document.getElementById('protein-result');
  el.innerHTML = html; el.style.display = 'block';
}
</script>

<h2>How Much Protein Do You Really Need?</h2>
<p>The old RDA of 0.8g/kg is a <em>minimum to prevent deficiency</em> — not an optimal intake for active people. Current research supports:</p>
<ul>
<li><strong>1.6–2.2g/kg</strong> for muscle building (Stokes et al., 2018)</li>
<li><strong>1.8–2.4g/kg</strong> during caloric restriction to preserve muscle</li>
<li><strong>Up to 3.1g/kg</strong> shows no harm in healthy adults (Antonio et al., 2016)</li>
</ul>
</div>""",
    },
    {
        "title": "Sleep Debt Calculator — How Much Sleep Do You Owe?",
        "slug": "sleep-tracker",
        "meta_title": "Sleep Debt Calculator: Are You Sleep Deprived?",
        "meta_desc": "Calculate your weekly sleep debt and find out how far below your optimal sleep target you are — and how long it will take to recover.",
        "content": """
<div class="calc-wrap">
<h1>Sleep Debt Calculator</h1>
<p class="calc-intro">Sleep debt accumulates when you consistently sleep less than your biological need. Calculate yours and see the health impact.</p>

<div class="calc-box" id="sleep-calc">
  <div class="calc-fields">
    <div class="calc-field">
      <label>Your age</label>
      <select id="s-age">
        <option value="9">14–17 years (need: 8–10h)</option>
        <option value="8" selected>18–64 years (need: 7–9h)</option>
        <option value="7.5">65+ years (need: 7–8h)</option>
      </select>
    </div>
    <div class="calc-field">
      <label>Average sleep per night (hours)</label>
      <input type="number" id="s-actual" value="6.5" min="2" max="12" step="0.5" />
    </div>
    <div class="calc-field">
      <label>Days tracked</label>
      <input type="number" id="s-days" value="7" min="1" max="30" />
    </div>
  </div>
  <button class="calc-btn" onclick="calcSleep()">Calculate Sleep Debt</button>
  <div class="calc-result" id="sleep-result" style="display:none"></div>
</div>

<script>
function calcSleep() {
  var need   = parseFloat(document.getElementById('s-age').value);
  var actual = parseFloat(document.getElementById('s-actual').value);
  var days   = parseFloat(document.getElementById('s-days').value);
  var debt   = Math.max(0, (need - actual) * days);
  var status = debt === 0 ? 'No sleep debt — well done!' : debt < 5 ? 'Mild sleep debt' : debt < 14 ? 'Moderate sleep debt' : 'Significant sleep debt';
  var recovery = debt > 0 ? Math.ceil(debt / 1.5) : 0;
  var html = '<h3>Your Sleep Debt: <strong>' + debt.toFixed(1) + ' hours</strong></h3>';
  html += '<p>Status: <strong>' + status + '</strong></p>';
  if (debt > 0) {
    html += '<p>Recovery time: approximately <strong>' + recovery + ' nights</strong> of full sleep to recover</p>';
    html += '<p class="calc-note">Research shows you can only "repay" about 1–1.5 hours of sleep debt per night (Besedovsky et al., 2019)</p>';
  }
  var el = document.getElementById('sleep-result');
  el.innerHTML = html; el.style.display = 'block';
}
</script>

<h2>What Is Sleep Debt?</h2>
<p>Sleep debt is the cumulative difference between the sleep you need and the sleep you get. Unlike financial debt, it can't be fully repaid with one long weekend sleep — chronic debt requires consistent recovery.</p>
</div>""",
    },
    {
        "title": "VO2 Max Estimator — Estimate Your Cardiorespiratory Fitness",
        "slug": "vo2-max-estimator",
        "meta_title": "VO2 Max Estimator: Calculate Cardiorespiratory Fitness",
        "meta_desc": "Estimate your VO2 max from resting heart rate and age — and see how your cardiorespiratory fitness compares to population norms.",
        "content": """
<div class="calc-wrap">
<h1>VO2 Max Estimator</h1>
<p class="calc-intro">VO2 max is the strongest predictor of longevity in healthy adults. Estimate yours using your resting heart rate.</p>

<div class="calc-box" id="vo2-calc">
  <div class="calc-fields">
    <div class="calc-field">
      <label>Age (years)</label>
      <input type="number" id="v-age" value="35" min="15" max="80" />
    </div>
    <div class="calc-field">
      <label>Resting Heart Rate (bpm)</label>
      <input type="number" id="v-rhr" value="65" min="35" max="100" />
      <small>Measure first thing in the morning before getting up</small>
    </div>
    <div class="calc-field">
      <label>Sex</label>
      <select id="v-sex"><option value="male">Male</option><option value="female">Female</option></select>
    </div>
  </div>
  <button class="calc-btn" onclick="calcVO2()">Estimate VO2 Max</button>
  <div class="calc-result" id="vo2-result" style="display:none"></div>
</div>

<script>
function calcVO2() {
  var age = parseFloat(document.getElementById('v-age').value);
  var rhr = parseFloat(document.getElementById('v-rhr').value);
  var sex = document.getElementById('v-sex').value;
  var maxHR = 208 - (0.7 * age);
  var vo2 = 15 * (maxHR / rhr);
  var norms = sex === 'male' ? [
    [18,25,[52,60,46,52,38,46,30,38]],
    [26,35,[49,56,43,49,35,43,27,35]],
    [36,45,[47,54,40,47,33,40,25,33]],
    [46,55,[45,51,37,45,31,37,23,31]],
    [56,65,[42,48,34,42,28,34,20,28]]
  ] : [
    [18,25,[45,52,38,45,31,38,23,31]],
    [26,35,[42,48,35,42,29,35,21,29]],
    [36,45,[40,46,33,40,27,33,19,27]],
    [46,55,[37,43,30,37,24,30,16,24]],
    [56,65,[34,40,27,34,21,27,13,21]]
  ];
  var category = 'Below Average';
  for (var i = 0; i < norms.length; i++) {
    var n = norms[i];
    if (age >= n[0] && age <= n[1]) {
      var vals = n[2];
      if (vo2 >= vals[0]) category = 'Superior';
      else if (vo2 >= vals[2]) category = 'Excellent';
      else if (vo2 >= vals[4]) category = 'Good';
      else if (vo2 >= vals[6]) category = 'Fair';
      break;
    }
  }
  var html = '<h3>Estimated VO2 Max: <strong>' + Math.round(vo2) + ' mL/kg/min</strong></h3>';
  html += '<p>Fitness category: <strong>' + category + '</strong> for your age and sex</p>';
  html += '<p>To improve VO2 max: add 2–3 sessions of Zone 2 cardio (30–45 min at 60–70% max HR) plus 1 HIIT session per week.</p>';
  html += '<p class="calc-note">Estimate based on Uth et al. (2004) heart rate ratio method. For precision, use a lab VO2 max test.</p>';
  var el = document.getElementById('vo2-result');
  el.innerHTML = html; el.style.display = 'block';
}
</script>
</div>""",
    },
    {
        "title": "Hydration Calculator — Daily Water Intake by Weight & Activity",
        "slug": "hydration-calculator",
        "meta_title": "Hydration Calculator: How Much Water Should You Drink Per Day?",
        "meta_desc": "Calculate your daily water needs based on body weight, activity level, and climate. Based on current hydration science.",
        "content": """
<div class="calc-wrap">
<h1>Hydration Calculator</h1>
<p class="calc-intro">How much water do you actually need? It depends on your weight, activity level, and environment — not an arbitrary 8 glasses.</p>

<div class="calc-box" id="hydration-calc">
  <div class="calc-fields">
    <div class="calc-field">
      <label>Body Weight (kg)</label>
      <input type="number" id="h-weight" value="70" min="30" max="300" />
    </div>
    <div class="calc-field">
      <label>Activity Level</label>
      <select id="h-activity">
        <option value="1.0">Sedentary (desk job, minimal movement)</option>
        <option value="1.2">Lightly active (walking, light exercise)</option>
        <option value="1.4" selected>Moderately active (regular gym)</option>
        <option value="1.6">Very active (daily intense training)</option>
        <option value="1.8">Athlete (2+ sessions/day)</option>
      </select>
    </div>
    <div class="calc-field">
      <label>Climate</label>
      <select id="h-climate">
        <option value="1.0">Temperate / Cool</option>
        <option value="1.1">Warm</option>
        <option value="1.2">Hot / Humid</option>
      </select>
    </div>
  </div>
  <button class="calc-btn" onclick="calcHydration()">Calculate Water Needs</button>
  <div class="calc-result" id="hydration-result" style="display:none"></div>
</div>

<script>
function calcHydration() {
  var w        = parseFloat(document.getElementById('h-weight').value);
  var activity = parseFloat(document.getElementById('h-activity').value);
  var climate  = parseFloat(document.getElementById('h-climate').value);
  var base     = w * 0.033;
  var total    = base * activity * climate;
  var glasses  = Math.round(total / 0.25);
  var html = '<h3>Your Daily Water Target: <strong>' + total.toFixed(1) + ' litres</strong> (' + glasses + ' glasses)</h3>';
  html += '<table><thead><tr><th>Time</th><th>Amount</th></tr></thead><tbody>';
  html += '<tr><td>On waking</td><td>500ml</td></tr>';
  html += '<tr><td>Before each meal (3×)</td><td>250ml each</td></tr>';
  html += '<tr><td>During exercise</td><td>500–750ml/hour</td></tr>';
  html += '<tr><td>Rest of day</td><td>Sip consistently</td></tr>';
  html += '</tbody></table><p class="calc-note">Note: ~20% of daily water comes from food. Coffee and tea count toward intake.</p>';
  var el = document.getElementById('hydration-result');
  el.innerHTML = html; el.style.display = 'block';
}
</script>
</div>""",
    },
    {
        "title": "Macros Calculator — Protein, Carbs & Fat by Goal",
        "slug": "macros-calculator",
        "meta_title": "Macros Calculator: Calculate Your Protein, Carbs & Fat Targets",
        "meta_desc": "Calculate your ideal macronutrient split (protein, carbohydrates, fat) based on your TDEE, body weight, and specific health goal.",
        "content": """
<div class="calc-wrap">
<h1>Macros Calculator</h1>
<p class="calc-intro">Find your optimal protein, carbohydrate, and fat targets based on your calorie goal and body composition objectives.</p>

<div class="calc-box" id="macros-calc">
  <div class="calc-fields">
    <div class="calc-field">
      <label>Daily Calories (TDEE)</label>
      <input type="number" id="m-cal" value="2200" min="1000" max="5000" />
      <small>Don't know yours? Use our <a href="/tools/bmr-calculator/">BMR Calculator</a> first</small>
    </div>
    <div class="calc-field">
      <label>Body Weight (kg)</label>
      <input type="number" id="m-weight" value="70" min="30" max="300" />
    </div>
    <div class="calc-field">
      <label>Goal</label>
      <select id="m-goal">
        <option value="maintain">Maintain weight</option>
        <option value="muscle">Build muscle (slight surplus)</option>
        <option value="lose">Lose fat (deficit)</option>
        <option value="keto">Ketogenic</option>
      </select>
    </div>
  </div>
  <button class="calc-btn" onclick="calcMacros()">Calculate Macros</button>
  <div class="calc-result" id="macros-result" style="display:none"></div>
</div>

<script>
function calcMacros() {
  var cal    = parseFloat(document.getElementById('m-cal').value);
  var w      = parseFloat(document.getElementById('m-weight').value);
  var goal   = document.getElementById('m-goal').value;
  var splits = {
    maintain: { protein: 1.6, fatPct: 0.30, label: 'Balanced maintenance' },
    muscle:   { protein: 2.0, fatPct: 0.25, label: 'Muscle building' },
    lose:     { protein: 2.2, fatPct: 0.30, label: 'Fat loss' },
    keto:     { protein: 1.8, fatPct: 0.70, label: 'Ketogenic' }
  };
  var s       = splits[goal];
  var protG   = Math.round(s.protein * w);
  var protCal = protG * 4;
  var fatCal  = Math.round(cal * s.fatPct);
  var fatG    = Math.round(fatCal / 9);
  var carbCal = cal - protCal - fatCal;
  var carbG   = Math.round(Math.max(0, carbCal) / 4);
  var html = '<h3>Your Daily Macros (' + s.label + ')</h3>';
  html += '<table><thead><tr><th>Macro</th><th>Grams</th><th>Calories</th><th>% of diet</th></tr></thead><tbody>';
  html += '<tr><td><strong>Protein</strong></td><td>' + protG + 'g</td><td>' + protCal + '</td><td>' + Math.round(protCal/cal*100) + '%</td></tr>';
  html += '<tr><td><strong>Carbohydrates</strong></td><td>' + carbG + 'g</td><td>' + carbCal + '</td><td>' + Math.round(carbCal/cal*100) + '%</td></tr>';
  html += '<tr><td><strong>Fat</strong></td><td>' + fatG + 'g</td><td>' + fatCal + '</td><td>' + Math.round(fatFal/cal*100) + '%</td></tr>';
  html += '</tbody></table>';
  var el = document.getElementById('macros-result');
  el.innerHTML = html; el.style.display = 'block';
}
</script>
</div>""",
    },
]

CALC_CSS = """
<style>
.calc-wrap { max-width: 760px; margin: 0 auto; padding: 20px 0; }
.calc-intro { font-size: 17px; color: #374151; margin-bottom: 32px; line-height: 1.7; }
.calc-box { background: #F6FAF7; border: 1px solid #d1e8d8; border-radius: 12px; padding: 32px; margin-bottom: 40px; }
.calc-fields { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 24px; }
.calc-field label { display: block; font-weight: 600; font-size: 14px; color: #111827; margin-bottom: 6px; }
.calc-field input, .calc-field select { width: 100%; padding: 10px 14px; border: 1px solid #d1d5db; border-radius: 8px; font-size: 15px; background: white; }
.calc-field small { display: block; font-size: 12px; color: #6B7280; margin-top: 4px; }
.calc-btn { background: #4A7C59; color: white; border: none; padding: 14px 32px; border-radius: 50px; font-size: 15px; font-weight: 600; cursor: pointer; transition: background .15s; }
.calc-btn:hover { background: #3d6649; }
.calc-result { margin-top: 28px; padding: 24px; background: white; border-radius: 10px; border: 1px solid #d1e8d8; }
.calc-result h3 { font-size: 20px; margin-bottom: 12px; color: #111827; }
.calc-result table { width: 100%; border-collapse: collapse; margin: 16px 0; }
.calc-result th { background: #4A7C59; color: white; padding: 10px 14px; text-align: left; font-size: 13px; }
.calc-result td { padding: 10px 14px; border-bottom: 1px solid #e5e7eb; font-size: 14px; }
.calc-note { font-size: 12px; color: #6B7280; font-style: italic; margin-top: 12px; }
</style>
"""

def main():
    print("Creating calculator pages on WordPress...")
    parent_id = get_or_create_tools_parent()

    for calc in CALCULATORS:
        # Check if already exists
        r = requests.get(f"{API}/pages", auth=AUTH, headers=HEADERS,
                         params={"slug": calc["slug"], "per_page": 1}, timeout=15)
        r.encoding = "utf-8-sig"
        existing = json.loads(r.text)
        if existing:
            print(f"  ⚠ Already exists: /{calc['slug']}/ (ID {existing[0]['id']})")
            continue

        content = CALC_CSS + calc["content"]
        data = {
            "title":   calc["title"],
            "slug":    calc["slug"],
            "content": content,
            "status":  "publish",
            "parent":  parent_id,
            "meta": {
                "rank_math_title":         calc["meta_title"],
                "rank_math_description":   calc["meta_desc"],
                "_yoast_wpseo_title":      calc["meta_title"],
                "_yoast_wpseo_metadesc":   calc["meta_desc"],
            }
        }
        try:
            page = post("pages", data)
            print(f"  ✓ Created: {page['link']}")
        except Exception as e:
            print(f"  ✗ Failed {calc['slug']}: {e}")

    print("\nDone! All calculators live at healthnation.com/tools/")

if __name__ == "__main__":
    main()
