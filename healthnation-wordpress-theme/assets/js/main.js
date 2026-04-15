/* HealthNation — main.js */
(function () {
  'use strict';

  /* ── Mobile menu ─────────────────────────────── */
  const menuToggle  = document.getElementById('menu-toggle');
  const mobileNav   = document.getElementById('mobile-nav-overlay');
  const mobileClose = document.getElementById('mobile-nav-close');

  if (menuToggle && mobileNav) {
    menuToggle.addEventListener('click', () => {
      mobileNav.classList.toggle('open');
      menuToggle.setAttribute('aria-expanded', mobileNav.classList.contains('open'));
      document.body.style.overflow = mobileNav.classList.contains('open') ? 'hidden' : '';
    });
  }
  if (mobileClose) {
    mobileClose.addEventListener('click', () => {
      mobileNav.classList.remove('open');
      document.body.style.overflow = '';
    });
  }
  if (mobileNav) {
    mobileNav.addEventListener('click', (e) => {
      if (e.target === mobileNav) {
        mobileNav.classList.remove('open');
        document.body.style.overflow = '';
      }
    });
  }

  /* ── Article filter tabs ─────────────────────── */
  const filterBtns = document.querySelectorAll('.art-filter');
  const articlesGrid = document.getElementById('articles-grid');

  filterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      filterBtns.forEach(b => { b.classList.remove('active'); b.setAttribute('aria-selected', 'false'); });
      btn.classList.add('active');
      btn.setAttribute('aria-selected', 'true');

      const cat = btn.dataset.cat;
      if (!articlesGrid) return;

      if (cat === 'all') {
        articlesGrid.querySelectorAll('.article-card').forEach(c => c.style.display = '');
        return;
      }
      // Filter by category slug in article-cat-label text
      articlesGrid.querySelectorAll('.article-card').forEach(card => {
        const label = card.querySelector('.article-cat-label');
        const match = label && label.textContent.trim().toLowerCase().replace(/\s+/g, '-') === cat;
        card.style.display = match ? '' : 'none';
      });
    });
  });

  /* ── Table of Contents auto-generator ───────── */
  const tocNav = document.getElementById('toc-nav');
  if (tocNav) {
    const headings = document.querySelectorAll('.entry-content h2, .entry-content h3');
    if (headings.length > 2) {
      const ul = document.createElement('ul');
      ul.style.cssText = 'display:flex;flex-direction:column;gap:8px;list-style:none';
      headings.forEach((h, i) => {
        if (!h.id) h.id = 'section-' + i;
        const li = document.createElement('li');
        const a  = document.createElement('a');
        a.href      = '#' + h.id;
        a.textContent = h.textContent;
        a.style.cssText = 'font-size:13px;color:var(--ink-mid);transition:color .15s;' +
                          (h.tagName === 'H3' ? 'padding-left:12px;font-size:12.5px;' : 'font-weight:500;');
        a.addEventListener('mouseenter', () => a.style.color = 'var(--sage)');
        a.addEventListener('mouseleave', () => a.style.color = 'var(--ink-mid)');
        li.appendChild(a);
        ul.appendChild(li);
      });
      tocNav.appendChild(ul);
    } else {
      document.getElementById('toc-widget')?.remove();
    }
  }

  /* ── Sticky header shadow on scroll ─────────── */
  const header = document.getElementById('site-header');
  if (header) {
    let lastY = 0;
    window.addEventListener('scroll', () => {
      const y = window.scrollY;
      header.style.boxShadow = y > 10 ? '0 2px 12px rgba(0,0,0,.08)' : '';
      lastY = y;
    }, { passive: true });
  }

  /* ── Newsletter form handler ─────────────────── */
  window.hnNewsletterSubmit = function (e) {
    e.preventDefault();
    const form  = e.target;
    const email = form.querySelector('input[type="email"]').value;
    const btn   = form.querySelector('button[type="submit"]');

    if (!email) return;

    btn.textContent = 'Subscribing…';
    btn.disabled = true;

    const data = new FormData();
    data.append('action', 'hn_newsletter');
    data.append('email',  email);
    data.append('nonce',  (window.hnData || {}).nonce || '');

    fetch((window.hnData || {}).ajaxUrl || '/wp-admin/admin-ajax.php', {
      method: 'POST',
      body: data,
    })
      .then(r => r.json())
      .then(res => {
        if (res.success) {
          form.innerHTML = '<p style="color:white;font-weight:600;font-size:14px">✓ You\'re in! Check your inbox.</p>';
        } else {
          btn.textContent = 'Try Again';
          btn.disabled = false;
        }
      })
      .catch(() => {
        btn.textContent = 'Error — Try Again';
        btn.disabled = false;
      });
  };

  /* ── Smooth scroll for anchor links ─────────── */
  document.querySelectorAll('a[href^="#"]').forEach(a => {
    a.addEventListener('click', e => {
      const target = document.querySelector(a.getAttribute('href'));
      if (target) {
        e.preventDefault();
        const offset = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--nav-h')) || 70;
        const top = target.getBoundingClientRect().top + window.scrollY - offset - 16;
        window.scrollTo({ top, behavior: 'smooth' });
      }
    });
  });

  /* ── Lazy loading fallback ───────────────────── */
  if ('loading' in HTMLImageElement.prototype === false) {
    document.querySelectorAll('img[loading="lazy"]').forEach(img => {
      img.src = img.dataset.src || img.src;
    });
  }

})();

/* ═══════════════════════════════════════════════
   HEALTH CALCULATORS — global functions
   (WordPress strips <script> from page content,
    so all calculator logic lives here)
═══════════════════════════════════════════════ */

function hnShowResult(id, html) {
  var el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = html;
  el.style.display = 'block';
}

/* ── BMR Calculator ─────────────────────────── */
window.calcBMR = function () {
  var sex    = document.getElementById('bmr-sex').value;
  var age    = parseFloat(document.getElementById('bmr-age').value);
  var weight = parseFloat(document.getElementById('bmr-weight').value);
  var height = parseFloat(document.getElementById('bmr-height').value);
  if (isNaN(age) || isNaN(weight) || isNaN(height)) return;
  var bmr = sex === 'male'
    ? 10 * weight + 6.25 * height - 5 * age + 5
    : 10 * weight + 6.25 * height - 5 * age - 161;
  var tdee = [
    ['Sedentary (little/no exercise)',          bmr * 1.2],
    ['Lightly active (1–3 days/week)',           bmr * 1.375],
    ['Moderately active (3–5 days/week)',        bmr * 1.55],
    ['Very active (6–7 days/week)',              bmr * 1.725],
    ['Extra active (physical job + exercise)',   bmr * 1.9],
  ];
  var rows = tdee.map(function(t) {
    return '<tr><td>' + t[0] + '</td><td><strong>' + Math.round(t[1]) + ' cal</strong></td></tr>';
  }).join('');
  hnShowResult('bmr-result',
    '<h3>Your BMR: <strong>' + Math.round(bmr) + ' calories/day</strong></h3>' +
    '<p>Add your activity level to get your Total Daily Energy Expenditure (TDEE):</p>' +
    '<table><thead><tr><th>Activity Level</th><th>Daily Calories</th></tr></thead><tbody>' + rows + '</tbody></table>' +
    '<p class="calc-note">Formula: Mifflin-St Jeor — most validated for accuracy (JADA, 2005)</p>');
};

/* ── Protein Calculator ─────────────────────── */
window.calcProtein = function () {
  var weight = parseFloat(document.getElementById('p-weight').value);
  var goal   = document.getElementById('p-goal').value;
  if (isNaN(weight)) return;
  var ranges = {
    'maintain': [1.2, 1.6, 'General health & weight maintenance'],
    'muscle':   [1.6, 2.2, 'Muscle hypertrophy & strength gains'],
    'lose':     [1.8, 2.4, 'Fat loss while preserving lean mass'],
    'athlete':  [1.4, 1.7, 'Endurance performance & recovery'],
  };
  var r = ranges[goal];
  var lo = Math.round(r[0] * weight), hi = Math.round(r[1] * weight);
  hnShowResult('protein-result',
    '<h3>Daily Protein Target: <strong>' + lo + '–' + hi + 'g</strong></h3>' +
    '<p>Goal: <em>' + r[2] + '</em> (' + r[0] + '–' + r[1] + 'g/kg body weight)</p>' +
    '<table><thead><tr><th>Meals per day</th><th>Protein per meal</th></tr></thead><tbody>' +
    '<tr><td>3 meals</td><td>' + Math.round(lo/3) + '–' + Math.round(hi/3) + 'g</td></tr>' +
    '<tr><td>4 meals</td><td>' + Math.round(lo/4) + '–' + Math.round(hi/4) + 'g</td></tr>' +
    '</tbody></table>' +
    '<p class="calc-note">Source: ISSN Position Stand on protein (Stokes et al., 2018)</p>');
};

/* ── Sleep Debt Calculator ──────────────────── */
window.calcSleep = function () {
  var need   = parseFloat(document.getElementById('s-age').value);
  var actual = parseFloat(document.getElementById('s-actual').value);
  var days   = parseFloat(document.getElementById('s-days').value);
  if (isNaN(actual) || isNaN(days)) return;
  var debt     = Math.max(0, (need - actual) * days);
  var status   = debt === 0 ? 'No sleep debt — well done!' : debt < 5 ? 'Mild sleep debt' : debt < 14 ? 'Moderate sleep debt' : 'Significant sleep debt';
  var recovery = debt > 0 ? Math.ceil(debt / 1.5) : 0;
  var html = '<h3>Sleep Debt: <strong>' + debt.toFixed(1) + ' hours</strong></h3>' +
             '<p>Status: <strong>' + status + '</strong></p>';
  if (recovery > 0) {
    html += '<p>Estimated recovery: <strong>' + recovery + ' nights</strong> of full sleep</p>' +
            '<p class="calc-note">You can recover ~1–1.5h of sleep debt per night (Besedovsky et al., 2019)</p>';
  }
  hnShowResult('sleep-result', html);
};

/* ── VO2 Max Estimator ──────────────────────── */
window.calcVO2 = function () {
  var age = parseFloat(document.getElementById('v-age').value);
  var rhr = parseFloat(document.getElementById('v-rhr').value);
  if (isNaN(age) || isNaN(rhr)) return;
  var maxHR = 208 - (0.7 * age);
  var vo2   = Math.round(15 * (maxHR / rhr));
  var cat   = vo2 >= 55 ? 'Superior' : vo2 >= 45 ? 'Excellent' : vo2 >= 38 ? 'Good' : vo2 >= 30 ? 'Fair' : 'Below Average';
  hnShowResult('vo2-result',
    '<h3>Estimated VO2 Max: <strong>' + vo2 + ' mL/kg/min</strong></h3>' +
    '<p>Fitness category: <strong>' + cat + '</strong></p>' +
    '<p>To improve: add 2–3 Zone 2 cardio sessions/week (30–45 min at 60–70% max HR) plus 1 HIIT session.</p>' +
    '<p class="calc-note">Method: Uth et al. (2004) heart rate ratio. For precision, use a lab VO2 max test.</p>');
};

/* ── Hydration Calculator ───────────────────── */
window.calcHydration = function () {
  var weight   = parseFloat(document.getElementById('h-weight').value);
  var activity = parseFloat(document.getElementById('h-activity').value);
  var climate  = parseFloat(document.getElementById('h-climate').value);
  if (isNaN(weight)) return;
  var total   = (weight * 0.033 * activity * climate).toFixed(1);
  var glasses = Math.round(total / 0.25);
  hnShowResult('hydration-result',
    '<h3>Daily Water Target: <strong>' + total + ' litres</strong> (' + glasses + ' glasses)</h3>' +
    '<table><thead><tr><th>When</th><th>How much</th></tr></thead><tbody>' +
    '<tr><td>On waking</td><td>500ml</td></tr>' +
    '<tr><td>Before each meal (×3)</td><td>250ml</td></tr>' +
    '<tr><td>During exercise</td><td>500–750ml/hour</td></tr>' +
    '<tr><td>Rest of day</td><td>Sip consistently</td></tr>' +
    '</tbody></table>' +
    '<p class="calc-note">~20% of daily water comes from food. Coffee and tea count toward intake.</p>');
};

/* ── Macros Calculator ──────────────────────── */
window.calcMacros = function () {
  var cal    = parseFloat(document.getElementById('m-cal').value);
  var weight = parseFloat(document.getElementById('m-weight').value);
  var goal   = document.getElementById('m-goal').value;
  if (isNaN(cal) || isNaN(weight)) return;
  var splits = {
    'maintain': { p: 1.6, f: 0.30 },
    'muscle':   { p: 2.0, f: 0.25 },
    'lose':     { p: 2.2, f: 0.30 },
    'keto':     { p: 1.8, f: 0.70 },
  };
  var s    = splits[goal];
  var pG   = Math.round(s.p * weight);
  var pC   = pG * 4;
  var fC   = Math.round(cal * s.f);
  var fG   = Math.round(fC / 9);
  var cC   = Math.max(0, cal - pC - fC);
  var cG   = Math.round(cC / 4);
  hnShowResult('macros-result',
    '<h3>Your Daily Macros</h3>' +
    '<table><thead><tr><th>Macro</th><th>Grams</th><th>Calories</th><th>% of diet</th></tr></thead><tbody>' +
    '<tr><td><strong>Protein</strong></td><td>' + pG + 'g</td><td>' + pC + '</td><td>' + Math.round(pC/cal*100) + '%</td></tr>' +
    '<tr><td><strong>Carbohydrates</strong></td><td>' + cG + 'g</td><td>' + cC + '</td><td>' + Math.round(cC/cal*100) + '%</td></tr>' +
    '<tr><td><strong>Fat</strong></td><td>' + fG + 'g</td><td>' + fC + '</td><td>' + Math.round(fC/cal*100) + '%</td></tr>' +
    '</tbody></table>');
};
