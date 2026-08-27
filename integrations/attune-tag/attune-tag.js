/*!
 * Attune Tag v1.0.0 — PGAM Media
 *
 * First-party loader for Attune connected-TV attribution. Advertisers install
 * this one file; it owns the vendor tag underneath so the vendor snippet never
 * appears in a client's codebase or install instructions.
 *
 *   <script async src="https://tag.pgammedia.com/attune.js"
 *           data-attune-id="XXXXXX"></script>
 *
 *   attune('event', 'lead');
 *   attune('event', 'lead', { source: 'call' });
 *   attune('event', 'purchase', { price_usd: 49.99, purchase_id: 'order-1' });
 *
 * ---------------------------------------------------------------------------
 * DO NOT PROXY THIS THROUGH A PGAM SERVER.
 *
 * Attribution is IP-based: the ad plays on a household's TV, and the match is
 * made when a browser on that same household IP later hits the tracker. The
 * beacon therefore has to leave the *visitor's* browser. If we relay it through
 * a PGAM origin, the tracker records PGAM's server IP and every conversion
 * goes unattributed. Wrapping the tag is safe; proxying it is not.
 *
 * The vendor host below is consequently reachable in DevTools. This wrapper
 * removes the vendor from the install and from the client's own source — it is
 * not a cloak against inspection.
 * ---------------------------------------------------------------------------
 */
(function (window, document) {
  'use strict';

  var VENDOR_SRC = 'https://tracker.vibe.co/vbpx.js';
  var VENDOR_NS = 'vbpx';
  var PUBLIC_NS = 'attune';

  // Already booted (double-include, or a tag manager fired us twice).
  if (window[PUBLIC_NS] && window[PUBLIC_NS].__attune) return;

  /* ------------------------------------------------------------------ config */

  function readConfig() {
    var cfg = window.__attuneConfig || {};
    var el =
      document.currentScript ||
      document.querySelector('script[data-attune-id]');

    if (el && el.getAttribute) {
      cfg.id = cfg.id || el.getAttribute('data-attune-id');
      // Opt out of automatic tel:/sms: lead tracking with data-attune-calls="off"
      if (el.getAttribute('data-attune-calls') === 'off') cfg.autoCalls = false;
    }
    if (cfg.autoCalls !== false) cfg.autoCalls = true;
    return cfg;
  }

  var config = readConfig();

  function warn(msg) {
    if (window.console && console.warn) console.warn('[attune] ' + msg);
  }

  if (!config.id) {
    warn('no measurement ID found — add data-attune-id to the script tag.');
    return;
  }

  /* ------------------------------------------------------------ vendor loader */

  // Vendor's own loader: creates the global + command queue, then injects the
  // script. Calls made before the script lands are replayed by the vendor.
  function bootVendor() {
    var p = window;
    var x = VENDOR_NS;
    if (p[x]) return;
    p[x] = function () {
      p[x].q.push(arguments);
    };
    p[x].q = [];
    var s = document.createElement('script');
    s.async = 1;
    s.src = VENDOR_SRC;
    s.onerror = function () {
      warn('measurement script failed to load; events will not be recorded.');
    };
    var t = document.getElementsByTagName('script')[0];
    if (t && t.parentNode) t.parentNode.insertBefore(s, t);
    else (document.head || document.documentElement).appendChild(s);
  }

  try {
    bootVendor();
    window[VENDOR_NS]('init', config.id);
  } catch (e) {
    warn('initialisation failed: ' + (e && e.message));
    return;
  }

  /* -------------------------------------------------------------- public API */

  var EVENTS = { pageview: 1, lead: 1, purchase: 1 };

  function attune(command, name, detail) {
    try {
      if (command !== 'event') {
        warn('unknown command "' + command + '".');
        return;
      }
      var evt = String(name || '').toLowerCase();
      if (!EVENTS[evt]) {
        warn('unknown event "' + name + '" — use pageview, lead or purchase.');
        return;
      }
      // `detail` is passed straight through. Only price_usd and purchase_id
      // are read downstream; anything else is inert but harmless.
      if (detail) window[VENDOR_NS]('event', evt, detail);
      else window[VENDOR_NS]('event', evt);
    } catch (e) {
      // Measurement must never take a client's page down with it.
      warn('event failed: ' + (e && e.message));
    }
  }

  attune.__attune = '1.0.0';
  window[PUBLIC_NS] = attune;

  // Replay anything queued before this file arrived.
  try {
    var pending = window.__attuneQueue;
    if (pending && pending.length) {
      for (var i = 0; i < pending.length; i++) attune.apply(null, pending[i]);
      window.__attuneQueue = [];
    }
  } catch (e) {
    warn('queue replay failed: ' + (e && e.message));
  }

  /* ------------------------------------------------- click-to-call as a lead */

  // Catches the mobile half of phone enquiries with no extra work from the
  // advertiser. Desk-phone callers who read the number off the screen are not
  // visible here — those need dynamic number insertion plus a server-side
  // event. See INSTALL.md.
  if (config.autoCalls) {
    document.addEventListener(
      'click',
      function (ev) {
        try {
          var node = ev.target;
          while (node && node !== document) {
            if (node.tagName === 'A' && node.href) {
              if (node.href.indexOf('tel:') === 0) {
                attune('event', 'lead', { source: 'call_click' });
              }
              return;
            }
            node = node.parentNode;
          }
        } catch (e) {
          /* never interfere with the click */
        }
      },
      true
    );
  }
})(window, document);
