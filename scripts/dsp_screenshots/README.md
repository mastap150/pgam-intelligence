# DSP self-serve screenshot capture

Captures a full-page screenshot of every route in the `pgam-dsp-dashboard`
self-serve app (`(self-serve)` route group — the `/ss-*` pages behind
`demo.dsp.pgammedia.com`) and builds a single self-contained HTML
inventory for design handoff.

The live demo host is **not reachable from Claude Code cloud sessions**
(blocked by the egress policy), so this runs the app locally in demo mode
instead. Demo mode is entirely fixture-driven — no Neon, SpringServe or
PubMatic credentials are needed.

## Run it

```bash
git clone https://github.com/mastap150/pgam-dsp-dashboard
cd pgam-dsp-dashboard && npm ci

# demo mode keys off a `demo.*` hostname
echo "127.0.0.1 demo.localhost" | sudo tee -a /etc/hosts

cat > .env.local <<'EOF'
DEMO_COOKIE_SECRET=any-local-value
EOF

npx next dev -p 3000
```

Then, from this directory:

```bash
export DEMO_COOKIE_SECRET=any-local-value   # must match .env.local
npm i playwright                            # or point CHROMIUM_PATH at an existing build
node capture.mjs                            # → ./png/*.png + ./results.json
python3 compress.py                         # → ./web + ./thumb (JPEG)
python3 build_doc.py                        # → ./attune-screen-inventory.html
```

Env knobs: `BASE_URL` (default `http://demo.localhost:3000`), `OUT_DIR`
(default `./png`), `CHROMIUM_PATH` (default: Playwright's own download).

## How the demo gate works

`src/lib/demo/demo-mode.ts` activates demo mode when **both** hold:

1. the hostname starts with `demo.` (`demo.localhost` is explicitly allowed), and
2. a valid `pgam_demo_auth` cookie is present — an HMAC-SHA256 token
   signed with `DEMO_COOKIE_SECRET`, minted by `capture.mjs`.

A second, non-HttpOnly `pgam_demo=1` marker cookie tells the client-side
`fetchApi` to route through the fixture dispatcher.

## Notes on the output

- Shots are 1440×900 CSS px at `deviceScaleFactor: 2`, full scroll height.
- Animations and transitions are zeroed so runs are deterministic.
- The Next dev-overlay (`nextjs-portal`) is hidden before capture.
- Dynamic routes use real fixture ids — `ss-cmp-001`, `ss-cr-001`. Guessing
  ids (`cmp-001`) silently yields a "Campaign not found" page that still
  returns HTTP 200, so `capture.mjs` flags empty/not-found bodies.
- Three routes legitimately render sparse: `/ss-campaigns/url` is a
  single-input page, `/ss-creatives/video-ai` ships an "unavailable" state,
  and `/ss-creatives/[id]` has no demo fixture (skeletons only).
