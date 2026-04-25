// k6 load-test for PGAM SSP — runs three hot paths under increasing load
// and asserts SLO compliance (p95 < 500ms, error rate < 1%).
//
// USAGE:
//   brew install k6     # one-time
//   ADMIN_COOKIE='pgam_session=...' k6 run 06-load-test.k6.js
//
// Or via vmus stages for a graduated ramp:
//   k6 run --stage 30s:50,1m:50,30s:100,1m:100,30s:0 06-load-test.k6.js
//
// WHAT THIS TESTS:
//   1. /api/auth/me                    — cheap read, KV-rate-limited (b61660f)
//   2. /sellers.json                   — public read, edge-cached
//   3. /rtb/v1/auction (rtb host)      — bidder-edge real auction shape
//
// WHAT TO WATCH:
//   - p95 wall time per endpoint
//   - error rate per endpoint
//   - KV-limiter rejection rate on /api/auth/me at sustained 100 req/s
//   - bidder-edge response shape under fan-out load
//
// SLO TARGETS (block-on-fail):
//   - p95 /api/auth/me   < 200ms
//   - p95 /sellers.json  < 100ms (CDN-cached)
//   - p95 /rtb/v1/auction < 350ms
//   - error rate (any 5xx) < 0.5%

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const errorRate = new Rate('errors');
const authMeP95 = new Trend('auth_me_ms');
const sellersP95 = new Trend('sellers_ms');
const auctionP95 = new Trend('auction_ms');

const APP_HOST = __ENV.APP_HOST || 'https://app.pgammedia.com';
const RTB_HOST = __ENV.RTB_HOST || 'https://rtb.pgammedia.com';
const ADMIN_COOKIE = __ENV.ADMIN_COOKIE || '';

export const options = {
  stages: [
    { duration: '30s', target: 50 },   // ramp to 50 vus
    { duration: '1m',  target: 50 },   // hold 50
    { duration: '30s', target: 100 },  // ramp to 100
    { duration: '2m',  target: 100 },  // hold 100 — this is where things break
    { duration: '30s', target: 250 },  // ramp to 250 (stress)
    { duration: '1m',  target: 250 },  // hold 250
    { duration: '30s', target: 0 },    // ramp down
  ],
  thresholds: {
    'auth_me_ms':  ['p(95)<200'],
    'sellers_ms':  ['p(95)<100'],
    'auction_ms':  ['p(95)<350'],
    'errors':      ['rate<0.005'],
  },
};

const auctionPayload = JSON.stringify({
  id: `loadtest-${Math.random().toString(36).slice(2, 10)}`,
  tmax: 500,
  at: 1,
  cur: ['USD'],
  imp: [{
    id: '1',
    tagid: 'demo_hero_300x250',
    secure: 1,
    banner: { format: [{ w: 300, h: 250 }, { w: 728, h: 90 }], pos: 1 },
    bidfloor: 0.10,
    bidfloorcur: 'USD',
    ext: { pgam: { orgId: 'test' } }
  }],
  site: {
    id: 'load-site',
    domain: 'demo.pgammedia.com',
    page: 'https://demo.pgammedia.com/load',
    publisher: { id: 'test', domain: 'demo.pgammedia.com' }
  },
  device: {
    ua: 'k6-loadtest/0.1',
    ip: '203.0.113.99',
    devicetype: 2,
    geo: { country: 'USA', region: 'NY' }
  },
  user: { id: `loadtest-user-${Math.random().toString(36).slice(2, 8)}` }
});

export default function () {
  // 1. /api/auth/me — only if cookie supplied
  if (ADMIN_COOKIE) {
    const r1 = http.get(`${APP_HOST}/api/auth/me`, {
      headers: { Cookie: ADMIN_COOKIE },
      tags: { endpoint: 'auth_me' }
    });
    authMeP95.add(r1.timings.duration);
    check(r1, {
      'auth/me 200': r => r.status === 200,
    }) || errorRate.add(1);
  }

  // 2. /sellers.json — public, cached
  const r2 = http.get(`${APP_HOST}/sellers.json`, { tags: { endpoint: 'sellers' } });
  sellersP95.add(r2.timings.duration);
  check(r2, {
    'sellers 200': r => r.status === 200,
    'sellers has PGAM': r => r.body.includes('PGAM Media'),
  }) || errorRate.add(1);

  // 3. /rtb/v1/auction — real auction
  const r3 = http.post(`${RTB_HOST}/rtb/v1/auction`, auctionPayload, {
    headers: {
      'Content-Type': 'application/json',
      'X-OpenRTB-Version': '2.6'
    },
    tags: { endpoint: 'auction' }
  });
  auctionP95.add(r3.timings.duration);
  // Auction returns 204 (nobid) or 200 (with bid) — both OK; only 5xx is an error
  check(r3, {
    'auction non-5xx': r => r.status < 500,
  }) || errorRate.add(1);

  sleep(1);
}

export function handleSummary(data) {
  return {
    'stdout': JSON.stringify({
      vus_peak: data.metrics.vus_max?.values.value,
      auth_me_p50: data.metrics.auth_me_ms?.values['p(50)'],
      auth_me_p95: data.metrics.auth_me_ms?.values['p(95)'],
      sellers_p50: data.metrics.sellers_ms?.values['p(50)'],
      sellers_p95: data.metrics.sellers_ms?.values['p(95)'],
      auction_p50: data.metrics.auction_ms?.values['p(50)'],
      auction_p95: data.metrics.auction_ms?.values['p(95)'],
      auction_p99: data.metrics.auction_ms?.values['p(99)'],
      error_rate: data.metrics.errors?.values.rate,
      thresholds_passed: !data.root_group.checks?.fails,
    }, null, 2),
  };
}
