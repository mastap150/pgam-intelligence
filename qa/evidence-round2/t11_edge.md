# T11 — Edge cases / injection / rate limits

## T11.A SQL injection in basics.name — SAFE
POST body contained `"name":"QA R2 INJ'; DROP TABLE pgam_direct.publisher_configs; --"`.

Result: HTTP 201. Row created with id=5, the name stored as literal text.
List returns 5 publishers afterwards — table still alive. Parameterisation
is working correctly.

## T11.B Oversized name (10,000 chars) — REJECTED
HTTP 400 `too_big max 120` on `basics.name`. Good.

## T11.C Negative floor_usd — REJECTED
HTTP 400 `too_small min 0` on `inventory[0].placements[0].floor_usd`. Good.

## T11.D rev_share_default_pct=99 — REJECTED
HTTP 400 `too_big max 95`. Good.

## T11.E direct_rtb without HMAC AND without IP allowlist — REJECTED
HTTP 400 custom: "Direct RTB mode requires at least one of HMAC signing or
an IP allow-list." Good — superRefine fires.

## T11.F Rate limit — authenticated
30 rapid GET /api/publishers as internal_admin → all 200. NO rate limiting.

## T11.G Rate limit — unauthenticated
30 rapid GET /api/auth/me → all 401 (no throttle envelope, no 429).
NO rate limiting on unauthenticated endpoints either — an attacker can
brute-force session-cookie candidates without backoff.

## T11.H Malformed JSON
HTTP 400 `{"error":"BAD_JSON","detail":"SyntaxError: Expected property name
or '}' in JSON at position 1 (line 1 column 2)"}`.

Minor leak — `detail` field surfaces the raw Node `SyntaxError` string.
Not exploitable on its own, but `detail` should be stripped in prod to
avoid revealing parser internals.

## T11.I XSS payload in name — stored as literal
`<script>alert(1)</script>` accepted, stored, and reflected verbatim in
GET responses. React's default escaping will block execution in the
dashboard render path, but any downstream consumer that `innerHTML`s the
name (CSV export, logs viewer, email template) would be exposed.
Recommend server-side rejection or stripping of control characters in
customer-visible name fields.
