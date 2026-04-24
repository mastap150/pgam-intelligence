# T9 — DSP management probes

## T9.A GET /api/dsps (admin)
```
HTTP 200
{"items":[],"limit":50,"offset":0}
```

## T9.B GET /api/dsps/health (admin)
16 DSPs returned from master `dsps` table (verve, amx, onetag, pubmatic, magnite,
loopme, zmaticoo, unruly, illumin, 33across, growintech, stirista, openweb, perion,
synatix, sovrn). All active:true, counters all zero.

## T9.C RBAC matrix — /api/dsps
| Role | HTTP |
|---|---|
| internal_admin | 200 |
| finance | 403 |
| am | 403 |
| publisher | 403 |
| dsp | 403 |

## T9.D RBAC matrix — /api/dsps/health
| Role | HTTP |
|---|---|
| internal_admin | 200 |
| finance | 200 |
| am | 200 |
| publisher | 403 |
| dsp | 200 |

## T9.E/F malformed create → 422 VALIDATION_FAILED
Zod schema requires nested `company / commercial / technical / limits / deploy`
objects with specific subfields (`company.name`, `commercial.take_rate_pct`,
`technical.media_types`, `technical.environments`, `technical.accepted_countries`,
`technical.schain_node_id`, `limits.qps_limit`, `deploy.regions`). Good gating.

## T9.G DSP create — full wizard payload
```
HTTP 201
{"dspId":1,"endpointIds":[2],"contractIds":[3],
 "authSecretRef":"arn:aws:secretsmanager:local:000000000000:secret:pgam/dsp/qa-r2-dsp/auth"}
```

Red flags in the response:
1. `dspId:1` collides with an active DSP — `/api/dsps/health` already returns
   `dsp_id:1 = verve`. Two different ID spaces, confusing.
2. `authSecretRef` region is literally `local` with account `000000000000` —
   **Secrets Manager is NOT wired to AWS**, auth_secret is being silently
   dropped into a stub ARN. This means DSPs configured with bearer/mtls/hmac
   will have no credential storage in prod.

## T9.H DSP readback — create does not persist
```
GET /api/dsps      → 200 {"items":[],"limit":50,"offset":0}
GET /api/dsps/1    → 404 {"error":"NOT_FOUND"}
```

DSP create returns a success envelope with synthetic IDs (1,2,3) but there is
NO persisted row, even though `/api/publishers` persistence was fixed.
This is a **DSP-side repeat of FND-002** (StubDb still backs DSP create/list).
