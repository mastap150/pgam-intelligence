# T1 Cross-tenant leak probe — prod 2026-04-24

## ADMIN GET /api/publishers
HTTP 200 bytes=743
count=1
{"id":1,"tenant_id":1,"org_id":"pgam-test-1","integration_mode":"prebid_s2s","auth_secret_ref":null}

## PUB1 GET /api/publishers
HTTP 200 bytes=743
count=1
{"id":1,"tenant_id":1,"org_id":"pgam-test-1","integration_mode":"prebid_s2s","auth_secret_ref":null}

## PUB9 GET /api/publishers
HTTP 200 bytes=743
count=1
{"id":1,"tenant_id":1,"org_id":"pgam-test-1","integration_mode":"prebid_s2s","auth_secret_ref":null}

## DSP1 GET /api/publishers
HTTP 200 bytes=743
count=1
{"id":1,"tenant_id":1,"org_id":"pgam-test-1","integration_mode":"prebid_s2s","auth_secret_ref":null}

## AM GET /api/publishers
HTTP 200 bytes=743
count=1
{"id":1,"tenant_id":1,"org_id":"pgam-test-1","integration_mode":"prebid_s2s","auth_secret_ref":null}

## FIN GET /api/publishers
HTTP 200 bytes=743
count=1
{"id":1,"tenant_id":1,"org_id":"pgam-test-1","integration_mode":"prebid_s2s","auth_secret_ref":null}
