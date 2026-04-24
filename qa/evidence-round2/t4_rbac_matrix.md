# T4 RBAC matrix — prod 2026-04-24
| Route | ADMIN | AM | FIN | PUB1 | DSP1 |
|---|---|---|---|---|---|
| /admin/users | 200 | 200 | 200 | 200 | 200 |
| /admin/floors | 200 | 200 | 200 | 200 | 200 |
| /admin/blocklist | 200 | 200 | 200 | 307 | 307 |
| /admin/publishers | 200 | 200 | 200 | 200 | 200 |
| /admin/dsps | 200 | 200 | 200 | 200 | 200 |
| /admin/deals | 200 | 200 | 200 | 307 | 307 |
| /publishers/new | 200 | 200 | 200 | 200 | 200 |
| /dsps/new | 200 | 404 | 404 | 404 | 404 |
| /rtb-tester | 200 | 200 | 200 | 200 | 200 |
| /api/rbac/allowed-metrics | 200 | 200 | 200 | 200 | 200 |
| /api/dsps | 200 | 403 | 403 | 403 | 403 |
| /api/publishers/1 | 200 | 200 | 200 | 200 | 200 |
