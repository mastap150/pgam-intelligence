# Reproduction steps for Vadym

Hey Vadym — specific action is updating `is_optimal_price` on a
placement (trying to flip TB's dynamic floor optimizer on across our
placements). Here's a clean reproduction you can run against your own
account in 30 seconds.

## Step 1 — create a token (same for you and us)

```bash
TOKEN=$(curl -s -X POST https://ssp.pgammedia.com/api/create_token \
  -d "email=YOUR_EMAIL&password=YOUR_PASSWORD&time=120" | jq -r .token)
echo $TOKEN
```

## Step 2 — confirm the token + reads work (these succeed for us)

```bash
# List inventories — works
curl -s "https://ssp.pgammedia.com/api/$TOKEN/list_inventory/45" | head -c 200

# Read one placement — works (GeeksForGeeks 300x250)
curl -s "https://ssp.pgammedia.com/api/$TOKEN/placement?placement_id=172" | jq .
```

## Step 3 — the failing call

We want to flip `is_optimal_price=true` on placement 172. Per your
Postman collection this should be `POST /edit_placement_video` (or
`_native`). What we get:

```bash
curl -i -X POST "https://ssp.pgammedia.com/api/$TOKEN/edit_placement_video" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "placement_id=172&is_optimal_price=true"
```

Response we receive:

```
HTTP/2 404
content-type: text/html; charset=UTF-8

<!DOCTYPE html>
<html lang="eng">
  <head>
    <title>Page not Found | PGAM</title>
    ...
```

## Step 4 — proof that the token itself is fine

The same token returns JSON (not HTML) errors on routes that DO exist:

```bash
# Route exists — returns JSON validation error
curl -s -X POST "https://ssp.pgammedia.com/api/$TOKEN/edit_inventory" \
  -d "inventory_id=99999999"
# → {"message":"Inventory not found"}

curl -s -X POST "https://ssp.pgammedia.com/api/$TOKEN/create_inventory" -d ""
# → {"message":"Bad Request","fields":{"title":["The title field is required."], ...}}

# But placement writes return the HTML 404 page instead of JSON:
curl -s -X POST "https://ssp.pgammedia.com/api/$TOKEN/edit_placement_video" \
  -d "placement_id=172&is_optimal_price=true" | head -c 60
# → <!DOCTYPE html><html lang="eng">  <head>    <title>Page n...
```

## What we've tested (all HTML 404 for us)

| Method | Endpoint | Response |
|---|---|---|
| POST | `/edit_placement_video`   | HTML 404 |
| POST | `/edit_placement_native`  | HTML 404 |
| POST | `/edit_placement_banner`  | HTML 404 |
| POST | `/create_placement_video` | HTML 404 |
| POST | `/create_placement_native`| HTML 404 |
| POST | `/edit_placement` (no suffix) | HTML 404 |
| POST | `/placement` | HTML 404 |
| POST | `/update_placement` | HTML 404 |
| POST | `/save_placement` | HTML 404 |
| PUT  | `/placement` | HTML 404 |
| PATCH| `/placement` | HTML 404 |
| POST | `/edit_placement_video` (JSON body) | HTML 404 |
| POST | `/api/edit_placement_video` (token in body) | HTML 404 |

Tested on multiple placement_ids: 172 (GeeksForGeeks), 2698 (OP.gg),
1077 (rough_ros), 6599 (whitepages), 2421–2536 (BoxingNews). All HTML
404.

## The question

Can you run Step 3 against your admin account and share the response?
If it returns a JSON error or success for you, we'll know the route
is registered but gated on our credential. If it also returns HTML
404 for you, then the route isn't deployed on this tenant and needs
to be enabled. Either way that tells us which direction to go.

Our credential is the one you provisioned for
priyesh@pgammedia.com. Thanks!
