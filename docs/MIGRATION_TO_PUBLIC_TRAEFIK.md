# Migration to the `public-traefik` host

This app can run on the same Docker host as [`akiba-hs/public-traefik`](https://github.com/akiba-hs/public-traefik). No change is required in `public-traefik`: it already enables the Docker provider with `exposedByDefault: false`, so this service is exposed through explicit Docker labels in `compose.yml`.

## Target shape

- Host: the existing server running `akiba-hs/public-traefik`.
- App path on the host: `/opt/payanyway-to-airtable`.
- Public hostname: set `PAYANYWAY_HOST` in `.env` (suggested: `payments.akiba.space`).
- Public webhook URL for PayAnyWay: `https://$PAYANYWAY_HOST/webhook`.
- Container entrypoint: `uvicorn main:app --proxy-headers --forwarded-allow-ips=*`.
- Health check: `GET /webhook` with no parameters returns `200 OK`.

## Prerequisites

1. `public-traefik` is already running on the host and has access to `/var/run/docker.sock`.
2. DNS for `PAYANYWAY_HOST` points to this host, or a wildcard under `akiba.space` already covers it.
3. The host has Docker Compose v2.
4. You have the production secrets:
   - `MNT_ID`
   - `MNT_INTEGRITY_CODE`
   - `AIRTABLE_API_KEY`
   - `AIRTABLE_BASE_ID`
   - optional `AIRTABLE_TABLE_NAME` (defaults to `Payments`)
   - `AUTH_URL`
   - `PUBLIC_KEY` as a single line with literal `\n` separators

## Deploy

```bash
sudo mkdir -p /opt/payanyway-to-airtable
sudo chown "$USER":"$USER" /opt/payanyway-to-airtable
git clone https://github.com/akiba-hs/payanyway-to-airtable.git /opt/payanyway-to-airtable
cd /opt/payanyway-to-airtable

cp .env.example .env
chmod 600 .env
$EDITOR .env

docker compose config
docker compose up -d --build
```

## Verify before cutover

```bash
docker compose ps
docker compose logs --tail=100 app

docker compose exec app python - <<'PY'
import urllib.request
print(urllib.request.urlopen('http://127.0.0.1:8000/webhook', timeout=3).read().decode())
PY

curl -fsS "https://$PAYANYWAY_HOST/webhook"
curl -fsSI "https://$PAYANYWAY_HOST/"
```

Expected results:

- `docker compose ps` shows the app as running/healthy.
- Local and public `GET /webhook` return `OK`.
- `GET /` returns either the invoices page for an authenticated user or a redirect to `AUTH_URL`.

## Cutover

1. Set or confirm PayAnyWay callback URL: `https://$PAYANYWAY_HOST/webhook`.
2. Run a PayAnyWay test-mode payment notification if available.
3. Confirm that the corresponding Airtable record is updated to `Test Paid`.
4. Run one production smoke payment only after test-mode verification succeeds.
5. Stop the old deployment after the new webhook has processed a real/test notification successfully.

## Rollback

1. Restore the previous PayAnyWay callback URL, or point DNS back to the previous host.
2. Stop the new container:

```bash
cd /opt/payanyway-to-airtable
docker compose down
```

The migration does not alter Airtable schema or PayAnyWay credentials, so rollback is limited to traffic routing and stopping the new app.

## Operations

Update/redeploy:

```bash
cd /opt/payanyway-to-airtable
git pull --ff-only
docker compose up -d --build
docker compose ps
```

Logs:

```bash
cd /opt/payanyway-to-airtable
docker compose logs -f --tail=200 app
```
