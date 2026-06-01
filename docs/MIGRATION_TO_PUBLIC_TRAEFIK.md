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

## GitHub Actions deploy

The repository includes `.github/workflows/deploy.yml`, intentionally matching the `akiba-hs/public-traefik` deployment pattern:

1. On every push to `main` or `master`, GitHub Actions builds `linux/amd64` with Buildx.
2. The image is pushed to GHCR as:
   - `ghcr.io/akiba-hs/payanyway-to-airtable:latest`
   - `ghcr.io/akiba-hs/payanyway-to-airtable:<short-sha>`
3. The workflow writes a production `.env` from repository secrets.
4. `humbertocrispim/ssh-docker-compose-action@v1.0.0` deploys `compose.yml` to `public.akiba.space` with the compose prefix `payanyway-to-airtable` and `pull: true`.

Required GitHub repository secrets:

- `AKIBA_PUBLIC_SSH_PRIVATE_KEY`
- `AKIBA_PUBLIC_SSH_USER`
- `PAYANYWAY_HOST`
- `MNT_ID`
- `MNT_INTEGRITY_CODE`
- `AIRTABLE_API_KEY`
- `AIRTABLE_BASE_ID`
- `AUTH_URL`
- `PUBLIC_KEY` — may be stored as a multiline PEM or as a single line with literal `\n`; the workflow normalizes it for Compose
- optional `AIRTABLE_TABLE_NAME` (defaults to `Payments`)

## Manual deploy

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

Update/redeploy via GitHub Actions: merge/push to `main` after the required repository secrets are set. The workflow rebuilds the image, pushes it to GHCR, and redeploys `compose.yml` on `public.akiba.space`.

Manual update/redeploy on the host:

```bash
cd /opt/payanyway-to-airtable
git pull --ff-only
docker compose pull
docker compose up -d --build
docker compose ps
```

Logs:

```bash
cd /opt/payanyway-to-airtable
docker compose logs -f --tail=200 app
```
