# payanyway-to-airtable

Updates payment status in Airtable when PayAnyWay sends a completed payment notification. Also renders a small authenticated invoice list for residents.

## Environment variables

```dotenv
PAYANYWAY_HOST        # public host exposed by public-traefik, for example payments.akiba.space
MNT_ID
MNT_INTEGRITY_CODE
AIRTABLE_API_KEY
AIRTABLE_BASE_ID
AIRTABLE_TABLE_NAME  # optional, defaults to "Payments"
AUTH_URL             # auth service URL; the app appends ?redirect_uri=...&error=...
PUBLIC_KEY           # JWT public key, use literal \n for line breaks
```

## Endpoints

- `GET /` — shows a simple HTML table with invoices for an authorized user.
- `GET /webhook` with no query/body — health check, returns `OK`.
- `POST /webhook` — PayAnyWay notification handler.

## Local run

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

## Docker deployment behind `akiba-hs/public-traefik`

The repository includes `Dockerfile` and `compose.yml` for running on the same Docker host as [`akiba-hs/public-traefik`](https://github.com/akiba-hs/public-traefik). The Traefik instance already enables the Docker provider with `exposedByDefault: false`; this app opts in via labels in `compose.yml`.

```bash
cp .env.example .env
chmod 600 .env
$EDITOR .env

docker compose config
docker compose up -d --build
docker compose ps
```

The public PayAnyWay webhook URL is:

```text
https://$PAYANYWAY_HOST/webhook
```

## Automated deploy

`.github/workflows/deploy.yml` mirrors the deploy workflow in `akiba-hs/public-traefik`: on push to `main`/`master` it builds an amd64 image, pushes it to GHCR as `latest` and the short commit SHA, generates `.env` from GitHub Secrets, and deploys `compose.yml` to `public.akiba.space` via SSH.

Required repository secrets:

- `AKIBA_PUBLIC_SSH_PRIVATE_KEY`
- `AKIBA_PUBLIC_SSH_USER`
- `PAYANYWAY_HOST`
- `MNT_ID`
- `MNT_INTEGRITY_CODE`
- `AIRTABLE_API_KEY`
- `AIRTABLE_BASE_ID`
- `AUTH_URL`
- `PUBLIC_KEY`
- optional `AIRTABLE_TABLE_NAME` (defaults to `Payments`)

See [`docs/MIGRATION_TO_PUBLIC_TRAEFIK.md`](docs/MIGRATION_TO_PUBLIC_TRAEFIK.md) for the full migration, deploy, and rollback runbook.
