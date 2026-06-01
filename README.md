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

