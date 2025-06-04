# payanyway-to-airtable
Updates status in airtable when payment is complete

## Environment variables

```
MNT_ID
MNT_INTEGRITY_CODE
AIRTABLE_API_KEY
AIRTABLE_BASE_ID
AIRTABLE_TABLE_NAME  # optional, defaults to "Payments"
AUTH_URL             # auth service url
PUBLIC_KEY           # JWT public key, use \n for line breaks

## Endpoints

- `POST /webhook` – PayAnyWay notification handler.
- `GET /` – shows a simple HTML table with invoices for authorized user.
