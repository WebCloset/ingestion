# Reverb Fetcher Module

Fetches listings from the [Reverb API](https://reverb.com/page/api) and upserts them into `item_source` (same table as eBay), so they flow through the same indexer and search.

## Structure

```
reverb/
├── README.md         # This file
├── __init__.py
├── config.py         # REVERB_API_BASE, REVERB_PER_PAGE, REVERB_MAX_PAGES
├── reverb_auth.py    # Bearer token from env
├── fetch_reverb.py   # Main fetcher (listings/all → item_source)
```

## API

- **Endpoint:** `GET https://api.reverb.com/api/listings/all?page=1&per_page=50`
- **Headers:** `Authorization: Bearer <token>`, `Accept: application/hal+json`, `Accept-Version: 3.0`
- **Auth:** Static Bearer token (no OAuth flow). Create a token in Reverb’s API/settings.

## Environment Variables

In `.env` (next to `ingestion-main/`):

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `REVERB_BEARER_TOKEN` | Yes | Reverb API Bearer token |
| `REVERB_PER_PAGE` | No | Page size (default `50`) |
| `REVERB_MAX_PAGES` | No | Max pages to fetch (default `10`) |
| `REVERB_API_BASE` | No | Base URL (default `https://api.reverb.com/api`) |

## Usage

### Run the Reverb fetcher

```bash
cd ingestion-main
python -m reverb.fetch_reverb
```

This will:

1. Call Reverb `listings/all` for each page (up to `REVERB_MAX_PAGES`).
2. Normalize each listing to the same `item_source` columns as eBay (`marketplace_code='reverb'`).
3. UPSERT into Postgres (conflict on `(marketplace_code, source_item_id)`).

### Normalized fields

| item_source column | Reverb source |
|--------------------|----------------|
| marketplace_code   | `"reverb"` |
| source_item_id     | `listing.id` |
| title              | `listing.title` |
| brand              | `listing.make` |
| condition          | `listing.condition.display_name` |
| price_cents        | `listing.price.amount_cents` or `amount * 100` |
| currency           | `listing.price.currency` |
| image_url          | First photo `large_crop` / `full` or `_links.photo.href` |
| seller_url         | `_links.web.href` |

## Notes

- Reverb is geared toward music gear; the same pipeline works for fashion if you add category/query filters when the API supports them.
- Rate limiting: a short delay (`REQUEST_DELAY_SEC`) is applied between pages.
