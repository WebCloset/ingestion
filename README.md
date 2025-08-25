# ingestion
ETL/importers for WebCloset

# WebCloset Ingestion

Ingestion pipeline for WebCloset.  
Responsible for fetching raw marketplace data (eBay first), storing it in Postgres (Neon), and indexing into Elasticsearch.

## Structure

```
ingestion/
  db/
    schema.sql               # Defines tables (item_source, item_canonical, item_links)
  ebay/
    fetch_ebay.py            # Fetches fashion items from eBay API, upserts into item_source
    token.py                 # Gets and caches eBay API OAuth token
  search/
    mappings/products.json   # Elasticsearch mapping for products index
    indexer.py               # Reads from item_source and writes docs to Elasticsearch
  requirements.txt           # Python dependencies
```

## Setup

1. **Clone & install**
   ```bash
   cd ingestion
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Environment variables**
   Copy `.env.example` to `.env` and fill in:
   ```
   DATABASE_URL=postgres://...
   EBAY_APP_ID=<client_id>
   EBAY_CERT_ID=<client_secret>
   EBAY_ENV=PRODUCTION
   ELASTICSEARCH_URL=...
   ES_API_KEY=...
   ELASTICSEARCH_INDEX=products
   ```

3. **Apply DB schema**
   ```bash
   psql "$DATABASE_URL" -f db/schema.sql
   ```

4. **Run eBay fetcher**
   ```bash
   python ebay/fetch_ebay.py
   ```
   DoD: ≥100 rows in `item_source` table.

5. **Run indexer**
   ```bash
   python search/indexer.py
   ```
   DoD: ≥100 docs in `products` index in Elasticsearch.

## Notes

- Scope eBay fetcher to **Clothing, Shoes & Bags** categories.
- Use `updated_at` for freshness when re-running.
- Temporary scrapers (RSS/HTML) are quarantined and not part of this pipeline.