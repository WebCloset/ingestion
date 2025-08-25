WebCloset Ingestion (eBay)

Setup

python3 -m venv .venv source .venv/bin/activate pip install -r requirements.txt cp .env.example .env # then edit with your own values

Initialize DB

python scripts/init_db.py

Fetch options

Fallback (no auth, uses RSS):

python sources/ebay/fetch_rss.py

Finding API (needs EBAY_APP_ID set):

python sources/ebay/fetch_finding.py

Browse API (needs EBAY_BEARER_TOKEN set and Buy/Browse access):

python sources/ebay/fetch_browse_token.py