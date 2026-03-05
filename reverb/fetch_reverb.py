from __future__ import annotations

"""
Reverb fetcher — ingest listings into Postgres `item_source`.

- Calls Reverb `/api/listings/all` with a Bearer token.
- Normalizes results into our generic item_source schema, alongside eBay.

NOTE:
- This version uses hard-coded configuration values instead of environment variables.
- Update `DATABASE_URL` and `REVERB_API_TOKEN` below with your actual values.
"""

import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

# ---- config ----

REVERB_API_URL = "https://api.reverb.com/api/listings/all"

# Hard‑coded configuration — replace with your actual values.
# Example:
#   DATABASE_URL = "postgresql://user:password@host:5432/dbname"
#   REVERB_API_TOKEN = "your-reverb-bearer-token"
DATABASE_URL = "postgresql://neondb_owner:npg_5LdJSKuC8bFY@ep-damp-field-aey694y3-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
REVERB_API_TOKEN = "c3377b241666dd53480d3e88ba65be07c7cfbdb6fd04b90d69089234096b7a4e"

# Pagination / politeness settings
REVERB_PER_PAGE = 50
REVERB_MAX_PAGES = 3
REQUEST_DELAY_SEC = 0.2


# ---- DB helpers ----

def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


UPSERT_SQL = """
INSERT INTO item_source (
  marketplace_code, source_item_id, title, brand, condition, price_cents, currency, image_url, seller_url
)
VALUES (
  %(marketplace_code)s, %(source_item_id)s, %(title)s, %(brand)s, %(condition)s, %(price_cents)s, %(currency)s, %(image_url)s, %(seller_url)s
)
ON CONFLICT (marketplace_code, source_item_id) DO UPDATE SET
  title = EXCLUDED.title,
  brand = EXCLUDED.brand,
  condition = EXCLUDED.condition,
  price_cents = EXCLUDED.price_cents,
  currency = EXCLUDED.currency,
  image_url = EXCLUDED.image_url,
  seller_url = EXCLUDED.seller_url;
"""


# ---- Reverb API ----

def _base_headers() -> Dict[str, str]:
    return {
        "Accept": "application/hal+json",
        "Accept-Version": "3.0",
        "Authorization": f"Bearer {REVERB_API_TOKEN}",
    }


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_page(page: int, per_page: int) -> Dict[str, Any]:
    params = {"page": page, "per_page": per_page}
    resp = requests.get(REVERB_API_URL, headers=_base_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _first_image(listing: Dict[str, Any]) -> Optional[str]:
    photos = listing.get("photos") or []
    if photos and isinstance(photos, list):
        links = (photos[0] or {}).get("_links") or {}
        full = (links.get("full") or {}).get("href")
        if full:
            return full
        large = (links.get("large_crop") or {}).get("href")
        if large:
            return large
    links = listing.get("_links") or {}
    photo = (links.get("photo") or {}).get("href")
    return photo


def _price_cents(price: Dict[str, Any] | None) -> Optional[int]:
    if not price:
        return None
    amount = price.get("amount")
    if amount is None:
        return None
    try:
        return int(round(float(amount) * 100))
    except Exception:
        return None


def normalize_listing(listing: Dict[str, Any]) -> Dict[str, Any]:
    price = listing.get("price") or {}
    condition = (listing.get("condition") or {}).get("display_name") or None
    links = listing.get("_links") or {}
    web_url = (links.get("web") or {}).get("href")

    return {
        "marketplace_code": "reverb",
        "source_item_id": str(listing.get("id")),
        "title": listing.get("title") or "",
        "brand": listing.get("make") or None,
        "condition": condition,
        "price_cents": _price_cents(price),
        "currency": price.get("currency") or None,
        "image_url": _first_image(listing),
        "seller_url": web_url,
    }


def fetch_all_listings(max_pages: int, per_page: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        data = fetch_page(page, per_page)
        listings = data.get("listings") or []
        if not listings:
            break
        for raw in listings:
            doc = normalize_listing(raw)
            if doc["source_item_id"] and doc["title"] and doc["seller_url"]:
                rows.append(doc)
        total_pages = int(data.get("total_pages") or page)
        page += 1
        if page > total_pages:
            break
        time.sleep(REQUEST_DELAY_SEC)
    return rows


def upsert_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=100)
    return len(rows)


def main():
    print("→ Fetching Reverb listings…")
    rows = fetch_all_listings(max_pages=REVERB_MAX_PAGES, per_page=REVERB_PER_PAGE)
    print(f"→ Normalized {len(rows)} Reverb rows; upserting into Postgres…")
    n = upsert_rows(rows)
    print(f"✓ Upserted {n} Reverb rows into item_source (marketplace_code='reverb')")


if __name__ == "__main__":
    main()

