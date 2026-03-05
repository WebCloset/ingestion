"""
Reverb fetcher — listings/all API

- GET https://api.reverb.com/api/listings/all?page=1&per_page=50
- Headers: Authorization: Bearer <token>, Accept: application/hal+json, Accept-Version: 3.0
- Normalizes results and UPSERTs into Neon Postgres `item_source` (same schema as eBay).

This version uses hard-coded DATABASE_URL and Reverb token (see `reverb_auth.py`),
so you do not need a .env file for ingestion.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from .config import REVERB_LISTINGS_URL, REVERB_MAX_PAGES, REVERB_PER_PAGE
from .reverb_auth import auth_headers

# ---- static config ----

# Direct Postgres URL for Neon (replaces .env-based configuration).
DATABASE_URL = (
    "postgresql://neondb_owner:npg_5LdJSKuC8bFY@"
    "ep-damp-field-aey694y3-pooler.c-2.us-east-2.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)

REQUEST_DELAY_SEC = 0.3  # be polite to API

# ---- DB (same as eBay) ----

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

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_listings_page(page: int = 1, per_page: int = REVERB_PER_PAGE) -> Dict[str, Any]:
    """GET /api/listings/all with pagination."""
    params = {"page": page, "per_page": per_page}
    resp = requests.get(
        REVERB_LISTINGS_URL,
        headers=auth_headers(),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _first_image_url(listing: Dict[str, Any]) -> Optional[str]:
    """First photo URL: photos[0]._links.large_crop.href or _links.photo.href."""
    photos = listing.get("photos") or []
    if photos and isinstance(photos, list):
        first = photos[0]
        if isinstance(first, dict):
            links = first.get("_links") or {}
            for key in ("large_crop", "full", "thumbnail"):
                href = (links.get(key) or {}).get("href")
                if href:
                    return href
    links = listing.get("_links") or {}
    return (links.get("photo") or {}).get("href")


def _seller_url(listing: Dict[str, Any]) -> Optional[str]:
    """Web listing URL."""
    links = listing.get("_links") or {}
    return (links.get("web") or {}).get("href")


def _condition_display(listing: Dict[str, Any]) -> Optional[str]:
    """Condition display name."""
    cond = listing.get("condition")
    if isinstance(cond, dict):
        return cond.get("display_name")
    return None


def normalize_listing(listing: Dict[str, Any]) -> Dict[str, Any]:
    """Map a Reverb listing to item_source row (same shape as eBay)."""
    price_obj = listing.get("price") or listing.get("buyer_price") or {}
    amount_cents = price_obj.get("amount_cents")
    if amount_cents is None and price_obj.get("amount") is not None:
        try:
            amount_cents = int(round(float(price_obj["amount"]) * 100))
        except (TypeError, ValueError):
            amount_cents = None

    source_id = listing.get("id")
    if source_id is not None:
        source_id = str(source_id)

    return {
        "marketplace_code": "reverb",
        "source_item_id": source_id or "",
        "title": (listing.get("title") or "").strip() or None,
        "brand": (listing.get("make") or "").strip() or None,
        "condition": _condition_display(listing),
        "price_cents": amount_cents,
        "currency": (price_obj.get("currency") or "USD")[:3] if price_obj else None,
        "image_url": _first_image_url(listing),
        "seller_url": _seller_url(listing) or "",
    }


def fetch_batch(max_pages: int = REVERB_MAX_PAGES, per_page: int = REVERB_PER_PAGE) -> List[Dict[str, Any]]:
    """Fetch multiple pages of listings and return normalized rows for item_source."""
    rows: List[Dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        data = fetch_listings_page(page=page, per_page=per_page)
        listings = data.get("listings") or []
        for listing in listings:
            doc = normalize_listing(listing)
            if doc["source_item_id"] and doc["title"] and doc["seller_url"]:
                rows.append(doc)
        if page < max_pages:
            time.sleep(REQUEST_DELAY_SEC)
    return rows


def upsert_rows(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=100)
        conn.commit()
    return len(rows)


def main():
    print("→ Fetching Reverb listings…")
    rows = fetch_batch(max_pages=REVERB_MAX_PAGES, per_page=REVERB_PER_PAGE)
    print(f"→ Normalized {len(rows)} rows; upserting into Postgres…")
    n = upsert_rows(rows)
    print(f"✓ Upserted {n} rows into item_source (marketplace=reverb)")

    if n < 50:
        print("⚠ Warning: fewer than 50 rows; check REVERB_BEARER_TOKEN and API access.")


if __name__ == "__main__":
    main()
