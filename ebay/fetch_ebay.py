"""
eBay fetcher (Browse API) — Phase 3/4

- Queries eBay Browse item_summary/search for fashion categories.
- Normalizes results and UPSERTs rows into Neon Postgres `item_source`.
- DoD: >= 100 rows in item_source after a run.

Env (see ../.env or README):
  DATABASE_URL=postgresql://...
  EBAY_APP_ID=<client_id>
  EBAY_CERT_ID=<client_secret>
  EBAY_ENV=PRODUCTION
  EBAY_MARKETPLACE=EBAY_GB   # optional; default EBAY_GB
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed

# ---- config ----

# Marketplace‑agnostic default: run on generic queries; categories optional via env.
FASHION_CATEGORY_IDS: List[str] = []
DEFAULT_QUERIES: List[str] = ["dress", "jeans", "sneakers", "handbag", "coat", "jacket"]

PER_PAGE = 50           # Browse API page size (max 200). Keep modest to be polite.
MAX_PAGES_PER_CAT = 3   # Used only if categories are provided (via env).
REQUEST_DELAY_SEC = 0.2  # be polite to API

EBAY_BROWSE_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

# ---- load env ----

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

DATABASE_URL = os.getenv("DATABASE_URL")
EBAY_APP_ID = os.getenv("EBAY_APP_ID")
EBAY_CERT_ID = os.getenv("EBAY_CERT_ID")
EBAY_ENV = os.getenv("EBAY_ENV", "PRODUCTION")
EBAY_MARKETPLACE = os.getenv("EBAY_MARKETPLACE", "EBAY_GB")

# Optional: marketplace-specific categories from env, e.g. EBAY_CATEGORY_IDS="11450,169291"
_env_cat = os.getenv("EBAY_CATEGORY_IDS", "").strip()
if _env_cat:
    FASHION_CATEGORY_IDS = [c.strip() for c in _env_cat.split(",") if c.strip()]

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

if not EBAY_APP_ID or not EBAY_CERT_ID:
    raise RuntimeError("EBAY_APP_ID / EBAY_CERT_ID not set (wait for eBay approval)")

# ---- token handling ----
# We keep token logic simple here; you can also move this into `ebay/token.py` and import it.

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def get_bearer_token() -> str:
    """
    Client-credentials OAuth for eBay APIs.
    """
    oauth_url = "https://api.ebay.com/identity/v1/oauth2/token" if EBAY_ENV.upper() == "PRODUCTION" else "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
    auth = (EBAY_APP_ID, EBAY_CERT_ID)
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(oauth_url, auth=auth, data=data, headers=headers, timeout=15)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Failed to obtain access_token: {resp.text}")
    return token

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

# ---- eBay API ----

def build_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": EBAY_MARKETPLACE,
    }

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def browse_search(token: str, q: str, category_ids: List[str], limit: int = 30, offset: int = 0) -> Dict[str, Any]:
    params = {
        "q": q,
        "limit": str(limit),
        "offset": str(offset),
        "fieldgroups": "ASPECT_REFINEMENTS",
    }
    if category_ids:
        params["category_ids"] = ",".join(category_ids)
    resp = requests.get(EBAY_BROWSE_URL, headers=build_headers(token), params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def browse_search_category(token: str, category_ids: List[str], limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    params = {
        "category_ids": ",".join(category_ids),
        "limit": str(limit),
        "offset": str(offset),
        "fieldgroups": "ASPECT_REFINEMENTS",
    }
    resp = requests.get(EBAY_BROWSE_URL, headers=build_headers(token), params=params, timeout=20)
    if not resp.ok:
        raise RuntimeError(f"eBay Browse error {resp.status_code}: {resp.text}")
    return resp.json()

def _first_image(item: Dict[str, Any]) -> Optional[str]:
    img = (item.get("image") or {}).get("imageUrl")
    if img:
        return img
    thumbs = item.get("thumbnailImages") or []
    if thumbs and isinstance(thumbs, list):
        url = (thumbs[0] or {}).get("imageUrl")
        if url:
            return url
    return None

def _brand(item: Dict[str, Any]) -> Optional[str]:
    # eBay may return "brand" directly or within item aspects; try both.
    if "brand" in item and isinstance(item.get("brand"), str):
        return item.get("brand")
    # aspects might look like {"brand": ["Nike"]} in some APIs; Browse may include in "additionalImages"/"localizedAspects"
    aspects = item.get("localizedAspects") or []
    for asp in aspects:
        if str(asp.get("name", "")).lower() == "brand":
            vals = asp.get("values") or []
            if vals:
                return str(vals[0])
    return None

def normalize_item(it: Dict[str, Any]) -> Dict[str, Any]:
    price = (it.get("price") or {})
    value = price.get("value")
    currency = price.get("currency")
    price_cents = None
    try:
        if value is not None:
            price_cents = int(round(float(value) * 100))
    except Exception:
        price_cents = None

    return {
        "marketplace_code": "ebay",
        "source_item_id": it.get("itemId") or it.get("legacyItemId") or it.get("item_id"),
        "title": it.get("title") or "",
        "brand": _brand(it) or None,
        "condition": it.get("condition") or None,
        "price_cents": price_cents,
        "currency": currency or None,
        "image_url": _first_image(it),
        "seller_url": it.get("itemWebUrl") or it.get("itemUrl") or None,
    }

def fetch_batch(token: str, queries: List[str], per_query_limit: int = PER_PAGE) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    # 1) Try category mode if categories are provided (via env).
    if FASHION_CATEGORY_IDS:
        try:
            for page in range(MAX_PAGES_PER_CAT):
                offset = page * PER_PAGE
                data = browse_search_category(token, category_ids=FASHION_CATEGORY_IDS, limit=PER_PAGE, offset=offset)
                items = data.get("itemSummaries") or []
                for it in items:
                    doc = normalize_item(it)
                    if doc["source_item_id"] and doc["title"] and doc["seller_url"]:
                        rows.append(doc)
                time.sleep(REQUEST_DELAY_SEC)
        except Exception as e:
            print(f"⚠ Category fetch failed ({e}); falling back to query mode…")

    # 2) Query mode always runs if categories not supplied or yielded nothing.
    if not rows:
        seeds = queries or DEFAULT_QUERIES
        for q in seeds:
            data = browse_search(token, q=q, category_ids=FASHION_CATEGORY_IDS, limit=per_query_limit, offset=0)
            items = data.get("itemSummaries") or []
            for it in items:
                doc = normalize_item(it)
                if doc["source_item_id"] and doc["title"] and doc["seller_url"]:
                    rows.append(doc)
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
    print("→ Obtaining eBay token…")
    token = get_bearer_token()

    queries = DEFAULT_QUERIES
    if FASHION_CATEGORY_IDS:
        print(f"→ Category mode: {FASHION_CATEGORY_IDS} | marketplace={EBAY_MARKETPLACE} | {MAX_PAGES_PER_CAT}×{PER_PAGE}")
    else:
        print(f"→ Query mode: {len(queries) or len(DEFAULT_QUERIES)} seeds | marketplace={EBAY_MARKETPLACE}")

    rows = fetch_batch(token, queries, PER_PAGE)
    print(f"→ Normalized {len(rows)} rows; upserting into Postgres…")
    n = upsert_rows(rows)
    print(f"✓ Upserted {n} rows into item_source")

    if n < 100:
        print("⚠ Warning: fewer than 100 rows inserted; consider adding more queries or increasing PER_QUERY_LIMIT")

if __name__ == "__main__":
    main()
