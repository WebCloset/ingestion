import os, time
import requests, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(".env")
DB  = os.getenv("DATABASE_URL")
TOK = os.getenv("EBAY_BEARER_TOKEN")
if not DB:  raise SystemExit("Missing DATABASE_URL in .env")
if not TOK: raise SystemExit("Missing EBAY_BEARER_TOKEN in .env")

SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

def search_page(q, limit, offset):
    headers = {
        "Authorization": f"Bearer {TOK}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }
    params = {"q": q, "limit": limit, "offset": offset}
    r = requests.get(SEARCH_URL, headers=headers, params=params, timeout=25)
    if r.status_code >= 400:
        raise SystemExit(f"Browse error {r.status_code}: {r.text[:400]}")
    return r.json().get("itemSummaries", []) or []

def norm(item):
    price = item.get("price") or {}
    img   = (item.get("image") or {}).get("imageUrl") \
         or (item.get("thumbnailImages") or [{}])[0].get("imageUrl") or ""
    return {
        "id":        item.get("itemId",""),
        "title":     item.get("title",""),
        "brand":     item.get("brand","") or "",
        "condition": item.get("condition","") or "",
        "price_cents": int(round(float(price.get("value",0.0))*100)),
        "currency":  price.get("currency","USD"),
        "image_url": img,
        "seller_url": item.get("itemWebUrl",""),
    }

def upsert(rows):
    rows = [r for r in rows if r["id"]]
    if not rows: return 0
    values = [(r["id"], r["title"], r["brand"], r["condition"],
               r["price_cents"], r["currency"], r["image_url"], r["seller_url"])
              for r in rows]
    conn = psycopg2.connect(DB, sslmode="require"); cur = conn.cursor()
    sql = """
    INSERT INTO item_source
      (id, title, brand, condition, price_cents, currency, image_url, seller_url)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
      title=EXCLUDED.title, brand=EXCLUDED.brand, condition=EXCLUDED.condition,
      price_cents=EXCLUDED.price_cents, currency=EXCLUDED.currency,
      image_url=EXCLUDED.image_url, seller_url=EXCLUDED.seller_url;
    """
    execute_values(cur, sql, values, page_size=200)
    conn.commit(); cur.close(); conn.close()
    return len(values)

def run(q, target=200, page_size=100):
    out=[]; 
    for offset in range(0, target, page_size):
        batch = search_page(q, page_size, offset)
        out.extend(norm(b) for b in batch)
        if len(batch) < page_size: break
        time.sleep(0.4)
    uniq = {r["id"]: r for r in out if r["id"]}
    return upsert(list(uniq.values()))

if __name__ == "__main__":
    total=0
    for q in ["vintage jacket","levis jeans","nike hoodie","patagonia fleece"]:
        n=run(q, target=200, page_size=100)
        print(f"[{q}] upserted {n}"); total+=n
    print("TOTAL upserted:", total)