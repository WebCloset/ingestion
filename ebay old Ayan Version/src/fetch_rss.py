import os, time, re, json, math, urllib.parse
import requests, feedparser, psycopg2
from psycopg2.extras import execute_values
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(".env")
DB=os.getenv("DATABASE_URL")
assert DB, "Missing DATABASE_URL in .env"

UA={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

def rss_url(q):
    return "https://www.ebay.com/sch/i.html?_nkw=" + urllib.parse.quote_plus(q) + "&_rss=1&_sop=10"

def parse_price(html):
    soup=BeautifulSoup(html,"html.parser")
    # JSON-LD first
    for s in soup.select('script[type="application/ld+json"]'):
        try:
            data=json.loads(s.string or "")
            if isinstance(data,dict):
                offer=data.get("offers") or {}
                price=offer.get("price"); cur=offer.get("priceCurrency") or "USD"
                if price: return float(str(price).replace(",","")), cur
            if isinstance(data,list):
                for d in data:
                    offer=(d or {}).get("offers") or {}
                    price=offer.get("price"); cur=offer.get("priceCurrency") or "USD"
                    if price: return float(str(price).replace(",","")), cur
        except Exception:
            pass
    # meta/itemprop fallback
    meta=soup.select_one('[itemprop="price"], meta[itemprop="price"]')
    if meta:
        val=meta.get("content") or meta.get("value") or meta.text
        if val:
            m=re.search(r"[\d\.,]+",val)
            if m: return float(m.group(0).replace(",","")), "USD"
    # $xx.xx in text as last resort
    m=re.search(r"\$\s*([\d,]+(?:\.\d{2})?)", soup.get_text(" ", strip=True))
    if m: return float(m.group(1).replace(",","")), "USD"
    return 0.0,"USD"

def fetch_items(q, target=120):
    url=rss_url(q)
    feed=feedparser.parse(url)
    items=[]
    for e in (feed.entries or [])[:target]:
        link=e.get("link"); title=e.get("title","")
        if not link: continue
        try:
            r=requests.get(link, headers=UA, timeout=20)
            price,cur=parse_price(r.text)
        except Exception:
            price,cur=0.0,"USD"
        items.append({
            "id": link,  # use URL as stable ID
            "title": title,
            "brand": "",
            "condition": "",
            "price_cents": int(round(price*100)),
            "currency": cur,
            "image_url": "",
            "seller_url": link
        })
        time.sleep(0.25)
    return items

def upsert(rows):
    rows=[r for r in rows if r["id"]]
    if not rows: return 0
    vals=[(r["id"],r["title"],r["brand"],r["condition"],r["price_cents"],r["currency"],r["image_url"],r["seller_url"]) for r in rows]
    conn=psycopg2.connect(DB, sslmode="require"); cur=conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS item_source (
      id TEXT PRIMARY KEY,
      title TEXT,
      brand TEXT,
      condition TEXT,
      price_cents INTEGER,
      currency TEXT,
      image_url TEXT,
      seller_url TEXT
    );""")
    sql="""INSERT INTO item_source (id,title,brand,condition,price_cents,currency,image_url,seller_url)
           VALUES %s ON CONFLICT (id) DO UPDATE SET
           title=EXCLUDED.title, brand=EXCLUDED.brand, condition=EXCLUDED.condition,
           price_cents=EXCLUDED.price_cents, currency=EXCLUDED.currency,
           image_url=EXCLUDED.image_url, seller_url=EXCLUDED.seller_url;"""
    execute_values(cur, sql, vals, page_size=150)
    conn.commit(); cur.close(); conn.close()
    return len(vals)

if __name__=="__main__":
    total=0
    for q in ["vintage jacket","levis jeans","nike hoodie","patagonia fleece"]:
        batch=fetch_items(q, target=120)
        n=upsert(batch)
        print(f"[{q}] upserted {n}")
        total+=n
    print("TOTAL upserted:", total)