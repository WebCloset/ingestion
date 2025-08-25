import os, math, time
import requests, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
load_dotenv(".env")
DB=os.getenv("DATABASE_URL"); APP=os.getenv("EBAY_APP_ID")
if not DB: raise SystemExit("Missing DATABASE_URL in .env")
if not APP: raise SystemExit("Missing EBAY_APP_ID in .env")
URL="https://svcs.ebay.com/services/search/FindingService/v1"
def _get0(v,d=""): return v[0] if isinstance(v,list) and v else d
def fetch_page(q,per,page):
    r=requests.get(URL,params={
        "OPERATION-NAME":"findItemsByKeywords",
        "SERVICE-VERSION":"1.0.0",
        "SECURITY-APPNAME":APP,
        "RESPONSE-DATA-FORMAT":"JSON",
        "keywords":q,
        "paginationInput.entriesPerPage":per,
        "paginationInput.pageNumber":page
    },timeout=25)
    r.raise_for_status()
    d=r.json(); resp=(d.get("findItemsByKeywordsResponse") or [{}])[0]
    res=(resp.get("searchResult") or [{}])[0]
    return res.get("item",[]) or []
def norm(it):
    c=(it.get("condition") or [{}])[0]
    p=(it.get("sellingStatus") or [{}])[0].get("currentPrice",[{}])[0]
    val=float(p.get("__value__",0.0)); cur=p.get("@currencyId","USD")
    return {"id":_get0(it.get("itemId")),"title":_get0(it.get("title")),"brand":"",
            "condition":_get0(c.get("conditionDisplayName")),
            "price_cents":int(round(val*100)),"currency":cur,
            "image_url":_get0(it.get("galleryURL")),"seller_url":_get0(it.get("viewItemURL"))}
def upsert(rows):
    rows=[r for r in rows if r["id"]]
    if not rows: return 0
    vals=[(r["id"],r["title"],r["brand"],r["condition"],r["price_cents"],r["currency"],r["image_url"],r["seller_url"]) for r in rows]
    conn=psycopg2.connect(DB,sslmode="require"); cur=conn.cursor()
    sql="""INSERT INTO item_source (id,title,brand,condition,price_cents,currency,image_url,seller_url)
           VALUES %s ON CONFLICT (id) DO UPDATE SET
           title=EXCLUDED.title,brand=EXCLUDED.brand,condition=EXCLUDED.condition,
           price_cents=EXCLUDED.price_cents,currency=EXCLUDED.currency,
           image_url=EXCLUDED.image_url,seller_url=EXCLUDED.seller_url;"""
    execute_values(cur,sql,vals,page_size=200); conn.commit(); cur.close(); conn.close(); return len(vals)
def run(q,target=200,per=100):
    pages=max(1,math.ceil(target/per)); out=[]
    for p in range(1,pages+1):
        out.extend(norm(i) for i in fetch_page(q,per,p)); time.sleep(0.4)
    uniq={r["id"]:r for r in out if r["id"]}; return upsert(list(uniq.values()))
if __name__=="__main__":
    total=0
    for q in ["vintage jacket","levis jeans","nike hoodie","patagonia fleece"]:
        n=run(q,200,100); print(f"[{q}] upserted {n}"); total+=n
    print("TOTAL upserted:", total)