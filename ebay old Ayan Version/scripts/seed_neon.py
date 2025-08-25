import os, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), ".env"))
DB = os.getenv("DATABASE_URL")
assert DB, "Missing DATABASE_URL in .env"

conn = psycopg2.connect(DB, sslmode="require")
cur = conn.cursor()

# 1) Ensure table exists
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
);
""")
conn.commit()

# 2) If the id column isn't TEXT yet, convert it
cur.execute("""
SELECT data_type FROM information_schema.columns
WHERE table_name = 'item_source' AND column_name = 'id';
""")
row = cur.fetchone()
if row and row[0].lower() != "text":
    cur.execute("ALTER TABLE item_source ALTER COLUMN id TYPE TEXT USING id::text;")
    conn.commit()

# 3) Seed a few rows (fake eBay-style IDs) and upsert
rows = [
  {"id":"v1|TEST0001","title":"Seed Test Jacket","brand":"TestCo","condition":"Used",
   "price_cents":2999,"currency":"USD","image_url":"","seller_url":"https://example.com/1"},
  {"id":"v1|TEST0002","title":"Seed Test Jeans","brand":"TestCo","condition":"New",
   "price_cents":4599,"currency":"USD","image_url":"","seller_url":"https://example.com/2"},
  {"id":"v1|TEST0003","title":"Seed Test Hoodie","brand":"TestCo","condition":"Used",
   "price_cents":3799,"currency":"USD","image_url":"","seller_url":"https://example.com/3"},
]
vals = [(r["id"], r["title"], r["brand"], r["condition"],
         r["price_cents"], r["currency"], r["image_url"], r["seller_url"]) for r in rows]

sql = """
INSERT INTO item_source
  (id, title, brand, condition, price_cents, currency, image_url, seller_url)
VALUES %s
ON CONFLICT (id) DO UPDATE SET
  title=EXCLUDED.title,
  brand=EXCLUDED.brand,
  condition=EXCLUDED.condition,
  price_cents=EXCLUDED.price_cents,
  currency=EXCLUDED.currency,
  image_url=EXCLUDED.image_url,
  seller_url=EXCLUDED.seller_url;
"""
execute_values(cur, sql, vals, page_size=100)
conn.commit()

# 4) Verify
cur.execute("SELECT COUNT(*) FROM item_source;")
count = cur.fetchone()[0]
cur.execute("SELECT id, title, price_cents, currency FROM item_source ORDER BY id LIMIT 5;")
sample = cur.fetchall()

print("Rows in item_source:", count)
print("Sample:", sample)

cur.close(); conn.close()