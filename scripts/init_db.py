import os, psycopg2
from dotenv import load_dotenv
load_dotenv(".env")
db=os.getenv("DATABASE_URL"); assert db, "Missing DATABASE_URL"
conn=psycopg2.connect(db, sslmode="require"); cur=conn.cursor()
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
# If column isn't TEXT yet, convert it (safe to run multiple times)
cur.execute("""SELECT data_type FROM information_schema.columns
               WHERE table_name='item_source' AND column_name='id';""")
t=cur.fetchone()
if t and t[0].lower()!="text":
    cur.execute("ALTER TABLE item_source ALTER COLUMN id TYPE TEXT USING id::text;")
conn.commit(); cur.close(); conn.close(); print("item_source ready (id=TEXT)")
