import os, psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
load_dotenv(".env")
conn=psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require")
cur=conn.cursor()
cur.execute("""
SELECT con.conname,
       rel_t.relname AS table_name,
       array_agg(att.attname ORDER BY cols.ord) AS columns
FROM pg_constraint con
JOIN pg_class rel_t ON rel_t.oid=con.conrelid
JOIN pg_class rel_f ON rel_f.oid=con.confrelid
JOIN unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ord) ON TRUE
JOIN pg_attribute att ON att.attrelid=con.conrelid AND att.attnum=cols.attnum
WHERE con.contype='f' AND rel_f.relname='item_source'
GROUP BY con.conname, rel_t.relname
""")
fks=cur.fetchall()
for conname, tbl, cols in fks:
    cur.execute(sql.SQL("ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};").format(
        sql.Identifier(tbl), sql.Identifier(conname)))
conn.commit()
for conname, tbl, cols in fks:
    for c in cols:
        cur.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE TEXT USING {}::text;").format(
            sql.Identifier(tbl), sql.Identifier(c), sql.Identifier(c)))
conn.commit()
cur.execute("""
DO $$
DECLARE t text;
BEGIN
  SELECT data_type INTO t FROM information_schema.columns
  WHERE table_name='item_source' AND column_name='id';
  IF t IS NOT NULL AND t <> 'text' THEN
    EXECUTE 'ALTER TABLE item_source ALTER COLUMN id TYPE TEXT USING id::text';
  END IF;
END $$;
""")
conn.commit()
for conname, tbl, cols in fks:
    cols_ident = sql.SQL(",").join(map(sql.Identifier, cols))
    cur.execute(sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES item_source(id);").format(
        sql.Identifier(tbl), sql.Identifier(conname), cols_ident))
conn.commit()
cur.close(); conn.close()
print("ok")
