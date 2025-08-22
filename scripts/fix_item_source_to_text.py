import os, psycopg2
from dotenv import load_dotenv
load_dotenv(".env")
conn=psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require")
cur=conn.cursor()
cur.execute("""DO $$ BEGIN
IF EXISTS (SELECT 1 FROM information_schema.table_constraints
           WHERE constraint_name='item_canonical_representative_source_id_fkey'
             AND table_name='item_canonical') THEN
  ALTER TABLE item_canonical DROP CONSTRAINT item_canonical_representative_source_id_fkey;
END IF;
END $$;""")
conn.commit()
cur.execute("""DO $$ DECLARE t text; BEGIN
SELECT data_type INTO t FROM information_schema.columns
 WHERE table_name='item_source' AND column_name='id';
IF t IS NOT NULL AND t <> 'text' THEN
  EXECUTE 'ALTER TABLE item_source ALTER COLUMN id TYPE TEXT USING id::text';
END IF;
END $$;""")
conn.commit()
cur.execute("""DO $$ DECLARE t text; BEGIN
IF EXISTS (SELECT 1 FROM information_schema.columns
            WHERE table_name='item_canonical' AND column_name='representative_source_id') THEN
  SELECT data_type INTO t FROM information_schema.columns
   WHERE table_name='item_canonical' AND column_name='representative_source_id';
  IF t <> 'text' THEN
    EXECUTE 'ALTER TABLE item_canonical ALTER COLUMN representative_source_id TYPE TEXT USING representative_source_id::text';
  END IF;
END IF;
END $$;""")
conn.commit()
cur.execute("""DO $$ BEGIN
IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='item_canonical')
   AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='item_canonical' AND column_name='representative_source_id') THEN
  ALTER TABLE item_canonical
    ADD CONSTRAINT item_canonical_representative_source_id_fkey
    FOREIGN KEY (representative_source_id) REFERENCES item_source(id);
END IF;
END $$;""")
conn.commit()
cur.close(); conn.close()
print("ok")
