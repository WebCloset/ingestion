DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='item_links' AND column_name='canonical_id') THEN
    ALTER TABLE item_links ADD COLUMN canonical_id BIGINT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='item_links' AND column_name='source_id') THEN
    ALTER TABLE item_links ADD COLUMN source_id BIGINT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='item_links' AND column_name='is_primary') THEN
    ALTER TABLE item_links ADD COLUMN is_primary BOOLEAN DEFAULT FALSE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='item_links' AND column_name='price_cents') THEN
    ALTER TABLE item_links ADD COLUMN price_cents INTEGER;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='item_links' AND column_name='seller_url') THEN
    ALTER TABLE item_links ADD COLUMN seller_url TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='item_links' AND column_name='active') THEN
    ALTER TABLE item_links ADD COLUMN active BOOLEAN DEFAULT TRUE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='item_links' AND column_name='created_at') THEN
    ALTER TABLE item_links ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW();
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name='item_links_canonical_fk') THEN
    ALTER TABLE item_links ADD CONSTRAINT item_links_canonical_fk FOREIGN KEY (canonical_id) REFERENCES item_canonical(id) ON DELETE CASCADE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name='item_links_source_fk') THEN
    ALTER TABLE item_links ADD CONSTRAINT item_links_source_fk FOREIGN KEY (source_id) REFERENCES item_source(id) ON DELETE CASCADE;
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename='item_links' AND indexname='uq_item_links_canonical_source') THEN
    CREATE UNIQUE INDEX uq_item_links_canonical_source ON item_links (canonical_id, source_id);
  END IF;
END$$;
