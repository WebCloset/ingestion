CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS item_source (
  id                BIGSERIAL PRIMARY KEY,
  marketplace_code  TEXT        NOT NULL,
  source_item_id    TEXT        NOT NULL,
  title             TEXT        NOT NULL,
  brand             TEXT,
  condition         TEXT,
  price_cents       INTEGER,
  CHECK (price_cents IS NULL OR price_cents >= 0),
  currency          TEXT DEFAULT 'USD' CHECK (char_length(currency) = 3),
  image_url         TEXT,
  seller_url        TEXT        NOT NULL,
  size              TEXT,
  color             TEXT,
  category          TEXT,
  raw               JSONB,
  first_seen        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (marketplace_code, source_item_id)
);

CREATE TABLE IF NOT EXISTS item_canonical (
  id               BIGSERIAL PRIMARY KEY,
  hash_key         TEXT        NOT NULL UNIQUE,
  brand            TEXT,
  title            TEXT,
  category         TEXT,
  size_norm        TEXT,
  color_norm       TEXT,
  image_url        TEXT,
  min_price_cents  INTEGER,
  currency         CHAR(3),
  first_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS item_links (
  id            BIGSERIAL PRIMARY KEY,
  canonical_id  BIGINT REFERENCES item_canonical(id) ON DELETE CASCADE,
  source_id     BIGINT REFERENCES item_source(id)     ON DELETE CASCADE,
  is_primary    BOOLEAN     NOT NULL DEFAULT FALSE,
  price_cents   INTEGER,
  seller_url    TEXT,
  active        BOOLEAN     NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (canonical_id, source_id)
);

CREATE TABLE IF NOT EXISTS saved_searches (
  id            BIGSERIAL PRIMARY KEY,
  email         CITEXT      NOT NULL,
  query         JSONB       NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  verified      BOOLEAN     NOT NULL DEFAULT FALSE,
  verify_token  UUID        NOT NULL DEFAULT uuid_generate_v4()
);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_item_source_updated ON item_source;
CREATE TRIGGER trg_item_source_updated
BEFORE UPDATE ON item_source
FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

CREATE INDEX IF NOT EXISTS idx_item_source_brand        ON item_source (LOWER(brand));
CREATE INDEX IF NOT EXISTS idx_item_source_marketplace  ON item_source (marketplace_code);
CREATE INDEX IF NOT EXISTS idx_item_source_updated_at   ON item_source (updated_at);
CREATE INDEX IF NOT EXISTS idx_item_source_source_item_id ON item_source (source_item_id);
CREATE INDEX IF NOT EXISTS idx_item_canonical_hash      ON item_canonical (hash_key);
CREATE INDEX IF NOT EXISTS idx_saved_searches_email     ON saved_searches (email);
CREATE INDEX IF NOT EXISTS idx_saved_searches_query     ON saved_searches USING GIN (query);