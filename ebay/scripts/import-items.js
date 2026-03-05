import fs from "node:fs";
import crypto from "node:crypto";
import dotenv from "dotenv";
import pkg from "pg";
const { Client } = pkg;
dotenv.config();

const file = process.argv[2];
if (!file) {
  console.error("Usage: node ebay/scripts/import-items.js <path-to-json>");
  process.exit(1);
}
const raw = fs.readFileSync(file, "utf8");
const items = JSON.parse(raw);

const norm = (s) => (s || "").toString().trim().toLowerCase();
const hashKey = (i) =>
  crypto
    .createHash("sha256")
    .update(
      [i.brand, i.title, i.category, i.size, i.color].map(norm).join("|")
    )
    .digest("hex");

const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

await client.connect();

for (const i of items) {
  const hkey = hashKey(i);
  await client.query("BEGIN");
  try {
    const upsertSource = `
      INSERT INTO item_source
        (marketplace_code, source_item_id, title, brand, condition, price_cents, currency, image_url, seller_url, size, color, category, raw, updated_at, first_seen)
      VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW(),NOW())
      ON CONFLICT (marketplace_code, source_item_id)
      DO UPDATE SET
        title=EXCLUDED.title,
        brand=EXCLUDED.brand,
        condition=EXCLUDED.condition,
        price_cents=EXCLUDED.price_cents,
        currency=EXCLUDED.currency,
        image_url=EXCLUDED.image_url,
        seller_url=EXCLUDED.seller_url,
        size=EXCLUDED.size,
        color=EXCLUDED.color,
        category=EXCLUDED.category,
        raw=EXCLUDED.raw,
        updated_at=NOW()
      RETURNING id, price_cents, currency
    `;
    const srcRes = await client.query(upsertSource, [
      i.marketplace_code,
      i.source_item_id,
      i.title,
      i.brand,
      i.condition,
      i.price_cents ?? null,
      i.currency ?? null,
      i.image_url ?? null,
      i.seller_url,
      i.size ?? null,
      i.color ?? null,
      i.category ?? null,
      i
    ]);
    const sourceId = srcRes.rows[0].id;
    const priceForCanon = srcRes.rows[0].price_cents ?? null;
    const currencyForCanon = srcRes.rows[0].currency ?? i.currency ?? null;

    const upsertCanon = `
      INSERT INTO item_canonical
        (hash_key, brand, title, category, size_norm, color_norm, image_url, min_price_cents, currency, first_seen, last_seen)
      VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),NOW())
      ON CONFLICT (hash_key)
      DO UPDATE SET
        last_seen=NOW(),
        min_price_cents=LEAST(item_canonical.min_price_cents, COALESCE(EXCLUDED.min_price_cents, item_canonical.min_price_cents)),
        image_url=COALESCE(item_canonical.image_url, EXCLUDED.image_url),
        brand=COALESCE(item_canonical.brand, EXCLUDED.brand),
        title=COALESCE(item_canonical.title, EXCLUDED.title),
        category=COALESCE(item_canonical.category, EXCLUDED.category),
        currency=COALESCE(item_canonical.currency, EXCLUDED.currency)
      RETURNING id
    `;
    const canRes = await client.query(upsertCanon, [
      hkey,
      i.brand ?? null,
      i.title ?? null,
      i.category ?? null,
      norm(i.size) || null,
      norm(i.color) || null,
      i.image_url ?? null,
      priceForCanon,
      currencyForCanon
    ]);
    const canonicalId = canRes.rows[0].id;

    const upsertLink = `
      INSERT INTO item_links
        (canonical_id, source_id, is_primary, price_cents, seller_url, active, created_at)
      VALUES
        ($1,$2,false,$3,$4,true,NOW())
      ON CONFLICT (canonical_id, source_id)
      DO UPDATE SET
        price_cents=EXCLUDED.price_cents,
        seller_url=EXCLUDED.seller_url,
        active=true
    `;
    await client.query(upsertLink, [
      canonicalId,
      sourceId,
      i.price_cents ?? null,
      i.seller_url
    ]);

    await client.query("COMMIT");
    console.log("imported", i.marketplace_code, i.source_item_id);
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("failed", i.marketplace_code, i.source_item_id, e.message);
  }
}

await client.end();
