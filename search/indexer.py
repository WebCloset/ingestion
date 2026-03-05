# ingestion/search/indexer.py
import os
import sys
import time
import argparse
from typing import Iterator, Dict, Any, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv, find_dotenv

# Load env from repo root (ingestion/.env) when run anywhere under ingestion/
load_dotenv(find_dotenv(filename=".env", usecwd=True))

DB_URL = os.getenv("DATABASE_URL")
ES_URL = os.getenv("ELASTICSEARCH_URL")
ES_API_KEY = os.getenv("ES_API_KEY")
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "products")

REQUIRED_ENVS = ["DATABASE_URL", "ELASTICSEARCH_URL", "ES_API_KEY"]


def get_pg():
    return psycopg2.connect(DB_URL)


def get_es():
    return Elasticsearch(
        ES_URL,
        api_key=ES_API_KEY,
        request_timeout=15,
        retry_on_timeout=True,
        max_retries=3,
    )


def fetch_item_source_batch(cur, limit: int, last_ts_iso: Optional[str], last_id: Optional[str]) -> List[Dict[str, Any]]:
    """
    Pull a batch using tuple keyset: (updated_at, id) > (last_ts, last_id)
    Works regardless of id type (text or int). Stable order by updated_at, id.
    If last_ts_iso is None, start from the beginning.
    """
    if last_ts_iso is None:
        cur.execute(
            """
            SELECT
              id,
              title,
              brand,
              condition,
              price_cents,
              currency,
              image_url,
              seller_url,
              marketplace_code,
              updated_at
            FROM item_source
            ORDER BY updated_at, id
            LIMIT %s;
            """,
            (limit,),
        )
    else:
        cur.execute(
            """
            SELECT
              id,
              title,
              brand,
              condition,
              price_cents,
              currency,
              image_url,
              seller_url,
              marketplace_code,
              updated_at
            FROM item_source
            WHERE (updated_at, id) > (%s, %s)
            ORDER BY updated_at, id
            LIMIT %s;
            """,
            (last_ts_iso, last_id, limit),
        )
    return cur.fetchall()


from datetime import datetime, timezone


def _to_iso8601(ts):
    if not ts:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    # fallback if string
    try:
        dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def row_to_doc(row: Dict[str, Any]) -> Dict[str, Any]:
    """Transform DB row → index doc. 1:1; include seller_urls array."""
    seller_urls = [row.get("seller_url")] if row.get("seller_url") else []
    return {
        "id": f"src-{row['id']}",
        "source_id": row["id"],
        "marketplace": row.get("marketplace_code") or "ebay",
        "title": row.get("title"),
        "brand": row.get("brand"),
        "condition": row.get("condition"),
        "price_cents": row.get("price_cents"),
        "currency": row.get("currency"),
        "image": row.get("image_url"),
        "seller_urls": seller_urls,
        "updated_at": _to_iso8601(row.get("updated_at")),
    }


def actions_from_rows(rows: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    for r in rows:
        doc = row_to_doc(r)
        yield {
            "_op_type": "index",
            "_index": ES_INDEX,
            "_id": doc["id"],
            "_source": doc,
        }


def main():
    parser = argparse.ArgumentParser(description="Index 1:1 item_source → Elasticsearch")
    parser.add_argument("--limit", type=int, default=1000, help="max rows to index (total)")
    parser.add_argument("--batch-size", type=int, default=500, help="DB/ES batch size")
    parser.add_argument("--since", type=str, default=None, help="ISO timestamp filter (updated_at > since)")
    parser.add_argument("--refresh", type=str, choices=["none", "wait_for"], default="none", help="Refresh index at end (tests)")
    parser.add_argument("--dry-run", action="store_true", help="print counts but don't index")
    args = parser.parse_args()

    missing = [k for k in REQUIRED_ENVS if not os.getenv(k)]
    if missing:
        print(f"Missing envs: {missing}", file=sys.stderr)
        sys.exit(2)

    # Connections sanity
    with get_pg() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute("SELECT COUNT(*) AS n FROM item_source;")
            src_count = cur.fetchone()["n"]
        except Exception as e:
            print(f"Postgres check failed (does item_source exist?): {e}", file=sys.stderr)
            sys.exit(2)

        es = get_es()
        if not es.ping():
            print("Elasticsearch ping failed", file=sys.stderr)
            sys.exit(2)
        if not es.indices.exists(index=ES_INDEX):
            print(f"Elasticsearch index '{ES_INDEX}' is missing (apply mappings then retry)", file=sys.stderr)
            sys.exit(2)

        to_index_total = min(args.limit, src_count)
        print(
            f"Indexer start — item_source rows={src_count}, target_index='{ES_INDEX}', plan_to_index={to_index_total}, since={args.since}, refresh={args.refresh}, dry_run={args.dry_run}"
        )

        if args.dry_run or to_index_total == 0:
            print("Nothing indexed (dry-run or zero rows).")
            return

        remaining = to_index_total
        last_ts_iso: Optional[str] = None
        last_id: Optional[str] = None
        total_ok = 0
        total_fail = 0

        while remaining > 0:
            take = min(args.batch_size, remaining)
            # if --since provided, start from that timestamp
            if last_ts_iso is None and args.since:
                last_ts_iso = args.since
            rows = fetch_item_source_batch(cur, take, last_ts_iso, last_id)
            if not rows:
                break

            actions = list(actions_from_rows(rows))
            try:
                ok, details = helpers.bulk(es, actions, raise_on_error=False)
            except Exception as e:
                print(f"bulk error, retrying once: {e}", file=sys.stderr)
                time.sleep(1.0)
                ok, details = helpers.bulk(es, actions, raise_on_error=False)

            total_ok += ok
            # details is a list of errors when raise_on_error=False
            batch_fail = len(details)
            total_fail += batch_fail

            print(f"batch indexed ok={ok}, fail={batch_fail}, last_ts={last_ts_iso}, last_id={last_id}")

            remaining -= len(rows)
            last_ts_iso = rows[-1]["updated_at"].isoformat() if hasattr(rows[-1]["updated_at"], "isoformat") else str(rows[-1]["updated_at"])
            last_id = str(rows[-1]["id"])

        if args.refresh == "wait_for":
            try:
                es.indices.refresh(index=ES_INDEX)
            except Exception as e:
                print(f"refresh failed: {e}", file=sys.stderr)

        print(f"DONE — indexed={total_ok}, failed={total_fail}, planned={to_index_total}")


if __name__ == "__main__":
    main()