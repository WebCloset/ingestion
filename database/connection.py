import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from typing import Any, Dict, Iterable, List, Optional, Tuple

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_URL = "postgresql://neondb_owner:npg_5LdJSKuC8bFY@ep-damp-field-aey694y3-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def upsert_rows(rows: List[Dict[str, Any]], upsert_sql: str) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=100)
    return len(rows)
