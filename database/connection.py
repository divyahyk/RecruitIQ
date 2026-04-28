# database/connection.py

import os
import psycopg2
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
#  SUPABASE  (REST / realtime)
# ─────────────────────────────────────────────────────────────
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")


def get_supabase_client() -> Client:
    """Returns a Supabase REST client instance."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_KEY must be set in your .env file."
        )
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────────────────────────────────────────
#  PSYCOPG2  (raw Postgres — used only for bulk COPY imports)
# ─────────────────────────────────────────────────────────────
PG_CONN_STRING: str = os.getenv("PG_CONN_STRING", "")


def get_pg_connection() -> psycopg2.extensions.connection:
    """
    Returns a raw psycopg2 connection.
    Used exclusively by excel_import.py for high-speed COPY.
    """
    if not PG_CONN_STRING:
        raise ValueError(
            "PG_CONN_STRING is not set.\n"
            "Add this to your .env file:\n\n"
            "PG_CONN_STRING=postgresql://postgres.[ref]:[password]"
            "@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"
        )
    return psycopg2.connect(PG_CONN_STRING)
