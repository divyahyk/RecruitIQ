# database/db_factory.py
"""
RecruitIQ – Database Factory (v1.1)

Selects and caches the correct DB manager based on environment:
  • Supabase  → when SUPABASE_URL + SUPABASE_KEY are set   (production)
  • SQLite    → fallback for local dev when Supabase is absent

All other modules should call get_db_manager() to obtain the manager.
They must NOT instantiate SupabaseManager or SQLiteManager directly.

The returned manager is guaranteed to expose:
  .client          – supabase-py REST client  (Supabase only)
  ._db()           – psycopg2 context manager (Supabase only)
  .initialize_schema()
  .get_all_jds()
  .save_jd()
  .search_candidates()
  .bulk_insert_candidates()
  ... (full SupabaseManager / SQLiteManager API)
"""

from __future__ import annotations

import os
import streamlit as st


@st.cache_resource(show_spinner="Connecting to database…")
def get_db_manager():
    """
    Returns a cached DB manager instance.

    Resolution order
    ─────────────────
    1. SupabaseManager  if SUPABASE_URL + SUPABASE_KEY are both set
    2. SQLiteManager    fallback for local / offline development

    Environment variables
    ──────────────────────
    SUPABASE_URL        https://<ref>.supabase.co
    SUPABASE_KEY        service_role or anon key   ← canonical name in .env
    SUPABASE_ANON_KEY   accepted as alias           ← legacy / alternate name
    SQLITE_PATH         path to .db file (default: recruitiq_local.db)
    """
    # ── Supabase ──────────────────────────────────────────────────────────────
    supabase_url = os.getenv("SUPABASE_URL", "").strip()

    # Accept both key names so neither .env layout breaks
    supabase_key = (
        os.getenv("SUPABASE_KEY",      "").strip()
        or os.getenv("SUPABASE_ANON_KEY", "").strip()
    )

    if supabase_url and supabase_key:
        try:
            from database.supabase_manager import SupabaseManager

            manager = SupabaseManager(supabase_url, supabase_key)
            manager.initialize_schema()
            print("✅ RecruitIQ → Supabase (cloud mode)")
            return manager

        except Exception as exc:
            print(f"⚠️  Supabase init failed ({exc}) — falling back to SQLite")

    # ── SQLite fallback ───────────────────────────────────────────────────────
    try:
        from database.sqlite_manager import SQLiteManager

        sqlite_path = os.getenv("SQLITE_PATH", "recruitiq_local.db")
        manager     = SQLiteManager(sqlite_path)
        manager.initialize_schema()
        print(f"✅ RecruitIQ → SQLite at {sqlite_path} (local mode)")
        return manager

    except Exception as exc:
        # Surface a clear error rather than a cryptic AttributeError later
        raise RuntimeError(
            f"Could not connect to any database.\n"
            f"SQLite error: {exc}\n\n"
            f"Set SUPABASE_URL and SUPABASE_KEY in your .env file, or\n"
            f"ensure SQLiteManager is importable for local development."
        ) from exc


def get_db_type(manager=None) -> str:
    """
    Returns 'supabase' or 'sqlite' for the active manager.
    Useful for feature-gating SQL that only works on Postgres.

    Usage:
        db = get_db_manager()
        if get_db_type(db) == 'supabase':
            # safe to use _db() / raw psycopg2
    """
    if manager is None:
        manager = get_db_manager()

    class_name = type(manager).__name__.lower()

    if "supabase" in class_name:
        return "supabase"
    if "sqlite" in class_name:
        return "sqlite"

    # Duck-type fallback — if it has _db() it's the psycopg2-backed manager
    return "supabase" if hasattr(manager, "_db") else "sqlite"
