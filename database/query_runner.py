# database/query_runner.py
from __future__ import annotations
from typing import Any, List, Dict

def execute_raw(db: Any, sql: str, params: list) -> list[dict]:
    """
    Execute parameterised SQL and return list[dict].

    Works with:
      • SupabaseManager  (psycopg2 pool via _db())
      • SQLiteManager    (sqlite3 connection via _get_connection() or _conn)
    """

    def _rows_from_conn(conn: Any) -> list[dict]:
        import re as _re
        psyco_sql = _re.sub(r"\$\d+", "%s", sql)

        # sqlite3 uses ? placeholders, psycopg2 uses %s
        import sqlite3
        if isinstance(conn, sqlite3.Connection):
            sqlite_sql = _re.sub(r"\$\d+", "?", sql)
            cur = conn.execute(sqlite_sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

        with conn.cursor() as cur:
            cur.execute(psyco_sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    # Strategy 1: SupabaseManager._db() — primary path
    if hasattr(db, "_db"):
        try:
            with db._db() as conn:
                return _rows_from_conn(conn)
        except Exception as exc:
            raise RuntimeError(f"_db() query failed: {exc}") from exc

    # Strategy 2: _get_pool() lazy init
    if hasattr(db, "_get_pool"):
        pool = db._get_pool()
        conn = pool.getconn()
        try:
            result = _rows_from_conn(conn)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)

    # Strategy 3: direct _pool / pool attribute
    pool = getattr(db, "_pool", None) or getattr(db, "pool", None)
    if pool:
        conn = pool.getconn()
        try:
            result = _rows_from_conn(conn)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)

    # Strategy 4: SQLiteManager._get_connection() or ._conn
    sqlite_conn = (
        db._get_connection() if hasattr(db, "_get_connection")
        else getattr(db, "_conn", None)
    )
    if sqlite_conn:
        return _rows_from_conn(sqlite_conn)

    # Strategy 5: standalone get_pg_connection()
    if hasattr(db, "get_pg_connection"):
        conn = db.get_pg_connection()
        try:
            return _rows_from_conn(conn)
        finally:
            conn.close()

    # Strategy 6: custom execute_query() helper
    if hasattr(db, "execute_query"):
        result = db.execute_query(sql, params)
        if isinstance(result, list):
            return result
        if hasattr(result, "fetchall"):
            cols = [d[0] for d in result.description]
            return [dict(zip(cols, row)) for row in result.fetchall()]

    raise RuntimeError(
        "No usable DB connection found.\n"
        "Manager must expose one of: "
        "_db(), _get_pool(), _pool, _conn, "
        "get_pg_connection(), execute_query()"
    )


# ============================================================================
# Additional Helper Functions
# ============================================================================

def execute_query(db: Any, sql: str, params: list | None = None) -> list[dict]:
    """
    Execute a SELECT query and return results as list of dicts.
    
    Args:
        db: Database manager instance
        sql: SQL query string
        params: Query parameters (default: empty list)
    
    Returns:
        List of dictionaries with column names as keys
    """
    if params is None:
        params = []
    return execute_raw(db, sql, params)


def execute_insert(db: Any, sql: str, params: list | None = None) -> None:
    """
    Execute an INSERT/UPDATE/DELETE query.
    
    Args:
        db: Database manager instance
        sql: SQL query string
        params: Query parameters (default: empty list)
    """
    if params is None:
        params = []
    execute_raw(db, sql, params)


def fetch_one(db: Any, sql: str, params: list | None = None) -> dict | None:
    """
    Fetch a single row as a dictionary.
    
    Args:
        db: Database manager instance
        sql: SQL query string
        params: Query parameters (default: empty list)
    
    Returns:
        Dictionary or None if no results
    """
    if params is None:
        params = []
    results = execute_raw(db, sql, params)
    return results[0] if results else None


def fetch_all(db: Any, sql: str, params: list | None = None) -> list[dict]:
    """
    Fetch all rows as list of dictionaries.
    
    Args:
        db: Database manager instance
        sql: SQL query string
        params: Query parameters (default: empty list)
    
    Returns:
        List of dictionaries
    """
    if params is None:
        params = []
    return execute_raw(db, sql, params)
