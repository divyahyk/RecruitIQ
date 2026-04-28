# ui/pages/profiles.py  — RecruitIQ · Candidate Profiles
# ─────────────────────────────────────────────────────────────────────────────
# Physical table columns (confirmed, including migration 003):
#
#   id                            PK
#   first_name
#   last_name
#   candidate_name                auto-composed
#   email_address                 NOT NULL  UNIQUE
#   phone_number
#   location
#   pin_code
#   title
#   current_company
#   current_position
#   current_position_start_date
#   total_experience
#   notice_period
#   work_mode_pref
#   education_degree
#   education_institution
#   linkedin_profile
#   skills                        JSONB
#   profile_summary
#   source
#   is_active                     BOOLEAN  DEFAULT FALSE   ← migration 003
#   remarks                       TEXT                     ← migration 003
#   imported_at
#   created_at
#   updated_at
#
# CACHE_VERSION — bump on EVERY schema or SQL change.
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import csv
import io
import json
import logging
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import streamlit as st
from psycopg2.pool import ThreadedConnectionPool

from config import Config, friendly_conn_error, get_pg_dsn

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

CACHE_VERSION   = "v8"
PAGE_SIZE       = 50
POOL_MIN        = 2
POOL_MAX        = 10
CONNECT_TIMEOUT = 15
STMT_TIMEOUT_MS = 30_000
SEARCH_TTL      = 120
COUNT_TTL       = 300
EXPORT_TIMEOUT  = 120

MIN_MULTI_FIELDS = 2          # ← changed from 3 to 2

# ─────────────────────────────────────────────────────────────────────────────
# Column definitions
# ─────────────────────────────────────────────────────────────────────────────

COLUMN_LABELS: Dict[str, str] = {
    "id"                          : "ID",
    "first_name"                  : "First Name",
    "last_name"                   : "Last Name",
    "candidate_name"              : "Full Name",
    "email_address"               : "Email Address",
    "phone_number"                : "Phone Number",
    "location"                    : "General Location",
    "pin_code"                    : "ZIP / PIN Code",
    "title"                       : "Current Title",
    "current_company"             : "Current Company",
    "current_position"            : "Current Position",
    "current_position_start_date" : "Position Start Date",
    "total_experience"            : "Experience (yrs)",
    "notice_period"               : "Notice Period",
    "work_mode_pref"              : "Work Mode Preference",
    "education_degree"            : "Education Degree",
    "education_institution"       : "Education Institution",
    "linkedin_profile"            : "LinkedIn Profile URL",
    "skills"                      : "Skills",
    "profile_summary"             : "Profile Summary / Headline",
    "source"                      : "Source",
    "is_active"                   : "Actively Seeking",
    "remarks"                     : "HR Remarks",
    "imported_at"                 : "Imported At",
    "created_at"                  : "Created At",
    "updated_at"                  : "Updated At",
}

# Columns fetched for the candidate grid
LIST_COLUMNS: Tuple[str, ...] = (
    "id",
    "candidate_name",
    "email_address",
    "phone_number",
    "location",
    "title",
    "current_company",
    "current_position",
    "total_experience",
    "notice_period",
    "work_mode_pref",
    "skills",
    "profile_summary",
    "is_active",
    "remarks",
    "created_at",
)

EXPORT_COLUMNS: Tuple[str, ...] = (
    "id",
    "first_name",
    "last_name",
    "candidate_name",
    "email_address",
    "phone_number",
    "location",
    "pin_code",
    "title",
    "current_company",
    "current_position",
    "current_position_start_date",
    "total_experience",
    "notice_period",
    "work_mode_pref",
    "education_degree",
    "education_institution",
    "linkedin_profile",
    "skills",
    "profile_summary",
    "source",
    "is_active",
    "remarks",
    "imported_at",
    "created_at",
    "updated_at",
)

SEARCH_COLUMNS: Tuple[str, ...] = (
    "candidate_name",
    "first_name",
    "last_name",
    "email_address",
    "phone_number",
    "location",
    "pin_code",
    "title",
    "current_company",
    "current_position",
    "education_degree",
    "education_institution",
    "linkedin_profile",
    "skills::text",
    "profile_summary",
    "source",
    "notice_period",
    "work_mode_pref",
    "remarks",
)

WRITE_COLUMNS: Tuple[str, ...] = (
    "first_name",
    "last_name",
    "candidate_name",
    "email_address",
    "phone_number",
    "location",
    "pin_code",
    "title",
    "current_company",
    "current_position",
    "current_position_start_date",
    "total_experience",
    "notice_period",
    "work_mode_pref",
    "education_degree",
    "education_institution",
    "linkedin_profile",
    "skills",
    "profile_summary",
    "source",
    "is_active",
    "remarks",
    "updated_at",
)

_LIST_SELECT   = ", ".join(LIST_COLUMNS)
_EXPORT_SELECT = ", ".join(EXPORT_COLUMNS)

NOTICE_PERIOD_OPTIONS: List[str] = [
    "", "Immediately", "15 days", "30 days",
    "45 days", "60 days", "90 days", "More than 90 days",
]

WORK_MODE_OPTIONS: List[str] = [
    "", "On-site", "Hybrid", "Remote", "Flexible",
]

# ─────────────────────────────────────────────────────────────────────────────
# Multi-field search definitions
# ─────────────────────────────────────────────────────────────────────────────

FILTER_FIELDS: List[Tuple[str, str]] = [
    ("Full Name",        "candidate_name"),
    ("Email",            "email_address"),
    ("Phone",            "phone_number"),
    ("Location",         "location"),
    ("PIN / ZIP",        "pin_code"),
    ("Current Title",    "title"),
    ("Current Company",  "current_company"),
    ("Current Position", "current_position"),
    ("Notice Period",    "notice_period"),
    ("Work Mode",        "work_mode_pref"),
    ("Education Degree", "education_degree"),
    ("Institution",      "education_institution"),
    ("Skills",           "skills::text"),
    ("Profile Summary",  "profile_summary"),
    ("Source",           "source"),
    ("Remarks",          "remarks"),
]
FILTER_FIELD_LABELS: List[str]       = [f[0] for f in FILTER_FIELDS]
_FILTER_LABEL_TO_COL: Dict[str, str] = {f[0]: f[1] for f in FILTER_FIELDS}

FILTER_OPS: List[str] = [
    "contains",
    "equals",
    "starts with",
    "ends with",
    "not contains",
    "is empty",
    "is not empty",
]
_OPS_NO_VALUE = {"is empty", "is not empty"}

# ─────────────────────────────────────────────────────────────────────────────
# Skills vocabulary
# ─────────────────────────────────────────────────────────────────────────────

SKILL_VOCAB: Dict[str, str] = {
    "python": "Python", "java": "Java", "javascript": "JavaScript",
    "typescript": "TypeScript", "c++": "C++", "c#": "C#",
    "golang": "Go", "go": "Go", "rust": "Rust", "kotlin": "Kotlin",
    "swift": "Swift", "php": "PHP", "ruby": "Ruby", "scala": "Scala",
    "matlab": "MATLAB", "perl": "Perl", "bash": "Bash", "shell": "Shell",
    "powershell": "PowerShell", "html": "HTML", "css": "CSS",
    "react": "React", "angular": "Angular", "vue.js": "Vue.js",
    "vue": "Vue.js", "next.js": "Next.js", "nextjs": "Next.js",
    "svelte": "Svelte", "jquery": "jQuery", "tailwind": "Tailwind CSS",
    "bootstrap": "Bootstrap", "webpack": "Webpack",
    "spring boot": "Spring Boot", "spring": "Spring", "django": "Django",
    "flask": "Flask", "fastapi": "FastAPI", "express": "Express.js",
    "node.js": "Node.js", "nodejs": "Node.js", "node": "Node.js",
    "laravel": "Laravel", "rails": "Ruby on Rails", "asp.net": "ASP.NET",
    ".net": ".NET", "dotnet": ".NET", "hibernate": "Hibernate",
    "postgresql": "PostgreSQL", "postgres": "PostgreSQL", "mysql": "MySQL",
    "sqlite": "SQLite", "mongodb": "MongoDB", "redis": "Redis",
    "elasticsearch": "Elasticsearch", "cassandra": "Cassandra",
    "dynamodb": "DynamoDB", "sql server": "MS SQL Server",
    "mssql": "MS SQL Server", "oracle": "Oracle DB", "neo4j": "Neo4j",
    "influxdb": "InfluxDB", "sql": "SQL",
    "amazon web services": "AWS", "aws": "AWS", "azure": "Azure",
    "google cloud": "GCP", "gcp": "GCP", "heroku": "Heroku",
    "vercel": "Vercel", "netlify": "Netlify", "cloudflare": "Cloudflare",
    "kubernetes": "Kubernetes", "k8s": "Kubernetes", "docker": "Docker",
    "terraform": "Terraform", "ansible": "Ansible", "jenkins": "Jenkins",
    "github actions": "GitHub Actions", "gitlab ci": "GitLab CI",
    "circleci": "CircleCI", "ci/cd": "CI/CD", "nginx": "NGINX",
    "linux": "Linux", "unix": "Unix",
    "machine learning": "Machine Learning", "deep learning": "Deep Learning",
    "natural language processing": "NLP", "nlp": "NLP",
    "computer vision": "Computer Vision", "tensorflow": "TensorFlow",
    "pytorch": "PyTorch", "keras": "Keras", "scikit-learn": "Scikit-learn",
    "sklearn": "Scikit-learn", "pandas": "Pandas", "numpy": "NumPy",
    "apache spark": "Apache Spark", "spark": "Apache Spark",
    "apache kafka": "Apache Kafka", "kafka": "Apache Kafka",
    "airflow": "Apache Airflow", "dbt": "dbt", "tableau": "Tableau",
    "power bi": "Power BI", "looker": "Looker", "excel": "Excel",
    "react native": "React Native", "flutter": "Flutter",
    "android": "Android", "ios": "iOS", "xamarin": "Xamarin",
    "selenium": "Selenium", "cypress": "Cypress", "jest": "Jest",
    "pytest": "pytest", "junit": "JUnit", "postman": "Postman",
    "graphql": "GraphQL", "grpc": "gRPC", "restful": "REST API",
    "rest": "REST API", "microservices": "Microservices", "agile": "Agile",
    "scrum": "Scrum", "kanban": "Kanban", "jira": "Jira",
    "github": "GitHub", "gitlab": "GitLab", "bitbucket": "Bitbucket",
    "git": "Git", "api": "API", "salesforce": "Salesforce", "sap": "SAP",
    "erp": "ERP", "kpo": "KPO", "bpo": "BPO",
}

_VOCAB_SORTED: List[Tuple[str, str]] = sorted(
    SKILL_VOCAB.items(), key=lambda kv: len(kv[0]), reverse=True
)


def extract_skills(text: str) -> List[str]:
    if not text or not isinstance(text, str):
        return []
    lowered = text.lower()
    found: Dict[str, int] = {}
    for token, canonical in _VOCAB_SORTED:
        start = 0
        while True:
            pos = lowered.find(token, start)
            if pos == -1:
                break
            after_pos = pos + len(token)
            before_ok = pos == 0 or not lowered[pos - 1].isalnum()
            after_ok  = after_pos >= len(lowered) or not lowered[after_pos].isalnum()
            if before_ok and after_ok:
                if canonical not in found:
                    found[canonical] = pos
                lowered = lowered[:pos] + " " * len(token) + lowered[after_pos:]
            start = pos + 1
    return [k for k, _ in sorted(found.items(), key=lambda kv: kv[1])]


def backfill_skills_from_summary(
    batch_size: int = 500,
    dry_run: bool = False,
) -> Dict[str, int]:
    stats: Dict[str, int] = {"scanned": 0, "updated": 0, "skipped": 0, "errors": 0}
    try:
        with _db() as cur:
            cur.execute(
                """
                SELECT id, profile_summary FROM candidates
                WHERE  (skills IS NULL OR skills = '[]'::jsonb)
                  AND  profile_summary IS NOT NULL
                  AND  profile_summary <> ''
                ORDER  BY id
                """
            )
            rows: List[Dict[str, Any]] = [dict(r) for r in cur.fetchall()]
    except psycopg2.Error as exc:
        logger.error("backfill_skills fetch failed: %s", exc)
        stats["errors"] += 1
        return stats

    stats["scanned"] = len(rows)
    now = datetime.now(timezone.utc).isoformat()

    for i in range(0, len(rows), batch_size):
        chunk   = rows[i : i + batch_size]
        updates: List[Tuple[str, str, int]] = []
        for row in chunk:
            skills = extract_skills(row.get("profile_summary") or "")
            if skills:
                updates.append((json.dumps(skills), now, row["id"]))
            else:
                stats["skipped"] += 1

        if dry_run:
            stats["updated"] += len(updates)
            continue
        if not updates:
            continue
        try:
            with _db() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    "UPDATE candidates SET skills=%s::jsonb, updated_at=%s WHERE id=%s",
                    updates,
                    page_size=batch_size,
                )
            stats["updated"] += len(updates)
        except psycopg2.Error as exc:
            logger.error("backfill_skills update failed: %s", exc)
            stats["errors"] += 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Quick-save helpers (inline table actions)
# ─────────────────────────────────────────────────────────────────────────────

def _toggle_active(candidate_id: int, new_value: bool) -> None:
    """Flip is_active for a single candidate."""
    now = datetime.now(timezone.utc).isoformat()
    with _db() as cur:
        cur.execute(
            "UPDATE candidates SET is_active=%s, updated_at=%s WHERE id=%s",
            (new_value, now, candidate_id),
        )


def _save_remarks(candidate_id: int, remarks: str) -> None:
    """Persist HR remarks for a single candidate."""
    now = datetime.now(timezone.utc).isoformat()
    with _db() as cur:
        cur.execute(
            "UPDATE candidates SET remarks=%s, updated_at=%s WHERE id=%s",
            (remarks.strip() or None, now, candidate_id),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Connection pool
# ─────────────────────────────────────────────────────────────────────────────

_pool_lock: threading.Lock                    = threading.Lock()
_pools:     Dict[int, ThreadedConnectionPool] = {}


def _get_pool() -> ThreadedConnectionPool:
    dsn      = get_pg_dsn()
    dsn_hash = hash(dsn)
    if dsn_hash in _pools:
        return _pools[dsn_hash]
    with _pool_lock:
        if dsn_hash in _pools:
            return _pools[dsn_hash]
        pool = ThreadedConnectionPool(
            POOL_MIN, POOL_MAX,
            dsn=dsn,
            connect_timeout=CONNECT_TIMEOUT,
            options=f"-c statement_timeout={STMT_TIMEOUT_MS}ms",
            keepalives=1, keepalives_idle=60,
            keepalives_interval=10, keepalives_count=5,
        )
        _pools[dsn_hash] = pool
        return pool


@contextmanager
def _db() -> Generator[psycopg2.extras.RealDictCursor, None, None]:
    pool = _get_pool()
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        conn = pool.getconn()
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        conn.commit()
    except psycopg2.Error:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn:
            try:
                conn.reset()
            except Exception:
                pass
            pool.putconn(conn)


def _get_export_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        dsn=get_pg_dsn(),
        connect_timeout=EXPORT_TIMEOUT,
        options=f"-c statement_timeout={EXPORT_TIMEOUT * 1_000}ms",
    )


def _pool_stats() -> Dict[str, int]:
    try:
        pool  = _get_pool()
        used  = len(pool._used)   # type: ignore[attr-defined]
        free  = len(pool._pool)   # type: ignore[attr-defined]
        return {"used": used, "free": free, "total": used + free, "max": POOL_MAX}
    except Exception:
        return {"used": 0, "free": 0, "total": 0, "max": POOL_MAX}


# ─────────────────────────────────────────────────────────────────────────────
# Display helpers
# ─────────────────────────────────────────────────────────────────────────────

def _clean(value: Any) -> str:
    """Coerce DB value → display string; returns '—' for None / NaN / blank."""
    if value is None:
        return "—"
    try:
        if isinstance(value, float) and value != value:
            return "—"
    except Exception:
        pass
    s = str(value).strip()
    if s.lower() in ("nan", "none", "null", ""):
        return "—"
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Search WHERE builder
# ─────────────────────────────────────────────────────────────────────────────

def _search_where(
    query:         str,
    multi_filters: Optional[List[Dict[str, str]]] = None,
) -> Tuple[str, tuple]:
    # MODE A — multi-field
    active: List[Dict[str, str]] = []
    if multi_filters:
        for f in multi_filters:
            op    = (f.get("op")    or "contains").strip()
            value = (f.get("value") or "").strip()
            label = (f.get("field") or "").strip()
            if not label or label not in _FILTER_LABEL_TO_COL:
                continue
            if op in _OPS_NO_VALUE or value:
                active.append(f)

    if active:
        clauses: List[str] = []
        params:  List[Any] = []
        for f in active:
            col   = _FILTER_LABEL_TO_COL[f["field"]]
            op    = f.get("op", "contains")
            value = (f.get("value") or "").strip()
            if op == "contains":
                clauses.append(f"{col} ILIKE %s"); params.append(f"%{value}%")
            elif op == "equals":
                clauses.append(f"{col} ILIKE %s"); params.append(value)
            elif op == "starts with":
                clauses.append(f"{col} ILIKE %s"); params.append(f"{value}%")
            elif op == "ends with":
                clauses.append(f"{col} ILIKE %s"); params.append(f"%{value}")
            elif op == "not contains":
                clauses.append(f"{col} NOT ILIKE %s"); params.append(f"%{value}%")
            elif op == "is empty":
                clauses.append(f"({col} IS NULL OR {col}::text = '')")
            elif op == "is not empty":
                clauses.append(f"({col} IS NOT NULL AND {col}::text <> '')")
        if clauses:
            return "WHERE " + " AND ".join(clauses), tuple(params)

    # MODE B — global keyword
    query = (query or "").strip()
    if not query:
        return "", ()
    term       = f"%{query}%"
    conditions = " OR ".join(f"{col} ILIKE %s" for col in SEARCH_COLUMNS)
    return f"WHERE {conditions}", (term,) * len(SEARCH_COLUMNS)


# ─────────────────────────────────────────────────────────────────────────────
# Cached read helpers
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=SEARCH_TTL, show_spinner=False)
def _fetch_page(
    query:         str,
    offset:        int,
    page_size:     int            = PAGE_SIZE,
    cache_ver:     str            = CACHE_VERSION,
    multi_filters: Optional[str] = None,
) -> List[Dict[str, Any]]:
    filters = json.loads(multi_filters) if multi_filters else None
    where, params = _search_where(query, filters)
    sql = (
        f"SELECT {_LIST_SELECT} FROM candidates "
        f"{where} ORDER BY created_at DESC NULLS LAST "
        f"LIMIT %s OFFSET %s"
    )
    try:
        with _db() as cur:
            cur.execute(sql, params + (page_size, offset))
            return [dict(r) for r in cur.fetchall()]
    except psycopg2.Error as exc:
        logger.error("_fetch_page failed: %s", exc)
        raise


@st.cache_data(ttl=COUNT_TTL, show_spinner=False)
def _fetch_count(
    query:         str,
    cache_ver:     str            = CACHE_VERSION,
    multi_filters: Optional[str] = None,
) -> int:
    filters = json.loads(multi_filters) if multi_filters else None
    where, params = _search_where(query, filters)
    sql = f"SELECT COUNT(*) FROM candidates {where}"
    try:
        with _db() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row["count"]) if row else 0
    except psycopg2.Error as exc:
        logger.error("_fetch_count failed: %s", exc)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Mutations
# ─────────────────────────────────────────────────────────────────────────────

def _coerce_skills(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(s).strip() for s in raw if str(s).strip()]
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(s).strip() for s in parsed if str(s).strip()]
        except (json.JSONDecodeError, ValueError):
            pass
        return [s.strip() for s in raw.split(",") if s.strip()]
    return []


def _coerce_date(raw: Any) -> Optional[str]:
    if isinstance(raw, date):
        return raw.isoformat()
    if isinstance(raw, str):
        raw = raw.strip()
        if raw:
            return raw
    return None


def _build_row(data: Dict[str, Any], now: str) -> Dict[str, Any]:
    first    = (data.get("first_name") or "").strip()
    last     = (data.get("last_name")  or "").strip()
    composed = (
        f"{first} {last}".strip()
        or (data.get("candidate_name") or "").strip()
        or None
    )
    return {
        "first_name"                  : first or None,
        "last_name"                   : last  or None,
        "candidate_name"              : composed,
        "email_address"               : (data.get("email_address")    or "").strip() or None,
        "phone_number"                : (data.get("phone_number")     or "").strip() or None,
        "location"                    : (data.get("location")         or "").strip() or None,
        "pin_code"                    : (data.get("pin_code")         or "").strip() or None,
        "title"                       : (data.get("title")            or "").strip() or None,
        "current_company"             : (data.get("current_company")  or "").strip() or None,
        "current_position"            : (data.get("current_position") or "").strip() or None,
        "current_position_start_date" : _coerce_date(data.get("current_position_start_date")),
        "total_experience"            : int(data.get("total_experience") or 0),
        "notice_period"               : (data.get("notice_period")    or "").strip() or None,
        "work_mode_pref"              : (data.get("work_mode_pref")   or "").strip() or None,
        "education_degree"            : (data.get("education_degree")      or "").strip() or None,
        "education_institution"       : (data.get("education_institution") or "").strip() or None,
        "linkedin_profile"            : (data.get("linkedin_profile") or "").strip() or None,
        "skills"                      : json.dumps(_coerce_skills(data.get("skills"))),
        "profile_summary"             : (data.get("profile_summary")  or "").strip() or None,
        "source"                      : (data.get("source")           or "").strip() or None,
        "is_active"                   : bool(data.get("is_active", False)),
        "remarks"                     : (data.get("remarks") or "").strip() or None,
        "updated_at"                  : now,
    }


def _save_candidate(
    data:         Dict[str, Any],
    candidate_id: Optional[int] = None,
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    row = _build_row(data, now)
    if candidate_id:
        set_clause = ", ".join(f"{k} = %s" for k in row)
        sql        = f"UPDATE candidates SET {set_clause} WHERE id=%s RETURNING id"
        params: tuple = (*row.values(), candidate_id)
    else:
        cols   = ", ".join(row.keys())
        phs    = ", ".join(["%s"] * len(row))
        sql    = f"INSERT INTO candidates ({cols}) VALUES ({phs}) RETURNING id"
        params = tuple(row.values())
    with _db() as cur:
        cur.execute(sql, params)
        result = cur.fetchone()
        if not result:
            raise RuntimeError("DB returned no id after save.")
        return int(result["id"])


def _delete_candidate(candidate_id: int) -> None:
    with _db() as cur:
        cur.execute("DELETE FROM candidates WHERE id=%s", (candidate_id,))


def _bust_caches() -> None:
    _fetch_page.clear()
    _fetch_count.clear()


# ─────────────────────────────────────────────────────────────────────────────
# CSV export
# ─────────────────────────────────────────────────────────────────────────────

def _build_csv_bytes(query: str) -> bytes:
    where, params = _search_where(query)
    sql = (
        f"SELECT {_EXPORT_SELECT} FROM candidates "
        f"{where} ORDER BY created_at DESC NULLS LAST"
    )
    buf    = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(EXPORT_COLUMNS), extrasaction="ignore")
    writer.writerow({col: COLUMN_LABELS.get(col, col) for col in EXPORT_COLUMNS})
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        conn = _get_export_connection()
        with conn.cursor(
            name="profiles_export_cursor",
            cursor_factory=psycopg2.extras.RealDictCursor,
            withhold=False,
        ) as cur:
            cur.itersize = 2_000
            cur.execute(sql, params)
            for raw_row in cur:
                row = dict(raw_row)
                val = row.get("skills")
                if isinstance(val, list):
                    row["skills"] = ", ".join(str(s) for s in val)
                writer.writerow(row)
        conn.commit()
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    return ("\ufeff" + buf.getvalue()).encode("utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

_RE_EMAIL    = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_RE_LINKEDIN = re.compile(r"^https?://(www\.)?linkedin\.com/", re.IGNORECASE)


def _validate_form(data: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    first = (data.get("first_name") or "").strip()
    last  = (data.get("last_name")  or "").strip()
    if not first and not last:
        errors.append("At least one of First Name or Last Name is required.")
    email = (data.get("email_address") or "").strip()
    if not email:
        errors.append("Email Address is required.")
    elif not _RE_EMAIL.match(email):
        errors.append(f"'{email}' is not a valid email address.")
    linkedin = (data.get("linkedin_profile") or "").strip()
    if linkedin and not _RE_LINKEDIN.match(linkedin):
        errors.append("LinkedIn URL must start with https://linkedin.com/…")
    exp = data.get("total_experience")
    try:
        if exp not in (None, "") and int(exp) < 0:
            errors.append("Experience cannot be negative.")
    except (ValueError, TypeError):
        errors.append("Experience must be a whole number.")
    return errors


# ─────────────────────────────────────────────────────────────────────────────
# Session-state initialisation
# ─────────────────────────────────────────────────────────────────────────────

def _make_blank_filter() -> Dict[str, str]:
    return {
        "id"   : str(uuid.uuid4()),
        "field": FILTER_FIELD_LABELS[0],
        "op"   : "contains",
        "value": "",
    }


def _init_state() -> None:
    defaults: Dict[str, Any] = {
        "prof_page"         : 0,
        "prof_query"        : "",
        "prof_edit_id"      : None,
        "prof_panel_open"   : False,
        "prof_confirm_del"  : None,
        "prof_export_ready" : False,
        "prof_export_bytes" : None,
        "prof_export_ts"    : None,
        "prof_add_mode"     : False,
        "prof_multi_mode"   : False,
        # Initialise with exactly MIN_MULTI_FIELDS (2) blank rows
        "prof_filters"      : [_make_blank_filter() for _ in range(MIN_MULTI_FIELDS)],
        "prof_multi_json"   : None,
        "prof_remarks_draft": {},
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ─────────────────────────────────────────────────────────────────────────────
# UI helpers
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_option_index(options: List[str], value: Any) -> int:
    try:
        return options.index(value or "")
    except ValueError:
        return 0


def _render_search_bar() -> None:
    col_input, col_clear = st.columns([5, 1])
    with col_input:
        new_query = st.text_input(
            "🔍 Search candidates",
            value=st.session_state.prof_query,
            placeholder="Name, email, skill, location, company…",
            label_visibility="collapsed",
            key="prof_search_input",
        )
    with col_clear:
        if st.button("✕ Clear", use_container_width=True, key="prof_clear_btn"):
            st.session_state.prof_query = ""
            st.session_state.prof_page  = 0
            st.rerun()
    if new_query != st.session_state.prof_query:
        st.session_state.prof_query = new_query
        st.session_state.prof_page  = 0


# ─────────────────────────────────────────────────────────────────────────────
# Multi-field filter panel
# ─────────────────────────────────────────────────────────────────────────────

def _render_multi_search() -> Optional[str]:
    """
    Render dynamic filter rows keyed by stable UUIDs (not row index).

    Each filter dict:  {"id": <uuid>, "field": label, "op": op, "value": v}

    Minimum rows is MIN_MULTI_FIELDS (2).  Deleting a row removes it by UUID
    and pads back to the minimum with fresh blank rows so widget keys never
    collide with previously deleted rows.
    """
    filters: List[Dict[str, str]] = st.session_state.prof_filters

    # Back-compat: assign stable id to any legacy row that lacks one
    for f in filters:
        if "id" not in f:
            f["id"] = str(uuid.uuid4())

    # Pad up to minimum if somehow below it
    while len(filters) < MIN_MULTI_FIELDS:
        filters.append(_make_blank_filter())

    to_remove: Optional[str] = None  # UUID of the row the user clicked 🗑 on

    for f in filters:
        row_id = f["id"]
        c_field, c_op, c_val, c_rm = st.columns([2.5, 1.8, 3.0, 0.5])

        with c_field:
            f["field"] = st.selectbox(
                "Field",
                options=FILTER_FIELD_LABELS,
                index=_resolve_option_index(FILTER_FIELD_LABELS, f.get("field")),
                key=f"mf_field_{row_id}",
                label_visibility="collapsed",
            )
        with c_op:
            f["op"] = st.selectbox(
                "Op",
                options=FILTER_OPS,
                index=_resolve_option_index(FILTER_OPS, f.get("op", "contains")),
                key=f"mf_op_{row_id}",
                label_visibility="collapsed",
            )
        with c_val:
            no_val = f["op"] in _OPS_NO_VALUE
            f["value"] = st.text_input(
                "Value",
                value="" if no_val else f.get("value", ""),
                placeholder="—" if no_val else "search term…",
                disabled=no_val,
                key=f"mf_val_{row_id}",
                label_visibility="collapsed",
            )
        with c_rm:
            # Only show the delete button when doing so won't drop below minimum
            can_delete = len(filters) > MIN_MULTI_FIELDS
            if st.button(
                "🗑",
                key=f"mf_rm_{row_id}",
                help="Delete this filter row" if can_delete
                     else f"Cannot delete — minimum {MIN_MULTI_FIELDS} rows required",
                disabled=not can_delete,
            ):
                to_remove = row_id

    # ── Apply delete AFTER rendering all rows ────────────────────────────────
    if to_remove is not None:
        filters = [f for f in filters if f["id"] != to_remove]
        # Pad back to minimum with brand-new blank rows (fresh UUIDs)
        while len(filters) < MIN_MULTI_FIELDS:
            filters.append(_make_blank_filter())
        st.session_state.prof_filters = filters
        st.rerun()

    # ── Footer controls ───────────────────────────────────────────────────────
    col_add, col_apply, col_clear, _ = st.columns([1.2, 1.2, 1.2, 3.4])
    with col_add:
        if st.button("➕ Add filter", key="mf_add", use_container_width=True):
            filters.append(_make_blank_filter())
            st.session_state.prof_filters = filters
            st.rerun()
    with col_apply:
        apply_clicked = st.button(
            "▶ Apply", key="mf_apply", type="primary", use_container_width=True
        )
    with col_clear:
        if st.button("✕ Clear all", key="mf_clear", use_container_width=True):
            st.session_state.prof_filters    = [_make_blank_filter() for _ in range(MIN_MULTI_FIELDS)]
            st.session_state.prof_multi_json = None
            st.session_state.prof_page       = 0
            st.rerun()

    if apply_clicked:
        st.session_state.prof_page = 0

    active = [
        f for f in filters
        if f.get("op") in _OPS_NO_VALUE or (f.get("value") or "").strip()
    ]
    result = json.dumps(active) if active else None
    st.session_state.prof_multi_json = result
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Candidate table
# ─────────────────────────────────────────────────────────────────────────────

def _render_candidate_table(rows: List[Dict[str, Any]]) -> None:
    """
    Grid columns:
      Active | Candidate (name + email) | Phone | Location | Title |
      Profile Summary | Remarks | Actions
    """
    if not rows:
        st.info("No candidates found. Try a different search or click ➕ Add.")
        return

    # ── Header ───────────────────────────────────────────────────────────────
    hcols = st.columns([0.5, 2.0, 1.3, 1.4, 1.7, 2.8, 2.2, 1.0])
    for col, lbl in zip(
        hcols,
        ["Active", "Candidate", "Phone", "Location",
         "Title", "Profile Summary", "Remarks", "Actions"],
    ):
        col.markdown(f"**{lbl}**")
    st.divider()

    # ── Rows ─────────────────────────────────────────────────────────────────
    for row in rows:
        rid        = row["id"]
        name       = _clean(row.get("candidate_name"))
        email      = _clean(row.get("email_address"))
        phone      = _clean(row.get("phone_number"))
        loc        = _clean(row.get("location"))
        title      = _clean(row.get("title"))
        is_active  = bool(row.get("is_active") or False)
        db_remarks = row.get("remarks") or ""

        raw_summary = row.get("profile_summary") or ""
        summary = ((raw_summary[:110] + "…") if len(raw_summary) > 110 else raw_summary) or "—"

        c_act, c_name, c_ph, c_loc, c_title, c_sum, c_rem, c_act2 = st.columns(
            [0.5, 2.0, 1.3, 1.4, 1.7, 2.8, 2.2, 1.0]
        )

        # ── Active toggle ─────────────────────────────────────────────────
        with c_act:
            badge = "🟢" if is_active else "⚪"
            tip   = (
                "Active seeker — click to mark inactive" if is_active
                else "Not actively seeking — click to mark active"
            )
            if st.button(badge, key=f"prof_active_{rid}", help=tip):
                try:
                    _toggle_active(rid, not is_active)
                    _bust_caches()
                    st.rerun()
                except psycopg2.Error as exc:
                    st.error(f"Update failed: {exc}")

        # ── Candidate name + email ────────────────────────────────────────
        with c_name:
            email_md = (
                f"  \n<small style='color:#888'>{email}</small>"
                if email != "—" else ""
            )
            active_badge = (
                " <span style='background:#22c55e;color:#fff;"
                "border-radius:4px;padding:1px 5px;font-size:11px'>ACTIVE</span>"
                if is_active else ""
            )
            st.markdown(
                f"**{name}**{active_badge}{email_md}",
                unsafe_allow_html=True,
            )

        with c_ph:
            st.caption(phone)

        with c_loc:
            st.caption(loc)

        with c_title:
            st.caption(title)

        with c_sum:
            st.caption(summary)

        # ── Inline remarks editor ─────────────────────────────────────────
        with c_rem:
            draft_key = f"prof_remarks_draft_{rid}"
            if draft_key not in st.session_state:
                st.session_state[draft_key] = db_remarks

            new_remark = st.text_input(
                "Remarks",
                value=st.session_state[draft_key],
                placeholder="Add HR note…",
                label_visibility="collapsed",
                key=f"prof_rem_input_{rid}",
            )
            if new_remark != st.session_state[draft_key]:
                st.session_state[draft_key] = new_remark

            if new_remark.strip() != db_remarks.strip():
                if st.button("💾", key=f"prof_rem_save_{rid}", help="Save remark"):
                    try:
                        _save_remarks(rid, new_remark)
                        _bust_caches()
                        st.session_state[draft_key] = new_remark
                        st.rerun()
                    except psycopg2.Error as exc:
                        st.error(f"Save failed: {exc}")

        # ── Edit / Delete actions ─────────────────────────────────────────
        with c_act2:
            b_edit, b_del = st.columns(2)
            with b_edit:
                if st.button("✏️", key=f"prof_edit_{rid}", help="Edit candidate"):
                    st.session_state.prof_edit_id    = rid
                    st.session_state.prof_panel_open = True
                    st.session_state.prof_add_mode   = False
                    st.rerun()
            with b_del:
                if st.button("🗑️", key=f"prof_del_{rid}", help="Delete candidate"):
                    st.session_state.prof_confirm_del = rid
                    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Supporting UI components
# ─────────────────────────────────────────────────────────────────────────────

def _render_backfill_section() -> None:
    with st.expander("⚙️  Backfill Skills from Profile Summary", expanded=False):
        st.caption(
            "Scans every row where **skills is empty** and extracts skills "
            "from **profile_summary** using the built-in vocabulary. Safe to re-run."
        )
        col_dry, col_run, _ = st.columns([1, 1, 4])
        with col_dry:
            dry = st.checkbox("Dry run only", value=True, key="bf_dry")
        with col_run:
            run_clicked = st.button(
                "▶ Run backfill", key="bf_run",
                type="primary", use_container_width=True,
            )
        if run_clicked:
            with st.spinner("Scanning candidates…"):
                t0    = time.perf_counter()
                stats = backfill_skills_from_summary(dry_run=dry)
                secs  = time.perf_counter() - t0
            if stats["errors"]:
                st.error(f"Completed with {stats['errors']} error(s).")
            elif dry:
                st.info(
                    f"**Dry run** — would update **{stats['updated']}** rows · "
                    f"{stats['skipped']} skipped · {stats['scanned']} scanned · {secs:.1f} s"
                )
            else:
                _bust_caches()
                st.success(
                    f"✓ Updated **{stats['updated']}** rows · "
                    f"{stats['skipped']} skipped · {stats['scanned']} scanned · {secs:.1f} s"
                )


def _render_pagination(total: int) -> None:
    total_pages = max(1, -(-total // PAGE_SIZE))
    page        = st.session_state.prof_page
    col_prev, col_info, col_next = st.columns([1, 3, 1])
    with col_prev:
        if st.button("◀ Prev", disabled=(page == 0),
                     use_container_width=True, key="prof_prev_btn"):
            st.session_state.prof_page -= 1
            st.rerun()
    with col_info:
        start = page * PAGE_SIZE + 1
        end   = min((page + 1) * PAGE_SIZE, total)
        st.markdown(
            f"<div style='text-align:center;padding-top:8px'>"
            f"Page <b>{page+1}</b>/<b>{total_pages}</b> · "
            f"Showing <b>{start:,}–{end:,}</b> of <b>{total:,}</b></div>",
            unsafe_allow_html=True,
        )
    with col_next:
        if st.button("Next ▶", disabled=(page >= total_pages - 1),
                     use_container_width=True, key="prof_next_btn"):
            st.session_state.prof_page += 1
            st.rerun()


def _render_delete_confirm(candidate_id: int) -> None:
    st.warning(
        f"⚠️  Delete candidate **#{candidate_id}**? This cannot be undone.",
        icon="⚠️",
    )
    col_yes, col_no, _ = st.columns([1, 1, 5])
    with col_yes:
        if st.button("Yes, delete", type="primary", key="prof_del_yes",
                     use_container_width=True):
            try:
                _delete_candidate(candidate_id)
                _bust_caches()
                st.session_state.prof_confirm_del = None
                st.success("Candidate deleted.")
                time.sleep(0.5)
                st.rerun()
            except psycopg2.Error as exc:
                st.error(f"Delete failed: {exc}")
    with col_no:
        if st.button("Cancel", key="prof_del_no", use_container_width=True):
            st.session_state.prof_confirm_del = None
            st.rerun()


def _render_edit_panel(candidate_id: Optional[int]) -> None:
    is_new = candidate_id is None
    if not is_new:
        try:
            with _db() as cur:
                cur.execute("SELECT * FROM candidates WHERE id=%s", (candidate_id,))
                fetched = cur.fetchone()
        except psycopg2.Error as exc:
            st.error(friendly_conn_error(str(exc), get_pg_dsn()))
            return
        if not fetched:
            st.error(f"Candidate #{candidate_id} not found.")
            st.session_state.prof_panel_open = False
            return
        existing: Dict[str, Any] = dict(fetched)
    else:
        existing = {}

    heading = (
        "➕ Add Candidate" if is_new
        else f"✏️  {existing.get('candidate_name') or '#' + str(candidate_id)}"
    )
    st.subheader(heading)

    raw_skills = existing.get("skills", [])
    skills_str = (
        ", ".join(str(s) for s in raw_skills)
        if isinstance(raw_skills, list) else str(raw_skills or "")
    )

    raw_start = existing.get("current_position_start_date")
    start_date_val: Optional[date] = None
    if isinstance(raw_start, date):
        start_date_val = raw_start
    elif isinstance(raw_start, str) and raw_start.strip():
        try:
            start_date_val = date.fromisoformat(raw_start.strip())
        except ValueError:
            if re.match(r"^\d{4}-\d{2}$", raw_start.strip()):
                try:
                    start_date_val = date.fromisoformat(raw_start.strip() + "-01")
                except ValueError:
                    pass

    with st.form(key=f"prof_form_{candidate_id or 'new'}", clear_on_submit=False):

        st.markdown("**§1  Identity**")
        c1a, c1b = st.columns(2)
        with c1a:
            first_name = st.text_input("First Name", value=existing.get("first_name") or "")
        with c1b:
            last_name  = st.text_input("Last Name",  value=existing.get("last_name") or "")
        c1c, c1d = st.columns(2)
        with c1c:
            email_address = st.text_input("Email Address *",
                                          value=existing.get("email_address") or "")
        with c1d:
            phone_number  = st.text_input("Phone Number",
                                          value=existing.get("phone_number") or "")

        st.divider()
        st.markdown("**§2  Location**")
        c2a, c2b = st.columns(2)
        with c2a:
            location = st.text_input("General Location", value=existing.get("location") or "")
        with c2b:
            pin_code = st.text_input("ZIP / PIN Code",   value=existing.get("pin_code") or "")

        st.divider()
        st.markdown("**§3  Professional**")
        c3a, c3b = st.columns(2)
        with c3a:
            title           = st.text_input("Current Title",
                                            value=existing.get("title") or "")
            current_company = st.text_input("Current Company",
                                            value=existing.get("current_company") or "")
        with c3b:
            current_position = st.text_input("Current Position",
                                             value=existing.get("current_position") or "")
            total_experience = st.number_input(
                "Experience (yrs)", min_value=0, max_value=60,
                value=int(existing.get("total_experience") or 0), step=1,
            )
        c3c, c3d = st.columns(2)
        with c3c:
            current_position_start_date = st.date_input(
                "Position Start Date", value=start_date_val,
                help="Leave blank / clear if unknown",
            )
        with c3d:
            notice_period = st.selectbox(
                "Notice Period", options=NOTICE_PERIOD_OPTIONS,
                index=_resolve_option_index(NOTICE_PERIOD_OPTIONS,
                                            existing.get("notice_period") or ""),
            )
        work_mode_pref = st.selectbox(
            "Work Mode Preference", options=WORK_MODE_OPTIONS,
            index=_resolve_option_index(WORK_MODE_OPTIONS,
                                        existing.get("work_mode_pref") or ""),
        )

        st.divider()
        st.markdown("**§4  Profile**")
        linkedin_profile = st.text_input("LinkedIn Profile URL",
                                         value=existing.get("linkedin_profile") or "")
        profile_summary  = st.text_area("Profile Summary / Headline",
                                        value=existing.get("profile_summary") or "",
                                        height=100)

        st.divider()
        st.markdown("**§5  Education**")
        c5a, c5b = st.columns(2)
        with c5a:
            education_degree      = st.text_input("Education Degree",
                                                  value=existing.get("education_degree") or "")
        with c5b:
            education_institution = st.text_input("Education Institution",
                                                  value=existing.get("education_institution") or "")

        st.divider()
        st.markdown("**§6  Skills**")
        skills_input = st.text_area(
            "Skills (comma-separated)", value=skills_str, height=80,
            help="e.g. Python, SQL, AWS — or use Backfill to auto-extract.",
        )

        st.divider()
        st.markdown("**§7  Status & Remarks**")
        is_active_input = st.toggle(
            "🟢  Actively seeking a new role",
            value=bool(existing.get("is_active") or False),
            help="Turn ON if the candidate is currently open to opportunities.",
        )
        remarks_input = st.text_area(
            "HR Remarks",
            value=existing.get("remarks") or "",
            height=80,
            placeholder="Internal notes visible only to HR team…",
        )

        st.divider()
        st.markdown("**§8  Meta**")
        source = st.text_input("Source", value=existing.get("source") or "")

        st.divider()
        col_save, col_cancel = st.columns(2)
        with col_save:
            submitted = st.form_submit_button(
                "➕ Create" if is_new else "💾 Save",
                type="primary", use_container_width=True,
            )
        with col_cancel:
            cancelled = st.form_submit_button("✕ Cancel", use_container_width=True)

    if cancelled:
        st.session_state.prof_panel_open = False
        st.session_state.prof_edit_id    = None
        st.session_state.prof_add_mode   = False
        st.rerun()

    if submitted:
        form_data: Dict[str, Any] = {
            "first_name"                  : first_name,
            "last_name"                   : last_name,
            "email_address"               : email_address,
            "phone_number"                : phone_number,
            "location"                    : location,
            "pin_code"                    : pin_code,
            "title"                       : title,
            "current_company"             : current_company,
            "current_position"            : current_position,
            "current_position_start_date" : current_position_start_date,
            "total_experience"            : total_experience,
            "notice_period"               : notice_period,
            "work_mode_pref"              : work_mode_pref,
            "linkedin_profile"            : linkedin_profile,
            "profile_summary"             : profile_summary,
            "education_degree"            : education_degree,
            "education_institution"       : education_institution,
            "skills"                      : skills_input,
            "source"                      : source,
            "is_active"                   : is_active_input,
            "remarks"                     : remarks_input,
        }
        errors = _validate_form(form_data)
        if errors:
            for err in errors:
                st.error(err)
        else:
            try:
                saved_id = _save_candidate(form_data, candidate_id)
                _bust_caches()
                st.success(
                    f"{'Created' if is_new else 'Updated'} candidate #{saved_id} ✓"
                )
                time.sleep(0.6)
                st.session_state.prof_panel_open = False
                st.session_state.prof_edit_id    = None
                st.session_state.prof_add_mode   = False
                st.rerun()
            except psycopg2.Error as exc:
                st.error(friendly_conn_error(str(exc), get_pg_dsn()))


def _render_export_section() -> None:
    st.markdown("---")
    st.markdown("#### 📥  Export Candidates")
    col_btn, col_dl, col_info = st.columns([1.5, 1.5, 4])
    with col_btn:
        if st.button("⚙️ Prepare CSV", use_container_width=True, key="prof_export_btn"):
            with st.spinner("Building CSV…"):
                try:
                    t0   = time.perf_counter()
                    data = _build_csv_bytes(st.session_state.prof_query)
                    st.session_state.prof_export_bytes = data
                    st.session_state.prof_export_ready = True
                    st.session_state.prof_export_ts    = time.perf_counter() - t0
                except Exception as exc:
                    st.error(f"Export failed: {exc}")
                    st.session_state.prof_export_ready = False
    with col_dl:
        if st.session_state.prof_export_ready and st.session_state.prof_export_bytes:
            ts    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
            q_tag = re.sub(r"[^\w\-]", "_", st.session_state.prof_query or "all")
            st.download_button(
                "⬇️ Download CSV",
                data=st.session_state.prof_export_bytes,
                file_name=f"candidates_{q_tag}_{ts}.csv",
                mime="text/csv",
                use_container_width=True,
                key="prof_download_btn",
            )
    with col_info:
        if (st.session_state.prof_export_ready
                and st.session_state.prof_export_bytes
                and st.session_state.prof_export_ts is not None):
            size_kb = len(st.session_state.prof_export_bytes) / 1024
            st.caption(
                f"✓ Ready · {size_kb:,.0f} KB · "
                f"built in {st.session_state.prof_export_ts:.1f} s"
            )


def _render_diagnostics() -> None:
    with st.expander("🔧  Connection Diagnostics", expanded=False):
        try:
            v      = Config.validate()
            stats  = _pool_stats()
            pct    = stats["used"] / max(stats["max"], 1)
            health = "🟢 OK" if pct < 0.8 else "🟡 Busy" if pct < 1.0 else "🔴 Exhausted"
            st.markdown(f"**DB mode:** `{v['database']}`")
            st.markdown(f"**DSN source:** `{v['pg_dsn_source']}`")
            st.markdown(f"**DSN (safe):** `{v['pg_dsn_safe']}`")
            st.markdown(f"**Cache version:** `{CACHE_VERSION}`")
            st.markdown(
                f"**Pool:** {health} — "
                f"{stats['used']} used / {stats['free']} free / "
                f"{stats['total']} open (max {stats['max']})"
            )
            st.markdown("---")
            col_test, col_clear = st.columns(2)
            with col_test:
                if st.button("🔄  Test connection", key="prof_diag_test"):
                    t0 = time.perf_counter()
                    try:
                        with _db() as cur:
                            cur.execute("SELECT COUNT(*) FROM candidates")
                            cnt = (cur.fetchone() or {}).get("count", 0)
                        st.success(
                            f"Connected — {cnt:,} rows · "
                            f"{(time.perf_counter()-t0)*1000:.1f} ms"
                        )
                    except Exception as exc:
                        st.error(f"Connection failed: {exc}")
            with col_clear:
                if st.button("🗑️  Clear query cache", key="prof_diag_clear"):
                    _fetch_page.clear()
                    _fetch_count.clear()
                    st.success("Cache cleared.")
                    time.sleep(0.4)
                    st.rerun()
            if v.get("warnings"):
                for w in v["warnings"]:
                    st.warning(w)
            else:
                st.success("No configuration warnings.")
        except Exception as exc:
            st.warning(f"Diagnostics unavailable: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Primary entry-point
# ─────────────────────────────────────────────────────────────────────────────

def render_profiles() -> None:
    _init_state()

    st.markdown("## 👥  Candidate Profiles")
    st.caption(
        f"Up to {PAGE_SIZE} records per page · "
        f"Search across {len(SEARCH_COLUMNS)} fields · "
        f"Schema {CACHE_VERSION}"
    )

    # ── Toolbar ───────────────────────────────────────────────────────────────
    tb1, tb2, tb3, tb4 = st.columns([4, 1.4, 1, 1])
    with tb1:
        if not st.session_state.prof_multi_mode:
            _render_search_bar()
        else:
            st.caption("🔎 Multi-field search active — configure filters below")
    with tb2:
        toggle_label = (
            "✕ Simple search" if st.session_state.prof_multi_mode
            else "🔎 Multi-filter"
        )
        if st.button(toggle_label, use_container_width=True, key="prof_toggle_mode"):
            st.session_state.prof_multi_mode = not st.session_state.prof_multi_mode
            st.session_state.prof_page       = 0
            st.session_state.prof_multi_json = None
            st.rerun()
    with tb3:
        if st.button("➕ Add", use_container_width=True, key="prof_add_btn"):
            st.session_state.prof_add_mode   = True
            st.session_state.prof_panel_open = True
            st.session_state.prof_edit_id    = None
            st.rerun()
    with tb4:
        pass

    # ── Multi-field filters ───────────────────────────────────────────────────
    multi_json: Optional[str] = None
    if st.session_state.prof_multi_mode:
        with st.container():
            st.markdown("##### 🔎 Multi-field filters")
            st.caption(
                "All active rows combined with **AND** logic. "
                "🗑 deletes a row (minimum "
                f"{MIN_MULTI_FIELDS} rows kept at all times)."
            )
            multi_json = _render_multi_search()

    # ── Delete confirmation ───────────────────────────────────────────────────
    if st.session_state.prof_confirm_del is not None:
        _render_delete_confirm(st.session_state.prof_confirm_del)

    # ── Data fetch ────────────────────────────────────────────────────────────
    query  = "" if st.session_state.prof_multi_mode else st.session_state.prof_query
    offset = st.session_state.prof_page * PAGE_SIZE

    try:
        total = _fetch_count(query, cache_ver=CACHE_VERSION, multi_filters=multi_json)
        rows  = _fetch_page(query, offset, cache_ver=CACHE_VERSION, multi_filters=multi_json)
    except EnvironmentError as exc:
        st.error(str(exc)); _render_diagnostics(); return
    except psycopg2.OperationalError as exc:
        st.error(friendly_conn_error(str(exc), get_pg_dsn()))
        _render_diagnostics(); return
    except psycopg2.Error as exc:
        st.error(f"Database error: {exc}"); _render_diagnostics(); return

    # ── Layout ────────────────────────────────────────────────────────────────
    if st.session_state.prof_panel_open:
        col_table, col_panel = st.columns([6, 4])
        with col_table:
            _render_pagination(total)
            _render_candidate_table(rows)
        with col_panel:
            _render_edit_panel(
                None if st.session_state.prof_add_mode
                else st.session_state.prof_edit_id
            )
    else:
        _render_pagination(total)
        _render_candidate_table(rows)

    _render_backfill_section()
    _render_export_section()
    _render_diagnostics()


render_profile_database = render_profiles
