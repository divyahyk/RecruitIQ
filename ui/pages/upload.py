# ui/pages/upload.py
# RecruitIQ · Upload Page — High-Performance Direct PostgreSQL Import
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import StringIO

import numpy as np
import pandas as pd
import psycopg2
import streamlit as st
from psycopg2.extras import execute_values

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
CHUNK_SIZE      = 5_000   # rows per INSERT statement
MAX_WORKERS     = 4       # parallel DB connections
CONNECT_TIMEOUT = 20      # seconds
EMAIL_COL       = "email_address"

# All recognised env-var / secret key names (checked in order)
# Priority: PG_CONN_STRING (your .env) → DATABASE_URL → POSTGRES_URL → DB_URL
_CONN_STR_KEYS = [
    "PG_CONN_STRING",   # your .env / secrets.toml
    "DATABASE_URL",     # common Heroku / Railway alias
    "POSTGRES_URL",     # Vercel / Neon alias
    "DB_URL",           # short alias
]

# ── Exact DB columns (must match your candidates table) ──────────────────────
DB_COLS: list[str] = [
    "email_address",
    "candidate_name",
    "first_name",
    "last_name",
    "phone_number",
    "location",
    "pin_code",
    "profile_summary",
    "title",
    "current_company",
    "current_position",
    "current_position_start_date",
    "education_degree",
    "education_institution",
    "linkedin_profile",
    "total_experience",
    "skills",
    "notice_period",
    "work_mode_pref",
    "source",
]

# ── Flexible header aliases (normalised matching) ─────────────────────────────
COLUMN_MAP: dict[str, list[str]] = {
    "email_address":                ["email address", "email", "e-mail",
                                     "e mail", "email_address"],
    "first_name":                   ["first name", "first_name", "firstname",
                                     "fname"],
    "last_name":                    ["last name", "last_name", "lastname",
                                     "surname", "lname"],
    "candidate_name":               ["candidate name", "full name", "name",
                                     "candidate_name"],
    "phone_number":                 ["phone number", "phone", "mobile",
                                     "contact number", "phone_number"],
    "location":                     ["general location", "location", "city",
                                     "place", "address"],
    "pin_code":                     ["zip code", "pin code", "pincode",
                                     "zip", "postal code", "pin_code"],
    "profile_summary":              ["headline", "summary", "profile summary",
                                     "about", "bio", "profile_summary"],
    "title":                        ["current title", "title", "job title",
                                     "designation", "role", "position"],
    "current_company":              ["current company", "company", "employer",
                                     "organisation", "organization",
                                     "current_company"],
    "current_position":             ["current position", "current_position",
                                     "job position"],
    "current_position_start_date":  ["current position start date",
                                     "position start date", "start date",
                                     "current_position_start_date"],
    "education_degree":             ["education degree", "degree",
                                     "qualification", "education_degree"],
    "education_institution":        ["education institution", "institution",
                                     "university", "college",
                                     "education_institution"],
    "linkedin_profile":             ["profile url", "linkedin", "linkedin url",
                                     "linkedin profile", "linkedin_profile"],
    "total_experience":             ["total experience", "experience",
                                     "years of experience", "exp", "yoe",
                                     "total_experience"],
    "skills":                       ["skills", "tech stack", "technologies",
                                     "keywords", "skill set"],
    "notice_period":                ["notice period", "notice",
                                     "notice_period"],
    "work_mode_pref":               ["work mode", "work preference",
                                     "remote", "work_mode_pref"],
    "source":                       ["source", "lead source"],
}

# ─────────────────────────────────────────────────────────────────────────────
# UPSERT SQL  (built once at module load)
# ─────────────────────────────────────────────────────────────────────────────
_UPDATE_COLS = [c for c in DB_COLS if c != EMAIL_COL]

UPSERT_SQL = f"""
    INSERT INTO candidates ({', '.join(DB_COLS)})
    VALUES %s
    ON CONFLICT (email_address) DO UPDATE SET
        {', '.join(f'{c} = EXCLUDED.{c}' for c in _UPDATE_COLS)},
        updated_at = now()
    RETURNING (xmax = 0) AS is_insert
"""

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION STRING RESOLVER  ← FIXED: checks all known key names
# ─────────────────────────────────────────────────────────────────────────────

def _get_conn_str(services: dict | None = None) -> str | None:
    """
    Resolve psycopg2 connection string.

    Priority order:
        1. st.secrets   — checks every key in _CONN_STR_KEYS
        2. os.environ   — checks every key in _CONN_STR_KEYS
        3. services dict — looks for conn_str / database_url / pg_conn_string attrs

    This means both PG_CONN_STRING and DATABASE_URL work transparently.
    """
    # 1. Streamlit secrets (.streamlit/secrets.toml or Streamlit Cloud)
    try:
        for key in _CONN_STR_KEYS:
            val = st.secrets.get(key)
            if val:
                return str(val)
    except Exception:
        pass

    # 2. Environment variables  (.env loaded by dotenv, or shell export)
    for key in _CONN_STR_KEYS:
        val = os.environ.get(key)
        if val:
            return val

    # 3. services dict passed from app.py  (db object with connection attribute)
    if services:
        db = services.get("db")
        if db:
            for attr in ("conn_str", "database_url", "pg_conn_string"):
                val = getattr(db, attr, None)
                if val:
                    return str(val)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# HEADER NORMALISATION  (fuzzy matching)
# ─────────────────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lowercase → strip punctuation → collapse spaces."""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _detect_mapping(columns: list[str]) -> dict[str, str]:
    """
    Return {canonical_db_col: actual_df_column}.
    Uses normalised fuzzy matching so 'E-Mail Address' → 'email_address'.
    """
    norm_to_actual = {_norm(c): c for c in columns}
    mapping: dict[str, str] = {}
    for db_col, aliases in COLUMN_MAP.items():
        for alias in aliases:
            if _norm(alias) in norm_to_actual:
                mapping[db_col] = norm_to_actual[_norm(alias)]
                break
    return mapping


# ─────────────────────────────────────────────────────────────────────────────
# FAST VECTORISED ROW BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _build_rows(
    df: pd.DataFrame,
    mapping: dict[str, str],
    source_label: str,
) -> tuple[list[tuple], list[dict]]:
    """
    Convert DataFrame → list of tuples matching DB_COLS order.
    Pure vectorised — handles 350k rows in ~2 seconds.
    """
    # Rename mapped columns to canonical names
    rename = {actual: canon for canon, actual in mapping.items()}
    work   = df.rename(columns=rename).copy()

    # Add any missing DB columns as None
    for col in DB_COLS:
        if col not in work.columns:
            work[col] = None

    # ── String columns: strip + None-ify empties ──────────────────────────────
    str_cols = [
        "email_address", "candidate_name", "first_name", "last_name",
        "phone_number", "location", "pin_code", "profile_summary",
        "title", "current_company", "current_position",
        "current_position_start_date", "education_degree",
        "education_institution", "linkedin_profile",
        "notice_period", "work_mode_pref",
    ]
    for col in str_cols:
        if col in work.columns:
            work[col] = (
                work[col]
                .astype(str)
                .str.strip()
                .replace({"nan": None, "None": None, "": None})
            )

    # ── Derive candidate_name if missing ──────────────────────────────────────
    mask = work["candidate_name"].isna()
    if mask.any():
        fn      = work.loc[mask, "first_name"].fillna("")
        ln      = work.loc[mask, "last_name"].fillna("")
        derived = (fn + " " + ln).str.strip().replace("", None)
        work.loc[mask, "candidate_name"] = derived

    # ── Source ────────────────────────────────────────────────────────────────
    work["source"] = source_label or "csv_import"

    # ── total_experience → int or None ───────────────────────────────────────
    work["total_experience"] = (
        pd.to_numeric(work["total_experience"], errors="coerce")
        .apply(lambda x: int(x) if pd.notna(x) else None)
    )

    # ── skills → JSON array string ────────────────────────────────────────────
    def _to_skills_json(val) -> str | None:
        if val is None or val is np.nan:
            return None
        if isinstance(val, (list, dict)):
            return json.dumps(val)
        s = str(val).strip()
        if not s or s.lower() in ("nan", "none", "null"):
            return None
        parts = [p.strip() for p in re.split(r"[,;|]", s) if p.strip()]
        return json.dumps(parts) if parts else None

    work["skills"] = work["skills"].apply(_to_skills_json)

    # ── Drop rows with no email ───────────────────────────────────────────────
    missing_mask = work[EMAIL_COL].isna()
    errors = [
        {"row": int(i) + 2, "reason": "Missing email_address"}
        for i in work.index[missing_mask]
    ]
    work = work[~missing_mask].copy()

    # ── Deduplicate within batch (keep last occurrence) ───────────────────────
    work = work.drop_duplicates(subset=[EMAIL_COL], keep="last")

    # ── Build tuples in exact DB_COLS order ───────────────────────────────────
    rows = list(work[DB_COLS].itertuples(index=False, name=None))

    return rows, errors


# ─────────────────────────────────────────────────────────────────────────────
# PARALLEL BULK INSERT
# ─────────────────────────────────────────────────────────────────────────────

def _insert_chunk(
    chunk: list[tuple],
    conn_str: str,
) -> dict:
    """Insert one chunk via psycopg2 — runs in a thread."""
    inserted = updated = 0
    try:
        with psycopg2.connect(conn_str, connect_timeout=CONNECT_TIMEOUT) as conn:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, chunk, page_size=len(chunk))
                for (is_ins,) in cur.fetchall():
                    if is_ins:
                        inserted += 1
                    else:
                        updated += 1
            conn.commit()
    except Exception as exc:
        return {"inserted": 0, "updated": 0, "error": str(exc)}
    return {"inserted": inserted, "updated": updated, "error": None}


def _bulk_insert_parallel(
    rows: list[tuple],
    conn_str: str,
    on_progress=None,          # callable(done_chunks, total_chunks, ins, upd)
) -> dict:
    """Split into chunks and insert with thread pool."""
    chunks       = [rows[i : i + CHUNK_SIZE] for i in range(0, len(rows), CHUNK_SIZE)]
    total_chunks = len(chunks)
    inserted = updated = done = 0
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_insert_chunk, chunk, conn_str): idx
            for idx, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            res       = future.result()
            inserted += res["inserted"]
            updated  += res["updated"]
            done     += 1
            if res["error"]:
                errors.append(res["error"])
            if on_progress:
                on_progress(done, total_chunks, inserted, updated)

    return {"inserted": inserted, "updated": updated, "errors": errors}


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTICS HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _render_db_diagnostics(services: dict) -> None:
    """Quick health check shown at top of page."""
    conn_str = _get_conn_str(services)
    db       = services.get("db")

    with st.expander("🔌 Database Status", expanded=False):

        # ── Show which key was resolved ───────────────────────────────────────
        resolved_key = "not found"
        try:
            for key in _CONN_STR_KEYS:
                if st.secrets.get(key):
                    resolved_key = f"st.secrets['{key}']"
                    break
        except Exception:
            pass
        if resolved_key == "not found":
            for key in _CONN_STR_KEYS:
                if os.environ.get(key):
                    resolved_key = f"env:{key}"
                    break

        st.caption(f"🔑 Connection resolved from: **{resolved_key}**")
        if conn_str:
            # Mask password for safe display
            masked = re.sub(r":(.*?)@", ":***@", conn_str)
            st.caption(f"🔗 `{masked}`")

        col1, col2 = st.columns(2)

        # ── Supabase REST API ─────────────────────────────────────────────────
        with col1:
            st.markdown("**Supabase REST API**")
            if db is None:
                st.error("❌ db object is None")
            elif not hasattr(db, "client"):
                st.error("❌ db.client missing")
            else:
                try:
                    r = db.client.table("candidates").select("email_address").limit(1).execute()
                    st.success(f"✅ Reachable · {len(r.data)} row sample")
                except Exception as e:
                    st.error(f"❌ {e}")

        # ── Direct psycopg2 ───────────────────────────────────────────────────
        with col2:
            st.markdown("**Direct PostgreSQL (psycopg2)**")
            if not conn_str:
                st.error(
                    "❌ No connection string found.\n\n"
                    f"Tried keys: `{', '.join(_CONN_STR_KEYS)}`"
                )
            else:
                try:
                    with psycopg2.connect(conn_str, connect_timeout=5) as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT COUNT(*) FROM candidates")
                            count = cur.fetchone()[0]
                    st.success(f"✅ Connected · {count:,} rows in candidates")
                except Exception as e:
                    st.error(f"❌ {e}")


def _diagnose_error(error_str: str) -> None:
    """Plain-English fix suggestions for common errors."""
    err  = error_str.lower()
    tips = {
        ("column", "does not exist"):
            "A column name in the payload doesn't exist in the DB table. "
            "Run the schema SQL in Supabase → SQL Editor.",
        ("schema cache",):
            "PostgREST cache is stale. Run: `NOTIFY pgrst, 'reload schema';` "
            "in Supabase SQL Editor, then wait 30s.",
        ("row-level security", "rls"):
            "RLS is blocking the insert. Add INSERT/UPDATE policies on `candidates` "
            "or use the service_role key.",
        ("unique", "duplicate"):
            "Duplicate email_address detected outside the upsert path. "
            "Ensure upsert uses `on_conflict='email_address'`.",
        ("permission denied",):
            "API key lacks INSERT permission. Use the service_role key.",
        ("relation", "does not exist"):
            "`candidates` table not found. Run the CREATE TABLE SQL.",
        ("jwt", "invalid api key", "apikey"):
            "Invalid API key. Check SUPABASE_URL and SUPABASE_KEY in .env.",
        ("not null",):
            "A NOT NULL column is receiving an empty value.",
        ("timeout", "timed out"):
            "Connection timed out. Verify port 5432 is used (not 6543). "
            "Check firewall / Supabase connection limit.",
    }
    for keywords, msg in tips.items():
        if all(k in err for k in keywords):
            st.warning(f"🔧 **Fix:** {msg}")
            return
    st.info("ℹ️ Unknown error — check Supabase Dashboard → Logs → API logs.")


# ─────────────────────────────────────────────────────────────────────────────
# IMPORT TAB  (main render)
# ─────────────────────────────────────────────────────────────────────────────

def _render_import_tab(services: dict) -> None:
    conn_str = _get_conn_str(services)

    if not conn_str:
        st.error(
            "❌ **No PostgreSQL connection string found.**\n\n"
            "Add one of these to `.env` or `.streamlit/secrets.toml`:\n\n"
            "```\n"
            "PG_CONN_STRING=postgresql://postgres:PASSWORD@db.xxx.supabase.co:5432/postgres\n"
            "DATABASE_URL=postgresql://postgres:PASSWORD@db.xxx.supabase.co:5432/postgres\n"
            "```\n\n"
            "⚠️ **Use port `5432`** (direct), not `6543` (transaction pooler)."
        )
        return

    # ── Column map reference ──────────────────────────────────────────────────
    with st.expander("📋 Recognised column headers", expanded=False):
        rows_ref = []
        for db_col, aliases in COLUMN_MAP.items():
            rows_ref.append({
                "DB Column":               db_col,
                "Recognised File Headers": " · ".join(aliases),
            })
        st.dataframe(
            pd.DataFrame(rows_ref),
            use_container_width=True,
            hide_index=True,
        )

    # ── File upload ───────────────────────────────────────────────────────────
    uploaded = st.file_uploader(
        "Choose a file (.xlsx, .xls, .csv)",
        type=["xlsx", "xls", "csv"],
        key="upload_file",
    )
    if not uploaded:
        return

    # ── Parse ─────────────────────────────────────────────────────────────────
    with st.spinner("Reading file…"):
        try:
            if uploaded.name.lower().endswith((".xlsx", ".xls")):
                df_raw = pd.read_excel(uploaded, dtype=str)
            else:
                raw    = uploaded.read().decode("utf-8", errors="replace")
                df_raw = pd.read_csv(StringIO(raw), dtype=str, low_memory=False)
        except Exception as exc:
            st.error(f"❌ Could not read file: {exc}")
            return

    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    mapping        = _detect_mapping(df_raw.columns.tolist())

    # ── Mapping diagnostics ───────────────────────────────────────────────────
    mapped_file_cols   = set(mapping.values())
    unmapped_file_cols = [c for c in df_raw.columns if c not in mapped_file_cols]

    st.success(
        f"✅ Loaded **{len(df_raw):,} rows** · "
        f"**{len(mapping)}/{len(COLUMN_MAP)}** columns mapped"
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**✅ Mapped columns**")
        mapped_df = pd.DataFrame([
            {"File Header": v, "→ DB Column": k}
            for k, v in mapping.items()
        ])
        st.dataframe(mapped_df, use_container_width=True, hide_index=True)
    with col_b:
        if unmapped_file_cols:
            st.markdown("**⚠️ Unmapped (will be ignored)**")
            st.dataframe(
                pd.DataFrame({"File Column": unmapped_file_cols}),
                use_container_width=True,
                hide_index=True,
            )

    if EMAIL_COL not in mapping:
        st.error(
            f"❌ No email column detected in your file.\n\n"
            f"File has: `{', '.join(df_raw.columns.tolist())}`\n\n"
            f"Expected one of: `{', '.join(COLUMN_MAP[EMAIL_COL])}`"
        )
        return

    # ── Preview ───────────────────────────────────────────────────────────────
    st.markdown("#### Preview — first 5 rows")
    st.dataframe(df_raw.head(5), use_container_width=True, hide_index=True)

    # ── Row counts ────────────────────────────────────────────────────────────
    email_col_name = mapping[EMAIL_COL]
    missing_email  = (
        df_raw[email_col_name].isna().sum()
        + df_raw[email_col_name].astype(str).str.strip().eq("").sum()
    )
    ready_count = len(df_raw) - missing_email

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Rows",      f"{len(df_raw):,}")
    m2.metric("Missing Email",   f"{missing_email:,}")
    m3.metric("Ready to Import", f"{ready_count:,}")

    source_label = st.text_input(
        "Source label",
        value="csv_import",
        help="Tag saved to the 'source' column so you know where records came from",
    )

    if not st.button(
        f"🚀 Import {ready_count:,} Candidates",
        type="primary",
        use_container_width=True,
        disabled=(ready_count == 0),
    ):
        return

    # ─────────────────────────────────────────────────────────────────────────
    # IMPORT PIPELINE
    # ─────────────────────────────────────────────────────────────────────────
    t0     = time.perf_counter()
    status = st.status("⚙️ Starting import…", expanded=True)

    with status:

        # Step 1 — Build rows
        st.write("🔄 **Step 1/2** — Preparing data (vectorised)…")
        rows, parse_errors = _build_rows(df_raw, mapping, source_label)
        build_s = time.perf_counter() - t0

        st.write(
            f"✅ **{len(rows):,}** rows prepared in **{build_s:.1f}s** · "
            f"⚠️ **{len(parse_errors)}** skipped (no email)"
        )

        if parse_errors:
            with st.expander(f"Skipped rows ({len(parse_errors)})"):
                st.dataframe(
                    pd.DataFrame(parse_errors),
                    use_container_width=True,
                    hide_index=True,
                )

        if not rows:
            st.error("No valid rows to insert.")
            status.update(label="❌ Nothing to insert", state="error")
            return

        # Step 2 — Insert
        total_chunks = -(-len(rows) // CHUNK_SIZE)
        st.write(
            f"📦 **Step 2/2** — Inserting via PostgreSQL direct connection…\n\n"
            f"`{total_chunks}` chunks × `{CHUNK_SIZE:,}` rows · "
            f"`{MAX_WORKERS}` parallel connections"
        )

        prog_bar  = st.progress(0.0)
        prog_text = st.empty()

        def _on_progress(done, total, ins, upd):
            pct     = done / total
            elapsed = time.perf_counter() - t0
            rate    = (ins + upd) / elapsed if elapsed > 0 else 0
            prog_bar.progress(pct)
            prog_text.markdown(
                f"**Chunk {done}/{total}** — "
                f"✅ `{ins:,}` new · "
                f"🔄 `{upd:,}` updated · "
                f"⚡ `{rate:,.0f}` rows/sec · "
                f"⏱ `{elapsed:.0f}s` elapsed"
            )

        result  = _bulk_insert_parallel(rows, conn_str, on_progress=_on_progress)
        elapsed = time.perf_counter() - t0
        rate    = len(rows) / elapsed if elapsed > 0 else 0

        if result["errors"]:
            status.update(label="⚠️ Import completed with errors", state="error")
            st.error(f"**{len(result['errors'])} chunk error(s):**")
            for err in result["errors"][:3]:
                st.code(err)
                _diagnose_error(err)
        else:
            status.update(label="✅ Import complete!", state="complete")

    # ── Final summary ─────────────────────────────────────────────────────────
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("⏱ Time",      f"{elapsed:.1f}s")
    s2.metric("⚡ Speed",    f"{rate:,.0f}/s")
    s3.metric("✅ Inserted", f"{result['inserted']:,}")
    s4.metric("🔄 Updated",  f"{result['updated']:,}")

    if result["inserted"] + result["updated"] > 0:
        st.success(
            f"🎉 **Import complete** in `{elapsed:.1f}s` at `{rate:,.0f}` rows/sec\n\n"
            f"- **{result['inserted']:,}** new candidates added\n"
            f"- **{result['updated']:,}** existing candidates updated"
        )


# ─────────────────────────────────────────────────────────────────────────────
# MANUAL ENTRY TAB (COMPLETE — ALL 20 COLUMNS)
# ─────────────────────────────────────────────────────────────────────────────

def _render_manual_tab(services: dict) -> None:
    """
    Manual single-candidate entry form matching ALL 20 columns in candidates table.
    Organized into logical sections for better UX.
    """
    db = services.get("db")
    st.subheader("➕ Add a Single Candidate")

    # ── SECTION 1: CONTACT INFORMATION ────────────────────────────────────────
    st.markdown("### 📧 Contact Information")
    c1, c2, c3 = st.columns(3)
    with c1:
        first_name = st.text_input("First Name *", key="m_fn", 
                                  help="Required for candidate_name derivation")
    with c2:
        last_name = st.text_input("Last Name *", key="m_ln",
                                 help="Required for candidate_name derivation")
    with c3:
        email = st.text_input("Email Address *", key="m_em", 
                             help="Unique identifier — cannot be duplicated")

    c4, c5, c6 = st.columns(3)
    with c4:
        phone = st.text_input("Phone Number", key="m_ph", 
                             placeholder="+1-555-0000 or 555-0000")
    with c5:
        location = st.text_input("Location / City", key="m_lo", 
                                placeholder="New York, NY or Dubai, UAE")
    with c6:
        pin_code = st.text_input("ZIP / PIN Code", key="m_pc", 
                                placeholder="10001 or 400001")

    # ── SECTION 2: CURRENT EMPLOYMENT ────────────────────────────────────────
    st.markdown("### 💼 Current Employment")
    c7, c8, c9 = st.columns(3)
    with c7:
        title = st.text_input("Current Title / Designation", key="m_ti", 
                             placeholder="Senior Software Engineer")
    with c8:
        company = st.text_input("Current Company", key="m_cc", 
                               placeholder="Acme Corp")
    with c9:
        position = st.text_input("Current Position", key="m_cp", 
                                placeholder="Engineer / Manager / Analyst")

    c10, c11, c12 = st.columns(3)
    with c10:
        start_date = st.text_input("Position Start Date (YYYY-MM-DD)", key="m_sd",
                                  placeholder="2023-01-15",
                                  help="When did they start this role?")
    with c11:
        experience = st.number_input("Total Years of Experience", 
                                    min_value=0, max_value=70, value=0,
                                    step=1, key="m_ex",
                                    help="Total career experience in years")
    with c12:
        notice_period = st.text_input("Notice Period", key="m_np",
                                     placeholder="Immediate / 2 weeks / 30 days",
                                     help="When can they start?")

    # ── SECTION 3: EDUCATION ──────────────────────────────────────────────────
    st.markdown("### 🎓 Education")
    c13, c14 = st.columns(2)
    with c13:
        edu_degree = st.text_input("Education Degree / Qualification", key="m_ed",
                                  placeholder="B.Tech, MBA, M.Sc, B.A., Ph.D.",
                                  help="Highest degree obtained")
    with c14:
        edu_inst = st.text_input("Education Institution", key="m_ei",
                                placeholder="Stanford University, MIT, IIT Delhi",
                                help="University or college name")

    # ── SECTION 4: SKILLS & WORK PREFERENCE ───────────────────────────────────
    st.markdown("### 🛠️ Skills & Work Preference")
    c15, c16 = st.columns(2)
    with c15:
        skills = st.text_input("Skills (comma or pipe separated)", key="m_sk",
                              placeholder="Python | JavaScript | AWS | SQL | Leadership | Project Management",
                              help="Comma or pipe separated list of technical & soft skills")
    with c16:
        work_mode = st.selectbox("Work Mode Preference", 
                                options=["", "Remote", "Hybrid", "On-site"],
                                key="m_wm",
                                help="Preferred work arrangement")

    # ── SECTION 5: PROFILE & LINKS ────────────────────────────────────────────
    st.markdown("### 🔗 Profile & Links")
    profile_url = st.text_input("LinkedIn Profile URL", key="m_li",
                               placeholder="https://www.linkedin.com/in/yourprofile/",
                               help="Full LinkedIn profile URL")

    # ── SECTION 6: PROFILE SUMMARY ────────────────────────────────────────────
    st.markdown("### 📝 Professional Summary")
    headline = st.text_area("Profile Summary / Headline", key="m_hl",
                           placeholder="B2B Growth & GTM | 300M+ in Tech Product Revenues | 12+ Years Scaling Tech Startups in Video AdTech, Crypto, PropTech, FinTech etc",
                           height=100,
                           help="2-3 sentence professional summary visible on LinkedIn")

    # ── SECTION 7: SOURCE & REMARKS ───────────────────────────────────────────
    st.markdown("### 📍 Source & Internal Notes")
    c17, c18 = st.columns(2)
    with c17:
        source = st.text_input("Source / Reference", key="m_src",
                              value="manual",
                              placeholder="manual / referral / job_board / etc",
                              help="How did this candidate come to us?")
    with c18:
        st.write("")  # Spacer for alignment
        st.write("")

    remarks = st.text_area("Internal Remarks (Optional)", key="m_remarks",
                          placeholder="Any special notes, recommendations, or follow-up actions...",
                          height=80,
                          help="For internal team use only")

    # ─────────────────────────────────────────────────────────────────────────
    # FORM ACTIONS
    # ─────────────────────────────────────────────────────────────────────────

    col_submit, col_clear = st.columns([2, 1])
    
    with col_submit:
        submit_btn = st.button("➕ Add Candidate", type="primary", 
                              use_container_width=True, key="manual_submit")
    
    with col_clear:
        if st.button("🔄 Clear Form", use_container_width=True, key="manual_clear"):
            # Clear all form fields from session state
            for key in [
                "m_fn", "m_ln", "m_em", "m_ph", "m_lo", "m_pc",
                "m_ti", "m_cc", "m_cp", "m_sd", "m_ex", "m_np",
                "m_ed", "m_ei", "m_sk", "m_wm", "m_li", "m_src",
                "m_hl", "m_remarks"
            ]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

    if not submit_btn:
        return

    # ─────────────────────────────────────────────────────────────────────────
    # VALIDATION
    # ─────────────────────────────────────────────────────────────────────────

    # Email is required and must be valid
    if not email.strip():
        st.error("❌ **Email Address is required** (unique identifier)")
        return

    if not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email.strip()):
        st.error("❌ Invalid email format (e.g., user@example.com)")
        return

    # Warn if both first & last names missing
    if not first_name.strip() and not last_name.strip():
        st.warning("⚠️ At least **First Name** or **Last Name** is recommended")

    # Validate date format if provided
    if start_date.strip():
        try:
            datetime.strptime(start_date.strip(), "%Y-%m-%d")
        except ValueError:
            st.error("❌ Position Start date must be in **YYYY-MM-DD** format (e.g., 2023-01-15)")
            return

    # Get connection string
    conn_str = _get_conn_str(services)
    if not conn_str:
        st.error(
            "❌ No PostgreSQL connection string configured.\n\n"
            f"Tried: `{', '.join(_CONN_STR_KEYS)}`\n\n"
            "Add `PG_CONN_STRING=postgresql://...` to `.env`"
        )
        return

    # ─────────────────────────────────────────────────────────────────────────
    # BUILD ROW TUPLE (matching DB_COLS order)
    # ─────────────────────────────────────────────────────────────────────────

    fn = first_name.strip()
    ln = last_name.strip()
    candidate_name = f"{fn} {ln}".strip() or email.strip()

    # Parse skills into JSON array
    skills_json = None
    if skills.strip():
        parts = [p.strip() for p in re.split(r"[,|]", skills) if p.strip()]
        if parts:
            skills_json = json.dumps(parts)

    # Parse experience as integer
    exp_int = int(experience) if experience else None

    # Normalize work mode preference (empty string → None)
    work_mode_clean = work_mode if work_mode and work_mode.strip() else None

    # Normalize source
    source_clean = source.strip() or "manual"

    # Build row as tuple in exact DB_COLS order
    row = tuple(
        {
            "email_address":               email.strip().lower(),
            "candidate_name":              candidate_name,
            "first_name":                  fn or None,
            "last_name":                   ln or None,
            "phone_number":                phone.strip() or None,
            "location":                    location.strip() or None,
            "pin_code":                    pin_code.strip() or None,
            "profile_summary":             headline.strip() or None,
            "title":                       title.strip() or None,
            "current_company":             company.strip() or None,
            "current_position":            position.strip() or None,
            "current_position_start_date": start_date.strip() or None,
            "education_degree":            edu_degree.strip() or None,
            "education_institution":       edu_inst.strip() or None,
            "linkedin_profile":            profile_url.strip() or None,
            "total_experience":            exp_int,
            "skills":                      skills_json,
            "notice_period":               notice_period.strip() or None,
            "work_mode_pref":              work_mode_clean,
            "source":                      source_clean,
        }[col]
        for col in DB_COLS
    )

    # ─────────────────────────────────────────────────────────────────────────
    # INSERT INTO DATABASE
    # ─────────────────────────────────────────────────────────────────────────

    with st.spinner("💾 Saving candidate to database…"):
        result = _insert_chunk([row], conn_str)

    if result["error"]:
        st.error(f"❌ **Failed to save**: {result['error']}")
        _diagnose_error(result["error"])
    else:
        action = "✅ **Inserted (new)**" if result["inserted"] else "🔄 **Updated (existing)**"
        st.success(
            f"{action}\n\n"
            f"📋 **{candidate_name}**\n"
            f"📧 `{email.strip()}`"
        )
        st.balloons()
        time.sleep(2)
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# DEBUG TAB
# ─────────────────────────────────────────────────────────────────────────────

def _render_debug_tab(services: dict) -> None:
    st.subheader("🔬 Debug Tools")

    conn_str = _get_conn_str(services)
    db       = services.get("db")

    # ── Show resolved connection info ─────────────────────────────────────────
    st.markdown("**Connection resolver**")
    if conn_str:
        masked = re.sub(r":(.*?)@", ":***@", conn_str)
        st.success(f"✅ Resolved: `{masked}`")
    else:
        st.error(
            f"❌ Not found. Tried: `{', '.join(_CONN_STR_KEYS)}`\n\n"
            "Add `PG_CONN_STRING=postgresql://...` to `.env` or `secrets.toml`"
        )

    st.divider()

    # ── Live row count ────────────────────────────────────────────────────────
    st.markdown("**Live table stats**")
    if st.button("🔄 Refresh stats"):
        if conn_str:
            try:
                with psycopg2.connect(conn_str, connect_timeout=5) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT
                                COUNT(*)                           AS total_rows,
                                COUNT(email_address)               AS with_email,
                                MAX(updated_at)::text              AS last_update,
                                MIN(created_at)::text              AS first_insert
                            FROM candidates
                        """)
                        row = cur.fetchone()
                st.dataframe(
                    pd.DataFrame(
                        [{"total_rows":   row[0],
                          "with_email":   row[1],
                          "last_update":  row[2],
                          "first_insert": row[3]}]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
            except Exception as exc:
                st.error(f"❌ {exc}")
        else:
            st.error("No connection string — cannot query.")

    st.divider()

    # ── Single probe insert ───────────────────────────────────────────────────
    st.markdown("**Single-row probe insert**")
    probe_email = st.text_input(
        "Probe email", value="probe.test@recruitiq.internal", key="probe_email"
    )
    if st.button("🧪 Run probe"):
        if not conn_str:
            st.error("No connection string — cannot probe.")
        else:
            probe_row = tuple(
                {
                    "email_address":               probe_email,
                    "candidate_name":              "Probe User",
                    "first_name":                  "Probe",
                    "last_name":                   "User",
                    "phone_number":                None,
                    "location":                    None,
                    "pin_code":                    None,
                    "profile_summary":             None,
                    "title":                       None,
                    "current_company":             None,
                    "current_position":            None,
                    "current_position_start_date": None,
                    "education_degree":            None,
                    "education_institution":       None,
                    "linkedin_profile":            None,
                    "total_experience":            None,
                    "skills":                      None,
                    "notice_period":               None,
                    "work_mode_pref":              None,
                    "source":                      "probe",
                }[col]
                for col in DB_COLS
            )
            result = _insert_chunk([probe_row], conn_str)
            if result["error"]:
                st.error(f"❌ Probe failed: {result['error']}")
                _diagnose_error(result["error"])
            else:
                action = "inserted" if result["inserted"] else "updated"
                st.success(f"✅ Probe {action} successfully")
                # Clean up probe row
                try:
                    with psycopg2.connect(conn_str, connect_timeout=5) as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "DELETE FROM candidates WHERE email_address = %s",
                                (probe_email,),
                            )
                        conn.commit()
                    st.info("🧹 Probe row deleted")
                except Exception as e:
                    st.warning(f"Could not delete probe: {e}")

    st.divider()

    # ── Raw table viewer ──────────────────────────────────────────────────────
    st.markdown("**Raw table contents**")
    n_rows = st.number_input("Rows to fetch", min_value=1, max_value=500, value=20)
    if st.button("📋 Fetch rows"):
        if conn_str:
            try:
                with psycopg2.connect(conn_str, connect_timeout=5) as conn:
                    df_live = pd.read_sql(
                        f"SELECT * FROM candidates ORDER BY created_at DESC LIMIT {n_rows}",
                        conn,
                    )
                st.dataframe(df_live, use_container_width=True, hide_index=True)
            except Exception as exc:
                st.error(f"❌ {exc}")
        else:
            st.error("No connection string — cannot fetch.")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def render_upload_page(services: dict) -> None:
    st.title("⬆️ Upload Candidates")

    _render_db_diagnostics(services)
    st.divider()

    tab_import, tab_manual, tab_debug = st.tabs([
        "📊 Excel / CSV Import",
        "✏️ Manual Entry",
        "🔬 Debug",
    ])

    with tab_import:
        _render_import_tab(services)

    with tab_manual:
        _render_manual_tab(services)

    with tab_debug:
        _render_debug_tab(services)
