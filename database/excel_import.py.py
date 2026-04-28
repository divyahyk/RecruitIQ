# database/excel_import.py

import io
import re
import time
import psycopg2
import pandas as pd
from typing import Dict

# ─────────────────────────────────────────────────────────────
#  COLUMN MAPPING  (Excel header → DB column)
# ─────────────────────────────────────────────────────────────
COLUMN_MAP: Dict[str, str] = {
    # Name
    "first name":                   "first_name",
    "firstname":                    "first_name",
    "last name":                    "last_name",
    "lastname":                     "last_name",
    "candidate name":               "candidate_name",
    "full name":                    "candidate_name",

    # Contact
    "email address":                "email_address",
    "email":                        "email_address",
    "phone number":                 "phone_number",
    "phone":                        "phone_number",
    "mobile":                       "phone_number",

    # Location
    "general location":             "location",
    "location":                     "location",
    "city":                         "location",
    "zip code":                     "pin_code",
    "zipcode":                      "pin_code",
    "pin code":                     "pin_code",
    "pincode":                      "pin_code",

    # Profile
    "headline":                     "profile_summary",
    "summary":                      "profile_summary",
    "profile summary":              "profile_summary",

    # Role
    "current title":                "title",
    "job title":                    "title",
    "title":                        "title",
    "current company":              "current_company",
    "company":                      "current_company",
    "current position":             "current_position",
    "position":                     "current_position",
    "current position start date":  "current_position_start_date",
    "start date":                   "current_position_start_date",

    # Education
    "education degree":             "education_degree",
    "degree":                       "education_degree",
    "education institution":        "education_institution",
    "institution":                  "education_institution",
    "school":                       "education_institution",
    "university":                   "education_institution",

    # LinkedIn
    "profile url":                  "linkedin_profile",
    "linkedin":                     "linkedin_profile",
    "linkedin url":                 "linkedin_profile",
    "linkedin profile":             "linkedin_profile",
}

# Target DB columns (must match candidates table exactly)
DB_COLUMNS: list = [
    "first_name",
    "last_name",
    "candidate_name",
    "email_address",
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
]

# Phone cleaning regex
_PHONE_RE = re.compile(r"[^\d\+\-\(\)\s]")


# ─────────────────────────────────────────────────────────────
#  STEP 1 — Load Excel
# ─────────────────────────────────────────────────────────────
def load_excel(filepath: str) -> pd.DataFrame:
    """Read Excel file into a DataFrame using openpyxl engine."""
    df = pd.read_excel(filepath, engine="openpyxl", dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    return df


# ─────────────────────────────────────────────────────────────
#  STEP 2 — Clean & normalise
# ─────────────────────────────────────────────────────────────
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Map Excel headers → DB columns
    2. Drop unmapped columns
    3. Coerce types
    4. Build candidate_name if missing
    5. Deduplicate by email_address
    """

    # ── Map columns ───────────────────────────────────────────
    rename = {}
    for col in df.columns:
        mapped = COLUMN_MAP.get(col.strip().lower())
        if mapped:
            rename[col] = mapped
    df = df.rename(columns=rename)

    # ── Keep only DB target columns ───────────────────────────
    present = [c for c in DB_COLUMNS if c in df.columns]
    df = df[present].copy()

    # Add any missing target columns as empty strings
    for col in DB_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[DB_COLUMNS]

    # ── Type coercions ────────────────────────────────────────

    # email → lowercase, strip
    if "email_address" in df.columns:
        df["email_address"] = (
            df["email_address"]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace("nan", None)
            .replace("", None)
        )

    # phone → digits/symbols only
    if "phone_number" in df.columns:
        df["phone_number"] = (
            df["phone_number"]
            .astype(str)
            .apply(lambda x: _PHONE_RE.sub("", x).strip()
                   if x not in ("nan", "", "None") else None)
        )

    # date column
    if "current_position_start_date" in df.columns:
        df["current_position_start_date"] = pd.to_datetime(
            df["current_position_start_date"], errors="coerce"
        ).dt.date.astype(object).where(
            df["current_position_start_date"].notna(), None
        )

    # All other text columns → strip / replace nan with None
    text_cols = [
        c for c in DB_COLUMNS
        if c not in ("email_address", "phone_number",
                     "current_position_start_date")
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace("nan", None)
                .replace("None", None)
                .replace("", None)
            )

    # ── Build candidate_name if blank ─────────────────────────
    mask = df["candidate_name"].isna()
    if mask.any():
        df.loc[mask, "candidate_name"] = (
            df.loc[mask, "first_name"].fillna("").str.strip()
            + " "
            + df.loc[mask, "last_name"].fillna("").str.strip()
        ).str.strip().replace("", None)

    # ── Drop rows with no email ───────────────────────────────
    df = df[df["email_address"].notna()].copy()

    # ── Deduplicate within the file by email ──────────────────
    df = df.drop_duplicates(subset=["email_address"], keep="first")

    df = df.reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────
#  STEP 3 — Stream CSV buffer
# ─────────────────────────────────────────────────────────────
def df_to_csv_buffer(df: pd.DataFrame) -> io.StringIO:
    """Serialise DataFrame to an in-memory CSV buffer for COPY."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=True, na_rep="")
    buf.seek(0)
    return buf


# ─────────────────────────────────────────────────────────────
#  STEP 4 — COPY into staging → upsert into candidates
# ─────────────────────────────────────────────────────────────
def copy_to_postgres(
    df: pd.DataFrame,
    conn: "psycopg2.extensions.connection",
    target_table: str = "candidates",
) -> Dict:
    """
    1. CREATE TEMP staging table
    2. COPY CSV → staging   (fast)
    3. INSERT INTO candidates SELECT FROM staging
       ON CONFLICT (email_address) DO NOTHING
    4. Returns {"inserted": N, "skipped": N}
    """
    staging = f"_staging_{target_table}"
    cols_ddl = ", ".join(
        f"{c} TEXT" if c != "current_position_start_date"
        else f"{c} DATE"
        for c in DB_COLUMNS
    )
    col_list = ", ".join(DB_COLUMNS)

    buf = df_to_csv_buffer(df)
    total_rows = len(df)

    with conn.cursor() as cur:
        # Create temp staging table
        cur.execute(f"""
            DROP TABLE IF EXISTS {staging};
            CREATE TEMP TABLE {staging} ({cols_ddl});
        """)

        # COPY CSV → staging
        cur.copy_expert(
            f"COPY {staging} ({col_list}) FROM STDIN WITH CSV HEADER NULL ''",
            buf,
        )

        # Upsert: staging → candidates
        cur.execute(f"""
            INSERT INTO {target_table} ({col_list})
            SELECT {col_list} FROM {staging}
            ON CONFLICT (email_address) DO NOTHING;
        """)
        inserted = cur.rowcount

        # Clean up
        cur.execute(f"DROP TABLE IF EXISTS {staging};")

    conn.commit()

    skipped = total_rows - inserted
    return {"inserted": inserted, "skipped": skipped}


# ─────────────────────────────────────────────────────────────
#  PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────
def import_excel_to_postgres(
    filepath: str,
    conn_string: str,
    table: str = "candidates",
) -> Dict:
    """
    Full pipeline:
      load_excel → clean_dataframe → copy_to_postgres
    Returns:
      {
        "inserted":     int,
        "skipped":      int,
        "total_rows":   int,
        "elapsed_sec":  float,
        "rows_per_sec": int,
      }
    """
    t0 = time.perf_counter()

    df = load_excel(filepath)
    df = clean_dataframe(df)
    total_rows = len(df)

    if total_rows == 0:
        return {
            "inserted": 0,
            "skipped": 0,
            "total_rows": 0,
            "elapsed_sec": 0.0,
            "rows_per_sec": 0,
        }

    conn = psycopg2.connect(conn_string)
    try:
        result = copy_to_postgres(df, conn, target_table=table)
    finally:
        conn.close()

    elapsed = round(time.perf_counter() - t0, 2)
    rows_per_sec = int(total_rows / elapsed) if elapsed > 0 else 0

    return {
        "inserted":     result["inserted"],
        "skipped":      result["skipped"],
        "total_rows":   total_rows,
        "elapsed_sec":  elapsed,
        "rows_per_sec": rows_per_sec,
    }
