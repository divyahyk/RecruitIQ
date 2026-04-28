#!/usr/bin/env python3
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
"""
RecruitIQ – Backfill: skills + total_experience
Optimised for large datasets (300K+ rows)
  • Targets is_active = false (bulk import state)
  • READ connection  → server-side streaming cursor (never committed)
  • WRITE connection → batch commits every N rows
  • Resume from last processed legacy_id
  • Progress bar with ETA
"""

import os
import sys
import re
import time
import argparse
import json
from datetime import date
from pathlib import Path

# ── Path bootstrap ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
DATABASE_DIR = PROJECT_ROOT / "database"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DATABASE_DIR))

# ── .env loader (UTF-8 BOM safe) ─────────────────────────────────────────────
def _load_env(env_path: Path) -> int:
    if not env_path.exists():
        return 0
    count = 0
    with open(env_path, "r", encoding="utf-8-sig") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if not key or not key.replace("_", "").isalnum():
                continue
            os.environ.setdefault(key, val)
            count += 1
    return count

_loaded = _load_env(PROJECT_ROOT / ".env")

# ── DB connection string ──────────────────────────────────────────────────────
try:
    from connection import PG_CONN_STRING
except ImportError:
    PG_CONN_STRING = os.environ.get("PG_CONN_STRING", "")

if not PG_CONN_STRING:
    sys.exit("❌  PG_CONN_STRING not found. Check .env or connection.py")

import psycopg2
import psycopg2.extras

print(f"📁  PROJECT_ROOT : {PROJECT_ROOT}")
print(f"📁  DATABASE_DIR : {DATABASE_DIR}")
print(f"✅  Loaded {_loaded} var(s) from .env")
print(f"✅  PG_CONN_STRING : postgresql://***@***")

# ── LLM (optional) ───────────────────────────────────────────────────────────
LLM_AVAILABLE = False
try:
    from llm_client import extract_skills_llm
    LLM_AVAILABLE = True
    print("✅  LLM client loaded")
except ImportError:
    print("⚠️   LLM unavailable — keyword fallback active")

# ═════════════════════════════════════════════════════════════════════════════
# KEYWORD SKILL BANK
# ═════════════════════════════════════════════════════════════════════════════
KEYWORD_SKILLS: dict[str, list[str]] = {
    "programming":  ["Python", "Java", "JavaScript", "TypeScript", "C++", "C#",
                     "Go", "Rust", "Ruby", "PHP", "Kotlin", "Swift", "Scala"],
    "web":          ["React", "Angular", "Vue", "Node.js", "Django", "Flask",
                     "FastAPI", "Spring", "HTML", "CSS", "REST", "GraphQL"],
    "data":         ["SQL", "PostgreSQL", "MySQL", "MongoDB", "Redis",
                     "Pandas", "NumPy", "Spark", "Hadoop", "dbt"],
    "cloud":        ["AWS", "Azure", "GCP", "Docker", "Kubernetes",
                     "Terraform", "CI/CD", "Jenkins", "GitHub Actions"],
    "ml":           ["Machine Learning", "Deep Learning", "TensorFlow",
                     "PyTorch", "Scikit-learn", "NLP", "Computer Vision"],
    "finance":      ["Financial Modelling", "Excel", "Bloomberg", "Valuation",
                     "Equity Research", "Risk Management", "VBA"],
    "accounting":   ["Tally", "SAP", "QuickBooks", "IFRS", "GAAP",
                     "Taxation", "Auditing", "Accounts Payable"],
    "hr":           ["Recruitment", "Talent Acquisition", "HRIS", "Payroll",
                     "Employee Relations", "Performance Management"],
    "marketing":    ["SEO", "SEM", "Google Analytics", "Social Media",
                     "Content Marketing", "Email Marketing", "HubSpot"],
    "operations":   ["Supply Chain", "Logistics", "ERP", "Six Sigma",
                     "Lean", "Project Management", "Agile", "Scrum"],
}

ALL_KEYWORDS: list[str] = [kw for grp in KEYWORD_SKILLS.values() for kw in grp]


def extract_skills_keyword(text: str) -> list[str]:
    if not text:
        return []
    text_lower = text.lower()
    found: list[str] = []
    for skill in ALL_KEYWORDS:
        if re.search(r"\b" + re.escape(skill.lower()) + r"\b", text_lower):
            found.append(skill)
        if len(found) >= 10:
            break
    return found


def extract_skills(text: str) -> list[str]:
    if LLM_AVAILABLE:
        try:
            return extract_skills_llm(text)[:10]
        except Exception:
            pass
    return extract_skills_keyword(text)


# ═════════════════════════════════════════════════════════════════════════════
# EXPERIENCE INFERENCE  — always returns int (whole years)
# ═════════════════════════════════════════════════════════════════════════════
FRESHER_TOKENS = {
    "fresher", "fresh graduate", "entry level", "entry-level",
    "no experience", "0 years", "zero experience", "trainee", "intern",
    "student", "graduate", "just graduated",
}

SENIORITY_MAP: list[tuple[list[str], int]] = [
    (["cto", "ceo", "cfo", "vp ", "vice president",
      "director", "head of", "principal"],  12),
    (["lead", "senior", "sr.", "sr ", "architect",
      "manager", "specialist"],              7),
    (["mid", "associate", "analyst", "consultant",
      "executive", "officer"],               3),
    (["junior", "jr.", "jr ", "assistant",
      "trainee", "intern", "fresher"],       1),
]


def _years_from_start_date(start_date) -> int | None:
    if not start_date:
        return None
    try:
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        return max(0, int((date.today() - start_date).days / 365.25))
    except Exception:
        return None


def _is_fresher(text: str) -> bool:
    t = text.lower()
    return any(token in t for token in FRESHER_TOKENS)


def infer_experience(title: str, start_date, summary: str) -> int:
    """Return total_experience as integer years (DB column type = integer)."""
    text = f"{title or ''} {summary or ''}".strip()

    m = re.search(r"(\d+(?:\.\d+)?)\s*\+?\s*years?\s*(of\s+)?experience",
                  text, re.I)
    if m:
        return max(0, int(float(m.group(1))))

    if _is_fresher(text):
        return 0

    years = _years_from_start_date(start_date)
    if years is not None:
        return years

    t_lower = (title or "").lower()
    for keywords, estimate in SENIORITY_MAP:
        if any(kw in t_lower for kw in keywords):
            return estimate

    return 2


# ═════════════════════════════════════════════════════════════════════════════
# RESUME CHECKPOINT
# ═════════════════════════════════════════════════════════════════════════════
RESUME_FILE = PROJECT_ROOT / ".backfill_resume"


def load_resume_id() -> int:
    if RESUME_FILE.exists():
        try:
            return int(RESUME_FILE.read_text().strip())
        except Exception:
            pass
    return 0


def save_resume_id(legacy_id: int) -> None:
    RESUME_FILE.write_text(str(legacy_id))


def clear_resume() -> None:
    if RESUME_FILE.exists():
        RESUME_FILE.unlink()


# ═════════════════════════════════════════════════════════════════════════════
# PROGRESS BAR
# ═════════════════════════════════════════════════════════════════════════════
def progress_bar(current: int, total: int, start_time: float,
                 bar_width: int = 40) -> str:
    pct     = current / total if total else 0
    filled  = int(bar_width * pct)
    bar     = "█" * filled + "░" * (bar_width - filled)
    elapsed = time.time() - start_time
    eta_str = ""
    if current > 0 and current < total:
        eta_sec = (elapsed / current) * (total - current)
        eta_str = f"  ETA {int(eta_sec // 60):02d}m {int(eta_sec % 60):02d}s"
    rate = current / elapsed if elapsed > 0 else 0
    return (f"\r[{bar}] {current:,}/{total:,} "
            f"({pct:.1%})  {rate:.0f} rows/s{eta_str}  ")


# ═════════════════════════════════════════════════════════════════════════════
# BATCH COMMIT  (uses its own dedicated write connection)
# ═════════════════════════════════════════════════════════════════════════════
def _commit_batch(write_cur, write_conn, batch_data: list) -> None:
    """
    batch_data: list of (skills_json_str, experience_int, uuid_str)
    Uses the WRITE connection — never touches the READ connection.
    """
    psycopg2.extras.execute_batch(
        write_cur,
        """
        UPDATE candidates
        SET
            skills           = %s::jsonb,
            total_experience = %s,
            updated_at       = NOW()
        WHERE id = %s
        """,
        batch_data,
        page_size=500,
    )
    write_conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
def main() -> None:
    parser = argparse.ArgumentParser(
        description="RecruitIQ — backfill skills + total_experience"
    )
    parser.add_argument("--live",       action="store_true",
                        help="Write to DB (default: dry run)")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Rows per commit (default: 500)")
    parser.add_argument("--resume",     action="store_true",
                        help="Resume from last checkpoint")
    parser.add_argument("--reset",      action="store_true",
                        help="Clear resume checkpoint and start fresh")
    parser.add_argument("--limit",      type=int, default=0,
                        help="Process at most N rows (0 = all)")
    args = parser.parse_args()

    BATCH_SIZE = args.batch_size
    LIVE       = args.live
    mode_label = "LIVE" if LIVE else "DRY RUN"

    if args.reset:
        clear_resume()
        print("🔄  Resume checkpoint cleared.")

    resume_from = load_resume_id() if args.resume else 0
    if resume_from:
        print(f"▶️   Resuming from legacy_id > {resume_from:,}")

    print()
    print("=" * 60)
    print(f"  RecruitIQ – Backfill: skills + total_experience")
    print(f"  Mode        : {mode_label}")
    print(f"  Target      : is_active = false  (bulk import rows)")
    print(f"  Batch size  : {BATCH_SIZE:,}")
    print(f"  Resume ID   : {resume_from:,}")
    print("=" * 60)
    print()

    # ── Two separate connections ──────────────────────────────────────────────
    #   read_conn  → autocommit=True  → keeps named cursor alive forever
    #   write_conn → autocommit=False → commits every BATCH_SIZE rows
    # ─────────────────────────────────────────────────────────────────────────
    read_conn  = psycopg2.connect(PG_CONN_STRING)
    read_conn.autocommit = True                        # ← keeps portal open

    write_conn = psycopg2.connect(PG_CONN_STRING)
    write_conn.autocommit = False

    meta_cur   = read_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    write_cur  = write_conn.cursor()

    print("✅  Database connected  (read + write connections)")

    # ── Column type sanity check ──────────────────────────────────────────────
    meta_cur.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_name  = 'candidates'
          AND column_name = 'total_experience'
        """
    )
    col = meta_cur.fetchone()
    print(f"ℹ️   total_experience column type : {col['data_type'] if col else 'unknown'}")

    # ── Count rows to process ─────────────────────────────────────────────────
    meta_cur.execute(
        """
        SELECT COUNT(*) AS n
        FROM candidates
        WHERE is_active = false
          AND (skills IS NULL OR skills = '[]'::jsonb OR total_experience IS NULL)
          AND legacy_id > %s
        """,
        (resume_from,)
    )
    total_rows = meta_cur.fetchone()["n"]
    if args.limit > 0:
        total_rows = min(total_rows, args.limit)

    print(f"📋  Candidates to process : {total_rows:,}")
    print()

    if total_rows == 0:
        print("✅  Nothing to do — all candidates already enriched.")
        read_conn.close()
        write_conn.close()
        return

    if not LIVE:
        print("⚠️   DRY RUN — no DB writes. Use --live to commit.\n")

    # ── Server-side streaming cursor (on read_conn, autocommit=True) ──────────
    FETCH_SIZE = BATCH_SIZE * 4

    stream_cur = read_conn.cursor(
        name="backfill_cursor",
        cursor_factory=psycopg2.extras.RealDictCursor,
        withhold=True,           # survives across transactions (belt + braces)
    )
    stream_cur.execute(
        """
        SELECT
            id,
            legacy_id,
            title,
            current_position_start_date,
            profile_summary
        FROM candidates
        WHERE is_active = false
          AND (skills IS NULL OR skills = '[]'::jsonb OR total_experience IS NULL)
          AND legacy_id > %s
        ORDER BY legacy_id
        """,
        (resume_from,)
    )

    # ── Processing loop ───────────────────────────────────────────────────────
    processed   = 0
    batch_data: list = []
    start_time  = time.time()
    last_id     = resume_from
    total_limit = args.limit if args.limit > 0 else float("inf")

    while True:
        rows = stream_cur.fetchmany(FETCH_SIZE)
        if not rows:
            break

        for row in rows:
            if processed >= total_limit:
                break

            text_blob = " ".join(filter(None, [
                row["title"],
                row["profile_summary"],
            ]))

            skills     = extract_skills(text_blob)
            experience = infer_experience(
                row["title"],
                row["current_position_start_date"],
                row["profile_summary"],
            )

            batch_data.append((
                json.dumps(skills),   # → jsonb
                experience,           # → integer
                str(row["id"]),       # → uuid
            ))

            last_id   = row["legacy_id"]
            processed += 1

            print(progress_bar(processed, total_rows, start_time),
                  end="", flush=True)

            if len(batch_data) >= BATCH_SIZE:
                if LIVE:
                    _commit_batch(write_cur, write_conn, batch_data)
                    save_resume_id(last_id)
                batch_data = []

        if processed >= total_limit:
            break

    # ── Flush final partial batch ─────────────────────────────────────────────
    if batch_data and LIVE:
        _commit_batch(write_cur, write_conn, batch_data)
        save_resume_id(last_id)

    stream_cur.close()
    read_conn.close()
    write_cur.close()
    write_conn.close()

    elapsed = time.time() - start_time
    print()
    print()
    print("=" * 60)
    print(f"  ✅  Backfill complete!")
    print(f"  Rows processed : {processed:,}")
    print(f"  Time elapsed   : {elapsed / 60:.1f} min  ({elapsed:.1f}s)")
    print(f"  Avg speed      : {processed / elapsed:.0f} rows/s")
    if LIVE:
        print(f"  DB writes      : committed ✅")
        clear_resume()
    else:
        print(f"  DB writes      : skipped (dry run)")
    print("=" * 60)

    # ── Dry-run sample ────────────────────────────────────────────────────────
    if not LIVE and processed > 0:
        print("\n📊  Sample output (first 5 rows preview):\n")
        preview_conn = psycopg2.connect(PG_CONN_STRING)
        preview_cur  = preview_conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        preview_cur.execute(
            """
            SELECT id, legacy_id, title, current_position_start_date, profile_summary
            FROM candidates
            WHERE is_active = false
              AND (skills IS NULL OR skills = '[]'::jsonb OR total_experience IS NULL)
              AND legacy_id > %s
            ORDER BY legacy_id
            LIMIT 5
            """,
            (resume_from,)
        )
        for r in preview_cur.fetchall():
            text = " ".join(filter(None, [r["title"], r["profile_summary"]]))
            s = extract_skills(text)
            e = infer_experience(r["title"],
                                 r["current_position_start_date"],
                                 r["profile_summary"])
            title_d = (r["title"] or "N/A")[:35]
            print(f"  [{r['legacy_id']:>7}]  {title_d:<35}  "
                  f"exp={e:<4} yrs  skills={s}")
        preview_cur.close()
        preview_conn.close()
        print()
        print("👆  Re-run with --live to commit these changes.")


if __name__ == "__main__":
    main()
