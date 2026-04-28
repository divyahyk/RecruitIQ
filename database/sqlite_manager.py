# database/sqlite_manager.py

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Union, Set, Callable


# ─────────────────────────────────────────────────────────────
#  SCHEMA DDL
# ─────────────────────────────────────────────────────────────
SQLITE_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ── Candidates ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS candidates (
    id                   TEXT PRIMARY KEY,
    full_name            TEXT NOT NULL,
    email                TEXT,
    phone                TEXT,
    linkedin_url         TEXT,
    current_title        TEXT,
    current_company      TEXT,
    total_experience     REAL DEFAULT 0,
    skills               TEXT DEFAULT '[]',
    education            TEXT,
    current_location     TEXT,
    preferred_locations  TEXT DEFAULT '[]',
    work_mode_preference TEXT,
    current_ctc          TEXT,
    expected_ctc         TEXT,
    notice_period        TEXT,
    source               TEXT DEFAULT 'Manual',
    profile_hash         TEXT UNIQUE,
    ai_summary           TEXT,
    enriched_at          TEXT,
    created_at           TEXT DEFAULT (datetime('now')),
    updated_at           TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_candidates_hash
    ON candidates(profile_hash);
CREATE INDEX IF NOT EXISTS idx_candidates_location
    ON candidates(current_location);
CREATE INDEX IF NOT EXISTS idx_candidates_experience
    ON candidates(total_experience);
CREATE INDEX IF NOT EXISTS idx_candidates_name
    ON candidates(full_name);

-- ── Job Descriptions ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_descriptions (
    id                    TEXT PRIMARY KEY,
    jd_code               TEXT UNIQUE,
    role_name             TEXT NOT NULL,
    client_name           TEXT,
    skillset_required     TEXT DEFAULT '[]',
    skillset_good_to_have TEXT DEFAULT '[]',
    location              TEXT,
    work_mode             TEXT DEFAULT 'Hybrid',
    experience_min        REAL DEFAULT 0,
    experience_max        REAL DEFAULT 10,
    budget_min            REAL DEFAULT 0,
    budget_max            REAL DEFAULT 0,
    budget_currency       TEXT DEFAULT 'INR',
    notice_period_max     TEXT DEFAULT '60 days',
    education_required    TEXT,
    industry_preference   TEXT,
    positions_count       INTEGER DEFAULT 1,
    priority              TEXT DEFAULT 'Medium',
    status                TEXT DEFAULT 'Active',
    recruiter_assigned    TEXT,
    raw_jd_text           TEXT,
    ai_parsed_data        TEXT DEFAULT '{}',
    created_at            TEXT DEFAULT (datetime('now')),
    updated_at            TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jd_status
    ON job_descriptions(status);

-- ── Applications ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS applications (
    id                     TEXT PRIMARY KEY,
    candidate_id           TEXT REFERENCES candidates(id) ON DELETE CASCADE,
    jd_id                  TEXT REFERENCES job_descriptions(id) ON DELETE CASCADE,
    stage                  TEXT DEFAULT 'New',
    match_score            REAL DEFAULT 0,
    match_breakdown        TEXT DEFAULT '{}',
    interview_scheduled_at TEXT,
    interview_type         TEXT,
    interview_link         TEXT,
    interviewer_emails     TEXT DEFAULT '[]',
    offer_amount           REAL,
    rejection_reason       TEXT,
    notes                  TEXT,
    stage_history          TEXT DEFAULT '[]',
    created_at             TEXT DEFAULT (datetime('now')),
    updated_at             TEXT DEFAULT (datetime('now')),
    UNIQUE(candidate_id, jd_id)
);

CREATE INDEX IF NOT EXISTS idx_apps_stage
    ON applications(stage);
CREATE INDEX IF NOT EXISTS idx_apps_jd
    ON applications(jd_id);
CREATE INDEX IF NOT EXISTS idx_apps_candidate
    ON applications(candidate_id);
CREATE INDEX IF NOT EXISTS idx_apps_interview
    ON applications(interview_scheduled_at);
"""


class SQLiteManager:

    def __init__(self, db_path: str = "recruitiq_local.db"):
        self.db_path = db_path
        self._init_connection()

    def _init_connection(self):
        """Create connection and enable WAL + row factory."""
        self._raw_conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=30
        )
        self._raw_conn.row_factory = sqlite3.Row
        self._raw_conn.execute("PRAGMA journal_mode=WAL")
        self._raw_conn.execute("PRAGMA foreign_keys=ON")

    @contextmanager
    def _conn(self):
        """Yield connection; commit on success, rollback on error."""
        try:
            yield self._raw_conn
            self._raw_conn.commit()
        except Exception as e:
            self._raw_conn.rollback()
            raise e

    # ─────────────────────────────────────────────────────────
    #  SCHEMA
    # ─────────────────────────────────────────────────────────

    def initialize_schema(self):
        with self._conn() as conn:
            conn.executescript(SQLITE_SCHEMA_SQL)
        print(f"✅ SQLite schema initialized at {self.db_path}")

    # ─────────────────────────────────────────────────────────
    #  HELPERS
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _new_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _make_hash(record: dict) -> str:
        key = "|".join([
            str(record.get("email", "")).lower().strip(),
            str(record.get("full_name", "")).lower().strip(),
            str(record.get("phone", "")).strip(),
        ])
        return hashlib.md5(key.encode()).hexdigest()

    @staticmethod
    def _dumps(value) -> str:
        """Safely serialise list/dict → JSON string for TEXT columns."""
        if value is None:
            return "[]"
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    @staticmethod
    def _loads(value, default=None):
        """Safely deserialise JSON string → Python object."""
        if value is None:
            return default if default is not None else []
        if isinstance(value, (list, dict)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return default if default is not None else []

    def _row_to_dict(self, row) -> dict:
        """Convert sqlite3.Row → plain dict with JSON fields parsed."""
        if row is None:
            return {}
        d = dict(row)
        for field in (
            "skills", "preferred_locations", "skillset_required",
            "skillset_good_to_have", "match_breakdown", "stage_history",
            "interviewer_emails", "ai_parsed_data", "raw_data"
        ):
            if field in d:
                d[field] = self._loads(d[field], [])
        return d

    # ─────────────────────────────────────────────────────────
    #  CANDIDATES
    # ─────────────────────────────────────────────────────────

    def bulk_insert_candidates(
        self,
        records: List[dict],
        source: str = "Manual"
    ) -> Dict:
        """
        Insert candidates with MD5 deduplication.
        Returns {"inserted": N, "skipped": N, "errors": N}
        """
        stats = {"inserted": 0, "skipped": 0, "errors": 0}

        sql = """
            INSERT OR IGNORE INTO candidates (
                id, full_name, email, phone, linkedin_url,
                current_title, current_company, total_experience,
                skills, education, current_location, preferred_locations,
                work_mode_preference, current_ctc, expected_ctc,
                notice_period, source, profile_hash
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?
            )
        """

        with self._conn() as conn:
            for rec in records:
                try:
                    phash = self._make_hash(rec)

                    # Check if already exists
                    exists = conn.execute(
                        "SELECT id FROM candidates WHERE profile_hash=?",
                        (phash,)
                    ).fetchone()

                    if exists:
                        stats["skipped"] += 1
                        continue

                    conn.execute(sql, (
                        self._new_id(),
                        str(rec.get("full_name", ""))[:255],
                        str(rec.get("email", "")).lower()[:255],
                        str(rec.get("phone", ""))[:50],
                        str(rec.get("linkedin_url", ""))[:500],
                        str(rec.get("current_title", ""))[:255],
                        str(rec.get("current_company", ""))[:255],
                        float(rec.get("total_experience") or 0),
                        self._dumps(rec.get("skills", [])),
                        str(rec.get("education", ""))[:500],
                        str(rec.get("current_location", ""))[:255],
                        self._dumps(rec.get("preferred_locations", [])),
                        str(rec.get("work_mode_preference", ""))[:100],
                        str(rec.get("current_ctc", ""))[:100],
                        str(rec.get("expected_ctc", ""))[:100],
                        str(rec.get("notice_period", ""))[:100],
                        source,
                        phash,
                    ))
                    stats["inserted"] += 1

                except Exception as e:
                    stats["errors"] += 1
                    print(f"Insert error: {e}")

        return stats

    def search_candidates(
        self,
        query: str = "",
        skills: List[str] = None,
        location: str = "",
        experience_min: float = 0,
        experience_max: float = 50,
        work_mode: str = "",
        source: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[dict], int]:
        """
        Full-featured search.
        Returns (candidates_list, total_count)
        """
        conditions = []
        params     = []

        if query:
            conditions.append("""(
                full_name        LIKE ?
                OR current_title LIKE ?
                OR current_company LIKE ?
                OR email         LIKE ?
            )""")
            like = f"%{query}%"
            params.extend([like, like, like, like])

        if location:
            conditions.append("current_location LIKE ?")
            params.append(f"%{location}%")

        if experience_min is not None:
            conditions.append("total_experience >= ?")
            params.append(float(experience_min))

        if experience_max is not None and experience_max < 50:
            conditions.append("total_experience <= ?")
            params.append(float(experience_max))

        if work_mode:
            conditions.append("work_mode_preference LIKE ?")
            params.append(f"%{work_mode}%")

        if source:
            conditions.append("source = ?")
            params.append(source)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        count_sql = f"SELECT COUNT(*) as total FROM candidates {where}"
        data_sql  = f"""
            SELECT * FROM candidates
            {where}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """

        with self._conn() as conn:
            total = conn.execute(
                count_sql, params
            ).fetchone()["total"]

            rows = conn.execute(
                data_sql, params + [limit, offset]
            ).fetchall()

        candidates = [self._row_to_dict(r) for r in rows]

        # Client-side skills filter (SQLite can't do JSONB GIN)
        if skills:
            filtered = []
            for c in candidates:
                cand_skills_low = [
                    s.lower() for s in (c.get("skills") or [])
                ]
                if any(
                    any(req.lower() in cs for cs in cand_skills_low)
                    for req in skills
                ):
                    filtered.append(c)
            candidates = filtered
            total = len(filtered)

        return candidates, total

    def get_candidate_by_id(self, candidate_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM candidates WHERE id=?",
                (candidate_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def update_candidate(self, candidate_id: str, data: dict) -> bool:
        allowed = {
            "full_name", "email", "phone", "current_title",
            "current_company", "total_experience", "skills",
            "education", "current_location", "work_mode_preference",
            "current_ctc", "expected_ctc", "notice_period",
            "ai_summary", "enriched_at"
        }
        clean = {}
        for k, v in data.items():
            if k not in allowed:
                continue
            if isinstance(v, (list, dict)):
                clean[k] = self._dumps(v)
            else:
                clean[k] = v

        if not clean:
            return False

        clean["updated_at"] = datetime.utcnow().isoformat()
        sets   = ", ".join(f"{k}=?" for k in clean)
        values = list(clean.values()) + [candidate_id]

        try:
            with self._conn() as conn:
                conn.execute(
                    f"UPDATE candidates SET {sets} WHERE id=?", values
                )
            return True
        except Exception as e:
            print(f"Update error: {e}")
            return False

    # ─────────────────────────────────────────────────────────
    #  JOB DESCRIPTIONS
    # ─────────────────────────────────────────────────────────

    def _generate_jd_code(self, conn) -> str:
        month = datetime.utcnow().strftime("%Y%m")
        count_row = conn.execute(
            "SELECT COUNT(*) as n FROM job_descriptions"
        ).fetchone()
        n = (count_row["n"] if count_row else 0) + 1
        return f"RIQ-{month}-{n:03d}"

    def save_jd(self, jd_data: dict) -> str:
        """Insert or update JD. Returns id."""
        jd_id = jd_data.get("id")

        fields_map = {
            "jd_code":              jd_data.get("jd_code"),
            "role_name":            jd_data.get("role_name", "Unknown"),
            "client_name":          jd_data.get("client_name", ""),
            "skillset_required":    self._dumps(jd_data.get("skillset_required", [])),
            "skillset_good_to_have":self._dumps(jd_data.get("skillset_good_to_have", [])),
            "location":             jd_data.get("location", ""),
            "work_mode":            jd_data.get("work_mode", "Hybrid"),
            "experience_min":       float(jd_data.get("experience_min") or 0),
            "experience_max":       float(jd_data.get("experience_max") or 10),
            "budget_min":           float(jd_data.get("budget_min") or 0),
            "budget_max":           float(jd_data.get("budget_max") or 0),
            "budget_currency":      jd_data.get("budget_currency", "INR"),
            "notice_period_max":    jd_data.get("notice_period_max", "60 days"),
            "education_required":   jd_data.get("education_required", ""),
            "industry_preference":  jd_data.get("industry_preference", ""),
            "positions_count":      int(jd_data.get("positions_count") or 1),
            "priority":             jd_data.get("priority", "Medium"),
            "status":               jd_data.get("status", "Active"),
            "recruiter_assigned":   jd_data.get("recruiter_assigned", ""),
            "raw_jd_text":          jd_data.get("raw_jd_text", ""),
            "ai_parsed_data":       self._dumps(jd_data.get("ai_parsed_data", {})),
        }

        with self._conn() as conn:
            if jd_id:
                # Update
                fields_map["updated_at"] = datetime.utcnow().isoformat()
                sets = ", ".join(f"{k}=?" for k in fields_map)
                conn.execute(
                    f"UPDATE job_descriptions SET {sets} WHERE id=?",
                    list(fields_map.values()) + [jd_id]
                )
                return jd_id
            else:
                # Insert
                new_id = self._new_id()
                if not fields_map["jd_code"]:
                    fields_map["jd_code"] = self._generate_jd_code(conn)

                cols = ", ".join(["id"] + list(fields_map.keys()))
                plac = ", ".join(["?"] * (len(fields_map) + 1))
                conn.execute(
                    f"INSERT INTO job_descriptions ({cols}) VALUES ({plac})",
                    [new_id] + list(fields_map.values())
                )
                return new_id

    def get_all_jds(self, status: Optional[str] = None) -> List[dict]:
        sql  = "SELECT * FROM job_descriptions"
        params = []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY created_at DESC"

        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_jd_by_id(self, jd_id: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM job_descriptions WHERE id=?",
                (jd_id,)
            ).fetchone()
        return self._row_to_dict(row)

    # ─────────────────────────────────────────────────────────
    #  PIPELINE / APPLICATIONS
    # ─────────────────────────────────────────────────────────

    def add_to_pipeline(
        self,
        candidate_id: str,
        jd_id: str,
        match_score: float = 0,
        match_breakdown: dict = None
    ) -> Optional[str]:
        """Add or update pipeline entry. Returns application id."""
        history = [{
            "stage": "New",
            "at": datetime.utcnow().isoformat(),
            "by": "RecruitIQ"
        }]

        try:
            with self._conn() as conn:
                # Check existing
                existing = conn.execute(
                    "SELECT id FROM applications WHERE candidate_id=? AND jd_id=?",
                    (candidate_id, jd_id)
                ).fetchone()

                if existing:
                    conn.execute(
                        """UPDATE applications
                           SET match_score=?, match_breakdown=?, updated_at=?
                           WHERE candidate_id=? AND jd_id=?""",
                        (
                            round(match_score, 2),
                            self._dumps(match_breakdown or {}),
                            datetime.utcnow().isoformat(),
                            candidate_id, jd_id
                        )
                    )
                    return existing["id"]

                new_id = self._new_id()
                conn.execute(
                    """INSERT INTO applications
                       (id, candidate_id, jd_id, stage, match_score,
                        match_breakdown, stage_history)
                       VALUES (?, ?, ?, 'New', ?, ?, ?)""",
                    (
                        new_id, candidate_id, jd_id,
                        round(match_score, 2),
                        self._dumps(match_breakdown or {}),
                        self._dumps(history)
                    )
                )
                return new_id
        except Exception as e:
            print(f"Pipeline error: {e}")
            return None

    def update_application_stage(
        self,
        application_id: str,
        new_stage: str,
        notes: str = ""
    ) -> bool:
        try:
            with self._conn() as conn:
                # Get current history
                row = conn.execute(
                    "SELECT stage_history FROM applications WHERE id=?",
                    (application_id,)
                ).fetchone()

                history = self._loads(
                    (row or {}).get("stage_history"), []
                ) if row else []

                history.append({
                    "stage": new_stage,
                    "at": datetime.utcnow().isoformat(),
                    "by": "RecruitIQ"
                })

                conn.execute(
                    """UPDATE applications
                       SET stage=?, stage_history=?,
                           notes=?, updated_at=?
                       WHERE id=?""",
                    (
                        new_stage,
                        self._dumps(history),
                        notes or None,
                        datetime.utcnow().isoformat(),
                        application_id
                    )
                )
            return True
        except Exception as e:
            print(f"Stage update error: {e}")
            return False

    def get_pipeline_stats(
        self, jd_id: Optional[str] = None
    ) -> Dict[str, int]:
        """Returns {stage: count}"""
        from config import Config

        sql = "SELECT stage, COUNT(*) as cnt FROM applications"
        params = []
        if jd_id:
            sql += " WHERE jd_id=?"
            params.append(jd_id)
        sql += " GROUP BY stage"

        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()

        counts = {s: 0 for s in Config.PIPELINE_STAGES}
        for row in rows:
            stage = row["stage"]
            if stage in counts:
                counts[stage] = row["cnt"]
        return counts

    def get_applications_for_candidate(
        self, candidate_id: str
    ) -> List[dict]:
        sql = """
            SELECT a.*, j.role_name, j.jd_code, j.client_name
            FROM applications a
            JOIN job_descriptions j ON a.jd_id = j.id
            WHERE a.candidate_id = ?
            ORDER BY a.created_at DESC
        """
        with self._conn() as conn:
            rows = conn.execute(sql, (candidate_id,)).fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ─────────────────────────────────────────────────────────
    #  INTERVIEWS
    # ─────────────────────────────────────────────────────────

    def get_upcoming_interviews(self, days_ahead: int = 7) -> List[dict]:
        now  = datetime.utcnow().isoformat()
        then = (datetime.utcnow() + timedelta(days=days_ahead)).isoformat()

        sql = """
            SELECT
                a.id, a.interview_scheduled_at,
                a.interview_type, a.interview_link, a.stage,
                c.full_name, c.email, c.phone,
                j.role_name, j.jd_code
            FROM applications a
            JOIN candidates c ON a.candidate_id = c.id
            JOIN job_descriptions j ON a.jd_id = j.id
            WHERE a.interview_scheduled_at IS NOT NULL
              AND a.interview_scheduled_at >= ?
              AND a.interview_scheduled_at <= ?
            ORDER BY a.interview_scheduled_at ASC
        """
        with self._conn() as conn:
            rows = conn.execute(sql, (now, then)).fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ─────────────────────────────────────────────────────────
    #  DASHBOARD METRICS
    # ─────────────────────────────────────────────────────────

    def get_dashboard_metrics(self) -> Dict:
        metrics = {
            "total_candidates": 0,
            "active_jds": 0,
            "in_pipeline": 0,
            "interviews_this_week": 0,
            "joined_mtd": 0,
        }

        queries = {
            "total_candidates": (
                "SELECT COUNT(*) as n FROM candidates", []
            ),
            "active_jds": (
                "SELECT COUNT(*) as n FROM job_descriptions WHERE status='Active'", []
            ),
            "in_pipeline": (
                "SELECT COUNT(*) as n FROM applications "
                "WHERE stage NOT IN ('Joined','Rejected')", []
            ),
            "interviews_this_week": (
                """SELECT COUNT(*) as n FROM applications
                   WHERE interview_scheduled_at IS NOT NULL
                     AND interview_scheduled_at >= ?
                     AND interview_scheduled_at <= ?""",
                [
                    datetime.utcnow().isoformat(),
                    (datetime.utcnow() + timedelta(days=7)).isoformat()
                ]
            ),
            "joined_mtd": (
                """SELECT COUNT(*) as n FROM applications
                   WHERE stage='Joined'
                     AND updated_at >= ?""",
                [datetime.utcnow().replace(
                    day=1, hour=0, minute=0, second=0
                ).isoformat()]
            ),
        }

        with self._conn() as conn:
            for key, (sql, params) in queries.items():
                try:
                    row = conn.execute(sql, params).fetchone()
                    metrics[key] = row["n"] if row else 0
                except Exception:
                    pass

        return metrics
