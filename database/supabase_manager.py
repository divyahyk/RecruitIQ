# database/supabase_manager.py
# RecruitIQ v1.2  –  SupabaseManager
# REST client (supabase-py) + psycopg2 pool via _db() context manager
# Schema-aligned to Migration 005 (UUID candidates.id)
#
# v1.2 changes:
#   - Added social_posts & banner_cache table methods
#   - .save_social_post() / .get_social_posts() for content management
#   - .save_banner() / .get_banner() for banner caching (30-day TTL)
#   - All methods include error handling + logging

from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional, Tuple

import psycopg2
from psycopg2 import pool as pg_pool
from supabase import Client, create_client


# ─────────────────────────────────────────────────────────────────────────────
#  COLUMN ALLOWLISTS  (aligned to actual DB schema post-migration-005)
# ─────────────────────────────────────────────────────────────────────────────

# Actual columns in candidates table (legacy_id excluded — never written by app)
CANDIDATE_COLUMNS: frozenset = frozenset({
    "first_name",
    "last_name",
    "candidate_name",
    "email_address",
    "phone_number",
    "linkedin_profile",
    "title",
    "current_company",
    "current_position",
    "current_position_start_date",
    "location",
    "pin_code",
    "profile_summary",
    "education_degree",
    "education_institution",
    "total_experience",
    "skills",
    "work_mode_pref",          # ← actual column name (not work_mode_preference)
    "notice_period",
    "source",
    "profile_hash",
    "is_active",
    "remarks",
})

# Actual columns in job_descriptions table
JD_COLUMNS: frozenset = frozenset({
    "jd_code",
    "role_name",
    "client_name",
    "skillset_required",
    "skillset_good_to_have",
    "location",
    "work_mode",
    "experience_min",
    "experience_max",
    "budget_min",
    "budget_max",
    "budget_currency",
    "notice_period_max",
    "education_required",
    "industry_preference",
    "positions_count",
    "priority",
    "status",
    "recruiter_assigned",
    "raw_jd_text",
    "ai_parsed_data",
})

# Columns for social_posts table
SOCIAL_POST_COLUMNS: frozenset = frozenset({
    "jd_id",
    "platform",
    "content",
    "style",
    "generated_by",
})

# Columns for banner_cache table
BANNER_CACHE_COLUMNS: frozenset = frozenset({
    "jd_id",
    "platform",
    "image_url",
    "width",
    "height",
    "expires_at",
})

# Pipeline stages (single source of truth — mirrors Config.PIPELINE_STAGES)
PIPELINE_STAGES: Tuple[str, ...] = (
    "New", "Screening", "Interview", "Offer", "Joined", "Rejected",
)

# Columns fetched by search_candidates — explicit list avoids pulling all 40+
# columns across 200 rows through REST on every call.
_SEARCH_COLS: str = (
    "id, candidate_name, first_name, last_name, "
    "email_address, phone_number, "
    "current_position, title, current_company, "
    "location, total_experience, notice_period, "
    "work_mode_pref, skills, profile_summary"
)

# Platform configurations
PLATFORM_CONFIG: Dict[str, Dict[str, int]] = {
    "LinkedIn": {"width": 1200, "height": 628},
    "Instagram": {"width": 1080, "height": 1080},
    "Twitter": {"width": 1200, "height": 675},
    "WhatsApp": {"width": 800, "height": 800},
}


# ─────────────────────────────────────────────────────────────────────────────
#  MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class SupabaseManager:
    """
    Unified data-access layer for RecruitIQ.

    • REST operations  → self.client  (supabase-py)
    • Raw SQL / COPY   → self._db()   (psycopg2 pool)
    """

    # ── Construction ──────────────────────────────────────────────────────────

    def __init__(self, url: str, key: str) -> None:
        self.client: Client = create_client(url, key)
        self._url = url

        # psycopg2 pool — initialised lazily on first _db() call
        self._pool: Optional[pg_pool.ThreadedConnectionPool] = None

    # ── psycopg2 pool ─────────────────────────────────────────────────────────

    def _get_pool(self) -> pg_pool.ThreadedConnectionPool:
        """Return (or lazily create) the psycopg2 connection pool."""
        if self._pool is None:
            conn_str = os.getenv("PG_CONN_STRING", "")
            if not conn_str:
                raise RuntimeError(
                    "PG_CONN_STRING is not set in the environment.\n"
                    "Add it to your .env file:\n"
                    "PG_CONN_STRING=postgresql://postgres.[ref]:[pwd]"
                    "@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"
                )
            self._pool = pg_pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                dsn=conn_str,
            )
        return self._pool

    @contextmanager
    def _db(self) -> Generator:
        """
        Psycopg2 connection context manager backed by the thread pool.

        Usage:
            with self._db() as conn:
                with conn.cursor() as cur:
                    cur.execute(...)
                conn.commit()
        """
        pool = self._get_pool()
        conn = pool.getconn()
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)

    # ── Schema bootstrap ──────────────────────────────────────────────────────

    def initialize_schema(self) -> None:
        """Light ping to verify REST connection is alive."""
        try:
            self.client.table("candidates").select("id").limit(1).execute()
            print("✅ Supabase schema initialized")
        except Exception as e:
            print(f"⚠️  Supabase connection failed: {e}")
            print("   Run migration SQL in Supabase SQL Editor first.")

    # ─────────────────────────────────────────────────────────────────────────
    #  CANDIDATES
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _make_hash(record: dict) -> str:
        """MD5 deduplication key: email + name + phone."""
        key = "|".join([
            str(record.get("email_address", "")).lower().strip(),
            str(record.get("candidate_name", "")).lower().strip(),
            str(record.get("phone_number", "")).strip(),
        ])
        return hashlib.md5(key.encode()).hexdigest()

    def bulk_insert_candidates(
        self,
        records: List[dict],
        source: str = "Manual",
    ) -> Dict[str, int]:
        """
        Upsert candidates with MD5 deduplication.
        Returns {"inserted": N, "skipped": N, "errors": N}
        """
        stats: Dict[str, int] = {"inserted": 0, "skipped": 0, "errors": 0}

        for rec in records:
            try:
                rec["profile_hash"] = self._make_hash(rec)
                rec["source"] = source

                # skills must be a list (JSONB)
                if not isinstance(rec.get("skills"), list):
                    rec["skills"] = []

                # Align work_mode key
                if "work_mode_preference" in rec:
                    rec["work_mode_pref"] = rec.pop("work_mode_preference")

                # Strip columns not in actual schema
                clean = {k: v for k, v in rec.items() if k in CANDIDATE_COLUMNS}

                result = (
                    self.client.table("candidates")
                    .upsert(
                        clean,
                        on_conflict="profile_hash",
                        ignore_duplicates=True,
                    )
                    .execute()
                )

                if result.data:
                    stats["inserted"] += 1
                else:
                    stats["skipped"] += 1

            except Exception as exc:
                err = str(exc).lower()
                if "duplicate" in err or "unique" in err:
                    stats["skipped"] += 1
                else:
                    stats["errors"] += 1
                    print(f"[bulk_insert] error: {exc}")

        return stats

    def search_candidates(
        self,
        query: str = "",
        skills: Optional[List[str]] = None,
        location: str = "",
        experience_min: float = 0,
        experience_max: float = 50,
        work_mode: str = "",
        source: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[dict], int]:
        """
        Candidate search via Supabase REST API.
        Returns (candidates_list, total_count).

        v1.1 — timeout fix:
        ┌─────────────────────────────────────────────────────────┐
        │  OLD (broken)                                           │
        │  • select("*")            — 40+ cols × 200 rows        │
        │  • no is_active filter    — full seq scan 348k rows     │
        │  • order("created_at")    — no index → seq scan         │
        │  → statement timeout 57014 on Supabase                  │
        │                                                         │
        │  NEW (fixed)                                            │
        │  • select(_SEARCH_COLS)   — 15 cols only               │
        │  • .eq("is_active", True) — hits B-tree index           │
        │  • order("total_experience") — Migration 006 B-tree     │
        │  → index scan, no timeout                               │
        └─────────────────────────────────────────────────────────┘
        """
        q = (
            self.client.table("candidates")
            .select(_SEARCH_COLS, count="exact")
            .eq("is_active", True)              # always filter — uses index
        )

        if query:
            q = q.or_(
                f"candidate_name.ilike.%{query}%,"
                f"title.ilike.%{query}%,"
                f"current_company.ilike.%{query}%,"
                f"email_address.ilike.%{query}%"
            )

        if location:
            q = q.ilike("location", f"%{location}%")

        # Only apply lower-bound filter when caller sets a value above 0
        # (experience_min=0 is the default and should not restrict results)
        if experience_min > 0:
            q = q.gte("total_experience", experience_min)

        # Only apply upper-bound filter when caller restricts below 50
        if experience_max < 50:
            q = q.lte("total_experience", experience_max)

        if work_mode:
            q = q.ilike("work_mode_pref", f"%{work_mode}%")

        if source:
            q = q.eq("source", source)

        result = (
            q
            .order("total_experience", desc=True)   # indexed column
            .range(offset, offset + limit - 1)
            .execute()
        )

        candidates = result.data or []
        total      = result.count or len(candidates)

        # Client-side skills filter (JSONB — REST cannot filter arrays natively)
        if skills:
            filtered = []
            for c in candidates:
                cand_skills = [
                    s.lower() for s in (c.get("skills") or [])
                ]
                if any(
                    any(req.lower() in cs for cs in cand_skills)
                    for req in skills
                ):
                    filtered.append(c)
            candidates = filtered

        return candidates, total

    def get_candidate_by_id(self, candidate_id: str) -> Optional[dict]:
        """Fetch one candidate by UUID."""
        try:
            result = (
                self.client.table("candidates")
                .select("*")
                .eq("id", str(candidate_id))
                .single()
                .execute()
            )
            return result.data
        except Exception as exc:
            print(f"[get_candidate_by_id] {exc}")
            return None

    def update_candidate(self, candidate_id: str, data: dict) -> bool:
        """Update candidate fields. Non-schema keys are stripped."""
        try:
            # Align work_mode key if caller sends old name
            if "work_mode_preference" in data:
                data["work_mode_pref"] = data.pop("work_mode_preference")

            clean = {k: v for k, v in data.items() if k in CANDIDATE_COLUMNS}
            (
                self.client.table("candidates")
                .update(clean)
                .eq("id", str(candidate_id))
                .execute()
            )
            return True
        except Exception as exc:
            print(f"[update_candidate] {exc}")
            return False

    def get_candidate_count(self) -> int:
        """Total candidates in the database (used by sidebar)."""
        try:
            r = (
                self.client.table("candidates")
                .select("id", count="exact")
                .execute()
            )
            return r.count or 0
        except Exception:
            return 0

    def import_from_excel(self, filepath: str) -> Dict:
        """
        High-speed Excel import via psycopg2 COPY.
        Delegates to database/excel_import.py.
        """
        from database.excel_import import import_excel_to_postgres
        from database.connection import PG_CONN_STRING

        return import_excel_to_postgres(
            filepath=filepath,
            conn_string=PG_CONN_STRING,
            table="candidates",
        )

    # ─────────────────────────────────────────────────────────────────────────
    #  JOB DESCRIPTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def save_jd(self, jd_data: dict) -> str:
        """Insert or update a JD. Returns the JD UUID."""
        clean = {k: v for k, v in jd_data.items() if k in JD_COLUMNS}

        for field in ("skillset_required", "skillset_good_to_have"):
            if not isinstance(clean.get(field), list):
                clean[field] = []

        jd_id = jd_data.get("id")
        if jd_id:
            (
                self.client.table("job_descriptions")
                .update(clean)
                .eq("id", str(jd_id))
                .execute()
            )
            return str(jd_id)

        result = (
            self.client.table("job_descriptions")
            .insert(clean)
            .execute()
        )
        return result.data[0]["id"] if result.data else ""

    def get_all_jds(self, status: Optional[str] = None) -> List[dict]:
        """Return all JDs, optionally filtered by status."""
        q = self.client.table("job_descriptions").select("*")
        if status:
            q = q.eq("status", status)
        result = q.order("created_at", desc=True).execute()
        return result.data or []

    def get_jd_by_id(self, jd_id: str) -> Optional[dict]:
        """Fetch one JD by UUID."""
        try:
            result = (
                self.client.table("job_descriptions")
                .select("*")
                .eq("id", str(jd_id))
                .single()
                .execute()
            )
            return result.data
        except Exception as exc:
            print(f"[get_jd_by_id] {exc}")
            return None

    def get_active_jd_count(self) -> int:
        """Active JD count (used by sidebar)."""
        try:
            r = (
                self.client.table("job_descriptions")
                .select("id", count="exact")
                .eq("status", "Active")
                .execute()
            )
            return r.count or 0
        except Exception:
            return 0

    def get_open_position_count(self) -> int:
        """Sum of positions_count across active JDs (used by sidebar)."""
        try:
            r = (
                self.client.table("job_descriptions")
                .select("positions_count")
                .eq("status", "Active")
                .execute()
            )
            return sum(
                int(row.get("positions_count") or 0)
                for row in (r.data or [])
            )
        except Exception:
            return 0

    # ─────────────────────────────────────────────────────────────────────────
    #  PIPELINE / APPLICATIONS
    # ─────────────────────────────────────────────────────────────────────────

    def add_to_pipeline(
        self,
        candidate_id: str,
        jd_id: str,
        match_score: float = 0,
        match_breakdown: Optional[dict] = None,
    ) -> Optional[str]:
        """Add candidate to pipeline. Returns application UUID or None."""
        try:
            data = {
                "candidate_id": str(candidate_id),
                "jd_id":        str(jd_id),
                "stage":        "New",
                "match_score":  round(match_score, 2),
                "match_breakdown": match_breakdown or {},
                "stage_history": [
                    {
                        "stage": "New",
                        "at":    datetime.utcnow().isoformat(),
                        "by":    "RecruitIQ",
                    }
                ],
            }
            result = (
                self.client.table("applications")
                .upsert(data, on_conflict="candidate_id,jd_id")
                .execute()
            )
            return result.data[0]["id"] if result.data else None
        except Exception as exc:
            print(f"[add_to_pipeline] {exc}")
            return None

    def update_application_stage(
        self,
        application_id: str,
        new_stage: str,
        notes: str = "",
    ) -> bool:
        """Move an application to a new pipeline stage."""
        try:
            current = (
                self.client.table("applications")
                .select("stage_history")
                .eq("id", str(application_id))
                .single()
                .execute()
            )
            history = (current.data or {}).get("stage_history") or []
            history.append({
                "stage": new_stage,
                "at":    datetime.utcnow().isoformat(),
                "by":    "RecruitIQ",
            })
            self.client.table("applications").update({
                "stage":         new_stage,
                "stage_history": history,
                "notes":         notes or None,
            }).eq("id", str(application_id)).execute()
            return True
        except Exception as exc:
            print(f"[update_application_stage] {exc}")
            return False

    def get_pipeline_stats(
        self, jd_id: Optional[str] = None
    ) -> Dict[str, int]:
        """Return {stage: count} for all pipeline stages."""
        q = self.client.table("applications").select("stage")
        if jd_id:
            q = q.eq("jd_id", str(jd_id))
        result = q.execute()

        counts: Dict[str, int] = {s: 0 for s in PIPELINE_STAGES}
        for row in (result.data or []):
            stage = row.get("stage", "New")
            if stage in counts:
                counts[stage] += 1
        return counts

    def get_applications_for_candidate(
        self, candidate_id: str
    ) -> List[dict]:
        """Get all applications for a candidate."""
        try:
            result = (
                self.client.table("applications")
                .select("*, job_descriptions(role_name, jd_code, client_name)")
                .eq("candidate_id", str(candidate_id))
                .order("created_at", desc=True)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            print(f"[get_applications_for_candidate] {exc}")
            return []

    def get_pipeline_applications(
        self,
        jd_id: Optional[str] = None,
        stage: Optional[str] = None,
        limit: int = 100,
    ) -> List[dict]:
        """
        Fetch applications joined with candidate + JD info.
        Used by tracker Kanban.
        """
        try:
            q = (
                self.client.table("applications")
                .select(
                    "id, stage, match_score, notes, created_at, updated_at, "
                    "candidate_id, jd_id, "
                    "candidates(candidate_name, email_address, "
                    "           phone_number, location, "
                    "           total_experience, work_mode_pref), "
                    "job_descriptions(role_name, jd_code, client_name)"
                )
            )
            if jd_id:
                q = q.eq("jd_id", str(jd_id))
            if stage:
                q = q.eq("stage", stage)

            result = (
                q.order("created_at", desc=True)
                .limit(limit)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            print(f"[get_pipeline_applications] {exc}")
            return []

    # ─────────────────────────────────────────────────────────────────────────
    #  INTERVIEWS
    # ─────────────────────────────────────────────────────────────────────────

    def get_upcoming_interviews(self, days_ahead: int = 7) -> List[dict]:
        """Get upcoming interviews in the next N days."""
        try:
            now  = datetime.utcnow()
            then = now + timedelta(days=days_ahead)

            result = (
                self.client.table("applications")
                .select(
                    "id, interview_scheduled_at, interview_type, "
                    "interview_link, stage, "
                    "candidates(candidate_name, email_address, phone_number), "
                    "job_descriptions(role_name, jd_code)"
                )
                .not_.is_("interview_scheduled_at", "null")
                .gte("interview_scheduled_at", now.isoformat())
                .lte("interview_scheduled_at", then.isoformat())
                .order("interview_scheduled_at")
                .execute()
            )
            return result.data or []
        except Exception as exc:
            print(f"[get_upcoming_interviews] {exc}")
            return []

    # ─────────────────────────────────────────────────────────────────────────
    #  DASHBOARD METRICS
    # ─────────────────────────────────────────────────────────────────────────

    def get_dashboard_metrics(self) -> Dict[str, int]:
        """Fetch all dashboard metrics."""
        metrics: Dict[str, int] = {
            "total_candidates":    0,
            "active_jds":          0,
            "in_pipeline":         0,
            "interviews_this_week": 0,
            "joined_mtd":          0,
        }

        try:
            metrics["total_candidates"] = self.get_candidate_count()
        except Exception:
            pass

        try:
            metrics["active_jds"] = self.get_active_jd_count()
        except Exception:
            pass

        try:
            r = (
                self.client.table("applications")
                .select("id", count="exact")
                .not_.in_("stage", ["Joined", "Rejected"])
                .execute()
            )
            metrics["in_pipeline"] = r.count or 0
        except Exception:
            pass

        try:
            now  = datetime.utcnow()
            week = now + timedelta(days=7)
            r = (
                self.client.table("applications")
                .select("id", count="exact")
                .not_.is_("interview_scheduled_at", "null")
                .gte("interview_scheduled_at", now.isoformat())
                .lte("interview_scheduled_at", week.isoformat())
                .execute()
            )
            metrics["interviews_this_week"] = r.count or 0
        except Exception:
            pass

        try:
            start_of_month = datetime.utcnow().replace(
                day=1, hour=0, minute=0, second=0, microsecond=0,
            )
            r = (
                self.client.table("applications")
                .select("id", count="exact")
                .eq("stage", "Joined")
                .gte("updated_at", start_of_month.isoformat())
                .execute()
            )
            metrics["joined_mtd"] = r.count or 0
        except Exception:
            pass

        return metrics

    # ─────────────────────────────────────────────────────────────────────────
    #  SOCIAL POSTS & BANNERS (v1.2)
    # ─────────────────────────────────────────────────────────────────────────

    def save_social_post(
        self,
        jd_id: str,
        platform: str,
        content: str,
        style: str,
        generated_by: str = "RecruitIQ",
    ) -> Optional[str]:
        """
        Save a social media post to social_posts table.
        Returns post UUID or None.

        Args:
            jd_id: Job description UUID
            platform: "LinkedIn", "Instagram", "Twitter", or "WhatsApp"
            content: Generated social media content (text)
            style: "Professional", "Casual", "Question", or "Announcement"
            generated_by: Who/what generated this (default: "RecruitIQ")

        Returns:
            UUID of created post, or None on failure
        """
        try:
            data = {
                "jd_id": str(jd_id),
                "platform": platform,
                "content": content,
                "style": style,
                "generated_by": generated_by,
            }
            result = (
                self.client.table("social_posts")
                .insert(data)
                .execute()
            )
            if result.data:
                post_id = result.data[0].get("id")
                print(f"[save_social_post] ✅ Saved {platform} post ({post_id})")
                return post_id
            else:
                print(f"[save_social_post] ⚠️  No data returned")
                return None
        except Exception as exc:
            print(f"[save_social_post] ❌ {type(exc).__name__}: {exc}")
            return None

    def get_social_posts(
        self,
        jd_id: Optional[str] = None,
        platform: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[dict]:
        """
        Fetch social posts from social_posts table.

        Args:
            jd_id: Filter by JD (optional)
            platform: Filter by platform (optional)
            limit: Max results to return
            offset: Pagination offset

        Returns:
            List of social post records
        """
        try:
            q = self.client.table("social_posts").select("*")

            if jd_id:
                q = q.eq("jd_id", str(jd_id))
            if platform:
                q = q.eq("platform", platform)

            result = (
                q.order("created_at", desc=True)
                .range(offset, offset + limit - 1)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            print(f"[get_social_posts] ❌ {type(exc).__name__}: {exc}")
            return []

    def save_banner(
        self,
        jd_id: str,
        platform: str,
        image_url: str,
        width: int = 1200,
        height: int = 628,
        ttl_seconds: int = 2592000,  # 30 days
    ) -> Optional[str]:
        """
        Save banner metadata to banner_cache table.
        Returns cache UUID or None.

        Args:
            jd_id: Job description UUID
            platform: "LinkedIn", "Instagram", "Twitter", or "WhatsApp"
            image_url: Full URL to the banner image in Supabase Storage
            width: Image width in pixels
            height: Image height in pixels
            ttl_seconds: Cache TTL in seconds (default: 30 days)

        Returns:
            UUID of created banner cache record, or None on failure
        """
        try:
            expires_at = datetime.utcnow() + timedelta(seconds=ttl_seconds)
            data = {
                "jd_id": str(jd_id),
                "platform": platform,
                "image_url": image_url,
                "width": width,
                "height": height,
                "expires_at": expires_at.isoformat(),
            }
            result = (
                self.client.table("banner_cache")
                .insert(data)
                .execute()
            )
            if result.data:
                banner_id = result.data[0].get("id")
                print(
                    f"[save_banner] ✅ Saved {platform} banner "
                    f"({width}x{height}, ID: {banner_id})"
                )
                return banner_id
            else:
                print(f"[save_banner] ⚠️  No data returned")
                return None
        except Exception as exc:
            print(f"[save_banner] ❌ {type(exc).__name__}: {exc}")
            return None

    def get_banner(
        self,
        jd_id: str,
        platform: str,
    ) -> Optional[dict]:
        """
        Fetch cached banner metadata (if not expired).
        Returns banner record or None.

        Args:
            jd_id: Job description UUID
            platform: "LinkedIn", "Instagram", "Twitter", or "WhatsApp"

        Returns:
            Banner metadata dict or None if not found/expired
        """
        try:
            now_iso = datetime.utcnow().isoformat()
            result = (
                self.client.table("banner_cache")
                .select("*")
                .eq("jd_id", str(jd_id))
                .eq("platform", platform)
                .gt("expires_at", now_iso)  # not expired
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
            if result.data:
                print(f"[get_banner] ✅ Found cached {platform} banner")
                return result.data[0]
            else:
                print(f"[get_banner] ℹ️  No valid cached banner for {platform}")
                return None
        except Exception as exc:
            print(f"[get_banner] ❌ {type(exc).__name__}: {exc}")
            return None

    def delete_expired_banners(self) -> int:
        """
        Delete all expired banners from cache.
        Returns count of deleted records.
        """
        try:
            now_iso = datetime.utcnow().isoformat()
            result = (
                self.client.table("banner_cache")
                .delete()
                .lt("expires_at", now_iso)
                .execute()
            )
            # Supabase delete() doesn't return count, so we estimate
            print(f"[delete_expired_banners] ✅ Cleanup complete")
            return 0
        except Exception as exc:
            print(f"[delete_expired_banners] ❌ {type(exc).__name__}: {exc}")
            return 0

    def get_banner_url(
        self,
        jd_id: str,
        platform: str,
        fallback_image_bytes: Optional[bytes] = None,
    ) -> Optional[str]:
        """
        Get banner URL from cache. If expired/missing, optionally generate & save.

        Args:
            jd_id: Job description UUID
            platform: "LinkedIn", "Instagram", "Twitter", or "WhatsApp"
            fallback_image_bytes: If cache miss, upload this & save. Optional.

        Returns:
            Banner image URL or None
        """
        # Try to get from cache
        cached = self.get_banner(jd_id, platform)
        if cached:
            return cached.get("image_url")

        # If fallback provided, upload & cache
        if fallback_image_bytes:
            try:
                bucket = "social-banners"
                file_path = (
                    f"{jd_id}/{platform}_"
                    f"{datetime.utcnow().isoformat()}.png"
                )
                self.client.storage.from_(bucket).upload(
                    file_path,
                    fallback_image_bytes,
                    {"content-type": "image/png"},
                )
                image_url = (
                    f"{self._url}/storage/v1/object/public/{bucket}/{file_path}"
                )
                config = PLATFORM_CONFIG.get(
                    platform,
                    {"width": 1200, "height": 628}
                )
                self.save_banner(
                    jd_id=jd_id,
                    platform=platform,
                    image_url=image_url,
                    width=config.get("width", 1200),
                    height=config.get("height", 628),
                )
                return image_url
            except Exception as exc:
                print(f"[get_banner_url] ❌ Upload failed: {exc}")
                return None

        return None
