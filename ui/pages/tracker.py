# ui/pages/tracker.py
# RecruitIQ – Candidate Tracker  (v3.0)
# ══════════════════════════════════════════════════════════════════
# • Kanban view reads from applications + candidates (UUID schema)
# • Submissions tab manages the legacy submissions table
# • db is SupabaseManager — REST for pipeline, _db() for submissions
# ══════════════════════════════════════════════════════════════════

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from config import Config
from ui.styles import page_header, stage_badge

# ─────────────────────────────────────────────────────────────────
#  SUBMISSIONS TABLE DDL  (legacy — read/write allowed)
# ─────────────────────────────────────────────────────────────────
_SUBMISSIONS_DDL = """
CREATE TABLE IF NOT EXISTS submissions (
    id                  SERIAL          PRIMARY KEY,
    candidate_name      TEXT            NOT NULL,
    candidate_id        UUID            REFERENCES candidates(id)
                                            ON DELETE SET NULL,
    jd_id               UUID            REFERENCES job_descriptions(id)
                                            ON DELETE SET NULL,
    client_name         TEXT,
    role_name           TEXT,
    experience_years    NUMERIC(4,1),
    skills              JSONB           DEFAULT '[]',
    current_ctc         NUMERIC(12,2),
    expected_ctc        NUMERIC(12,2),
    notice_period       TEXT,
    last_working_day    DATE,
    current_location    TEXT,
    stage               TEXT            DEFAULT 'New',
    remarks             TEXT,
    submitted_by        TEXT,
    submitted_on        DATE            DEFAULT CURRENT_DATE,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_submissions_jd_id
    ON submissions(jd_id);
CREATE INDEX IF NOT EXISTS idx_submissions_stage
    ON submissions(stage);
CREATE INDEX IF NOT EXISTS idx_submissions_candidate_id
    ON submissions(candidate_id);
"""

_NOTICE_OPTIONS: List[str] = [
    "Immediate", "15 days", "30 days",
    "45 days",  "60 days", "90 days", "Serving notice",
]
_DATE_FMT = "%d %b %Y"


# ═════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═════════════════════════════════════════════════════════════════

def render_candidate_tracker(services: dict) -> None:
    page_header("Candidate Tracker", "Kanban pipeline + submissions", "📊")

    db = services["db"]

    # Ensure submissions table exists (idempotent)
    _ensure_submissions_table(db)

    tab_kanban, tab_submissions = st.tabs(
        ["📋 Pipeline Kanban", "📝 Submissions"]
    )

    with tab_kanban:
        _render_kanban(db)

    with tab_submissions:
        _render_submissions(db)


# ─────────────────────────────────────────────────────────────────
#  TABLE BOOTSTRAP
# ─────────────────────────────────────────────────────────────────

def _ensure_submissions_table(db) -> None:
    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                cur.execute(_SUBMISSIONS_DDL)
            conn.commit()
    except Exception as exc:
        st.warning(f"⚠️ Could not verify submissions table: {exc}")


# ═════════════════════════════════════════════════════════════════
#  KANBAN TAB  —  reads from applications table (UUID schema)
# ═════════════════════════════════════════════════════════════════

def _render_kanban(db) -> None:

    # ── JD selector ──────────────────────────────────────────────
    jds = _safe(db.get_all_jds, [])
    jd_opts: Dict[str, dict] = {
        f"{j.get('jd_code', '?')} — {j.get('role_name', '?')}": j
        for j in jds
    }
    sel = st.selectbox("Filter by JD", ["All JDs"] + list(jd_opts.keys()))
    jd_id: Optional[str] = (
        str(jd_opts[sel]["id"])
        if sel != "All JDs" and sel in jd_opts
        else None
    )

    # ── Stage counts from applications table ─────────────────────
    stats = _get_pipeline_stats(db, jd_id)

    total_active = sum(
        v for k, v in stats.items()
        if k not in ("Joined", "Rejected")
    )

    # ── KPI strip ─────────────────────────────────────────────────
    kpi_items = [
        ("New",       "🆕"),
        ("Screening", "🔍"),
        ("Interview", "🎤"),
        ("Offer",     "💼"),
        ("Joined",    "✅"),
    ]
    kpi_cols = st.columns(len(kpi_items))
    for col, (stage, icon) in zip(kpi_cols, kpi_items):
        color = Config.STAGE_COLORS.get(stage, "#6C63FF")
        with col:
            st.markdown(
                f"""
                <div class='riq-metric-card'>
                  <div style='font-size:1.2rem'>{icon}</div>
                  <div class='riq-metric-val' style='color:{color}'>
                    {stats.get(stage, 0)}
                  </div>
                  <div class='riq-metric-label'>{stage}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown(
        f"<div style='color:#8892b0;font-size:0.82rem;margin:10px 0 16px'>"
        f"Active pipeline: "
        f"<b style='color:#6C63FF'>{total_active}</b></div>",
        unsafe_allow_html=True,
    )

    # ── Kanban board ──────────────────────────────────────────────
    stages = list(Config.PIPELINE_STAGES)
    for row_start in range(0, len(stages), 5):
        row_stages = stages[row_start: row_start + 5]
        cols = st.columns(len(row_stages))

        for col, stage in zip(cols, row_stages):
            color = Config.STAGE_COLORS.get(stage, "#6C63FF")
            count = stats.get(stage, 0)

            with col:
                st.markdown(
                    f"""
                    <div class='riq-kanban-header'
                         style='background:{color}22;color:{color};
                                border:1px solid {color}'>
                      {stage}
                      <span style='font-size:0.75rem;
                                   background:{color}44;
                                   border-radius:10px;
                                   padding:1px 6px'>
                        {count}
                      </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                # Cards from applications table
                cards = _get_kanban_cards(db, stage, jd_id, limit=6)
                for card in cards:
                    cand     = card.get("candidates") or {}
                    jd_info  = card.get("job_descriptions") or {}
                    exp_raw  = cand.get("total_experience")
                    exp_str  = f"{exp_raw}y" if exp_raw is not None else "—"

                    st.markdown(
                        f"""
                        <div class='riq-kanban-card'>
                          <div style='color:#ccd6f6;font-weight:600;
                                      font-size:0.78rem'>
                            {str(cand.get("candidate_name", "?"))[:22]}
                          </div>
                          <div style='color:#8892b0;font-size:0.68rem;
                                      margin-top:3px'>
                            {str(jd_info.get("client_name", "—"))[:18]}
                            &nbsp;·&nbsp;{exp_str}
                          </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                    # Quick stage-move via applications table
                    next_stages = _next_stages(stage)
                    if next_stages:
                        new_stage = st.selectbox(
                            "Move to",
                            ["—"] + next_stages,
                            key=f"move_{card['id']}_{stage}",
                            label_visibility="collapsed",
                        )
                        if new_stage != "—":
                            db.update_application_stage(
                                card["id"], new_stage
                            )
                            st.rerun()

        st.markdown(
            "<hr style='border-color:#1a1a3e;margin:8px 0'>",
            unsafe_allow_html=True,
        )


# ═════════════════════════════════════════════════════════════════
#  SUBMISSIONS TAB  —  reads/writes submissions table
# ═════════════════════════════════════════════════════════════════

def _render_submissions(db) -> None:

    col_add, col_search, col_filter = st.columns([1, 2, 2])

    with col_add:
        if st.button(
            "➕ Add Submission", use_container_width=True, type="primary"
        ):
            st.session_state.sub_form_mode = "add"
            st.session_state.sub_edit_id   = None
            st.session_state.show_sub_form = True

    with col_search:
        search_q = st.text_input(
            "Search",
            placeholder="Name / client / skill…",
            label_visibility="collapsed",
        )

    with col_filter:
        stage_filter = st.selectbox(
            "Stage filter",
            ["All Stages"] + list(Config.PIPELINE_STAGES),
            label_visibility="collapsed",
        )

    if st.session_state.get("show_sub_form"):
        _render_submission_form(db)
        st.markdown("---")

    submissions = _fetch_submissions(
        db,
        search=search_q or None,
        stage=None if stage_filter == "All Stages" else stage_filter,
    )

    if not submissions:
        st.info("No submissions yet. Click **➕ Add Submission** to start.")
        return

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total",       len(submissions))
    m2.metric("Screening",   sum(1 for s in submissions if s["stage"] == "Screening"))
    m3.metric("Offer",       sum(1 for s in submissions if s["stage"] == "Offer"))
    m4.metric("Joined",      sum(1 for s in submissions if s["stage"] == "Joined"))

    st.markdown(f"**{len(submissions)} submission(s)**")
    _table_header()

    for sub in submissions:
        _render_submission_row(db, sub)


# ─────────────────────────────────────────────────────────────────
#  ADD / EDIT FORM
# ─────────────────────────────────────────────────────────────────

def _render_submission_form(db) -> None:
    mode    = st.session_state.get("sub_form_mode", "add")
    edit_id = st.session_state.get("sub_edit_id")

    prefill: Dict[str, Any] = {}
    if mode == "edit" and edit_id:
        prefill = _fetch_submission_by_id(db, edit_id) or {}

    title = "✏️ Edit Submission" if mode == "edit" else "➕ New Submission"

    with st.expander(title, expanded=True):

        # ── Candidate lookup ──────────────────────────────────────
        all_candidates = _fetch_candidate_options(db)
        cand_names = ["— type manually —"] + [
            f"{c['candidate_name']} ({str(c['id'])[:8]}…)"
            for c in all_candidates
        ]

        default_cand_idx = 0
        if prefill.get("candidate_id"):
            for i, c in enumerate(all_candidates, start=1):
                if str(c["id"]) == str(prefill["candidate_id"]):
                    default_cand_idx = i
                    break

        sel_cand = st.selectbox(
            "Link to Candidate Profile (optional)",
            cand_names,
            index=default_cand_idx,
            key="sub_form_cand_sel",
        )

        linked: Dict[str, Any] = {}
        if sel_cand != "— type manually —":
            # Extract UUID from display string
            raw_id = sel_cand.split("(")[-1].rstrip("…)")
            # Match back to full UUID
            for c in all_candidates:
                if str(c["id"]).startswith(raw_id):
                    linked = _fetch_candidate_profile(db, str(c["id"])) or {}
                    break

        # ── Row 1 ─────────────────────────────────────────────────
        r1c1, r1c2, r1c3 = st.columns(3)

        with r1c1:
            candidate_name = st.text_input(
                "Candidate Name *",
                value=linked.get("candidate_name") or prefill.get("candidate_name", ""),
                key="sub_f_cname",
            )
        with r1c2:
            client_name = st.text_input(
                "Client Name",
                value=prefill.get("client_name", ""),
                key="sub_f_client",
            )
        with r1c3:
            jds     = _safe(db.get_all_jds, [])
            jd_opts = {
                f"{j.get('jd_code', '?')} — {j.get('role_name', '?')}": j
                for j in jds
            }
            jd_choices  = ["— none —"] + list(jd_opts.keys())
            default_jd  = "— none —"

            if prefill.get("jd_id"):
                for k, v in jd_opts.items():
                    if str(v["id"]) == str(prefill["jd_id"]):
                        default_jd = k
                        break

            sel_jd    = st.selectbox(
                "Link to JD (optional)",
                jd_choices,
                index=jd_choices.index(default_jd),
                key="sub_f_jd",
            )
            linked_jd = jd_opts.get(sel_jd, {}) if sel_jd != "— none —" else {}

        role_name = st.text_input(
            "Role / Position",
            value=linked_jd.get("role_name") or prefill.get("role_name", ""),
            key="sub_f_role",
        )

        # ── Row 2 ─────────────────────────────────────────────────
        r2c1, r2c2 = st.columns([1, 3])

        with r2c1:
            exp_default = float(
                linked.get("total_experience")
                or prefill.get("experience_years")
                or 0.0
            )
            experience_years = st.number_input(
                "Experience (yrs)",
                min_value=0.0, max_value=60.0,
                value=exp_default, step=0.1, format="%.1f",
                key="sub_f_exp",
            )

        with r2c2:
            default_skills = _skills_to_str(
                linked.get("skills") or prefill.get("skills") or []
            )
            skills_raw = st.text_input(
                "Skills (comma-separated)",
                value=default_skills,
                placeholder="Python, Django, PostgreSQL",
                key="sub_f_skills",
            )

        # ── Row 3: CTC ────────────────────────────────────────────
        r3c1, r3c2, r3c3 = st.columns(3)

        with r3c1:
            current_ctc = st.number_input(
                "Current CTC (LPA)",
                min_value=0.0, max_value=999.0,
                value=float(prefill.get("current_ctc") or 0.0),
                step=0.1, format="%.1f", key="sub_f_cctc",
            )
        with r3c2:
            expected_ctc = st.number_input(
                "Expected CTC (LPA)",
                min_value=0.0, max_value=999.0,
                value=float(prefill.get("expected_ctc") or 0.0),
                step=0.1, format="%.1f", key="sub_f_ectc",
            )
        with r3c3:
            notice_opts    = list(_NOTICE_OPTIONS)
            default_notice = prefill.get("notice_period") or "30 days"
            if default_notice not in notice_opts:
                notice_opts = [default_notice] + notice_opts
            notice_period = st.selectbox(
                "Notice Period", notice_opts,
                index=notice_opts.index(default_notice),
                key="sub_f_notice",
            )

        # ── Row 4: LWD / Location / Stage ────────────────────────
        r4c1, r4c2, r4c3 = st.columns(3)

        with r4c1:
            lwd_raw     = prefill.get("last_working_day")
            lwd_default: Optional[date] = None
            if lwd_raw:
                if isinstance(lwd_raw, (date, datetime)):
                    lwd_default = (
                        lwd_raw.date()
                        if isinstance(lwd_raw, datetime)
                        else lwd_raw
                    )
                else:
                    try:
                        lwd_default = datetime.strptime(
                            str(lwd_raw), "%Y-%m-%d"
                        ).date()
                    except Exception:
                        lwd_default = None
            last_working_day = st.date_input(
                "Last Working Day", value=lwd_default, key="sub_f_lwd"
            )

        with r4c2:
            current_location = st.text_input(
                "Current Location",
                value=prefill.get("current_location", ""),
                key="sub_f_loc",
            )

        with r4c3:
            stages        = list(Config.PIPELINE_STAGES)
            default_stage = prefill.get("stage", "New")
            if default_stage not in stages:
                default_stage = "New"
            stage = st.selectbox(
                "Pipeline Stage", stages,
                index=stages.index(default_stage),
                key="sub_f_stage",
            )

        # ── Remarks / Submitted by ────────────────────────────────
        remarks = st.text_area(
            "Remarks / Notes",
            value=prefill.get("remarks", ""),
            height=90,
            key="sub_f_remarks",
        )
        submitted_by = st.text_input(
            "Submitted by",
            value=prefill.get("submitted_by", ""),
            key="sub_f_by",
        )

        # ── Buttons ───────────────────────────────────────────────
        btn_col1, btn_col2, _ = st.columns([1, 1, 4])

        with btn_col1:
            save_clicked = st.button(
                "💾 Save" if mode == "edit" else "✅ Submit",
                type="primary", use_container_width=True, key="sub_f_save",
            )
        with btn_col2:
            cancel_clicked = st.button(
                "✖ Cancel", use_container_width=True, key="sub_f_cancel"
            )

        if cancel_clicked:
            st.session_state.show_sub_form = False
            st.session_state.sub_form_mode = None
            st.session_state.sub_edit_id   = None
            st.rerun()

        if save_clicked:
            if not candidate_name.strip():
                st.error("Candidate Name is required.")
                return

            skills_list = [
                s.strip()
                for s in re.split(r"[,;|]", skills_raw)
                if s.strip()
            ]

            # jd_id as plain string UUID or None
            jd_id_val: Optional[str] = (
                str(linked_jd["id"]) if linked_jd.get("id") else None
            )
            # candidate_id as plain string UUID or None
            cand_id_val: Optional[str] = (
                str(linked["id"]) if linked.get("id") else None
            )

            payload: Dict[str, Any] = {
                "candidate_name":   candidate_name.strip(),
                "candidate_id":     cand_id_val,
                "jd_id":            jd_id_val,
                "client_name":      client_name.strip() or None,
                "role_name":        role_name.strip() or None,
                "experience_years": experience_years or None,
                "skills":           json.dumps(skills_list),
                "current_ctc":      current_ctc or None,
                "expected_ctc":     expected_ctc or None,
                "notice_period":    notice_period,
                "last_working_day": (
                    str(last_working_day) if last_working_day else None
                ),
                "current_location": current_location.strip() or None,
                "stage":            stage,
                "remarks":          remarks.strip() or None,
                "submitted_by":     submitted_by.strip() or None,
            }

            if mode == "edit" and edit_id:
                ok, err = _update_submission(db, edit_id, payload)
            else:
                ok, err = _insert_submission(db, payload)

            if ok:
                st.success("✅ Submission saved!")
                st.session_state.show_sub_form = False
                st.session_state.sub_form_mode = None
                st.session_state.sub_edit_id   = None
                st.rerun()
            else:
                st.error(f"❌ Save failed: {err}")


# ═════════════════════════════════════════════════════════════════
#  PIPELINE DB HELPERS  (REST via SupabaseManager)
# ═════════════════════════════════════════════════════════════════

def _get_pipeline_stats(
    db, jd_id: Optional[str] = None
) -> Dict[str, int]:
    """Stage counts from applications table via REST."""
    try:
        return db.get_pipeline_stats(jd_id=jd_id)
    except Exception:
        return {s: 0 for s in Config.PIPELINE_STAGES}


def _get_kanban_cards(
    db,
    stage:  str,
    jd_id:  Optional[str],
    limit:  int = 6,
) -> List[Dict]:
    """Application cards for one Kanban column via REST."""
    try:
        return db.get_pipeline_applications(
            jd_id=jd_id, stage=stage, limit=limit
        )
    except Exception:
        return []


# ═════════════════════════════════════════════════════════════════
#  SUBMISSIONS DB HELPERS  (psycopg2 via db._db())
# ═════════════════════════════════════════════════════════════════

def _fetch_submissions(
    db,
    search: Optional[str] = None,
    stage:  Optional[str] = None,
) -> List[Dict]:
    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                conditions: List[str] = []
                params:     List[Any] = []

                if search:
                    conditions.append(
                        "(LOWER(candidate_name) LIKE %s"
                        " OR LOWER(client_name)  LIKE %s"
                        " OR LOWER(role_name)    LIKE %s"
                        " OR LOWER(remarks)      LIKE %s)"
                    )
                    term = f"%{search.lower()}%"
                    params.extend([term, term, term, term])

                if stage:
                    conditions.append("stage = %s")
                    params.append(stage)

                where = (
                    "WHERE " + " AND ".join(conditions)
                    if conditions else ""
                )

                cur.execute(
                    f"""
                    SELECT
                        id, candidate_name, candidate_id, jd_id,
                        client_name, role_name, experience_years,
                        skills, current_ctc, expected_ctc,
                        notice_period, last_working_day,
                        current_location, stage, remarks,
                        submitted_by, submitted_on
                    FROM submissions
                    {where}
                    ORDER BY created_at DESC
                    """,
                    params,
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        st.error(f"Error loading submissions: {exc}")
        return []


def _fetch_submission_by_id(db, sub_id: int) -> Optional[Dict]:
    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM submissions WHERE id = %s", (sub_id,)
                )
                cols = [d[0] for d in cur.description]
                row  = cur.fetchone()
                return dict(zip(cols, row)) if row else None
    except Exception:
        return None


def _insert_submission(
    db, payload: Dict
) -> Tuple[bool, Optional[str]]:
    # Remove None UUID fields to avoid cast errors
    clean = {
        k: v for k, v in payload.items()
        if not (k in ("candidate_id", "jd_id") and v is None)
    }

    cols  = list(clean.keys())
    vals  = list(clean.values())

    placeholders = []
    for col in cols:
        if col in ("candidate_id", "jd_id"):
            placeholders.append("%s::uuid")
        else:
            placeholders.append("%s")

    col_str = ", ".join(cols)
    ph_str  = ", ".join(placeholders)

    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO submissions ({col_str}) VALUES ({ph_str})",
                    vals,
                )
            conn.commit()
        return True, None
    except Exception as exc:
        return False, str(exc)


def _update_submission(
    db, sub_id: int, payload: Dict
) -> Tuple[bool, Optional[str]]:
    set_parts = []
    for k in payload:
        if k in ("candidate_id", "jd_id"):
            set_parts.append(f"{k} = %s::uuid")
        else:
            set_parts.append(f"{k} = %s")
    set_parts.append("updated_at = NOW()")

    set_clause = ", ".join(set_parts)
    vals = list(payload.values()) + [sub_id]

    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE submissions SET {set_clause} WHERE id = %s",
                    vals,
                )
            conn.commit()
        return True, None
    except Exception as exc:
        return False, str(exc)


def _delete_submission(db, sub_id: int) -> Tuple[bool, Optional[str]]:
    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM submissions WHERE id = %s", (sub_id,)
                )
            conn.commit()
        return True, None
    except Exception as exc:
        return False, str(exc)


def _update_submission_stage(db, sub_id: int, stage: str) -> None:
    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE submissions "
                    "SET stage = %s, updated_at = NOW() "
                    "WHERE id = %s",
                    (stage, sub_id),
                )
            conn.commit()
    except Exception as exc:
        st.error(f"Stage update failed: {exc}")


def _fetch_candidate_options(db) -> List[Dict]:
    """Return id (UUID) + candidate_name for the candidate picker."""
    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, candidate_name
                    FROM   candidates
                    WHERE  is_active = TRUE
                    ORDER  BY candidate_name
                    LIMIT  500
                    """
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception:
        return []


def _fetch_candidate_profile(db, cand_id: str) -> Optional[Dict]:
    """Key candidate fields for form auto-fill. cand_id is a UUID string."""
    try:
        with db._db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id, candidate_name, total_experience,
                        skills,             -- JSONB column (not skills_primary)
                        notice_period,
                        location            AS current_location
                    FROM   candidates
                    WHERE  id = %s::uuid
                    """,
                    (str(cand_id),),
                )
                cols = [d[0] for d in cur.description]
                row  = cur.fetchone()
                return dict(zip(cols, row)) if row else None
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════
#  TABLE RENDER HELPERS
# ═════════════════════════════════════════════════════════════════

def _table_header() -> None:
    hcols = st.columns([2, 1.5, 1, 1.5, 1, 1, 1, 1.2, 0.8, 0.8])
    headers = [
        "Candidate", "Client / Role", "Exp",
        "Skills", "Curr CTC", "Exp CTC",
        "Notice", "Stage", "Edit", "Del",
    ]
    for col, h in zip(hcols, headers):
        col.markdown(
            f"<div style='font-size:0.72rem;color:#8892b0;"
            f"font-weight:700;text-transform:uppercase'>{h}</div>",
            unsafe_allow_html=True,
        )
    st.markdown(
        "<hr style='border-color:#1a1a3e;margin:4px 0 8px'>",
        unsafe_allow_html=True,
    )


def _render_submission_row(db, sub: Dict[str, Any]) -> None:
    sub_id = sub["id"]
    cols   = st.columns([2, 1.5, 1, 1.5, 1, 1, 1, 1.2, 0.8, 0.8])

    stage       = sub.get("stage", "New")
    color       = Config.STAGE_COLORS.get(stage, "#6C63FF")
    skills_list = _parse_skills(sub.get("skills"))
    skills_disp = ", ".join(skills_list[:3]) + (
        f" +{len(skills_list) - 3}" if len(skills_list) > 3 else ""
    )
    lwd     = sub.get("last_working_day")
    lwd_str = (
        lwd.strftime(_DATE_FMT)
        if isinstance(lwd, (date, datetime))
        else (str(lwd) if lwd else "—")
    )

    with cols[0]:
        st.markdown(
            f"<div style='font-size:0.82rem;font-weight:600;"
            f"color:#ccd6f6'>{sub.get('candidate_name','—')}</div>"
            f"<div style='font-size:0.7rem;color:#8892b0'>"
            f"{sub.get('current_location','—')} · LWD {lwd_str}</div>",
            unsafe_allow_html=True,
        )
    with cols[1]:
        st.markdown(
            f"<div style='font-size:0.78rem;color:#a8b4d0'>"
            f"{sub.get('client_name','—')}</div>"
            f"<div style='font-size:0.7rem;color:#8892b0'>"
            f"{sub.get('role_name','—')}</div>",
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            f"<div style='font-size:0.82rem;color:#ccd6f6'>"
            f"{sub.get('experience_years','—')}y</div>",
            unsafe_allow_html=True,
        )
    with cols[3]:
        st.markdown(
            f"<div style='font-size:0.72rem;color:#8892b0'>"
            f"{skills_disp or '—'}</div>",
            unsafe_allow_html=True,
        )
    with cols[4]:
        val = sub.get("current_ctc")
        st.markdown(
            f"<div style='font-size:0.82rem;color:#ccd6f6'>"
            f"{'₹' + str(val) + 'L' if val else '—'}</div>",
            unsafe_allow_html=True,
        )
    with cols[5]:
        val = sub.get("expected_ctc")
        st.markdown(
            f"<div style='font-size:0.82rem;color:#ccd6f6'>"
            f"{'₹' + str(val) + 'L' if val else '—'}</div>",
            unsafe_allow_html=True,
        )
    with cols[6]:
        st.markdown(
            f"<div style='font-size:0.75rem;color:#8892b0'>"
            f"{sub.get('notice_period','—')}</div>",
            unsafe_allow_html=True,
        )
    with cols[7]:
        st.markdown(
            f"<span style='background:{color}22;color:{color};"
            f"border:1px solid {color};border-radius:10px;"
            f"padding:2px 8px;font-size:0.72rem'>{stage}</span>",
            unsafe_allow_html=True,
        )
        new_stage = st.selectbox(
            "Move", ["—"] + list(Config.PIPELINE_STAGES),
            key=f"stg_{sub_id}", label_visibility="collapsed",
        )
        if new_stage != "—" and new_stage != stage:
            _update_submission_stage(db, sub_id, new_stage)
            st.rerun()

    with cols[8]:
        if st.button("✏️", key=f"edit_{sub_id}", help="Edit"):
            st.session_state.sub_form_mode = "edit"
            st.session_state.sub_edit_id   = sub_id
            st.session_state.show_sub_form = True
            st.rerun()

    with cols[9]:
        if st.button("🗑️", key=f"del_{sub_id}", help="Delete"):
            st.session_state[f"confirm_del_{sub_id}"] = True

    if st.session_state.get(f"confirm_del_{sub_id}"):
        st.warning(
            f"Delete **{sub.get('candidate_name')}**? Cannot be undone."
        )
        dc1, dc2, _ = st.columns([1, 1, 6])
        if dc1.button("Yes, delete", key=f"yes_del_{sub_id}", type="primary"):
            ok, err = _delete_submission(db, sub_id)
            if ok:
                st.success("Deleted.")
                del st.session_state[f"confirm_del_{sub_id}"]
                st.rerun()
            else:
                st.error(f"Delete failed: {err}")
        if dc2.button("Cancel", key=f"no_del_{sub_id}"):
            del st.session_state[f"confirm_del_{sub_id}"]
            st.rerun()

    if sub.get("remarks"):
        st.caption(
            f"💬 {sub['remarks'][:120]}"
            f"{'…' if len(sub['remarks']) > 120 else ''}"
        )

    st.markdown(
        "<hr style='border-color:#1a1a3e;margin:4px 0'>",
        unsafe_allow_html=True,
    )


# ═════════════════════════════════════════════════════════════════
#  UTILITY
# ═════════════════════════════════════════════════════════════════

def _parse_skills(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [s for s in raw if s]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return [s.strip() for s in re.split(r"[,;|]", str(raw)) if s.strip()]


def _skills_to_str(raw: Any) -> str:
    parsed = _parse_skills(raw)
    return ", ".join(parsed)


def _next_stages(current: str) -> List[str]:
    stages = list(Config.PIPELINE_STAGES)
    try:
        idx   = stages.index(current)
        nexts = []
        if idx + 1 < len(stages):
            nexts.append(stages[idx + 1])
        if current not in ("Joined", "Rejected"):
            nexts.append("Rejected")
        return nexts
    except ValueError:
        return []


def _safe(fn: Any, default: Any = None) -> Any:
    try:
        return fn()
    except Exception:
        return default
