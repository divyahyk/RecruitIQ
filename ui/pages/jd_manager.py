# ui/pages/jd_manager.py
"""
RecruitIQ – JD Manager (v2.5)

Changes from v2.4:
  - _fetch_candidates_for_jd: replaced broken FTS arm
    (plainto_tsquery | plainto_tsquery → "operator does not exist" error)
    with an ILIKE keyword scan across current_position, title,
    and profile_summary. Handles multi-word skills and special chars
    (S4HANA, C++, Node.js) that the FTS lexer strips.
  - _pg_literal helper retained but no longer used for FTS;
    kept for any future SQL literal needs.
"""

from __future__ import annotations

import json
import re

import streamlit as st

from database.query_runner import execute_raw
from ui.styles import page_header, skill_pills


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_STOP: set[str] = {
    "and", "or", "the", "a", "an", "of", "for", "in",
    "with", "at", "by", "to", "on", "be", "as", "is",
}

_CANDIDATE_SELECT = """
    id,
    candidate_name,
    first_name,
    last_name,
    email_address,
    phone_number,
    current_position,
    title,
    current_company,
    location,
    total_experience,
    notice_period,
    work_mode_pref,
    skills,
    profile_summary,
    remarks
"""


# ─────────────────────────────────────────────────────────────────────────────
# Small utilities
# ─────────────────────────────────────────────────────────────────────────────

def _get_name(c: dict) -> str:
    if c.get("candidate_name"):
        return c["candidate_name"].strip()
    first = (c.get("first_name") or "").strip()
    last  = (c.get("last_name")  or "").strip()
    return f"{first} {last}".strip() or "—"


def _get_title(c: dict) -> str:
    return (c.get("current_position") or c.get("title") or "—").strip()


def _normalise_skills(raw) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [s.lower().strip() for s in raw if s]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [s.lower().strip() for s in parsed if s]
    except Exception:
        pass
    return [s.lower().strip() for s in re.split(r"[,;]", str(raw)) if s.strip()]


def _skill_in_text(skill_lower: str, text_lower: str) -> bool:
    return bool(
        re.search(
            r"(?<![a-z0-9])" + re.escape(skill_lower) + r"(?![a-z0-9])",
            text_lower,
        )
    )


def _tokenise(text: str) -> set[str]:
    return {
        w for w in re.split(r"[\s,/|–\-]+", text.lower())
        if len(w) > 2 and w not in _STOP
    }


def _pg_literal(value: str) -> str:
    """Safely quote a string for embedding in a SQL literal."""
    return "'" + value.replace("'", "''") + "'"


# ─────────────────────────────────────────────────────────────────────────────
# Indexed candidate pre-fetch  (v2.5 — FTS arm replaced with ILIKE)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_candidates_for_jd(db, jd: dict, limit: int = 500) -> list[dict]:
    """
    Three-arm pre-filter using Migration 006 indexes.

    Arm 1 – GIN JSONB overlap on skills
    Arm 2 – ILIKE keyword scan on current_position / title / profile_summary
             Replaces the broken plainto_tsquery | plainto_tsquery FTS arm.
             Benefits over FTS here:
               • No Postgres operator error
               • Handles multi-word skills ("machine learning", "S4 HANA")
               • Handles special chars (C++, Node.js, .NET)
               • Wide net — precise scoring happens in Python afterward
    Arm 3 – B-tree range scan on total_experience

    Falls back to experience-range only if the combined query fails.
    """
    req_skills  = _normalise_skills(jd.get("skillset_required"))
    nice_skills = _normalise_skills(jd.get("skillset_good_to_have"))
    role_name   = (jd.get("role_name") or "").strip()

    try:
        exp_min = max(0.0, float(jd.get("experience_min") or 0) - 2)
        exp_max = float(jd.get("experience_max") or 99) + 2
    except (TypeError, ValueError):
        exp_min, exp_max = 0.0, 99.0

    all_skills = list(dict.fromkeys(req_skills + nice_skills))
    params: list = []

    # ── Arm 1: JSONB @> overlap (GIN index) ──────────────────────────────────
    skill_clauses: list[str] = []
    for sk in all_skills[:10]:
        params.append(json.dumps([sk]))
        skill_clauses.append(f"skills @> ${len(params)}::jsonb")

    skill_arm = (
        "(" + " OR ".join(skill_clauses) + ")"
        if skill_clauses else "FALSE"
    )

    # ── Arm 2: ILIKE keyword scan ─────────────────────────────────────────────
    # Take up to 4 role tokens + top 4 skills = max 6 unique terms.
    # Each term gets a single $N param; the same param index is reused
    # across the three columns via a subquery alias trick — BUT Postgres
    # does not allow the same $N in multiple positions without repeating it,
    # so we append a separate param per column per term.
    keyword_terms = list(dict.fromkeys(
        list(_tokenise(role_name))[:4] + all_skills[:4]
    ))[:6]

    ilike_clauses: list[str] = []
    for term in keyword_terms:
        # One param per column (Postgres requires distinct $N per reference)
        params.append(f"%{term}%")
        p1 = len(params)
        params.append(f"%{term}%")
        p2 = len(params)
        params.append(f"%{term}%")
        p3 = len(params)
        ilike_clauses.append(
            f"(current_position ILIKE ${p1} "
            f"OR title ILIKE ${p2} "
            f"OR profile_summary ILIKE ${p3})"
        )

    keyword_arm = (
        "(" + " OR ".join(ilike_clauses) + ")"
        if ilike_clauses else "FALSE"
    )

    # ── Arm 3: Experience range (B-tree index) ────────────────────────────────
    params.append(exp_min)
    params.append(exp_max)
    exp_arm = (
        f"(total_experience BETWEEN ${len(params) - 1} AND ${len(params)})"
    )

    sql = f"""
        SELECT DISTINCT
            {_CANDIDATE_SELECT}
        FROM candidates
        WHERE
            is_active = TRUE
            AND (
                {skill_arm}
                OR {keyword_arm}
                OR {exp_arm}
            )
        ORDER BY total_experience DESC NULLS LAST
        LIMIT {int(limit)};
    """

    try:
        return execute_raw(db, sql, params)
    except Exception as e:
        st.warning(
            f"⚠️ Pre-filter query failed ({e}), "
            "falling back to experience-range filter."
        )
        fallback_sql = f"""
            SELECT {_CANDIDATE_SELECT}
            FROM candidates
            WHERE
                is_active = TRUE
                AND total_experience BETWEEN $1 AND $2
            ORDER BY total_experience DESC NULLS LAST
            LIMIT {int(limit)};
        """
        try:
            return execute_raw(db, fallback_sql, [exp_min, exp_max])
        except Exception as e2:
            st.error(f"Fallback query also failed: {e2}")
            return []


# ─────────────────────────────────────────────────────────────────────────────
# Scorer  (unchanged from v2.4)
# ─────────────────────────────────────────────────────────────────────────────

def _match_candidates(db, jd: dict) -> list[dict]:
    """
    Score relevant candidates against a JD.

    Scoring matrix
    ┌─────────────────────────────────────────────────────┬────────┐
    │ Signal                                              │ Points │
    ├─────────────────────────────────────────────────────┼────────┤
    │ Role exact phrase in current_position / title       │  +30   │
    │ Role token overlap in position / title              │ up +25 │
    │ Role phrase in profile_summary                      │  +15   │
    │ Role token overlap in profile_summary               │ up +12 │
    │ Required skill in candidate skills (JSONB)          │  +40   │
    │ Required skill in profile_summary                   │  +20   │
    │ Nice-to-have skill in candidate skills              │  +15   │
    │ Nice-to-have skill in profile_summary               │  +10   │
    │ Experience within [exp_min, exp_max]                │  +20   │
    │ Experience within ±2 yrs of range                   │  +10   │
    │ Location city match (case-insensitive)              │  +15   │
    │ Location state / region match                       │   +5   │
    └─────────────────────────────────────────────────────┴────────┘
    """
    req_skills  = jd.get("skillset_required")     or []
    nice_skills = jd.get("skillset_good_to_have") or []
    jd_role     = (jd.get("role_name")  or "").strip().lower()
    jd_location = (jd.get("location")   or "").strip().lower()

    try:
        exp_min = float(jd.get("experience_min") or 0)
        exp_max = float(jd.get("experience_max") or 99)
    except (TypeError, ValueError):
        exp_min, exp_max = 0.0, 99.0

    role_tokens = _tokenise(jd_role) if jd_role else set()

    loc_tokens    = [
        t.strip() for t in re.split(r"[,/|]+", jd_location) if t.strip()
    ]
    loc_primary   = loc_tokens[0]  if loc_tokens      else ""
    loc_secondary = loc_tokens[1:] if len(loc_tokens) > 1 else []

    candidates = _fetch_candidates_for_jd(db, jd, limit=500)
    if not candidates:
        return []

    scored: list[dict] = []

    for c in candidates:
        score         = 0
        matched_req  : list[str] = []
        matched_nice : list[str] = []
        match_signals: list[str] = []

        designation = " ".join(filter(None, [
            (c.get("current_position") or "").lower(),
            (c.get("title")            or "").lower(),
        ]))
        summary     = (c.get("profile_summary") or "").lower()
        cand_skills = set(_normalise_skills(c.get("skills")))
        cand_loc    = (c.get("location") or "").lower()

        # ── 1. Role matching ──────────────────────────────────────────────────
        if role_tokens:
            if jd_role and jd_role in designation:
                score += 30
                match_signals.append("Role exact match in position (+30)")
            else:
                des_tok = _tokenise(designation)
                overlap = role_tokens & des_tok
                if overlap:
                    pts = min(25, len(overlap) * 8)
                    score += pts
                    match_signals.append(
                        f"Role tokens {overlap} in position (+{pts})"
                    )

            if jd_role and _skill_in_text(jd_role, summary):
                score += 15
                match_signals.append("Role phrase in summary (+15)")
            else:
                sum_tok     = _tokenise(summary)
                sum_overlap = role_tokens & sum_tok
                if sum_overlap:
                    pts = min(12, len(sum_overlap) * 4)
                    score += pts
                    match_signals.append(
                        f"Role tokens {sum_overlap} in summary (+{pts})"
                    )

        # ── 2. Skills matching ────────────────────────────────────────────────
        for skill in req_skills:
            sl = skill.lower().strip()
            if sl in cand_skills:
                score += 40
                matched_req.append(skill)
                match_signals.append(f"Req skill '{skill}' in skills (+40)")
            elif _skill_in_text(sl, summary):
                score += 20
                matched_req.append(f"{skill}*")
                match_signals.append(f"Req skill '{skill}' in summary (+20)")

        for skill in nice_skills:
            sl = skill.lower().strip()
            if sl in cand_skills:
                score += 15
                matched_nice.append(skill)
                match_signals.append(f"Nice skill '{skill}' in skills (+15)")
            elif _skill_in_text(sl, summary):
                score += 10
                matched_nice.append(f"{skill}*")
                match_signals.append(f"Nice skill '{skill}' in summary (+10)")

        # ── 3. Experience matching ────────────────────────────────────────────
        try:
            exp = float(c.get("total_experience") or 0)
            if exp_min <= exp <= exp_max:
                score += 20
                match_signals.append(
                    f"Exp {exp}y within [{exp_min}-{exp_max}]y (+20)"
                )
            elif (exp_min - 2) <= exp <= (exp_max + 2):
                score += 10
                match_signals.append(f"Exp {exp}y within ±2y of range (+10)")
        except (TypeError, ValueError):
            exp = 0.0

        # ── 4. Location matching ──────────────────────────────────────────────
        if loc_primary and cand_loc:
            if loc_primary in cand_loc or cand_loc in loc_primary:
                score += 15
                match_signals.append(f"City match '{loc_primary}' (+15)")
            else:
                for sec in loc_secondary:
                    if sec and (sec in cand_loc or cand_loc in sec):
                        score += 5
                        match_signals.append(f"Region match '{sec}' (+5)")
                        break

        if score > 0:
            scored.append({
                **c,
                "_score":        score,
                "_matched_req":  matched_req,
                "_matched_nice": matched_nice,
                "_signals":      match_signals,
            })

    scored.sort(key=lambda x: x["_score"], reverse=True)
    return scored


# ─────────────────────────────────────────────────────────────────────────────
# Page entry point
# ─────────────────────────────────────────────────────────────────────────────

def render_jd_manager(services: dict):
    page_header(
        "JD Manager",
        "Parse and manage job descriptions with AI",
        "📋",
    )

    db        = services["db"]
    jd_parser = services["jd_parser"]

    for key, default in [
        ("jd_parsed_data",   None),
        ("jd_parse_source",  None),
        ("jd_match_results", None),
        ("jd_match_jd_id",   None),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    tabs = st.tabs(["📋 All JDs", "➕ New JD", "📄 Upload JD File"])

    # ─────────────────────────────────────────────────────────────────────────
    # Tab 1 – All JDs
    # ─────────────────────────────────────────────────────────────────────────
    with tabs[0]:
        col_f, col_btn = st.columns([3, 1])
        with col_f:
            jd_status_filter = st.selectbox(
                "Filter by status",
                ["All", "Active", "On Hold", "Closed"],
            )
        with col_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄 Refresh", use_container_width=True):
                st.session_state.jd_match_results = None
                st.session_state.jd_match_jd_id   = None
                st.rerun()

        try:
            jds = db.get_all_jds(
                status=None if jd_status_filter == "All"
                       else jd_status_filter
            )
        except Exception as e:
            st.error(f"Could not load JDs: {e}")
            jds = []

        if not jds:
            st.markdown(
                "<div class='riq-info-box'>"
                "No JDs found. Create one using the <b>New JD</b> tab.</div>",
                unsafe_allow_html=True,
            )
        else:
            for jd in jds:
                req_skills = jd.get("skillset_required") or []
                if isinstance(req_skills, str):
                    try:
                        req_skills = json.loads(req_skills)
                    except Exception:
                        req_skills = []

                jd_label = (
                    f"📋 {jd.get('jd_code', '—')} | "
                    f"{jd.get('role_name', 'Unknown')} | "
                    f"{jd.get('location', '—')} | "
                    f"{jd.get('experience_min', 0)}-"
                    f"{jd.get('experience_max', 0)}y"
                )

                with st.expander(jd_label):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.markdown(
                            f"**Role:** {jd.get('role_name', '')}<br>"
                            f"**Client:** {jd.get('client_name', 'TBD')}<br>"
                            f"**Location:** {jd.get('location', '')}<br>"
                            f"**Work Mode:** {jd.get('work_mode', '')}",
                            unsafe_allow_html=True,
                        )
                    with c2:
                        st.markdown(
                            f"**Experience:** "
                            f"{jd.get('experience_min', 0)}-"
                            f"{jd.get('experience_max', 0)} years<br>"
                            f"**Budget:** "
                            f"{jd.get('budget_min', 0)}-"
                            f"{jd.get('budget_max', 0)} "
                            f"{jd.get('budget_currency', 'LPA')}<br>"
                            f"**Notice Period Max:** "
                            f"{jd.get('notice_period_max', '—')}<br>"
                            f"**Positions:** {jd.get('positions_count', 1)}",
                            unsafe_allow_html=True,
                        )
                    with c3:
                        st.markdown(
                            f"**Priority:** {jd.get('priority', 'Medium')}<br>"
                            f"**Status:** {jd.get('status', 'Active')}<br>"
                            f"**Recruiter:** "
                            f"{jd.get('recruiter_assigned', 'Unassigned')}",
                            unsafe_allow_html=True,
                        )

                    if req_skills:
                        st.markdown("**Required Skills:**")
                        st.markdown(
                            skill_pills(req_skills),
                            unsafe_allow_html=True,
                        )

                    col_m, col_s, _ = st.columns([1, 1, 3])
                    with col_m:
                        if st.button(
                            "🎯 Match Candidates",
                            key=f"match_{jd['id']}",
                            type="primary",
                        ):
                            with st.spinner("Scoring candidates…"):
                                results = _match_candidates(db, jd)
                            st.session_state.jd_match_results = results
                            st.session_state.jd_match_jd_id   = jd["id"]
                            st.rerun()

                    with col_s:
                        current_status  = (jd.get("status") or "Active")
                        status_opts     = ["Active", "On Hold", "Closed"]
                        current_display = current_status.title()
                        new_status = st.selectbox(
                            "Status",
                            status_opts,
                            index=status_opts.index(current_display)
                                  if current_display in status_opts else 0,
                            key=f"status_{jd['id']}",
                        )
                        if new_status.lower() != current_status.lower():
                            try:
                                db.update_jd_status(jd["id"], new_status)
                                st.success(f"Status → {new_status}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Update failed: {e}")

                    if (
                        st.session_state.jd_match_jd_id == jd["id"]
                        and st.session_state.jd_match_results is not None
                    ):
                        _render_match_results(
                            st.session_state.jd_match_results, jd
                        )

    # ─────────────────────────────────────────────────────────────────────────
    # Tab 2 – New JD
    # ─────────────────────────────────────────────────────────────────────────
    with tabs[1]:
        st.markdown(
            "<div class='riq-section-title'>Create New Job Description</div>",
            unsafe_allow_html=True,
        )

        with st.form("new_jd_form"):
            jd_text = st.text_area(
                "Paste Job Description",
                height=250,
                placeholder=(
                    "Paste the full job description here. "
                    "RecruitIQ AI will auto-extract all parameters…"
                ),
            )
            col_a, col_b = st.columns(2)
            with col_a:
                client_name = st.text_input("Client / Company Name")
                recruiter   = st.text_input("Recruiter Assigned")
                priority    = st.selectbox("Priority", ["High", "Medium", "Low"])
            with col_b:
                positions = st.number_input("Open Positions", 1, 100, 1)
                st.date_input("Application Deadline")

            parse_btn = st.form_submit_button(
                "🧠 Parse with AI",
                type="primary",
                use_container_width=True,
            )

        if parse_btn and jd_text:
            with st.spinner("🧠 RecruitIQ AI is parsing your JD…"):
                parsed = jd_parser.parse(jd_text)
                if client_name:
                    parsed["client_name"] = client_name
                parsed["recruiter_assigned"] = recruiter
                parsed["priority"]           = priority
                parsed["positions_count"]    = positions
            st.session_state.jd_parsed_data  = parsed
            st.session_state.jd_parse_source = "text"

        if (
            st.session_state.jd_parsed_data is not None
            and st.session_state.jd_parse_source == "text"
        ):
            _render_jd_review_and_save(
                db,
                st.session_state.jd_parsed_data,
                key_prefix="jdt",
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Tab 3 – Upload file
    # ─────────────────────────────────────────────────────────────────────────
    with tabs[2]:
        st.markdown(
            "<div class='riq-section-title'>"
            "Upload JD File (PDF / DOCX / TXT)</div>",
            unsafe_allow_html=True,
        )

        jd_file = st.file_uploader(
            "Upload JD Document",
            type=["pdf", "docx", "txt"],
            help="Supports PDF, Word (.docx), and plain text",
        )

        if jd_file:
            file_bytes = jd_file.read()
            with st.spinner("Extracting text…"):
                jd_text_extracted = jd_parser.extract_text_from_file(
                    file_bytes, jd_file.name
                )

            if not jd_text_extracted:
                st.error("Could not extract text from file.")
            else:
                st.markdown(
                    f"<div class='riq-success-box'>"
                    f"✅ Extracted {len(jd_text_extracted):,} characters "
                    f"from <b>{jd_file.name}</b></div>",
                    unsafe_allow_html=True,
                )
                with st.expander("View extracted text"):
                    st.text(
                        jd_text_extracted[:2000]
                        + ("…" if len(jd_text_extracted) > 2000 else "")
                    )

                if st.button(
                    "🧠 Parse with AI",
                    type="primary",
                    use_container_width=True,
                    key="jd_file_parse_btn",
                ):
                    with st.spinner("Parsing with AI…"):
                        parsed = jd_parser.parse(jd_text_extracted)
                    st.session_state.jd_parsed_data  = parsed
                    st.session_state.jd_parse_source = "file"
                    st.rerun()

                if (
                    st.session_state.jd_parsed_data is not None
                    and st.session_state.jd_parse_source == "file"
                ):
                    _render_jd_review_and_save(
                        db,
                        st.session_state.jd_parsed_data,
                        key_prefix="jdf",
                    )


# ─────────────────────────────────────────────────────────────────────────────
# Sub-renderer: JD review + save form
# ─────────────────────────────────────────────────────────────────────────────

def _render_jd_review_and_save(db, parsed: dict, key_prefix: str):
    st.markdown(
        "<div class='riq-success-box'>"
        "✅ JD parsed! Review the fields below then click Save.</div>",
        unsafe_allow_html=True,
    )

    with st.expander("📝 Review Parsed JD", expanded=True):
        ec1, ec2 = st.columns(2)
        with ec1:
            parsed["role_name"] = st.text_input(
                "Role Title",
                parsed.get("role_name", ""),
                key=f"{key_prefix}_role",
            )
            parsed["location"] = st.text_input(
                "Location",
                parsed.get("location", ""),
                key=f"{key_prefix}_location",
            )
            wm_opts = ["Remote", "Hybrid", "On-site"]
            parsed["work_mode"] = st.selectbox(
                "Work Mode",
                wm_opts,
                index=(
                    wm_opts.index(parsed.get("work_mode", "Hybrid"))
                    if parsed.get("work_mode") in wm_opts else 1
                ),
                key=f"{key_prefix}_workmode",
            )
            parsed["client_name"] = st.text_input(
                "Client / Company",
                parsed.get("client_name", ""),
                key=f"{key_prefix}_client",
            )

        with ec2:
            parsed["experience_min"] = st.number_input(
                "Exp Min (yrs)",
                min_value=0.0, max_value=50.0,
                step=0.5, format="%.1f",
                value=float(parsed.get("experience_min") or 0),
                key=f"{key_prefix}_exp_min",
            )
            parsed["experience_max"] = st.number_input(
                "Exp Max (yrs)",
                min_value=0.0, max_value=50.0,
                step=0.5, format="%.1f",
                value=float(parsed.get("experience_max") or 5),
                key=f"{key_prefix}_exp_max",
            )
            parsed["budget_min"] = st.number_input(
                "Budget Min (LPA)",
                min_value=0.0, step=0.5, format="%.1f",
                value=float(parsed.get("budget_min") or 0),
                key=f"{key_prefix}_bud_min",
            )
            parsed["budget_max"] = st.number_input(
                "Budget Max (LPA)",
                min_value=0.0, step=0.5, format="%.1f",
                value=float(parsed.get("budget_max") or 0),
                key=f"{key_prefix}_bud_max",
            )

        req_s_edit = st.text_input(
            "Required Skills (comma-separated)",
            ", ".join(parsed.get("skillset_required") or []),
            key=f"{key_prefix}_req_skills",
        )
        nice_s_edit = st.text_input(
            "Nice-to-have Skills (comma-separated)",
            ", ".join(parsed.get("skillset_good_to_have") or []),
            key=f"{key_prefix}_nice_skills",
        )
        parsed["skillset_required"] = [
            s.strip() for s in req_s_edit.split(",") if s.strip()
        ]
        parsed["skillset_good_to_have"] = [
            s.strip() for s in nice_s_edit.split(",") if s.strip()
        ]

        if st.button(
            "💾 Save JD",
            type="primary",
            use_container_width=True,
            key=f"{key_prefix}_save_btn",
        ):
            try:
                jd_id = db.save_jd(parsed)
                st.success(
                    f"✅ JD saved! Code: {parsed.get('jd_code', jd_id)}"
                )
                st.balloons()
                st.session_state.jd_parsed_data  = None
                st.session_state.jd_parse_source = None
                st.rerun()
            except Exception as e:
                st.error(f"Save failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Sub-renderer: match results
# ─────────────────────────────────────────────────────────────────────────────

def _render_match_results(results: list[dict], jd: dict):
    """Render scored candidate cards with full signal breakdown."""

    st.markdown("---")
    st.markdown(
        f"### 🎯 Matching Candidates for "
        f"**{jd.get('role_name', 'this JD')}**"
    )

    if not results:
        st.info(
            "No matching candidates found. "
            "Try broadening the required skills or experience range."
        )
        return

    try:
        exp_min = float(jd.get("experience_min") or 0)
        exp_max = float(jd.get("experience_max") or 99)
    except (TypeError, ValueError):
        exp_min, exp_max = 0.0, 99.0

    with st.expander("ℹ️ How scores are calculated", expanded=False):
        st.markdown("""
| Signal | Points |
|---|---|
| Role exact match in current_position / title | +30 |
| Role token overlap in position / title | up to +25 |
| Role phrase in profile_summary | +15 |
| Role token overlap in profile_summary | up to +12 |
| Required skill in candidate skills (JSONB) | +40 each |
| Required skill in profile_summary | +20 each |
| Nice-to-have skill in candidate skills | +15 each |
| Nice-to-have skill in profile_summary | +10 each |
| Experience within JD range | +20 |
| Experience within ±2 yrs of range | +10 |
| City / location match | +15 |
| Region / state match | +5 |
        """)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Matches",    len(results))
    m2.metric("🟢 Strong (≥80)",  sum(1 for r in results if r["_score"] >= 80))
    m3.metric("🟡 Good (50–79)",  sum(1 for r in results if 50 <= r["_score"] < 80))
    m4.metric("🔴 Partial (<50)", sum(1 for r in results if r["_score"] < 50))

    max_score = results[0]["_score"] if results else 100
    min_show  = st.slider(
        "Minimum score to display",
        min_value=0,
        max_value=int(max_score),
        value=max(0, int(max_score * 0.25)),
        step=5,
        key=f"match_slider_{jd['id']}",
    )
    filtered = [r for r in results if r["_score"] >= min_show]

    st.caption(
        f"Showing {len(filtered)} candidate(s) with score ≥ {min_show} "
        f"(out of {len(results)} total matches)"
    )

    for idx, cand in enumerate(filtered, 1):
        score        = cand["_score"]
        matched_req  = cand.get("_matched_req",  [])
        matched_nice = cand.get("_matched_nice", [])
        signals      = cand.get("_signals",      [])

        cand_name  = _get_name(cand)
        cand_title = _get_title(cand)

        if score >= 80:
            band, colour = "🟢 Strong",  "#1a7a4a"
        elif score >= 50:
            band, colour = "🟡 Good",    "#8a6d00"
        else:
            band, colour = "🔴 Partial", "#9b2335"

        try:
            exp    = float(cand.get("total_experience") or 0)
            exp_ok = exp_min <= exp <= exp_max
        except (TypeError, ValueError):
            exp, exp_ok = 0.0, False

        exp_badge = (
            f"<span style='color:#1a7a4a;font-weight:600'>"
            f"{int(exp)}y ✓</span>"
            if exp_ok else
            f"<span style='color:#9b2335;font-weight:600'>"
            f"{int(exp)}y ✗</span>"
        )

        with st.expander(
            f"#{idx}  {cand_name}  │  {cand_title}  │  "
            f"Score: {score}  {band}"
        ):
            d1, d2, d3 = st.columns(3)

            with d1:
                st.markdown(
                    f"**Name:** {cand_name}<br>"
                    f"**Position:** {cand_title}<br>"
                    f"**Company:** {cand.get('current_company', '—')}<br>"
                    f"**Location:** {cand.get('location', '—')}",
                    unsafe_allow_html=True,
                )
            with d2:
                st.markdown(
                    f"**Experience:** {exp_badge}<br>"
                    f"**Notice Period:** {cand.get('notice_period', '—')}<br>"
                    f"**Work Mode Pref:** {cand.get('work_mode_pref', '—')}<br>"
                    f"**Phone:** {cand.get('phone_number', '—')}",
                    unsafe_allow_html=True,
                )
            with d3:
                st.markdown(
                    f"**Match Score:** "
                    f"<span style='color:{colour};"
                    f"font-weight:700'>{score}</span><br>"
                    f"**Grade:** {band}<br>"
                    f"**Email:** {cand.get('email_address', '—')}<br>"
                    f"**Remarks:** {cand.get('remarks') or '—'}",
                    unsafe_allow_html=True,
                )

            if matched_req:
                pills = " ".join(
                    f"<span style='background:#1a7a4a;color:#fff;"
                    f"padding:2px 8px;border-radius:12px;"
                    f"font-size:0.78rem;margin:2px;'>"
                    f"{'⭐' if '*' not in s else '📄'} "
                    f"{s.replace('*', '')}</span>"
                    for s in matched_req
                )
                st.markdown(
                    f"**✅ Required Skills Matched:** {pills}<br>"
                    f"<span style='font-size:0.75rem;color:#666;'>"
                    f"⭐ = in skills list &nbsp;&nbsp; "
                    f"📄 = in profile summary</span>",
                    unsafe_allow_html=True,
                )

            if matched_nice:
                pills = " ".join(
                    f"<span style='background:#1a6fa8;color:#fff;"
                    f"padding:2px 8px;border-radius:12px;"
                    f"font-size:0.78rem;margin:2px;'>"
                    f"{s.replace('*', '')}</span>"
                    for s in matched_nice
                )
                st.markdown(
                    f"**💡 Nice-to-have Matched:** {pills}",
                    unsafe_allow_html=True,
                )

            if signals:
                with st.expander("📊 Score breakdown"):
                    for sig in signals:
                        st.markdown(f"- {sig}")

            summary_raw = (cand.get("profile_summary") or "").strip()
            if summary_raw:
                highlighted  = summary_raw
                jd_role_name = (jd.get("role_name") or "").strip()
                all_highlight = (
                    [jd_role_name] if jd_role_name else []
                ) + (jd.get("skillset_required") or []) + (
                    jd.get("skillset_good_to_have") or []
                )
                for term in all_highlight:
                    if term:
                        highlighted = re.sub(
                            rf"(?i)({re.escape(term)})",
                            r"**\1**",
                            highlighted,
                        )
                with st.expander("📄 Profile Summary"):
                    st.markdown(
                        highlighted[:1000]
                        + ("…" if len(highlighted) > 1000 else "")
                    )

            if st.button(
                "👤 View Full Profile",
                key=f"view_{jd['id']}_{cand['id']}_{idx}",
            ):
                st.session_state.selected_candidate_id = cand["id"]
                st.session_state["nav_page"] = "👥 Profiles"
                st.rerun()
