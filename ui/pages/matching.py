# ui/pages/matching.py
"""
RecruitIQ – Candidate Matching Engine (v2.7)

Changes from v2.6-debug:
  - FTS arm: replaced broken  tsquery | tsquery  with a single
    to_tsquery('english', 'term1 | term2 | ...')  — fixes
    "operator does not exist: tsquery | tsquery" Postgres error.
  - Step 2 auto-population: prefill values are written to
    st.session_state BEFORE widgets render so Streamlit honours them.
  - Debug expander removed (root cause was confirmed).
  - jd_label_map built before selectbox (stale-key fix from v2.6).
  - index=0 on selectbox prevents stale persisted widget state.
"""

from __future__ import annotations

import io
import json
import re
from typing import Any, Optional

import streamlit as st

from database.query_runner import execute_raw
from ui.styles import page_header, skill_pills


# ── Nav key — must match app.py ───────────────────────────────────────────────
_NAV_KEY = "nav_page"

# Status values (lowercased) that mean "show this JD in the dropdown"
_ACTIVE_STATUSES: set[str] = {"active", "open", "published", "live"}


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
# JD loader
# ─────────────────────────────────────────────────────────────────────────────

def _load_active_jds(db) -> list[dict]:
    """
    Fetch JDs whose status (case-insensitive) is in _ACTIVE_STATUSES.
    No DB writes — all filtering in Python.
    """
    try:
        if hasattr(db, "client"):
            resp = (
                db.client.table("job_descriptions")
                .select(
                    "id, jd_code, role_name, client_name, "
                    "skillset_required, skillset_good_to_have, "
                    "location, experience_min, experience_max, "
                    "raw_jd_text, ai_parsed_data, status"
                )
                .order("jd_code")
                .execute()
            )
            rows = resp.data or []
            return [
                r for r in rows
                if (r.get("status") or "").lower() in _ACTIVE_STATUSES
            ]

        rows = execute_raw(
            db,
            """
            SELECT
                id, jd_code, role_name, client_name,
                skillset_required, skillset_good_to_have,
                location, experience_min, experience_max,
                raw_jd_text, ai_parsed_data, status
            FROM   job_descriptions
            ORDER  BY jd_code
            """,
            [],
        )
        rows = rows or []
        return [
            r for r in rows
            if (r.get("status") or "").lower() in _ACTIVE_STATUSES
        ]

    except Exception as exc:
        st.warning(f"Could not load JDs from database: {exc}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# JD prefill builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_prefill(jd: dict) -> dict:
    """
    Convert a job_descriptions row into the dict used to pre-fill the form.
    Handles JSONB lists, JSON strings, and CSV strings for skill fields.
    Merges ai_parsed_data when the direct columns are empty.
    """
    def _to_list(val: Any) -> list[str]:
        if isinstance(val, list):
            return [str(v).strip() for v in val if v]
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if v]
            except (json.JSONDecodeError, ValueError):
                pass
            return [s.strip() for s in val.split(",") if s.strip()]
        return []

    req  = _to_list(jd.get("skillset_required")    or [])
    nice = _to_list(jd.get("skillset_good_to_have") or [])

    ai_raw = jd.get("ai_parsed_data") or {}
    if isinstance(ai_raw, str):
        try:
            ai_raw = json.loads(ai_raw)
        except Exception:
            ai_raw = {}

    if not req:
        req  = _to_list(ai_raw.get("required_skills")     or [])
    if not nice:
        nice = _to_list(ai_raw.get("good_to_have_skills") or [])

    try:
        exp_min = float(jd.get("experience_min") or 0)
        exp_max = float(jd.get("experience_max") or 10)
    except (TypeError, ValueError):
        exp_min, exp_max = 0.0, 10.0

    return {
        "role"    : (jd.get("role_name") or "").strip(),
        "req"     : req,
        "nice"    : nice,
        "exp_min" : exp_min,
        "exp_max" : exp_max,
        "location": (jd.get("location") or "").strip(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Shared utilities
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
    """Wrap a string as a safe single-quoted Postgres literal."""
    return "'" + value.replace("'", "''") + "'"


# ─────────────────────────────────────────────────────────────────────────────
# Indexed pre-fetch
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_candidates_for_jd(db, jd: dict, limit: int = 500) -> list[dict]:
    """
    Three-arm pre-filter using Migration 006 indexes.

    Arm 1 – GIN JSONB overlap on skills
    Arm 2 – GIN FTS on current_position | title | profile_summary
             Uses a single  to_tsquery('english', 'a | b | c')
             to avoid the  tsquery | tsquery  operator error.
    Arm 3 – B-tree range scan on total_experience
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

    # ── Arm 1: JSONB @> overlap ───────────────────────────────────────────────
    skill_clauses: list[str] = []
    for sk in all_skills[:10]:
        params.append(json.dumps([sk]))
        skill_clauses.append(f"skills @> ${len(params)}::jsonb")
    skill_arm = (
        "(" + " OR ".join(skill_clauses) + ")"
        if skill_clauses else "FALSE"
    )

    # ── Arm 2: FTS — single to_tsquery with | inside the string ──────────────
    # Collect up to 8 single-word tokens; skip multi-word phrases because
    # to_tsquery does NOT accept spaces — use plainto_tsquery per phrase instead.
    fts_tokens = list(dict.fromkeys(
        list(_tokenise(role_name))[:4] + all_skills[:4]
    ))[:8]

    # Keep only single-word tokens safe for to_tsquery
    safe_tokens = [t for t in fts_tokens if re.match(r'^[a-z0-9]+$', t)]

    if safe_tokens:
        # Build:  to_tsquery('english', 'word1 | word2 | word3')
        tsquery_str = _pg_literal(" | ".join(safe_tokens))
        fts_arm = f"""(
            to_tsvector('english',
                coalesce(current_position, '') || ' ' ||
                coalesce(title,            '') || ' ' ||
                coalesce(profile_summary,  '')
            ) @@ to_tsquery('english', {tsquery_str})
        )"""
    else:
        fts_arm = "FALSE"

    # ── Arm 3: Experience range ───────────────────────────────────────────────
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
                OR {fts_arm}
                OR {exp_arm}
            )
        ORDER BY total_experience DESC NULLS LAST
        LIMIT {int(limit)};
    """

    try:
        return execute_raw(db, sql, params)
    except Exception as e:
        st.warning(
            f"⚠️ Pre-filter failed ({e}), "
            "falling back to experience-range filter."
        )
        fallback = f"""
            SELECT {_CANDIDATE_SELECT}
            FROM candidates
            WHERE
                is_active = TRUE
                AND total_experience BETWEEN $1 AND $2
            ORDER BY total_experience DESC NULLS LAST
            LIMIT {int(limit)};
        """
        try:
            return execute_raw(db, fallback, [exp_min, exp_max])
        except Exception as e2:
            st.error(f"Fallback query also failed: {e2}")
            return []


# ─────────────────────────────────────────────────────────────────────────────
# Core scorer
# ─────────────────────────────────────────────────────────────────────────────

def score_candidates(
    db,
    role_name: str    = "",
    req_skills: list  = None,
    nice_skills: list = None,
    exp_min: float    = 0.0,
    exp_max: float    = 99.0,
    location: str     = "",
    limit: int        = 500,
) -> list[dict]:
    """Score candidates against a role spec. Returns list sorted by _score desc."""
    req_skills  = req_skills  or []
    nice_skills = nice_skills or []

    jd_proxy = {
        "role_name":             role_name,
        "skillset_required":     req_skills,
        "skillset_good_to_have": nice_skills,
        "experience_min":        exp_min,
        "experience_max":        exp_max,
        "location":              location,
    }

    jd_role     = role_name.strip().lower()
    jd_location = location.strip().lower()
    role_tokens = _tokenise(jd_role) if jd_role else set()

    loc_tokens    = [t.strip() for t in re.split(r"[,/|]+", jd_location) if t.strip()]
    loc_primary   = loc_tokens[0]  if loc_tokens      else ""
    loc_secondary = loc_tokens[1:] if len(loc_tokens) > 1 else []

    candidates = _fetch_candidates_for_jd(db, jd_proxy, limit=limit)
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

        # ── 1. Role ───────────────────────────────────────────────────────────
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

        # ── 2. Skills ─────────────────────────────────────────────────────────
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

        # ── 3. Experience ─────────────────────────────────────────────────────
        try:
            exp = float(c.get("total_experience") or 0)
            if exp_min <= exp <= exp_max:
                score += 20
                match_signals.append(
                    f"Exp {exp}y within [{exp_min}-{exp_max}]y (+20)"
                )
            elif (exp_min - 2) <= exp <= (exp_max + 2):
                score += 10
                match_signals.append(
                    f"Exp {exp}y within ±2y of range (+10)"
                )
        except (TypeError, ValueError):
            exp = 0.0

        # ── 4. Location ───────────────────────────────────────────────────────
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
# AI summary helper
# ─────────────────────────────────────────────────────────────────────────────

def _ai_summary(
    llm, cand: dict, role: str,
    matched_req: list, matched_nice: list,
) -> str:
    """2-sentence recruiter narrative. Returns '' on any failure."""
    if not llm:
        return ""
    try:
        name  = _get_name(cand)
        score = cand.get("_score", 0)
        exp   = cand.get("total_experience", "?")
        loc   = cand.get("location", "?")
        grade = (
            "strong"   if score >= 80 else
            "moderate" if score >= 50 else
            "partial"
        )
        prompt = (
            f"You are a senior recruiter. Write exactly 2 concise sentences "
            f"explaining why {name} (score {score}/100) is a {grade} fit for "
            f"the role of {role}. "
            f"Matched skills: {', '.join(matched_req + matched_nice) or 'none'}. "
            f"Experience: {exp} years. Location: {loc}. "
            f"Be specific and professional. Do not use bullet points."
        )
        return llm.generate(prompt) or ""
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Session-state helpers for form prefill
# ─────────────────────────────────────────────────────────────────────────────

# Keys used by Step 2 widgets
_FORM_KEYS = {
    "match_role":        "role",
    "match_req_skills":  "req",     # stored as comma string
    "match_nice_skills": "nice",    # stored as comma string
    "match_location":    "location",
    "match_exp_min":     "exp_min",
    "match_exp_max":     "exp_max",
}


def _write_prefill_to_session(pf: dict) -> None:
    """
    Write prefill values into st.session_state under each widget key
    BEFORE the widgets are rendered.  Streamlit reads session_state
    first, so this is the only reliable way to set widget values after
    the first render.
    """
    st.session_state["match_role"]        = pf["role"]
    st.session_state["match_req_skills"]  = ", ".join(pf["req"])
    st.session_state["match_nice_skills"] = ", ".join(pf["nice"])
    st.session_state["match_location"]    = pf["location"]
    st.session_state["match_exp_min"]     = float(pf["exp_min"])
    st.session_state["match_exp_max"]     = float(pf["exp_max"])


# ─────────────────────────────────────────────────────────────────────────────
# Page render
# ─────────────────────────────────────────────────────────────────────────────

def render_matching(services: dict) -> None:
    """
    JD Matching page — v2.7

    Flow
    ────
    1. JD dropdown auto-populated from job_descriptions (active only).
    2. Selecting a JD writes prefill values into session_state so that
       Step 2 widgets show the JD data immediately — and the user can
       still edit every field before running the search.
    3. Results: scored cards + AI narrative + Excel export.
    """
    db  = services.get("db")
    llm = services.get("llm")

    page_header(
        "Candidate Matching",
        "Find the best candidates for any role",
        "🎯",
    )

    # ── Step 1 — JD Selector ─────────────────────────────────────────────────
    st.markdown("### 📋 Step 1 — Select a Job Description")

    jds = _load_active_jds(db)

    # Build label map BEFORE the selectbox so lookups always use current data.
    jd_label_map: dict[str, dict] = {}
    for j in jds:
        label = (
            f"[{j['jd_code']}]  {j['role_name']}"
            + (f"  ·  {j['client_name']}" if j.get("client_name") else "")
        )
        jd_label_map[label] = j

    options = ["— Manual entry —"] + list(jd_label_map.keys())

    sel_col, refresh_col = st.columns([6, 1])

    with refresh_col:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄", help="Refresh JD list", key="match_refresh_jds"):
            # Clear prefill so Step 2 goes back to blank
            for k in list(_FORM_KEYS):
                st.session_state.pop(k, None)
            st.session_state.pop("match_last_jd_id", None)
            st.rerun()

    with sel_col:
        if not jds:
            st.info(
                "No active JDs found — using **manual entry** below. "
                "Go to **📄 JD Manager** to create or activate a JD."
            )

        chosen = st.selectbox(
            "Choose a JD to auto-fill the form",
            options,
            index=0,          # default to manual entry; user picks from list
            key="match_jd_selectbox",
            help="Only active JDs appear here. Change status in JD Manager.",
        )

    selected_jd: Optional[dict] = jd_label_map.get(chosen)

    # ── Auto-populate Step 2 when JD changes ─────────────────────────────────
    # Track which JD was last loaded so we only write session_state once
    # (prevents overwriting user edits on every rerun while the same JD
    # is still selected).
    last_jd_id = st.session_state.get("match_last_jd_id")
    current_jd_id = selected_jd["id"] if selected_jd else None

    if current_jd_id != last_jd_id:
        # JD selection changed — write new prefill values
        if selected_jd:
            pf = _build_prefill(selected_jd)
        else:
            pf = {
                "role": "", "req": [], "nice": [],
                "exp_min": 0.0, "exp_max": 10.0, "location": "",
            }
        _write_prefill_to_session(pf)
        st.session_state["match_last_jd_id"] = current_jd_id

    if selected_jd:
        st.success(
            f"✅ Loaded **{selected_jd['role_name']}**"
            + (
                f" · {selected_jd['client_name']}"
                if selected_jd.get("client_name") else ""
            )
            + f" (JD {selected_jd['jd_code']}) — fields pre-filled below"
        )
        if selected_jd.get("raw_jd_text"):
            with st.expander("📄 View full JD text", expanded=False):
                st.markdown(selected_jd["raw_jd_text"])

    st.markdown("---")

    # ── Step 2 — Criteria form ────────────────────────────────────────────────
    st.markdown("### 🔍 Step 2 — Review / Override Criteria")
    st.caption(
        "Fields are pre-filled from the selected JD. "
        "You can edit anything before clicking Find Matches."
    )

    with st.container(border=True):
        col1, col2 = st.columns(2)

        with col1:
            # Use key= only — value comes from session_state written above
            role_name = st.text_input(
                "Role / Job Title",
                placeholder="e.g. Senior Python Developer",
                key="match_role",
            )
            req_skills_raw = st.text_input(
                "Required Skills (comma-separated)",
                placeholder="e.g. Python, Django, PostgreSQL",
                key="match_req_skills",
            )
            location = st.text_input(
                "Preferred Location",
                placeholder="e.g. Bangalore, Mumbai",
                key="match_location",
            )

        with col2:
            nice_skills_raw = st.text_input(
                "Nice-to-have Skills (comma-separated)",
                placeholder="e.g. Docker, AWS, React",
                key="match_nice_skills",
            )

            exp_c1, exp_c2 = st.columns(2)
            with exp_c1:
                exp_min = st.number_input(
                    "Exp Min (yrs)",
                    min_value=0.0, max_value=50.0,
                    step=0.5, format="%.1f",
                    key="match_exp_min",
                )
            with exp_c2:
                exp_max = st.number_input(
                    "Exp Max (yrs)",
                    min_value=0.0, max_value=50.0,
                    step=0.5, format="%.1f",
                    key="match_exp_max",
                )

            result_limit = st.slider(
                "Max candidates to score",
                min_value=50, max_value=500,
                value=200, step=50,
                key="match_limit",
            )

    st.markdown("")
    search_btn = st.button(
        "🎯 Find Matches",
        type="primary",
        use_container_width=True,
        key="match_run_btn",
    )

    if search_btn:
        if not role_name.strip() and not req_skills_raw.strip():
            st.warning("⚠️ Enter at least a Role / Job Title or Required Skills.")
            return

        req_skills  = [s.strip() for s in req_skills_raw.split(",")  if s.strip()]
        nice_skills = [s.strip() for s in nice_skills_raw.split(",") if s.strip()]

        with st.spinner("🔍 Scanning candidates…"):
            results = score_candidates(
                db=db,
                role_name=role_name.strip(),
                req_skills=req_skills,
                nice_skills=nice_skills,
                exp_min=float(exp_min),
                exp_max=float(exp_max),
                location=location.strip(),
                limit=int(result_limit),
            )

        st.session_state["matching_results"]     = results
        st.session_state["matching_role"]        = role_name.strip()
        st.session_state["matching_req_skills"]  = req_skills
        st.session_state["matching_nice_skills"] = nice_skills
        st.session_state["matching_exp_min"]     = float(exp_min)
        st.session_state["matching_exp_max"]     = float(exp_max)
        st.session_state["matching_jd_code"]     = (
            selected_jd["jd_code"] if selected_jd else "manual"
        )

    # ── Results ───────────────────────────────────────────────────────────────
    results = st.session_state.get("matching_results")
    if results is None:
        st.info("Fill in the form above and click **Find Matches** to begin.")
        return

    role      = st.session_state.get("matching_role", "")
    exp_min_r = st.session_state.get("matching_exp_min", 0.0)
    exp_max_r = st.session_state.get("matching_exp_max", 99.0)
    req_s     = st.session_state.get("matching_req_skills",  [])
    nice_s    = st.session_state.get("matching_nice_skills", [])
    jd_code   = st.session_state.get("matching_jd_code", "manual")

    if not results:
        st.info(
            "No candidates matched. "
            "Try broadening skills or expanding the experience range."
        )
        return

    st.markdown("---")
    st.markdown(
        f"### 🎯 Results for **{role or 'your search'}**"
        + (f"  ·  JD `{jd_code}`" if jd_code != "manual" else "")
    )

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
        key="matching_score_slider",
    )
    filtered = [r for r in results if r["_score"] >= min_show]
    st.caption(
        f"Showing {len(filtered)} candidate(s) with score ≥ {min_show} "
        f"(out of {len(results)} total)"
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
            exp_ok = exp_min_r <= exp <= exp_max_r
        except (TypeError, ValueError):
            exp, exp_ok = 0.0, False

        exp_badge = (
            f"<span style='color:#1a7a4a;font-weight:600'>{int(exp)}y ✓</span>"
            if exp_ok else
            f"<span style='color:#9b2335;font-weight:600'>{int(exp)}y ✗</span>"
        )

        with st.expander(
            f"#{idx}  {cand_name}  │  {cand_title}  │  Score: {score}  {band}",
            expanded=(score >= 80),
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
                    f"<span style='color:{colour};font-weight:700'>{score}</span><br>"
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
                    f"⭐ = in skills list &nbsp;&nbsp; 📄 = in profile summary"
                    f"</span>",
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

            ai_key = f"_ai_summary_{cand['id']}"
            if ai_key not in st.session_state:
                st.session_state[ai_key] = ""

            if llm and not st.session_state[ai_key]:
                if st.button(
                    "✨ Generate AI Summary",
                    key=f"ai_btn_{cand['id']}_{idx}",
                ):
                    with st.spinner("Generating…"):
                        st.session_state[ai_key] = _ai_summary(
                            llm, cand, role, matched_req, matched_nice
                        )

            if st.session_state[ai_key]:
                st.info(f"✨ **AI Summary:** {st.session_state[ai_key]}")

            if signals:
                with st.expander("📊 Score breakdown"):
                    for sig in signals:
                        st.markdown(f"- {sig}")

            summary_raw = (cand.get("profile_summary") or "").strip()
            if summary_raw:
                highlighted = summary_raw
                for term in ([role] if role else []) + req_s + nice_s:
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
                key=f"match_view_{cand['id']}_{idx}",
            ):
                st.session_state.selected_candidate_id = cand["id"]
                st.session_state[_NAV_KEY] = "👥 Profiles"
                st.rerun()

    # ── Excel export ──────────────────────────────────────────────────────────
    if filtered:
        try:
            import pandas as pd

            export_rows = [
                {
                    "Rank"               : i + 1,
                    "Name"               : _get_name(r),
                    "Title"              : _get_title(r),
                    "Score"              : r["_score"],
                    "Grade"              : (
                        "Strong"  if r["_score"] >= 80 else
                        "Good"    if r["_score"] >= 50 else
                        "Partial"
                    ),
                    "Experience (yrs)"   : r.get("total_experience", ""),
                    "Location"           : r.get("location", ""),
                    "Email"              : r.get("email_address", ""),
                    "Phone"              : r.get("phone_number", ""),
                    "Notice Period"      : r.get("notice_period", ""),
                    "Work Mode Pref"     : r.get("work_mode_pref", ""),
                    "Matched Req Skills" : ", ".join(r.get("_matched_req",  [])),
                    "Matched Nice Skills": ", ".join(r.get("_matched_nice", [])),
                    "Current Company"    : r.get("current_company", ""),
                    "Remarks"            : r.get("remarks", ""),
                }
                for i, r in enumerate(filtered)
            ]

            buf = io.BytesIO()
            pd.DataFrame(export_rows).to_excel(buf, index=False)
            buf.seek(0)

            fname = (
                f"matches_{role.replace(' ', '_')}_{jd_code}.xlsx"
                if role else "matches_export.xlsx"
            )

            st.markdown("---")
            st.download_button(
                "⬇️ Export Results to Excel",
                data=buf,
                file_name=fname,
                mime=(
                    "application/vnd.openxmlformats-"
                    "officedocument.spreadsheetml.sheet"
                ),
            )

        except ImportError:
            st.caption(
                "Install `openpyxl` to enable Excel export: "
                "`pip install openpyxl`"
            )
