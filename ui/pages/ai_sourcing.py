# ui/pages/ai_sourcing.py
# RecruitIQ – AI Sourcing Assistant (v2.0)
#
# Changes from v1.0:
#   - Outreach tab: removed db.search_candidates() call on page load.
#     Candidates are now loaded ONLY when user clicks "Load Candidates",
#     result cached in st.session_state["ai_source_cands"].
#   - Candidate map uses correct schema keys (candidate_name, current_position,
#     current_company) instead of full_name / current_title which don't exist.
#   - search_candidates() now called with explicit filters so the indexed
#     columns (total_experience B-tree) are always used — never a seq scan.

from __future__ import annotations

import streamlit as st
from ui.styles import page_header
from modules.ai_engine.prompt_templates import PromptTemplates


def render_ai_sourcing(services: dict):
    page_header(
        "AI Sourcing Assistant",
        "Boolean search, target companies & personalised outreach",
        "🤖",
    )

    db  = services["db"]
    llm = services["llm"]

    jds = db.get_all_jds(status="Active")
    if not jds:
        st.info("No active JDs. Create one first.")
        return

    jd_map = {
        f"{j.get('jd_code', '?')} — {j.get('role_name', '?')}": j
        for j in jds
    }
    sel = st.selectbox("Select JD", list(jd_map.keys()))
    jd  = jd_map[sel]

    # Initialise session state keys once
    for key, default in [
        ("bool_result",       None),
        ("bool_raw",          None),
        ("ai_source_cands",   None),   # ← loaded on demand, not on page open
        ("ai_source_jd_id",   None),   # ← track which JD the cache belongs to
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    # Invalidate candidate cache when JD selection changes
    if st.session_state["ai_source_jd_id"] != jd.get("id"):
        st.session_state["ai_source_cands"] = None
        st.session_state["ai_source_jd_id"] = jd.get("id")

    tabs = st.tabs([
        "🔍 Boolean Search",
        "🏢 Target Companies",
        "✉️ Outreach Templates",
        "📊 Sourcing Plan",
    ])

    # ── Tab 0: Boolean Search ─────────────────────────────────────────────────
    with tabs[0]:
        st.markdown(
            "<div class='riq-section-title'>"
            "AI Boolean Search Generator</div>",
            unsafe_allow_html=True,
        )

        if st.button(
            "🧠 Generate Boolean Strings",
            type="primary",
            use_container_width=True,
        ):
            with st.spinner("Generating boolean search strings..."):
                prompt = PromptTemplates.boolean_search(jd)
                resp   = llm.complete(prompt, max_tokens=1000)
                result = llm.extract_json(resp)
            st.session_state["bool_result"] = result
            st.session_state["bool_raw"]    = resp

        result = st.session_state.get("bool_result")
        if result:
            platforms = [
                ("💼 LinkedIn Boolean",  "linkedin_boolean",  "#0077B5"),
                ("🔍 Google X-Ray",      "google_xray",       "#4285F4"),
                ("💼 Naukri Keywords",   "naukri_keywords",   "#FF6B35"),
            ]
            for label, key, color in platforms:
                val = result.get(key, "")
                if val:
                    st.markdown(
                        f"<div class='riq-section-title' "
                        f"style='color:{color};'>{label}</div>",
                        unsafe_allow_html=True,
                    )
                    st.code(val, language="text")
                    st.download_button(
                        f"⬇️ Copy {label.split()[1]}",
                        val,
                        f"{key}.txt",
                        key=f"dl_{key}",
                    )

            companies = result.get("target_companies", [])
            if companies:
                st.markdown(
                    "<div class='riq-section-title'>"
                    "🏢 Target Companies</div>",
                    unsafe_allow_html=True,
                )
                cols = st.columns(min(len(companies), 4))
                for i, comp in enumerate(companies):
                    with cols[i % len(cols)]:
                        st.markdown(
                            f"<div class='riq-card' style='text-align:center;"
                            f"padding:10px;'>"
                            f"<div style='color:#6C63FF;font-weight:600;"
                            f"font-size:0.85rem;'>🏢 {comp}</div></div>",
                            unsafe_allow_html=True,
                        )

            tips = result.get("sourcing_tips", [])
            if tips:
                st.markdown(
                    "<div class='riq-section-title'>💡 Sourcing Tips</div>",
                    unsafe_allow_html=True,
                )
                for tip in tips:
                    st.markdown(f"  ✅ {tip}")

        elif st.session_state.get("bool_raw"):
            st.text_area(
                "Raw Output", st.session_state["bool_raw"], height=200
            )

    # ── Tab 1: Target Companies ───────────────────────────────────────────────
    with tabs[1]:
        st.markdown(
            "<div class='riq-section-title'>"
            "Company Targeting Strategy</div>",
            unsafe_allow_html=True,
        )

        col_l, col_r = st.columns(2)
        with col_l:
            industry = st.text_input(
                "Target Industry",
                placeholder="FinTech, SaaS, E-Commerce...",
            )
            company_size = st.selectbox(
                "Company Size",
                ["Any", "Startup (1-50)", "SME (50-500)",
                 "Mid-size (500-5000)", "Enterprise (5000+)"],
            )
        with col_r:
            geography = st.text_input(
                "Geography",
                placeholder="Bangalore, Mumbai, Pan-India...",
            )
            avoid = st.text_input(
                "Avoid Companies",
                placeholder="Company A, Company B...",
            )

        if st.button(
            "🧠 Generate Target Company List",
            type="primary",
            use_container_width=True,
        ):
            custom_prompt = f"""You are RecruitIQ's sourcing strategist.
Generate a target company list for sourcing {jd.get('role_name')} candidates.

JD CONTEXT:
Required Skills: {jd.get('skillset_required', [])}
Experience: {jd.get('experience_min', 0)}-{jd.get('experience_max', 0)} years
Location: {jd.get('location', '')}

FILTERS:
Industry: {industry or 'Any'}
Company Size: {company_size}
Geography: {geography or 'India'}
Avoid: {avoid or 'None'}

Return JSON:
{{
  "tier1_companies": ["top 5 ideal companies"],
  "tier2_companies": ["next 8 good companies"],
  "tier3_companies": ["10 backup companies"],
  "linkedin_company_search": "search string for LinkedIn",
  "rationale": "brief explanation of targeting strategy"
}}"""
            with st.spinner("Building target list..."):
                resp        = llm.complete(custom_prompt, max_tokens=800)
                target_data = llm.extract_json(resp)

            if target_data:
                for tier, label, color in [
                    ("tier1_companies", "🥇 Tier 1 — Ideal Targets", "#2ecc71"),
                    ("tier2_companies", "🥈 Tier 2 — Good Targets",  "#f39c12"),
                    ("tier3_companies", "🥉 Tier 3 — Backup Pool",   "#e74c3c"),
                ]:
                    companies = target_data.get(tier, [])
                    if companies:
                        st.markdown(
                            f"<div class='riq-section-title' "
                            f"style='color:{color};'>{label}</div>",
                            unsafe_allow_html=True,
                        )
                        cols = st.columns(min(len(companies), 5))
                        for i, c in enumerate(companies):
                            with cols[i % 5]:
                                st.markdown(
                                    f"<div class='riq-card' "
                                    f"style='text-align:center;padding:8px;'>"
                                    f"<div style='color:{color};"
                                    f"font-weight:600;font-size:0.82rem;'>"
                                    f"🏢 {c}</div></div>",
                                    unsafe_allow_html=True,
                                )
                if target_data.get("rationale"):
                    st.info(f"💡 **Strategy:** {target_data['rationale']}")
            else:
                st.text_area("Raw Output", resp, height=200)

    # ── Tab 2: Outreach Templates ─────────────────────────────────────────────
    with tabs[2]:
        st.markdown(
            "<div class='riq-section-title'>"
            "Personalised Outreach Generator</div>",
            unsafe_allow_html=True,
        )

        # ── Candidate loader (on-demand, not on page open) ────────────────────
        #
        # OLD (broken):
        #   cands, _ = db.search_candidates(limit=200)
        #   → runs on every page render → seq scan on 348k rows → TIMEOUT
        #
        # NEW:
        #   1. Show a "Load Candidates" button with optional filters
        #   2. Only query when user clicks it
        #   3. Cache result in session_state so reruns don't re-query
        #   4. Use experience range from the JD so the B-tree index is used

        try:
            jd_exp_min = float(jd.get("experience_min") or 0)
            jd_exp_max = float(jd.get("experience_max") or 50)
        except (TypeError, ValueError):
            jd_exp_min, jd_exp_max = 0.0, 50.0

        with st.expander("🔧 Candidate Filters", expanded=False):
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                f_exp_min = st.number_input(
                    "Exp Min (yrs)",
                    min_value=0.0, max_value=50.0,
                    value=max(0.0, jd_exp_min - 1.0),
                    step=0.5, format="%.1f",
                    key="out_exp_min",
                )
            with fc2:
                f_exp_max = st.number_input(
                    "Exp Max (yrs)",
                    min_value=0.0, max_value=50.0,
                    value=min(50.0, jd_exp_max + 1.0),
                    step=0.5, format="%.1f",
                    key="out_exp_max",
                )
            with fc3:
                f_location = st.text_input(
                    "Location contains",
                    value=jd.get("location", ""),
                    key="out_location",
                )
            f_limit = st.slider(
                "Max candidates to load",
                min_value=10, max_value=200,
                value=50, step=10,
                key="out_limit",
            )

        load_col, clear_col = st.columns([2, 1])
        with load_col:
            load_clicked = st.button(
                "📥 Load Candidates",
                type="primary",
                use_container_width=True,
                key="out_load_btn",
            )
        with clear_col:
            if st.button(
                "🗑 Clear",
                use_container_width=True,
                key="out_clear_btn",
            ):
                st.session_state["ai_source_cands"] = None
                st.rerun()

        if load_clicked:
            with st.spinner(
                f"Loading candidates "
                f"({int(f_exp_min)}–{int(f_exp_max)} yrs exp)…"
            ):
                cands, total = db.search_candidates(
                    location=f_location,
                    experience_min=f_exp_min,
                    experience_max=f_exp_max,
                    limit=int(f_limit),
                )
            st.session_state["ai_source_cands"] = cands
            st.caption(
                f"Loaded {len(cands)} candidate(s) "
                f"(total matching: {total})"
            )

        cands = st.session_state.get("ai_source_cands")

        if not cands:
            st.info(
                "Click **Load Candidates** above to fetch candidates "
                "for outreach generation."
            )
        else:
            # Build display map using correct schema column names
            def _cand_label(c: dict) -> str:
                name    = (
                    c.get("candidate_name")
                    or f"{c.get('first_name','')} {c.get('last_name','')}".strip()
                    or "Unknown"
                )
                pos     = (
                    c.get("current_position")
                    or c.get("title")
                    or ""
                )
                company = c.get("current_company") or ""
                return f"{name} — {pos} @ {company}".strip(" —@")

            cand_map = {_cand_label(c): c for c in cands}

            if not cand_map:
                st.warning("No candidates returned. Try adjusting filters.")
            else:
                sel_cand = cand_map[
                    st.selectbox(
                        "Select Candidate",
                        list(cand_map.keys()),
                        key="out_cand_select",
                    )
                ]

                platforms = st.multiselect(
                    "Platforms",
                    ["LinkedIn", "Email", "WhatsApp"],
                    default=["LinkedIn", "Email"],
                    key="out_platforms",
                )

                if st.button(
                    "✍️ Generate Outreach Messages",
                    type="primary",
                    use_container_width=True,
                    key="out_generate_btn",
                ):
                    p_icons = {
                        "LinkedIn": "💼",
                        "Email":    "📧",
                        "WhatsApp": "💬",
                    }
                    for platform in platforms:
                        with st.spinner(f"Writing {platform} message..."):
                            prompt = PromptTemplates.outreach_template(
                                sel_cand, jd, platform
                            )
                            msg = llm.complete(
                                prompt, max_tokens=600, temperature=0.5
                            )

                        st.markdown(
                            f"<div class='riq-section-title'>"
                            f"{p_icons.get(platform, '📩')} "
                            f"{platform} Message</div>",
                            unsafe_allow_html=True,
                        )
                        edited = st.text_area(
                            f"Edit {platform} message",
                            msg,
                            height=180,
                            key=f"out_{platform}_{sel_cand.get('id')}",
                        )
                        st.download_button(
                            f"⬇️ Download {platform} Message",
                            edited,
                            f"outreach_{platform.lower()}.txt",
                            key=f"dlout_{platform}",
                        )

    # ── Tab 3: Sourcing Plan ──────────────────────────────────────────────────
    with tabs[3]:
        st.markdown(
            "<div class='riq-section-title'>"
            "AI Sourcing Plan Generator</div>",
            unsafe_allow_html=True,
        )

        col_1, col_2 = st.columns(2)
        with col_1:
            timeline = st.selectbox(
                "Timeline",
                ["1 week", "2 weeks", "30 days", "45 days", "60 days"],
            )
            target_count = st.number_input(
                "Target Profiles to Source", 10, 500, 50
            )
        with col_2:
            team_size = st.number_input(
                "Recruiting Team Size", 1, 20, 2
            )
            budget_available = st.selectbox(
                "Paid Tools Available",
                ["None (free only)", "LinkedIn Recruiter Basic",
                 "LinkedIn Recruiter Full", "Naukri RMS", "Multiple"],
            )

        if st.button(
            "📋 Generate Sourcing Plan",
            type="primary",
            use_container_width=True,
        ):
            plan_prompt = f"""You are RecruitIQ's sourcing strategist.
Create a detailed sourcing plan.

ROLE: {jd.get('role_name')} — {jd.get('experience_min', 0)}-{jd.get('experience_max', 0)}y
SKILLS: {jd.get('skillset_required', [])}
LOCATION: {jd.get('location', '')} | {jd.get('work_mode', '')}
TIMELINE: {timeline}
TARGET: {target_count} profiles
TEAM SIZE: {team_size} recruiters
TOOLS: {budget_available}

Return JSON:
{{
  "week_1_actions": ["action1", "action2"],
  "week_2_actions": ["action1", "action2"],
  "channels": [{{"channel": "LinkedIn", "daily_target": 10, "method": ""}}],
  "daily_task_per_recruiter": ["task1", "task2"],
  "kpis": {{"profiles_per_day": 0, "conversion_rate": ""}},
  "tools_recommended": ["tool1"],
  "quick_wins": ["tip1", "tip2"]
}}"""

            with st.spinner("Building sourcing plan..."):
                resp = llm.complete(plan_prompt, max_tokens=1200)
                plan = llm.extract_json(resp)

            if plan:
                for week_key, week_label in [
                    ("week_1_actions", "📅 Week 1 Actions"),
                    ("week_2_actions", "📅 Week 2 Actions"),
                ]:
                    actions = plan.get(week_key, [])
                    if actions:
                        st.markdown(
                            f"<div class='riq-section-title'>"
                            f"{week_label}</div>",
                            unsafe_allow_html=True,
                        )
                        for a in actions:
                            st.markdown(f"  ✅ {a}")

                channels = plan.get("channels", [])
                if channels:
                    st.markdown(
                        "<div class='riq-section-title'>"
                        "📡 Channels & Targets</div>",
                        unsafe_allow_html=True,
                    )
                    for ch in channels:
                        st.markdown(
                            f"**{ch.get('channel', '')}** — "
                            f"{ch.get('daily_target', 0)} profiles/day — "
                            f"{ch.get('method', '')}"
                        )

                quick_wins = plan.get("quick_wins", [])
                if quick_wins:
                    st.markdown(
                        "<div class='riq-section-title'>"
                        "⚡ Quick Wins</div>",
                        unsafe_allow_html=True,
                    )
                    for qw in quick_wins:
                        st.markdown(f"  🚀 {qw}")
            else:
                st.text_area("Raw Plan", resp, height=300)
