from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
# modules/sourcing/ai_sourcing.py

"""
AI Sourcing Assistant
─────────────────────
Generates:
  1. Boolean search strings (LinkedIn, Google X-Ray, Naukri)
  2. Target company lists with rationale
  3. Personalised outreach templates (LinkedIn DM, cold email, WhatsApp)
  4. Sourcing strategy brief
  5. Talent pool gap analysis
"""

import json
import re

import streamlit as st


# ─────────────────────────────────────────────────────────────────────────────
#  BOOLEAN SEARCH GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

class BooleanSearchGenerator:

    def __init__(self, ai_handler):
        self.ai = ai_handler

    def generate(self, jd: dict) -> Dict[str, str]:
        """
        Generate platform-specific Boolean search strings.
        Returns {"linkedin": str, "google_xray": str, "naukri": str,
                 "indeed": str, "explanation": str}
        """
        role             = jd.get("role_name", "")
        required_skills  = jd.get("skillset_required", [])
        nice_to_have     = jd.get("skillset_good_to_have", [])
        location         = jd.get("location", "")
        experience_range = (
            f"{jd.get('experience_min', 0)}-{jd.get('experience_max', 10)} years"
        )
        work_mode        = jd.get("work_mode", "")

        prompt = f"""
You are an expert technical recruiter and sourcing specialist.
Generate optimised Boolean search strings for this job opening.

JOB DETAILS:
- Role: {role}
- Required Skills: {', '.join(required_skills[:12])}
- Nice to Have: {', '.join(nice_to_have[:6])}
- Location: {location}
- Experience: {experience_range}
- Work Mode: {work_mode}

Generate EXACTLY this JSON structure:
{{
  "linkedin": "LinkedIn Recruiter / Sales Navigator search string",
  "google_xray": "Google X-Ray search (site:linkedin.com/in/ ...)",
  "naukri": "Naukri Boolean search string",
  "indeed": "Indeed / Monster search string",
  "github": "GitHub profile search (if technical role)",
  "explanation": "Brief explanation of search logic and tips"
}}

Rules:
- Use AND, OR, NOT, quotes correctly
- LinkedIn string should use proper field operators (title:, company:)
- Google X-Ray: start with site:linkedin.com/in/
- Include experience-related keywords where relevant
- Skills should have synonyms grouped with OR
- Location should be in the string
- Return ONLY valid JSON, no markdown
"""
        try:
            response = self.ai.generate(prompt)
            clean    = re.sub(r"```(?:json)?|```", "", response).strip()
            match    = re.search(r"\{.*\}", clean, re.DOTALL)
            if match:
                data = json.loads(match.group())
                for key in ("linkedin", "google_xray", "naukri",
                            "indeed", "github", "explanation"):
                    if key not in data:
                        data[key] = ""
                return data
        except Exception as e:
            print(f"Boolean gen error: {e}")

        return self._fallback_boolean(jd)

    @staticmethod
    def _fallback_boolean(jd: dict) -> dict:
        role   = jd.get("role_name", "Software Engineer")
        skills = jd.get("skillset_required", [])[:4]
        loc    = jd.get("location", "India")

        skill_str = " OR ".join(f'"{s}"' for s in skills) if skills else f'"{role}"'

        return {
            "linkedin": (
                f'title:("{role}") AND ({skill_str}) AND "{loc}"'
            ),
            "google_xray": (
                f'site:linkedin.com/in/ ("{role}") ({skill_str}) {loc}'
            ),
            "naukri": (
                f'"{role}" AND ({skill_str})'
            ),
            "indeed": (
                f'"{role}" {skill_str}'
            ),
            "github": (
                f'"{role.lower()}" location:"{loc}" {skills[0] if skills else ""}'
            ),
            "explanation": (
                "Fallback Boolean string. "
                "Add experience keywords and synonyms for better results."
            ),
        }


# ─────────────────────────────────────────────────────────────────────────────
#  TARGET COMPANY GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

class TargetCompanyGenerator:

    def __init__(self, ai_handler):
        self.ai = ai_handler

    def generate(
        self,
        jd: dict,
        num_companies: int = 15
    ) -> Dict:
        """
        Generate a targeted company list with sourcing rationale.
        Returns {
            "tier1": [{"name": str, "reason": str, "linkedin_url": str}],
            "tier2": [...],
            "tier3": [...],
            "strategy": str
        }
        """
        role       = jd.get("role_name", "")
        skills     = jd.get("skillset_required", [])
        industry   = jd.get("industry_preference", "")
        client     = jd.get("client_name", "")
        location   = jd.get("location", "")

        prompt = f"""
You are a top-tier technical recruiter with deep market knowledge.
Generate a targeted company list for sourcing candidates.

JOB OPENING:
- Role: {role}
- Required Skills: {', '.join(skills[:10])}
- Industry: {industry or 'Technology'}
- Client/Hiring Company: {client or 'Confidential'}
- Location: {location}

Generate EXACTLY this JSON:
{{
  "tier1": [
    {{"name": "Company Name", "reason": "Why strong talent pool here",
      "linkedin_url": "https://linkedin.com/company/...", "size": "1000-5000"}}
  ],
  "tier2": [...same structure, 5 companies...],
  "tier3": [...same structure, 5 companies...],
  "avoid": ["Company Name — reason to avoid (e.g., client competitor)"],
  "strategy": "Overall sourcing strategy in 2-3 sentences"
}}

Tier 1 = direct competitors or companies with exact skill match (5 companies)
Tier 2 = adjacent industry or transferable skills (5 companies)  
Tier 3 = emerging or growing companies (5 companies)
Avoid  = companies not to poach from (client's competitors or own company)

Make company names realistic and relevant to the role and location.
Return ONLY valid JSON.
"""
        try:
            response = self.ai.generate(prompt)
            clean    = re.sub(r"```(?:json)?|```", "", response).strip()
            match    = re.search(r"\{.*\}", clean, re.DOTALL)
            if match:
                data = json.loads(match.group())
                return data
        except Exception as e:
            print(f"Company gen error: {e}")

        return {
            "tier1":    [],
            "tier2":    [],
            "tier3":    [],
            "avoid":    [],
            "strategy": "Unable to generate. Please try again.",
        }


# ─────────────────────────────────────────────────────────────────────────────
#  OUTREACH TEMPLATE GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

class OutreachTemplateGenerator:

    def __init__(self, ai_handler):
        self.ai = ai_handler

    def generate(
        self,
        jd: dict,
        candidate_persona: str = "senior professional",
    ) -> Dict[str, str]:
        """
        Generate personalised outreach templates for multiple channels.
        Returns {"linkedin_dm": str, "cold_email_subject": str,
                 "cold_email_body": str, "whatsapp": str,
                 "followup_linkedin": str, "followup_email": str}
        """
        role     = jd.get("role_name", "")
        client   = jd.get("client_name", "a leading company")
        location = jd.get("location", "")
        skills   = jd.get("skillset_required", [])[:5]
        work_mode = jd.get("work_mode", "Hybrid")
        budget_max = jd.get("budget_max", 0)

        budget_hint = (
            f"up to {budget_max:,.0f} LPA" if budget_max else "competitive"
        )

        prompt = f"""
You are an expert tech recruiter known for high response rates on outreach.
Generate personalised outreach templates for this role.

ROLE DETAILS:
- Position: {role}
- Company: {client}
- Location: {location}
- Work Mode: {work_mode}
- Key Skills: {', '.join(skills)}
- Budget: {budget_hint}
- Target: {candidate_persona}

Generate EXACTLY this JSON:
{{
  "linkedin_dm": "LinkedIn direct message (max 300 chars, conversational, not spammy)",
  "cold_email_subject": "Compelling subject line",
  "cold_email_body": "Professional cold email body (150-200 words)",
  "whatsapp": "WhatsApp message (casual, concise, max 200 chars)",
  "followup_linkedin": "Follow-up LinkedIn message if no reply after 5 days",
  "followup_email": "Follow-up email subject + body for no-reply scenario",
  "tips": ["tip1 for higher response rate", "tip2", "tip3"]
}}

Rules:
- Never mention salary upfront in LinkedIn DM
- Use [CANDIDATE_NAME] placeholder
- Make it feel personalised, not templated
- Focus on opportunity + growth, not just job description
- Add emojis sparingly in LinkedIn/WhatsApp messages
- Return ONLY valid JSON
"""
        try:
            response = self.ai.generate(prompt)
            clean    = re.sub(r"```(?:json)?|```", "", response).strip()
            match    = re.search(r"\{.*\}", clean, re.DOTALL)
            if match:
                data = json.loads(match.group())
                return data
        except Exception as e:
            print(f"Outreach gen error: {e}")

        return self._fallback_templates(jd)

    @staticmethod
    def _fallback_templates(jd: dict) -> dict:
        role   = jd.get("role_name", "this role")
        client = jd.get("client_name", "our client")
        return {
            "linkedin_dm": (
                f"Hi [CANDIDATE_NAME], I came across your profile and "
                f"think you'd be a great fit for a {role} opportunity "
                f"at {client}. Would you be open to a quick chat? 🙂"
            ),
            "cold_email_subject": f"Exciting {role} Opportunity — {client}",
            "cold_email_body": (
                f"Dear [CANDIDATE_NAME],\n\n"
                f"I hope this message finds you well. I'm reaching out "
                f"about an exciting {role} position at {client} that "
                f"aligns well with your background.\n\n"
                f"Would you be open to a brief call to discuss?\n\n"
                f"Best regards,\n[YOUR_NAME]"
            ),
            "whatsapp": (
                f"Hi [CANDIDATE_NAME]! Exciting {role} opportunity at "
                f"{client}. Interested to know more? 😊"
            ),
            "followup_linkedin": (
                f"Hi [CANDIDATE_NAME], just checking in on my earlier "
                f"message about the {role} role. Still happy to share "
                f"details if you're curious!"
            ),
            "followup_email": (
                f"Just following up on my email regarding the {role} "
                f"opportunity. Let me know if you'd like more details."
            ),
            "tips": [
                "Personalise the first line with a specific achievement from their profile",
                "Send LinkedIn DMs Tuesday-Thursday 9am-11am for best open rates",
                "Keep follow-ups friendly and short — 1-2 sentences max",
            ],
        }


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCING STRATEGY GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

class SourcingStrategyGenerator:

    def __init__(self, ai_handler):
        self.ai = ai_handler

    def generate(self, jd: dict) -> Dict:
        """
        Generate a full sourcing strategy brief.
        """
        role       = jd.get("role_name", "")
        skills     = jd.get("skillset_required", [])
        exp_min    = jd.get("experience_min", 0)
        exp_max    = jd.get("experience_max", 10)
        location   = jd.get("location", "")
        work_mode  = jd.get("work_mode", "")
        budget_max = jd.get("budget_max", 0)
        positions  = jd.get("positions_count", 1)

        prompt = f"""
Create a comprehensive sourcing strategy for this recruitment brief.

ROLE: {role}
SKILLS: {', '.join(skills[:10])}
EXPERIENCE: {exp_min}-{exp_max} years
LOCATION: {location}
WORK MODE: {work_mode}
POSITIONS: {positions}
BUDGET: {budget_max or 'Not specified'}

Return a JSON object:
{{
  "talent_pool_size": "estimate of available talent (e.g., ~2,500 in Mumbai)",
  "difficulty": "Easy|Medium|Hard|Very Hard",
  "difficulty_reason": "why this search is easy/hard",
  "primary_channels": [
    {{"channel": "LinkedIn Recruiter", "priority": "High", 
      "expected_response_rate": "15-25%", "time_to_fill": "2-3 weeks"}}
  ],
  "alternative_channels": ["GitHub", "Naukri", "AngelList", "Referrals"],
  "skill_synonyms": {{"skill": ["alias1", "alias2"]}},
  "red_flags": ["potential issue 1", "issue 2"],
  "timeline": {{
    "week1": "action plan",
    "week2": "action plan",
    "week3": "action plan",
    "week4": "action plan"
  }},
  "kpis": {{
    "profiles_to_source": 50,
    "target_screening_calls": 15,
    "target_l1_interviews": 6,
    "expected_offers": 2
  }}
}}

Return ONLY valid JSON.
"""
        try:
            response = self.ai.generate(prompt)
            clean    = re.sub(r"```(?:json)?|```", "", response).strip()
            match    = re.search(r"\{.*\}", clean, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception as e:
            print(f"Strategy gen error: {e}")

        return {
            "talent_pool_size":   "Unknown",
            "difficulty":         "Medium",
            "difficulty_reason":  "Standard tech role",
            "primary_channels":   [],
            "alternative_channels": ["LinkedIn", "Naukri", "Referrals"],
            "skill_synonyms":     {},
            "red_flags":          [],
            "timeline":           {},
            "kpis": {
                "profiles_to_source":   50,
                "target_screening_calls": 15,
                "target_l1_interviews": 6,
                "expected_offers":      2,
            }
        }


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

class AISourcingAssistant:
    """
    Facade combining all sourcing generators.
    Single point of entry for the UI.
    """

    def __init__(self, ai_handler):
        self.ai        = ai_handler
        self.boolean   = BooleanSearchGenerator(ai_handler)
        self.companies = TargetCompanyGenerator(ai_handler)
        self.outreach  = OutreachTemplateGenerator(ai_handler)
        self.strategy  = SourcingStrategyGenerator(ai_handler)

    def run_full_analysis(self, jd: dict) -> Dict:
        """Run all generators and return combined output."""
        return {
            "boolean":   self.boolean.generate(jd),
            "companies": self.companies.generate(jd),
            "outreach":  self.outreach.generate(jd),
            "strategy":  self.strategy.generate(jd),
        }


# ─────────────────────────────────────────────────────────────────────────────
#  STREAMLIT UI COMPONENT
# ─────────────────────────────────────────────────────────────────────────────

def render_sourcing_ui(jd: dict, ai_handler):
    """
    Full Streamlit UI for AI sourcing assistant.
    Pass the selected JD dict and AI handler.
    """
    assistant = AISourcingAssistant(ai_handler)

    st.subheader(
        f"🔍 AI Sourcing Assistant — {jd.get('role_name', 'Role')}"
    )

    # Overview pill
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Experience",
                f"{jd.get('experience_min',0)}-{jd.get('experience_max',10)} yrs")
    col2.metric("Location",    jd.get("location", "Any"))
    col3.metric("Work Mode",   jd.get("work_mode", "Hybrid"))
    col4.metric("Positions",   jd.get("positions_count", 1))

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "🔎 Boolean Search",
        "🏢 Target Companies",
        "📨 Outreach Templates",
        "📊 Strategy Brief",
    ])

    # ── TAB 1: Boolean Search ─────────────────────────────────────────────────
    with tab1:
        if st.button(
            "⚡ Generate Boolean Strings",
            key="gen_boolean",
            type="primary",
            use_container_width=True
        ):
            with st.spinner("Crafting Boolean search strings..."):
                result = assistant.boolean.generate(jd)
            st.session_state["sourcing_boolean"] = result

        if "sourcing_boolean" in st.session_state:
            data = st.session_state["sourcing_boolean"]

            platforms = {
                "💼 LinkedIn / Sales Navigator": data.get("linkedin", ""),
                "🌐 Google X-Ray":               data.get("google_xray", ""),
                "📋 Naukri":                     data.get("naukri", ""),
                "🔍 Indeed / Monster":           data.get("indeed", ""),
                "💻 GitHub (Technical roles)":   data.get("github", ""),
            }

            for platform_name, search_str in platforms.items():
                if search_str:
                    st.markdown(f"**{platform_name}**")
                    st.code(search_str, language="text")

                    col1, col2 = st.columns([3, 1])
                    with col2:
                        st.download_button(
                            "📋 Copy",
                            data=search_str,
                            file_name=f"boolean_{platform_name.split()[1].lower()}.txt",
                            mime="text/plain",
                            key=f"dl_bool_{platform_name}"
                        )

            if data.get("explanation"):
                with st.expander("💡 Search Tips"):
                    st.info(data["explanation"])

    # ── TAB 2: Target Companies ───────────────────────────────────────────────
    with tab2:
        if st.button(
            "🏢 Generate Target Companies",
            key="gen_companies",
            type="primary",
            use_container_width=True
        ):
            with st.spinner("Building target company list..."):
                result = assistant.companies.generate(jd)
            st.session_state["sourcing_companies"] = result

        if "sourcing_companies" in st.session_state:
            data = st.session_state["sourcing_companies"]

            if data.get("strategy"):
                st.info(f"📌 **Strategy:** {data['strategy']}")

            tier_configs = [
                ("tier1", "🥇 Tier 1 — Prime Targets",     "#28a745"),
                ("tier2", "🥈 Tier 2 — Strong Candidates",  "#17a2b8"),
                ("tier3", "🥉 Tier 3 — Emerging Talent",    "#ffc107"),
            ]

            for tier_key, tier_label, color in tier_configs:
                companies = data.get(tier_key, [])
                if companies:
                    st.markdown(f"**{tier_label}**")
                    for co in companies:
                        with st.container():
                            c1, c2 = st.columns([3, 1])
                            with c1:
                                name = co.get("name", "Unknown")
                                reason = co.get("reason", "")
                                size   = co.get("size", "")
                                st.markdown(
                                    f"**{name}**"
                                    + (f" _{size}_" if size else "")
                                )
                                if reason:
                                    st.caption(f"→ {reason}")
                            with c2:
                                url = co.get("linkedin_url", "")
                                if url:
                                    st.link_button("LinkedIn 🔗", url)
                    st.markdown("---")

            if data.get("avoid"):
                with st.expander("⚠️ Companies to Avoid"):
                    for item in data["avoid"]:
                        st.warning(f"❌ {item}")

    # ── TAB 3: Outreach Templates ─────────────────────────────────────────────
    with tab3:
        col1, col2 = st.columns([2, 1])
        with col1:
            persona = st.selectbox(
                "Target Candidate Persona",
                [
                    "senior professional",
                    "mid-level professional",
                    "fresher / entry level",
                    "startup founder / CXO",
                    "passive candidate",
                    "actively job-seeking",
                ],
                key="outreach_persona"
            )
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            gen_btn = st.button(
                "✍️ Generate Templates",
                key="gen_outreach",
                type="primary",
                use_container_width=True
            )

        if gen_btn:
            with st.spinner("Writing outreach templates..."):
                result = assistant.outreach.generate(jd, persona)
            st.session_state["sourcing_outreach"] = result

        if "sourcing_outreach" in st.session_state:
            data = st.session_state["sourcing_outreach"]

            template_sections = [
                ("💼 LinkedIn DM",          "linkedin_dm",          300),
                ("📧 Cold Email Subject",    "cold_email_subject",   60),
                ("📧 Cold Email Body",       "cold_email_body",      300),
                ("💬 WhatsApp Message",      "whatsapp",             200),
                ("🔄 LinkedIn Follow-up",    "followup_linkedin",    200),
                ("🔄 Email Follow-up",       "followup_email",       300),
            ]

            for label, key, height in template_sections:
                content = data.get(key, "")
                if content:
                    st.markdown(f"**{label}**")
                    edited = st.text_area(
                        label="",
                        value=content,
                        height=height,
                        key=f"tpl_{key}",
                        label_visibility="collapsed"
                    )

            if data.get("tips"):
                with st.expander("💡 Pro Tips for Higher Response Rates"):
                    for tip in data["tips"]:
                        st.success(f"✅ {tip}")

            # Bulk download
            all_templates = "\n\n".join([
                f"{'='*50}\n{label}\n{'='*50}\n{data.get(key,'')}"
                for label, key, _ in template_sections
                if data.get(key)
            ])
            st.download_button(
                "📄 Download All Templates",
                data=all_templates,
                file_name=f"outreach_templates_{jd.get('jd_code','role')}.txt",
                mime="text/plain",
                key="dl_all_templates"
            )

    # ── TAB 4: Strategy Brief ─────────────────────────────────────────────────
    with tab4:
        if st.button(
            "📊 Generate Sourcing Strategy",
            key="gen_strategy",
            type="primary",
            use_container_width=True
        ):
            with st.spinner("Building sourcing strategy..."):
                result = assistant.strategy.generate(jd)
            st.session_state["sourcing_strategy"] = result

        if "sourcing_strategy" in st.session_state:
            data = st.session_state["sourcing_strategy"]

            # Header metrics
            difficulty_colors = {
                "Easy":      "🟢",
                "Medium":    "🟡",
                "Hard":      "🔴",
                "Very Hard": "🔴"
            }
            difficulty = data.get("difficulty", "Medium")
            icon       = difficulty_colors.get(difficulty, "🟡")

            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Talent Pool", data.get("talent_pool_size", "Unknown"))
            mc2.metric("Difficulty",  f"{icon} {difficulty}")
            mc3.metric(
                "Profiles to Source",
                data.get("kpis", {}).get("profiles_to_source", 50)
            )

            if data.get("difficulty_reason"):
                st.info(f"ℹ️ {data['difficulty_reason']}")

            # KPI funnel
            kpis = data.get("kpis", {})
            if kpis:
                st.markdown("**📈 Recruitment Funnel KPIs**")
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Source",    kpis.get("profiles_to_source", 50))
                k2.metric("Screen",    kpis.get("target_screening_calls", 15))
                k3.metric("Interview", kpis.get("target_l1_interviews", 6))
                k4.metric("Offer",     kpis.get("expected_offers", 2))

            # Primary channels
            channels = data.get("primary_channels", [])
            if channels:
                st.markdown("**📡 Primary Sourcing Channels**")
                for ch in channels:
                    with st.container():
                        cc1, cc2, cc3 = st.columns([2, 1, 1])
                        cc1.markdown(f"**{ch.get('channel', '')}**")
                        cc2.caption(
                            f"Priority: {ch.get('priority', 'Medium')}"
                        )
                        cc3.caption(
                            f"Response: {ch.get('expected_response_rate', '?')}"
                        )

            # Weekly timeline
            timeline = data.get("timeline", {})
            if timeline:
                st.markdown("**📅 4-Week Sourcing Plan**")
                for week, plan in timeline.items():
                    st.markdown(f"- **{week.replace('_', ' ').title()}:** {plan}")

            # Skill synonyms
            synonyms = data.get("skill_synonyms", {})
            if synonyms:
                with st.expander("🔤 Skill Synonyms & Aliases"):
                    for skill, aliases in synonyms.items():
                        st.markdown(
                            f"**{skill}** → {' | '.join(aliases)}"
                        )

            # Red flags
            red_flags = data.get("red_flags", [])
            if red_flags:
                with st.expander("⚠️ Potential Challenges"):
                    for flag in red_flags:
                        st.warning(f"⚠️ {flag}")

            # Download brief
            brief_text = f"""
SOURCING STRATEGY BRIEF
{'='*50}
Role:         {jd.get('role_name', '')}
JD Code:      {jd.get('jd_code', '')}
Location:     {jd.get('location', '')}
Generated:    {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')}

OVERVIEW
--------
Talent Pool:  {data.get('talent_pool_size', 'Unknown')}
Difficulty:   {difficulty}
Reason:       {data.get('difficulty_reason', '')}

KPI FUNNEL
----------
Source {kpis.get('profiles_to_source', 50)} → Screen {kpis.get('target_screening_calls', 15)} → Interview {kpis.get('target_l1_interviews', 6)} → Offer {kpis.get('expected_offers', 2)}

WEEKLY PLAN
-----------
{chr(10).join(f'{w.replace("_"," ").title()}: {p}' for w, p in timeline.items())}

RED FLAGS
---------
{chr(10).join(f'- {f}' for f in red_flags)}
""".strip()

            st.download_button(
                "📄 Download Strategy Brief",
                data=brief_text,
                file_name=f"sourcing_brief_{jd.get('jd_code','role')}.txt",
                mime="text/plain",
                key="dl_strategy"
            )
