from typing import Dict, List, Tuple, Optional, Any, Union, Set, Callable
# ui/pages/interviews.py

import streamlit as st
from datetime import datetime, timedelta, time as dtime
from ui.styles import page_header
from modules.ai_engine.prompt_templates import PromptTemplates


def render_interview_scheduler(services: dict):
    page_header(
        "Interview Scheduler",
        "Schedule interviews and generate AI questions",
        "🗓️"
    )

    db  = services["db"]
    llm = services["llm"]

    tabs = st.tabs([
        "📅 Upcoming Interviews",
        "➕ Schedule Interview",
        "🧠 AI Question Bank"
    ])

    # ── Tab 1: Upcoming ──
    with tabs[0]:
        days = st.slider("Show next N days", 1, 30, 7)
        try:
            interviews = db.get_upcoming_interviews(days_ahead=days)
        except Exception:
            interviews = []

        if not interviews:
            st.markdown(
                "<div class='riq-info-box'>"
                f"No interviews scheduled in the next {days} days.</div>",
                unsafe_allow_html=True
            )
        else:
            for iv in interviews:
                if isinstance(iv.get("candidates"), dict):
                    cname  = iv["candidates"].get("full_name", "?")
                    cemail = iv["candidates"].get("email", "")
                    role   = iv.get("job_descriptions", {}).get(
                        "role_name", "?"
                    )
                else:
                    cname  = iv.get("full_name", "?")
                    cemail = iv.get("email", "")
                    role   = iv.get("role_name", "?")

                dt     = str(iv.get("interview_scheduled_at", ""))[:16]
                itype  = iv.get("interview_type", "Interview")
                link   = iv.get("interview_link", "")
                stage  = iv.get("stage", "")

                st.markdown(
                    f"""<div class='riq-card' style='margin:6px 0;'>
                      <div style='display:flex;justify-content:space-between;'>
                        <div>
                          <div style='color:#ccd6f6;font-weight:700;
                                      font-size:0.92rem;'>
                            🗓 {cname} — {role}
                          </div>
                          <div style='color:#8892b0;font-size:0.78rem;
                                      margin-top:4px;'>
                            📅 {dt} IST &nbsp;|&nbsp;
                            🎯 {itype} &nbsp;|&nbsp;
                            📧 {cemail}
                          </div>
                          {f"<div style='margin-top:6px;'><a href='{link}' "
                           f"style='color:#6C63FF;font-size:0.78rem;'>"
                           f"🔗 Join Link</a></div>" if link else ""}
                        </div>
                        <div style='text-align:right;'>
                          <span style='background:#6C63FF22;color:#6C63FF;"
                            "border:1px solid #6C63FF;border-radius:12px;"
                            "padding:3px 10px;font-size:0.72rem;'>{stage}</span>
                        </div>
                      </div>
                    </div>""",
                    unsafe_allow_html=True
                )

    # ── Tab 2: Schedule ──
    with tabs[1]:
        st.markdown(
            "<div class='riq-section-title'>Schedule New Interview</div>",
            unsafe_allow_html=True
        )

        col_f, col_s = st.columns(2)

        with col_f:
            jds = db.get_all_jds(status="Active")
            if not jds:
                st.warning("No active JDs. Create one first.")
                return

            jd_map = {
                f"{j.get('jd_code','?')} — {j.get('role_name','?')}": j
                for j in jds
            }
            sel_jd_label = st.selectbox("Job Description", list(jd_map.keys()))
            sel_jd = jd_map[sel_jd_label]

            cands, _ = db.search_candidates(limit=200)
            if not cands:
                st.warning("No candidates in database.")
                return

            cand_map = {
                f"{c.get('full_name','?')} — {c.get('current_title','')}": c
                for c in cands
            }
            sel_cand_label = st.selectbox("Candidate", list(cand_map.keys()))
            sel_cand = cand_map[sel_cand_label]

        with col_s:
            interview_type = st.selectbox(
                "Interview Type",
                ["Screening Call", "Technical Round", "HR Round",
                 "Manager Round", "Panel Interview", "Final Discussion"]
            )
            ivdate = st.date_input(
                "Interview Date",
                min_value=datetime.today()
            )
            ivtime = st.select_slider(
                "Interview Time (IST)",
                options=[
                    f"{h:02d}:{m:02d}"
                    for h in range(9, 19)
                    for m in (0, 30)
                ],
                value="10:00"
            )
            duration = st.selectbox(
                "Duration",
                ["30 minutes", "45 minutes", "60 minutes", "90 minutes"]
            )
            link = st.text_input(
                "Meeting Link",
                placeholder="https://meet.google.com/..."
            )
            interviewer_email = st.text_input(
                "Interviewer Email(s)",
                placeholder="interviewer@company.com"
            )

        scheduled_dt = datetime.combine(
            ivdate,
            dtime(
                int(ivtime.split(":")[0]),
                int(ivtime.split(":")[1])
            )
        )

        if st.button(
            "📅 Schedule Interview", type="primary",
            use_container_width=True
        ):
            # Find application ID for this candidate + JD
            try:
                app_result = None
                if hasattr(db, 'client'):
                    r = db.client.table("applications").select("id").eq(
                        "candidate_id", sel_cand["id"]
                    ).eq("jd_id", sel_jd["id"]).execute()
                    if r.data:
                        app_result = r.data[0]["id"]
                else:
                    with db._conn() as conn:
                        row = conn.execute(
                            "SELECT id FROM applications "
                            "WHERE candidate_id=? AND jd_id=?",
                            (sel_cand["id"], sel_jd["id"])
                        ).fetchone()
                        if row:
                            app_result = row["id"]

                if not app_result:
                    # Auto-add to pipeline at Screening stage
                    db.add_to_pipeline(
                        sel_cand["id"], sel_jd["id"], 0, {}
                    )
                    if hasattr(db, 'client'):
                        r = db.client.table("applications").select("id").eq(
                            "candidate_id", sel_cand["id"]
                        ).eq("jd_id", sel_jd["id"]).execute()
                        app_result = r.data[0]["id"] if r.data else None
                    else:
                        with db._conn() as conn:
                            row = conn.execute(
                                "SELECT id FROM applications "
                                "WHERE candidate_id=? AND jd_id=?",
                                (sel_cand["id"], sel_jd["id"])
                            ).fetchone()
                            app_result = row["id"] if row else None

                if app_result:
                    update_data = {
                        "interview_scheduled_at": scheduled_dt.isoformat(),
                        "interview_type": interview_type,
                        "interview_link": link,
                        "stage": "Interview L1"
                    }
                    if hasattr(db, 'client'):
                        db.client.table("applications").update(
                            update_data
                        ).eq("id", app_result).execute()
                    else:
                        with db._conn() as conn:
                            conn.execute(
                                """UPDATE applications SET
                                   interview_scheduled_at=?,
                                   interview_type=?,
                                   interview_link=?,
                                   stage='Interview L1'
                                   WHERE id=?""",
                                (scheduled_dt.isoformat(),
                                 interview_type, link, app_result)
                            )

                    st.success(
                        f"✅ Interview scheduled for {sel_cand.get('full_name')} "
                        f"on {scheduled_dt.strftime('%b %d, %Y at %H:%M IST')}"
                    )

                    # Generate email invite
                    with st.expander("📧 Interview Invite Email", expanded=True):
                        invite = _generate_invite(
                            sel_cand, sel_jd, scheduled_dt,
                            interview_type, link, duration
                        )
                        st.text_area(
                            "Email Content", invite,
                            height=200, label_visibility="collapsed"
                        )
                        st.download_button(
                            "⬇️ Download Invite",
                            invite,
                            f"invite_{sel_cand.get('full_name','').replace(' ','_')}.txt",
                            use_container_width=True
                        )
            except Exception as e:
                st.error(f"Scheduling failed: {e}")

    # ── Tab 3: Question Bank ──
    with tabs[2]:
        st.markdown(
            "<div class='riq-section-title'>"
            "AI-Generated Interview Questions</div>",
            unsafe_allow_html=True
        )

        col_q1, col_q2 = st.columns(2)
        with col_q1:
            jds = db.get_all_jds(status="Active")
            jd_map = {
                f"{j.get('jd_code','?')} — {j.get('role_name','?')}": j
                for j in jds
            }
            if jd_map:
                sel_jd_q = jd_map[
                    st.selectbox("Job Description", list(jd_map.keys()),
                                 key="qjd")
                ]
        with col_q2:
            cands, _ = db.search_candidates(limit=200)
            if cands:
                cand_map_q = {
                    f"{c.get('full_name','?')} — {c.get('current_title','')}": c
                    for c in cands
                }
                sel_cand_q = cand_map_q[
                    st.selectbox("Candidate", list(cand_map_q.keys()),
                                 key="qcand")
                ]

        q_type = st.selectbox(
            "Interview Focus",
            ["Technical", "Behavioral", "Mixed", "Cultural Fit", "Leadership"]
        )

        if st.button(
            "🧠 Generate Questions", type="primary",
            use_container_width=True
        ):
            with st.spinner("Generating tailored questions..."):
                prompt = PromptTemplates.interview_questions(
                    sel_cand_q, sel_jd_q, q_type
                )
                resp = llm.complete(prompt, max_tokens=1500)
                questions = llm.extract_json(resp)

            if questions and isinstance(questions, list):
                st.markdown(
                    f"<div class='riq-success-box'>"
                    f"✅ Generated {len(questions)} tailored questions for "
                    f"<b>{sel_cand_q.get('full_name')}</b></div>",
                    unsafe_allow_html=True
                )
                for i, q in enumerate(questions, 1):
                    qtype_colors = {
                        "Technical":   "#6C63FF",
                        "Behavioral":  "#43E8D8",
                        "Situational": "#FF6584",
                        "Cultural":    "#FFEAA7"
                    }
                    qc = qtype_colors.get(
                        q.get("type", "Technical"), "#6C63FF"
                    )
                    with st.expander(
                        f"Q{i}: {q.get('question','')[:80]}..."
                    ):
                        st.markdown(
                            f"<span style='background:{qc}22;color:{qc};"
                            f"border:1px solid {qc};border-radius:12px;"
                            f"padding:2px 8px;font-size:0.72rem;'>"
                            f"{q.get('type','')}</span>"
                            f" <span style='color:#8892b0;font-size:0.75rem;"
                            f"margin-left:8px;'>{q.get('purpose','')}</span>",
                            unsafe_allow_html=True
                        )
                        st.markdown(
                            f"**Question:** {q.get('question','')}",
                        )
                        points = q.get("expected_answer_points", [])
                        if points:
                            st.markdown("**Expected answer points:**")
                            for p in points:
                                st.markdown(f"  • {p}")
            else:
                st.text_area("Raw Questions", resp, height=300)


def _generate_invite(
    candidate: dict, jd: dict,
    dt: datetime, itype: str,
    link: str, duration: str
) -> str:
    return f"""Subject: Interview Invitation — {jd.get('role_name', 'Position')} | RecruitIQ

Dear {candidate.get('full_name', 'Candidate')},

We are pleased to invite you for a {itype} for the position of:

  Role       : {jd.get('role_name', '')}
  Company    : {jd.get('client_name', 'Our Client')}
  Date       : {dt.strftime('%A, %B %d, %Y')}
  Time       : {dt.strftime('%I:%M %p')} IST
  Duration   : {duration}
  Mode       : {"Video Call" if link else "Telephonic"}
{f"  Join Link  : {link}" if link else ""}

Please confirm your availability by replying to this email.

Kindly keep the following handy:
  • Updated resume
  • Government-issued photo ID
  • Laptop with stable internet connection

We look forward to speaking with you!

Best regards,
Recruitment Team
Powered by RecruitIQ 🧠
"""
