from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
# modules/interview/scheduler.py

"""
Interview Scheduler
───────────────────
- Schedule interviews linked to applications
- AI-generated tailored interview questions
- Email invitation composer (SMTP send or copy-paste)
- Upcoming interview dashboard widget
"""

import json
import os
import re
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import streamlit as st


# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

INTERVIEW_TYPES = [
    "Screening Call",
    "Technical Round 1",
    "Technical Round 2",
    "System Design",
    "Managerial Round",
    "HR Round",
    "Client Round",
    "Final Round",
    "Assignment Review",
    "Bar Raiser",
]

DURATION_OPTIONS = [15, 30, 45, 60, 90, 120]

MEETING_PLATFORMS = {
    "Google Meet":     "https://meet.google.com/new",
    "Zoom":            "https://zoom.us/meeting/schedule",
    "Microsoft Teams": "https://teams.microsoft.com",
    "Webex":           "https://webex.com",
    "Custom Link":     "",
}


# ─────────────────────────────────────────────────────────────────────────────
#  AI QUESTION GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

class InterviewQuestionGenerator:

    def __init__(self, ai_handler):
        self.ai = ai_handler

    def generate(
        self,
        candidate: dict,
        jd: dict,
        interview_type: str,
        num_questions: int = 8,
    ) -> Dict[str, List[str]]:
        """
        Generate tailored interview questions.
        Returns {category: [questions]}
        """
        candidate_skills = candidate.get("skills", [])
        required_skills  = jd.get("skillset_required", [])
        role             = jd.get("role_name", "the role")
        experience       = candidate.get("total_experience", 0)
        candidate_title  = candidate.get("current_title", "candidate")

        prompt = f"""
You are a senior technical interviewer at a top recruitment firm.
Generate exactly {num_questions} tailored interview questions.

CANDIDATE PROFILE:
- Name: {candidate.get('full_name', 'Candidate')}
- Current Role: {candidate_title}
- Experience: {experience} years
- Skills: {', '.join(candidate_skills[:15])}

JOB REQUIREMENTS:
- Role: {role}
- Required Skills: {', '.join(required_skills[:10])}
- Experience Needed: {jd.get('experience_min', 0)}-{jd.get('experience_max', 10)} years
- Work Mode: {jd.get('work_mode', 'Hybrid')}

INTERVIEW TYPE: {interview_type}

Return ONLY a valid JSON object:
{{
  "technical": ["question1", "question2", ...],
  "behavioral": ["question1", "question2"],
  "situational": ["question1", "question2"],
  "culture_fit": ["question1"]
}}

Rules:
- Questions must be specific to candidate background + JD requirements
- Technical questions should test skills gaps between candidate and JD
- Behavioral questions should use STAR format prompts
- For screening calls: focus on availability, expectations, motivation
- For technical rounds: deep-dive on required skills
- Return ONLY valid JSON, no markdown
"""
        try:
            response = self.ai.generate(prompt)
            clean    = re.sub(r"```(?:json)?|```", "", response).strip()
            match    = re.search(r"\{.*\}", clean, re.DOTALL)
            if match:
                data = json.loads(match.group())
                # Ensure all categories are lists
                for cat in ("technical", "behavioral", "situational", "culture_fit"):
                    if not isinstance(data.get(cat), list):
                        data[cat] = []
                return data
        except Exception as e:
            print(f"Question generation error: {e}")

        return self._fallback_questions(interview_type, required_skills)

    @staticmethod
    def _fallback_questions(
        interview_type: str,
        skills: List[str]
    ) -> Dict[str, List[str]]:
        """Default questions when AI fails."""
        skill_str = skills[0] if skills else "your primary skill"

        return {
            "technical": [
                f"Walk me through your experience with {skill_str}.",
                "Describe a technically challenging project you led.",
                "How do you stay updated with industry developments?",
            ],
            "behavioral": [
                "Tell me about a time you handled a tight deadline.",
                "Describe a conflict with a colleague and how you resolved it.",
            ],
            "situational": [
                "If you joined next week, what would be your 30-day plan?",
                "How would you handle unclear requirements from a stakeholder?",
            ],
            "culture_fit": [
                "What kind of work environment brings out your best?",
            ],
        }


# ─────────────────────────────────────────────────────────────────────────────
#  EMAIL INVITATION COMPOSER
# ─────────────────────────────────────────────────────────────────────────────

class InterviewEmailComposer:

    def __init__(self, ai_handler=None):
        self.ai = ai_handler

    def compose_invitation(
        self,
        candidate: dict,
        jd: dict,
        interview_type: str,
        scheduled_at: datetime,
        duration_minutes: int,
        meeting_link: str,
        interviewer_names: List[str],
        sender_name: str = "Recruitment Team",
        company_name: str = "RecruitIQ",
        additional_notes: str = "",
    ) -> Dict[str, str]:
        """
        Compose interview invitation email.
        Returns {"subject": str, "body_html": str, "body_text": str}
        """
        date_str     = scheduled_at.strftime("%A, %d %B %Y")
        time_str     = scheduled_at.strftime("%I:%M %p")
        end_time     = (
            scheduled_at + timedelta(minutes=duration_minutes)
        ).strftime("%I:%M %p")

        interviewers_str = (
            ", ".join(interviewer_names) if interviewer_names
            else sender_name
        )

        candidate_name = candidate.get("full_name", "Candidate")
        role_name      = jd.get("role_name", "the position")
        client_name    = jd.get("client_name", company_name)
        jd_code        = jd.get("jd_code", "")

        subject = (
            f"Interview Invitation – {role_name}"
            + (f" ({jd_code})" if jd_code else "")
            + f" | {date_str}"
        )

        body_text = f"""
Dear {candidate_name},

We are pleased to invite you for a {interview_type} for the role of 
{role_name}{f' at {client_name}' if client_name else ''}.

INTERVIEW DETAILS:
─────────────────────────
Date:        {date_str}
Time:        {time_str} – {end_time} (IST)
Duration:    {duration_minutes} minutes
Type:        {interview_type}
Interviewer: {interviewers_str}
Meeting:     {meeting_link if meeting_link else 'Link will be shared separately'}
─────────────────────────

{f'Additional Information:{chr(10)}{additional_notes}{chr(10)}' if additional_notes else ''}

To confirm your availability, please reply to this email or contact us.

Please ensure you:
• Join the meeting 5 minutes early
• Keep your resume handy
• Ensure stable internet and quiet environment (for video calls)

We look forward to speaking with you!

Warm regards,
{sender_name}
{company_name}
""".strip()

        body_html = f"""
<!DOCTYPE html>
<html>
<head>
<style>
  body       {{ font-family: Arial, sans-serif; color: #333; line-height: 1.6; }}
  .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
  .header    {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white; padding: 25px; border-radius: 8px 8px 0 0; }}
  .body      {{ background: #f9f9f9; padding: 25px;
                border: 1px solid #e0e0e0; }}
  .details   {{ background: white; border-left: 4px solid #667eea;
                padding: 15px; margin: 20px 0; border-radius: 0 8px 8px 0; }}
  .detail-row{{ display: flex; margin: 8px 0; }}
  .label     {{ font-weight: bold; min-width: 120px; color: #667eea; }}
  .btn       {{ display: inline-block; background: #667eea; color: white !important;
                padding: 12px 24px; border-radius: 6px; text-decoration: none;
                font-weight: bold; margin: 15px 0; }}
  .footer    {{ text-align: center; color: #999; font-size: 12px;
                padding: 15px; }}
  .checklist {{ background: #fff3cd; border: 1px solid #ffc107;
                padding: 15px; border-radius: 8px; margin: 15px 0; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h2 style="margin:0">🎯 Interview Invitation</h2>
    <p style="margin:5px 0 0 0; opacity:0.9">{role_name}</p>
  </div>
  <div class="body">
    <p>Dear <strong>{candidate_name}</strong>,</p>
    <p>
      We are pleased to invite you for a <strong>{interview_type}</strong>
      for the role of <strong>{role_name}</strong>
      {f' at <strong>{client_name}</strong>' if client_name else ''}.
    </p>

    <div class="details">
      <h3 style="margin-top:0; color:#667eea">📅 Interview Details</h3>
      <div class="detail-row">
        <span class="label">📆 Date:</span>
        <span>{date_str}</span>
      </div>
      <div class="detail-row">
        <span class="label">⏰ Time:</span>
        <span>{time_str} – {end_time} (IST)</span>
      </div>
      <div class="detail-row">
        <span class="label">⏱️ Duration:</span>
        <span>{duration_minutes} minutes</span>
      </div>
      <div class="detail-row">
        <span class="label">🎤 Type:</span>
        <span>{interview_type}</span>
      </div>
      <div class="detail-row">
        <span class="label">👤 Interviewer:</span>
        <span>{interviewers_str}</span>
      </div>
    </div>

    {f'''
    <div style="text-align:center">
      <a href="{meeting_link}" class="btn">
        🔗 Join Meeting
      </a>
      <p style="color:#999; font-size:12px">{meeting_link}</p>
    </div>
    ''' if meeting_link else ''}

    {f'<p><strong>Additional Information:</strong><br>{additional_notes}</p>'
     if additional_notes else ''}

    <div class="checklist">
      <strong>✅ Please ensure you:</strong>
      <ul style="margin:5px 0">
        <li>Join 5 minutes before the scheduled time</li>
        <li>Keep a copy of your resume handy</li>
        <li>Have a stable internet connection</li>
        <li>Are in a quiet environment (for video calls)</li>
      </ul>
    </div>

    <p>
      To confirm your availability, please reply to this email.
    </p>
    <p>
      We look forward to speaking with you!<br><br>
      Warm regards,<br>
      <strong>{sender_name}</strong><br>
      {company_name}
    </p>
  </div>
  <div class="footer">
    This invitation was sent via RecruitIQ Platform
  </div>
</div>
</body>
</html>
"""

        return {
            "subject":   subject,
            "body_html": body_html,
            "body_text": body_text,
        }

    # ── SMTP Send ─────────────────────────────────────────────────────────────

    @staticmethod
    def send_email(
        smtp_host: str,
        smtp_port: int,
        sender_email: str,
        sender_password: str,
        recipient_emails: List[str],
        subject: str,
        body_html: str,
        body_text: str,
        use_tls: bool = True,
    ) -> Tuple[bool, str]:
        """
        Send email via SMTP.
        Returns (success, message)
        """
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"]    = sender_email
            msg["To"]      = ", ".join(recipient_emails)

            msg.attach(MIMEText(body_text, "plain"))
            msg.attach(MIMEText(body_html,  "html"))

            if use_tls:
                server = smtplib.SMTP(smtp_host, smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(smtp_host, smtp_port)

            server.login(sender_email, sender_password)
            server.sendmail(
                sender_email,
                recipient_emails,
                msg.as_string()
            )
            server.quit()
            return True, f"✅ Email sent to {', '.join(recipient_emails)}"

        except Exception as e:
            return False, f"❌ SMTP error: {e}"


# Fix missing Tuple import at top


# ─────────────────────────────────────────────────────────────────────────────
#  INTERVIEW SCHEDULER  (core logic)
# ─────────────────────────────────────────────────────────────────────────────

class InterviewScheduler:

    def __init__(self, db_manager, ai_handler):
        self.db              = db_manager
        self.ai              = ai_handler
        self.question_gen    = InterviewQuestionGenerator(ai_handler)
        self.email_composer  = InterviewEmailComposer(ai_handler)

    def schedule(
        self,
        application_id: str,
        candidate: dict,
        jd: dict,
        interview_type: str,
        scheduled_at: datetime,
        duration_minutes: int,
        meeting_link: str,
        interviewer_emails: List[str],
        notes: str = "",
    ) -> Dict:
        """
        Schedule interview + move pipeline stage.
        Returns {"success": bool, "message": str, "application_id": str}
        """
        try:
            # 1) Update application record
            update_data = {
                "interview_scheduled_at": scheduled_at.isoformat(),
                "interview_type":         interview_type,
                "interview_link":         meeting_link,
                "interviewer_emails":     interviewer_emails,
                "notes":                  notes,
            }

            # Map interview type to pipeline stage
            stage_map = {
                "Screening Call":      "Screening",
                "Technical Round 1":   "Technical L1",
                "Technical Round 2":   "Technical L2",
                "System Design":       "Technical L2",
                "Managerial Round":    "Managerial",
                "HR Round":            "HR Round",
                "Client Round":        "Client Round",
                "Final Round":         "Offer",
                "Assignment Review":   "Technical L1",
                "Bar Raiser":          "Technical L2",
            }
            new_stage = stage_map.get(interview_type, "Screening")

            # Update via DB manager
            # (application update + stage change)
            ok = self.db.update_application_stage(
                application_id, new_stage, notes
            )

            if ok:
                return {
                    "success":        True,
                    "message":        f"Interview scheduled for {scheduled_at.strftime('%d %b %Y %I:%M %p')}",
                    "application_id": application_id,
                    "new_stage":      new_stage,
                }
            else:
                return {
                    "success": False,
                    "message": "Failed to update application record.",
                    "application_id": application_id,
                }

        except Exception as e:
            return {
                "success": False,
                "message": f"Scheduler error: {e}",
                "application_id": application_id,
            }

    def get_upcoming(self, days_ahead: int = 7) -> List[dict]:
        return self.db.get_upcoming_interviews(days_ahead)


# ─────────────────────────────────────────────────────────────────────────────
#  STREAMLIT UI COMPONENT
# ─────────────────────────────────────────────────────────────────────────────

def render_scheduler_ui(
    application_id: str,
    candidate: dict,
    jd: dict,
    db_manager,
    ai_handler,
):
    """
    Render the full interview scheduling UI for one application.
    Call from the pipeline/application detail view.
    """
    scheduler = InterviewScheduler(db_manager, ai_handler)
    composer  = InterviewEmailComposer(ai_handler)

    st.subheader(
        f"📅 Schedule Interview — "
        f"{candidate.get('full_name', 'Candidate')} "
        f"for {jd.get('role_name', 'Role')}"
    )

    tab1, tab2, tab3 = st.tabs([
        "📅 Schedule",
        "❓ Questions",
        "📧 Email Invite"
    ])

    # ── TAB 1: Schedule ───────────────────────────────────────────────────────
    with tab1:
        col1, col2 = st.columns(2)

        with col1:
            interview_type = st.selectbox(
                "Interview Type",
                INTERVIEW_TYPES,
                key=f"itype_{application_id}"
            )
            interview_date = st.date_input(
                "Date",
                min_value=datetime.today(),
                key=f"idate_{application_id}"
            )
            interview_time = st.time_input(
                "Time (IST)",
                key=f"itime_{application_id}"
            )

        with col2:
            duration = st.selectbox(
                "Duration (minutes)",
                DURATION_OPTIONS,
                index=3,   # default 60
                key=f"idur_{application_id}"
            )
            platform = st.selectbox(
                "Meeting Platform",
                list(MEETING_PLATFORMS.keys()),
                key=f"iplat_{application_id}"
            )
            meeting_link = st.text_input(
                "Meeting Link",
                value=MEETING_PLATFORMS.get(platform, ""),
                placeholder="https://meet.google.com/xxx-yyyy-zzz",
                key=f"ilink_{application_id}"
            )

        interviewer_input = st.text_area(
            "Interviewer Emails (one per line)",
            placeholder="interviewer1@company.com\ninterviewer2@company.com",
            height=80,
            key=f"iemails_{application_id}"
        )
        notes = st.text_area(
            "Notes for candidate",
            placeholder="Bring original documents, wear formal attire...",
            height=60,
            key=f"inotes_{application_id}"
        )

        if st.button(
            "✅ Schedule Interview",
            key=f"isched_{application_id}",
            type="primary",
            use_container_width=True
        ):
            scheduled_dt = datetime.combine(interview_date, interview_time)
            interviewers = [
                e.strip()
                for e in interviewer_input.split("\n")
                if e.strip()
            ]

            result = scheduler.schedule(
                application_id=application_id,
                candidate=candidate,
                jd=jd,
                interview_type=interview_type,
                scheduled_at=scheduled_dt,
                duration_minutes=duration,
                meeting_link=meeting_link,
                interviewer_emails=interviewers,
                notes=notes,
            )

            if result["success"]:
                st.success(result["message"])
                st.info(f"Pipeline stage → **{result['new_stage']}**")
                st.session_state[f"scheduled_at_{application_id}"] = scheduled_dt
                st.session_state[f"sched_data_{application_id}"] = {
                    "interview_type": interview_type,
                    "scheduled_at":   scheduled_dt,
                    "duration":       duration,
                    "meeting_link":   meeting_link,
                    "interviewers":   interviewers,
                }
            else:
                st.error(result["message"])

    # ── TAB 2: Questions ──────────────────────────────────────────────────────
    with tab2:
        qgen = InterviewQuestionGenerator(ai_handler)

        col1, col2 = st.columns(2)
        with col1:
            q_type = st.selectbox(
                "Interview Type (for questions)",
                INTERVIEW_TYPES,
                key=f"qtype_{application_id}"
            )
        with col2:
            num_q = st.slider(
                "Total questions",
                min_value=4,
                max_value=20,
                value=8,
                key=f"qnum_{application_id}"
            )

        if st.button(
            "🤖 Generate AI Questions",
            key=f"qgen_{application_id}",
            use_container_width=True
        ):
            with st.spinner("Generating tailored questions..."):
                questions = qgen.generate(
                    candidate=candidate,
                    jd=jd,
                    interview_type=q_type,
                    num_questions=num_q,
                )
            st.session_state[
                f"questions_{application_id}"
            ] = questions

        if f"questions_{application_id}" in st.session_state:
            questions = st.session_state[f"questions_{application_id}"]
            cat_icons = {
                "technical":    "⚙️",
                "behavioral":   "🧠",
                "situational":  "🎯",
                "culture_fit":  "🤝",
            }

            for category, qs in questions.items():
                if qs:
                    icon = cat_icons.get(category, "❓")
                    st.markdown(
                        f"**{icon} {category.replace('_', ' ').title()}**"
                    )
                    for i, q in enumerate(qs, 1):
                        st.markdown(f"{i}. {q}")
                    st.markdown("---")

            # Download as text
            full_text = "\n\n".join([
                f"{'='*40}\n{cat.upper()}\n{'='*40}\n" +
                "\n".join(f"{i+1}. {q}" for i, q in enumerate(qs))
                for cat, qs in questions.items() if qs
            ])
            st.download_button(
                "📄 Download Questions",
                data=full_text,
                file_name=f"interview_questions_{application_id[:8]}.txt",
                mime="text/plain",
                key=f"qdl_{application_id}"
            )

    # ── TAB 3: Email Invite ───────────────────────────────────────────────────
    with tab3:
        sched_data = st.session_state.get(f"sched_data_{application_id}")

        if not sched_data:
            st.info("⬅️ Schedule the interview first (Tab 1), then compose the invite here.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                sender_name  = st.text_input(
                    "Your Name",
                    value="Recruitment Team",
                    key=f"sndr_{application_id}"
                )
                company_name = st.text_input(
                    "Company / Agency Name",
                    value="RecruitIQ",
                    key=f"cname_{application_id}"
                )
            with col2:
                additional_notes = st.text_area(
                    "Additional notes for invite",
                    height=80,
                    key=f"anotes_{application_id}"
                )

            if st.button(
                "📝 Compose Email",
                key=f"compose_{application_id}",
                use_container_width=True
            ):
                email_content = composer.compose_invitation(
                    candidate=candidate,
                    jd=jd,
                    interview_type=sched_data["interview_type"],
                    scheduled_at=sched_data["scheduled_at"],
                    duration_minutes=sched_data["duration"],
                    meeting_link=sched_data["meeting_link"],
                    interviewer_names=sched_data["interviewers"],
                    sender_name=sender_name,
                    company_name=company_name,
                    additional_notes=additional_notes,
                )
                st.session_state[
                    f"email_content_{application_id}"
                ] = email_content

            if f"email_content_{application_id}" in st.session_state:
                ec = st.session_state[f"email_content_{application_id}"]

                st.text_input(
                    "Subject",
                    value=ec["subject"],
                    key=f"esubj_{application_id}"
                )
                st.text_area(
                    "Email Body (Plain Text)",
                    value=ec["body_text"],
                    height=300,
                    key=f"ebody_{application_id}"
                )

                # SMTP send option
                with st.expander("📤 Send via SMTP"):
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        smtp_host  = st.text_input(
                            "SMTP Host",
                            value="smtp.gmail.com",
                            key=f"shost_{application_id}"
                        )
                        smtp_email = st.text_input(
                            "From Email",
                            key=f"semail_{application_id}"
                        )
                    with sc2:
                        smtp_port = st.number_input(
                            "SMTP Port",
                            value=587,
                            key=f"sport_{application_id}"
                        )
                        smtp_pass = st.text_input(
                            "Password / App Password",
                            type="password",
                            key=f"spass_{application_id}"
                        )

                    recipient_email = candidate.get("email", "")
                    st.text_input(
                        "Recipient Email",
                        value=recipient_email,
                        key=f"srec_{application_id}"
                    )

                    if st.button(
                        "📨 Send Email Now",
                        key=f"ssend_{application_id}",
                        type="primary"
                    ):
                        if not smtp_email or not smtp_pass:
                            st.error("Enter SMTP credentials.")
                        elif not recipient_email:
                            st.error("No candidate email found.")
                        else:
                            ok, msg = InterviewEmailComposer.send_email(
                                smtp_host=smtp_host,
                                smtp_port=int(smtp_port),
                                sender_email=smtp_email,
                                sender_password=smtp_pass,
                                recipient_emails=[recipient_email],
                                subject=ec["subject"],
                                body_html=ec["body_html"],
                                body_text=ec["body_text"],
                            )
                            if ok:
                                st.success(msg)
                            else:
                                st.error(msg)


def render_upcoming_interviews_widget(db_manager, days_ahead: int = 7):
    """
    Compact widget showing upcoming interviews.
    Use on Dashboard page.
    """
    interviews = db_manager.get_upcoming_interviews(days_ahead)

    if not interviews:
        st.info(f"No interviews in the next {days_ahead} days.")
        return

    st.markdown(f"### 📅 Upcoming Interviews ({len(interviews)})")

    for iv in interviews:
        scheduled = iv.get("interview_scheduled_at", "")
        try:
            dt = datetime.fromisoformat(scheduled)
            date_display = dt.strftime("%d %b %Y, %I:%M %p")
        except Exception:
            date_display = scheduled

        name     = iv.get("full_name", "Unknown")
        role     = iv.get("role_name", "Unknown Role")
        iv_type  = iv.get("interview_type", "Interview")
        link     = iv.get("interview_link", "")

        with st.container():
            col1, col2, col3 = st.columns([3, 2, 1])
            with col1:
                st.markdown(f"**{name}** — {role}")
                st.caption(f"🎤 {iv_type}")
            with col2:
                st.markdown(f"📅 {date_display}")
            with col3:
                if link:
                    st.link_button("Join 🔗", link)
            st.divider()
