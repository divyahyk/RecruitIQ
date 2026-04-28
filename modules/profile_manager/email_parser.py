from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
# modules/profile_manager/email_parser.py

"""
Email-based Resume Parser
─────────────────────────
Connects to Gmail / Outlook via IMAP, downloads resume attachments,
extracts candidate data using AI, and bulk-inserts into the DB.

Supported attachment types: PDF, DOCX, DOC, TXT
"""

import email
import imaplib
import io
import os
import re
import tempfile
from datetime import datetime
from email.header import decode_header
from pathlib import Path

import streamlit as st


# ─────────────────────────────────────────────────────────────────────────────
#  IMAP CONFIG PRESETS
# ─────────────────────────────────────────────────────────────────────────────

IMAP_PRESETS = {
    "Gmail":        {"host": "imap.gmail.com",     "port": 993},
    "Outlook":      {"host": "outlook.office365.com", "port": 993},
    "Yahoo":        {"host": "imap.mail.yahoo.com", "port": 993},
    "Zoho":         {"host": "imap.zoho.com",       "port": 993},
    "Custom IMAP":  {"host": "",                    "port": 993},
}

RESUME_EXTENSIONS = {".pdf", ".docx", ".doc", ".txt", ".rtf"}

RESUME_KEYWORDS = [
    "resume", "cv", "curriculum", "vitae", "profile",
    "biodata", "bio-data", "bio_data"
]


# ─────────────────────────────────────────────────────────────────────────────
#  ATTACHMENT TEXT EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

def extract_text_from_bytes(
    file_bytes: bytes,
    filename: str
) -> str:
    """
    Extract plain text from resume bytes based on file extension.
    Returns empty string on failure (never raises).
    """
    ext = Path(filename).suffix.lower()

    try:
        if ext == ".pdf":
            import fitz  # PyMuPDF
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            return "\n".join(page.get_text() for page in doc)

        elif ext in (".docx",):
            from docx import Document
            doc = Document(io.BytesIO(file_bytes))
            return "\n".join(p.text for p in doc.paragraphs)

        elif ext == ".doc":
            # Fallback: write to temp file and use textract if available
            try:
                import textract
                with tempfile.NamedTemporaryFile(
                    suffix=".doc", delete=False
                ) as tmp:
                    tmp.write(file_bytes)
                    tmp_path = tmp.name
                text = textract.process(tmp_path).decode("utf-8", errors="ignore")
                os.unlink(tmp_path)
                return text
            except ImportError:
                # Best-effort: decode raw bytes
                return file_bytes.decode("utf-8", errors="ignore")

        elif ext in (".txt", ".rtf"):
            return file_bytes.decode("utf-8", errors="ignore")

    except Exception as e:
        print(f"⚠️  Text extraction failed for {filename}: {e}")

    return ""


# ─────────────────────────────────────────────────────────────────────────────
#  IMAP EMAIL CONNECTOR
# ─────────────────────────────────────────────────────────────────────────────

class IMAPResumeConnector:
    """
    Connects to an IMAP mailbox, searches for resume emails,
    extracts attachments, and returns structured candidate data.
    """

    def __init__(
        self,
        host: str,
        port: int,
        email_address: str,
        password: str,
        mailbox: str = "INBOX",
        use_ssl: bool = True,
    ):
        self.host          = host
        self.port          = port
        self.email_address = email_address
        self.password      = password
        self.mailbox       = mailbox
        self.use_ssl       = use_ssl
        self._imap: Optional[imaplib.IMAP4_SSL] = None

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> Tuple[bool, str]:
        """Connect and authenticate. Returns (success, message)."""
        try:
            if self.use_ssl:
                self._imap = imaplib.IMAP4_SSL(self.host, self.port)
            else:
                self._imap = imaplib.IMAP4(self.host, self.port)

            self._imap.login(self.email_address, self.password)
            self._imap.select(self.mailbox)
            return True, f"✅ Connected to {self.host}"
        except imaplib.IMAP4.error as e:
            return False, f"❌ IMAP auth failed: {e}"
        except Exception as e:
            return False, f"❌ Connection error: {e}"

    def disconnect(self):
        if self._imap:
            try:
                self._imap.close()
                self._imap.logout()
            except Exception:
                pass
            self._imap = None

    # ── Search ────────────────────────────────────────────────────────────────

    def search_resume_emails(
        self,
        since_days: int = 30,
        subject_filter: str = "",
        sender_filter: str = "",
        max_emails: int = 100,
    ) -> List[bytes]:
        """
        Search mailbox for resume-related emails.
        Returns list of raw email byte strings.
        """
        if not self._imap:
            return []

        # Build IMAP search criteria
        criteria_parts = []

        # Date filter
        since_date = datetime.utcnow()
        from datetime import timedelta
        since_dt = since_date - timedelta(days=since_days)
        imap_date = since_dt.strftime("%d-%b-%Y")
        criteria_parts.append(f'SINCE {imap_date}')

        # Subject filter
        if subject_filter:
            criteria_parts.append(f'SUBJECT "{subject_filter}"')

        # Sender filter
        if sender_filter:
            criteria_parts.append(f'FROM "{sender_filter}"')

        criteria = " ".join(criteria_parts) if criteria_parts else "ALL"

        try:
            status, message_ids = self._imap.search(None, criteria)
            if status != "OK":
                return []

            ids = message_ids[0].split()
            # Most recent first, limited
            ids = ids[-max_emails:][::-1]

            raw_emails = []
            for eid in ids:
                try:
                    status, msg_data = self._imap.fetch(eid, "(RFC822)")
                    if status == "OK" and msg_data[0]:
                        raw_emails.append(msg_data[0][1])
                except Exception:
                    continue

            return raw_emails

        except Exception as e:
            print(f"Search error: {e}")
            return []

    # ── Parse ─────────────────────────────────────────────────────────────────

    def extract_attachments(
        self,
        raw_email_bytes: bytes
    ) -> List[Dict]:
        """
        Extract resume attachments from a raw email.
        Returns list of {"filename": str, "bytes": bytes, "sender": str,
                         "subject": str, "date": str}
        """
        attachments = []

        try:
            msg = email.message_from_bytes(raw_email_bytes)

            # Decode subject
            raw_subject = msg.get("Subject", "")
            subject = self._decode_header_value(raw_subject)

            # Sender
            sender = msg.get("From", "")

            # Date
            date_str = msg.get("Date", "")

            for part in msg.walk():
                content_disp = str(part.get("Content-Disposition", ""))
                content_type = part.get_content_type()
                filename     = part.get_filename()

                if filename:
                    filename = self._decode_header_value(filename)
                    ext = Path(filename).suffix.lower()

                    if ext in RESUME_EXTENSIONS:
                        try:
                            file_bytes = part.get_payload(decode=True)
                            if file_bytes:
                                attachments.append({
                                    "filename": filename,
                                    "bytes":    file_bytes,
                                    "sender":   sender,
                                    "subject":  subject,
                                    "date":     date_str,
                                })
                        except Exception:
                            continue

                # Also check inline text/plain for resume content
                elif (
                    content_type == "text/plain"
                    and "attachment" not in content_disp
                    and self._subject_is_resume(subject)
                ):
                    try:
                        body = part.get_payload(decode=True)
                        if body and len(body) > 200:
                            attachments.append({
                                "filename": "email_body.txt",
                                "bytes":    body,
                                "sender":   sender,
                                "subject":  subject,
                                "date":     date_str,
                            })
                    except Exception:
                        continue

        except Exception as e:
            print(f"Attachment extraction error: {e}")

        return attachments

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _decode_header_value(value: str) -> str:
        """Decode MIME-encoded header values."""
        try:
            parts = decode_header(value)
            decoded = []
            for text, charset in parts:
                if isinstance(text, bytes):
                    decoded.append(
                        text.decode(charset or "utf-8", errors="ignore")
                    )
                else:
                    decoded.append(str(text))
            return " ".join(decoded).strip()
        except Exception:
            return str(value)

    @staticmethod
    def _subject_is_resume(subject: str) -> bool:
        s = subject.lower()
        return any(kw in s for kw in RESUME_KEYWORDS)


# ─────────────────────────────────────────────────────────────────────────────
#  AI-POWERED RESUME → CANDIDATE RECORD PARSER
# ─────────────────────────────────────────────────────────────────────────────

class EmailResumeParser:
    """
    High-level orchestrator:
    Email → Attachments → Text → AI parse → Candidate records → DB insert
    """

    def __init__(self, ai_handler, db_manager):
        self.ai  = ai_handler
        self.db  = db_manager

    # ── Parse single resume text ──────────────────────────────────────────────

    def parse_resume_text(self, text: str, source_email: str = "") -> dict:
        """
        Use AI to parse raw resume text into structured candidate data.
        Returns a dict matching the candidates table schema.
        """
        prompt = f"""
You are an expert resume parser. Extract structured data from the resume below.
Return ONLY a valid JSON object with these exact keys:

{{
  "full_name": "string",
  "email": "string or empty",
  "phone": "string or empty",
  "linkedin_url": "string or empty",
  "current_title": "string",
  "current_company": "string",
  "total_experience": number (years, float),
  "skills": ["skill1", "skill2", ...],
  "education": "highest degree and institution",
  "current_location": "City, Country",
  "preferred_locations": ["city1", "city2"],
  "work_mode_preference": "Remote|Hybrid|Onsite|Any",
  "current_ctc": "string or empty",
  "expected_ctc": "string or empty",
  "notice_period": "string (e.g. 30 days, Immediate)",
  "ai_summary": "2-3 sentence professional summary"
}}

Rules:
- total_experience must be a number (e.g. 5.5 for 5 years 6 months)
- skills must be a flat list of strings
- Return ONLY the JSON object, no markdown, no explanation

RESUME:
{text[:4000]}
"""
        try:
            response = self.ai.generate(prompt)
            # Strip markdown fences if present
            clean = re.sub(r"```(?:json)?|```", "", response).strip()
            # Extract first JSON object
            match = re.search(r"\{.*\}", clean, re.DOTALL)
            if match:
                import json
                data = json.loads(match.group())
                # Sanitise
                data["total_experience"] = float(
                    data.get("total_experience") or 0
                )
                if not isinstance(data.get("skills"), list):
                    data["skills"] = []
                if not isinstance(data.get("preferred_locations"), list):
                    data["preferred_locations"] = []
                if source_email and not data.get("email"):
                    data["email"] = source_email
                return data
        except Exception as e:
            print(f"AI parse error: {e}")

        # Fallback: basic regex extraction
        return self._regex_fallback(text, source_email)

    # ── Regex fallback ────────────────────────────────────────────────────────

    @staticmethod
    def _regex_fallback(text: str, source_email: str = "") -> dict:
        """Simple regex-based extraction when AI fails."""
        lines = [l.strip() for l in text.split("\n") if l.strip()]

        # Email
        email_match = re.search(
            r'[\w.+-]+@[\w-]+\.[a-z]{2,}', text, re.IGNORECASE
        )
        found_email = email_match.group() if email_match else source_email

        # Phone
        phone_match = re.search(
            r'(?:\+?\d[\d\s\-().]{8,14}\d)', text
        )
        found_phone = phone_match.group().strip() if phone_match else ""

        # LinkedIn
        linkedin_match = re.search(
            r'linkedin\.com/in/[\w\-]+', text, re.IGNORECASE
        )
        linkedin = (
            "https://" + linkedin_match.group()
            if linkedin_match else ""
        )

        # Name: best guess = first non-empty line
        name = lines[0] if lines else "Unknown"
        if len(name) > 60 or "@" in name:
            name = "Unknown"

        # Experience: look for "X years" pattern
        exp_match = re.search(
            r'(\d+(?:\.\d+)?)\s*\+?\s*(?:years?|yrs?)', text, re.IGNORECASE
        )
        experience = float(exp_match.group(1)) if exp_match else 0.0

        # Skills: lines containing common tech keywords
        skill_patterns = [
            "python", "java", "javascript", "react", "angular", "node",
            "sql", "aws", "azure", "docker", "kubernetes", "machine learning",
            "data science", "power bi", "tableau", "excel", "salesforce",
            "sap", "oracle", "c++", "c#", ".net", "php", "ruby", "swift",
        ]
        found_skills = [
            sk for sk in skill_patterns
            if sk.lower() in text.lower()
        ]

        return {
            "full_name":            name,
            "email":                found_email,
            "phone":                found_phone,
            "linkedin_url":         linkedin,
            "current_title":        "",
            "current_company":      "",
            "total_experience":     experience,
            "skills":               found_skills,
            "education":            "",
            "current_location":     "",
            "preferred_locations":  [],
            "work_mode_preference": "",
            "current_ctc":          "",
            "expected_ctc":         "",
            "notice_period":        "",
            "ai_summary":           f"Resume parsed via email. {experience} years experience."
        }

    # ── Full pipeline: emails → DB ────────────────────────────────────────────

    def process_email_batch(
        self,
        connector: IMAPResumeConnector,
        since_days: int = 30,
        subject_filter: str = "",
        sender_filter: str = "",
        max_emails: int = 100,
        progress_callback=None,
    ) -> Dict:
        """
        Full pipeline: fetch emails → extract → parse → insert.
        Returns {"processed": N, "inserted": N, "skipped": N, "errors": N}
        """
        stats = {
            "emails_fetched":   0,
            "attachments_found": 0,
            "processed":        0,
            "inserted":         0,
            "skipped":          0,
            "errors":           0,
        }

        # 1) Fetch raw emails
        raw_emails = connector.search_resume_emails(
            since_days=since_days,
            subject_filter=subject_filter,
            sender_filter=sender_filter,
            max_emails=max_emails,
        )
        stats["emails_fetched"] = len(raw_emails)

        if not raw_emails:
            return stats

        # 2) Extract attachments
        all_attachments = []
        for raw in raw_emails:
            atts = connector.extract_attachments(raw)
            all_attachments.extend(atts)

        stats["attachments_found"] = len(all_attachments)

        if not all_attachments:
            return stats

        # 3) Parse + insert
        candidate_records = []
        for i, att in enumerate(all_attachments):
            try:
                if progress_callback:
                    progress_callback(
                        i + 1,
                        len(all_attachments),
                        f"Parsing {att['filename']}..."
                    )

                text = extract_text_from_bytes(att["bytes"], att["filename"])
                if not text.strip():
                    stats["errors"] += 1
                    continue

                # Extract sender email for fallback
                sender_email = re.search(
                    r'[\w.+-]+@[\w-]+\.[a-z]{2,}',
                    att.get("sender", ""),
                    re.IGNORECASE
                )
                sender_email_str = (
                    sender_email.group() if sender_email else ""
                )

                record = self.parse_resume_text(text, sender_email_str)
                record["source"] = "Email Import"
                candidate_records.append(record)
                stats["processed"] += 1

            except Exception as e:
                stats["errors"] += 1
                print(f"Parse error for {att.get('filename')}: {e}")

        # 4) Bulk insert
        if candidate_records:
            result = self.db.bulk_insert_candidates(
                candidate_records, source="Email Import"
            )
            stats["inserted"] = result.get("inserted", 0)
            stats["skipped"]  = result.get("skipped", 0)
            stats["errors"]  += result.get("errors", 0)

        return stats


# ─────────────────────────────────────────────────────────────────────────────
#  STREAMLIT UI COMPONENT
# ─────────────────────────────────────────────────────────────────────────────

def render_email_parser_ui(ai_handler, db_manager):
    """
    Self-contained Streamlit UI for the email parser module.
    Call this from your pages/profile_manager.py
    """
    st.subheader("📧 Import Resumes from Email")

    # ── Connection settings ───────────────────────────────────────────────────
    with st.expander("⚙️ Email Connection Settings", expanded=True):
        col1, col2 = st.columns(2)

        with col1:
            provider = st.selectbox(
                "Email Provider",
                list(IMAP_PRESETS.keys()),
                key="email_provider"
            )
            host = st.text_input(
                "IMAP Host",
                value=IMAP_PRESETS[provider]["host"],
                key="imap_host"
            )
            port = st.number_input(
                "Port",
                value=IMAP_PRESETS[provider]["port"],
                min_value=1,
                max_value=65535,
                key="imap_port"
            )

        with col2:
            email_addr = st.text_input(
                "Email Address",
                placeholder="recruiter@company.com",
                key="imap_email"
            )
            password = st.text_input(
                "Password / App Password",
                type="password",
                key="imap_pass",
                help=(
                    "For Gmail: use App Password from "
                    "myaccount.google.com/apppasswords"
                )
            )
            mailbox = st.text_input(
                "Mailbox / Folder",
                value="INBOX",
                key="imap_mailbox"
            )

    # ── Search filters ────────────────────────────────────────────────────────
    with st.expander("🔍 Search Filters"):
        col1, col2, col3 = st.columns(3)
        with col1:
            since_days = st.slider(
                "Emails from last N days",
                min_value=1,
                max_value=365,
                value=30,
                key="email_since_days"
            )
        with col2:
            subject_filter = st.text_input(
                "Subject contains",
                placeholder="Resume, CV, Application...",
                key="email_subject"
            )
        with col3:
            max_emails = st.number_input(
                "Max emails to scan",
                min_value=1,
                max_value=500,
                value=100,
                key="email_max"
            )
        sender_filter = st.text_input(
            "From (optional)",
            placeholder="noreply@naukri.com",
            key="email_sender"
        )

    # ── Actions ───────────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔌 Test Connection", use_container_width=True):
            if not email_addr or not password:
                st.error("Enter email and password first.")
            else:
                connector = IMAPResumeConnector(
                    host=host,
                    port=int(port),
                    email_address=email_addr,
                    password=password,
                    mailbox=mailbox,
                )
                success, msg = connector.connect()
                if success:
                    st.success(msg)
                    connector.disconnect()
                else:
                    st.error(msg)

    with col2:
        if st.button(
            "📥 Fetch & Import Resumes",
            use_container_width=True,
            type="primary"
        ):
            if not email_addr or not password:
                st.error("Enter email credentials first.")
                return

            connector = IMAPResumeConnector(
                host=host,
                port=int(port),
                email_address=email_addr,
                password=password,
                mailbox=mailbox,
            )
            ok, msg = connector.connect()
            if not ok:
                st.error(msg)
                return

            parser = EmailResumeParser(ai_handler, db_manager)

            progress_bar = st.progress(0)
            status_text  = st.empty()

            def update_progress(current, total, label):
                progress_bar.progress(int(current / total * 100))
                status_text.text(f"[{current}/{total}] {label}")

            with st.spinner("Processing emails..."):
                result = parser.process_email_batch(
                    connector=connector,
                    since_days=since_days,
                    subject_filter=subject_filter,
                    sender_filter=sender_filter,
                    max_emails=int(max_emails),
                    progress_callback=update_progress,
                )
                connector.disconnect()

            progress_bar.progress(100)
            status_text.empty()

            # Results
            st.success("✅ Import complete!")
            rc1, rc2, rc3, rc4, rc5 = st.columns(5)
            rc1.metric("Emails Scanned",    result["emails_fetched"])
            rc2.metric("Attachments Found", result["attachments_found"])
            rc3.metric("Parsed",            result["processed"])
            rc4.metric("Inserted",          result["inserted"])
            rc5.metric("Skipped/Dupes",     result["skipped"])

    # ── Tips ──────────────────────────────────────────────────────────────────
    with st.expander("💡 Setup Tips"):
        st.markdown("""
**Gmail Setup:**
1. Enable 2-Step Verification in Google Account
2. Go to `myaccount.google.com` → Security → App Passwords
3. Create app password for "Mail"
4. Use that 16-character password here

**Outlook/Office 365:**
- Enable IMAP in Outlook settings
- Use your regular O365 password

**What gets scanned:**
- PDF, DOCX, DOC, TXT resume attachments
- Email body text (if subject contains resume keywords)
- Emails from the last N days you specify
        """)
