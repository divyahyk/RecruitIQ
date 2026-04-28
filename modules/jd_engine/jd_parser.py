# modules/jd_engine/jd_parser.py

import io
import re
from typing import Dict, List, Tuple, Optional, Any, Union, Set, Callable
from modules.ai_engine.llm_handler import LLMHandler
from modules.ai_engine.prompt_templates import PromptTemplates


class JDParser:

    def __init__(self, llm: LLMHandler):
        self.llm = llm

    # ── File extraction ──

    def extract_text_from_file(
        self, file_bytes: bytes, filename: str
    ) -> str:
        ext = filename.lower().rsplit(".", 1)[-1]
        if ext == "pdf":
            return self._from_pdf(file_bytes)
        if ext in ("docx", "doc"):
            return self._from_docx(file_bytes)
        if ext == "txt":
            return file_bytes.decode("utf-8", errors="ignore")
        return ""

    def _from_pdf(self, data: bytes) -> str:
        try:
            import fitz
            doc = fitz.open(stream=data, filetype="pdf")
            return "\n".join(p.get_text() for p in doc)
        except Exception as e:
            return f"PDF extraction error: {e}"

    def _from_docx(self, data: bytes) -> str:
        try:
            from docx import Document
            doc = Document(io.BytesIO(data))
            return "\n".join(p.text for p in doc.paragraphs)
        except Exception as e:
            return f"DOCX extraction error: {e}"

    # ── AI parsing ──

    def parse(self, jd_text: str) -> dict:
        prompt = PromptTemplates.jd_parser(jd_text)
        response = self.llm.complete(prompt, max_tokens=1024)
        parsed = self.llm.extract_json(response) or {}

        # Defaults & sanitisation
        defaults = {
            "role_name": self._extract_title(jd_text),
            "client_name": "",
            "skillset_required": [],
            "skillset_good_to_have": [],
            "location": "",
            "work_mode": "Hybrid",
            "experience_min": 0.0,
            "experience_max": 5.0,
            "budget_min": 0.0,
            "budget_max": 0.0,
            "budget_currency": "INR",
            "notice_period_max": "60 days",
            "education_required": "",
            "industry_preference": "",
            "positions_count": 1,
        }
        for k, v in defaults.items():
            if k not in parsed or not parsed[k]:
                parsed[k] = v

        # Ensure numeric
        for f in ("experience_min", "experience_max",
                  "budget_min", "budget_max"):
            try:
                parsed[f] = float(parsed[f])
            except Exception:
                parsed[f] = defaults[f]

        parsed["raw_jd_text"] = jd_text
        parsed["ai_parsed_data"] = {
            "confidence": "high" if parsed["role_name"] else "low"
        }
        return parsed

    def _extract_title(self, text: str) -> str:
        """Fallback regex title extraction"""
        patterns = [
            r"(?:position|role|title|hiring for)[:\s]+([^\n]+)",
            r"^([A-Z][^\n]{5,60})$",
        ]
        for p in patterns:
            m = re.search(p, text, re.I | re.M)
            if m:
                return m.group(1).strip()[:100]
        return "Unknown Role"
