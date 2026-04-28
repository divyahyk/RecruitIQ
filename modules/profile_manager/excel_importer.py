# modules/profile_manager/excel_importer.py

import io
import hashlib
import re
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Union, Set, Callable
from database.db_factory import get_db_manager
from modules.ai_engine.llm_handler import LLMHandler


# Column aliases for common LinkedIn export formats
COLUMN_ALIASES = {
    "full_name":            ["full name", "name", "candidate name",
                             "first name", "firstname"],
    "email":                ["email", "email address", "e-mail"],
    "phone":                ["phone", "mobile", "contact number",
                             "phone number", "mobile number"],
    "linkedin_url":         ["linkedin", "linkedin url", "profile url",
                             "linkedin profile"],
    "current_title":        ["title", "current title", "job title",
                             "designation", "position", "headline"],
    "current_company":      ["company", "current company", "employer",
                             "organisation", "organization"],
    "total_experience":     ["experience", "total experience", "exp",
                             "years of experience", "yoe", "total exp"],
    "skills":               ["skills", "skill set", "skillset",
                             "key skills", "technical skills"],
    "education":            ["education", "qualification", "degree"],
    "current_location":     ["location", "city", "current location",
                             "current city", "base location"],
    "work_mode_preference": ["work mode", "work preference",
                             "remote preference"],
    "current_ctc":          ["current ctc", "current salary",
                             "ctc", "salary"],
    "expected_ctc":         ["expected ctc", "expected salary",
                             "expectation"],
    "notice_period":        ["notice period", "notice", "availability",
                             "joining time"],
}


class LinkedInExcelImporter:

    CHUNK_SIZE = 1000

    def __init__(self, db, llm: LLMHandler):
        self.db = db
        self.llm = llm

    def import_file(
        self,
        file_bytes: bytes,
        filename: str,
        source: str = "LinkedIn_Excel",
        progress_callback: Optional[Callable] = None
    ) -> Dict:
        ext = filename.lower().rsplit(".", 1)[-1]
        try:
            if ext in ("xlsx", "xls"):
                df = pd.read_excel(io.BytesIO(file_bytes))
            elif ext == "csv":
                df = pd.read_csv(io.BytesIO(file_bytes))
            else:
                return {"success": False, "error": f"Unsupported: {ext}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

        df.columns = [str(c).strip().lower() for c in df.columns]
        col_map = self._build_column_map(df.columns.tolist())

        total = len(df)
        stats = {"inserted": 0, "skipped": 0, "errors": 0, "total": total}

        for chunk_start in range(0, total, self.CHUNK_SIZE):
            chunk = df.iloc[chunk_start: chunk_start + self.CHUNK_SIZE]
            records = []

            for _, row in chunk.iterrows():
                try:
                    rec = self._row_to_record(row, col_map)
                    if rec:
                        records.append(rec)
                except Exception:
                    stats["errors"] += 1

            if records:
                result = self.db.bulk_insert_candidates(records, source=source)
                stats["inserted"] += result.get("inserted", 0)
                stats["skipped"]  += result.get("skipped", 0)
                stats["errors"]   += result.get("errors", 0)

            if progress_callback:
                done = min(chunk_start + self.CHUNK_SIZE, total)
                progress_callback(done / total, done, total, stats)

        stats["success"] = True
        return stats

    def _build_column_map(self, cols: List[str]) -> Dict[str, str]:
        """Map canonical field names → actual dataframe column names"""
        mapping = {}
        for field, aliases in COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in cols:
                    mapping[field] = alias
                    break
            if field not in mapping:
                # Try partial match
                for col in cols:
                    if any(a in col for a in aliases):
                        mapping[field] = col
                        break
        return mapping

    def _row_to_record(self, row: pd.Series, col_map: Dict) -> Optional[Dict]:
        def get(field, default=""):
            col = col_map.get(field)
            if col and col in row.index:
                val = row[col]
                if pd.isna(val):
                    return default
                return str(val).strip()
            return default

        name = get("full_name")
        if not name or name.lower() in ("nan", "none", ""):
            return None

        skills_raw = get("skills")
        skills = self._parse_skills(skills_raw)

        exp_raw = get("total_experience")
        exp = self._parse_experience(exp_raw)

        return {
            "full_name":            name[:255],
            "email":                get("email")[:255].lower(),
            "phone":                self._clean_phone(get("phone")),
            "linkedin_url":         get("linkedin_url")[:500],
            "current_title":        get("current_title")[:255],
            "current_company":      get("current_company")[:255],
            "total_experience":     exp,
            "skills":               skills,
            "education":            get("education")[:500],
            "current_location":     get("current_location")[:255],
            "work_mode_preference": get("work_mode_preference")[:100],
            "current_ctc":          get("current_ctc")[:100],
            "expected_ctc":         get("expected_ctc")[:100],
            "notice_period":        get("notice_period")[:100],
            "raw_data":             row.to_dict()
        }

    def _parse_skills(self, raw: str) -> List[str]:
        if not raw:
            return []
        for sep in [",", ";", "|", "\n"]:
            if sep in raw:
                return [s.strip() for s in raw.split(sep) if s.strip()][:50]
        return [raw.strip()][:50] if raw.strip() else []

    def _parse_experience(self, raw: str) -> float:
        if not raw:
            return 0.0
        nums = re.findall(r'\d+\.?\d*', str(raw))
        if nums:
            return round(min(float(nums[0]), 50), 1)
        return 0.0

    def _clean_phone(self, raw: str) -> str:
        cleaned = re.sub(r'[^\d+\-\s\(\)]', '', raw)
        return cleaned[:50]

    def get_column_preview(
        self, file_bytes: bytes, filename: str
    ) -> Dict:
        """Return first 5 rows for column mapping preview"""
        try:
            ext = filename.lower().rsplit(".", 1)[-1]
            if ext in ("xlsx", "xls"):
                df = pd.read_excel(io.BytesIO(file_bytes), nrows=5)
            else:
                df = pd.read_csv(io.BytesIO(file_bytes), nrows=5)

            df.columns = [str(c).strip().lower() for c in df.columns]
            col_map = self._build_column_map(df.columns.tolist())

            return {
                "columns": df.columns.tolist(),
                "detected_mapping": col_map,
                "sample": df.head(3).to_dict("records"),
                "total_columns": len(df.columns)
            }
        except Exception as e:
            return {"error": str(e)}
