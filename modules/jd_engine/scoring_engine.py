from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
# modules/jd_engine/scoring_engine.py

import re
from modules.ai_engine.llm_handler import LLMHandler
from modules.ai_engine.prompt_templates import PromptTemplates


class ProfileScoringEngine:
    """
    100-point weighted scoring across 7 dimensions:
      Required Skills    → 45 pts
      Good-to-have Skills→ 10 pts  (was separate, now 55 combined)
      Experience         → 20 pts
      Budget             → 10 pts
      Location           → 10 pts
      Work Mode          →  8 pts
      Notice Period      →  7 pts
    Wait — original spec: 45+10+20+10+10+8+7 = 110 → normalise to 100
    """

    WEIGHTS = {
        "required_skills":  45,
        "nice_skills":      10,
        "experience":       20,
        "budget":           10,
        "location":         10,
        "work_mode":         8,
        "notice_period":     7,
    }
    # We'll score each out of its weight, total = 110, scale to 100

    def __init__(self, llm: LLMHandler):
        self.llm = llm

    def score(
        self, candidate: dict, jd: dict
    ) -> Tuple[float, Dict]:
        """Return (total_score_0_100, breakdown_dict)"""

        breakdown = {}

        # 1 ── Required Skills (45)
        req_skills = [s.lower() for s in (jd.get("skillset_required") or [])]
        cand_skills = [
            s.lower() for s in (candidate.get("skills") or [])
        ]
        if req_skills:
            matched = [s for s in req_skills if any(
                s in cs or cs in s for cs in cand_skills
            )]
            ratio = len(matched) / len(req_skills)
            req_score = self.WEIGHTS["required_skills"] * ratio
            breakdown["required_skills"] = {
                "score": round(req_score, 1),
                "max": self.WEIGHTS["required_skills"],
                "matched": matched,
                "missing": [s for s in req_skills if s not in matched],
                "detail": f"{len(matched)}/{len(req_skills)} required skills"
            }
        else:
            req_score = self.WEIGHTS["required_skills"]
            breakdown["required_skills"] = {
                "score": req_score, "max": req_score,
                "matched": [], "missing": [],
                "detail": "No required skills specified"
            }

        # 2 ── Nice-to-have Skills (10)
        nice_skills = [
            s.lower() for s in (jd.get("skillset_good_to_have") or [])
        ]
        if nice_skills:
            n_matched = [s for s in nice_skills if any(
                s in cs or cs in s for cs in cand_skills
            )]
            n_ratio = len(n_matched) / len(nice_skills)
            nice_score = self.WEIGHTS["nice_skills"] * n_ratio
            breakdown["nice_skills"] = {
                "score": round(nice_score, 1),
                "max": self.WEIGHTS["nice_skills"],
                "matched": n_matched,
                "detail": f"{len(n_matched)}/{len(nice_skills)} bonus skills"
            }
        else:
            nice_score = self.WEIGHTS["nice_skills"] * 0.5
            breakdown["nice_skills"] = {
                "score": nice_score, "max": self.WEIGHTS["nice_skills"],
                "matched": [], "detail": "No nice-to-have specified"
            }

        # 3 ── Experience (20)
        exp = float(candidate.get("total_experience") or 0)
        exp_min = float(jd.get("experience_min") or 0)
        exp_max = float(jd.get("experience_max") or 99)
        if exp_min <= exp <= exp_max:
            exp_score = self.WEIGHTS["experience"]
            exp_detail = f"{exp}y — within {exp_min}-{exp_max}y range ✓"
        elif exp < exp_min:
            gap = exp_min - exp
            exp_score = max(0, self.WEIGHTS["experience"] * (1 - gap / exp_min)) \
                if exp_min > 0 else 0
            exp_detail = f"{exp}y — {gap:.1f}y below minimum"
        else:
            # Over-experienced: slight penalty
            exp_score = self.WEIGHTS["experience"] * 0.85
            exp_detail = f"{exp}y — over-experienced (max {exp_max}y)"
        breakdown["experience"] = {
            "score": round(exp_score, 1),
            "max": self.WEIGHTS["experience"],
            "candidate_exp": exp,
            "jd_range": f"{exp_min}-{exp_max}",
            "detail": exp_detail
        }

        # 4 ── Budget (10)
        budget_score = self._score_budget(candidate, jd)
        breakdown["budget"] = {
            "score": round(budget_score, 1),
            "max": self.WEIGHTS["budget"],
            "detail": self._budget_detail(candidate, jd)
        }

        # 5 ── Location (10)
        loc_score, loc_detail = self._score_location(candidate, jd)
        breakdown["location"] = {
            "score": round(loc_score, 1),
            "max": self.WEIGHTS["location"],
            "detail": loc_detail
        }

        # 6 ── Work Mode (8)
        wm_score, wm_detail = self._score_work_mode(candidate, jd)
        breakdown["work_mode"] = {
            "score": round(wm_score, 1),
            "max": self.WEIGHTS["work_mode"],
            "detail": wm_detail
        }

        # 7 ── Notice Period (7)
        np_score, np_detail = self._score_notice(candidate, jd)
        breakdown["notice_period"] = {
            "score": round(np_score, 1),
            "max": self.WEIGHTS["notice_period"],
            "detail": np_detail
        }

        # ── Total (normalise 110 → 100) ──
        raw = (req_score + nice_score + exp_score + budget_score
               + loc_score + wm_score + np_score)
        max_raw = sum(self.WEIGHTS.values())          # 110
        total = round((raw / max_raw) * 100, 1)

        breakdown["total"] = total
        breakdown["grade"] = self._grade(total)

        return total, breakdown

    # ── Sub-scorers ──

    def _score_budget(self, candidate: dict, jd: dict) -> float:
        w = self.WEIGHTS["budget"]
        try:
            expected_raw = str(
                candidate.get("expected_ctc") or
                candidate.get("current_ctc") or ""
            )
            nums = re.findall(r'\d+\.?\d*', expected_raw)
            if not nums:
                return w * 0.7
            expected = float(nums[0])
            # Convert to LPA if looks like monthly
            if expected > 1000:
                expected = expected / 100000
            budget_max = float(jd.get("budget_max") or 0)
            budget_min = float(jd.get("budget_min") or 0)
            if budget_max <= 0:
                return w * 0.7
            if expected <= budget_max:
                return w
            overshoot = (expected - budget_max) / budget_max
            return max(0, w * (1 - overshoot * 2))
        except Exception:
            return w * 0.7

    def _budget_detail(self, candidate: dict, jd: dict) -> str:
        try:
            expected_raw = str(
                candidate.get("expected_ctc") or
                candidate.get("current_ctc") or "Not specified"
            )
            budget_max = float(jd.get("budget_max") or 0)
            return (
                f"Expected: {expected_raw} | "
                f"Budget: up to {budget_max} {jd.get('budget_currency','LPA')}"
            )
        except Exception:
            return "Budget info unavailable"

    def _score_location(
        self, candidate: dict, jd: dict
    ) -> Tuple[float, str]:
        w = self.WEIGHTS["location"]
        jd_loc = (jd.get("location") or "").lower().strip()
        cand_loc = (candidate.get("current_location") or "").lower().strip()
        pref_locs = [
            l.lower() for l in (candidate.get("preferred_locations") or [])
        ]

        if not jd_loc or "remote" in jd_loc:
            return w, "Remote — location not a constraint"
        if jd_loc in cand_loc or cand_loc in jd_loc:
            return w, f"Location match: {candidate.get('current_location')}"
        for pl in pref_locs:
            if jd_loc in pl or pl in jd_loc:
                return w * 0.9, f"Preferred location match: {pl}"
        # Partial city match
        jd_words = set(jd_loc.split())
        cand_words = set(cand_loc.split())
        if jd_words & cand_words:
            return w * 0.7, "Partial location match"
        return w * 0.3, (
            f"Location mismatch: {candidate.get('current_location','?')} "
            f"vs {jd.get('location','?')}"
        )

    def _score_work_mode(
        self, candidate: dict, jd: dict
    ) -> Tuple[float, str]:
        w = self.WEIGHTS["work_mode"]
        jd_wm  = (jd.get("work_mode") or "").lower()
        c_wm   = (candidate.get("work_mode_preference") or "").lower()
        if not jd_wm or not c_wm or "any" in c_wm:
            return w, "Work mode flexible"
        if jd_wm in c_wm or c_wm in jd_wm:
            return w, f"Work mode match: {jd.get('work_mode')}"
        if "hybrid" in jd_wm and ("remote" in c_wm or "on-site" in c_wm):
            return w * 0.6, "Partial work mode match (hybrid flexibility)"
        return w * 0.3, (
            f"Work mode mismatch: prefers {candidate.get('work_mode_preference')}"
            f" vs {jd.get('work_mode')}"
        )

    def _score_notice(
        self, candidate: dict, jd: dict
    ) -> Tuple[float, str]:
        w = self.WEIGHTS["notice_period"]
        jd_np_raw  = str(jd.get("notice_period_max") or "")
        cand_np_raw = str(candidate.get("notice_period") or "")

        jd_days   = self._notice_to_days(jd_np_raw)
        cand_days = self._notice_to_days(cand_np_raw)

        if jd_days is None or cand_days is None:
            return w * 0.7, "Notice period not specified"
        if cand_days <= jd_days:
            return w, f"Notice: {cand_np_raw} ≤ {jd_np_raw} ✓"
        overshoot = (cand_days - jd_days) / max(jd_days, 1)
        score = max(0, w * (1 - overshoot * 0.5))
        return score, (
            f"Notice: {cand_np_raw} > required {jd_np_raw}"
        )

    def _notice_to_days(self, text: str) -> Optional[int]:
        text = text.lower()
        nums = re.findall(r'\d+', text)
        if not nums:
            if "immediate" in text or "serving" in text:
                return 0
            return None
        n = int(nums[0])
        if "month" in text:
            return n * 30
        if "week" in text:
            return n * 7
        return n  # assume days

    def _grade(self, score: float) -> str:
        if score >= 85:
            return "Excellent"
        if score >= 70:
            return "Strong"
        if score >= 55:
            return "Good"
        if score >= 40:
            return "Fair"
        return "Weak"

    def bulk_score(
        self,
        candidates: List[dict],
        jd: dict,
        min_score: float = 0
    ) -> List[dict]:
        """Score many candidates, return sorted list"""
        results = []
        for c in candidates:
            try:
                score, breakdown = self.score(c, jd)
                if score >= min_score:
                    results.append({
                        **c,
                        "match_score": score,
                        "match_breakdown": breakdown
                    })
            except Exception as e:
                print(f"Score error for {c.get('full_name')}: {e}")
        return sorted(results, key=lambda x: x["match_score"], reverse=True)

    def generate_match_summary(
        self, candidate: dict, jd: dict,
        score: float, breakdown: dict
    ) -> str:
        prompt = PromptTemplates.match_explainer(
            candidate, jd, score, breakdown
        )
        return self.llm.complete(prompt, max_tokens=200, temperature=0.4)
