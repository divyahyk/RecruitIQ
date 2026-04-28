"""
RecruitIQ JD Engine

Exports:
  - JobData: Dataclass for parsed job descriptions
  - JDParser: Parser for extracting structured JD data
  - score_candidate: Scoring function for candidate matching
"""

from .job_data import JobData
from .jd_parser import JDParser
from .scoring_engine import ProfileScoringEngine

__all__ = [
    "JobData",
    "JDParser",
    "ProfileScoringEngine",
]
