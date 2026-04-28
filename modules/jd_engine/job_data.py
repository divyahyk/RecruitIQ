"""
JobData Dataclass

Represents a parsed job description with validation and serialization.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
import json
from datetime import datetime


@dataclass
class JobData:
    """
    Represents a structured job description.
    
    All fields are optional to allow progressive population during parsing.
    Serialization/deserialization handles JSON and dict formats.
    """
    
    # ── Core JD Information ───────────────────────────────────────────────────
    jd_code: Optional[str] = None
    """Unique identifier for the JD (e.g., 'JD20250115001')"""
    
    role_name: Optional[str] = None
    """Job title/role name (e.g., 'Senior Python Developer')"""
    
    location: Optional[str] = None
    """Job location (e.g., 'Bangalore, India' or 'Remote')"""
    
    work_mode: Optional[str] = None
    """Work mode: 'On-site', 'Remote', or 'Hybrid'"""
    
    # ── Skills & Requirements ─────────────────────────────────────────────────
    skillset_required: List[str] = field(default_factory=list)
    """List of required technical skills"""
    
    skillset_good_to_have: List[str] = field(default_factory=list)
    """List of nice-to-have skills"""
    
    experience_min: Optional[float] = None
    """Minimum years of experience required"""
    
    experience_max: Optional[float] = None
    """Maximum years of experience (for level matching)"""
    
    # ── Budget & Compensation ─────────────────────────────────────────────────
    budget_min: Optional[float] = None
    """Minimum salary/budget in base currency"""
    
    budget_max: Optional[float] = None
    """Maximum salary/budget in base currency"""
    
    currency: Optional[str] = None
    """Currency code (e.g., 'INR', 'USD', 'EUR')"""
    
    # ── Positions & Management ────────────────────────────────────────────────
    positions_count: Optional[int] = None
    """Number of open positions for this JD"""
    
    client_name: Optional[str] = None
    """Name of the hiring client/company"""
    
    recruiter_assigned: Optional[str] = None
    """Name of the assigned recruiter"""
    
    # ── Status & Metadata ─────────────────────────────────────────────────────
    status: Optional[str] = None
    """JD status: 'Active', 'Inactive', 'On Hold', 'Closed'"""
    
    priority: Optional[str] = None
    """Priority level: 'High', 'Medium', 'Low'"""
    
    created_at: Optional[str] = None
    """ISO 8601 timestamp when JD was created"""
    
    updated_at: Optional[str] = None
    """ISO 8601 timestamp when JD was last updated"""
    
    # ── Additional Notes ──────────────────────────────────────────────────────
    description: Optional[str] = None
    """Full JD description text"""
    
    special_requirements: Optional[str] = None
    """Any special or unique requirements"""
    
    # ── Internal Fields ───────────────────────────────────────────────────────
    parse_confidence: float = 0.0
    """Confidence score of parsing (0.0-1.0)"""
    
    validation_errors: List[str] = field(default_factory=list)
    """List of validation errors during parsing"""

    # ── Methods ───────────────────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert JobData to a dictionary suitable for database storage.
        
        Handles:
        - List serialization (JSON strings for DB storage)
        - None value handling
        - Type conversion for edge cases
        
        Returns:
            Dictionary with serialized fields ready for DB insert/update
        """
        d = {}
        
        for key, value in asdict(self).items():
            if value is None:
                d[key] = None
            elif isinstance(value, list):
                # Serialize lists as JSON strings for DB storage
                d[key] = json.dumps(value) if value else json.dumps([])
            elif isinstance(value, (int, float, str, bool)):
                d[key] = value
            else:
                # Fallback: convert to string
                d[key] = str(value)
        
        return d

    @classmethod
    def from_jd_dict(cls, data: Dict[str, Any]) -> "JobData":
        """
        Create a JobData object from a dictionary (typically from database or JSON).
        
        Handles:
        - JSON string deserialization for lists
        - CSV string parsing for skill lists
        - Type coercion (str → float for experience/budget)
        - Missing fields (uses defaults)
        
        Args:
            data: Dictionary with JD fields
            
        Returns:
            Initialized JobData instance
        """
        # Helper to parse list fields
        def parse_list_field(val):
            if val is None or val == "":
                return []
            if isinstance(val, list):
                return val
            if isinstance(val, str):
                # Try JSON parsing first
                if val.strip().startswith('['):
                    try:
                        return json.loads(val)
                    except (json.JSONDecodeError, ValueError):
                        pass
                # Fall back to CSV parsing
                return [s.strip() for s in val.split(',') if s.strip()]
            return []
        
        # Helper to parse numeric fields
        def parse_float(val):
            if val is None:
                return None
            if isinstance(val, (int, float)):
                return float(val)
            if isinstance(val, str):
                try:
                    return float(val.strip())
                except ValueError:
                    return None
            return None
        
        # Helper to parse integer fields
        def parse_int(val):
            if val is None:
                return None
            if isinstance(val, int):
                return val
            if isinstance(val, str):
                try:
                    return int(val.strip())
                except ValueError:
                    return None
            return None

        return cls(
            jd_code=data.get("jd_code"),
            role_name=data.get("role_name"),
            location=data.get("location"),
            work_mode=data.get("work_mode"),
            skillset_required=parse_list_field(data.get("skillset_required")),
            skillset_good_to_have=parse_list_field(data.get("skillset_good_to_have")),
            experience_min=parse_float(data.get("experience_min")),
            experience_max=parse_float(data.get("experience_max")),
            budget_min=parse_float(data.get("budget_min")),
            budget_max=parse_float(data.get("budget_max")),
            currency=data.get("currency"),
            positions_count=parse_int(data.get("positions_count")),
            client_name=data.get("client_name"),
            recruiter_assigned=data.get("recruiter_assigned"),
            status=data.get("status"),
            priority=data.get("priority"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            description=data.get("description"),
            special_requirements=data.get("special_requirements"),
            parse_confidence=parse_float(data.get("parse_confidence")) or 0.0,
            validation_errors=parse_list_field(data.get("validation_errors")),
        )

    def to_json(self) -> str:
        """
        Serialize JobData to JSON string.
        
        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), indent=2, default=str)

    @classmethod
    def from_json(cls, json_str: str) -> "JobData":
        """
        Deserialize JobData from JSON string.
        
        Args:
            json_str: JSON string with JD data
            
        Returns:
            Initialized JobData instance
        """
        data = json.loads(json_str)
        return cls.from_jd_dict(data)

    def validate(self) -> tuple[bool, List[str]]:
        """
        Validate the JobData object.
        
        Checks:
        - Mandatory fields present
        - Experience range sensible (min <= max)
        - Budget range sensible (min <= max)
        - Skill lists not empty
        
        Returns:
            (is_valid: bool, errors: List[str])
        """
        errors = []
        
        # Mandatory fields
        if not self.role_name:
            errors.append("role_name is mandatory")
        if not self.location:
            errors.append("location is mandatory")
        if not self.skillset_required:
            errors.append("skillset_required cannot be empty")
        
        # Logical constraints
        if (self.experience_min is not None and 
            self.experience_max is not None and 
            self.experience_min > self.experience_max):
            errors.append(f"experience_min ({self.experience_min}) > experience_max ({self.experience_max})")
        
        if (self.budget_min is not None and 
            self.budget_max is not None and 
            self.budget_min > self.budget_max):
            errors.append(f"budget_min ({self.budget_min}) > budget_max ({self.budget_max})")
        
        # Range sanity checks
        if self.experience_min is not None and self.experience_min < 0:
            errors.append(f"experience_min cannot be negative: {self.experience_min}")
        
        if self.experience_max is not None and self.experience_max > 70:
            errors.append(f"experience_max seems unrealistic: {self.experience_max} years")
        
        if self.budget_min is not None and self.budget_min < 0:
            errors.append(f"budget_min cannot be negative: {self.budget_min}")
        
        return len(errors) == 0, errors

    def __str__(self) -> str:
        """Human-readable representation."""
        return f"JobData({self.jd_code or 'NEW'} - {self.role_name or 'UNNAMED'} @ {self.location or 'UNKNOWN'})"

    def __repr__(self) -> str:
        """Developer representation."""
        return (
            f"JobData("
            f"code={self.jd_code!r}, "
            f"role={self.role_name!r}, "
            f"location={self.location!r}, "
            f"skills={len(self.skillset_required)}, "
            f"exp={self.experience_min}-{self.experience_max}"
            f")"
        )
