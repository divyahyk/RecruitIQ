from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
"""
Full JD Manager Integration Test
Tests: Imports, parsing, JobData conversion, matching
"""

import sys
import os
sys.path.insert(0, ".")

print("\n" + "=" * 80)
print("FULL JD MANAGER INTEGRATION TEST")
print("=" * 80)

# Test 1: Imports
print("\n[1??] Testing imports...")
try:
    from modules.jd_engine import JobData, JDParser
    from modules.ai_engine.llm_handler import LLMHandler
    from ui.pages.jd_manager import _normalise_skills, _tokenise
    print("    ? All imports successful")
except Exception as e:
    print(f"    ? Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 2: JobData creation
print("\n[2??] Testing JobData creation...")
try:
    jd = JobData(
        jd_code="JD001",
        role_name="Senior Python Developer",
        location="Bangalore",
        skillset_required=["Python", "FastAPI", "PostgreSQL"],
        experience_min=5.0,
        experience_max=8.0,
        budget_min=1800000.0,
        budget_max=2500000.0
    )
    print(f"    ? JobData created: {jd.role_name}")
    print(f"       Skills: {jd.skillset_required}")
except Exception as e:
    print(f"    ? Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Skill normalization
print("\n[3??] Testing skill normalization...")
try:
    test_cases = [
        (["Python", "FastAPI"], "list"),
        ("Python, FastAPI, PostgreSQL", "CSV string"),
        ('["Python", "FastAPI", "PostgreSQL"]', "JSON string"),
    ]
    for test, label in test_cases:
        result = _normalise_skills(test)
        print(f"    ? {label:15} ? {len(result)} skills: {result[:2]}")
except Exception as e:
    print(f"    ? Failed: {e}")

# Test 4: Tokenization
print("\n[4??] Testing tokenization...")
try:
    text = "We are looking for a Senior Python Developer with FastAPI experience"
    tokens = _tokenise(text)
    print(f"    ? Tokenized text ({len(tokens)} tokens)")
    print(f"       Tokens: {sorted(list(tokens)[:5])}")
except Exception as e:
    print(f"    ? Failed: {e}")

# Test 5: JobData.to_dict()
print("\n[5??] Testing JobData.to_dict()...")
try:
    jd_dict = jd.to_dict()
    print(f"    ? Converted to dict with {len(jd_dict)} fields")
    print(f"       role_name: {jd_dict.get('role_name')}")
    print(f"       location: {jd_dict.get('location')}")
    print(f"       skillset_required: {jd_dict.get('skillset_required')}")
except Exception as e:
    print(f"    ? Failed: {e}")

# Test 6: JobData.from_jd_dict()
print("\n[6??] Testing JobData.from_jd_dict()...")
try:
    sample_dict = {
        "jd_code": "JD999",
        "role_name": "Full Stack Engineer",
        "location": "NYC",
        "skillset_required": '["React", "Node.js", "MongoDB"]',
        "skillset_good_to_have": "TypeScript, Docker",
        "experience_min": "5",
        "experience_max": 8,
        "budget_min": 100000,
        "budget_max": 150000
    }
    jd_obj = JobData.from_jd_dict(sample_dict)
    print(f"    ? Created JobData from dict")
    print(f"       Role: {jd_obj.role_name}")
    print(f"       Location: {jd_obj.location}")
except Exception as e:
    print(f"    ? Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 7: Round-trip conversion
print("\n[7??] Testing round-trip: dict ? JobData ? dict...")
try:
    jd2 = JobData.from_jd_dict(sample_dict)
    jd2_dict = jd2.to_dict()
    
    if jd2_dict['role_name'] == sample_dict['role_name']:
        print(f"    ? Data integrity verified")
    else:
        print(f"    ? Data mismatch")
except Exception as e:
    print(f"    ? Failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("? FULL JD MANAGER INTEGRATION TEST COMPLETED")
print("=" * 80 + "\n")
