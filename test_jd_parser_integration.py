import sys
import os

# Setup path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

print("\n" + "=" * 80)
print("JD PARSER → JobData INTEGRATION TEST")
print("=" * 80)

# Test 1: Import validation
print("\n[1️⃣] Testing imports...")
try:
    from modules.jd_engine.job_data import JobData
    from modules.jd_engine.jd_parser import JDParser
    from modules.ai_engine.llm_handler import LLMHandler
    print("    ✅ All imports successful")
except Exception as e:
    print(f"    ❌ Import failed: {e}")
    sys.exit(1)

# Test 2: JobData instantiation
print("\n[2️⃣] Testing JobData instantiation...")
try:
    jd = JobData(
        jd_code="JD001",
        role_name="Senior Backend Engineer",
        location="Bangalore",
        skillset_required=["Python", "FastAPI", "PostgreSQL"],
        experience_min=5.0,
        experience_max=8.0,
        budget_min=1800000.0,
        budget_max=2500000.0,
        budget_currency="INR"
    )
    print(f"    ✅ JobData created: {jd}")
    print(f"       Skills: {jd.skillset_required}")
    print(f"       Exp: {jd.experience_min}-{jd.experience_max} years")
    print(f"       Budget: ₹{jd.budget_min:,.0f} - ₹{jd.budget_max:,.0f}")
except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 3: from_jd_dict with various formats
print("\n[3️⃣] Testing JobData.from_jd_dict() with mixed formats...")
try:
    raw_dict = {
        "jd_code": "JD002",
        "role_name": "Full Stack Developer",
        "location": "NYC",
        "skillset_required": '["React", "Node.js", "MongoDB"]',  # JSON string
        "skillset_good_to_have": "TypeScript, Docker, AWS",      # CSV string
        "experience_min": "3",                                     # String number
        "experience_max": "6.5",                                   # Float string
        "budget_min": "100000",
        "budget_max": "140000",
        "budget_currency": "USD"
    }
    jd2 = JobData.from_jd_dict(raw_dict)
    print(f"    ✅ Parsed dict successfully")
    print(f"       Role: {jd2.role_name}")
    print(f"       Required: {jd2.skillset_required}")
    print(f"       Good-to-have: {jd2.skillset_good_to_have}")
    print(f"       Exp: {jd2.experience_min}-{jd2.experience_max} years")
except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 4: skill_summary
print("\n[4️⃣] Testing skill_summary()...")
try:
    jd3 = JobData(
        role_name="DevOps Engineer",
        skillset_required=["Kubernetes", "Docker", "AWS", "Terraform", "Jenkins"],
        skillset_good_to_have=["Helm", "ArgoCD"],
    )
    summary_3 = jd3.skill_summary(max_skills=3)
    summary_all = jd3.skill_summary(max_skills=10)
    print(f"    ✅ Skill summary (3): {summary_3}")
    print(f"    ✅ Skill summary (all): {summary_all}")
except Exception as e:
    print(f"    ❌ Failed: {e}")

# Test 5: to_dict conversion
print("\n[5️⃣] Testing to_dict() conversion...")
try:
    jd4_dict = jd.to_dict()
    print(f"    ✅ Converted to dict")
    print(f"       Keys: {len(jd4_dict)} fields")
    print(f"       Sample: role={jd4_dict['role_name']}, loc={jd4_dict['location']}")
except Exception as e:
    print(f"    ❌ Failed: {e}")

# Test 6: JDParser initialization (mock LLM)
print("\n[6️⃣] Testing JDParser initialization...")
try:
    # Create mock LLMHandler
    class MockLLM:
        def complete(self, prompt, max_tokens=1024):
            return '{"role_name": "Test Role", "location": "Test City"}'
        def extract_json(self, response):
            import json
            try:
                return json.loads(response)
            except:
                return {}
    
    mock_llm = MockLLM()
    parser = JDParser(llm=mock_llm)
    print(f"    ✅ JDParser initialized")
    print(f"       LLM type: {type(parser.llm).__name__}")
except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 7: Parse raw JD text (with mock)
print("\n[7️⃣] Testing parse() with mock LLM...")
try:
    sample_jd = """
    Position: Senior Python Developer
    Location: Bangalore
    Skills Required: Python, FastAPI, PostgreSQL
    Experience: 5-8 years
    Salary: 18,00,000 - 25,00,000
    """
    parsed = parser.parse(sample_jd)
    print(f"    ✅ Parsing successful")
    print(f"       Fields parsed: {len(parsed)}")
    print(f"       Role: {parsed.get('role_name')}")
    print(f"       Location: {parsed.get('location')}")
except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 8: parse_to_job_data
print("\n[8️⃣] Testing parse_to_job_data()...")
try:
    job_data = parser.parse_to_job_data(sample_jd)
    print(f"    ✅ Converted to JobData")
    print(f"       {job_data}")
except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("✅ ALL INTEGRATION TESTS COMPLETED")
print("=" * 80 + "\n")
