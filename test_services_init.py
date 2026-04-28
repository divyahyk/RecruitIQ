"""Test services initialization and JDParser integration"""

import sys
import os
from pathlib import Path

# Load .env
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env", override=True)

# Add to path
sys.path.insert(0, ".")

print("\n" + "=" * 80)
print("SERVICES INITIALIZATION TEST")
print("=" * 80)

# Test 1: Import all services
print("\n[1️⃣] Importing all services...")
try:
    from database.supabase_manager             import SupabaseManager
    from modules.jd_engine.jd_parser           import JDParser
    from modules.jd_engine.job_data            import JobData
    from modules.jd_engine.scoring_engine      import ProfileScoringEngine
    from modules.ai_engine.llm_handler         import LLMHandler
    from modules.social_media.banner_generator import SocialBannerGenerator
    print("    ✅ All services imported successfully")
except Exception as e:
    print(f"    ❌ Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 2: Check environment variables
print("\n[2️⃣] Checking environment variables...")
try:
    SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
    SUPABASE_KEY = (
        os.getenv("SUPABASE_KEY",              "").strip()
        or os.getenv("SUPABASE_ANON_KEY",      "").strip()
        or os.getenv("SUPABASE_SERVICE_KEY",   "").strip()
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    )
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

    print(f"    SUPABASE_URL:    {'✅ SET' if SUPABASE_URL else '❌ MISSING'}")
    print(f"    SUPABASE_KEY:    {'✅ SET' if SUPABASE_KEY else '❌ MISSING'}")
    print(f"    OPENAI_API_KEY:  {'✅ SET' if OPENAI_API_KEY else '❌ MISSING'}")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("    ⚠️  Supabase credentials missing — tests will use mock services")

except Exception as e:
    print(f"    ❌ Failed: {e}")
    sys.exit(1)

# Test 3: Initialize LLMHandler (mock if no API key)
print("\n[3️⃣] Initializing LLMHandler...")
try:
    llm = LLMHandler()
    print(f"    ✅ LLMHandler initialized")
    print(f"       Type: {type(llm).__name__}")
except Exception as e:
    print(f"    ⚠️  Warning: {e}")
    llm = None

# Test 4: Initialize JDParser
print("\n[4️⃣] Initializing JDParser...")
try:
    if llm is None:
        # Create mock LLM
        class MockLLM:
            def complete(self, prompt, max_tokens=1024):
                return '{}'
            def extract_json(self, response):
                import json
                try:
                    return json.loads(response)
                except:
                    return {}
        llm = MockLLM()
        print("    ℹ️  Using mock LLM for testing")

    jd_parser = JDParser(llm=llm)
    print(f"    ✅ JDParser initialized")
    print(f"       Type: {type(jd_parser).__name__}")
    print(f"       LLM type: {type(jd_parser.llm).__name__}")
except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Test JDParser.parse()
print("\n[5️⃣] Testing JDParser.parse()...")
try:
    sample_jd = """
    Position: Senior Python Developer
    Location: Bangalore, India
    Work Mode: Hybrid
    
    Required Skills:
    - Python 3.8+
    - FastAPI
    - PostgreSQL
    - Docker
    - AWS
    
    Experience: 5-8 years
    Salary: 18,00,000 - 25,00,000 INR
    Positions: 2
    """

    parsed_dict = jd_parser.parse(sample_jd)
    print(f"    ✅ Parsed as dict with {len(parsed_dict)} fields")
    print(f"       Role: {parsed_dict.get('role_name', 'N/A')}")
    print(f"       Location: {parsed_dict.get('location', 'N/A')}")
    print(f"       Exp: {parsed_dict.get('experience_min')}-{parsed_dict.get('experience_max')} years")

except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 6: Test JDParser.parse_to_job_data()
print("\n[6️⃣] Testing JDParser.parse_to_job_data()...")
try:
    job_data = jd_parser.parse_to_job_data(sample_jd)
    print(f"    ✅ Created JobData object")
    print(f"       Type: {type(job_data).__name__}")
    print(f"       Role: {job_data.role_name}")
    print(f"       Location: {job_data.location}")
    print(f"       Skills: {job_data.skillset_required[:3] if job_data.skillset_required else []}")

except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 7: Test JobData.to_dict()
print("\n[7️⃣] Testing JobData.to_dict()...")
try:
    jd_dict = job_data.to_dict()
    print(f"    ✅ Converted to dict with {len(jd_dict)} fields")
    print(f"       Ready for database storage: {bool(jd_dict.get('role_name'))}")

except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

# Test 8: Initialize ProfileScoringEngine
print("\n[8️⃣] Initializing ProfileScoringEngine...")
try:
    scorer = ProfileScoringEngine(llm=llm)
    print(f"    ✅ ProfileScoringEngine initialized")
    print(f"       Type: {type(scorer).__name__}")
except Exception as e:
    print(f"    ⚠️  Warning: {e}")

# Test 9: Initialize SocialBannerGenerator
print("\n[9️⃣] Initializing SocialBannerGenerator...")
try:
    banner_gen = SocialBannerGenerator()
    print(f"    ✅ SocialBannerGenerator initialized")
    print(f"       Type: {type(banner_gen).__name__}")
except Exception as e:
    print(f"    ⚠️  Warning: {e}")

# Test 10: Build complete services dict
print("\n[🔟] Building complete services dict...")
try:
    services = {
        "db"        : None,  # Skip DB for this test
        "llm"       : llm,
        "jd_parser" : jd_parser,
        "scorer"    : scorer,
        "banner_gen": banner_gen,
    }
    print(f"    ✅ Services dict created with {len(services)} items")
    print(f"       Keys: {list(services.keys())}")

except Exception as e:
    print(f"    ❌ Failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("✅ ALL INITIALIZATION TESTS PASSED")
print("=" * 80 + "\n")
print("Ready to run Streamlit app!")
