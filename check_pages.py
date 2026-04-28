import ast, pathlib

pages = [
    ("ui/pages/dashboard.py",   "render_dashboard"),
    ("ui/pages/profiles.py",    "render_profiles"),
    ("ui/pages/jd_manager.py",  "render_jd_manager"),
    ("ui/pages/matching.py",    "render_jd_matching"),
    ("ui/pages/tracker.py",     "render_tracker"),
    ("ui/pages/interviews.py",  "render_interviews"),
    ("ui/pages/social.py",      "render_social_media"),
    ("ui/pages/ai_sourcing.py", "render_ai_sourcing"),
    ("ui/pages/upload.py",      "render_upload_page"),
    ("ui/auth.py",              "check_password"),
    ("ui/styles.py",            "get_global_css"),
]

all_ok = True
for path, fn in pages:
    try:
        src = pathlib.Path(path).read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(src)
        funcs = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        ok = fn in funcs
        mark = "OK     " if ok else "MISSING"
        print(f"{mark}  {path}  ->  {fn}")
        if not ok:
            all_ok = False
            available = [f for f in funcs if "render" in f or f in ("check_password", "get_global_css")]
            print(f"         available: {available}")
    except Exception as e:
        print(f"ERROR    {path}: {e}")
        all_ok = False

print()
print("All OK!" if all_ok else "Fix the MISSING items above.")
