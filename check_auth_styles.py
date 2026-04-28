import ast, pathlib

for path in ("ui/auth.py", "ui/styles.py"):
    print(f"=== {path} ===")
    try:
        src = pathlib.Path(path).read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(src)
        funcs = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        classes = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        print(f"  functions : {funcs}")
        print(f"  classes   : {classes}")
        print(f"  first 30 lines:")
        for i, line in enumerate(src.splitlines()[:30], 1):
            print(f"    {i:02d}  {line}")
    except Exception as e:
        print(f"  ERROR: {e}")
    print()
