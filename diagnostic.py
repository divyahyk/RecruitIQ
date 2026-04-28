# diagnostic.py
import os
import sys
import re
from pathlib import Path

print("=" * 80)
print("🔍 RECRUITIQ DIAGNOSTIC SCAN")
print("=" * 80)

# Scan for problematic bytes and characters
PROBLEMATIC_BYTES = [0x92, 0x93, 0x94, 0x96]  # Smart quotes, em-dashes
PROBLEMATIC_CHARS = ['??', '???', '\ufffd']  # Replacement char

py_files = list(Path('.').rglob('*.py'))
print(f"\n📂 Found {len(py_files)} Python files\n")

issues_found = False

for filepath in py_files:
    if '__pycache__' in str(filepath):
        continue
    
    try:
        with open(filepath, 'rb') as f:
            content_bytes = f.read()
        
        # Check for problematic bytes
        for byte_val in PROBLEMATIC_BYTES:
            if bytes([byte_val]) in content_bytes:
                print(f"⚠️  {filepath}: Contains byte 0x{byte_val:02x}")
                issues_found = True
        
        # Check for UTF-8 replacement character
        if b'\xef\xbf\xbd' in content_bytes:  # UTF-8 for U+FFFD
            print(f"⚠️  {filepath}: Contains UTF-8 replacement character (U+FFFD)")
            issues_found = True
        
        # Try to decode and check for '??'
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content_str = f.read()
            
            if '??' in content_str:
                # Find line numbers
                for i, line in enumerate(content_str.split('\n'), 1):
                    if '??' in line:
                        print(f"❌ {filepath}:{i}: {line.strip()[:100]}")
                        issues_found = True
        except UnicodeDecodeError as e:
            print(f"🔴 {filepath}: DECODE ERROR - {e}")
            issues_found = True
    
    except Exception as e:
        print(f"⚠️  {filepath}: Error reading file - {e}")

# Check app.py specifically
print("\n" + "=" * 80)
print("📋 CHECKING: app.py")
print("=" * 80)

try:
    with open('app.py', 'r', encoding='utf-8') as f:
        app_content = f.read()
    
    # Look for emoji placeholders
    emoji_lines = []
    for i, line in enumerate(app_content.split('\n'), 1):
        if '??' in line or '\ufffd' in line or '????' in line:
            emoji_lines.append((i, line.strip()))
    
    if emoji_lines:
        print("Found ?? in app.py:")
        for line_num, line_text in emoji_lines:
            print(f"  Line {line_num}: {line_text[:100]}")
            issues_found = True
    else:
        print("✅ No ?? found in app.py")
except Exception as e:
    print(f"❌ Error checking app.py: {e}")

# Check requirements.txt
print("\n" + "=" * 80)
print("📋 CHECKING: requirements.txt")
print("=" * 80)

if os.path.exists('requirements.txt'):
    with open('requirements.txt', 'r') as f:
        reqs = f.read()
    print("Current requirements.txt:")
    print(reqs)
else:
    print("❌ requirements.txt not found")

# Check .streamlit/config.toml
print("\n" + "=" * 80)
print("📋 CHECKING: .streamlit/config.toml")
print("=" * 80)

if os.path.exists('.streamlit/config.toml'):
    with open('.streamlit/config.toml', 'r') as f:
        config = f.read()
    print("Current config.toml:")
    print(config)
else:
    print("❌ config.toml not found - WILL CREATE")

print("\n" + "=" * 80)
if issues_found:
    print("🔴 ISSUES FOUND - See details above")
else:
    print("✅ NO ISSUES FOUND IN SOURCE CODE")
print("=" * 80)
