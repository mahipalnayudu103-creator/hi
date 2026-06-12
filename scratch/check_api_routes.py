import re

with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

# Look for route decorators, e.g., @router.post or @app.post
routes = re.findall(r'(@[a-zA-Z_]+\.(?:post|get|put|delete|websocket)\([^)]+\))', content)
print("Found routes:")
for r in routes[:40]:
    print(r)

# Let's search for "build-renko" or similar
print("\nOccurrences of 'build-renko' or 'build_renko':")
for idx, line in enumerate(content.splitlines()):
    if "build-renko" in line or "build_renko" in line:
        print(f"Line {idx+1}: {line.strip()}")
