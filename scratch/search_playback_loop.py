with open("D:\\renko_playback\\frontend\\static\\js\\main.js", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

import re
# Search for lines containing "charts.forEach" or updating charts
for idx, line in enumerate(content.splitlines()):
    if "charts[" in line or "charts.forEach" in line or "update" in line or "draw" in line:
        if any(keyword in line for keyword in ["brick", "data", "play", "feed", "tick", "render", "setData"]):
            print(f"Line {idx+1}: {line.strip()}")
