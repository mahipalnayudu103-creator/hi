with open("D:\\renko_playback\\frontend\\index.html", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if "pip" in line.lower() or "chart" in line.lower():
        if "input" in line or "label" in line:
            print(f"{idx+1}: {line.strip()}")
