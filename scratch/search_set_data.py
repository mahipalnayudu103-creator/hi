with open("D:\\renko_playback\\frontend\\static\\js\\main.js", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if "setData" in line or "result" in line:
        if "chart" in line.lower() or "setData" in line:
            print(f"{idx+1}: {line.strip()}")
