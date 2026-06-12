with open("D:\\renko_playback\\frontend\\index.html", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if "wick" in line.lower():
        print(f"{idx+1}: {line.strip()}")
