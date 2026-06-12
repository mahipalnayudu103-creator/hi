with open("D:\\renko_playback\\frontend\\static\\js\\main.js", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

for idx in range(815 - 1, min(845, len(lines))):
    val = f"{idx+1}: {lines[idx]}"
    print(val.encode('ascii', errors='ignore').decode('ascii'), end="")
