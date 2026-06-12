with open("D:\\renko_playback\\frontend\\static\\js\\main.js", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if "showWicks" in line or "show_wicks" in line:
        val = f"{idx+1}: {line}"
        print(val.encode('ascii', errors='ignore').decode('ascii'), end="")
