with open("D:\\renko_playback\\frontend\\static\\js\\main.js", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if "submitBuildJob" in line:
        print(f"--- submitBuildJob at {idx+1} ---")
        for i in range(max(0, idx - 45), min(len(lines), idx + 25)):
            val = f"{i+1}: {lines[i]}"
            print(val.encode('ascii', errors='ignore').decode('ascii'), end="")
