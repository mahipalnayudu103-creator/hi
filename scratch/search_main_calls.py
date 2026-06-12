with open(r"D:\renko_playback\frontend\static\js\main.js", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

lines = content.split("\n")
for idx, line in enumerate(lines):
    if "_loadAndRenderResult" in line:
        safe_line = line.strip().encode('ascii', errors='replace').decode('ascii')
        print(f"Line {idx+1}: {safe_line}")
