with open(r"D:\renko_playback\backend\routes\api.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

# Let's search for keywords
keywords = ["date", "filter", "nrows", "chunk", "slice", "limit", "time"]
lines = content.split("\n")
for idx, line in enumerate(lines):
    line_lower = line.lower()
    matched = [kw for kw in keywords if kw in line_lower]
    if matched:
        print(f"Line {idx+1}: {line.strip()[:120]}")
