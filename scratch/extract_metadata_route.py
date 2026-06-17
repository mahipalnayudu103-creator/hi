with open(r"D:\renko_playback\backend\routes\api.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

lines = content.split("\n")
idx = 0
for i, line in enumerate(lines):
    if "/metadata" in line:
        idx = i
        break

print("Found /metadata at line:", idx+1)
with open(r"D:\renko_playback\scratch\extracted_metadata_route.txt", "w", encoding="utf-8") as out:
    start = max(0, idx - 15)
    end = min(len(lines), idx + 80)
    for j in range(start, end):
        out.write(f"{j+1}: {lines[j]}\n")
