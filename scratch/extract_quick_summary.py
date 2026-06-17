with open(r"D:\renko_playback\backend\utils\csv_reader.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

lines = content.split("\n")
idx = 0
for i, line in enumerate(lines):
    if "def quick_csv_summary_from_preview" in line:
        idx = i
        break

print("Found quick_csv_summary_from_preview at line:", idx+1)
with open(r"D:\renko_playback\scratch\extracted_quick_summary.txt", "w", encoding="utf-8") as out:
    start = max(0, idx - 5)
    end = min(len(lines), idx + 80)
    for j in range(start, end):
        out.write(f"{j+1}: {lines[j]}\n")
