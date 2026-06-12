with open(r"D:\renko_playback\frontend\static\js\main.js", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

lines = content.split("\n")
target_indices = []
for idx, line in enumerate(lines):
    if "_loadAndRenderResult" in line or "loadAndRenderResult" in line or "renderResult" in line or "setData" in line:
        target_indices.append(idx)

print("Matches found at indices:", target_indices)

with open(r"D:\renko_playback\scratch\extracted_main_render.txt", "w", encoding="utf-8") as out:
    for target_idx in target_indices:
        out.write(f"=== Match at line {target_idx+1} ===\n")
        start = max(0, target_idx - 15)
        end = min(len(lines), target_idx + 45)
        for idx in range(start, end):
            out.write(f"{idx+1}: {lines[idx]}\n")
        out.write("\n")
