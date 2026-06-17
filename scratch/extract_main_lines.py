with open(r"D:\renko_playback\frontend\static\js\main.js", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

lines = content.split("\n")
with open(r"D:\renko_playback\scratch\extracted_main.txt", "w", encoding="utf-8") as out:
    for idx in range(599, min(760, len(lines))):
        out.write(f"{idx+1}: {lines[idx]}\n")
