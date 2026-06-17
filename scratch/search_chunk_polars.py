import sys
sys.stdout.reconfigure(encoding='utf-8')
with open(r"D:\renko_playback\backend\utils\csv_stream.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()
lines = content.split("\n")
for idx, line in enumerate(lines):
    if "_process_chunk_polars" in line:
        safe_line = line.strip().encode('ascii', errors='replace').decode('ascii')[:150]
        print(f"Line {idx+1}: {safe_line}")
