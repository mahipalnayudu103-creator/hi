import re
import os
import sys

# force stdout to utf-8 if possible, or print ascii-safe
sys.stdout.reconfigure(encoding='utf-8')

keywords = ['date', 'time', 'timestamp', 'utc', 'parse', 'epoch', 'second', 'ms']
js_dir = r"D:\renko_playback\frontend\static\js"

for fname in os.listdir(js_dir):
    if not fname.endswith('.js') or fname == 'lightweight-charts.js':
        continue
    fpath = os.path.join(js_dir, fname)
    print(f"=== {fname} ===")
    with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    lines = content.split('\n')
    for idx, line in enumerate(lines):
        line_lower = line.lower()
        matched = [kw for kw in keywords if kw in line_lower]
        if matched:
            # strip and make safe for console print
            safe_line = line.strip()[:150].encode('ascii', errors='replace').decode('ascii')
            print(f"Line {idx+1}: {safe_line} (matched: {matched})")
