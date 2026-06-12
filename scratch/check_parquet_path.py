with open("D:\\renko_playback\\backend\\services\\parquet_cache.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

import re
# Find definitions like def _legacy_parquet_path
for m in re.finditer(r'def ([a-zA-Z0-9_]+)\(.*?\):', content):
    name = m.group(1)
    if "path" in name or "cache" in name:
        def_match = re.search(r'def ' + name + r'.*?:(.*?)(?=def|\Z)', content, re.DOTALL)
        if def_match:
            print(f"--- {name} ---")
            print(def_match.group(0)[:1000])
