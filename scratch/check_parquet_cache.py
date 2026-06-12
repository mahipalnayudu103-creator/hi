with open("D:\\renko_playback\\backend\\services\\parquet_cache.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

import re
matches = [line for line in content.splitlines() if "check_legacy_cache_per_pip_counts" in line]
print("Occurrences of check_legacy_cache_per_pip_counts:")
for m in matches:
    print(m)

# Find the definition of check_legacy_cache_per_pip_counts
def_match = re.search(r'def check_legacy_cache_per_pip_counts.*?:(.*?)(?=def|\Z)', content, re.DOTALL)
if def_match:
    print("\nDefinition:")
    print(def_match.group(0)[:1500])
