with open("D:\\renko_playback\\backend\\services\\parquet_cache.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

import re
# Find def _legacy_parquet_path
def_match = re.search(r'def _legacy_parquet_path.*?:(.*?)(?=def|\Z)', content, re.DOTALL)
if def_match:
    print("--- _legacy_parquet_path ---")
    val = def_match.group(0)[:500]
    print(val.encode('ascii', errors='ignore').decode('ascii'))

# Find def check_legacy_cache_per_pip
def_match2 = re.search(r'def check_legacy_cache_per_pip\(.*?:(.*?)(?=def|\Z)', content, re.DOTALL)
if def_match2:
    print("\n--- check_legacy_cache_per_pip ---")
    val = def_match2.group(0)[:1500]
    print(val.encode('ascii', errors='ignore').decode('ascii'))

# Find def read_legacy_pip_cache_window
def_match3 = re.search(r'def read_legacy_pip_cache_window\(.*?:(.*?)(?=def|\Z)', content, re.DOTALL)
if def_match3:
    print("\n--- read_legacy_pip_cache_window ---")
    val = def_match3.group(0)[:1500]
    print(val.encode('ascii', errors='ignore').decode('ascii'))
