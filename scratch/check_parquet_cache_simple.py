with open("D:\\renko_playback\\backend\\services\\parquet_cache.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

for line in content.splitlines():
    if "_legacy_parquet_path" in line or "def " in line:
        if "def " in line or "_legacy_parquet_path" in line:
            print(line)
