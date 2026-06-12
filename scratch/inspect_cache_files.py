import os
from pathlib import Path
import pyarrow.parquet as pq

cache_dir = Path("D:/renko_playback/backend/cache_store")
if not cache_dir.exists():
    cache_dir = Path("D:/renko_playback/cache_store")

print("Files in cache_dir:")
for f in cache_dir.iterdir():
    if f.is_file():
        size = f.stat().st_size
        print(f"File: {f.name}, Size: {size} bytes")
        if f.suffix == ".parquet":
            try:
                meta = pq.read_metadata(str(f))
                print(f"  Rows: {meta.num_rows}")
            except Exception as e:
                print(f"  Error reading metadata: {e}")
    elif f.is_dir():
        print(f"Dir: {f.name}")
        for sub in f.iterdir():
            if sub.is_file():
                size = sub.stat().st_size
                print(f"  File: {sub.name}, Size: {size} bytes")
                if sub.suffix == ".parquet":
                    try:
                        meta = pq.read_metadata(str(sub))
                        print(f"    Rows: {meta.num_rows}")
                    except Exception as e:
                        print(f"    Error reading metadata: {e}")
