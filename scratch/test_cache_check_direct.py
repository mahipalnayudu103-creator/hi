import sys
from pathlib import Path
backend_dir = Path("D:/renko_playback/backend")
sys.path.insert(0, str(backend_dir))

from services.parquet_cache import check_legacy_cache_per_pip_counts

key = "941f1a6e20f298d5407bb33da3899bffde4ae55c087dcdc6b284c3e92f75d116"
pips = [1.37, 2.64]

res = check_legacy_cache_per_pip_counts(key, pips)
print("Result of check_legacy_cache_per_pip_counts:", res)
