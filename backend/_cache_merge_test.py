"""Verify per-pip partial caching: build subset, then superset reuses cached pips.
Temporary diagnostic — safe to delete."""
from services.parquet_cache import save_pip_cache, check_legacy_cache_per_pip, _legacy_parquet_path
from utils.cache import get_cache_key
from pathlib import Path

# Fake but stable identity (the real key also folds in csv size+mtime; here csv_path
# is non-existent so those are 0 — fine for testing the lookup/merge mechanics).
csv = Path("C:/fake/EURUSD_demo.csv")
common = dict(start_utc="2020-01-01T00:00:00Z", end_utc="2026-06-05T00:00:00Z",
              price_source="Bid", reversal_boxes=2, pip_size=0.0001, anchor="floor")

# BASE key = no pip list → shared across different pip-list builds
base_key = get_cache_key(csv, chart_pips=[], **common)

def fake_bricks(pip, n):
    return [{"time": i, "confirm_tick_index": i, "confirm_time": "2020-01-01 00:00:00",
             "open": 1.1, "high": 1.1001, "low": 1.0999, "close": 1.1001,
             "direction": "up", "tick_count": 1, "brick_size_pips": float(pip),
             "bid": 1.1, "ask": 1.1002} for i in range(n)]

# Clean any stale files from a prior run
for p in [1, 2, 3, 4, 5, 6]:
    _legacy_parquet_path(base_key, p).unlink(missing_ok=True)

print("BASE KEY:", base_key[:16], "...\n")

# ── User's first build: pips 1,2,3,4 ─────────────────────────────────────────
print("BUILD #1 — user builds pips [1,2,3,4]")
per_pip = check_legacy_cache_per_pip(base_key, [1, 2, 3, 4])
missing = [p for p in [1, 2, 3, 4] if per_pip[str(p)] is None]
print("  cache lookup ->", {k: (None if v is None else f"{len(v)} bricks") for k, v in per_pip.items()})
print("  missing (to build):", missing)
for p in missing:                       # simulate building + caching only missing pips
    save_pip_cache(base_key, p, fake_bricks(p, 10 * p))
print("  built & cached:", missing)

# ── User's second build: pips 5,6 only ───────────────────────────────────────
print("\nBUILD #2 — user builds pips [5,6] only (1,2,3,4 already cached)")
per_pip = check_legacy_cache_per_pip(base_key, [5, 6])
missing = [p for p in [5, 6] if per_pip[str(p)] is None]
print("  missing (to build):", missing, "  <- only 5,6 build, 1-4 untouched")
for p in missing:
    save_pip_cache(base_key, p, fake_bricks(p, 10 * p))

# ── User's third build: the full set 1..6 → everything reused, nothing rebuilt ─
print("\nBUILD #3 — user builds full [1,2,3,4,5,6]")
per_pip = check_legacy_cache_per_pip(base_key, [1, 2, 3, 4, 5, 6])
missing = [p for p in [1, 2, 3, 4, 5, 6] if per_pip[str(p)] is None]
cached = [k for k, v in per_pip.items() if v is not None]
print("  cached (reused, NO rebuild):", cached)
print("  missing (to build):", missing)

# ── Show the individual cache files on disk ──────────────────────────────────
print("\nIndividual per-pip cache files on disk:")
for p in [1, 2, 3, 4, 5, 6]:
    path = _legacy_parquet_path(base_key, p)
    print(f"  pip {p}: {'EXISTS' if path.exists() else 'MISSING'}  ({path.name})")

ok = (missing == [] and sorted(int(c) for c in cached) == [1, 2, 3, 4, 5, 6])
print("\nRESULT:", "PASS — full reuse, zero duplicate builds" if ok else "FAIL")

# cleanup
for p in [1, 2, 3, 4, 5, 6]:
    _legacy_parquet_path(base_key, p).unlink(missing_ok=True)
