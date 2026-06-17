"""
Check: what dates does the frontend slider actually send?
The frontend converts the metadata timestamps to epoch ms via:
  new Date(metadata.file_start_utc).getTime()
  new Date(metadata.file_end_utc).getTime()

Then the slider returns ms values, which are converted back to ISO strings.
Let's check if there's a precision/rounding issue.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

# The backend returns these ISOs:
start_utc_iso = "2020-01-01T22:01:12.821000Z"
end_utc_iso = "2026-06-05T20:59:59.853000Z"

# JavaScript Date.parse behavior:
# new Date("2020-01-01T22:01:12.821000Z").getTime()
# JS Date only has millisecond precision, so "821000" → "821" ms
# Actually JS only sees ".821000" and parses first 3 digits as ms → 821 ms

# After the slider, the frontend does:
# selectedStartUtc = startDate.toISOString() → "2020-01-01T22:01:12.821Z"
# selectedEndUtc = endDate.toISOString() → "2026-06-05T20:59:59.853Z"

# So the frontend sends back ISO strings with ms precision (3 decimal places)
# The backend then does: pd.Timestamp(request.start_utc, tz="UTC")

import pandas as pd

# What the frontend would send:
fe_start = "2020-01-01T22:01:12.821Z"
fe_end = "2026-06-05T20:59:59.853Z"

start_t = pd.Timestamp(fe_start, tz="UTC")
end_t = pd.Timestamp(fe_end, tz="UTC")
print(f"Frontend start: {start_t}")
print(f"Frontend end:   {end_t}")

# Naive versions for comparison
start_naive = start_t.tz_convert("UTC").tz_localize(None)
end_naive = end_t.tz_convert("UTC").tz_localize(None)
print(f"Start naive: {start_naive}")
print(f"End naive:   {end_naive}")

# BUT WAIT: the noUiSlider has step=1000 (1 second). 
# So the slider snaps to whole seconds!
# That means:
# fileStartMs = 1577916072821 → slider rounds down to 1577916072000
# fileEndMs = 1749156599853 → slider rounds down to 1749156599000

# Actually noUiSlider step=1000 means it rounds to nearest 1000ms
import math

file_start_ms = 1577916072821
file_end_ms = 1749156599853

# noUiSlider with step=1000: start=[fileStartMs, defEndMs]
# But the initial values are the exact ms timestamps
# The step only affects dragging — the initial values are set exactly
# Let's test the actual slider values

# Default start: [fileStartMs, fileEndMs] → these are the raw ms values
# When the slider fires 'update', it returns [start, end] as floats
# Then: const s = parseFloat(values[0]);
# new Date(s) → new Date(1577916072821.0) → 2020-01-01T22:01:12.821Z
# startDate.toISOString() → "2020-01-01T22:01:12.821Z"
# This should be correct.

# HOWEVER — what if the slider's range has step=1000, and the initial values
# get snapped to the nearest step?
# noUiSlider documentation says: If step is set, values will be rounded to step.
# That means:
# fileStartMs=1577916072821 → snapped to 1577916073000 (rounded up)
# fileEndMs=1749156599853 → snapped to 1749156600000 (rounded up)

# NO WAIT: noUiSlider's step=1000 rounds to nearest multiple of 1000
# 1577916072821 / 1000 = 1577916072.821 → rounds to 1577916073.0 → 1577916073000
# 1749156599853 / 1000 = 1749156599.853 → rounds to 1749156600.0 → 1749156600000

# So the slider might send:
start_snapped_ms = round(file_start_ms / 1000) * 1000
end_snapped_ms = round(file_end_ms / 1000) * 1000
print(f"\nOriginal start_ms: {file_start_ms}")
print(f"Snapped start_ms:  {start_snapped_ms}")
print(f"Original end_ms:   {file_end_ms}")
print(f"Snapped end_ms:    {end_snapped_ms}")

# Now check if the snapped values cause issues:
from datetime import datetime, timezone
snapped_start = datetime.fromtimestamp(start_snapped_ms / 1000, tz=timezone.utc)
snapped_end = datetime.fromtimestamp(end_snapped_ms / 1000, tz=timezone.utc)
print(f"Snapped start: {snapped_start.isoformat()}")
print(f"Snapped end:   {snapped_end.isoformat()}")

# Check: is the isDateOnly check in main.js relevant?
# isDateOnly = (startDate.getUTCHours() === 0 && startDate.getUTCMinutes() === 0 && ...)
# 2020-01-01T22:01:13Z → hours=22, not 0 → isDateOnly = false
# So no adjustment.

# The real question: what does the noUiSlider actually do with step=1000
# when the initial values aren't multiples of 1000?
# According to noUiSlider docs, start values ARE snapped to the step.
# So the slider default range would be:
# start = [1577916073000, 1749156600000]
# which corresponds to:
# "2020-01-01T22:01:13.000Z" to "2026-06-05T21:00:00.000Z"

# This is CLOSE but slightly off from the actual file range.
# 2020-01-01T22:01:13.000Z > 2020-01-01T22:01:12.821Z
# This means the start_t is 179ms AFTER the first tick!
# And end_t is 147ms AFTER the last tick — which is fine.

# But wait — the binary seek should still find byte 34 since 22:01:13.000 > 22:01:12.821
# Actually no — if start_t is 22:01:13.000, the seek would find the first row >= that time.
# 22:01:12.821 < 22:01:13.000, so the first few ticks would be SKIPPED.
# But that doesn't explain 642,187 ticks out of 166M.

# Let me check if there's a Date range slider issue with the update callback
# Hmm... Actually there's another issue. Let me check the isDateOnly logic more carefully.
# When BOTH slider handles are at times with H:M:S = 00:00:00, isDateOnly becomes true
# and the end is adjusted by +24h. But that shouldn't apply here.

# Actually, let me just check what the frontend ACTUALLY sends by looking at logs more carefully.
# The server log shows "642,187 ticks" — let me check if this matches a date sub-range.

# 642,187 ticks at ~65 bytes/tick = ~41.7 MB → that's about 1 month of data
# The first row is 2020-01-01 and the small test showed ~1.65M ticks per month
# So 642K ticks ≈ ~12 days of data

# WAIT: the test_http_submission.py script in the scratch folder uses:
# "end_utc": "2020-01-13T22:01:12.821Z"  → 12 days only!
# But that was just a test script, not the actual frontend.

# The actual answer: we need to see what the frontend sends.
# The server should log the actual start/end parameters.

print("\n\n=== CONCLUSION ===")
print("Need to add diagnostic logging to the build endpoint to see exact")
print("start_utc/end_utc the frontend sends for the large CSV build.")
print("The binary seek and polars parsing are working correctly.")
print("The slider step=1000 snapping is negligible.")
