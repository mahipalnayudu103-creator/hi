"""Reproduce the exact bug: Read first chunk from the large CSV and check polars parsing."""
import sys
sys.path.append(r"D:\renko_playback\backend")
sys.stdout.reconfigure(encoding='utf-8')

from pathlib import Path
from io import BytesIO
import numpy as np
import pandas as pd

csv_path = Path(r"C:\cTraderData\New folder\EURUSD_Ticks_2020_2026.csv")
delimiter = ","
time_col = "Time"

# Read header
with open(csv_path, "rb") as fh:
    header_bytes = fh.readline()
    data_start = fh.tell()
    
print(f"Header: {header_bytes.strip()}")

# Simulate stream_ticks reading the first chunk
# chunk_rows=1,677,721  ~= 64MB / 40 bytes_per_row
chunk_rows = 1_677_721
approx_bytes_per_row = 65  # real file has about 65 bytes per row
chunk_bytes = chunk_rows * approx_bytes_per_row

with open(csv_path, "rb") as fh:
    fh.seek(data_start)  # start at beginning
    raw_chunk = fh.read(chunk_bytes)
    
print(f"Read {len(raw_chunk):,} bytes for first chunk")

# Align to last newline
last_nl = raw_chunk.rfind(b"\n")
if last_nl != -1:
    aligned = raw_chunk[:last_nl + 1]
else:
    aligned = raw_chunk

print(f"Aligned chunk: {len(aligned):,} bytes")

# Count lines in chunk
line_count = aligned.count(b"\n")
print(f"Lines in chunk: {line_count:,}")

# Now parse with Polars (same as _process_chunk_polars)
import polars as pl
from utils.csv_reader import normalize_polars_time_col, extract_base_year

start_naive = pd.Timestamp("2020-01-01T22:01:12.821000")
end_naive = pd.Timestamp("2026-06-05T20:59:59.853000")
base_year = extract_base_year(csv_path)

usecols = [time_col, "Bid", "Ask"]
df = pl.read_csv(
    BytesIO(header_bytes + aligned),
    separator=delimiter,
    columns=usecols,
    infer_schema=True,
    try_parse_dates=True,
    ignore_errors=True,
)
print(f"\nPolars parsed {len(df):,} rows")
print(f"Time column dtype: {df[time_col].dtype}")
print(f"First 3 times: {df[time_col].head(3).to_list()}")
print(f"Last 3 times: {df[time_col].tail(3).to_list()}")

# Normalize time
state = None  # not wrapping
df = normalize_polars_time_col(df, time_col, state, base_year)
df = df.filter(pl.col(time_col).is_not_null())
print(f"\nAfter normalize: {len(df):,} rows")
print(f"Time dtype after normalize: {df[time_col].dtype}")

# Check time zone
tc = pl.col(time_col)
if df[time_col].dtype.time_zone:
    print(f"  Time zone detected: {df[time_col].dtype.time_zone}")
    tc = tc.dt.replace_time_zone(None)

# Apply start filter
df_filtered = df.filter(tc >= start_naive)
print(f"After start filter (>= {start_naive}): {len(df_filtered):,} rows")

# Apply end_t check on times array
if len(df_filtered) > 0:
    times = df_filtered[time_col].to_numpy()
    end_np = np.datetime64(end_naive)
    print(f"\ntimes dtype: {times.dtype}")
    print(f"end_np: {end_np}")
    
    if not np.issubdtype(times.dtype, np.datetime64):
        print("  WARNING: times are NOT datetime64! Trying conversion...")
        try:
            times_dt = times.astype("datetime64[ms]")
            print(f"  After conversion dtype: {times_dt.dtype}")
        except Exception as e:
            print(f"  Conversion failed: {e}")
    else:
        times_dt = times
        
    mask = times_dt <= end_np
    filtered_count = mask.sum()
    print(f"  After end_t filter: {filtered_count:,} rows (mask.all() = {mask.all()})")
    if not mask.all():
        # Find first row that fails the mask
        first_fail = np.argmin(mask)
        print(f"  First failing index: {first_fail}")
        print(f"  Time at fail: {times_dt[first_fail]}")
        print(f"  Time before fail: {times_dt[first_fail-1] if first_fail > 0 else 'N/A'}")
