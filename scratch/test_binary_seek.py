"""Test the binary seek function on the large CSV to verify it finds the correct start position."""
import sys
sys.path.append(r"D:\renko_playback\backend")
sys.stdout.reconfigure(encoding='utf-8')

from pathlib import Path
import pandas as pd
from utils.csv_reader import (
    seek_first_timestamp_offset, read_header_columns, 
    open_compressed_file, get_file_size
)

csv_path = Path(r"C:\cTraderData\New folder\EURUSD_Ticks_2020_2026.csv")
delimiter = ","
time_col = "Time"

# Get the header and data_start
columns = read_header_columns(csv_path, delimiter)
time_index = columns.index(time_col) if time_col in columns else 0
print(f"Columns: {columns}")
print(f"Time index: {time_index}")

with open_compressed_file(csv_path, "rb") as fh:
    header_line = fh.readline()
    data_start = fh.tell()
    print(f"Header: {header_line.strip()}")
    print(f"Data starts at byte: {data_start}")

file_size = get_file_size(csv_path)
print(f"File size: {file_size:,} bytes ({file_size / 1e9:.2f} GB)")

# Now test binary seek with the full date range
start_t = pd.Timestamp("2020-01-01T22:01:12.821Z")
print(f"\nTesting seek for start_t = {start_t}")
start_offset = seek_first_timestamp_offset(csv_path, start_t, data_start, time_index, delimiter)
print(f"Binary seek returned offset: {start_offset:,}")
print(f"Percentage into file: {start_offset / file_size * 100:.4f}%")

# Read the first few lines from that offset
with open_compressed_file(csv_path, "rb") as fh:
    fh.seek(start_offset)
    for i in range(5):
        line = fh.readline().decode("utf-8", errors="replace").strip()
        print(f"  Line {i}: {line}")

# Read the last few lines before EOF for context
print(f"\n--- Last lines near end of file ---")
with open_compressed_file(csv_path, "rb") as fh:
    fh.seek(max(0, file_size - 500))
    remainder = fh.read().decode("utf-8", errors="replace").strip()
    for line in remainder.split("\n")[-5:]:
        print(f"  {line}")

# What happens if we start from the beginning?
print(f"\n--- First data lines ---")
with open_compressed_file(csv_path, "rb") as fh:
    fh.seek(data_start)
    for i in range(5):
        line = fh.readline().decode("utf-8", errors="replace").strip()
        print(f"  Line {i}: {line}")

# Test: what if the end_t used in the frontend is wrong?
# Let's check the selected range that would be sent by the frontend
print(f"\n--- Metadata timestamps ---")
from utils.csv_reader import summarize_csv_file
summary = summarize_csv_file(csv_path)
print(f"  first_time: {summary['first_time']}")
print(f"  last_time: {summary['last_time']}")

# ISO conversion as done by the API
start_utc = summary['first_time'].isoformat()
if not start_utc.endswith("Z") and "+00:00" not in start_utc:
    start_utc += "Z"
end_utc = summary['last_time'].isoformat()
if not end_utc.endswith("Z") and "+00:00" not in end_utc:
    end_utc += "Z"
print(f"  start_utc ISO: {start_utc}")
print(f"  end_utc ISO: {end_utc}")

# Now test: JavaScript Date.parse of these values
# The frontend does: new Date(metadata.file_start_utc).getTime()
# Then sends it back as ISO string via selectedStartUtc/selectedEndUtc
# Let's check the end_t binary seek
end_t_ts = pd.Timestamp(end_utc)
print(f"\nTesting seek for end_t = {end_t_ts}")
end_offset = seek_first_timestamp_offset(csv_path, end_t_ts, data_start, time_index, delimiter)
print(f"Binary seek returned offset: {end_offset:,}")
print(f"Percentage into file: {end_offset / file_size * 100:.4f}%")

# Read lines at that offset
with open_compressed_file(csv_path, "rb") as fh:
    fh.seek(end_offset)
    for i in range(3):
        line = fh.readline().decode("utf-8", errors="replace").strip()
        print(f"  Line {i}: {line}")
