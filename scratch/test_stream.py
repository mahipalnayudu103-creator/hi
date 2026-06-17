import sys
from pathlib import Path
import pandas as pd
import time
sys.path.append(r"D:\renko_playback\backend")
from utils.csv_stream import stream_ticks

csv_path = Path(r"C:\cTraderData\New folder\EURUSD_Ticks_2020_2026.csv")
start_t = pd.Timestamp("2020-01-01 22:00:00", tz="UTC")
end_t = pd.Timestamp("2026-06-05 21:00:00", tz="UTC")

print("Initializing stream_ticks...")
tick_gen = stream_ticks(
    csv_path   = csv_path,
    delimiter  = ",",
    time_col   = "Time",
    source     = "Bid",
    bid_col    = "Bid",
    ask_col    = "Ask",
    start_t    = start_t,
    end_t      = end_t,
    chunk_rows = 10000000, # 10M rows per chunk for test
)

t0 = time.perf_counter()
chunk_count = 0
total_rows = 0

for chunk in tick_gen:
    prices, times, bids, asks, nrows = chunk
    chunk_count += 1
    total_rows += nrows
    print(f"Chunk {chunk_count}: nrows={nrows}, first_time={times[0]}, last_time={times[-1]}")
    if chunk_count >= 5:
        print("Stopping test after 5 chunks.")
        break

t1 = time.perf_counter()
print(f"Streamed {total_rows} rows in {chunk_count} chunks in {t1-t0:.2f}s")
