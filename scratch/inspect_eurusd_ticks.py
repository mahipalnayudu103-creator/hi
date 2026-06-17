import pandas as pd
from pathlib import Path

csv_path = Path("C:\\cTraderData\\New folder\\EURUSD_Ticks_2020_2026.csv")
if not csv_path.exists():
    print(f"File not found: {csv_path}")
else:
    print(f"File size: {csv_path.stat().st_size / 1024 / 1024 / 1024:.3f} GB")
    # Read the first 10 lines using standard python open to see raw structure
    with open(csv_path, "r", encoding="utf-8") as f:
        for i in range(10):
            print(f"Line {i+1}: {repr(f.readline())}")
