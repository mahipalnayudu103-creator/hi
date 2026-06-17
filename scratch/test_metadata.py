import sys
from pathlib import Path
sys.path.append(r"D:\renko_playback")
from backend.utils.csv_reader import summarize_csv_file

path = Path(r"C:\cTraderData\New folder\EURUSD_Ticks_2020_2026.csv")
res = summarize_csv_file(path)
print("Metadata results:")
for k, v in res.items():
    print(f"  {k}: {v} (type: {type(v)})")
