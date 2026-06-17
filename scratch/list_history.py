import sqlite3
import json
from pathlib import Path

db_path = Path("D:/renko_playback/backend/cache_store/build_history.sqlite")
if not db_path.exists():
    print(f"DB not found: {db_path}")
else:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM builds ORDER BY created_at DESC LIMIT 10")
    rows = cursor.fetchall()
    print("Recent builds in DB:")
    for r in rows:
        print(f"ID: {r['id']}, CSV: {Path(r['csv_path']).name}, range: {r['start_utc']} to {r['end_utc']}")
        print(f"  Pips: {r['chart_pips']}")
        print(f"  Brick counts: {r['brick_counts']}")
        print(f"  Ticks used: {r['ticks_used']}, Engine: {r['engine_used']}")
        print("-" * 50)
    conn.close()
