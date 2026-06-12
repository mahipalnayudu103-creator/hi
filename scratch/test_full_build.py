"""Submit a clean build for the large CSV with full date range, then inspect results."""
import sys
import requests
import time
import json

sys.stdout.reconfigure(encoding='utf-8')

BASE = "http://127.0.0.1:5006"

# Wait for server to be ready
for i in range(10):
    try:
        r = requests.get(f"{BASE}/api/health", timeout=2)
        if r.ok:
            print("Server is ready.")
            break
    except:
        pass
    time.sleep(1)
else:
    print("Server not ready after 10s!")
    sys.exit(1)

# Clear cache first
print("Clearing cache...")
requests.post(f"{BASE}/api/clear-cache")
time.sleep(0.5)

# Build with the full date range that the frontend would send
params = {
    "csv_path": r"C:\cTraderData\New folder\EURUSD_Ticks_2020_2026.csv",
    "start_utc": "2020-01-01T22:01:12.821Z",
    "end_utc": "2026-06-05T20:59:59.853Z",
    "price_source": "Bid",
    "reversal_boxes": 2,
    "pip_size": 0.0001,
    "anchor": "floor",
    "chart_pips": [1.0, 2.0, 3.0, 4.0],
    "processing_engine": "gpu",
    "build_mode": "full",
    "chunk_size_mb": 64
}

print(f"\nSubmitting build with full range: {params['start_utc']} to {params['end_utc']}")
resp = requests.post(f"{BASE}/api/jobs/build-renko", json=params)
print(f"Submission status: {resp.status_code}")
job_data = resp.json()
print(f"job_id: {job_data['job_id']}, cache_hit: {job_data.get('cache_hit', False)}")

job_id = job_data["job_id"]

# Poll until done
for i in range(120):  # max 2 minutes
    time.sleep(2)
    status_resp = requests.get(f"{BASE}/api/jobs/{job_id}/status").json()
    status = status_resp['status']
    engine = status_resp.get('engine_used', '')
    ticks = status_resp.get('ticks_used', 0)
    bricks = status_resp.get('bricks_built', {})
    print(f"  [{i*2}s] Status={status} | Ticks={ticks:,} | Bricks={bricks}")
    if status in ('done', 'error'):
        break

if status == 'done':
    # Get the result
    result_resp = requests.get(f"{BASE}/api/jobs/{job_id}/result").json()
    print(f"\n=== RESULT ===")
    print(f"Engine: {result_resp.get('engine_used')}")
    print(f"Ticks used: {result_resp.get('ticks_used'):,}")
    print(f"Rows scanned: {result_resp.get('rows_scanned'):,}")
    print(f"Bricks built: {result_resp.get('bricks_built')}")
    print(f"Total bricks built: {result_resp.get('total_bricks_built')}")
    
    charts = result_resp.get("charts", {})
    for pip_str, bricks_list in charts.items():
        if bricks_list:
            first = bricks_list[0]
            last = bricks_list[-1]
            print(f"\n  Pip {pip_str}: {len(bricks_list)} bricks")
            print(f"    First: time={first.get('time')}, open={first.get('open')}, close={first.get('close')}, direction={first.get('direction')}")
            print(f"    Last:  time={last.get('time')}, open={last.get('open')}, close={last.get('close')}, direction={last.get('direction')}")
            
            # Verify brick sizes
            if len(bricks_list) > 1:
                sizes = set()
                for b in bricks_list[:100]:
                    s = abs(round(b['close'] - b['open'], 6))
                    sizes.add(s)
                print(f"    Unique body sizes (first 100): {sorted(sizes)}")
                expected_size = round(float(pip_str) * 0.0001, 6)
                print(f"    Expected body size: {expected_size}")
                if expected_size in sizes:
                    print(f"    ✅ Correct brick size found")
                else:
                    print(f"    ❌ WRONG brick size!")
        else:
            print(f"\n  Pip {pip_str}: EMPTY!")
else:
    print(f"\nBuild failed with status: {status}")
    print(f"Diagnostics: {status_resp.get('diagnostics', [])}")
