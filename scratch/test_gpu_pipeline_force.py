import asyncio
import time
import requests
import json
import sys

def test_gpu_pipeline():
    url = "http://127.0.0.1:5006/api/jobs/build-renko"
    # Submit a job with processing_engine = "gpu" and custom pip sizes to bypass cache
    payload = {
        "csv_path": "C:\\cTraderData\\EURUSD_Ticks_2026_Jun01_Jun07.csv",
        "start_utc": "2026-06-01T00:00:00.817Z",
        "end_utc": "2026-06-07T23:59:59.774Z",
        "price_source": "Bid",
        "reversal_boxes": 2,
        "pip_size": 0.0001,
        # Using non-standard pips to ensure cache miss
        "chart_pips": [1.05, 2.05, 3.05, 4.05],
        "processing_engine": "gpu",
        "build_mode": "full",
        "chunk_rows": 250000
    }
    
    t0 = time.time()
    response = requests.post(url, json=payload)
    print("Response:", response.json())
    job_id = response.json().get("job_id")
    if not job_id:
        print("Failed to get job_id. Response:", response.text)
        sys.exit(1)
        
    # Poll job status
    status_url = f"http://127.0.0.1:5006/api/jobs/{job_id}/status"
    while True:
        status_resp = requests.get(status_url)
        status_data = status_resp.json()
        status = status_data.get("status")
        progress = status_data.get("progress_percent")
        engine = status_data.get("engine_used")
        print(f"Job Status: {status}, progress: {progress}%, engine: {engine}")
        if status in ("done", "error"):
            print("Diagnostics:")
            for d in status_data.get("diagnostics", []):
                clean_d = d.encode('ascii', errors='replace').decode('ascii')
                print("  ", clean_d)
            break
        time.sleep(0.1)
        
    duration = time.time() - t0
    print(f"Build job finished in {duration:.3f} seconds.")
    if status == "error":
        print("Error message:", status_data.get("error_message"))
    
    assert status == "done", f"Job failed with status {status}"
    
    # Check result
    result_url = f"http://127.0.0.1:5006/api/jobs/{job_id}/result"
    result_resp = requests.get(result_url)
    result_data = result_resp.json()
    print("Job Result:", result_data.get("bricks_built"))
    print("All pipeline tests passed!")

if __name__ == "__main__":
    test_gpu_pipeline()
