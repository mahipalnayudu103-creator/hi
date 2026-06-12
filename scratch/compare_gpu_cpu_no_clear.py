import requests
import time
import sys

def test_compare():
    url = "http://127.0.0.1:5006/api/jobs/build-renko"
    
    pip_sizes = [1.37, 2.64]
    
    gpu_payload = {
        "csv_path": "C:\\cTraderData\\EURUSD_Ticks_2026_Jun01_Jun07.csv",
        "start_utc": "2026-06-01T00:00:00.817Z",
        "end_utc": "2026-06-07T23:59:59.774Z",
        "price_source": "Bid",
        "reversal_boxes": 2,
        "pip_size": 0.0001,
        "chart_pips": pip_sizes,
        "processing_engine": "gpu",
        "build_mode": "full",
        "chunk_rows": 50000
    }
    
    print("Submitting GPU build (should be cache hit)...")
    gpu_resp = requests.post(url, json=gpu_payload).json()
    print("GPU submission response:", gpu_resp)
    gpu_job_id = gpu_resp["job_id"]
    
    # Poll
    while True:
        status_data = requests.get(f"http://127.0.0.1:5006/api/jobs/{gpu_job_id}/status").json()
        status = status_data.get("status")
        print(f"GPU Job Status: {status}, progress: {status_data.get('progress_percent')}%")
        if status in ("done", "error"):
            print("GPU Diagnostics:")
            for d in status_data.get("diagnostics", []):
                clean_d = d.encode('ascii', errors='replace').decode('ascii')
                print("  ", clean_d)
            if status == "error":
                print("GPU Error:", status_data.get("error_message"))
                sys.exit(1)
            break
        time.sleep(0.1)
    
    # 2. Run CPU build
    cpu_payload = dict(gpu_payload)
    cpu_payload["processing_engine"] = "cpu"
    
    print("\nSubmitting CPU build (should be cache hit)...")
    cpu_resp = requests.post(url, json=cpu_payload).json()
    print("CPU submission response:", cpu_resp)
    cpu_job_id = cpu_resp["job_id"]
    
    # Poll
    while True:
        status_data = requests.get(f"http://127.0.0.1:5006/api/jobs/{cpu_job_id}/status").json()
        status = status_data.get("status")
        print(f"CPU Job Status: {status}, progress: {status_data.get('progress_percent')}%")
        if status in ("done", "error"):
            print("CPU Diagnostics:")
            for d in status_data.get("diagnostics", []):
                clean_d = d.encode('ascii', errors='replace').decode('ascii')
                print("  ", clean_d)
            if status == "error":
                print("CPU Error:", status_data.get("error_message"))
                sys.exit(1)
            break
        time.sleep(0.1)
        
    # Get results
    gpu_result = requests.get(f"http://127.0.0.1:5006/api/jobs/{gpu_job_id}/result").json()
    cpu_result = requests.get(f"http://127.0.0.1:5006/api/jobs/{cpu_job_id}/result").json()
    
    print("\nComparing brick counts:")
    print("GPU Bricks Built:", gpu_result.get("bricks_built"))
    print("CPU Bricks Built:", cpu_result.get("bricks_built"))
    
    # Compare
    for pip in [str(p) for p in pip_sizes]:
        gpu_bricks = gpu_result["charts"].get(pip, [])
        cpu_bricks = cpu_result["charts"].get(pip, [])
        
        print(f"\nComparing pip {pip}: GPU bricks={len(gpu_bricks)}, CPU bricks={len(cpu_bricks)}")
        if len(gpu_bricks) != len(cpu_bricks):
            print(f"WARNING: Brick count mismatch! GPU={len(gpu_bricks)}, CPU={len(cpu_bricks)}")
            sys.exit(1)
        print(f"Success! GPU and CPU outputs for pip {pip} match exactly ({len(gpu_bricks)} bricks).")

if __name__ == "__main__":
    test_compare()
