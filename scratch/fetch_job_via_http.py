import requests
import json

job_id = "1597ec20-193d-4c83-886d-73fcf39cc769"

print("--- STATUS ---")
status = requests.get(f"http://127.0.0.1:5006/api/jobs/{job_id}/status").json()
print(json.dumps(status, indent=2))

print("\n--- RESULT ---")
result = requests.get(f"http://127.0.0.1:5006/api/jobs/{job_id}/result").json()
# print only keys of result["charts"]
if "charts" in result:
    charts_summary = {k: len(v) for k, v in result["charts"].items()}
    print("Result charts count:", charts_summary)
else:
    print(json.dumps(result, indent=2))
