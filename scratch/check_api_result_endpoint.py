with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

import re
# Find def get_job_result or similar
def_match = re.search(r'async def .*?job_id.*?result.*?:(.*?)(?=async def|\Z)', content, re.DOTALL)
if def_match:
    print("--- Result Endpoint ---")
    val = def_match.group(0)[:1500]
    print(val.encode('ascii', errors='ignore').decode('ascii'))
