with open("D:\\renko_playback\\backend\\utils\\csv_reader.py", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

import re
# Find def seek_first_timestamp_offset
def_match = re.search(r'def seek_first_timestamp_offset.*?:(.*?)(?=def|\Z)', content, re.DOTALL)
if def_match:
    print("--- seek_first_timestamp_offset ---")
    val = def_match.group(0)[:1500]
    print(val.encode('ascii', errors='ignore').decode('ascii'))
