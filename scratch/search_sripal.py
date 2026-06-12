import json

log_path = r"C:\Users\LocalAdmin.Professor\.gemini\antigravity\brain\a42b331c-2686-4535-9c44-bb2130fb07fb\.system_generated\logs\transcript.jsonl"
with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        if "sripal" in line.lower():
            print(line[:300])
