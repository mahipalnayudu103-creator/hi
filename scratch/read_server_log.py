import os
log_path = r"C:\Users\LocalAdmin.Professor\.gemini\antigravity\brain\a42b331c-2686-4535-9c44-bb2130fb07fb\.system_generated\tasks\task-6285.log"
with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
    lines = f.readlines()
for line in lines:
    if 'BUILD' in line or 'Created job' in line or 'streaming' in line.lower() or 'complete' in line.lower() or 'ticks' in line.lower():
        print(line.strip())
