import os
log_path = r"C:\Users\LocalAdmin.Professor\.gemini\antigravity\brain\a42b331c-2686-4535-9c44-bb2130fb07fb\.system_generated\tasks\task-5997.log"

if os.path.exists(log_path):
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    print("Total lines:", len(lines))
    print("=== Last 100 lines ===")
    for line in lines[-100:]:
        print(line, end='')
else:
    print("Log file not found.")
