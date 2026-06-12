import json

log_path = r"C:\Users\LocalAdmin.Professor\.gemini\antigravity\brain\a42b331c-2686-4535-9c44-bb2130fb07fb\.system_generated\logs\transcript.jsonl"
with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        if any(term in line.lower() for term in ["github", "convert"]):
            try:
                data = json.loads(line)
                if "content" in data:
                    print("CONTENT:", data["content"][:200])
                elif "tool_calls" in line:
                    print("TOOL CALL:", line[:200])
            except Exception as e:
                print("Error parsing line:", e)
