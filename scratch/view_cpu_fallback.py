with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if "run_build_pipeline(" in line:
        print(f"--- Occurrence at {idx+1} ---")
        for i in range(max(0, idx - 10), min(len(lines), idx + 25)):
            val = f"{i+1}: {lines[i]}"
            print(val.encode('ascii', errors='ignore').decode('ascii'), end="")
