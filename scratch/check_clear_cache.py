with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

target_idx = -1
for idx, line in enumerate(lines):
    if '/api/clear-cache' in line:
        target_idx = idx
        break

if target_idx != -1:
    print(f"Found on line {target_idx+1}")
    for i in range(target_idx - 5, target_idx + 40):
        val = f"{i+1}: {lines[i]}"
        print(val.encode('ascii', errors='ignore').decode('ascii'), end="")
else:
    print("Not found")
