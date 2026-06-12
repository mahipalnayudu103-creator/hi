with open("D:\\renko_playback\\frontend\\static\\js\\main.js", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

def print_section(start_idx, end_idx):
    for i in range(start_idx - 1, min(end_idx, len(lines))):
        val = f"{i+1}: {lines[i]}"
        print(val.encode('ascii', errors='ignore').decode('ascii'), end="")

print("--- SECTION 1 (around 311) ---")
print_section(295, 335)

print("\n--- SECTION 2 (around 1579) ---")
print_section(1560, 1600)
