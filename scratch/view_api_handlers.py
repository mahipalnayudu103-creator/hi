with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

def print_section(start_idx, end_idx):
    for i in range(start_idx - 1, min(end_idx, len(lines))):
        print(f"{i+1}: {lines[i]}", end="")

print("--- SECTION 1 (build_renko around 270) ---")
print_section(260, 310)

print("\n--- SECTION 2 (post_build_renko_job around 1443) ---")
print_section(1435, 1495)
