with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

print("Lines 1540 to 1640:")
for i in range(1540 - 1, 1640):
    if i < len(lines):
        print(f"{i+1}: {lines[i]}", end="")
