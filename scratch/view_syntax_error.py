with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

print("Lines 1470 to 1540:")
for i in range(1470 - 1, 1540):
    if i < len(lines):
        print(f"{i+1}: {lines[i]}", end="")
