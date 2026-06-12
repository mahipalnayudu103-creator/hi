with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

print("Lines 1420 to 1450:")
for i in range(1420 - 1, 1450):
    if i < len(lines):
        print(f"{i+1}: {lines[i]}", end="")
