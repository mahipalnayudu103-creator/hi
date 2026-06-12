import os

keyword = "pip_size"
backend_dir = r"D:\renko_playback\backend"

for root, dirs, files in os.walk(backend_dir):
    for fname in files:
        if fname.endswith(".py"):
            fpath = os.path.join(root, fname)
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            lines = content.split("\n")
            for idx, line in enumerate(lines):
                if keyword in line:
                    rel_path = os.path.relpath(fpath, backend_dir)
                    print(f"{rel_path}:{idx+1}: {line.strip()}")
