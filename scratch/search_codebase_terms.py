import os

# Search all files in D:\renko_playback for "01" or "mahipal" or "nayudu"
terms = ["01", "mahipal", "nayudu"]
for root, dirs, files in os.walk(r"D:\renko_playback"):
    if ".git" in root or ".venv" in root or ".claude" in root or ".ipynb_checkpoints" in root:
        continue
    for file in files:
        filepath = os.path.join(root, file)
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            for term in terms:
                if term in content:
                    print(f"Found '{term}' in {filepath}")
        except Exception as e:
            pass
