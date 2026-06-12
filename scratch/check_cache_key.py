import os

def search_files(directory, query):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                    for idx, line in enumerate(lines):
                        if query in line:
                            print(f"{path}:{idx+1}: {line.strip()}")
                except Exception as e:
                    print(f"Error reading {path}: {e}")

search_files("D:\\renko_playback", "def get_cache_key")
