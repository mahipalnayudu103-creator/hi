import os

def search_files(directory, query):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.py', '.js', '.html')):
                path = os.path.join(root, file)
                try:
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()
                    for idx, line in enumerate(lines):
                        if query.lower() in line.lower():
                            print(f"{path}:{idx+1}: {line.strip()}")
                except Exception as e:
                    print(f"Error reading {path}: {e}")

print("Searching for 'terminal':")
search_files("D:\\renko_playback", "terminal")
print("\nSearching for 'grid':")
search_files("D:\\renko_playback", "grid")
