import os

users = ["mahip", "Professor2", "Sripal1", "LocalAdmin.Professor"]
for user in users:
    path = f"C:\\Users\\{user}"
    if os.path.exists(path):
        # Check subdirectories like AppData, Documents, etc.
        for root, dirs, files in os.walk(path):
            # prune search to avoid long running
            if any(p in root.lower() for p in ["appdata\\local\\microsoft", "appdata\\local\\packages", "node_modules", "package_cache"]):
                continue
            for file in files:
                if file.lower() in [".gitconfig", "gitconfig", "github"]:
                    print(f"File: {os.path.join(root, file)}")
                elif "git" in file.lower() or "github" in file.lower():
                    # check if it contains useful info
                    if file.endswith((".txt", ".json", ".ini", ".cfg", ".gitconfig")):
                        print(f"Candidate file: {os.path.join(root, file)}")
