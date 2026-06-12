import os

users = ["mahip", "Professor2", "Sripal1", "LocalAdmin.Professor"]
for user in users:
    path = f"C:\\Users\\{user}"
    if os.path.exists(path):
        for item in os.listdir(path):
            if "git" in item.lower():
                print(f"Found in {path}: {item}")
