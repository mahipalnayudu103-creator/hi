# 🚀 Running RenkoTerminal Locally

This guide provides the exact commands needed to initialize the environment and run the local server.

---

## 🛠️ Step 1: Automatic Setup

Run the setup batch file from the project root. This script will automatically check for Python, initialize a virtual environment (`.venv`), upgrade pip, and install all required libraries from the root `requirements.txt`:

```cmd
setup.bat
```

---

## 🏃‍♂️ Step 2: Start the Backend Server

Start the FastAPI backend server using the run batch script:

```cmd
run.bat
```

Alternatively, you can manually activate the environment and run it:

```cmd
.venv\Scripts\activate
cd backend
python app.py
```

The server will start on: **http://127.0.0.1:5006**

---

## 🖥️ Step 3: Open the Dashboard

Because the backend serves the frontend statically from the `/` path, you can access the full terminal dashboard by opening:

👉 **[http://127.0.0.1:5006](http://127.0.0.1:5006)**

---

## 🧪 Step 4: Verification

To make sure everything is running correctly, you can execute the automated test scripts in the background:

```cmd
.venv\Scripts\activate

# Run smoke tests
python -m unittest tests/test_smoke.py

# Run WebSocket playback test
python scratch/test_playback_ws.py

# Run streaming pipeline build test
python scratch/test_build_pipeline.py
```
