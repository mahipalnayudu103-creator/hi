@echo off
echo ===================================================
echo   Renko Tick Playback Dashboard — Startup Script
echo ===================================================
echo.

:: Check if virtual environment exists
if not exist ".venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found. Please run setup.bat first!
    pause
    exit /b 1
)

:: Activate virtual environment
echo Activating virtual environment (.venv)...
call .venv\Scripts\activate.bat

:: Start backend server
echo Starting backend server on http://127.0.0.1:5006 ...
cd backend
python main.py

pause
