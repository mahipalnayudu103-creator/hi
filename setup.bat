@echo off
setlocal enabledelayedexpansion

echo ===================================================
echo   Renko Tick Playback Dashboard — Setup Script
echo ===================================================
echo.

:: 1. Check Python installation
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not added to your system PATH.
    echo Please install Python 3.10+ from https://www.python.org/
    pause
    exit /b 1
)

:: 2. Create Virtual Environment
echo Creating virtual environment (.venv)...
python -m venv .venv
if errorlevel 1 (
    echo [ERROR] Failed to create virtual environment.
    pause
    exit /b 1
)
echo [OK] Virtual environment created successfully.
echo.

:: 3. Activate Virtual Environment and Install Requirements
echo Activating virtual environment and installing requirements...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERROR] Failed to activate virtual environment.
    pause
    exit /b 1
)

python -m pip install --upgrade pip
echo Installing dependencies from requirements.txt...
pip install -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Failed to install dependencies.
    pause
    exit /b 1
)
echo [OK] Dependencies installed successfully.
echo.

:: 4. Create local directories
if not exist "backend\cache_store" (
    echo Creating cache directory...
    mkdir "backend\cache_store"
)

echo.
echo ===================================================
echo   Setup Complete!
echo ===================================================
echo.
echo You can now run the backend server using the run.bat script
echo or by executing:
echo   .venv\Scripts\activate
echo   cd backend
echo   python app.py
echo.
pause
