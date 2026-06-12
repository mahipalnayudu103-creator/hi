@echo off
setlocal

set "ROOT=%~dp0"
set "PORT=5006"
set "VENV_PYTHON=%ROOT%.venv\Scripts\python.exe"

echo ===================================================
echo   Renko Tick Playback Dashboard - Start
echo ===================================================
echo.

cd /d "%ROOT%"

echo Stopping existing Renko server on port %PORT%...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$renkoPids = @(Get-NetTCPConnection -LocalPort %PORT% -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique); foreach ($id in $renkoPids) { Stop-Process -Id $id -Force -ErrorAction SilentlyContinue }"
timeout /t 1 /nobreak >nul 2>&1
echo [OK] Port %PORT% is clear.
echo.

if not exist "%VENV_PYTHON%" (
    echo Setting up Python environment...

    python --version >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] Python not found. Install Python 3.10+ from https://www.python.org/
        pause
        exit /b 1
    )

    python -m venv "%ROOT%.venv"
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )

    call "%ROOT%.venv\Scripts\activate.bat"
    python -m pip install --upgrade pip
    pip install -r "%ROOT%requirements.txt"
    if errorlevel 1 (
        echo [ERROR] Failed to install dependencies.
        pause
        exit /b 1
    )
    echo [OK] Setup complete.
    echo.
)

if not exist "%ROOT%backend\cache_store" mkdir "%ROOT%backend\cache_store"

set "RENKO_OPEN_BROWSER=1"

echo Starting backend server...
start "Renko Backend" cmd /k ""%VENV_PYTHON%" "%ROOT%backend\app.py""

echo.
echo [OK] Server starting. Python will open a new tab at http://127.0.0.1:%PORT%/
echo [OK] Close the "Renko Backend" window to stop the server.
echo.
