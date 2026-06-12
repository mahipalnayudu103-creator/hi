@echo off
setlocal

set "ROOT=%~dp0"
set "PORT=5006"
set "VENV_PYTHON=%ROOT%.venv\Scripts\python.exe"

echo ===================================================
echo   Renko Tick Playback Dashboard - Startup Script
echo ===================================================
echo.

cd /d "%ROOT%"

echo Stopping existing Renko server on port %PORT%...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$renkoPids = @(Get-NetTCPConnection -LocalPort %PORT% -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique); foreach ($id in $renkoPids) { Stop-Process -Id $id -Force -ErrorAction SilentlyContinue }"
timeout /t 1 /nobreak >nul 2>&1
echo [OK] Port %PORT% is clear.
echo.

if exist "%VENV_PYTHON%" (
    set "PYTHON=%VENV_PYTHON%"
    echo Using virtual environment.
) else (
    set "PYTHON=python"
    echo Using system Python.
)

if not exist "%ROOT%backend\cache_store" mkdir "%ROOT%backend\cache_store"

set "RENKO_OPEN_BROWSER=1"

echo Starting backend server on http://127.0.0.1:%PORT% ...
"%PYTHON%" "%ROOT%backend\app.py"

echo.
echo Server stopped.
pause
