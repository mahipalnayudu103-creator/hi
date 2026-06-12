import logging
import os
from pathlib import Path
from fastapi import FastAPI

# ── Load .env from project root ───────────────────────────────────────────────
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().split("#")[0].strip())

from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import ORJSONResponse, JSONResponse

# Import router & setup
from routes.api import router as api_router, init_engine_status
from utils.monitor import set_high_priority, configure_thread_pools

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("renko_playback")

app = FastAPI(
    title="Renko Tick Playback Dashboard API",
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from pydantic import BaseModel
from typing import Optional

class ClientLog(BaseModel):
    level: str
    message: str
    url: Optional[str] = None
    line: Optional[int] = None
    column: Optional[int] = None
    error: Optional[str] = None

# Include API Router
app.include_router(api_router)

@app.post("/api/log")
async def client_log(log: ClientLog):
    logger.info(f"[CLIENT {log.level.upper()}] {log.message} (URL: {log.url}, Line: {log.line}, Col: {log.column}, Error: {log.error})")
    return {"status": "ok"}


@app.on_event("startup")
async def _on_startup():
    # Boost process priority (non-fatal if denied)
    set_high_priority()
    
    # Configure Polars/DuckDB thread pools BEFORE first import
    configure_thread_pools()
    
    # Probe and cache engine status
    engine_status = init_engine_status()
    logger.info(f"Engine matrix: {engine_status}")
    
    # Kick psutil CPU% baseline (first call always returns 0.0)
    try:
        import psutil
        psutil.cpu_percent(interval=None)
    except Exception:
        pass

# Mount Frontend static files
frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
logger.info(f"Frontend dir: {frontend_dir}, exists: {frontend_dir.exists()}")
if frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")
else:
    logger.warning(f"Frontend directory not found at {frontend_dir.resolve()}. Static mounting skipped.")

def _open_browser_when_ready(url: str, host: str, port: int) -> None:
    """Wait until the server socket is accepting connections, then open the app in a
    NEW TAB of the user's default browser (not a hardcoded Chrome). Runs in a daemon
    thread so it never blocks server startup. Gated by the RENKO_OPEN_BROWSER env var
    so headless/preview runs don't pop a window."""
    import socket
    import time
    import webbrowser

    check_host = "127.0.0.1" if host in ("0.0.0.0", "", "::") else host
    for _ in range(150):  # poll up to ~15s for the server to come up
        try:
            with socket.create_connection((check_host, port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)
    try:
        webbrowser.open_new_tab(url)
        logger.info(f"Opened browser tab at {url}")
    except Exception as exc:  # pragma: no cover - browser launch is best-effort
        logger.warning(f"Could not open browser automatically: {exc}")


if __name__ == "__main__":
    import threading
    import uvicorn
    # Import HOST, PORT, RELOAD from config
    from config import HOST, PORT, RELOAD

    # Auto-open the dashboard in a new browser tab when launched via start.bat / run.bat
    # (those set RENKO_OPEN_BROWSER=1). Uses the default browser, not Chrome specifically.
    if os.environ.get("RENKO_OPEN_BROWSER", "").strip().lower() in ("1", "true", "yes", "on"):
        _browser_url = f"http://127.0.0.1:{PORT}/"
        threading.Thread(
            target=_open_browser_when_ready,
            args=(_browser_url, HOST, PORT),
            daemon=True,
        ).start()

    if RELOAD:
        uvicorn.run("app:app", host=HOST, port=PORT, reload=True)
    else:
        uvicorn.run(app, host=HOST, port=PORT)
