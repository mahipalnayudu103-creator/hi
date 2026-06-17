import os
from pathlib import Path

# Base Directories
BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BACKEND_DIR.parent

# Cache directory (disk cache & parquet outputs)
CACHE_DIR = BACKEND_DIR / "cache_store"

# System limits & Constants
MAX_RAM_MB       = int(os.environ.get("RENKO_MAX_RAM", "24000"))
BRICK_BATCH_SIZE = int(os.environ.get("RENKO_BRICK_BATCH", "50000"))
TICK_TIME_FMT    = "%Y-%m-%d %H:%M:%S.%f"
MAX_MARKET_GAP_SECONDS = 10.0

# CUDA Settings
CUDA_DEVICE     = int(os.environ.get("CUDA_DEVICE", "0"))
CUDA_PINNED_MEM = os.environ.get("CUDA_PINNED_MEM", "False").lower() == "true"

# Server Configuration
HOST   = os.environ.get("RENKO_HOST", "127.0.0.1")
PORT   = int(os.environ.get("RENKO_PORT", "5006"))
RELOAD = os.environ.get("RENKO_RELOAD", "False").lower() == "true"

