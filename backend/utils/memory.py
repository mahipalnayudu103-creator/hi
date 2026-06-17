"""
memory.py — RAM monitoring helpers for streaming Renko pipeline.

Provides:
  - get_process_ram_mb()  : current process RSS in MB
  - log_chunk_memory()    : structured per-chunk log line
  - check_ram_pressure()  : returns True if RAM exceeds MAX_RAM_MB
  - force_gc()            : flush unreachable objects + return new RAM usage

MAX_RAM_MB is the soft limit. When exceeded:
  - gc.collect() is forced
  - A warning is broadcast to job subscribers
  - Chunk size may be reduced (caller responsibility)
"""

import gc
import logging
import os
from typing import Dict
from config import MAX_RAM_MB

logger = logging.getLogger("renko_playback.memory")

# ── Configuration ─────────────────────────────────────────────────────────────
WARN_RAM_MB: float = MAX_RAM_MB * 0.80   # warn at 80 % of limit

# ── psutil (optional) ─────────────────────────────────────────────────────────
try:
    import psutil as _psutil
    _PSUTIL_OK = True
except ImportError:
    _psutil = None   # type: ignore
    _PSUTIL_OK = False

_process = _psutil.Process(os.getpid()) if _PSUTIL_OK else None


def get_process_ram_mb() -> float:
    """Return current process RSS in MB.  Returns 0.0 if psutil unavailable."""
    if not _PSUTIL_OK or _process is None:
        return 0.0
    try:
        return _process.memory_info().rss / (1024 * 1024)
    except Exception:
        return 0.0


def get_system_ram_mb() -> float:
    """Return total system used RAM in MB."""
    if not _PSUTIL_OK:
        return 0.0
    try:
        vm = _psutil.virtual_memory()
        return vm.used / (1024 * 1024)
    except Exception:
        return 0.0


def check_ram_pressure() -> bool:
    """Return True if process RAM exceeds MAX_RAM_MB."""
    return get_process_ram_mb() >= MAX_RAM_MB


def force_gc() -> float:
    """Run full GC and return new RAM usage in MB."""
    gc.collect()
    return get_process_ram_mb()


def log_chunk_memory(
    job_id: str,
    chunk_num: int,
    rows_scanned: int,
    current_time: str,
    brick_counts: Dict[str, int],
) -> float:
    """
    Log a structured per-chunk memory line.  Returns current RAM in MB.

    Example output:
      [BUILD] job=abc123 chunk=15 rows=7,500,000 time=2026-01-03T10:21:11Z
              ram=8200MB bricks_1=12000 bricks_2=7000 bricks_3=4100 bricks_4=2500
    """
    ram_mb = get_process_ram_mb()
    brick_parts = " ".join(
        f"bricks_{k}={v:,}" for k, v in sorted(brick_counts.items())
    )
    logger.info(
        f"[BUILD] job={job_id[:8]} chunk={chunk_num} "
        f"rows={rows_scanned:,} time={current_time} "
        f"ram={ram_mb:.0f}MB {brick_parts}"
    )
    if ram_mb >= MAX_RAM_MB:
        logger.warning(
            f"[MEMORY WARNING] job={job_id[:8]} RAM={ram_mb:.0f}MB "
            f"exceeds MAX_RAM_MB={MAX_RAM_MB:.0f}MB — forcing GC"
        )
        ram_mb = force_gc()
        logger.warning(f"[MEMORY] After GC: {ram_mb:.0f}MB")
    elif ram_mb >= WARN_RAM_MB:
        logger.warning(
            f"[MEMORY CAUTION] job={job_id[:8]} RAM={ram_mb:.0f}MB "
            f"approaching limit ({WARN_RAM_MB:.0f}MB threshold)"
        )
    return ram_mb
