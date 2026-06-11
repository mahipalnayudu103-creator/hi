"""
monitor.py — Real-time CPU / GPU / RAM monitoring.

Uses:
  psutil   → CPU%, RAM, process info
  pynvml   → NVIDIA GPU utilisation, VRAM (optional)

Never returns fake values. If a metric is unavailable, returns None.
"""

import os
import logging
from typing import Any, Dict

logger = logging.getLogger("renko_playback.monitor")

# ── psutil ────────────────────────────────────────────────────────────────────
try:
    import psutil
    _PSUTIL_OK = True
except ImportError:
    psutil = None          # type: ignore
    _PSUTIL_OK = False
    logger.warning("psutil not installed — CPU/RAM monitoring disabled.")

# ── pynvml ────────────────────────────────────────────────────────────────────
_NVML_OK = False
_nvml_handle = None
_gpu_name: str = ""

try:
    import pynvml
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        pynvml.nvmlInit()
    _nvml_handle = pynvml.nvmlDeviceGetHandleByIndex(0)
    _gpu_name = pynvml.nvmlDeviceGetName(_nvml_handle)
    if isinstance(_gpu_name, bytes):
        _gpu_name = _gpu_name.decode()
    _NVML_OK = True
    logger.info(f"NVML initialised: {_gpu_name}")
except Exception as exc:
    pynvml = None          # type: ignore
    logger.info(f"pynvml unavailable (no NVIDIA driver or not installed): {exc}")


# ─────────────────────────────────────────────────────────────────────────────

def set_high_priority() -> bool:
    """Attempt to raise process priority. Always safe — ignores failures."""
    if not _PSUTIL_OK:
        return False
    try:
        proc = psutil.Process()
        if os.name == "nt":
            proc.nice(psutil.HIGH_PRIORITY_CLASS)
        else:
            proc.nice(-10)
        logger.info("Process priority set to HIGH.")
        return True
    except Exception as exc:
        logger.info(f"Could not set high priority (non-fatal): {exc}")
        return False


def configure_thread_pools() -> Dict[str, int]:
    """
    Set environment variables so Polars and DuckDB use all CPU cores.
    Must be called BEFORE any polars / duckdb import.
    """
    cpu_count = os.cpu_count() or 4
    os.environ.setdefault("POLARS_MAX_THREADS", str(cpu_count))
    os.environ.setdefault("RAYON_NUM_THREADS",  str(cpu_count))   # Polars Rayon
    os.environ.setdefault("OMP_NUM_THREADS",    str(cpu_count))   # OpenMP
    logger.info(f"Thread pools configured for {cpu_count} logical cores.")
    return {"cpu_count": cpu_count}


def get_system_stats() -> Dict[str, Any]:
    """
    Return a snapshot of real system performance.
    All values are real; None means the metric is unavailable.
    """
    stats: Dict[str, Any] = {
        "cpu_percent":       None,
        "cpu_count":         None,
        "ram_used_gb":       None,
        "ram_total_gb":      None,
        "ram_percent":       None,
        "gpu_available":     _NVML_OK,
        "gpu_name":          _gpu_name or None,
        "gpu_percent":       None,
        "gpu_vram_used_gb":  None,
        "gpu_vram_total_gb": None,
    }

    # CPU + RAM via psutil
    if _PSUTIL_OK:
        try:
            stats["cpu_percent"] = psutil.cpu_percent(interval=None)
            stats["cpu_count"]   = psutil.cpu_count(logical=True)
            vm = psutil.virtual_memory()
            stats["ram_used_gb"]  = round(vm.used  / 1e9, 2)
            stats["ram_total_gb"] = round(vm.total / 1e9, 2)
            stats["ram_percent"]  = round(vm.percent, 1)
        except Exception as exc:
            logger.debug(f"psutil stats error: {exc}")

    # GPU via pynvml
    if _NVML_OK and _nvml_handle is not None:
        try:
            util     = pynvml.nvmlDeviceGetUtilizationRates(_nvml_handle)
            mem_info = pynvml.nvmlDeviceGetMemoryInfo(_nvml_handle)
            stats["gpu_percent"]       = util.gpu
            stats["gpu_vram_used_gb"]  = round(mem_info.used  / 1e9, 2)
            stats["gpu_vram_total_gb"] = round(mem_info.total / 1e9, 2)
        except Exception as exc:
            logger.debug(f"pynvml stats error: {exc}")

    return stats
