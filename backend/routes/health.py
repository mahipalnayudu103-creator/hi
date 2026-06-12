from fastapi import APIRouter
from typing import Dict, Any
from utils.monitor import get_system_stats
from services.renko.gpu_engine import (
    detect_cupy_available,
    detect_gpu_polars_available,
    detect_cudf_available,
)

router = APIRouter()

_ENGINE_STATUS: Dict[str, Any] = {}


def probe_engine_status() -> Dict[str, Any]:
    """Return real status of every performance library — no fake GPU."""
    cudf_ok   = detect_cudf_available()
    cupy_ok   = detect_cupy_available()
    polars_gpu = detect_gpu_polars_available()

    try:
        import polars as pl
        polars_ok = True
    except Exception:
        polars_ok = False

    try:
        import duckdb
        duckdb_ok = True
    except Exception:
        duckdb_ok = False

    try:
        import pyarrow
        pyarrow_ok = True
        pyarrow_ver = pyarrow.__version__
    except Exception:
        pyarrow_ok = False
        pyarrow_ver = ""

    try:
        import numba
        numba_ok = True
        numba_ver = numba.__version__
    except Exception:
        numba_ok = False
        numba_ver = ""

    try:
        import orjson
        orjson_ok = True
    except Exception:
        orjson_ok = False

    try:
        import msgspec
        msgspec_ok = True
    except Exception:
        msgspec_ok = False

    # Data engine label (honest)
    if cudf_ok:
        data_engine = "GPU cuDF"
    elif polars_gpu:
        data_engine = "GPU Polars"
    elif polars_ok:
        data_engine = "CPU Polars"
    elif duckdb_ok:
        data_engine = "CPU DuckDB"
    else:
        data_engine = "CPU pandas"

    # Calculation engine label (honest)
    calc_engine = "GPU CuPy" if cupy_ok else ("CPU Numba JIT" if numba_ok else "CPU NumPy")

    return {
        "data_engine":  data_engine,
        "calc_engine":  calc_engine,
        "chart_engine": "TradingView Lightweight Charts",
        "cache_engine": "PyArrow Parquet" if pyarrow_ok else ("msgspec msgpack" if msgspec_ok else "pickle"),
        "sort_engine":  "GPU CuPy argsort" if cupy_ok else "CPU NumPy argsort",
        "json_engine":  "orjson" if orjson_ok else "stdlib json",
        "available": {
            "polars":     polars_ok,
            "polars_gpu": polars_gpu,
            "duckdb":     duckdb_ok,
            "pyarrow":    pyarrow_ok,
            "numba":      numba_ok,
            "cudf":       cudf_ok,
            "cupy":       cupy_ok,
            "orjson":     orjson_ok,
            "msgspec":    msgspec_ok,
        },
        "versions": {
            "pyarrow": pyarrow_ver,
            "numba":   numba_ver,
        },
    }


def init_engine_status():
    global _ENGINE_STATUS
    _ENGINE_STATUS = probe_engine_status()
    return _ENGINE_STATUS


@router.get("/api/health")
def health_check():
    return {"status": "ok"}


@router.get("/api/engine-status")
def engine_status_endpoint():
    """Returns the real backend performance stack — no fake GPU labels."""
    return _ENGINE_STATUS if _ENGINE_STATUS else probe_engine_status()


@router.get("/api/system-stats")
async def system_stats():
    """Real-time CPU/GPU/RAM snapshot. No fake values."""
    stats = get_system_stats()
    stats["engine_status"] = _ENGINE_STATUS if _ENGINE_STATUS else probe_engine_status()
    return stats
