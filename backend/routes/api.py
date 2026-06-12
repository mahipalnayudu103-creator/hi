import asyncio
import json
import logging
import math
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
import pandas as pd

try:
    import orjson
    _USE_ORJSON = True
except ImportError:
    _USE_ORJSON = False

try:
    import msgspec
    _USE_MSGSPEC = True
except ImportError:
    _USE_MSGSPEC = False

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import ORJSONResponse, JSONResponse

# Pydantic models from models package
from models.schemas import (
    MetadataRequest, MetadataResponse, RenkoRequest, RenkoResponse,
    RenkoBrick, JobBuildRequest, RenkoWindowRequest, CacheLookupRequest,
)


# Utils & Services imports
from utils.csv_reader import (
    resolve_csv_path, summarize_csv_file, detect_pip_size,
    read_selected_range_cpu, read_selected_range_duckdb,
)
from services.renko_engine import build_renko_cpu
from services.renko_state import build_streaming_engines
from utils.csv_stream import stream_ticks
from services.gpu_engine import (
    detect_gpu_available, detect_cupy_available, detect_gpu_polars_available, detect_cudf_available,
    read_selected_range_gpu, build_renko_gpu_multi,
)
from utils.monitor import get_system_stats, set_high_priority, configure_thread_pools
from services.job_manager import job_manager, SENTINEL
from services.pipeline import run_build_pipeline, _check_full_cache
from services.parquet_cache import read_window as parquet_read_window, read_last_n_bricks
from utils.parquet_meta_cache import read_window_cached as _pmeta_read_window, invalidate_job as _pmeta_invalidate_job
from config import CACHE_DIR, TICK_TIME_FMT, MAX_MARKET_GAP_SECONDS
from services.build_history import lookup_exact, lookup_similar, lookup_sub_range, delete_by_key



logger = logging.getLogger("renko_playback.routes")
RENKO_METHOD_LABEL = "cTrader body v2"
RENKO_METHOD_CACHE_VARIANT = "ctrader_body_v2"

def _boost_playback_start():
    """Boost process priority to ABOVE_NORMAL during active playback."""
    try:
        import psutil
        proc = psutil.Process()
        if os.name == "nt":
            proc.nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS)
        else:
            proc.nice(-5)
        logger.info("Playback booster started: priority set to ABOVE_NORMAL.")
    except Exception as exc:
        logger.warning(f"Could not boost playback priority (non-fatal): {exc}")

def _boost_playback_stop():
    """Restore process priority to NORMAL when playback is paused, ended, or cancelled."""
    try:
        import psutil
        proc = psutil.Process()
        if os.name == "nt":
            proc.nice(psutil.NORMAL_PRIORITY_CLASS)
        else:
            proc.nice(0)
        logger.info("Playback booster stopped: priority restored to NORMAL.")
    except Exception as exc:
        logger.warning(f"Could not restore playback priority (non-fatal): {exc}")

router = APIRouter()

# Fast JSON serialiser — orjson if available, else stdlib json
def _fast_dumps(obj: Any) -> str:
    if _USE_ORJSON:
        return orjson.dumps(obj).decode("utf-8")
    return json.dumps(obj)


# ── Binary MsgPack helpers ────────────────────────────────────────────────────
try:
    import msgspec.msgpack as _msgpack
    _USE_MSGPACK_BINARY = True
except ImportError:
    _USE_MSGPACK_BINARY = False

def _pack(obj: Any) -> bytes:
    """Encode to MsgPack bytes (84% smaller than JSON for numeric payloads)."""
    if _USE_MSGPACK_BINARY:
        return _msgpack.encode(obj)
    # Fallback: send as UTF-8 JSON bytes
    if _USE_ORJSON:
        return orjson.dumps(obj)
    return json.dumps(obj).encode("utf-8")

async def _ws_send(websocket, obj: Any) -> None:
    """Send object as binary MsgPack if available, else JSON text."""
    if _USE_MSGPACK_BINARY:
        await websocket.send_bytes(_pack(obj))
    else:
        await websocket.send_text(_fast_dumps(obj))


# ─── Engine capability probe ──────────────────────────────────────────────────
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
        # Actual engines that will be used
        "data_engine":  data_engine,
        "calc_engine":  calc_engine,
        "chart_engine": "TradingView Lightweight Charts",
        "cache_engine": "PyArrow Parquet" if pyarrow_ok else ("msgspec msgpack" if msgspec_ok else "pickle"),
        "sort_engine":  "GPU CuPy argsort" if cupy_ok else "CPU NumPy argsort",
        "json_engine":  "orjson" if orjson_ok else "stdlib json",
        # Library availability matrix
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

_ENGINE_STATUS: Dict[str, Any] = {}  # populated at startup

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


@router.post("/api/metadata", response_model=MetadataResponse)
def get_metadata(request: MetadataRequest):
    csv_path = resolve_csv_path(request.csv_path)
    if not csv_path.exists() or not csv_path.is_file():
        raise HTTPException(status_code=400, detail=f"CSV file not found: {request.csv_path}")
    
    try:
        summary = summarize_csv_file(csv_path)
        if summary.get("status") == "error":
            raise HTTPException(status_code=400, detail=f"Error parsing CSV: {summary.get('error')}")
        
        # Convert timestamp to ISO 8601 strings with Z indicator
        start_utc = ""
        if summary.get("first_time"):
            start_utc = summary["first_time"].isoformat()
            if not start_utc.endswith("Z") and "+00:00" not in start_utc:
                start_utc += "Z"
            else:
                start_utc = start_utc.replace("+00:00", "Z")
            
        end_utc = ""
        if summary.get("last_time"):
            end_utc = summary["last_time"].isoformat()
            if not end_utc.endswith("Z") and "+00:00" not in end_utc:
                end_utc += "Z"
            else:
                end_utc = end_utc.replace("+00:00", "Z")
            
        return MetadataResponse(
            path=str(summary["path"]),
            rows_estimated=summary["rows"],
            size_bytes=summary["size"],
            file_start_utc=start_utc,
            file_end_utc=end_utc,
            time_col=summary["time_col"],
            bid_col=summary["price_col"],
            ask_col=summary["ask_col"] if summary["ask_col"] else None,
            delimiter=summary["delimiter"],
            detected_pip_size=detect_pip_size(summary["path"])
        )
    except Exception as e:
        logger.exception("Error in get_metadata")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/build-renko", response_model=RenkoResponse)
def build_renko(request: RenkoRequest):
    diagnostics = []
    diagnostics.append(f"Request: CSV={request.csv_path}, start={request.start_utc}, end={request.end_utc}")
    diagnostics.append(f"Params: engine={request.processing_engine}, anchor={request.anchor}, pip_size={request.pip_size}, reversal={request.reversal_boxes}")
    
    try:
        csv_path = resolve_csv_path(request.csv_path)
        diagnostics.append(f"Resolved CSV path to: {csv_path}")
        if not csv_path.exists() or not csv_path.is_file():
            err_msg = f"CSV file not found: {request.csv_path}"
            diagnostics.append(f"ERROR: {err_msg}")
            return RenkoResponse(
                status="error",
                engine_used="",
                rows_scanned=0,
                ticks_loaded=0,
                charts={},
                diagnostics=diagnostics
            )
        
        # 1. Parse start and end timestamps
        try:
            start_t = pd.Timestamp(request.start_utc)
            end_t = pd.Timestamp(request.end_utc)
            diagnostics.append(f"Parsed timestamps: start={start_t}, end={end_t}")
        except Exception as te:
            logger.warning(f"Timestamp parsing failed for request (start_utc={request.start_utc}, end_utc={request.end_utc}): {te}")
            err_msg = f"Timestamp parsing failed: {te}"
            diagnostics.append(f"ERROR: {err_msg}")
            return RenkoResponse(
                status="error",
                engine_used="",
                rows_scanned=0,
                ticks_loaded=0,
                charts={},
                diagnostics=diagnostics
            )
            
        # Check cache
        try:
            from utils.cache import get_cache_key, check_cache, save_cache
            cache_key = get_cache_key(
                csv_path=csv_path,
                start_utc=request.start_utc,
                end_utc=request.end_utc,
                price_source=request.price_source,
                reversal_boxes=request.reversal_boxes,
                pip_size=request.pip_size,
                anchor=request.anchor,
                chart_pips=request.chart_pips,
                cache_variant=RENKO_METHOD_CACHE_VARIANT,
            )
            cached_result = check_cache(cache_key)
            if cached_result is not None:
                diagnostics.append("CACHE HIT: Loaded pre-calculated Renko bricks from disk cache.")
                return RenkoResponse(
                    status="ok",
                    engine_used=f"Disk Cache + {RENKO_METHOD_LABEL}",
                    rows_scanned=cached_result["rows_scanned"],
                    ticks_loaded=cached_result["ticks_loaded"],
                    charts=cached_result["charts"],
                    diagnostics=diagnostics
                )
            diagnostics.append("Cache miss. Proceeding with loading and calculations...")
        except Exception as ce:
            logger.warning(f"Cache check failed: {ce}")
            diagnostics.append(f"Cache check failed: {ce}. Proceeding...")
            cache_key = None
            
        # 2. Get columns info via brief CSV summary
        diagnostics.append("Analyzing CSV structure...")
        summary = summarize_csv_file(csv_path)
        if summary.get("status") == "error":
            err_msg = f"Error parsing CSV structure: {summary.get('error')}"
            diagnostics.append(f"ERROR: {err_msg}")
            return RenkoResponse(
                status="error",
                engine_used="",
                rows_scanned=0,
                ticks_loaded=0,
                charts={},
                diagnostics=diagnostics
            )
            
        time_col = summary["time_col"]
        bid_col = summary["price_col"]
        ask_col = summary["ask_col"]
        delimiter = summary["delimiter"]
        diagnostics.append(f"Detected columns: time='{time_col}', bid='{bid_col}', ask='{ask_col or 'None'}', delimiter='{delimiter}'")
        
        # Determine source column
        if request.price_source.lower() == "bid":
            source = bid_col
        elif request.price_source.lower() == "ask":
            source = ask_col if ask_col else bid_col
        elif request.price_source.lower() == "mid":
            if bid_col and ask_col:
                source = "__mid__"
            else:
                source = bid_col
        else:
            source = bid_col
        diagnostics.append(f"Price source: {request.price_source.upper()} maps to column: {source}")
            
        # Unlimited row support for full synchronous builds
        max_rows = None
        
        # Check processing engine requested vs available
        gpu_requested = request.processing_engine.lower() != "cpu"
        if summary.get("is_wrapping"):
            gpu_requested = False
            diagnostics.append("Wrapping timestamps (MM:SS) detected. GPU loaders bypassed; falling back to sequential CPU loading.")
        else:
            diagnostics.append(f"GPU requested: {gpu_requested}. Checking availability...")
        
        prices = None
        times = None
        rows_scanned = 0
        rows_loaded = 0
        loader_engine = ""
        
        # ── Tier 1: GPU loaders ─────────────────────────────────────────────
        loaded_ok = False
        if gpu_requested:
            if detect_gpu_polars_available():
                try:
                    diagnostics.append("[Tier 1-A] GPU Polars loader...")
                    from services.gpu_engine import read_selected_range_gpu_polars
                    prices, times, rows_scanned, rows_loaded, loader_engine = read_selected_range_gpu_polars(
                        path=csv_path, delimiter=delimiter, time_col=time_col,
                        source=source, bid_col=bid_col, ask_col=ask_col,
                        start_t=start_t, end_t=end_t, max_rows=max_rows,
                    )
                    loaded_ok = True
                    diagnostics.append(f"[OK] GPU Polars: {rows_loaded:,} rows")
                except Exception as e:
                    diagnostics.append(f"[SKIP] GPU Polars: {e}")

            if not loaded_ok and detect_cudf_available():
                try:
                    diagnostics.append("[Tier 1-B] GPU cuDF loader...")
                    prices, times, rows_scanned, rows_loaded, loader_engine = read_selected_range_gpu(
                        path=csv_path, delimiter=delimiter, time_col=time_col,
                        source=source, bid_col=bid_col, ask_col=ask_col,
                        start_t=start_t, end_t=end_t, max_rows=max_rows,
                    )
                    loaded_ok = True
                    diagnostics.append(f"[OK] GPU cuDF: {rows_loaded:,} rows")
                except Exception as e:
                    diagnostics.append(f"[SKIP] GPU cuDF: {e}")

        # ── Tier 2: CPU Polars ───────────────────────────────────────────────
        if not loaded_ok:
            try:
                diagnostics.append("[Tier 2] CPU Polars loader...")
                prices, times, rows_scanned, rows_loaded, loader_engine, _, _ = read_selected_range_cpu(
                    path=csv_path, delimiter=delimiter, time_col=time_col,
                    source=source, bid_col=bid_col, ask_col=ask_col,
                    start_t=start_t, end_t=end_t, max_rows=max_rows,
                )
                loaded_ok = True
                diagnostics.append(f"[OK] CPU Polars: {rows_loaded:,} rows")
            except Exception as e:
                diagnostics.append(f"[SKIP] CPU Polars: {e}")

        # ── Tier 3: DuckDB SQL fallback ──────────────────────────────────────
        if not loaded_ok:
            try:
                diagnostics.append("[Tier 3] CPU DuckDB SQL fallback...")
                prices, times, rows_scanned, rows_loaded, loader_engine = read_selected_range_duckdb(
                    path=csv_path, delimiter=delimiter, time_col=time_col,
                    source=source, bid_col=bid_col, ask_col=ask_col,
                    start_t=start_t, end_t=end_t, max_rows=max_rows,
                )
                loaded_ok = True
                diagnostics.append(f"[OK] DuckDB: {rows_loaded:,} rows")
            except Exception as e:
                diagnostics.append(f"[SKIP] DuckDB: {e}")

        # ── Tier 4: pandas last resort ───────────────────────────────────────
        if not loaded_ok:
            try:
                diagnostics.append("[Tier 4] CPU pandas last-resort loader...")
                prices, times, rows_scanned, rows_loaded, loader_engine, _, _ = read_selected_range_cpu(
                    path=csv_path, delimiter=delimiter, time_col=time_col,
                    source=source, bid_col=bid_col, ask_col=ask_col,
                    start_t=start_t, end_t=end_t, max_rows=max_rows,
                )
                diagnostics.append(f"[OK] CPU pandas: {rows_loaded:,} rows")
            except Exception as e:
                err_msg = f"All CSV loaders failed. Last error: {e}"
                diagnostics.append(f"ERROR: {err_msg}")
                import traceback
                diagnostics.extend(traceback.format_exc().splitlines())
                return RenkoResponse(
                    status="error", engine_used="",
                    rows_scanned=0, ticks_loaded=0, charts={}, diagnostics=diagnostics
                )
            
        if prices is None or len(prices) == 0:
            diagnostics.append("WARNING: No price data loaded. Double check date range and CSV columns.")
            return RenkoResponse(
                status="ok",
                engine_used=loader_engine,
                rows_scanned=rows_scanned,
                ticks_loaded=0,
                charts={},
                diagnostics=diagnostics
            )
            
        charts = {}
        engine_used = loader_engine
        
        # Decoupled Renko Calculation (GPU via CuPy vs CPU via ThreadPoolExecutor + Numba)
        gpu_calc_run = False
        if gpu_requested and detect_cupy_available():
            try:
                diagnostics.append("Running Renko calculations on GPU via CuPy...")
                results = build_renko_gpu_multi(
                    prices=prices,
                    times=times,
                    pips=request.chart_pips,
                    reversal_boxes=request.reversal_boxes,
                    pip_size=request.pip_size,
                    anchor_mode=request.anchor,
                )
                for idx, pip in enumerate(request.chart_pips):
                    res = results[idx]
                    indices, opens, closes, tops, bottoms, highs, lows, colors, borders, brick_times, ticks, dir_strs, times_idx, _ = res
                    bricks = []
                    for j in range(len(indices)):
                        bricks.append(RenkoBrick(
                            time=int(indices[j] + 1),
                            x=int(times_idx[j]),
                            confirm_tick_index=int(times_idx[j]),
                            brick_index=int(indices[j] + 1),
                            confirm_time=str(brick_times[j]),
                            open=float(opens[j]),
                            high=float(highs[j]),
                            low=float(lows[j]),
                            close=float(closes[j]),
                            direction=str(dir_strs[j]),
                            tick_count=int(ticks[j])
                        ))
                    charts[str(pip)] = bricks
                    diagnostics.append(f"Success: GPU generated {len(bricks)} bricks for {pip} pip chart.")
                engine_used = f"GPU CuPy / {loader_engine} + {RENKO_METHOD_LABEL}"
                gpu_calc_run = True
            except Exception as e:
                diagnostics.append(f"WARNING: GPU Renko calculation failed: {e}. Falling back to CPU...")
                logger.warning(f"GPU Renko calculation failed, falling back to CPU: {e}")
                
        if not gpu_calc_run:
            try:
                diagnostics.append("Running Renko calculations on CPU via parallel Numba...")
                from concurrent.futures import ThreadPoolExecutor
                
                def process_pip(pip):
                    (
                        indices, opens, closes, tops, bottoms, highs, lows,
                        colors, borders, brick_times, ticks, dir_strs, times_idx
                    ) = build_renko_cpu(
                        prices=prices,
                        times=times,
                        brick_pips=pip,
                        reversal_boxes=request.reversal_boxes,
                        pip_size=request.pip_size,
                        anchor_mode=request.anchor,
                    )
                    bricks = []
                    for j in range(len(indices)):
                        bricks.append(RenkoBrick(
                            time=int(indices[j] + 1),
                            x=int(times_idx[j]),
                            confirm_tick_index=int(times_idx[j]),
                            brick_index=int(indices[j] + 1),
                            confirm_time=str(brick_times[j]),
                            open=float(opens[j]),
                            high=float(highs[j]),
                            low=float(lows[j]),
                            close=float(closes[j]),
                            direction=str(dir_strs[j]),
                            tick_count=int(ticks[j])
                        ))
                    return str(pip), bricks

                with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
                    res_list = list(executor.map(process_pip, request.chart_pips))
                    for pip_str, bricks in res_list:
                        charts[pip_str] = bricks
                        diagnostics.append(f"Success: CPU generated {len(bricks)} bricks for {pip_str} pip chart.")
                        
                engine_used = f"CPU Parallel Numba / {loader_engine} + {RENKO_METHOD_LABEL}"
            except Exception as e:
                err_msg = f"CPU Renko calculation failed: {e}"
                diagnostics.append(f"ERROR: {err_msg}")
                import traceback
                diagnostics.extend(traceback.format_exc().splitlines())
                return RenkoResponse(
                    status="error",
                    engine_used="",
                    rows_scanned=rows_scanned,
                    ticks_loaded=rows_loaded,
                    charts={},
                    diagnostics=diagnostics
                )
            
        # Save to cache if built successfully
        if cache_key is not None:
            try:
                cache_data = {
                    "rows_scanned": rows_scanned,
                    "ticks_loaded": rows_loaded,
                    "charts": charts
                }
                save_cache(cache_key, cache_data)
                diagnostics.append("Saved calculated Renko bricks to disk cache.")
            except Exception as ce:
                diagnostics.append(f"Failed to save to cache: {ce}")
        diagnostics.append("All charts computed successfully.")
        return RenkoResponse(
            status="ok",
            engine_used=loader_engine,
            rows_scanned=rows_scanned,
            ticks_loaded=rows_loaded,
            charts=charts,
            diagnostics=diagnostics
        )
    except Exception as e:
        logger.exception("Error in build_renko")
        diagnostics.append(f"CRITICAL ERROR: {e}")
        import traceback
        diagnostics.extend(traceback.format_exc().splitlines())
        return RenkoResponse(
            status="error",
            engine_used="",
            rows_scanned=0,
            ticks_loaded=0,
            charts={},
            diagnostics=diagnostics
        )


def count_ticks_in_range(csv_path, delimiter, time_col, start_t, end_t) -> int:
    try:
        from utils.csv_reader import read_header_columns, seek_first_timestamp_offset, open_compressed_file
        columns = read_header_columns(csv_path, delimiter)
        if time_col not in columns:
            return 100_000
        time_index = columns.index(time_col)
        
        # Find data start offset
        with open_compressed_file(csv_path, "rb") as fh:
            fh.readline()
            data_start = fh.tell()
            first_line = fh.readline()
            
        if not first_line:
            return 0
            
        # Bisect to find start and end offsets (very fast, <15ms)
        start_offset = seek_first_timestamp_offset(csv_path, start_t, data_start, time_index, delimiter)
        end_offset = seek_first_timestamp_offset(csv_path, end_t, data_start, time_index, delimiter)
        
        # Count exact newlines in range
        exact_ticks = 0

        with open_compressed_file(csv_path, "rb") as fh:
            fh.seek(start_offset)
            bytes_to_read = max(0, end_offset - start_offset)
            chunk_size = 1024 * 1024
            read_so_far = 0
            last_byte = b""
            while read_so_far < bytes_to_read:
                to_read = min(chunk_size, bytes_to_read - read_so_far)
                chunk = fh.read(to_read)
                if not chunk:
                    break
                exact_ticks += chunk.count(b"\n")
                if chunk:
                    last_byte = chunk[-1:]
                read_so_far += len(chunk)
            if last_byte and last_byte != b"\n":
                exact_ticks += 1
        return max(1, exact_ticks)
    except Exception as e:
        logger.exception("Error counting exact ticks in range")
        return 100_000  # safe fallback estimate


@router.websocket("/ws/playback")
async def ws_playback(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket playback client connected")
    
    playback_task = None
    diagnostics = []
    diagnostics.append("WebSocket connection accepted.")

    state = {
        "is_playing": False,
        "speed": 100.0,
        "speed_mode": "tick",   # "tick" = ticks/sec, "time" = market-time multiplier (0–100x)
        "virtual_dt": None,     # virtual market clock for time-based playback
        "chart_pips": [],
        "reversal_boxes": 2,
        "pip_size": 0.0001,
        "anchor": "floor",
        "renko_engines": [],
        "generator": None,
        "total_ticks": 0,
        "global_tick_index": 0,
        "chunk_prices": None,
        "chunk_times": None,
        "chunk_bids": None,
        "chunk_asks": None,
        "chunk_length": 0,
        "chunk_index": 0,
        
        # CSV configuration stored for resetting/seeking
        "csv_path": None,
        "delimiter": ",",
        "time_col": "",
        "source": "",
        "bid_col": None,
        "ask_col": None,
        "start_t": None,
        "end_t": None,
    }

    async def load_next_chunk_async() -> bool:
        if state["generator"] is None:
            return False
        
        def _get_next():
            try:
                return next(state["generator"])
            except StopIteration:
                return None
                
        chunk = await asyncio.to_thread(_get_next)
        if chunk is None:
            state["generator"] = None
            state["chunk_prices"] = None
            state["chunk_times"] = None
            state["chunk_bids"] = None
            state["chunk_asks"] = None
            state["chunk_length"] = 0
            state["chunk_index"] = 0
            return False
            
        prices, times, bids, asks, nrows = chunk
        if nrows == 0:
            del prices, times, bids, asks, chunk
            import gc
            gc.collect()
            return await load_next_chunk_async()
        
        state["chunk_prices"] = prices
        state["chunk_times"] = times
        state["chunk_bids"] = bids
        state["chunk_asks"] = asks
        state["chunk_length"] = nrows
        state["chunk_index"] = 0
        return True

    async def get_next_tick_batch_async(count: int) -> Optional[Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, int]]:
        if state["chunk_prices"] is None or state["chunk_index"] >= state["chunk_length"]:
            state["chunk_prices"] = None
            state["chunk_times"] = None
            state["chunk_bids"] = None
            state["chunk_asks"] = None
            import gc
            gc.collect()
            
            has_more = await load_next_chunk_async()
            if not has_more:
                return None
                
        idx = state["chunk_index"]
        avail = state["chunk_length"] - idx
        actual = min(count, avail)
        
        prices = state["chunk_prices"][idx : idx + actual]
        times = state["chunk_times"][idx : idx + actual]
        bids = state["chunk_bids"][idx : idx + actual] if state["chunk_bids"] is not None else prices
        asks = state["chunk_asks"][idx : idx + actual] if state["chunk_asks"] is not None else prices
        
        state["chunk_index"] += actual
        state["global_tick_index"] += actual
        return prices, times, bids, asks, actual

    TICK_TIME_FMT = "%Y-%m-%d %H:%M:%S.%f"
    MAX_MARKET_GAP_SECONDS = 10.0  # quiet periods longer than this are skipped in time mode

    def _parse_tick_ts(s: str) -> datetime:
        try:
            return datetime.strptime(s, TICK_TIME_FMT)
        except ValueError:
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")

    async def peek_next_tick_time() -> Optional[str]:
        """Return the next unconsumed tick's timestamp string without consuming it."""
        if state["chunk_prices"] is None or state["chunk_index"] >= state["chunk_length"]:
            has_more = await load_next_chunk_async()
            if not has_more:
                return None
        return str(state["chunk_times"][state["chunk_index"]])

    async def get_next_tick_async() -> Optional[Tuple[float, str, float, float]]:
        batch = await get_next_tick_batch_async(1)
        if batch is None:
            return None
        prices, times, bids, asks, _ = batch
        return float(prices[0]), str(times[0]), float(bids[0]), float(asks[0])

    async def skip_to_target_idx(target_idx: int):
        target_idx = max(0, min(target_idx, state["total_ticks"]))
        state["virtual_dt"] = None  # re-anchor virtual clock after any seek
        
        # Check if we can fast-forward or if we must rewind
        if target_idx < state["global_tick_index"]:
            # Rewind: reset engines and generator
            for engine in state["renko_engines"]:
                engine.reset()
            state["global_tick_index"] = 0
            state["chunk_prices"] = None
            state["chunk_times"] = None
            state["chunk_bids"] = None
            state["chunk_asks"] = None
            state["chunk_length"] = 0
            state["chunk_index"] = 0
            
            state["generator"] = stream_ticks(
                csv_path   = state["csv_path"],
                delimiter  = state["delimiter"],
                time_col   = state["time_col"],
                source     = state["source"],
                bid_col    = state["bid_col"],
                ask_col    = state["ask_col"],
                start_t    = state["start_t"],
                end_t      = state["end_t"],
            )
        
        await _ws_send(websocket,{"type": "status", "status": "reset"})
        
        batch_by_chart = {str(idx+1): [] for idx in range(len(state["renko_engines"]))}
        
        last_price = None
        last_time = None
        last_bid = None
        last_ask = None
        
        last_tick_seq = [0] * len(state["renko_engines"])
        
        # Fast forward loop using batches to eliminate async call overhead
        while state["global_tick_index"] < target_idx:
            needed = target_idx - state["global_tick_index"]
            batch = await get_next_tick_batch_async(needed)
            if batch is None:
                break
                
            prices, times, bids, asks, batch_size = batch
            global_start = state["global_tick_index"] - batch_size
            
            for j in range(batch_size):
                price = float(prices[j])
                tick_time = str(times[j])
                bid = float(bids[j])
                ask = float(asks[j])
                curr_global_idx = global_start + j
                
                last_price = price
                last_time = tick_time
                last_bid = bid
                last_ask = ask
                
                for idx, engine in enumerate(state["renko_engines"]):
                    formed = engine.process_tick(price, tick_time, curr_global_idx, bid, ask)
                    if formed:
                        for brick in formed:
                            brick["confirm_tick_index"] = curr_global_idx
                            brick["brick_index"] = brick["time"]
                            brick["time"] = brick["brick_index"]
                            brick["confirm_time"] = tick_time
                        batch_by_chart[str(idx+1)].extend(formed)
                        last_tick_seq[idx] = len(formed)
                    else:
                        last_tick_seq[idx] = 0
                        
        # Get live state
        live_bricks_by_chart = {}
        if last_price is not None:
            for idx, engine in enumerate(state["renko_engines"]):
                chart_idx = idx + 1
                live_brick = engine.get_live_brick(last_price, last_bid, last_ask, state["global_tick_index"] - 1, last_tick_seq[idx])
                if live_brick:
                    live_brick["confirm_tick_index"] = state["global_tick_index"] - 1
                    live_brick["brick_index"] = live_brick["time"]
                    live_brick["time"] = live_brick["brick_index"]
                    live_brick["confirm_time"] = last_time
                    live_bricks_by_chart[str(chart_idx)] = live_brick
        else:
            last_bid = last_ask = 0.0
            last_time = ""
                    
        total_formed = sum(eng.total_bricks_confirmed for eng in state["renko_engines"])
        
        await _ws_send(websocket,{
            "type": "playback_frame",
            "bricks_by_chart": batch_by_chart,
            "live_bricks_by_chart": live_bricks_by_chart,
            "processed_ticks": state["global_tick_index"],
            "total_ticks": state["total_ticks"],
            "formed_bricks": total_formed,
            "speed": state["speed"],
            "latest_bid": last_bid,
            "latest_ask": last_ask,
            "latest_time": last_time
        })

    async def playback_loop():
        """
        20 FPS tick-based playback loop on WebSocket.
        Uses elapsed-time accumulator to simulate tick-by-tick movement.
        """
        loop_diags = []
        loop_diags.append(
            f"Playback loop started. Speed: {state['speed']} ticks/sec. "
            f"Total ticks: {state['total_ticks']}"
        )
        ticks_per_second = state["speed"]
        tick_accumulator = 0.0
        last_frame_time = asyncio.get_event_loop().time()
        FRAME_INTERVAL = 1.0 / 20  # 50 ms – fixed 20 FPS
        try:
            while True:
                if not state["is_playing"]:
                    await asyncio.sleep(0.1)
                    last_frame_time = asyncio.get_event_loop().time()
                    continue

                now = asyncio.get_event_loop().time()
                delta_seconds = now - last_frame_time
                last_frame_time = now

                ticks_per_second = state["speed"]

                if state["speed_mode"] == "time":
                    multiplier = state["speed"]
                    if multiplier <= 0:
                        await asyncio.sleep(FRAME_INTERVAL)
                        continue

                    next_ts_str = await peek_next_tick_time()
                    if next_ts_str is None:
                        ticks_to_process = 1
                    else:
                        next_dt = _parse_tick_ts(next_ts_str)
                        if state["virtual_dt"] is None:
                            state["virtual_dt"] = next_dt
                        state["virtual_dt"] += timedelta(seconds=delta_seconds * multiplier)

                        if state["virtual_dt"] < next_dt:
                            gap = (next_dt - state["virtual_dt"]).total_seconds()
                            if gap > MAX_MARKET_GAP_SECONDS:
                                state["virtual_dt"] = next_dt  # skip quiet period
                            else:
                                await asyncio.sleep(FRAME_INTERVAL)
                                continue

                        target_str = state["virtual_dt"].strftime(TICK_TIME_FMT)[:23]
                        idx = state["chunk_index"]
                        ticks_to_process = int(np.searchsorted(
                            state["chunk_times"][idx:state["chunk_length"]],
                            target_str, side="right"
                        ))
                else:
                    tick_accumulator += delta_seconds * ticks_per_second
                    ticks_to_process = int(math.floor(tick_accumulator))
                    tick_accumulator -= ticks_to_process

                if ticks_to_process > 0:
                    batch_by_chart = {}
                    for chart_idx_1based in range(1, len(state["renko_engines"]) + 1):
                        batch_by_chart[str(chart_idx_1based)] = []
                        
                    last_price = None
                    last_time = None
                    last_bid = None
                    last_ask = None
                    
                    actual_processed = 0
                    ticks_left = ticks_to_process
                    last_tick_seq = [0] * len(state["renko_engines"])
                    
                    tick_prices = []
                    
                    while ticks_left > 0:
                        batch = await get_next_tick_batch_async(ticks_left)
                        if batch is None:
                            break
                            
                        prices, times, bids, asks, batch_size = batch
                        global_start = state["global_tick_index"] - batch_size
                        
                        for j in range(batch_size):
                            price = float(prices[j])
                            tick_prices.append(price)
                            
                            tick_time = str(times[j])
                            bid = float(bids[j])
                            ask = float(asks[j])
                            curr_global_idx = global_start + j
                            
                            last_price = price
                            last_time = tick_time
                            last_bid = bid
                            last_ask = ask
                            
                            for idx, engine in enumerate(state["renko_engines"]):
                                chart_idx = idx + 1
                                formed = engine.process_tick(price, tick_time, curr_global_idx, bid, ask)
                                if formed:
                                    for brick in formed:
                                        brick["confirm_tick_index"] = curr_global_idx
                                        brick["brick_index"] = brick["time"]
                                        brick["time"] = brick["brick_index"]
                                        brick["confirm_time"] = tick_time
                                    batch_by_chart[str(chart_idx)].extend(formed)
                                    last_tick_seq[idx] = len(formed)
                                else:
                                    last_tick_seq[idx] = 0
                                    
                        actual_processed += batch_size
                        ticks_left -= batch_size

                    # Check if generator is fully exhausted
                    if actual_processed == 0 and state["generator"] is None and (state["chunk_prices"] is None or state["chunk_index"] >= state["chunk_length"]):
                        state["is_playing"] = False
                        loop_diags.append(f"Playback finished. Processed {state['global_tick_index']} ticks.")
                        asyncio.get_event_loop().run_in_executor(None, _boost_playback_stop)
                        await _ws_send(websocket,{
                            "type": "status",
                            "status": "ended",
                            "total_ticks": state["total_ticks"],
                            "diagnostics": diagnostics + loop_diags
                        })
                        break

                    if actual_processed > 0:
                        # Get live/forming brick state
                        live_bricks_by_chart = {}
                        for idx, engine in enumerate(state["renko_engines"]):
                            chart_idx = idx + 1
                            live_brick = engine.get_live_brick(last_price, last_bid, last_ask, state["global_tick_index"] - 1, last_tick_seq[idx])
                            if live_brick:
                                live_brick["confirm_tick_index"] = state["global_tick_index"] - 1
                                live_brick["brick_index"] = live_brick["time"]
                                live_brick["time"] = live_brick["brick_index"]
                                live_brick["confirm_time"] = last_time
                                live_bricks_by_chart[str(chart_idx)] = live_brick

                        total_formed = sum(eng.total_bricks_confirmed for eng in state["renko_engines"])

                        await _ws_send(websocket,{
                            "type": "playback_frame",
                            "bricks_by_chart": batch_by_chart,
                            "live_bricks_by_chart": live_bricks_by_chart,
                            "tick_prices": tick_prices,
                            "processed_ticks": state["global_tick_index"],
                            "total_ticks": state["total_ticks"],
                            "formed_bricks": total_formed,
                            "speed": ticks_per_second,
                            "latest_bid": last_bid,
                            "latest_ask": last_ask,
                            "latest_time": last_time
                        })

                await asyncio.sleep(FRAME_INTERVAL)

        except asyncio.CancelledError:
            loop_diags.append("Playback loop cancelled by system/client request.")
            logger.info("Playback loop cancelled")
        except Exception as exc:
            logger.exception("Playback loop exception")
            try:
                await _ws_send(websocket, {
                    "type": "error",
                    "message": str(exc),
                    "error_class": exc.__class__.__name__
                })
            except Exception:
                pass

    try:
        while True:
            data = await websocket.receive_text()
            cmd = json.loads(data)
            action = cmd.get("action")
            
            if action == "start":
                if playback_task:
                    diagnostics.append("Stopping existing playback task...")
                    playback_task.cancel()
                    playback_task = None
                    
                diagnostics = []
                diagnostics.append(f"Start request received for CSV: {cmd.get('csv_path')}")
                diagnostics.append(f"Range: {cmd.get('start_utc')} .. {cmd.get('end_utc')}")
                
                await _ws_send(websocket,{
                    "type": "status",
                    "status": "loading",
                    "message": "Initializing playback...",
                    "diagnostics": diagnostics
                })
                
                try:
                    csv_path = resolve_csv_path(cmd["csv_path"])
                    diagnostics.append(f"Resolved path to: {csv_path}")
                    if not csv_path.exists() or not csv_path.is_file():
                        raise FileNotFoundError(f"CSV file not found: {cmd['csv_path']}")
                        
                    start_t = pd.Timestamp(cmd["start_utc"])
                    end_t = pd.Timestamp(cmd["end_utc"])
                    price_source = cmd.get("price_source", "Bid")
                    reversal_boxes = int(cmd.get("reversal_boxes", 2))
                    pip_size = float(cmd.get("pip_size", 0.0001))
                    anchor = cmd.get("anchor", "floor")
                    chart_pips = list(cmd.get("chart_pips", [1.0, 2.0, 3.0, 4.0]))
                    state["speed"] = float(cmd.get("speed", 100.0))
                    state["speed_mode"] = cmd.get("speed_mode", "tick")
                    state["virtual_dt"] = None
                    
                    logger.info(
                        f"\\n[Playback Request]\\n"
                        f"CSV: {csv_path}\\n"
                        f"Requested start_utc: {cmd.get('start_utc')}\\n"
                        f"Requested end_utc: {cmd.get('end_utc')}\\n"
                    )
                    
                    diagnostics.append("Summarizing CSV structure...")
                    summary = await asyncio.to_thread(summarize_csv_file, csv_path)
                    if summary.get("status") == "error":
                        raise ValueError(f"Error parsing CSV structure: {summary.get('error')}")
                        
                    time_col = summary["time_col"]
                    bid_col = summary["price_col"]
                    ask_col = summary["ask_col"]
                    delimiter = summary["delimiter"]
                    
                    diagnostics.append(f"Detected columns: time='{time_col}', bid='{bid_col}', ask='{ask_col or 'None'}', delimiter='{delimiter}'")
                    
                    # Determine price source
                    if price_source.lower() == "bid":
                        source = bid_col
                    elif price_source.lower() == "ask":
                        source = ask_col if ask_col else bid_col
                    elif price_source.lower() == "mid":
                        if bid_col and ask_col:
                            source = "__mid__"
                        else:
                            source = bid_col
                    else:
                        source = bid_col
                    diagnostics.append(f"Source column: {source}")
                    
                    state["csv_path"] = csv_path
                    state["delimiter"] = delimiter
                    state["time_col"] = time_col
                    state["source"] = source
                    state["bid_col"] = bid_col
                    state["ask_col"] = ask_col
                    state["start_t"] = start_t
                    state["end_t"] = end_t
                    state["chart_pips"] = chart_pips
                    state["reversal_boxes"] = reversal_boxes
                    state["pip_size"] = pip_size
                    state["anchor"] = anchor
                    
                    # Async count matching ticks
                    diagnostics.append("Counting ticks in selected range...")
                    total_ticks = await asyncio.to_thread(
                        count_ticks_in_range,
                        csv_path, delimiter, time_col, start_t, end_t
                    )
                    state["total_ticks"] = total_ticks
                    diagnostics.append(f"Ticks in range: {total_ticks}")
                    
                    # Reset generator & indexes
                    state["global_tick_index"] = 0
                    state["chunk_prices"] = None
                    state["chunk_times"] = None
                    state["chunk_bids"] = None
                    state["chunk_asks"] = None
                    state["chunk_length"] = 0
                    state["chunk_index"] = 0
                    
                    state["generator"] = stream_ticks(
                        csv_path   = csv_path,
                        delimiter  = delimiter,
                        time_col   = time_col,
                        source     = source,
                        bid_col    = bid_col,
                        ask_col    = ask_col,
                        start_t    = start_t,
                        end_t      = end_t,
                    )
                    
                    # Initialize streaming Renko engines
                    engines_dict = build_streaming_engines(
                        chart_pips=chart_pips,
                        pip_size=pip_size,
                        reversal_boxes=reversal_boxes,
                        anchor=anchor,
                    )
                    state["renko_engines"] = list(engines_dict.values())
                    state["renko_engine_pips"] = chart_pips
                    
                    # Load first tick to verify data and get start time
                    first_tick = await get_next_tick_async()
                    if first_tick is None:
                        raise ValueError("No price ticks were found in the selected range.")
                    
                    state["chunk_index"] = 0
                    state["global_tick_index"] = 0
                    
                    first_loaded_time = first_tick[1]
                    state["is_playing"] = True
                    
                    diagnostics.append(
                        f"Ready. Ticks: {state['total_ticks']} | "
                        f"Engines: {len(state['renko_engines'])}"
                    )
                    await _ws_send(websocket,{
                        "type": "status",
                        "status": "ready",
                        "total_bricks": 0,
                        "ticks_loaded": state["total_ticks"],
                        "first_tick": first_loaded_time,
                        "last_tick": cmd["end_utc"],
                        "diagnostics": diagnostics
                    })
                    
                    asyncio.get_event_loop().run_in_executor(None, _boost_playback_start)
                    playback_task = asyncio.create_task(playback_loop())
                    
                except Exception as build_err:
                    logger.exception("Playback initialization failed")
                    err_msg = f"Setup failed: {build_err}"
                    diagnostics.append(f"ERROR: {err_msg}")
                    import traceback
                    diagnostics.extend(traceback.format_exc().splitlines())
                    await _ws_send(websocket,{
                        "type": "error",
                        "message": err_msg,
                        "diagnostics": diagnostics
                    })
                    
            elif action == "pause":
                state["is_playing"] = False
                asyncio.get_event_loop().run_in_executor(None, _boost_playback_stop)
                await _ws_send(websocket,{"type": "status", "status": "paused"})
                
            elif action == "resume":
                state["is_playing"] = True
                asyncio.get_event_loop().run_in_executor(None, _boost_playback_start)
                await _ws_send(websocket,{"type": "status", "status": "playing"})
                
            elif action == "step":
                if not state["renko_engines"]:
                    continue
                state["is_playing"] = False
                target_idx = state["global_tick_index"] + 1
                await skip_to_target_idx(target_idx)
                
            elif action == "speed":
                new_speed = float(cmd.get("speed", 100.0))
                state["speed"] = new_speed
                if "mode" in cmd:
                    state["speed_mode"] = cmd["mode"]
                await _ws_send(websocket,{
                    "type": "status",
                    "status": "speed_updated",
                    "speed": new_speed
                })
                
            elif action == "skip_to":
                if not state["renko_engines"]:
                    continue
                state["is_playing"] = False
                target_idx = int(cmd.get("index", 0))
                await skip_to_target_idx(target_idx)
                
            elif action == "step_multi":
                if not state["renko_engines"]:
                    continue
                state["is_playing"] = False
                count = int(cmd.get("count", 1))
                direction = cmd.get("direction", "forward")
                if direction == "forward":
                    target_idx = state["global_tick_index"] + count
                else:
                    target_idx = state["global_tick_index"] - count
                await skip_to_target_idx(target_idx)

    except WebSocketDisconnect:
        logger.info("WebSocket playback client disconnected")
    finally:
        asyncio.get_event_loop().run_in_executor(None, _boost_playback_stop)
        if playback_task:
            playback_task.cancel()


@router.post("/api/cache/lookup")
def cache_lookup(request: CacheLookupRequest):
    """
    Check if a matching completed build exists in the SQLite database.
    Verifies that the CSV metadata matches exactly (size, mtime) and 
    that the corresponding Parquet cache files exist on disk.
    """
    csv_path = resolve_csv_path(request.csv_path)
    if not csv_path.exists() or not csv_path.is_file():
        return {
            "exact_match": False,
            "sub_range_match": False,
            "cache_key": None,
            "job_id": None,
            "metadata": None,
            "similar_matches": [],
            "error": "CSV file not found"
        }

    try:
        stat = csv_path.stat()
        csv_size = stat.st_size
        csv_mtime = stat.st_mtime
    except Exception as e:
        return {
            "exact_match": False,
            "sub_range_match": False,
            "cache_key": None,
            "job_id": None,
            "metadata": None,
            "similar_matches": [],
            "error": f"Failed to read CSV attributes: {e}"
        }

    # 1. Exact or sub-range match lookup
    match_record = lookup_sub_range(
        csv_path=csv_path,
        price_source=request.price_source,
        reversal_boxes=request.reversal_boxes,
        pip_size=request.pip_size,
        anchor=request.anchor,
        start_utc=request.start_utc,
        end_utc=request.end_utc,
        chart_pips=request.chart_pips,
    )

    exact_match_found = False
    sub_range_match_found = False
    valid_record = None

    if match_record:
        if RENKO_METHOD_LABEL not in str(match_record.get("engine_used", "")):
            match_record = None

    if match_record:
        job_id = match_record.get("job_id")
        cache_key = match_record.get("cache_key")
        # Verify cache files on disk
        files_ok = True
        if job_id:
            meta_file = CACHE_DIR / job_id / "job_meta.json"
            if not meta_file.exists():
                files_ok = False
            else:
                for pip in request.chart_pips:
                    pip_safe = str(pip).replace(".", "_")
                    p_file = CACHE_DIR / job_id / f"renko_{pip_safe}pip.parquet"
                    if not p_file.exists():
                        files_ok = False
                        break
        else:
            files_ok = False

        if files_ok:
            valid_record = match_record
            if match_record["start_utc"] == request.start_utc and match_record["end_utc"] == request.end_utc:
                exact_match_found = True
            else:
                sub_range_match_found = True
                try:
                    from services.parquet_cache import read_last_n_bricks
                    start_ts = pd.Timestamp(request.start_utc)
                    end_ts = pd.Timestamp(request.end_utc)
                    
                    # Estimate sliced ticks based on duration ratio
                    full_start = pd.Timestamp(valid_record["start_utc"])
                    full_end = pd.Timestamp(valid_record["end_utc"])
                    full_dur = (full_end - full_start).total_seconds()
                    sel_dur = (end_ts - start_ts).total_seconds()
                    if full_dur > 0:
                        ratio = max(0.0, min(1.0, sel_dur / full_dur))
                        sliced_ticks = int(valid_record["ticks_used"] * ratio)
                    else:
                        sliced_ticks = valid_record["ticks_used"]
                    
                    sliced_brick_counts = {
                        pip_str: int(count * ratio) if full_dur > 0 else count
                        for pip_str, count in valid_record.get("brick_counts", {}).items()
                    }
                    
                    # Create a copy with sliced metadata
                    sliced_meta = dict(valid_record)
                    sliced_meta["brick_counts"] = sliced_brick_counts
                    sliced_meta["ticks_used"] = sliced_ticks
                    valid_meta = sliced_meta
                    valid_record = valid_meta
                except Exception as slice_exc:
                    logger.warning(f"Failed to calculate sub-range slice details in lookup: {slice_exc}")
        else:
            # Stale record in SQLite, delete it
            logger.info(f"Stale cache database record found for key {cache_key}. Deleting...")
            delete_by_key(cache_key)

    # 2. Similar matches lookup
    similar_rows = lookup_similar(
        csv_path=csv_path,
        price_source=request.price_source,
        reversal_boxes=request.reversal_boxes,
        pip_size=request.pip_size,
        anchor=request.anchor,
    )

    valid_similar = []
    for record in similar_rows:
        if RENKO_METHOD_LABEL not in str(record.get("engine_used", "")):
            continue

        # Skip if it's the exact/sub-range match we just returned
        if valid_record and record["cache_key"] == valid_record["cache_key"]:
            continue

        job_id = record.get("job_id")
        # Verify files exist on disk
        files_ok = True
        if job_id:
            meta_file = CACHE_DIR / job_id / "job_meta.json"
            if not meta_file.exists():
                files_ok = False
            else:
                for pip in record.get("chart_pips", []):
                    pip_safe = str(pip).replace(".", "_")
                    p_file = CACHE_DIR / job_id / f"renko_{pip_safe}pip.parquet"
                    if not p_file.exists():
                        files_ok = False
                        break
        else:
            files_ok = False

        if files_ok:
            valid_similar.append(record)
        else:
            logger.info(f"Stale cache database record found for key {record['cache_key']}. Deleting...")
            delete_by_key(record["cache_key"])

    return {
        "exact_match": exact_match_found,
        "sub_range_match": sub_range_match_found,
        "cache_key": valid_record["cache_key"] if valid_record else None,
        "job_id": valid_record["job_id"] if valid_record else None,
        "metadata": valid_record if valid_record else None,
        "similar_matches": valid_similar
    }



@router.post("/api/jobs/build-renko")
async def post_build_renko_job(request: JobBuildRequest):
    """
    Submit a Renko build job. Returns job_id immediately.
    Processing happens in the background — poll /api/jobs/{job_id}/status
    or stream /ws/jobs/{job_id} for live updates.
    """
    csv_path = resolve_csv_path(request.csv_path)
    if not csv_path.exists():
        raise HTTPException(status_code=400, detail=f"CSV not found: {csv_path}")

    summary = await asyncio.to_thread(summarize_csv_file, csv_path)
    if summary.get("status") == "error":
        raise HTTPException(status_code=400, detail=f"CSV error: {summary.get('error')}")

    time_col  = summary["time_col"]
    bid_col   = summary["price_col"]
    ask_col   = summary.get("ask_col") or None
    delimiter = summary["delimiter"]

    src = request.price_source.lower()
    if   src == "bid":  source = bid_col
    elif src == "ask":  source = ask_col or bid_col
    elif src == "mid":  source = "__mid__" if (bid_col and ask_col) else bid_col
    else:               source = bid_col

    try:
        start_t = pd.Timestamp(request.start_utc, tz="UTC")
        end_t   = pd.Timestamp(request.end_utc,   tz="UTC")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid UTC range: {exc}")

    logger.info(f"BUILD REQUEST: csv={csv_path.name}, start_utc_raw='{request.start_utc}', end_utc_raw='{request.end_utc}', start_t={start_t}, end_t={end_t}, pips={request.chart_pips}")

    build_mode = (request.build_mode or "full").strip().lower()
    if build_mode not in {"full", "preview", "cache_only"}:
        raise HTTPException(status_code=400, detail=f"Invalid build mode: {request.build_mode}")

    max_rows = None
    cache_variant = RENKO_METHOD_CACHE_VARIANT
    if build_mode == "preview":
        max_rows = max(1_000, int(request.max_preview_ticks or 50_000))
        cache_variant = f"{RENKO_METHOD_CACHE_VARIANT}_preview_{max_rows}"

    from utils.cache import get_cache_key
    # Base cache key — without pip list — so individual pip caches are shareable
    base_cache_key = get_cache_key(
        csv_path, request.start_utc, request.end_utc,
        request.price_source, request.reversal_boxes,
        request.pip_size, request.anchor, [],  # no pips in base key
        cache_variant=cache_variant,
    )
    cache_key = get_cache_key(
        csv_path, request.start_utc, request.end_utc,
        request.price_source, request.reversal_boxes,
        request.pip_size, request.anchor, request.chart_pips,
        cache_variant=cache_variant,
    )

    # Check per-pip partial cache — load cached pips, only build missing ones
    from services.parquet_cache import check_legacy_cache_per_pip_counts
    per_pip_counts = check_legacy_cache_per_pip_counts(base_cache_key, request.chart_pips)
    cached_pip_counts = {k: v for k, v in per_pip_counts.items() if v is not None}
    cached_pips = {}
    missing_pips = [pip for pip in request.chart_pips if per_pip_counts.get(str(pip)) is None]

    if not missing_pips:
        # Full cache hit -- all pips cached
        job = job_manager.create_job()
        job.status        = "done"
        job.progress_percent = 100.0
        job.result_charts = {k: int(v) for k, v in cached_pip_counts.items()}
        job.bricks_built  = job.result_charts
        job.engine_used   = f"Cache (PyArrow Parquet) + {RENKO_METHOD_LABEL}"
        job._base_cache_key = base_cache_key
        job._partial_cached_pip_counts = cached_pip_counts
        import time as _t
        job.completed_at = _t.time()
        job.log(f"All {len(cached_pip_counts)} pips loaded from cache -- no CSV processing needed.")
        return {"job_id": job.job_id, "cache_hit": True, "build_mode": build_mode}

    if cached_pip_counts:
        logger.info(f"Partial cache hit: {list(cached_pip_counts.keys())} cached, building missing: {missing_pips}")

    job = job_manager.create_job()
    job._partial_cached_pips = cached_pips
    job._partial_cached_pip_counts = cached_pip_counts
    job._missing_pips = missing_pips
    job._base_cache_key = base_cache_key
    chunk_rows = getattr(request, "chunk_rows", 250_000) or 250_000
    if getattr(request, "chunk_size_mb", 0) > 0 and chunk_rows == 250_000:
        chunk_rows = max(250_000, int((request.chunk_size_mb * 1024 * 1024) / 40))

    gpu_requested = request.processing_engine.lower() != "cpu"
    cupy_ok = detect_cupy_available()
    is_wrapping = summary.get("is_wrapping", False)
    use_gpu = gpu_requested and cupy_ok and not is_wrapping
    logger.info(f"JOB BUILD: gpu_requested={gpu_requested}, cupy_ok={cupy_ok}, is_wrapping={is_wrapping}, use_gpu={use_gpu}, processing_engine={request.processing_engine}")

    async def _run():
        import time as _t
        t_start = _t.perf_counter()
        build_pips = missing_pips if missing_pips else request.chart_pips
        
        gpu_success = False
        if use_gpu:
            try:
                job.status = "running"
                job.stage = "gpu_build"
                job.log("GPU streaming engine requested. Processing CSV in chunks...")

                from services.gpu_pipeline import run_gpu_streaming_pipeline
                await run_gpu_streaming_pipeline(
                    job            = job,
                    csv_path       = csv_path,
                    delimiter      = delimiter,
                    time_col       = time_col,
                    source         = source,
                    bid_col        = bid_col,
                    ask_col        = ask_col,
                    start_t        = start_t,
                    end_t          = end_t,
                    chart_pips     = build_pips,
                    reversal_boxes = request.reversal_boxes,
                    pip_size       = request.pip_size,
                    anchor         = request.anchor,
                    base_cache_key = base_cache_key,
                    max_rows       = max_rows,
                    chunk_rows     = chunk_rows,
                    price_source   = request.price_source,
                )

                # Merge partial-cached pips into final counts
                if cached_pip_counts:
                    for k, v in cached_pip_counts.items():
                        if k not in job.bricks_built:
                            job.bricks_built[k] = int(v)
                    if job.result_charts:
                        for k, v in cached_pip_counts.items():
                            if k not in job.result_charts:
                                job.result_charts[k] = int(v)

                gpu_success = True
                job.log(f"✅ GPU streaming build done.")

            except Exception as gpu_exc:
                job.log(f"GPU streaming failed: {gpu_exc}. Falling back to CPU streaming...")
                logger.warning(f"GPU streaming failed for job {job.job_id}: {gpu_exc}")

        if False and use_gpu and not gpu_success:  # old monolithic GPU path (kept for reference)
            try:
                job.status = "running"
                job.stage = "gpu_build"
                job.log("GPU engine requested. Loading data for GPU Renko build...")
                
                # Load the data range (run in ThreadPoolExecutor/to_thread)
                def _load_data():
                    prices, times, bids, asks = None, None, None, None
                    rows_scanned, rows_loaded = 0, 0
                    loader_engine = ""
                    loaded_ok = False
                    
                    # 1. Try GPU Polars loader
                    if detect_gpu_polars_available():
                        try:
                            from services.gpu_engine import read_selected_range_gpu_polars
                            prices, times, rows_scanned, rows_loaded, loader_engine = read_selected_range_gpu_polars(
                                path=csv_path, delimiter=delimiter, time_col=time_col,
                                source=source, bid_col=bid_col, ask_col=ask_col,
                                start_t=start_t, end_t=end_t, max_rows=max_rows,
                            )
                            loaded_ok = True
                        except Exception as e:
                            logger.info(f"GPU build: GPU Polars loader skipped: {e}")
                            
                    # 2. Try GPU cuDF loader
                    if not loaded_ok and detect_cudf_available():
                        try:
                            prices, times, rows_scanned, rows_loaded, loader_engine = read_selected_range_gpu(
                                path=csv_path, delimiter=delimiter, time_col=time_col,
                                source=source, bid_col=bid_col, ask_col=ask_col,
                                start_t=start_t, end_t=end_t, max_rows=max_rows,
                            )
                            loaded_ok = True
                        except Exception as e:
                            logger.info(f"GPU build: GPU cuDF loader skipped: {e}")
                            
                    # 3. Try CPU Polars loader
                    if not loaded_ok:
                        try:
                            prices, times, rows_scanned, rows_loaded, loader_engine, bids, asks = read_selected_range_cpu(
                                path=csv_path, delimiter=delimiter, time_col=time_col,
                                source=source, bid_col=bid_col, ask_col=ask_col,
                                start_t=start_t, end_t=end_t, max_rows=max_rows,
                            )
                            loaded_ok = True
                        except Exception as e:
                            logger.info(f"GPU build: CPU Polars loader skipped: {e}")
                            
                    # 4. Fallback to CPU pandas loader
                    if not loaded_ok:
                        prices, times, rows_scanned, rows_loaded, loader_engine, bids, asks = read_selected_range_cpu(
                            path=csv_path, delimiter=delimiter, time_col=time_col,
                            source=source, bid_col=bid_col, ask_col=ask_col,
                            start_t=start_t, end_t=end_t, max_rows=max_rows,
                        )
                        
                    return prices, times, bids, asks, rows_scanned, rows_loaded, loader_engine

                prices, times, bids, asks, rows_scanned, rows_loaded, loader_engine = await asyncio.to_thread(_load_data)
                
                if prices is None or len(prices) == 0:
                    raise ValueError("No price ticks were found in the selected range.")
                
                if bids is None:
                    bids = prices
                if asks is None:
                    asks = prices
                
                job.log(f"Loaded {rows_loaded:,} ticks via {loader_engine}. Running GPU CuPy calculations...")
                
                # Run the CuPy calculation — only for missing pips
                _build_pips = missing_pips if missing_pips else request.chart_pips
                def _calc_gpu():
                    return build_renko_gpu_multi(
                        prices=prices,
                        times=times,
                        pips=_build_pips,
                        reversal_boxes=request.reversal_boxes,
                        pip_size=request.pip_size,
                        anchor_mode=request.anchor
                    )
                
                results = await asyncio.to_thread(_calc_gpu)
                
                # Write to Parquet cache — only newly built pips
                job.log("Writing brick buffers to Parquet...")
                from services.parquet_cache import RenkoParquetWriter, save_pip_cache
                writer = RenkoParquetWriter(job.job_id, _build_pips)
                brick_counts = {}

                for idx, pip in enumerate(_build_pips):
                    pip_str = str(pip)
                    res = results[idx]
                    indices, opens, closes, tops, bottoms, highs, lows, colors, borders, brick_times, ticks, dir_strs, times_idx, _ = res
                    
                    try:
                        import polars as pl
                        import numpy as np
                        
                        if len(opens) == 0:
                            writer.write_batch(pip_str, [])
                            brick_counts[pip_str] = 0
                            continue
                            
                        # Slice bids/asks with numpy
                        bid_vals = bids[times_idx] if bids is not None else closes
                        ask_vals = asks[times_idx] if asks is not None else closes
                        
                        # Vectorized conversion using Polars
                        df = pl.DataFrame({
                            "confirm_tick_index": times_idx.astype(np.int64),
                            "confirm_time": brick_times.astype(str),
                            "open": opens.astype(np.float64),
                            "high": highs.astype(np.float64),
                            "low": lows.astype(np.float64),
                            "close": closes.astype(np.float64),
                            "direction": dir_strs.astype(str),
                            "tick_count": ticks.astype(np.int32),
                            "brick_size_pips": np.full(len(opens), float(pip), dtype=np.float32),
                            "bid": bid_vals.astype(np.float64),
                            "ask": ask_vals.astype(np.float64)
                        })
                        
                        df = df.with_columns(
                            (pl.col("confirm_tick_index").cum_count().over("confirm_tick_index") - 1).alias("seq")
                        )
                        df = df.with_columns(
                            (pl.col("confirm_tick_index") * 1000 + pl.col("seq")).alias("time")
                        ).drop("seq")
                        
                        # Round prices to 10 decimal places to match original logic and cast to exact Arrow schema
                        df = df.select([
                            pl.col("time").cast(pl.Int64),
                            pl.col("confirm_tick_index").cast(pl.Int64),
                            pl.col("confirm_time").cast(pl.String),
                            pl.col("open").round(10).cast(pl.Float64),
                            pl.col("high").round(10).cast(pl.Float64),
                            pl.col("low").round(10).cast(pl.Float64),
                            pl.col("close").round(10).cast(pl.Float64),
                            pl.col("direction").cast(pl.String),
                            pl.col("tick_count").cast(pl.Int32),
                            pl.col("brick_size_pips").cast(pl.Float32),
                            pl.col("bid").round(10).cast(pl.Float64),
                            pl.col("ask").round(10).cast(pl.Float64),
                        ])
                        
                        from services.parquet_cache import _BRICK_SCHEMA
                        table = df.to_arrow().cast(_BRICK_SCHEMA)
                        writer.write_table(pip_str, table)
                        brick_counts[pip_str] = len(df)
                    except Exception as fallback_exc:
                        logger.warning(f"Vectorized Polars table creation failed for pip {pip_str}: {fallback_exc}. Falling back to Python loop.")
                        bricks_list = []
                        seq_map = {}
                        for j in range(len(opens)):
                            tick_idx = int(times_idx[j])
                            seq = seq_map.get(tick_idx, 0)
                            seq_map[tick_idx] = seq + 1
                            chart_time = tick_idx * 1000 + seq
                            
                            bricks_list.append({
                                "time":               chart_time,
                                "confirm_tick_index": tick_idx,
                                "confirm_time":       str(brick_times[j]),
                                "open":               round(float(opens[j]),  10),
                                "high":               round(float(highs[j]),  10),
                                "low":                round(float(lows[j]),   10),
                                "close":              round(float(closes[j]), 10),
                                "direction":          str(dir_strs[j]),
                                "tick_count":         int(ticks[j]),
                                "brick_size_pips":    float(pip),
                                "bid":                float(bids[tick_idx]) if bids is not None and tick_idx < len(bids) else float(closes[j]),
                                "ask":                float(asks[tick_idx]) if asks is not None and tick_idx < len(asks) else float(closes[j]),
                            })
                        
                        writer.write_batch(pip_str, bricks_list)
                        brick_counts[pip_str] = len(bricks_list)
                writer.close_all()

                # Save each newly built pip to per-pip base-key cache for future reuse
                try:
                    for pip in _build_pips:
                        pip_str = str(pip)
                        pip_parquet = CACHE_DIR / job.job_id / f"renko_{pip_str.replace('.', '_')}pip.parquet"
                        if pip_parquet.exists():
                            import shutil, hashlib
                            dest = CACHE_DIR / f"{base_cache_key}_pip{pip_str.replace('.', '_')}.parquet"
                            shutil.copy2(str(pip_parquet), str(dest))
                except Exception as _cp_exc:
                    logger.warning(f"Per-pip cache copy failed: {_cp_exc}")

                # Merge cached pips into brick_counts
                for k, v in cached_pip_counts.items():
                    if k not in brick_counts:
                        brick_counts[k] = int(v)

                # Write job_meta.json
                writer.write_meta({
                    "rows_scanned": rows_scanned,
                    "ticks_loaded": rows_loaded,
                    "chunk_count":  1,
                    "engine":       f"GPU CuPy / {loader_engine} + {RENKO_METHOD_LABEL}",
                    "elapsed_s":    round(_t.perf_counter() - t_start, 2),
                })
                
                # Write build history
                try:
                    if "preview" not in cache_key:
                        from services.build_history import record_build
                        cache_files = [
                            str(CACHE_DIR / job.job_id / f"renko_{str(p).replace('.', '_')}pip.parquet")
                            for p in request.chart_pips
                        ]
                        record_build(
                            cache_key     = cache_key,
                            csv_path      = csv_path,
                            start_utc     = str(start_t),
                            end_utc       = str(end_t),
                            price_source  = request.price_source,
                            reversal_boxes= request.reversal_boxes,
                            pip_size      = request.pip_size,
                            anchor        = request.anchor,
                            chart_pips    = request.chart_pips,
                            brick_counts  = {k: int(v) for k, v in brick_counts.items()},
                            ticks_used    = rows_loaded,
                            rows_scanned  = rows_scanned,
                            engine_used   = f"GPU CuPy / {loader_engine} + {RENKO_METHOD_LABEL}",
                            cache_files   = cache_files,
                            job_id        = job.job_id,
                        )
                except Exception as bh_exc:
                    logger.warning(f"Build history write skipped on GPU path: {bh_exc}")
                
                # Merge cached pips into brick_counts BEFORE set_done
                for k, v in cached_pip_counts.items():
                    if k not in brick_counts:
                        brick_counts[k] = int(v)

                job.engine_used = f"GPU CuPy / {loader_engine} + {RENKO_METHOD_LABEL}"
                job.ticks_used = rows_loaded
                job.rows_scanned = rows_scanned
                job.completed_at = _t.time()
                _pmeta_invalidate_job(job.job_id)
                job.set_done(brick_counts, job.engine_used)
                job.log(f"✅ GPU build completed in {round(_t.perf_counter() - t_start, 2)}s!")
                gpu_success = True
                
            except Exception as gpu_exc:
                job.log(f"GPU build failed: {gpu_exc}. Falling back to CPU streaming...")
                logger.warning(f"GPU build failed for job {job.job_id}: {gpu_exc}. Falling back to CPU streaming.")
        
        if not gpu_success:
            try:
                _build_pips_cpu = missing_pips if missing_pips else request.chart_pips
                if cached_pip_counts:
                    job.log(f"Partial cache: reusing {list(cached_pip_counts.keys())}, building {_build_pips_cpu}")
                # Attach cached pips to job so pipeline.py can merge before set_done
                job._partial_cached_pips = cached_pips
                job._partial_cached_pip_counts = cached_pip_counts
                await run_build_pipeline(
                    job            = job,
                    csv_path       = csv_path,
                    delimiter      = delimiter,
                    time_col       = time_col,
                    source         = source,
                    bid_col        = bid_col,
                    ask_col        = ask_col,
                    start_t        = start_t,
                    end_t          = end_t,
                    chart_pips     = _build_pips_cpu,
                    reversal_boxes = request.reversal_boxes,
                    pip_size       = request.pip_size,
                    anchor         = request.anchor,
                    cache_key      = base_cache_key,  # use base key for per-pip storage
                    max_rows       = max_rows,
                    chunk_rows     = chunk_rows,
                    return_ticks   = False,
                    price_source   = request.price_source,
                )
            except Exception as exc:
                import traceback
                job.set_error(f"{exc}\n{traceback.format_exc()}")
                logger.error(f"Job {job.job_id} failed: {exc}")

    asyncio.create_task(_run())
    return {"job_id": job.job_id, "cache_hit": False, "build_mode": build_mode}


@router.get("/api/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """Poll job progress. Includes CPU/GPU/RAM metrics."""
    job = job_manager.get_job(job_id)
    if not job:
        from services.parquet_cache import get_job_meta
        meta = get_job_meta(job_id)
        if meta:
            status = {
                "job_id": job_id,
                "status": "done",
                "progress_percent": 100.0,
                "rows_scanned": meta.get("rows_scanned", 0),
                "ticks_used": meta.get("ticks_loaded", 0),
                "bricks_built": meta.get("brick_counts", {}),
                "engine_used": meta.get("engine", "Cache"),
            }
            sys_stats = get_system_stats()
            status.update(sys_stats)
            return status
        raise HTTPException(status_code=404, detail="Job not found")
    status = job.to_status_dict()
    sys_stats = get_system_stats()
    status.update(sys_stats)
    return status


@router.get("/api/jobs/{job_id}/window")
async def get_job_window(
    job_id:     str,
    pip:        str,
    start_time: int = 0,
    end_time:   int = 0,
    max_bricks: int = 10_000,
):
    """
    Paginated viewport slice — returns only bricks in [start_time, end_time].
    Uses Polars lazy scan with predicate pushdown (reads only matching row groups).
    Ideal for zoomed-in frontend requests.
    """
    pip_safe = pip.replace(".", "_")
    path = CACHE_DIR / job_id / f"renko_{pip_safe}pip.parquet"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Parquet not found for pip {pip}")

    try:
        bricks = _pmeta_read_window(
            job_id=job_id,
            pip_str=pip,
            path=path,
            start_time=start_time,
            end_time=end_time,
            max_bricks=max_bricks,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Window query failed: {exc}")

    return {"pip": pip, "count": len(bricks), "bricks": bricks}


@router.get("/api/jobs/{job_id}/result")
async def get_job_result(
    job_id: str,
    pip: str = "",
    max_bricks: int = 20_000,
    start_utc: str = "",
    end_utc: str = "",
    downsample: bool = True,
):
    """
    Return job result. Bricks are loaded lazily from Parquet (max_bricks limit).
    Supports optional sub-range filtering (slicing) via start_utc and end_utc.
    Applies LTTB downsampling when brick count exceeds threshold.
    """
    job = job_manager.get_job(job_id)
    max_bricks = max(1_000, min(int(max_bricks or 20_000), 50_000))
    
    # Helper to filter bricks
    def _filter_bricks(bricks_list):
        if not start_utc and not end_utc:
            return bricks_list
        filtered = []
        try:
            start_ts = pd.Timestamp(start_utc) if start_utc else None
            end_ts = pd.Timestamp(end_utc) if end_utc else None
            for b in bricks_list:
                confirm_time = b.get("confirm_time")
                if not confirm_time:
                    filtered.append(b)
                    continue
                brick_t = pd.Timestamp(confirm_time)
                # handle timezone-aware vs naive timestamp comparison
                if start_ts and start_ts.tzinfo and not brick_t.tzinfo:
                    brick_t = brick_t.tz_localize("UTC")
                elif start_ts and not start_ts.tzinfo and brick_t.tzinfo:
                    brick_t = brick_t.tz_convert(None)
                    
                if start_ts and brick_t < start_ts:
                    continue
                if end_ts and brick_t > end_ts:
                    continue
                filtered.append(b)
            return filtered
        except Exception as filter_exc:
            logger.warning(f"Error filtering bricks by sub-range: {filter_exc}")
            return bricks_list

    if not job:
        from services.parquet_cache import get_job_meta
        meta = get_job_meta(job_id)
        if meta:
            charts: Dict[str, Any] = {}
            for pip_str, count in meta.get("brick_counts", {}).items():
                if pip and pip_str != pip:
                    continue
                bricks = read_last_n_bricks(job_id=job_id, pip_str=pip_str, n=max_bricks)
                bricks = _filter_bricks(bricks)
                if len(bricks) > max_bricks:
                    bricks = bricks[-max_bricks:]
                charts[pip_str] = bricks
            
            slice_counts = {k: len(v) for k, v in charts.items()}
            return {
                "status":       "done",
                "engine_used":  meta.get("engine", "Disk Cache"),
                "ticks_used":   meta.get("ticks_loaded", 0),
                "rows_scanned": meta.get("rows_scanned", 0),
                "bricks_built": slice_counts,
                "total_bricks_built": meta.get("brick_counts", slice_counts),
                "charts":       charts,
                "diagnostics":  meta.get("diagnostics", ["Loaded and sliced from persistent Parquet storage cache."]),
            }
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in ("done",):
        raise HTTPException(status_code=202, detail=f"Job status: {job.status}")

    # Pips that were cached (loaded from flat-file per-pip cache, not built this job)
    _cached_pips_data: dict = getattr(job, "_partial_cached_pips", {}) or {}
    _cached_pip_counts: dict = getattr(job, "_partial_cached_pip_counts", {}) or {}
    _base_cache_key: str = getattr(job, "_base_cache_key", "") or ""

    charts: Dict[str, Any] = {}
    for pip_str, count in job.bricks_built.items():
        if pip and pip_str != pip:
            continue
        # If this pip was served from partial cache, inject directly
        if pip_str in _cached_pips_data:
            bricks = _cached_pips_data[pip_str]
            bricks = _filter_bricks(bricks)
            if len(bricks) > max_bricks:
                bricks = bricks[-max_bricks:]
            charts[pip_str] = bricks
        elif pip_str in _cached_pip_counts and _base_cache_key:
            from services.parquet_cache import read_legacy_pip_cache_window
            bricks = read_legacy_pip_cache_window(_base_cache_key, pip_str, max_bricks=max_bricks)
            bricks = _filter_bricks(bricks)
            if len(bricks) > max_bricks:
                bricks = bricks[-max_bricks:]
            charts[pip_str] = bricks
        else:
            bricks = read_last_n_bricks(job_id=job_id, pip_str=pip_str, n=max_bricks)
            bricks = _filter_bricks(bricks)
            if len(bricks) > max_bricks:
                bricks = bricks[-max_bricks:]
            charts[pip_str] = bricks

    # Apply LTTB downsampling for large result sets
    if downsample:
        from utils.downsample import maybe_downsample
        charts = {k: maybe_downsample(v) for k, v in charts.items()}

    slice_counts = {k: len(v) for k, v in charts.items()}
    return {
        "status":       "done",
        "engine_used":  job.engine_used,
        "ticks_used":   job.ticks_used,
        "rows_scanned": job.rows_scanned,
        "bricks_built": slice_counts,
        "total_bricks_built": job.bricks_built,
        "charts":       charts,
        "diagnostics":  job.diagnostics,
        "downsampled":  downsample,
    }




@router.post("/api/clear-cache")
def clear_cache():
    try:
        import shutil
        if CACHE_DIR.exists():
            shutil.rmtree(CACHE_DIR)
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return {"status": "ok", "message": "Backend cache cleared successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@router.get("/api/renko-window")
def get_renko_window(
    job_id:    str,
    chart_id:  str,
    from_x:    int = None,
    to_x:      int = None,
    max_bricks: int = 1_000_000_000,
):
    """
    Lazy-load a window of Renko bricks from Parquet for a specific pip size.
    chart_id = pip string e.g. '1.0', '2.0'
    from_x / to_x = brick time values (tick_index * 1000 + seq)
    """
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in ("done",):
        raise HTTPException(status_code=202, detail=f"Job not done yet: {job.status}")

    bricks = parquet_read_window(
        job_id     = job_id,
        pip_str    = chart_id,
        from_x     = from_x,
        to_x       = to_x,
        max_bricks = max_bricks,
    )
    total = job.bricks_built.get(chart_id, 0)
    return {
        "job_id":        job_id,
        "chart_id":      chart_id,
        "from_x":        from_x,
        "to_x":          to_x,
        "total_bricks":  total,
        "returned":      len(bricks),
        "bricks":        bricks,
    }


@router.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running build job gracefully."""
    ok = job_manager.cancel_job(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": "cancelled"}


@router.get("/api/jobs")
async def list_jobs():
    """List all jobs (for debugging)."""
    return job_manager.list_jobs()


@router.get("/api/system-stats")
async def system_stats():
    """Real-time CPU/GPU/RAM snapshot. No fake values."""
    stats = get_system_stats()
    stats["engine_status"] = _ENGINE_STATUS
    return stats


@router.websocket("/ws/jobs/{job_id}")
async def ws_job_progress(websocket: WebSocket, job_id: str):
    """
    Stream live progress for a build job.
    Sends JSON frames:  { type: "progress"|"log"|"done"|"error", ... }
    Also pushes system-stats updates every 2 seconds.
    """
    await websocket.accept()
    job = job_manager.get_job(job_id)
    if not job:
        await _ws_send(websocket,{"type": "error", "message": "Job not found"})
        await websocket.close()
        return

    # If job already done, send result immediately and close
    if job.status == "done":
        await _ws_send(websocket,{"type": "done", **job.to_status_dict()})
        await websocket.close()
        return

    if job.status == "error":
        await _ws_send(websocket,{"type": "error", **job.to_status_dict()})
        await websocket.close()
        return

    q = job.add_subscriber()
    stats_task = None

    async def _push_stats():
        while True:
            await asyncio.sleep(2)
            try:
                stats = get_system_stats()
                await _ws_send(websocket,{"type": "system_stats", **stats})
            except Exception:
                break

    try:
        stats_task = asyncio.create_task(_push_stats())
        while True:
            try:
                item = await asyncio.wait_for(q.get(), timeout=60.0)
            except asyncio.TimeoutError:
                await _ws_send(websocket,{"type": "heartbeat"})
                continue
            if item is SENTINEL:
                break
            await _ws_send(websocket,item)
            if item.get("type") in ("done", "error"):
                break
    except (WebSocketDisconnect, Exception) as exc:
        logger.info(f"Job WS {job_id} disconnected: {exc}")
    finally:
        if stats_task:
            stats_task.cancel()
        job.remove_subscriber(q)
        try:
            await websocket.close()
        except Exception:
            pass







