import asyncio
import json
import logging
import math
from pathlib import Path
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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import ORJSONResponse, JSONResponse

from models import (
    MetadataRequest, MetadataResponse, RenkoRequest, RenkoResponse,
    RenkoBrick, JobBuildRequest, RenkoWindowRequest,
)
from csv_reader import (
    resolve_csv_path, summarize_csv_file, detect_pip_size,
    read_selected_range_cpu, read_selected_range_duckdb,
)
from renko_engine import build_renko_cpu
from renko_state import build_streaming_engines
from csv_stream import stream_ticks
from gpu_engine import (
    detect_gpu_available, detect_cupy_available, detect_gpu_polars_available, detect_cudf_available,
    read_selected_range_gpu, build_renko_gpu_multi,
    sort_timeline_by_timestamp_gpu, sort_timeline_by_timestamp_cpu,
    precompute_frame_plan_gpu, precompute_frame_plan_cpu,
    recompute_frame_plan_from_position,
)
from monitor import get_system_stats, set_high_priority, configure_thread_pools
from job_manager import job_manager, SENTINEL
from pipeline import run_build_pipeline, _check_full_cache, _cache_parquet_path
from parquet_cache import read_window as parquet_read_window, read_last_n_bricks

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("renko_playback")

# Fast JSON serialiser — orjson if available, else stdlib json
def _fast_dumps(obj: Any) -> str:
    if _USE_ORJSON:
        return orjson.dumps(obj).decode("utf-8")
    return json.dumps(obj)

# Default response class
_ResponseClass = ORJSONResponse if _USE_ORJSON else None

app = FastAPI(
    title="Renko Tick Playback Dashboard API",
    default_response_class=ORJSONResponse if _USE_ORJSON else JSONResponse,
)

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


# ─── CORS ─────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _on_startup():
    global _ENGINE_STATUS
    # Boost process priority (non-fatal if denied)
    set_high_priority()
    # Configure Polars/DuckDB thread pools BEFORE first import
    configure_thread_pools()
    # Probe and cache engine status
    _ENGINE_STATUS = probe_engine_status()
    logger.info(f"Engine matrix: {_ENGINE_STATUS}")
    # Kick psutil CPU% baseline (first call always returns 0.0)
    try:
        import psutil
        psutil.cpu_percent(interval=None)
    except Exception:
        pass


@app.get("/api/health")
def health_check():
    return {"status": "ok"}


@app.get("/api/engine-status")
def engine_status_endpoint():
    """Returns the real backend performance stack — no fake GPU labels."""
    return _ENGINE_STATUS if _ENGINE_STATUS else probe_engine_status()

@app.post("/api/metadata", response_model=MetadataResponse)
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
            start_utc = summary["first_time"].isoformat().replace("+00:00", "Z")
            
        end_utc = ""
        if summary.get("last_time"):
            end_utc = summary["last_time"].isoformat().replace("+00:00", "Z")
            
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

@app.post("/api/build-renko", response_model=RenkoResponse)
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
            from cache import get_cache_key, check_cache, save_cache
            cache_key = get_cache_key(
                csv_path=csv_path,
                start_utc=request.start_utc,
                end_utc=request.end_utc,
                price_source=request.price_source,
                reversal_boxes=request.reversal_boxes,
                pip_size=request.pip_size,
                anchor=request.anchor,
                chart_pips=request.chart_pips
            )
            cached_result = check_cache(cache_key)
            if cached_result is not None:
                diagnostics.append("CACHE HIT: Loaded pre-calculated Renko bricks from disk cache.")
                return RenkoResponse(
                    status="ok",
                    engine_used="Disk Cache",
                    rows_scanned=cached_result["rows_scanned"],
                    ticks_loaded=cached_result["ticks_loaded"],
                    charts=cached_result["charts"],
                    diagnostics=diagnostics
                )
            diagnostics.append("Cache miss. Proceeding with loading and calculations...")
        except Exception as ce:
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
        gpu_requested = request.processing_engine.lower() in ("gpu", "auto")
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
                    from gpu_engine import read_selected_range_gpu_polars
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
                engine_used = f"GPU CuPy / {loader_engine}"
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

                with ThreadPoolExecutor(max_workers=min(4, len(request.chart_pips))) as executor:
                    res_list = list(executor.map(process_pip, request.chart_pips))
                    for pip_str, bricks in res_list:
                        charts[pip_str] = bricks
                        diagnostics.append(f"Success: CPU generated {len(bricks)} bricks for {pip_str} pip chart.")
                        
                engine_used = f"CPU Parallel Numba / {loader_engine}"
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
        from csv_reader import read_header_columns, seek_first_timestamp_offset, csv_row_values
        columns = read_header_columns(csv_path, delimiter)
        if time_col not in columns:
            return 100_000
        time_index = columns.index(time_col)
        
        # Find data start offset
        with csv_path.open("rb") as fh:
            fh.readline()
            data_start = fh.tell()
            first_line = fh.readline()
            
        if not first_line:
            return 0
            
        approx_bytes_per_row = max(30, len(first_line))
        
        # Bisect to find start and end offsets (very fast, <15ms)
        start_offset = seek_first_timestamp_offset(csv_path, start_t, data_start, time_index, delimiter)
        end_offset = seek_first_timestamp_offset(csv_path, end_t, data_start, time_index, delimiter)
        
        # Sample average row size near start_offset for higher precision
        with csv_path.open("rb") as fh:
            fh.seek(start_offset)
            sample_bytes = 0
            sample_rows = 0
            for _ in range(100):
                line = fh.readline()
                if not line:
                    break
                sample_bytes += len(line)
                sample_rows += 1
        if sample_rows > 0:
            approx_bytes_per_row = sample_bytes / sample_rows
            
        range_bytes = max(0, end_offset - start_offset)
        estimated_ticks = int(range_bytes / approx_bytes_per_row)
        return max(1, estimated_ticks)
    except Exception as e:
        logger.exception("Error estimating ticks in range")
        return 100_000  # safe fallback estimate


@app.websocket("/ws/playback")
async def ws_playback(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket playback client connected")
    
    playback_task = None
    diagnostics = []
    diagnostics.append("WebSocket connection accepted.")

    state = {
        "is_playing": False,
        "speed": 100.0,
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

    async def get_next_tick_async() -> Optional[Tuple[float, str, float, float]]:
        batch = await get_next_tick_batch_async(1)
        if batch is None:
            return None
        prices, times, bids, asks, _ = batch
        return float(prices[0]), str(times[0]), float(bids[0]), float(asks[0])

    async def skip_to_target_idx(target_idx: int):
        target_idx = max(0, min(target_idx, state["total_ticks"]))
        
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
        
        await websocket.send_text(_fast_dumps({"type": "status", "status": "reset"}))
        
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
        
        await websocket.send_text(_fast_dumps({
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
        }))

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
                    
                    while ticks_left > 0:
                        batch = await get_next_tick_batch_async(ticks_left)
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
                        await websocket.send_text(_fast_dumps({
                            "type": "status",
                            "status": "ended",
                            "total_ticks": state["total_ticks"],
                            "diagnostics": diagnostics + loop_diags
                        }))
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

                        await websocket.send_text(_fast_dumps({
                            "type": "playback_frame",
                            "bricks_by_chart": batch_by_chart,
                            "live_bricks_by_chart": live_bricks_by_chart,
                            "processed_ticks": state["global_tick_index"],
                            "total_ticks": state["total_ticks"],
                            "formed_bricks": total_formed,
                            "speed": ticks_per_second,
                            "latest_bid": last_bid,
                            "latest_ask": last_ask,
                            "latest_time": last_time
                        }))

                await asyncio.sleep(FRAME_INTERVAL)

        except asyncio.CancelledError:
            loop_diags.append("Playback loop cancelled by system/client request.")
            logger.info("Playback loop cancelled")
        except Exception as exc:
            logger.exception("Playback loop exception")
            try:
                await websocket.send_json({
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
                # Stop existing loop if any
                if playback_task:
                    diagnostics.append("Stopping existing playback task...")
                    playback_task.cancel()
                    playback_task = None
                    
                diagnostics = []
                diagnostics.append(f"Start request received for CSV: {cmd.get('csv_path')}")
                diagnostics.append(f"Range: {cmd.get('start_utc')} .. {cmd.get('end_utc')}")
                
                await websocket.send_text(_fast_dumps({
                    "type": "status",
                    "status": "loading",
                    "message": "Initializing playback...",
                    "diagnostics": diagnostics
                }))
                
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
                    
                    logger.info(
                        f"\n[Playback Request]\n"
                        f"CSV: {csv_path}\n"
                        f"Requested start_utc: {cmd.get('start_utc')}\n"
                        f"Requested end_utc: {cmd.get('end_utc')}\n"
                    )
                    
                    diagnostics.append("Summarizing CSV structure...")
                    summary = summarize_csv_file(csv_path)
                    if summary.get("status") == "error":
                        raise ValueError(f"Error parsing CSV structure: {summary.get('error')}")
                        
                    time_col = summary["time_col"]
                    bid_col = summary["price_col"]
                    ask_col = summary["ask_col"]
                    delimiter = summary["delimiter"]
                    
                    diagnostics.append(f"Detected columns: time='{time_col}', bid='{bid_col}', ask='{ask_col or 'None'}', delimiter='{delimiter}'")
                    
                    # Determine source column
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
                    
                    # Store CSV params in state for seeking
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
                    
                    # Reset indices back to 0
                    state["chunk_index"] = 0
                    state["global_tick_index"] = 0
                    
                    first_loaded_time = first_tick[1]
                    state["is_playing"] = True
                    
                    diagnostics.append(
                        f"Ready. Ticks: {state['total_ticks']} | "
                        f"Engines: {len(state['renko_engines'])}"
                    )
                    await websocket.send_text(_fast_dumps({
                        "type": "status",
                        "status": "ready",
                        "total_bricks": 0,
                        "ticks_loaded": state["total_ticks"],
                        "first_tick": first_loaded_time,
                        "last_tick": cmd["end_utc"],
                        "diagnostics": diagnostics
                    }))
                    
                    playback_task = asyncio.create_task(playback_loop())
                    
                except Exception as build_err:
                    err_msg = f"Setup failed: {build_err}"
                    diagnostics.append(f"ERROR: {err_msg}")
                    import traceback
                    diagnostics.extend(traceback.format_exc().splitlines())
                    await websocket.send_text(_fast_dumps({
                        "type": "error",
                        "message": err_msg,
                        "diagnostics": diagnostics
                    }))
                    
            elif action == "pause":
                state["is_playing"] = False
                await websocket.send_text(_fast_dumps({"type": "status", "status": "paused"}))
                
            elif action == "resume":
                state["is_playing"] = True
                await websocket.send_text(_fast_dumps({"type": "status", "status": "playing"}))
                
            elif action == "step":
                if not state["renko_engines"]:
                    continue
                state["is_playing"] = False
                target_idx = state["global_tick_index"] + 1
                await skip_to_target_idx(target_idx)
                
            elif action == "speed":
                new_speed = float(cmd.get("speed", 100.0))
                state["speed"] = new_speed
                await websocket.send_text(_fast_dumps({
                    "type": "status",
                    "status": "speed_updated",
                    "speed": new_speed
                }))
                
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
        if playback_task:
            playback_task.cancel()

# ─────────────────────────────────────────────────────────────────────────────
# Job-based Renko Build API
# ─────────────────────────────────────────────────────────────────────────────

# JobBuildRequest is now imported from models.py


@app.post("/api/jobs/build-renko")
async def post_build_renko_job(request: JobBuildRequest):
    """
    Submit a Renko build job. Returns job_id immediately.
    Processing happens in the background — poll /api/jobs/{job_id}/status
    or stream /ws/jobs/{job_id} for live updates.
    """
    csv_path = resolve_csv_path(request.csv_path)
    if not csv_path.exists():
        raise HTTPException(status_code=400, detail=f"CSV not found: {csv_path}")

    summary = summarize_csv_file(csv_path)
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

    build_mode = (request.build_mode or "full").strip().lower()
    if build_mode not in {"full", "preview", "cache_only"}:
        raise HTTPException(status_code=400, detail=f"Invalid build mode: {request.build_mode}")

    max_rows = None
    cache_variant = ""
    if build_mode == "preview":
        max_rows = max(1_000, int(request.preview_ticks or 50_000))
        cache_variant = f"preview_{max_rows}"

    from cache import get_cache_key
    cache_key = get_cache_key(
        csv_path, request.start_utc, request.end_utc,
        request.price_source, request.reversal_boxes,
        request.pip_size, request.anchor, request.chart_pips,
        cache_variant=cache_variant,
    )

    # Check legacy flat-file Parquet cache first
    cached = _check_full_cache(cache_key, request.chart_pips)
    if cached:
        job = job_manager.create_job()
        job.status        = "done"
        job.progress_percent = 100.0
        job.result_charts = {str(pip): len(v) for pip, v in cached.items()}
        job.bricks_built  = job.result_charts
        job.engine_used   = "Cache (PyArrow Parquet)"
        import time as _t
        job.completed_at = _t.time()
        job.log("Loaded from Parquet cache — no CSV processing needed.")
        return {"job_id": job.job_id, "cache_hit": True, "build_mode": build_mode}

    job = job_manager.create_job()
    chunk_rows = getattr(request, "chunk_rows", 250_000) or 250_000

    async def _run():
        try:
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
                chart_pips     = request.chart_pips,
                reversal_boxes = request.reversal_boxes,
                pip_size       = request.pip_size,
                anchor         = request.anchor,
                cache_key      = cache_key,
                max_rows       = max_rows,
                chunk_rows     = chunk_rows,
                return_ticks   = False,   # streaming arch never returns raw ticks
            )
        except Exception as exc:
            import traceback
            job.set_error(f"{exc}\n{traceback.format_exc()}")
            logger.error(f"Job {job.job_id} failed: {exc}")

    asyncio.create_task(_run())
    return {"job_id": job.job_id, "cache_hit": False, "build_mode": build_mode}


@app.get("/api/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """Poll job progress. Includes CPU/GPU/RAM metrics."""
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    status = job.to_status_dict()
    # Inject live system stats
    sys_stats = get_system_stats()
    status.update(sys_stats)
    return status


@app.get("/api/jobs/{job_id}/result")
async def get_job_result(job_id: str, pip: str = "", max_bricks: int = 10_000):
    """
    Return job result. Bricks are loaded lazily from Parquet (max_bricks limit).
    Use GET /api/renko-window for windowed access during scrolling.
    """
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in ("done",):
        raise HTTPException(status_code=202, detail=f"Job status: {job.status}")

    # Load last N bricks per pip from Parquet (not from RAM)
    charts: Dict[str, Any] = {}
    for pip_str, count in job.bricks_built.items():
        if pip and pip_str != pip:
            continue
        bricks = read_last_n_bricks(job_id=job_id, pip_str=pip_str, n=max_bricks)
        charts[pip_str] = bricks

    return {
        "status":       "done",
        "engine_used":  job.engine_used,
        "ticks_used":   job.ticks_used,
        "rows_scanned": job.rows_scanned,
        "bricks_built": job.bricks_built,
        "charts":       charts,
        "diagnostics":  job.diagnostics,
    }


@app.post("/api/clear-cache")
def clear_cache():
    try:
        import shutil
        from cache import CACHE_DIR
        if CACHE_DIR.exists():
            shutil.rmtree(CACHE_DIR)
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return {"status": "ok", "message": "Backend cache cleared successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/renko-window")
def get_renko_window(
    job_id:    str,
    chart_id:  str,
    from_x:    int = None,
    to_x:      int = None,
    max_bricks: int = 10_000,
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


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running build job gracefully."""
    ok = job_manager.cancel_job(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": "cancelled"}


@app.get("/api/jobs")
async def list_jobs():
    """List all jobs (for debugging)."""
    return job_manager.list_jobs()


@app.get("/api/system-stats")
async def system_stats():
    """Real-time CPU/GPU/RAM snapshot. No fake values."""
    stats = get_system_stats()
    stats["engine_status"] = _ENGINE_STATUS
    return stats


@app.websocket("/ws/jobs/{job_id}")
async def ws_job_progress(websocket: WebSocket, job_id: str):
    """
    Stream live progress for a build job.
    Sends JSON frames:  { type: "progress"|"log"|"done"|"error", ... }
    Also pushes system-stats updates every 2 seconds.
    """
    await websocket.accept()
    job = job_manager.get_job(job_id)
    if not job:
        await websocket.send_text(_fast_dumps({"type": "error", "message": "Job not found"}))
        await websocket.close()
        return

    # If job already done, send result immediately and close
    if job.status == "done":
        await websocket.send_text(_fast_dumps({"type": "done", **job.to_status_dict()}))
        await websocket.close()
        return

    if job.status == "error":
        await websocket.send_text(_fast_dumps({"type": "error", **job.to_status_dict()}))
        await websocket.close()
        return

    q = job.add_subscriber()
    stats_task = None

    async def _push_stats():
        while True:
            await asyncio.sleep(2)
            try:
                stats = get_system_stats()
                await websocket.send_text(_fast_dumps({"type": "system_stats", **stats}))
            except Exception:
                break

    try:
        stats_task = asyncio.create_task(_push_stats())
        while True:
            try:
                item = await asyncio.wait_for(q.get(), timeout=60.0)
            except asyncio.TimeoutError:
                # Send heartbeat to keep connection alive
                await websocket.send_text(_fast_dumps({"type": "heartbeat"}))
                continue
            if item is SENTINEL:
                break
            await websocket.send_text(_fast_dumps(item))
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


# Mount Frontend static files
frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
logger.info(f"Frontend dir: {frontend_dir}, exists: {frontend_dir.exists()}")
if frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")
else:
    logger.warning(f"Frontend directory not found at {frontend_dir.resolve()}. Static mounting skipped.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=5006, reload=True)
