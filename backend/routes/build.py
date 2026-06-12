import logging
import os
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter
import pandas as pd

from models.schemas import RenkoRequest, RenkoResponse, RenkoBrick
from services.csv.metadata import resolve_csv_path, summarize_csv_file
from services.renko.cpu_engine import build_renko_cpu
from services.renko.gpu_engine import (
    detect_cupy_available,
    detect_gpu_polars_available,
    detect_cudf_available,
    read_selected_range_gpu_polars,
    read_selected_range_gpu,
    build_renko_gpu_multi,
)
from services.csv.reader import (
    read_selected_range_cpu,
    read_selected_range_duckdb,
)
from services.cache.keys import get_cache_key, check_cache, save_cache
from services.renko.rules import RENKO_METHOD_LABEL, RENKO_METHOD_CACHE_VARIANT

logger = logging.getLogger("renko_playback.routes.build")
router = APIRouter()


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
                    # Note: read_selected_range_gpu uses offset-based reading, but for full build we seek manually
                    # In sync api.py, it calls read_selected_range_gpu with start/end offset, which were resolved
                    # from seeking. For simplicity, let's fall back to CPU if offsets are needed or load direct.
                    from services.csv.reader import seek_first_timestamp_offset, detect_first_data_line_offset
                    from services.csv.metadata import detect_csv_delimiter
                    delim = detect_csv_delimiter(csv_path)
                    header_cols = summarize_csv_file(csv_path)
                    time_idx = 0
                    bid_idx = 1
                    ask_idx = 2
                    data_offset = detect_first_data_line_offset(csv_path)
                    start_off = seek_first_timestamp_offset(csv_path, start_t, data_offset, time_idx, delim)
                    end_off = seek_first_timestamp_offset(csv_path, end_t, data_offset, time_idx, delim)
                    prices, times, bids, asks, rows_loaded = read_selected_range_gpu(
                        csv_path=str(csv_path),
                        start_offset=start_off,
                        end_offset=end_off,
                        time_index=time_idx,
                        bid_index=bid_idx,
                        ask_index=ask_idx,
                        delimiter=delim,
                    )
                    rows_scanned = rows_loaded
                    loader_engine = "GPU cuDF"
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
            engine_used=engine_used,
            rows_scanned=rows_scanned,
            ticks_loaded=rows_loaded,
            charts=charts,
            diagnostics=diagnostics
        )
    except Exception as exc:
        logger.exception("Unexpected error in build_renko")
        diagnostics.append(f"CRITICAL: {exc}")
        return RenkoResponse(
            status="error",
            engine_used="",
            rows_scanned=0,
            ticks_loaded=0,
            charts={},
            diagnostics=diagnostics
        )
