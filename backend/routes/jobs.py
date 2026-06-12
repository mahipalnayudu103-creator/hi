import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional
import pandas as pd
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException

try:
    import orjson
    _USE_ORJSON = True
except ImportError:
    _USE_ORJSON = False

try:
    import msgspec.msgpack as _msgpack
    _USE_MSGPACK_BINARY = True
except ImportError:
    _USE_MSGPACK_BINARY = False

from models.schemas import JobBuildRequest
from services.csv.metadata import resolve_csv_path, summarize_csv_file
from services.jobs.manager import job_manager, SENTINEL
from services.jobs.pipeline import run_build_pipeline
from services.cache.parquet_store import (
    read_window as parquet_read_window,
    read_last_n_bricks,
    read_legacy_pip_cache_window,
    check_legacy_cache_per_pip_counts,
    get_job_meta,
)
from services.cache.keys import get_cache_key
from services.renko.gpu_engine import detect_cupy_available
from utils.downsample import maybe_downsample
from utils.monitor import get_system_stats
from utils.parquet_meta_cache import (
    read_window_cached as _pmeta_read_window,
    invalidate_job as _pmeta_invalidate_job,
)
from config import CACHE_DIR

logger = logging.getLogger("renko_playback.routes.jobs")
router = APIRouter()

RENKO_METHOD_LABEL = "cTrader body v2"
RENKO_METHOD_CACHE_VARIANT = "ctrader_body_v2"


def _fast_dumps(obj: Any) -> str:
    if _USE_ORJSON:
        return orjson.dumps(obj).decode("utf-8")
    return json.dumps(obj)


def _pack(obj: Any) -> bytes:
    if _USE_MSGPACK_BINARY:
        return _msgpack.encode(obj)
    if _USE_ORJSON:
        return orjson.dumps(obj)
    return json.dumps(obj).encode("utf-8")


async def _ws_send(websocket, obj: Any) -> None:
    if _USE_MSGPACK_BINARY:
        await websocket.send_bytes(_pack(obj))
    else:
        await websocket.send_text(_fast_dumps(obj))


@router.post("/api/jobs/build-renko")
async def post_build_renko_job(request: JobBuildRequest):
    """Submit a Renko build job. Returns job_id immediately."""
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

    logger.info(f"BUILD REQUEST: csv={csv_path.name}, start_t={start_t}, end_t={end_t}")

    build_mode = (request.build_mode or "full").strip().lower()
    if build_mode not in {"full", "preview", "cache_only"}:
        raise HTTPException(status_code=400, detail=f"Invalid build mode: {request.build_mode}")

    max_rows = None
    cache_variant = RENKO_METHOD_CACHE_VARIANT
    if build_mode == "preview":
        max_rows = max(1_000, int(request.max_preview_ticks or 50_000))
        cache_variant = f"{RENKO_METHOD_CACHE_VARIANT}_preview_{max_rows}"

    base_cache_key = get_cache_key(
        csv_path, request.start_utc, request.end_utc,
        request.price_source, request.reversal_boxes,
        request.pip_size, request.anchor, [],
        cache_variant=cache_variant,
    )
    cache_key = get_cache_key(
        csv_path, request.start_utc, request.end_utc,
        request.price_source, request.reversal_boxes,
        request.pip_size, request.anchor, request.chart_pips,
        cache_variant=cache_variant,
    )

    per_pip_counts = check_legacy_cache_per_pip_counts(base_cache_key, request.chart_pips)
    cached_pip_counts = {k: v for k, v in per_pip_counts.items() if v is not None}
    cached_pips = {}
    missing_pips = [pip for pip in request.chart_pips if per_pip_counts.get(str(pip)) is None]

    if not missing_pips:
        job = job_manager.create_job()
        job.status        = "done"
        job.progress_percent = 100.0
        job.result_charts = {k: int(v) for k, v in cached_pip_counts.items()}
        job.bricks_built  = job.result_charts
        job.engine_used   = f"Cache (PyArrow Parquet) + {RENKO_METHOD_LABEL}"
        job._base_cache_key = base_cache_key
        job._partial_cached_pip_counts = cached_pip_counts
        job.completed_at = time.time()
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

    async def _run():
        build_pips = missing_pips if missing_pips else request.chart_pips
        gpu_success = False
        if use_gpu:
            try:
                job.status = "running"
                job.stage = "gpu_build"
                job.log("GPU streaming engine requested. Processing CSV in chunks...")

                from services.jobs.pipeline import run_gpu_streaming_pipeline
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

                if cached_pip_counts:
                    for k, v in cached_pip_counts.items():
                        if k not in job.bricks_built:
                            job.bricks_built[k] = int(v)
                    if job.result_charts:
                        for k, v in cached_pip_counts.items():
                            if k not in job.result_charts:
                                job.result_charts[k] = int(v)

                gpu_success = True
                job.log("GPU streaming build done.")
            except Exception as gpu_exc:
                job.log(f"GPU streaming failed: {gpu_exc}. Falling back to CPU streaming...")
                logger.warning(f"GPU streaming failed: {gpu_exc}")

        if not gpu_success:
            try:
                _build_pips_cpu = missing_pips if missing_pips else request.chart_pips
                if cached_pip_counts:
                    job.log(f"Partial cache: reusing {list(cached_pip_counts.keys())}, building {_build_pips_cpu}")
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
                    cache_key      = base_cache_key,
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
    """Paginated viewport slice."""
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
    """Return job result. Bricks are loaded lazily from Parquet."""
    job = job_manager.get_job(job_id)
    max_bricks = max(1_000, min(int(max_bricks or 20_000), 50_000))
    
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
            logger.warning(f"Error filtering bricks: {filter_exc}")
            return bricks_list

    if not job:
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
                "diagnostics":  meta.get("diagnostics", ["Loaded from persistent Parquet storage cache."]),
            }
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in ("done",):
        raise HTTPException(status_code=202, detail=f"Job status: {job.status}")

    _cached_pips_data: dict = getattr(job, "_partial_cached_pips", {}) or {}
    _cached_pip_counts: dict = getattr(job, "_partial_cached_pip_counts", {}) or {}
    _base_cache_key: str = getattr(job, "_base_cache_key", "") or ""

    charts: Dict[str, Any] = {}
    for pip_str, count in job.bricks_built.items():
        if pip and pip_str != pip:
            continue
        if pip_str in _cached_pips_data:
            bricks = _cached_pips_data[pip_str]
            bricks = _filter_bricks(bricks)
            if len(bricks) > max_bricks:
                bricks = bricks[-max_bricks:]
            charts[pip_str] = bricks
        elif pip_str in _cached_pip_counts and _base_cache_key:
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

    if downsample:
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


@router.get("/api/renko-window")
def get_renko_window(
    job_id:    str,
    chart_id:  str,
    from_x:    int = None,
    to_x:      int = None,
    max_bricks: int = 1_000_000_000,
):
    """Lazy-load a window of Renko bricks from Parquet."""
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
    ok = job_manager.cancel_job(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": "cancelled"}


@router.get("/api/jobs")
async def list_jobs():
    return job_manager.list_jobs()


@router.websocket("/ws/jobs/{job_id}")
async def ws_job_progress(websocket: WebSocket, job_id: str):
    await websocket.accept()
    job = job_manager.get_job(job_id)
    if not job:
        await _ws_send(websocket, {"type": "error", "message": "Job not found"})
        await websocket.close()
        return

    if job.status == "done":
        await _ws_send(websocket, {"type": "done", **job.to_status_dict()})
        await websocket.close()
        return

    if job.status == "error":
        await _ws_send(websocket, {"type": "error", **job.to_status_dict()})
        await websocket.close()
        return

    q = job.add_subscriber()
    stats_task = None

    async def _push_stats():
        while True:
            await asyncio.sleep(2)
            try:
                stats = get_system_stats()
                await _ws_send(websocket, {"type": "system_stats", **stats})
            except Exception:
                break

    try:
        stats_task = asyncio.create_task(_push_stats())
        while True:
            try:
                item = await asyncio.wait_for(q.get(), timeout=60.0)
            except asyncio.TimeoutError:
                await _ws_send(websocket, {"type": "heartbeat"})
                continue
            if item is SENTINEL:
                break
            await _ws_send(websocket, item)
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
