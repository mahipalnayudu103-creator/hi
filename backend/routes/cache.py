import logging
import shutil
import pandas as pd
from fastapi import APIRouter, HTTPException

from models.schemas import CacheLookupRequest
from services.csv.metadata import resolve_csv_path
from services.cache.build_history import lookup_sub_range, lookup_similar, delete_by_key
from services.renko.rules import RENKO_METHOD_LABEL
from config import CACHE_DIR

logger = logging.getLogger("renko_playback.routes.cache")
router = APIRouter()


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
                        ratio = 1.0
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


@router.post("/api/clear-cache")
def clear_cache():
    try:
        if CACHE_DIR.exists():
            shutil.rmtree(CACHE_DIR)
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return {"status": "ok", "message": "Backend cache cleared successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
