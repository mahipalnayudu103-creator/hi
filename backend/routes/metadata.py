import logging
import pandas as pd
from fastapi import APIRouter, HTTPException

from models.schemas import MetadataRequest, MetadataResponse
from services.csv.metadata import (
    resolve_csv_path,
    summarize_csv_file,
    detect_pip_size,
)

logger = logging.getLogger("renko_playback.routes.metadata")
router = APIRouter()


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
