from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional


class MetadataRequest(BaseModel):
    csv_path: str = Field(..., description="Absolute path to the tick CSV file")


class MetadataResponse(BaseModel):
    path: str
    rows_estimated: int
    size_bytes: int
    file_start_utc: str
    file_end_utc: str
    time_col: str
    bid_col: str
    ask_col: Optional[str] = None
    delimiter: str
    detected_pip_size: float


class RenkoBrick(BaseModel):
    """Confirmed Renko brick metadata. No raw tick data."""
    time:               int      # confirm_tick_index * 1000 + seq_in_tick
    confirm_tick_index: int
    confirm_time:       str
    open:               float
    high:               float
    low:                float
    close:              float
    direction:          str      # "up" | "down"
    tick_count:         int
    brick_size_pips:    float = 0.0
    bid:                float = 0.0
    ask:                float = 0.0


class RenkoRequest(BaseModel):
    """Request model for synchronous (small-file) /api/build-renko endpoint."""
    csv_path:          str
    start_utc:         str
    end_utc:           str
    price_source:      str
    reversal_boxes:    int
    pip_size:          float
    anchor:            str
    chart_pips:        List[float]
    processing_engine: str = "auto"


class RenkoResponse(BaseModel):
    """Response for synchronous /api/build-renko (kept for backward compat)."""
    status:       str
    engine_used:  str
    rows_scanned: int
    ticks_loaded: int
    charts:       Dict[str, List[RenkoBrick]]
    diagnostics:  List[str] = Field(default_factory=list)


class JobBuildRequest(BaseModel):
    """Request model for async streaming /api/jobs/build-renko endpoint."""
    csv_path:          str
    start_utc:         str
    end_utc:           str
    price_source:      str       # "Bid" | "Ask" | "Mid"
    reversal_boxes:    int   = 2
    pip_size:          float = 0.0001
    anchor:            str   = "floor"
    chart_pips:        List[float] = Field(default_factory=lambda: [1.0, 2.0, 3.0, 4.0])
    processing_engine: str   = "auto"
    chunk_rows:        int   = 250_000   # rows per CSV chunk
    chunk_size_mb:     int   = 0         # legacy compat — ignored if chunk_rows set
    build_mode:        str   = "full"    # "full" | "preview" | "cache_only"
    max_preview_ticks: int   = 50_000


class RenkoWindowRequest(BaseModel):
    """Request for lazy-loading a window of bricks for the frontend."""
    job_id:    str
    chart_id:  str          # e.g. "1.0"
    from_x:    Optional[int] = None
    to_x:      Optional[int] = None
    max_bricks: int = 1_000_000_000


class CacheLookupRequest(BaseModel):
    """Request for checking previously completed builds."""
    csv_path:          str
    price_source:      str
    reversal_boxes:    int
    pip_size:          float
    anchor:            str
    chart_pips:        List[float]
    start_utc:         str
    end_utc:           str

