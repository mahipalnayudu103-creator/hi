"""
cache.py  —  High-performance Renko brick cache using PyArrow Parquet.

Strategy
--------
* Each unique (csv, range, params) combination maps to a SHA-256 key.
* Bricks are stored as Parquet files via PyArrow (columnar, compressed).
* Parquet is 5-20x smaller than pickle and reads back via memory-mapped I/O.
* Fallback to msgspec MessagePack if PyArrow is unavailable.
"""

import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, List
from config import CACHE_DIR

logger = logging.getLogger("renko_playback.cache")


# ─── Key generation ───────────────────────────────────────────────────────────

def get_cache_key(
    csv_path: Path,
    start_utc: str,
    end_utc: str,
    price_source: str,
    reversal_boxes: int,
    pip_size: float,
    anchor: str,
    chart_pips: List[float],
    cache_variant: str = "",
) -> str:
    """Return a stable SHA-256 hex key for the given build parameters."""
    try:
        stat = csv_path.stat()
        file_size = stat.st_size
        file_mtime = stat.st_mtime_ns
    except Exception:
        file_size = 0
        file_mtime = 0

    pips_str = ",".join(map(str, sorted(chart_pips)))
    key_src = (
        f"{csv_path}|{file_size}|{file_mtime}|"
        f"{start_utc}|{end_utc}|{price_source}|"
        f"{reversal_boxes}|{pip_size}|{anchor}|{pips_str}"
    )
    if cache_variant:
        key_src = f"{key_src}|{cache_variant}"
    return hashlib.sha256(key_src.encode("utf-8")).hexdigest()


# ─── Parquet helpers ─────────────────────────────────────────────────────────

def _bricks_to_arrow_table(charts: dict):
    """Convert charts dict → PyArrow Table (one row per brick) using Polars."""
    import pyarrow as pa
    if not charts:
        schema = pa.schema([
            ("pip", pa.string()), ("time", pa.int64()),
            ("confirm_time", pa.string()), ("open", pa.float64()),
            ("high", pa.float64()), ("low", pa.float64()),
            ("close", pa.float64()), ("direction", pa.string()),
            ("tick_count", pa.int32()),
        ])
        return pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)

    try:
        import polars as pl
        schema = {
            "time": pl.Int64,
            "confirm_time": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "direction": pl.String,
            "tick_count": pl.Int32,
        }
        
        dfs = []
        for pip_str, bricks in charts.items():
            if not bricks:
                continue
            first_b = bricks[0]
            is_dict = isinstance(first_b, dict)
            if not is_dict:
                bricks_dicts = [b.dict() for b in bricks]
            else:
                bricks_dicts = bricks
                
            df = pl.DataFrame(bricks_dicts, schema=schema)
            df = df.with_columns(pl.lit(pip_str).alias("pip"))
            df = df.select(["pip", "time", "confirm_time", "open", "high", "low", "close", "direction", "tick_count"])
            dfs.append(df)

        if not dfs:
            schema = pa.schema([
                ("pip", pa.string()), ("time", pa.int64()),
                ("confirm_time", pa.string()), ("open", pa.float64()),
                ("high", pa.float64()), ("low", pa.float64()),
                ("close", pa.float64()), ("direction", pa.string()),
                ("tick_count", pa.int32()),
            ])
            return pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
            
        final_df = pl.concat(dfs)
        return final_df.to_arrow()
    except Exception as e:
        logger.warning(f"Polars _bricks_to_arrow_table failed: {e}. Falling back to Python loop...")
        rows = []
        for pip_str, bricks in charts.items():
            for b in bricks:
                bdict = b if isinstance(b, dict) else b.dict()
                rows.append({
                    "pip":          pip_str,
                    "time":         int(bdict["time"]),
                    "confirm_time": str(bdict["confirm_time"]),
                    "open":         float(bdict["open"]),
                    "high":         float(bdict["high"]),
                    "low":          float(bdict["low"]),
                    "close":        float(bdict["close"]),
                    "direction":    str(bdict["direction"]),
                    "tick_count":   int(bdict["tick_count"]),
                })

        if not rows:
            schema = pa.schema([
                ("pip", pa.string()), ("time", pa.int64()),
                ("confirm_time", pa.string()), ("open", pa.float64()),
                ("high", pa.float64()), ("low", pa.float64()),
                ("close", pa.float64()), ("direction", pa.string()),
                ("tick_count", pa.int32()),
            ])
            return pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)

        return pa.Table.from_pylist(rows)


def _arrow_table_to_charts(table) -> dict:
    """Reconstruct charts dict from a PyArrow Table."""
    charts = {}
    pip_col          = table.column("pip").to_pylist()
    time_col         = table.column("time").to_pylist()
    confirm_col      = table.column("confirm_time").to_pylist()
    open_col         = table.column("open").to_pylist()
    high_col         = table.column("high").to_pylist()
    low_col          = table.column("low").to_pylist()
    close_col        = table.column("close").to_pylist()
    direction_col    = table.column("direction").to_pylist()
    tick_count_col   = table.column("tick_count").to_pylist()

    for i, pip_str in enumerate(pip_col):
        brick = {
            "time":         time_col[i],
            "confirm_time": confirm_col[i],
            "open":         open_col[i],
            "high":         high_col[i],
            "low":          low_col[i],
            "close":        close_col[i],
            "direction":    direction_col[i],
            "tick_count":   tick_count_col[i],
        }
        charts.setdefault(pip_str, []).append(brick)

    return charts


# ─── msgspec fallback ─────────────────────────────────────────────────────────

def _msgpack_save(path: Path, data: dict):
    import msgspec.msgpack
    path.write_bytes(msgspec.msgpack.encode(data))

def _msgpack_load(path: Path) -> dict | None:
    import msgspec.msgpack
    try:
        raw = path.read_bytes()
        return msgspec.msgpack.decode(raw)
    except Exception:
        return None


# ─── Public API ───────────────────────────────────────────────────────────────

def check_cache(key: str) -> Dict[str, Any] | None:
    """
    Return cached result dict or None.
    Tries Parquet first, then msgspec msgpack, then legacy pickle.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Try Parquet (PyArrow)
    parquet_file = CACHE_DIR / f"{key}.parquet"
    meta_file    = CACHE_DIR / f"{key}.meta.msgpack"

    if parquet_file.exists() and meta_file.exists():
        try:
            # 1. Try Polars (ultra-fast Rust multi-threaded memory-mapped read)
            try:
                import polars as pl
                df = pl.read_parquet(str(parquet_file))
                charts = {}
                unique_pips = df["pip"].unique().to_list()
                for pip_val in unique_pips:
                    pip_str = str(pip_val)
                    group = df.filter(pl.col("pip") == pip_val).drop("pip")
                    charts[pip_str] = group.to_dicts()
            except Exception as pl_exc:
                logger.info(f"Polars cache read failed: {pl_exc}. Falling back to PyArrow...")
                import pyarrow.parquet as pq
                table = pq.read_table(str(parquet_file), memory_map=True)
                charts = _arrow_table_to_charts(table)

            meta = _msgpack_load(meta_file)
            if meta is None:
                raise ValueError("meta file corrupt")

            return {
                "rows_scanned": meta.get("rows_scanned", 0),
                "ticks_loaded": meta.get("ticks_loaded", 0),
                "charts": charts,
                "cache_engine": "PyArrow Parquet",
            }
        except Exception as e:
            logger.warning(f"Parquet cache read failed: {e}")
            try:
                parquet_file.unlink(missing_ok=True)
                meta_file.unlink(missing_ok=True)
            except Exception:
                pass

    # 2. Try msgspec msgpack
    msgpack_file = CACHE_DIR / f"{key}.msgpack"
    if msgpack_file.exists():
        try:
            data = _msgpack_load(msgpack_file)
            if data is not None:
                data["cache_engine"] = "msgspec msgpack"
                return data
        except Exception as e:
            logger.warning(f"msgpack cache read failed: {e}")
            try:
                msgpack_file.unlink(missing_ok=True)
            except Exception:
                pass

    # 3. Legacy pickle fallback (migrate on read)
    import pickle
    pkl_file = CACHE_DIR / f"{key}.pkl"
    if pkl_file.exists():
        try:
            with open(pkl_file, "rb") as f:
                data = pickle.load(f)
            data["cache_engine"] = "pickle (legacy)"
            return data
        except Exception as e:
            logger.warning(f"Pickle cache read failed: {e}")
            try:
                pkl_file.unlink(missing_ok=True)
            except Exception:
                pass

    return None


def save_cache(key: str, data: Dict[str, Any]):
    """
    Persist cache using PyArrow Parquet + msgspec meta.
    Falls back to msgspec msgpack if PyArrow is unavailable.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    charts       = data.get("charts", {})
    rows_scanned = data.get("rows_scanned", 0)
    ticks_loaded = data.get("ticks_loaded", 0)

    meta = {"rows_scanned": rows_scanned, "ticks_loaded": ticks_loaded}

    # 1. Try Parquet
    try:
        import pyarrow.parquet as pq
        table = _bricks_to_arrow_table(charts)
        parquet_file = CACHE_DIR / f"{key}.parquet"
        meta_file    = CACHE_DIR / f"{key}.meta.msgpack"
        pq.write_table(table, str(parquet_file), compression="snappy")
        _msgpack_save(meta_file, meta)
        logger.info(f"Cache saved as Parquet: {parquet_file.name}")
        return
    except Exception as e:
        logger.warning(f"Parquet cache write failed: {e}. Trying msgpack…")

    # 2. Try msgspec msgpack
    try:
        msgpack_file = CACHE_DIR / f"{key}.msgpack"
        import msgspec.msgpack
        # Convert bricks to plain dicts for msgpack
        plain_charts = {}
        for pip_str, bricks in charts.items():
            plain_charts[pip_str] = [
                b if isinstance(b, dict) else b.dict()
                for b in bricks
            ]
        _msgpack_save(msgpack_file, {**meta, "charts": plain_charts})
        logger.info(f"Cache saved as msgpack: {msgpack_file.name}")
        return
    except Exception as e:
        logger.warning(f"msgpack cache write failed: {e}. Cache not saved.")


def delete_cache(key: str):
    """Remove all cache files for a given key."""
    for suffix in (".parquet", ".meta.msgpack", ".msgpack", ".pkl"):
        p = CACHE_DIR / f"{key}{suffix}"
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass


def list_cache_entries() -> List[Dict[str, Any]]:
    """Return a summary of all cached entries."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    seen_keys = set()
    entries   = []
    for f in sorted(CACHE_DIR.iterdir()):
        key = f.name.split(".")[0]
        if key in seen_keys:
            continue
        seen_keys.add(key)
        size = sum(
            (CACHE_DIR / f"{key}{s}").stat().st_size
            for s in (".parquet", ".meta.msgpack", ".msgpack", ".pkl")
            if (CACHE_DIR / f"{key}{s}").exists()
        )
        entries.append({"key": key[:12] + "…", "size_bytes": size})
    return entries
