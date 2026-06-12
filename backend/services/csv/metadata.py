import csv
import io
import os
import gzip
import zipfile
import numpy as np
import pandas as pd
import polars as pl
from pathlib import Path
from io import BytesIO
from typing import Tuple, List, Any

# Dynamic imports from services.csv.reader are used in functions below to prevent circular dependency

DEFAULT_DELIMITER = ","
EXACT_ROW_COUNT_MAX_BYTES = 200 * 1024 * 1024

def normalize_column_name(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def numeric_sample_count(values: pd.Series, sample_size: int = 100) -> int:
    return int(pd.to_numeric(values.head(sample_size), errors="coerce").notna().sum())


def detect_columns(df: pd.DataFrame) -> Tuple[str, str, str | None]:
    if df.empty or len(df.columns) == 0:
        raise ValueError("CSV has no columns.")

    columns = list(df.columns)
    normalized = {normalize_column_name(col): col for col in columns}

    time_col = None
    direct_time_candidates = (
        "timestamputc",
        "timestamp",
        "datetime",
        "timeutc",
        "dateutc",
        "time",
        "date",
        "gmt",
    )
    for candidate in direct_time_candidates:
        if candidate in normalized:
            time_col = normalized[candidate]
            break

    if time_col is None:
        for col in columns:
            key = normalize_column_name(col)
            if "timestamp" in key or "datetime" in key or key.endswith("time") or key.endswith("date"):
                time_col = col
                break

    if time_col is None:
        time_col = columns[0]

    bid_col = None
    for candidate in ("bid", "bidprice", "close", "price", "last", "value", "mid", "open"):
        if candidate in normalized:
            possible = normalized[candidate]
            if numeric_sample_count(df[possible]) > 0:
                bid_col = possible
                break

    if bid_col is None:
        skip_words = ("volume", "qty", "quantity", "size", "spread")
        for col in columns:
            key = normalize_column_name(col)
            if any(skip in key for skip in skip_words):
                continue
            sample_count = numeric_sample_count(df[col])
            if sample_count >= max(1, min(10, len(df[col].head(100)) // 2)):
                bid_col = col
                break

    if bid_col is None:
        raise ValueError(
            "Could not detect a numeric price column. Expected columns like Bid, Ask, Close, Price, Last, or Mid."
        )

    ask_col = None
    for candidate in ("ask", "askprice", "offer"):
        if candidate in normalized:
            possible = normalized[candidate]
            if numeric_sample_count(df[possible]) > 0:
                ask_col = possible
                break

    if ask_col is None:
        for col in columns:
            key = normalize_column_name(col)
            if key.endswith("ask") or key.endswith("offer"):
                if numeric_sample_count(df[col]) == 0:
                    continue
                ask_col = col
                break

    return time_col, bid_col, ask_col


def detect_pip_size(path: Path, symbol: str | None = None) -> float:
    symbol = (symbol or path.stem).upper()
    if "JPY" in symbol:
        return 0.01
    if "XAU" in symbol or "GOLD" in symbol:
        return 0.1
    if "XAG" in symbol or "SILVER" in symbol:
        return 0.01
    if "BTC" in symbol or "ETH" in symbol:
        return 1.0
    if "US30" in symbol or "DJI" in symbol or "NAS" in symbol or "US100" in symbol:
        return 1.0
    return 0.0001


def extract_base_year(path: Path) -> int:
    import re
    if path:
        year_match = re.search(r'\b(19|20)\d{2}\b', path.name)
        if year_match:
            return int(year_match.group(0))
    return 2026


def resolve_csv_path(raw_path: str) -> Path:
    cleaned = str(raw_path).strip().strip("\"'")
    if not cleaned:
        return Path("")
    expanded = Path(os.path.expandvars(os.path.expanduser(cleaned)))
    if expanded.is_absolute():
        return expanded.resolve()
    
    cwd_resolved = expanded.resolve()
    if cwd_resolved.exists():
        return cwd_resolved
        
    project_root = Path(__file__).parent.parent.parent.resolve()
    root_resolved = (project_root / expanded).resolve()
    if root_resolved.exists():
        return root_resolved
        
    return cwd_resolved


def detect_csv_delimiter(path: Path) -> str:
    from services.csv.reader import get_file_size, open_compressed_file
    if not path.exists() or get_file_size(path) == 0:
        return DEFAULT_DELIMITER

    try:
        with open_compressed_file(path, "r", encoding="utf-8-sig") as handle:
            sample = handle.read(8192)
    except Exception:
        return DEFAULT_DELIMITER

    if not sample.strip():
        return DEFAULT_DELIMITER

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        first_line = sample.splitlines()[0] if sample.splitlines() else sample
        counts = {delimiter: first_line.count(delimiter) for delimiter in [",", ";", "\t", "|"]}
        return max(counts, key=counts.get) if max(counts.values()) > 0 else DEFAULT_DELIMITER


def file_signature(path: Path) -> Tuple[int, int]:
    from services.csv.reader import get_file_size
    stat = path.stat()
    return get_file_size(path), stat.st_mtime_ns


def count_csv_data_rows(path: Path) -> int:
    from services.csv.reader import get_file_size, open_compressed_file
    if not path.exists() or not path.is_file() or get_file_size(path) == 0:
        return 0

    try:
        line_count = 0
        with open_compressed_file(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                line_count += chunk.count(b"\n")
            handle.seek(-1, io.SEEK_END)
            if handle.read(1) != b"\n":
                line_count += 1
    except Exception:
        return 0

    return max(0, line_count - 1)


def estimate_csv_rows_from_head(path: Path, sample_rows: int = 10000) -> int:
    from services.csv.reader import open_compressed_file, get_file_size
    with open_compressed_file(path, "rb") as handle:
        header = handle.readline()
        sampled_rows = 0
        sampled_bytes = 0
        for raw_line in handle:
            if not raw_line.strip():
                continue
            sampled_rows += 1
            sampled_bytes += len(raw_line)
            if sampled_rows >= sample_rows:
                break

    if sampled_rows == 0 or sampled_bytes == 0:
        return 0

    average_row_bytes = sampled_bytes / sampled_rows
    return max(0, int((get_file_size(path) - len(header)) / average_row_bytes))


def parse_csv_time_value(value: Any, base_year: int = 2026) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    raw_value = str(value).strip()
    if not raw_value:
        return None
    
    res = None
    
    if raw_value.count(':') == 1 and '-' not in raw_value and '/' not in raw_value and ' ' not in raw_value:
        parts = raw_value.split(':')
        try:
            mins = int(parts[0])
            secs = float(parts[1])
            base = pd.Timestamp(f"{base_year}-01-01", tz="UTC")
            res = base + pd.Timedelta(minutes=mins, seconds=secs)
        except Exception:
            pass

    if res is None:
        try:
            val = float(raw_value)
            if 1e9 <= val < 5e9:
                res = pd.to_datetime(val, unit='s', utc=True)
            elif 1e12 <= val < 5e12:
                res = pd.to_datetime(val, unit='ms', utc=True)
            elif 1e15 <= val < 5e15:
                res = pd.to_datetime(val, unit='us', utc=True)
            elif 1e18 <= val < 5e18:
                res = pd.to_datetime(val, unit='ns', utc=True)
        except ValueError:
            pass

    if res is None:
        try:
            parsed = pd.Timestamp(raw_value)
            if parsed.tzinfo is None:
                parsed = parsed.tz_localize("UTC")
            else:
                parsed = parsed.tz_convert("UTC")
            res = parsed
        except Exception:
            pass

    if res is None:
        try:
            parsed_fallback = pd.to_datetime(raw_value, errors="coerce", utc=True)
            if not pd.isna(parsed_fallback):
                res = pd.Timestamp(parsed_fallback)
        except Exception:
            pass

    if res is not None:
        if res.tzinfo is not None:
            return res.tz_convert("UTC").tz_localize(None)
        return res
    return None


def normalize_polars_time_col(df: pl.DataFrame, time_col: str, state: dict | None = None, base_year: int = 2026) -> pl.DataFrame:
    if df.is_empty():
        return df
    col_type = df[time_col].dtype
    if col_type.is_numeric():
        non_nulls = df[time_col].drop_nulls()
        if len(non_nulls) > 0:
            val = non_nulls[0]
            if 1e9 <= val < 5e9:
                df = df.with_columns((pl.col(time_col) * 1000).cast(pl.Datetime("ms")))
            elif 1e12 <= val < 5e12:
                df = df.with_columns(pl.col(time_col).cast(pl.Datetime("ms")))
            elif 1e15 <= val < 5e15:
                df = df.with_columns((pl.col(time_col) // 1000).cast(pl.Datetime("ms")))
            elif 1e18 <= val < 5e18:
                df = df.with_columns((pl.col(time_col) // 1000000).cast(pl.Datetime("ms")))
        return df
    
    if col_type == pl.String:
        non_nulls = df[time_col].drop_nulls()
        if len(non_nulls) > 0:
            sample_val = str(non_nulls[0]).strip()
            if sample_val.isdigit():
                df = df.with_columns(pl.col(time_col).cast(pl.Int64, strict=False))
                return normalize_polars_time_col(df, time_col, state, base_year)
            elif sample_val.count(':') == 1 and '-' not in sample_val and '/' not in sample_val and ' ' not in sample_val:
                split_col = pl.col(time_col).str.split(':')
                df = df.with_columns([
                    split_col.list.get(0).cast(pl.Int32).alias('_min'),
                    split_col.list.get(1).cast(pl.Float64).alias('_sec')
                ])
                if state is not None:
                    last_min = state.get("last_min", 0)
                    cum_hours = state.get("cum_hours", 0)
                else:
                    last_min = 0
                    cum_hours = 0
                
                df = df.with_columns(
                    pl.col('_min').diff().fill_null(pl.col('_min') - last_min).alias('_diff')
                )
                df = df.with_columns(
                    pl.when(pl.col('_diff') < 0).then(1).otherwise(0).alias('_wrap')
                )
                df = df.with_columns(
                    (pl.col('_wrap').cum_sum() + cum_hours).alias('_hour')
                )
                df = df.with_columns(
                    ((pl.col('_hour') * 3600 + pl.col('_min') * 60 + pl.col('_sec')) * 1_000_000).cast(pl.Int64).alias('_us')
                )
                base_epoch_us = int(pd.Timestamp(f"{base_year}-01-01").timestamp() * 1_000_000)
                df = df.with_columns(
                    (pl.col('_us') + base_epoch_us).cast(pl.Datetime("us")).alias(time_col)
                )
                
                if state is not None and len(df) > 0:
                    state["last_min"] = int(df["_min"][-1])
                    state["cum_hours"] = int(df["_hour"][-1])
                    
                df = df.drop(['_min', '_sec', '_diff', '_wrap', '_hour', '_us'])
                return df
        df = df.with_columns(pl.col(time_col).str.to_datetime(strict=False))

    if isinstance(df[time_col].dtype, pl.Datetime) and df[time_col].dtype.time_zone is not None:
        df = df.with_columns(pl.col(time_col).dt.convert_time_zone("UTC").dt.replace_time_zone(None))
    return df


def _normalize_pandas_time_col_impl(df: pd.DataFrame, time_col: str, state: dict | None = None, base_year: int = 2026) -> pd.DataFrame:
    if df.empty:
        return df
    s = df[time_col]
    if pd.api.types.is_numeric_dtype(s):
        non_nulls = s.dropna()
        if not non_nulls.empty:
            val = non_nulls.iloc[0]
            if 1e9 <= val < 5e9:
                df[time_col] = pd.to_datetime(s, unit='s', utc=True)
            elif 1e12 <= val < 5e12:
                df[time_col] = pd.to_datetime(s, unit='ms', utc=True)
            elif 1e15 <= val < 5e15:
                df[time_col] = pd.to_datetime(s, unit='us', utc=True)
            elif 1e18 <= val < 5e18:
                df[time_col] = pd.to_datetime(s, unit='ns', utc=True)
        return df

    if pd.api.types.is_string_dtype(s):
        non_nulls = s.dropna()
        if not non_nulls.empty:
            sample_val = str(non_nulls.iloc[0]).strip()
            if sample_val.isdigit():
                df[time_col] = pd.to_numeric(s, errors='coerce')
                return _normalize_pandas_time_col_impl(df, time_col, state, base_year)
            elif sample_val.count(':') == 1 and '-' not in sample_val and '/' not in sample_val and ' ' not in sample_val:
                parts = s.str.split(':', expand=True)
                mins = pd.to_numeric(parts[0], errors='coerce').fillna(0).astype(int)
                secs = pd.to_numeric(parts[1], errors='coerce').fillna(0).astype(float)
                
                if state is not None:
                    last_min = state.get("last_min", 0)
                    cum_hours = state.get("cum_hours", 0)
                else:
                    last_min = 0
                    cum_hours = 0
                
                diffs = mins.diff()
                diffs.iloc[0] = mins.iloc[0] - last_min
                
                wraps = (diffs < 0).astype(int)
                hours = wraps.cumsum() + cum_hours
                
                us = (hours * 3600 + mins * 60 + secs) * 1_000_000
                base_epoch_us = int(pd.Timestamp(f"{base_year}-01-01").timestamp() * 1_000_000)
                df[time_col] = pd.to_datetime(us + base_epoch_us, unit='us', utc=True)
                
                if state is not None:
                    state["last_min"] = int(mins.iloc[-1])
                    state["cum_hours"] = int(hours.iloc[-1])
                return df

    df[time_col] = pd.to_datetime(s, errors='coerce', utc=True)
    return df


def normalize_pandas_time_col(df: pd.DataFrame, time_col: str, state: dict | None = None, base_year: int = 2026) -> pd.DataFrame:
    df = _normalize_pandas_time_col_impl(df, time_col, state, base_year)
    if time_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[time_col]):
        s = df[time_col]
        if s.dt.tz is not None:
            df[time_col] = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def quick_csv_summary_from_preview(path: Path, preview: pd.DataFrame, time_col: str, delimiter: str) -> dict[str, Any]:
    from services.csv.reader import get_file_size, read_last_nonempty_line, csv_row_values
    summary: dict[str, Any] = {
        "path": path,
        "rows": 0,
        "rows_exact": False,
        "delimiter": delimiter,
        "size": get_file_size(path),
        "mtime_ns": path.stat().st_mtime_ns,
        "first_time": None,
        "last_time": None,
        "is_wrapping": False,
        "status": "ok",
        "error": "",
    }

    base_year = extract_base_year(path)
    preview_copy = preview.copy()
    preview_copy = normalize_pandas_time_col(preview_copy, time_col, base_year=base_year)
    valid_preview = preview_copy[time_col].notna()
    if valid_preview.any():
        summary["first_time"] = pd.Timestamp(preview_copy.loc[valid_preview, time_col].iloc[0])

    try:
        last_line = read_last_nonempty_line(path)
        columns = list(preview.columns)
        last_values = csv_row_values(last_line, delimiter)
        last_row = dict(zip(columns, last_values))
        if time_col in last_row:
            summary["last_time"] = parse_csv_time_value(last_row[time_col], base_year)
    except Exception as exc:
        summary["status"] = "partial"
        summary["error"] = str(exc)

    summary["rows"] = estimate_csv_rows_from_head(path)
    return summary


def summarize_csv_file(path: Path) -> dict[str, Any]:
    from services.csv.reader import get_file_size, open_compressed_file, read_last_nonempty_line, csv_row_values
    delimiter = detect_csv_delimiter(path)
    summary: dict[str, Any] = {
        "path": path,
        "rows": 0,
        "rows_exact": True,
        "delimiter": delimiter,
        "size": get_file_size(path),
        "mtime_ns": path.stat().st_mtime_ns,
        "first_time": None,
        "last_time": None,
        "time_col": "",
        "price_col": "",
        "ask_col": "",
        "is_wrapping": False,
        "status": "ok",
        "error": "",
    }

    if summary["size"] <= 0:
        summary["status"] = "empty"
        return summary

    try:
        with open_compressed_file(path, "rt", encoding="utf-8-sig") as f:
            preview = pd.read_csv(f, nrows=25, sep=delimiter)
        if preview.empty:
            summary["status"] = "empty"
            return summary

        time_col, price_col, ask_col = detect_columns(preview)
        summary["time_col"] = time_col
        summary["price_col"] = price_col
        summary["ask_col"] = ask_col or ""

        is_wrapping = False
        base_year = extract_base_year(path)

        with open_compressed_file(path, "r", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter)
            first_row = next(reader, None)

        if first_row and time_col in first_row:
            first_val = str(first_row[time_col]).strip()
            if first_val.count(':') == 1 and '-' not in first_val and '/' not in first_val and ' ' not in first_val:
                is_wrapping = True

        if is_wrapping:
            summary["is_wrapping"] = True
            df_time = pl.read_csv(path, columns=[time_col], separator=delimiter)
            state = {"last_min": 0, "cum_hours": 0}
            df_time = normalize_polars_time_col(df_time, time_col, state, base_year)
            if not df_time.is_empty():
                summary["first_time"] = pd.Timestamp(df_time[time_col][0])
                summary["last_time"] = pd.Timestamp(df_time[time_col][-1])
            summary["rows"] = len(df_time)
            summary["rows_exact"] = True
            return summary

        if summary["size"] > EXACT_ROW_COUNT_MAX_BYTES:
            quick_summary = quick_csv_summary_from_preview(path, preview, time_col, delimiter)
            quick_summary["time_col"] = time_col
            quick_summary["price_col"] = price_col
            quick_summary["ask_col"] = ask_col or ""
            quick_summary["status"] = "large_estimate"
            quick_summary["rows_exact"] = False
            return quick_summary

        summary["rows"] = count_csv_data_rows(path)
        last_line = read_last_nonempty_line(path)

        if first_row and time_col in first_row:
            summary["first_time"] = parse_csv_time_value(first_row[time_col], base_year)

        if last_line:
            with open_compressed_file(path, "r", encoding="utf-8-sig") as handle:
                header = next(csv.reader(handle, delimiter=delimiter), [])
            last_values = csv_row_values(last_line, delimiter)
            last_row = dict(zip(header, last_values))
            if time_col in last_row:
                summary["last_time"] = parse_csv_time_value(last_row[time_col], base_year)

        return summary
    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = str(exc)
        return summary
