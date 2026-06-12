import csv
import math
import os
import io
import gzip
import zipfile
from pathlib import Path
from io import BytesIO
from typing import Any, Tuple, List
import numpy as np
import pandas as pd
import polars as pl

DEFAULT_DELIMITER = ","
EXACT_ROW_COUNT_MAX_BYTES = 200 * 1024 * 1024

class CompressedFileWrapper:
    def __init__(self, fh, uncompressed_size, archive=None):
        self.fh = fh
        self.uncompressed_size = uncompressed_size
        self.archive = archive

    def read(self, *args, **kwargs):
        return self.fh.read(*args, **kwargs)

    def readline(self, *args, **kwargs):
        return self.fh.readline(*args, **kwargs)

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_END:
            target = self.uncompressed_size + offset
            return self.fh.seek(target, io.SEEK_SET)
        return self.fh.seek(offset, whence)

    def tell(self):
        return self.fh.tell()

    def close(self):
        self.fh.close()
        if self.archive:
            self.archive.close()

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def flush(self):
        pass

    @property
    def closed(self):
        return self.fh.closed

    def __iter__(self):
        return self

    def __next__(self):
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def get_compressed_uncompressed_size(path: Path) -> int:
    path_str = str(path).lower()
    if path_str.endswith(".gz"):
        try:
            with open(path, "rb") as f:
                f.seek(-4, io.SEEK_END)
                return int.from_bytes(f.read(4), "little")
        except Exception:
            return 0
    elif path_str.endswith(".zip"):
        try:
            with zipfile.ZipFile(path, "r") as archive:
                info = archive.infolist()[0]
                return info.file_size
        except Exception:
            return 0
    return path.stat().st_size

def get_file_size(path: Path) -> int:
    path_str = str(path).lower()
    if path_str.endswith((".gz", ".zip")):
        return get_compressed_uncompressed_size(path)
    return path.stat().st_size

def open_compressed_file(path: Path, mode: str = "rb", encoding: str = None):
    path_str = str(path).lower()
    is_text = "t" in mode or "b" not in mode
    
    if path_str.endswith(".gz"):
        uncompressed_size = get_compressed_uncompressed_size(path)
        binary_fh = gzip.open(path, "rb")
        wrapped = CompressedFileWrapper(binary_fh, uncompressed_size)
        if is_text:
            return io.TextIOWrapper(wrapped, encoding=encoding or "utf-8-sig", errors="replace")
        return wrapped
    elif path_str.endswith(".zip"):
        uncompressed_size = get_compressed_uncompressed_size(path)
        archive = zipfile.ZipFile(path, "r")
        first_file = archive.namelist()[0]
        binary_fh = archive.open(first_file, "r")
        wrapped = CompressedFileWrapper(binary_fh, uncompressed_size, archive)
        if is_text:
            return io.TextIOWrapper(wrapped, encoding=encoding or "utf-8-sig", errors="replace")
        return wrapped
    else:
        # 4MB buffer for maximum SSD sequential reading throughput
        try:
            return path.open(mode, buffering=4 * 1024 * 1024, encoding=encoding)
        except Exception:
            return path.open(mode, encoding=encoding)

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
    
    # Check relative to CWD first
    cwd_resolved = expanded.resolve()
    if cwd_resolved.exists():
        return cwd_resolved
        
    # Fallback to project root directory (three levels up from backend/utils/csv_reader.py)
    project_root = Path(__file__).parent.parent.parent.resolve()
    root_resolved = (project_root / expanded).resolve()
    if root_resolved.exists():
        return root_resolved
        
    return cwd_resolved

def detect_csv_delimiter(path: Path) -> str:
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
    stat = path.stat()
    return get_file_size(path), stat.st_mtime_ns

def count_csv_data_rows(path: Path) -> int:
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

def read_last_nonempty_line(path: Path, chunk_size: int = 8192) -> str:
    with open_compressed_file(path, "rb") as handle:
        handle.seek(0, io.SEEK_END)
        position = handle.tell()
        buffer = b""
        while position > 0:
            read_size = min(chunk_size, position)
            position -= read_size
            handle.seek(position)
            buffer = handle.read(read_size) + buffer
            lines = [line for line in buffer.splitlines() if line.strip()]
            if len(lines) >= 2 or position == 0:
                return lines[-1].decode("utf-8", errors="replace") if lines else ""
    return ""

def parse_csv_time_value(value: Any, base_year: int = 2026) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    raw_value = str(value).strip()
    if not raw_value:
        return None
    
    res = None
    
    # Check if MM:SS.f format
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
        # Check if numeric epoch
        try:
            val = float(raw_value)
            if 1e9 <= val < 5e9:  # seconds (2026 is ~1.76e9)
                res = pd.to_datetime(val, unit='s', utc=True)
            elif 1e12 <= val < 5e12:  # ms
                res = pd.to_datetime(val, unit='ms', utc=True)
            elif 1e15 <= val < 5e15:  # us
                res = pd.to_datetime(val, unit='us', utc=True)
            elif 1e18 <= val < 5e18:  # ns
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
                # MM:SS format! Reconstruct timestamps.
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
                # MM:SS format!
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

def csv_row_values(line: str, delimiter: str = DEFAULT_DELIMITER) -> List[str]:
    try:
        return next(csv.reader([line], delimiter=delimiter))
    except Exception:
        return []

def read_header_columns(path: Path, delimiter: str = DEFAULT_DELIMITER) -> List[str]:
    with open_compressed_file(path, "r", encoding="utf-8-sig") as handle:
        return next(csv.reader(handle, delimiter=delimiter), [])

def quick_csv_summary_from_preview(path: Path, preview: pd.DataFrame, time_col: str, delimiter: str) -> dict[str, Any]:
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

        # Check for wrapping timestamps
        with open_compressed_file(path, "r", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter)
            first_row = next(reader, None)

        is_wrapping = False
        base_year = extract_base_year(path)

        if first_row and time_col in first_row:
            first_val = str(first_row[time_col]).strip()
            if first_val.count(':') == 1 and '-' not in first_val and '/' not in first_val and ' ' not in first_val:
                is_wrapping = True

        if is_wrapping:
            summary["is_wrapping"] = True
            # We must sequentially process the time column to get the correct last time!
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

def line_at_or_after(handle, offset: int, data_start: int) -> Tuple[int, int, str]:
    if offset <= data_start:
        handle.seek(data_start)
    else:
        handle.seek(offset)
        handle.readline()

    line_start = handle.tell()
    raw = handle.readline()
    line_end = handle.tell()
    if not raw:
        return line_start, line_end, ""
    return line_start, line_end, raw.decode("utf-8", errors="replace").strip()

def seek_first_timestamp_offset(path: Path, target: pd.Timestamp, data_start: int, time_index: int, delimiter: str) -> int:
    file_size = get_file_size(path)
    low = data_start
    high = file_size

    # Ensure target is timezone-naive UTC for safe comparison
    target_naive = target.tz_convert("UTC").tz_localize(None) if target.tzinfo is not None else target

    with open_compressed_file(path, "rb") as handle:
        for _ in range(80):
            if high - low <= 256:
                break
            midpoint = (low + high) // 2
            line_start, line_end, line = line_at_or_after(handle, midpoint, data_start)
            if not line:
                high = midpoint
                continue

            values = csv_row_values(line, delimiter)
            row_time = parse_csv_time_value(values[time_index]) if len(values) > time_index else None
            # Both row_time and target_naive are now timezone-naive UTC
            if row_time is None or row_time < target_naive:
                low = max(line_end, midpoint + 1)
            else:
                high = line_start if line_start < high else midpoint

        aligned_start, _, _ = line_at_or_after(handle, low, data_start)
        return max(data_start, aligned_start)

def read_selected_range_duckdb(
    path: Path,
    delimiter: str,
    time_col: str,
    source: str,
    bid_col: str | None,
    ask_col: str | None,
    start_t: pd.Timestamp,
    end_t: pd.Timestamp,
    max_rows: int | None,
) -> tuple:
    import duckdb
    import numpy as np

    start_str = start_t.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
    end_str   = end_t.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
    sep_char  = delimiter.replace("'", "''")

    if source == "__mid__":
        if not bid_col or not ask_col:
            raise ValueError("Mid price requires both bid and ask columns.")
        price_expr = f'(TRY_CAST("{bid_col}" AS DOUBLE) + TRY_CAST("{ask_col}" AS DOUBLE)) / 2.0'
    else:
        price_expr = f'TRY_CAST("{source}" AS DOUBLE)'

    con = duckdb.connect(database=":memory:")
    try:
        sql = f"""
            SELECT
                CAST(strptime(CAST("{time_col}" AS VARCHAR),
                    '%Y-%m-%d %H:%M:%S.%f') AS VARCHAR) AS _ts,
                {price_expr} AS _price
            FROM read_csv_auto(
                '{str(path).replace(chr(92), "/")}',
                sep='{sep_char}',
                header=true,
                ignore_errors=true,
                parallel=true
            )
            WHERE TRY_CAST("{time_col}" AS TIMESTAMP) >= TIMESTAMP '{start_str}'
              AND TRY_CAST("{time_col}" AS TIMESTAMP) <= TIMESTAMP '{end_str}'
              AND {price_expr} IS NOT NULL
              AND {price_expr} != 'NaN'
              AND {price_expr} IS FINITE
            {f'LIMIT {max_rows}' if max_rows is not None else ''}
        """
        rel = con.execute(sql)
        result = rel.fetchall()
    finally:
        con.close()

    if not result:
        return np.array([], dtype=np.float64), np.array([], dtype=object), 0, 0, "CPU DuckDB"

    rows = len(result)
    times_raw = [r[0] if r[0] else "" for r in result]
    prices_raw = [r[1] for r in result]

    times_np = np.array(
        [t[:23] if len(t) > 23 else t for t in times_raw], dtype=object
    )
    prices_np = np.array(prices_raw, dtype=np.float64)

    valid = np.isfinite(prices_np)
    return prices_np[valid], times_np[valid], rows, int(valid.sum()), "CPU DuckDB"

def read_selected_range_cpu(
    path: Path,
    delimiter: str,
    time_col: str,
    source: str,
    bid_col: str | None,
    ask_col: str | None,
    start_t: pd.Timestamp,
    end_t: pd.Timestamp,
    max_rows: int | None,
) -> Tuple[np.ndarray, np.ndarray, int, int, str, np.ndarray, np.ndarray]:
    usecols = [time_col]
    if source == "__mid__":
        if not bid_col or not ask_col:
            raise ValueError("Mid price requires both bid and ask columns.")
        usecols.extend([bid_col, ask_col])
    else:
        usecols.append(source)
    usecols = list(dict.fromkeys(usecols))
    
    prices_list = []
    times_list = []
    bids_list = []
    asks_list = []
    rows_scanned = 0
    rows_loaded = 0
    
    try:
        columns = read_header_columns(path, delimiter)
        time_index = columns.index(time_col)
        with open_compressed_file(path, "rb") as handle:
            header = handle.readline()
            data_start = handle.tell()
            first_data_line = handle.readline()
        
        is_wrapping = False
        if first_data_line:
            row_vals = csv_row_values(first_data_line.decode("utf-8", errors="replace").strip(), delimiter)
            if time_index < len(row_vals):
                first_val = str(row_vals[time_index]).strip()
                if first_val.count(':') == 1 and '-' not in first_val and '/' not in first_val and ' ' not in first_val:
                    is_wrapping = True

        base_year = extract_base_year(path)
        
        if is_wrapping:
            offset = data_start
        else:
            offset = seek_first_timestamp_offset(path, start_t, data_start, time_index, delimiter)
        
        chunk_size_bytes = 100 * 1024 * 1024
        
        start_naive = start_t.tz_convert("UTC").tz_localize(None) if start_t.tzinfo is not None else start_t
        end_naive = end_t.tz_convert("UTC").tz_localize(None) if end_t.tzinfo is not None else end_t
        
        state = {"last_min": 0, "cum_hours": 0} if is_wrapping else None
        
        with open_compressed_file(path, "rb") as handle:
            handle.seek(offset)
            while True:
                chunk_data = handle.read(chunk_size_bytes)
                if not chunk_data:
                    break
                last_newline = chunk_data.rfind(b"\n")
                if last_newline == -1:
                    aligned_data = chunk_data
                else:
                    aligned_data = chunk_data[:last_newline + 1]
                    back_seek = len(chunk_data) - (last_newline + 1)
                    handle.seek(handle.tell() - back_seek)
                
                df = pl.read_csv(
                    BytesIO(header + aligned_data),
                    separator=delimiter,
                    columns=usecols,
                    infer_schema=True,
                    try_parse_dates=True,
                )
                if df.is_empty():
                    continue
                    
                rows_scanned += len(df)
                
                df = normalize_polars_time_col(df, time_col, state, base_year)
                
                df = df.filter(pl.col(time_col).is_not_null())
                if df.is_empty():
                    continue
                
                df_time_col = pl.col(time_col)
                if df[time_col].dtype.time_zone is not None:
                    df_time_col = df_time_col.dt.replace_time_zone(None)
                    
                df_filtered = df.filter((df_time_col >= start_naive) & (df_time_col <= end_naive))
                if df_filtered.is_empty():
                    if is_wrapping:
                        continue
                    min_chunk_t = df.select(df_time_col.min()).item()
                    if min_chunk_t is not None and min_chunk_t > end_naive:
                        break
                    continue
                
                if source == "__mid__":
                    df_filtered = df_filtered.with_columns(
                        ((pl.col(bid_col).cast(pl.Float64, strict=False) + 
                          pl.col(ask_col).cast(pl.Float64, strict=False)) / 2.0).alias("price")
                    )
                else:
                    df_filtered = df_filtered.with_columns(
                        pl.col(source).cast(pl.Float64, strict=False).alias("price")
                    )
                    
                df_filtered = df_filtered.filter(pl.col("price").is_finite() & pl.col("price").is_not_null())
                if df_filtered.is_empty():
                    continue
                    
                p_arr = df_filtered["price"].to_numpy()
                t_arr = df_filtered[time_col].dt.strftime("%Y-%m-%d %H:%M:%S.%3f").to_numpy()
                
                if bid_col and bid_col in df_filtered.columns:
                    b_arr = df_filtered[bid_col].cast(pl.Float64, strict=False).to_numpy()
                else:
                    b_arr = p_arr
                    
                if ask_col and ask_col in df_filtered.columns:
                    a_arr = df_filtered[ask_col].cast(pl.Float64, strict=False).to_numpy()
                else:
                    a_arr = p_arr

                prices_list.append(p_arr)
                times_list.append(t_arr)
                bids_list.append(b_arr)
                asks_list.append(a_arr)
                rows_loaded += len(p_arr)
                
                if max_rows is not None and rows_loaded >= max_rows:
                    break
                    
        engine_name = "CPU Polars"
        
    except Exception as exc:
        start_t_naive = start_t.tz_convert("UTC").tz_localize(None) if start_t.tzinfo is not None else start_t
        end_t_naive = end_t.tz_convert("UTC").tz_localize(None) if end_t.tzinfo is not None else end_t

        prices_list = []
        times_list = []
        bids_list = []
        asks_list = []
        rows_scanned = 0
        rows_loaded = 0
        
        columns = read_header_columns(path, delimiter)
        time_index = columns.index(time_col)
        with open_compressed_file(path, "rb") as handle:
            header = handle.readline()
            data_start = handle.tell()
            first_data_line = handle.readline()
        
        is_wrapping = False
        if first_data_line:
            row_vals = csv_row_values(first_data_line.decode("utf-8", errors="replace").strip(), delimiter)
            if time_index < len(row_vals):
                first_val = str(row_vals[time_index]).strip()
                if first_val.count(':') == 1 and '-' not in first_val and '/' not in first_val and ' ' not in first_val:
                    is_wrapping = True

        base_year = extract_base_year(path)
        
        if is_wrapping:
            offset = data_start
        else:
            offset = seek_first_timestamp_offset(path, start_t, data_start, time_index, delimiter)
        
        chunk_size_bytes = 100 * 1024 * 1024
        
        state = {"last_min": 0, "cum_hours": 0} if is_wrapping else None
        
        with open_compressed_file(path, "rb") as handle:
            handle.seek(offset)
            while True:
                chunk_data = handle.read(chunk_size_bytes)
                if not chunk_data:
                    break
                last_newline = chunk_data.rfind(b"\n")
                if last_newline == -1:
                    aligned_data = chunk_data
                else:
                    aligned_data = chunk_data[:last_newline + 1]
                    back_seek = len(chunk_data) - (last_newline + 1)
                    handle.seek(handle.tell() - back_seek)
                    
                df = pd.read_csv(
                    BytesIO(header + aligned_data),
                    sep=delimiter,
                    usecols=usecols,
                )
                if df.empty:
                    continue
                    
                rows_scanned += len(df)
                
                df = normalize_pandas_time_col(df, time_col, state, base_year)
                valid_times = df[time_col].notna()
                if not valid_times.any():
                    continue
                    
                df = df.loc[valid_times]
                parsed_times = df[time_col]
                
                if not is_wrapping and parsed_times.min() > end_t_naive:
                    break
                if not is_wrapping and parsed_times.max() < start_t_naive:
                    continue
                    
                mask = (parsed_times >= start_t_naive) & (parsed_times <= end_t_naive)
                if not mask.any():
                    continue
                    
                sub_df = df.loc[mask]
                sub_times = parsed_times.loc[mask]
                
                if source == "__mid__":
                    raw_prices = (pd.to_numeric(sub_df[bid_col], errors="coerce") + pd.to_numeric(sub_df[ask_col], errors="coerce")) / 2.0
                else:
                    raw_prices = pd.to_numeric(sub_df[source], errors="coerce")
                    
                valid_prices = np.isfinite(raw_prices.to_numpy(dtype=np.float64))
                if not valid_prices.any():
                    continue
                    
                idx_valid = np.flatnonzero(valid_prices)
                p_arr = raw_prices.iloc[idx_valid].to_numpy(dtype=np.float64)
                t_arr = sub_times.iloc[idx_valid].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3].to_numpy(dtype=object)
                
                if bid_col and bid_col in sub_df.columns:
                    b_arr = pd.to_numeric(sub_df.iloc[idx_valid][bid_col], errors="coerce").to_numpy(dtype=np.float64)
                else:
                    b_arr = p_arr
                    
                if ask_col and ask_col in sub_df.columns:
                    a_arr = pd.to_numeric(sub_df.iloc[idx_valid][ask_col], errors="coerce").to_numpy(dtype=np.float64)
                else:
                    a_arr = p_arr

                prices_list.append(p_arr)
                times_list.append(t_arr)
                bids_list.append(b_arr)
                asks_list.append(a_arr)
                rows_loaded += len(p_arr)
                
                if max_rows is not None and rows_loaded >= max_rows:
                    break
        engine_name = "CPU pandas"
        
    if not prices_list:
        empty_price = np.array([], dtype=np.float64)
        empty_time = np.array([], dtype=object)
        return empty_price, empty_time, rows_scanned, 0, engine_name, empty_price, empty_price
        
    return (
        np.concatenate(prices_list),
        np.concatenate(times_list),
        rows_scanned,
        rows_loaded,
        engine_name,
        np.concatenate(bids_list),
        np.concatenate(asks_list)
    )
