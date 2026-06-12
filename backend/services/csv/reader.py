import csv
import io
import gzip
import zipfile
import numpy as np
import pandas as pd
import polars as pl
from pathlib import Path
from io import BytesIO
from typing import Tuple, List, Any

# Import metadata helpers from the sibling module
from services.csv.metadata import (
    extract_base_year,
    parse_csv_time_value,
    normalize_polars_time_col,
    normalize_pandas_time_col,
)

DEFAULT_DELIMITER = ","

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
        try:
            return path.open(mode, buffering=4 * 1024 * 1024, encoding=encoding)
        except Exception:
            return path.open(mode, encoding=encoding)


def csv_row_values(line: str, delimiter: str = DEFAULT_DELIMITER) -> List[str]:
    # fast split by delimiter if no quotes
    if '"' not in line:
        return line.split(delimiter)
    reader = csv.reader([line], delimiter=delimiter)
    try:
        return next(reader)
    except StopIteration:
        return []


def read_header_columns(path: Path, delimiter: str = DEFAULT_DELIMITER) -> List[str]:
    with open_compressed_file(path, "rt", encoding="utf-8-sig") as f:
        first_line = f.readline()
        if not first_line:
            return []
        return csv_row_values(first_line.strip(), delimiter)


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
