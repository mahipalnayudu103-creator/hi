"""
csv_stream.py — Streaming CSV chunk generator.

stream_ticks() yields one chunk at a time as (prices, times, bids, asks, rows_in_chunk).
It NEVER concatenates chunks — the caller must process and discard each chunk before
requesting the next one.

Supported engines (auto-selected):
  1. Polars  — fast, handles most formats
  2. DuckDB  — SQL fallback
  3. pandas  — last resort

Chunk size is configurable via CSV_CHUNK_ROWS (default 250,000 rows).
"""

import csv as csv_mod
import gc
import logging
import math
import os
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("renko_playback.csv_stream")

# ── Config ────────────────────────────────────────────────────────────────────
CSV_CHUNK_ROWS: int = int(os.environ.get("RENKO_CHUNK_ROWS", "250000"))

# ── Optional libraries ────────────────────────────────────────────────────────
try:
    import polars as pl
    _POLARS_OK = True
except ImportError:
    pl = None          # type: ignore
    _POLARS_OK = False

try:
    import duckdb
    _DUCKDB_OK = True
except ImportError:
    duckdb = None      # type: ignore
    _DUCKDB_OK = False

# Re-use helpers from csv_reader for time normalisation
from csv_reader import (
    normalize_polars_time_col,
    normalize_pandas_time_col,
    seek_first_timestamp_offset,
    read_header_columns,
    csv_row_values,
    extract_base_year,
)

# ── Chunk type alias ──────────────────────────────────────────────────────────
# Each chunk yielded: (prices, times, bids, asks, row_count)
Chunk = Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, int]


def stream_ticks(
    csv_path:     Path,
    delimiter:    str,
    time_col:     str,
    source:       str,          # column name or "__mid__"
    bid_col:      Optional[str],
    ask_col:      Optional[str],
    start_t:      pd.Timestamp,
    end_t:        pd.Timestamp,
    chunk_rows:   int = CSV_CHUNK_ROWS,
) -> Generator[Chunk, None, None]:
    """
    Generator: yields one chunk at a time as (prices, times, bids, asks, nrows).

    The caller should:
      1. Process the chunk immediately.
      2. Delete references to allow GC before requesting the next chunk.

    Never loads the whole file. Stops when end_t is passed.
    """
    # Detect wrapping timestamps (MM:SS format)
    columns    = read_header_columns(csv_path, delimiter)
    time_index = columns.index(time_col) if time_col in columns else 0
    is_wrapping = False
    base_year   = extract_base_year(csv_path)

    with csv_path.open("rb") as fh:
        header_bytes = fh.readline()
        first_line   = fh.readline()
    if first_line:
        row_vals = csv_row_values(
            first_line.decode("utf-8", errors="replace").strip(), delimiter
        )
        if time_index < len(row_vals):
            first_val = str(row_vals[time_index]).strip()
            if (
                first_val.count(":") == 1
                and "-" not in first_val
                and "/" not in first_val
                and " " not in first_val
            ):
                is_wrapping = True

    # Use Polars primary, DuckDB fallback, pandas last resort
    use_polars = _POLARS_OK
    use_duckdb = _DUCKDB_OK and not is_wrapping

    # Find binary start offset via bisect (skip for wrapping timestamps)
    data_start: int
    with csv_path.open("rb") as fh:
        fh.readline()
        data_start = fh.tell()

    if is_wrapping:
        start_offset = data_start
    else:
        start_offset = seek_first_timestamp_offset(
            csv_path, start_t, data_start, time_index, delimiter
        )

    start_naive = start_t.tz_localize(None) if start_t.tzinfo else start_t
    end_naive   = end_t.tz_localize(None)   if end_t.tzinfo   else end_t

    # Estimate bytes per row for chunk_bytes calculation
    approx_bytes_per_row = max(30, (csv_path.stat().st_size - data_start) // max(1, _estimate_total_rows(csv_path, data_start)))
    chunk_bytes = chunk_rows * approx_bytes_per_row

    state = {"last_min": 0, "cum_hours": 0} if is_wrapping else None

    with csv_path.open("rb") as fh:
        fh.seek(start_offset)

        while True:
            raw_chunk = fh.read(chunk_bytes)
            if not raw_chunk:
                break

            # Align to last newline so we don't split a row
            last_nl = raw_chunk.rfind(b"\n")
            if last_nl == -1:
                aligned = raw_chunk
            else:
                aligned = raw_chunk[: last_nl + 1]
                back    = len(raw_chunk) - (last_nl + 1)
                if back:
                    fh.seek(fh.tell() - back)

            if not aligned:
                continue

            chunk_result: Optional[Chunk] = None

            # ── Try Polars ─────────────────────────────────────────────────
            if use_polars and chunk_result is None:
                try:
                    chunk_result = _process_chunk_polars(
                        header_bytes, aligned, delimiter,
                        time_col, source, bid_col, ask_col,
                        start_naive, end_naive, state, base_year
                    )
                except Exception as exc:
                    logger.warning(f"Polars chunk failed: {exc}. Trying DuckDB.")
                    use_polars = False  # disable for subsequent chunks

            # ── Try DuckDB ─────────────────────────────────────────────────
            if use_duckdb and chunk_result is None:
                try:
                    chunk_result = _process_chunk_duckdb(
                        header_bytes, aligned, delimiter,
                        time_col, source, bid_col, ask_col,
                        start_naive, end_naive
                    )
                except Exception as exc:
                    logger.warning(f"DuckDB chunk failed: {exc}. Trying pandas.")
                    use_duckdb = False

            # ── pandas last resort ──────────────────────────────────────────
            if chunk_result is None:
                try:
                    chunk_result = _process_chunk_pandas(
                        header_bytes, aligned, delimiter,
                        time_col, source, bid_col, ask_col,
                        start_t, end_t, state, base_year
                    )
                except Exception as exc:
                    logger.error(f"All chunk parsers failed: {exc}")
                    continue

            if chunk_result is None:
                continue

            prices, times, bids, asks, nrows = chunk_result

            # Stop if we've passed end_t
            if len(times) > 0:
                # Check last time string for early exit (non-wrapping only)
                if not is_wrapping and len(times) > 0:
                    last_time_str = str(times[-1])
                    try:
                        last_t = pd.Timestamp(last_time_str)
                        if last_t >= end_naive:
                            # Still yield this chunk (it may contain valid rows up to end_t)
                            if nrows > 0:
                                yield prices, times, bids, asks, nrows
                            break
                    except Exception:
                        pass

                if nrows > 0:
                    yield prices, times, bids, asks, nrows

            # Explicitly delete chunk arrays to allow GC
            del prices, times, bids, asks, chunk_result
            gc.collect()


# ── Chunk processors ──────────────────────────────────────────────────────────

def _process_chunk_polars(
    header:     bytes,
    chunk_data: bytes,
    delimiter:  str,
    time_col:   str,
    source:     str,
    bid_col:    Optional[str],
    ask_col:    Optional[str],
    start_naive,
    end_naive,
    state:      Optional[dict],
    base_year:  int,
) -> Chunk:
    usecols = _build_usecols(time_col, source, bid_col, ask_col)

    df = pl.read_csv(
        BytesIO(header + chunk_data),
        separator=delimiter,
        columns=usecols,
        infer_schema=True,
        try_parse_dates=True,
        ignore_errors=True,
    )
    if df.is_empty():
        return _empty_chunk()

    df = normalize_polars_time_col(df, time_col, state, base_year)
    df = df.filter(pl.col(time_col).is_not_null())
    if df.is_empty():
        return _empty_chunk()

    tc = pl.col(time_col)
    if df[time_col].dtype.time_zone:
        tc = tc.dt.replace_time_zone(None)

    df = df.filter((tc >= start_naive) & (tc < end_naive))
    if df.is_empty():
        return _empty_chunk()

    # Compute price
    if source == "__mid__":
        df = df.with_columns(
            ((pl.col(bid_col).cast(pl.Float64, strict=False) +
              pl.col(ask_col).cast(pl.Float64, strict=False)) / 2.0
             ).alias("_price")
        )
    else:
        df = df.with_columns(
            pl.col(source).cast(pl.Float64, strict=False).alias("_price")
        )

    df = df.filter(pl.col("_price").is_not_null() & pl.col("_price").is_finite())
    if df.is_empty():
        return _empty_chunk()

    prices = df["_price"].to_numpy(allow_copy=True).astype(np.float64)
    times  = df[time_col].dt.strftime("%Y-%m-%d %H:%M:%S.%f").to_numpy()
    times  = np.array([t[:23] if len(t) > 23 else t for t in times], dtype=object)

    # Bid / Ask arrays (fall back to price if column absent)
    if bid_col and bid_col in df.columns:
        bids = df[bid_col].cast(pl.Float64, strict=False).to_numpy(allow_copy=True).astype(np.float64)
    else:
        bids = prices.copy()

    if ask_col and ask_col in df.columns:
        asks = df[ask_col].cast(pl.Float64, strict=False).to_numpy(allow_copy=True).astype(np.float64)
    else:
        asks = prices.copy()

    nrows = len(prices)
    del df
    return prices, times, bids, asks, nrows


def _process_chunk_duckdb(
    header:     bytes,
    chunk_data: bytes,
    delimiter:  str,
    time_col:   str,
    source:     str,
    bid_col:    Optional[str],
    ask_col:    Optional[str],
    start_naive,
    end_naive,
) -> Chunk:
    import pyarrow as pa
    import pyarrow.csv as pa_csv

    start_str = str(start_naive)[:23]
    end_str   = str(end_naive)[:23]

    opts  = pa_csv.ParseOptions(delimiter=delimiter)
    table = pa_csv.read_csv(BytesIO(header + chunk_data), parse_options=opts)

    con = duckdb.connect(":memory:")
    try:
        con.register("_chunk", table)
        if source == "__mid__":
            price_expr = f'(TRY_CAST("{bid_col}" AS DOUBLE) + TRY_CAST("{ask_col}" AS DOUBLE)) / 2.0'
        else:
            price_expr = f'TRY_CAST("{source}" AS DOUBLE)'

        bid_expr = f'TRY_CAST("{bid_col}" AS DOUBLE)' if bid_col else price_expr
        ask_expr = f'TRY_CAST("{ask_col}" AS DOUBLE)' if ask_col else price_expr

        sql = f"""
            SELECT
                CAST("{time_col}" AS VARCHAR)  AS _ts,
                {price_expr}                   AS _price,
                {bid_expr}                     AS _bid,
                {ask_expr}                     AS _ask
            FROM _chunk
            WHERE TRY_CAST("{time_col}" AS TIMESTAMP) >= TIMESTAMP '{start_str}'
              AND TRY_CAST("{time_col}" AS TIMESTAMP) <  TIMESTAMP '{end_str}'
              AND {price_expr} IS NOT NULL
        """
        rows = con.execute(sql).fetchall()
    finally:
        con.close()
    del table

    if not rows:
        return _empty_chunk()

    times_raw  = [r[0][:23] if r[0] and len(r[0]) > 23 else (r[0] or "") for r in rows]
    prices_raw = [r[1] for r in rows]
    bids_raw   = [r[2] if r[2] is not None else r[1] for r in rows]
    asks_raw   = [r[3] if r[3] is not None else r[1] for r in rows]

    prices = np.array(prices_raw, dtype=np.float64)
    bids   = np.array(bids_raw,   dtype=np.float64)
    asks   = np.array(asks_raw,   dtype=np.float64)
    times  = np.array(times_raw,  dtype=object)
    valid  = np.isfinite(prices)
    return prices[valid], times[valid], bids[valid], asks[valid], int(valid.sum())


def _process_chunk_pandas(
    header:     bytes,
    chunk_data: bytes,
    delimiter:  str,
    time_col:   str,
    source:     str,
    bid_col:    Optional[str],
    ask_col:    Optional[str],
    start_t:    pd.Timestamp,
    end_t:      pd.Timestamp,
    state:      Optional[dict],
    base_year:  int,
) -> Chunk:
    usecols = _build_usecols(time_col, source, bid_col, ask_col)
    df = pd.read_csv(
        BytesIO(header + chunk_data),
        sep=delimiter,
        usecols=usecols,
        low_memory=False,
    )
    if df.empty:
        return _empty_chunk()

    df = normalize_pandas_time_col(df, time_col, state, base_year)
    parsed = df[time_col]
    valid  = parsed.notna()
    df     = df.loc[valid]
    parsed = parsed.loc[valid]

    mask   = (parsed >= start_t) & (parsed < end_t)
    df2    = df.loc[mask]
    parsed2 = parsed.loc[mask]
    if df2.empty:
        return _empty_chunk()

    if source == "__mid__":
        raw = (pd.to_numeric(df2[bid_col], errors="coerce") +
               pd.to_numeric(df2[ask_col], errors="coerce")) / 2.0
    else:
        raw = pd.to_numeric(df2[source], errors="coerce")

    prices = raw.to_numpy(dtype=np.float64)
    times  = parsed2.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:23].to_numpy(dtype=object)

    bids = pd.to_numeric(df2[bid_col], errors="coerce").to_numpy(dtype=np.float64) if bid_col and bid_col in df2.columns else prices.copy()
    asks = pd.to_numeric(df2[ask_col], errors="coerce").to_numpy(dtype=np.float64) if ask_col and ask_col in df2.columns else prices.copy()

    valid2 = np.isfinite(prices)
    return prices[valid2], times[valid2], bids[valid2], asks[valid2], int(valid2.sum())


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_usecols(
    time_col: str,
    source:   str,
    bid_col:  Optional[str],
    ask_col:  Optional[str],
) -> list:
    cols = [time_col]
    if source == "__mid__":
        if bid_col: cols.append(bid_col)
        if ask_col: cols.append(ask_col)
    else:
        cols.append(source)
    # Always include bid/ask for spread display
    if bid_col and bid_col not in cols:
        cols.append(bid_col)
    if ask_col and ask_col not in cols:
        cols.append(ask_col)
    return list(dict.fromkeys(c for c in cols if c))


def _empty_chunk() -> Chunk:
    empty = np.array([], dtype=np.float64)
    return empty, np.array([], dtype=object), empty.copy(), empty.copy(), 0


def _estimate_total_rows(path: Path, data_start: int) -> int:
    """Fast row count estimate from file size and a 10k-row sample."""
    try:
        with path.open("rb") as fh:
            fh.seek(data_start)
            sample_bytes = 0
            sample_rows  = 0
            for line in fh:
                sample_bytes += len(line)
                sample_rows  += 1
                if sample_rows >= 10_000:
                    break
        if sample_rows == 0 or sample_bytes == 0:
            return 1
        avg = sample_bytes / sample_rows
        return max(1, int((path.stat().st_size - data_start) / avg))
    except Exception:
        return 1_000_000  # safe fallback
