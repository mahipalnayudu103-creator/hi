import asyncio
import json
import logging
import math
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

import numpy as np
import pandas as pd
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

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

from services.csv.metadata import resolve_csv_path, summarize_csv_file
from services.renko.state import build_streaming_engines
from services.csv.stream import stream_ticks

logger = logging.getLogger("renko_playback.routes.playback")
router = APIRouter()

TICK_TIME_FMT = "%Y-%m-%d %H:%M:%S.%f"
MAX_MARKET_GAP_SECONDS = 10.0


def _boost_playback_start():
    """Boost process priority to ABOVE_NORMAL during active playback."""
    try:
        import psutil
        proc = psutil.Process()
        if os.name == "nt":
            proc.nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS)
        else:
            proc.nice(-5)
        logger.info("Playback booster started: priority set to ABOVE_NORMAL.")
    except Exception as exc:
        logger.warning(f"Could not boost playback priority (non-fatal): {exc}")


def _boost_playback_stop():
    """Restore process priority to NORMAL when playback is paused, ended, or cancelled."""
    try:
        import psutil
        proc = psutil.Process()
        if os.name == "nt":
            proc.nice(psutil.NORMAL_PRIORITY_CLASS)
        else:
            proc.nice(0)
        logger.info("Playback booster stopped: priority restored to NORMAL.")
    except Exception as exc:
        logger.warning(f"Could not restore playback priority (non-fatal): {exc}")


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


def count_ticks_in_range(csv_path, delimiter, time_col, start_t, end_t) -> int:
    try:
        from services.csv.reader import read_header_columns, seek_first_timestamp_offset, open_compressed_file
        columns = read_header_columns(csv_path, delimiter)
        if time_col not in columns:
            return 100_000
        time_index = columns.index(time_col)
        
        with open_compressed_file(csv_path, "rb") as fh:
            fh.readline()
            data_start = fh.tell()
            first_line = fh.readline()
            
        if not first_line:
            return 0
            
        start_offset = seek_first_timestamp_offset(csv_path, start_t, data_start, time_index, delimiter)
        end_offset = seek_first_timestamp_offset(csv_path, end_t, data_start, time_index, delimiter)
        
        exact_ticks = 0
        with open_compressed_file(csv_path, "rb") as fh:
            fh.seek(start_offset)
            bytes_to_read = max(0, end_offset - start_offset)
            chunk_size = 1024 * 1024
            read_so_far = 0
            last_byte = b""
            while read_so_far < bytes_to_read:
                to_read = min(chunk_size, bytes_to_read - read_so_far)
                chunk = fh.read(to_read)
                if not chunk:
                    break
                exact_ticks += chunk.count(b"\n")
                if chunk:
                    last_byte = chunk[-1:]
                read_so_far += len(chunk)
            if last_byte and last_byte != b"\n":
                exact_ticks += 1
        return max(1, exact_ticks)
    except Exception as e:
        logger.exception("Error counting exact ticks in range")
        return 100_000


@router.websocket("/ws/playback")
async def ws_playback(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket playback client connected")
    
    playback_task = None
    diagnostics = []
    diagnostics.append("WebSocket connection accepted.")

    state = {
        "is_playing": False,
        "speed": 100.0,
        "speed_mode": "tick",   # "tick" = ticks/sec, "time" = market-time multiplier (0–100x)
        "virtual_dt": None,     # virtual market clock for time-based playback
        "chart_pips": [],
        "reversal_boxes": 2,
        "pip_size": 0.0001,
        "anchor": "floor",
        "renko_engines": [],
        "generator": None,
        "total_ticks": 0,
        "global_tick_index": 0,
        "chunk_prices": None,
        "chunk_times": None,
        "chunk_bids": None,
        "chunk_asks": None,
        "chunk_length": 0,
        "chunk_index": 0,
        
        # CSV configuration stored for resetting/seeking
        "csv_path": None,
        "delimiter": ",",
        "time_col": "",
        "source": "",
        "bid_col": None,
        "ask_col": None,
        "start_t": None,
        "end_t": None,
    }

    async def load_next_chunk_async() -> bool:
        if state["generator"] is None:
            return False
        
        def _get_next():
            try:
                return next(state["generator"])
            except StopIteration:
                return None
                
        chunk = await asyncio.to_thread(_get_next)
        if chunk is None:
            state["generator"] = None
            state["chunk_prices"] = None
            state["chunk_times"] = None
            state["chunk_bids"] = None
            state["chunk_asks"] = None
            state["chunk_length"] = 0
            state["chunk_index"] = 0
            return False
            
        prices, times, bids, asks, nrows = chunk
        if nrows == 0:
            del prices, times, bids, asks, chunk
            import gc
            gc.collect()
            return await load_next_chunk_async()
        
        state["chunk_prices"] = prices
        state["chunk_times"] = times
        state["chunk_bids"] = bids
        state["chunk_asks"] = asks
        state["chunk_length"] = nrows
        state["chunk_index"] = 0
        return True

    async def get_next_tick_batch_async(count: int) -> Optional[Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, int]]:
        if state["chunk_prices"] is None or state["chunk_index"] >= state["chunk_length"]:
            state["chunk_prices"] = None
            state["chunk_times"] = None
            state["chunk_bids"] = None
            state["chunk_asks"] = None
            import gc
            gc.collect()
            
            has_more = await load_next_chunk_async()
            if not has_more:
                return None
                
        idx = state["chunk_index"]
        avail = state["chunk_length"] - idx
        actual = min(count, avail)
        
        prices = state["chunk_prices"][idx : idx + actual]
        times = state["chunk_times"][idx : idx + actual]
        bids = state["chunk_bids"][idx : idx + actual] if state["chunk_bids"] is not None else prices
        asks = state["chunk_asks"][idx : idx + actual] if state["chunk_asks"] is not None else prices
        
        state["chunk_index"] += actual
        state["global_tick_index"] += actual
        return prices, times, bids, asks, actual

    def _parse_tick_ts(s: str) -> datetime:
        try:
            return datetime.strptime(s, TICK_TIME_FMT)
        except ValueError:
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")

    async def peek_next_tick_time() -> Optional[str]:
        if state["chunk_prices"] is None or state["chunk_index"] >= state["chunk_length"]:
            has_more = await load_next_chunk_async()
            if not has_more:
                return None
        return str(state["chunk_times"][state["chunk_index"]])

    async def get_next_tick_async() -> Optional[Tuple[float, str, float, float]]:
        batch = await get_next_tick_batch_async(1)
        if batch is None:
            return None
        prices, times, bids, asks, _ = batch
        return float(prices[0]), str(times[0]), float(bids[0]), float(asks[0])

    async def skip_to_target_idx(target_idx: int):
        target_idx = max(0, min(target_idx, state["total_ticks"]))
        state["virtual_dt"] = None
        
        if target_idx < state["global_tick_index"]:
            for engine in state["renko_engines"]:
                engine.reset()
            state["global_tick_index"] = 0
            state["chunk_prices"] = None
            state["chunk_times"] = None
            state["chunk_bids"] = None
            state["chunk_asks"] = None
            state["chunk_length"] = 0
            state["chunk_index"] = 0
            
            state["generator"] = stream_ticks(
                csv_path   = state["csv_path"],
                delimiter  = state["delimiter"],
                time_col   = state["time_col"],
                source     = state["source"],
                bid_col    = state["bid_col"],
                ask_col    = state["ask_col"],
                start_t    = state["start_t"],
                end_t      = state["end_t"],
            )
        
        await _ws_send(websocket, {"type": "status", "status": "reset"})
        
        batch_by_chart = {str(idx+1): [] for idx in range(len(state["renko_engines"]))}
        
        last_price = None
        last_time = None
        last_bid = None
        last_ask = None
        last_tick_seq = [0] * len(state["renko_engines"])
        
        while state["global_tick_index"] < target_idx:
            needed = target_idx - state["global_tick_index"]
            batch = await get_next_tick_batch_async(needed)
            if batch is None:
                break
                
            prices, times, bids, asks, batch_size = batch
            global_start = state["global_tick_index"] - batch_size
            
            for j in range(batch_size):
                price = float(prices[j])
                tick_time = str(times[j])
                bid = float(bids[j])
                ask = float(asks[j])
                curr_global_idx = global_start + j
                
                last_price = price
                last_time = tick_time
                last_bid = bid
                last_ask = ask
                
                for idx, engine in enumerate(state["renko_engines"]):
                    formed = engine.process_tick(price, tick_time, curr_global_idx, bid, ask)
                    if formed:
                        for brick in formed:
                            brick["confirm_tick_index"] = curr_global_idx
                            brick["brick_index"] = brick["time"]
                            brick["time"] = brick["brick_index"]
                            brick["confirm_time"] = tick_time
                        batch_by_chart[str(idx+1)].extend(formed)
                        last_tick_seq[idx] = len(formed)
                    else:
                        last_tick_seq[idx] = 0
                        
        live_bricks_by_chart = {}
        if last_price is not None:
            for idx, engine in enumerate(state["renko_engines"]):
                chart_idx = idx + 1
                live_brick = engine.get_live_brick(last_price, last_bid, last_ask, state["global_tick_index"] - 1, last_tick_seq[idx])
                if live_brick:
                    live_brick["confirm_tick_index"] = state["global_tick_index"] - 1
                    live_brick["brick_index"] = live_brick["time"]
                    live_brick["time"] = live_brick["brick_index"]
                    live_brick["confirm_time"] = last_time
                    live_bricks_by_chart[str(chart_idx)] = live_brick
        else:
            last_bid = last_ask = 0.0
            last_time = ""
                    
        total_formed = sum(eng.total_bricks_confirmed for eng in state["renko_engines"])
        
        await _ws_send(websocket, {
            "type": "playback_frame",
            "bricks_by_chart": batch_by_chart,
            "live_bricks_by_chart": live_bricks_by_chart,
            "processed_ticks": state["global_tick_index"],
            "total_ticks": state["total_ticks"],
            "formed_bricks": total_formed,
            "speed": state["speed"],
            "latest_bid": last_bid,
            "latest_ask": last_ask,
            "latest_time": last_time
        })

    async def playback_loop():
        loop_diags = []
        loop_diags.append(
            f"Playback loop started. Speed: {state['speed']} ticks/sec. "
            f"Total ticks: {state['total_ticks']}"
        )
        ticks_per_second = state["speed"]
        tick_accumulator = 0.0
        last_frame_time = asyncio.get_event_loop().time()
        FRAME_INTERVAL = 1.0 / 20
        try:
            while True:
                if not state["is_playing"]:
                    await asyncio.sleep(0.1)
                    last_frame_time = asyncio.get_event_loop().time()
                    continue

                now = asyncio.get_event_loop().time()
                delta_seconds = now - last_frame_time
                last_frame_time = now

                ticks_per_second = state["speed"]

                if state["speed_mode"] == "time":
                    multiplier = state["speed"]
                    if multiplier <= 0:
                        await asyncio.sleep(FRAME_INTERVAL)
                        continue

                    next_ts_str = await peek_next_tick_time()
                    if next_ts_str is None:
                        ticks_to_process = 1
                    else:
                        next_dt = _parse_tick_ts(next_ts_str)
                        if state["virtual_dt"] is None:
                            state["virtual_dt"] = next_dt
                        state["virtual_dt"] += timedelta(seconds=delta_seconds * multiplier)

                        if state["virtual_dt"] < next_dt:
                            gap = (next_dt - state["virtual_dt"]).total_seconds()
                            if gap > MAX_MARKET_GAP_SECONDS:
                                state["virtual_dt"] = next_dt
                            else:
                                await asyncio.sleep(FRAME_INTERVAL)
                                continue

                        target_str = state["virtual_dt"].strftime(TICK_TIME_FMT)[:23]
                        idx = state["chunk_index"]
                        ticks_to_process = int(np.searchsorted(
                            state["chunk_times"][idx:state["chunk_length"]],
                            target_str, side="right"
                        ))
                else:
                    tick_accumulator += delta_seconds * ticks_per_second
                    ticks_to_process = int(math.floor(tick_accumulator))
                    tick_accumulator -= ticks_to_process

                if ticks_to_process > 0:
                    batch_by_chart = {}
                    for chart_idx_1based in range(1, len(state["renko_engines"]) + 1):
                        batch_by_chart[str(chart_idx_1based)] = []
                        
                    last_price = None
                    last_time = None
                    last_bid = None
                    last_ask = None
                    actual_processed = 0
                    ticks_left = ticks_to_process
                    last_tick_seq = [0] * len(state["renko_engines"])
                    tick_prices = []
                    
                    while ticks_left > 0:
                        batch = await get_next_tick_batch_async(ticks_left)
                        if batch is None:
                            break
                            
                        prices, times, bids, asks, batch_size = batch
                        global_start = state["global_tick_index"] - batch_size
                        
                        for j in range(batch_size):
                            price = float(prices[j])
                            tick_prices.append(price)
                            
                            tick_time = str(times[j])
                            bid = float(bids[j])
                            ask = float(asks[j])
                            curr_global_idx = global_start + j
                            
                            last_price = price
                            last_time = tick_time
                            last_bid = bid
                            last_ask = ask
                            
                            for idx, engine in enumerate(state["renko_engines"]):
                                chart_idx = idx + 1
                                formed = engine.process_tick(price, tick_time, curr_global_idx, bid, ask)
                                if formed:
                                    for brick in formed:
                                        brick["confirm_tick_index"] = curr_global_idx
                                        brick["brick_index"] = brick["time"]
                                        brick["time"] = brick["brick_index"]
                                        brick["confirm_time"] = tick_time
                                    batch_by_chart[str(chart_idx)].extend(formed)
                                    last_tick_seq[idx] = len(formed)
                                else:
                                    last_tick_seq[idx] = 0
                                    
                        actual_processed += batch_size
                        ticks_left -= batch_size

                    if actual_processed == 0 and state["generator"] is None and (state["chunk_prices"] is None or state["chunk_index"] >= state["chunk_length"]):
                        state["is_playing"] = False
                        loop_diags.append(f"Playback finished. Processed {state['global_tick_index']} ticks.")
                        asyncio.get_event_loop().run_in_executor(None, _boost_playback_stop)
                        await _ws_send(websocket, {
                            "type": "status",
                            "status": "ended",
                            "total_ticks": state["total_ticks"],
                            "diagnostics": diagnostics + loop_diags
                        })
                        break

                    if actual_processed > 0:
                        live_bricks_by_chart = {}
                        for idx, engine in enumerate(state["renko_engines"]):
                            chart_idx = idx + 1
                            live_brick = engine.get_live_brick(last_price, last_bid, last_ask, state["global_tick_index"] - 1, last_tick_seq[idx])
                            if live_brick:
                                live_brick["confirm_tick_index"] = state["global_tick_index"] - 1
                                live_brick["brick_index"] = live_brick["time"]
                                live_brick["time"] = live_brick["brick_index"]
                                live_brick["confirm_time"] = last_time
                                live_bricks_by_chart[str(chart_idx)] = live_brick

                        total_formed = sum(eng.total_bricks_confirmed for eng in state["renko_engines"])

                        await _ws_send(websocket, {
                            "type": "playback_frame",
                            "bricks_by_chart": batch_by_chart,
                            "live_bricks_by_chart": live_bricks_by_chart,
                            "tick_prices": tick_prices,
                            "processed_ticks": state["global_tick_index"],
                            "total_ticks": state["total_ticks"],
                            "formed_bricks": total_formed,
                            "speed": ticks_per_second,
                            "latest_bid": last_bid,
                            "latest_ask": last_ask,
                            "latest_time": last_time
                        })

                await asyncio.sleep(FRAME_INTERVAL)

        except asyncio.CancelledError:
            loop_diags.append("Playback loop cancelled by system/client request.")
            logger.info("Playback loop cancelled")
        except Exception as exc:
            logger.exception("Playback loop exception")
            try:
                await _ws_send(websocket, {
                    "type": "error",
                    "message": str(exc),
                    "error_class": exc.__class__.__name__
                })
            except Exception:
                pass

    try:
        while True:
            data = await websocket.receive_text()
            cmd = json.loads(data)
            action = cmd.get("action")
            
            if action == "start":
                if playback_task:
                    diagnostics.append("Stopping existing playback task...")
                    playback_task.cancel()
                    playback_task = None
                    
                diagnostics = []
                diagnostics.append(f"Start request received for CSV: {cmd.get('csv_path')}")
                diagnostics.append(f"Range: {cmd.get('start_utc')} .. {cmd.get('end_utc')}")
                
                await _ws_send(websocket, {
                    "type": "status",
                    "status": "loading",
                    "message": "Initializing playback...",
                    "diagnostics": diagnostics
                })
                
                try:
                    csv_path = resolve_csv_path(cmd["csv_path"])
                    diagnostics.append(f"Resolved path to: {csv_path}")
                    if not csv_path.exists() or not csv_path.is_file():
                        raise FileNotFoundError(f"CSV file not found: {cmd['csv_path']}")
                        
                    start_t = pd.Timestamp(cmd["start_utc"])
                    end_t = pd.Timestamp(cmd["end_utc"])
                    price_source = cmd.get("price_source", "Bid")
                    reversal_boxes = int(cmd.get("reversal_boxes", 2))
                    pip_size = float(cmd.get("pip_size", 0.0001))
                    anchor = cmd.get("anchor", "floor")
                    chart_pips = list(cmd.get("chart_pips", [1.0, 2.0, 3.0, 4.0]))
                    state["speed"] = float(cmd.get("speed", 100.0))
                    state["speed_mode"] = cmd.get("speed_mode", "tick")
                    state["virtual_dt"] = None
                    
                    logger.info(
                        f"\n[Playback Request]\n"
                        f"CSV: {csv_path}\n"
                        f"Requested start_utc: {cmd.get('start_utc')}\n"
                        f"Requested end_utc: {cmd.get('end_utc')}\n"
                    )
                    
                    diagnostics.append("Summarizing CSV structure...")
                    summary = await asyncio.to_thread(summarize_csv_file, csv_path)
                    if summary.get("status") == "error":
                        raise ValueError(f"Error parsing CSV structure: {summary.get('error')}")
                        
                    time_col = summary["time_col"]
                    bid_col = summary["price_col"]
                    ask_col = summary["ask_col"]
                    delimiter = summary["delimiter"]
                    
                    diagnostics.append(f"Detected columns: time='{time_col}', bid='{bid_col}', ask='{ask_col or 'None'}', delimiter='{delimiter}'")
                    
                    if price_source.lower() == "bid":
                        source = bid_col
                    elif price_source.lower() == "ask":
                        source = ask_col if ask_col else bid_col
                    elif price_source.lower() == "mid":
                        if bid_col and ask_col:
                            source = "__mid__"
                        else:
                            source = bid_col
                    else:
                        source = bid_col
                    diagnostics.append(f"Source column: {source}")
                    
                    state["csv_path"] = csv_path
                    state["delimiter"] = delimiter
                    state["time_col"] = time_col
                    state["source"] = source
                    state["bid_col"] = bid_col
                    state["ask_col"] = ask_col
                    state["start_t"] = start_t
                    state["end_t"] = end_t
                    state["chart_pips"] = chart_pips
                    state["reversal_boxes"] = reversal_boxes
                    state["pip_size"] = pip_size
                    state["anchor"] = anchor
                    
                    diagnostics.append("Counting ticks in selected range...")
                    total_ticks = await asyncio.to_thread(
                        count_ticks_in_range,
                        csv_path, delimiter, time_col, start_t, end_t
                    )
                    state["total_ticks"] = total_ticks
                    diagnostics.append(f"Ticks in range: {total_ticks}")
                    
                    state["global_tick_index"] = 0
                    state["chunk_prices"] = None
                    state["chunk_times"] = None
                    state["chunk_bids"] = None
                    state["chunk_asks"] = None
                    state["chunk_length"] = 0
                    state["chunk_index"] = 0
                    
                    state["generator"] = stream_ticks(
                        csv_path   = csv_path,
                        delimiter  = delimiter,
                        time_col   = time_col,
                        source     = source,
                        bid_col    = bid_col,
                        ask_col    = ask_col,
                        start_t    = start_t,
                        end_t      = end_t,
                    )
                    
                    engines_dict = build_streaming_engines(
                        chart_pips=chart_pips,
                        pip_size=pip_size,
                        reversal_boxes=reversal_boxes,
                        anchor=anchor,
                    )
                    state["renko_engines"] = list(engines_dict.values())
                    state["renko_engine_pips"] = chart_pips
                    
                    first_tick = await get_next_tick_async()
                    if first_tick is None:
                        raise ValueError("No price ticks were found in the selected range.")
                    
                    state["chunk_index"] = 0
                    state["global_tick_index"] = 0
                    
                    first_loaded_time = first_tick[1]
                    state["is_playing"] = True
                    
                    diagnostics.append(
                        f"Ready. Ticks: {state['total_ticks']} | "
                        f"Engines: {len(state['renko_engines'])}"
                    )
                    await _ws_send(websocket, {
                        "type": "status",
                        "status": "ready",
                        "total_bricks": 0,
                        "ticks_loaded": state["total_ticks"],
                        "first_tick": first_loaded_time,
                        "last_tick": cmd["end_utc"],
                        "diagnostics": diagnostics
                    })
                    
                    asyncio.get_event_loop().run_in_executor(None, _boost_playback_start)
                    playback_task = asyncio.create_task(playback_loop())
                    
                except Exception as build_err:
                    logger.exception("Playback initialization failed")
                    err_msg = f"Setup failed: {build_err}"
                    diagnostics.append(f"ERROR: {err_msg}")
                    import traceback
                    diagnostics.extend(traceback.format_exc().splitlines())
                    await _ws_send(websocket, {
                        "type": "error",
                        "message": err_msg,
                        "diagnostics": diagnostics
                    })
                    
            elif action == "pause":
                state["is_playing"] = False
                asyncio.get_event_loop().run_in_executor(None, _boost_playback_stop)
                await _ws_send(websocket, {"type": "status", "status": "paused"})
                
            elif action == "resume":
                state["is_playing"] = True
                asyncio.get_event_loop().run_in_executor(None, _boost_playback_start)
                await _ws_send(websocket, {"type": "status", "status": "playing"})
                
            elif action == "step":
                if not state["renko_engines"]:
                    continue
                state["is_playing"] = False
                target_idx = state["global_tick_index"] + 1
                await skip_to_target_idx(target_idx)
                
            elif action == "speed":
                new_speed = float(cmd.get("speed", 100.0))
                state["speed"] = new_speed
                if "mode" in cmd:
                    state["speed_mode"] = cmd["mode"]
                await _ws_send(websocket, {
                    "type": "status",
                    "status": "speed_updated",
                    "speed": new_speed
                })
                
            elif action == "skip_to":
                if not state["renko_engines"]:
                    continue
                state["is_playing"] = False
                target_idx = int(cmd.get("index", 0))
                await skip_to_target_idx(target_idx)
                
            elif action == "step_multi":
                if not state["renko_engines"]:
                    continue
                state["is_playing"] = False
                count = int(cmd.get("count", 1))
                direction = cmd.get("direction", "forward")
                if direction == "forward":
                    target_idx = state["global_tick_index"] + count
                else:
                    target_idx = state["global_tick_index"] - count
                await skip_to_target_idx(target_idx)

    except WebSocketDisconnect:
        logger.info("WebSocket playback client disconnected")
    finally:
        asyncio.get_event_loop().run_in_executor(None, _boost_playback_stop)
        if playback_task:
            playback_task.cancel()
