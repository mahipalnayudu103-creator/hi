import math
import numpy as np
import numba
from typing import Tuple, List, Dict, Any

from services.renko.rules import (
    UP_FILL, UP_LINE, DOWN_FILL, DOWN_LINE,
    get_triggers, get_up_brick_bounds, get_down_brick_bounds
)

class RenkoState:
    def __init__(
        self,
        brick_pips: float,
        reversal_boxes: int = 2,
        pip_size: float = 0.0001,
        anchor: str = "floor",
    ) -> None:
        self.brick_pips = float(brick_pips)
        self.pip_size = float(pip_size)
        self.brick_size = self.brick_pips * self.pip_size
        self.reversal_boxes = int(reversal_boxes)
        self.anchor = anchor
        self.reset()

    def reset(self) -> None:
        self.last_close: float | None = None
        self.direction = 0
        self.confirmed_indices: List[int] = []
        self.confirmed_opens: List[float] = []
        self.confirmed_closes: List[float] = []
        self.confirmed_tops: List[float] = []
        self.confirmed_bottoms: List[float] = []
        self.confirmed_highs: List[float] = []
        self.confirmed_lows: List[float] = []
        self.confirmed_colors: List[str] = []
        self.confirmed_borders: List[str] = []
        self.confirmed_times: List[str] = []
        self.confirmed_ticks: List[int] = []
        self.confirmed_directions: List[str] = []
        self.live_open: float | None = None
        self.live_high: float | None = None
        self.live_low: float | None = None
        self.live_tick_count = 0

    def configure(self, brick_pips: float, reversal_boxes: int, pip_size: float, anchor: str) -> None:
        self.brick_pips = float(brick_pips)
        self.pip_size = float(pip_size)
        self.brick_size = self.brick_pips * self.pip_size
        self.reversal_boxes = max(1, int(reversal_boxes))
        self.anchor = anchor
        self.reset()

    def _initial_close(self, price: float) -> float:
        if self.anchor == "first":
            return price
        if self.anchor == "round":
            return round(price / self.brick_size) * self.brick_size
        return math.floor(price / self.brick_size) * self.brick_size

    def process_tick(self, price: float, time_str: str, bid: float = 0.0, ask: float = 0.0) -> List[Dict[str, Any]]:
        if not math.isfinite(price):
            return []

        if self.last_close is None:
            self.last_close = self._initial_close(price)
            self.live_open = self.last_close
            self.live_high = price
            self.live_low = price
            self.live_tick_count = 1
            return []

        assert self.live_high is not None
        assert self.live_low is not None
        self.live_high = max(self.live_high, price)
        self.live_low = min(self.live_low, price)
        self.live_tick_count += 1

        new_rows: List[Dict[str, Any]] = []
        eps = self.brick_size / 1_000_000.0

        while True:
            up_trigger, down_trigger = get_triggers(self.last_close, self.direction, self.reversal_boxes, self.brick_size)

            if price >= up_trigger - eps:
                brick_open, brick_close = get_up_brick_bounds(self.last_close, self.direction, self.reversal_boxes, self.brick_size)

                row_data = self._append_brick(
                    brick_open=brick_open,
                    brick_close=brick_close,
                    wick_high=brick_close,
                    wick_low=brick_open,
                    time_str=time_str,
                    fill=UP_FILL,
                    line=UP_LINE,
                    direction="up",
                    bid=bid,
                    ask=ask
                )
                new_rows.append(row_data)
                self.last_close = brick_close
                self.direction = 1
                self._reset_live_from_close()
                continue

            if price <= down_trigger + eps:
                brick_open, brick_close = get_down_brick_bounds(self.last_close, self.direction, self.reversal_boxes, self.brick_size)

                row_data = self._append_brick(
                    brick_open=brick_open,
                    brick_close=brick_close,
                    wick_high=brick_open,
                    wick_low=brick_close,
                    time_str=time_str,
                    fill=DOWN_FILL,
                    line=DOWN_LINE,
                    direction="down",
                    bid=bid,
                    ask=ask
                )
                new_rows.append(row_data)
                self.last_close = brick_close
                self.direction = -1
                self._reset_live_from_close()
                continue

            break

        return new_rows

    def _append_brick(
        self,
        brick_open: float,
        brick_close: float,
        wick_high: float,
        wick_low: float,
        time_str: str,
        fill: str,
        line: str,
        direction: str,
        bid: float = 0.0,
        ask: float = 0.0
    ) -> Dict[str, Any]:
        index = len(self.confirmed_indices)
        body_top = max(brick_open, brick_close)
        body_bottom = min(brick_open, brick_close)
        tick_count = self.live_tick_count

        self.confirmed_indices.append(index)
        self.confirmed_opens.append(brick_open)
        self.confirmed_closes.append(brick_close)
        self.confirmed_tops.append(body_top)
        self.confirmed_bottoms.append(body_bottom)
        self.confirmed_highs.append(wick_high)
        self.confirmed_lows.append(wick_low)
        self.confirmed_colors.append(fill)
        self.confirmed_borders.append(line)
        self.confirmed_times.append(time_str)
        self.confirmed_ticks.append(tick_count)
        self.confirmed_directions.append(direction)

        return {
            "time": index + 1,
            "confirm_time": time_str,
            "open": brick_open,
            "high": wick_high,
            "low": wick_low,
            "close": brick_close,
            "direction": direction,
            "tick_count": tick_count,
            "bid": bid,
            "ask": ask
        }

    def get_live_brick(self, current_price: float, current_bid: float = 0.0, current_ask: float = 0.0) -> dict | None:
        if self.last_close is None or self.live_open is None:
            return None
        index = len(self.confirmed_indices)
        is_up = current_price >= self.live_open
        return {
            "time": index + 1,
            "confirm_time": "",
            "open": self.live_open,
            "high": max(self.live_open, current_price),
            "low":  min(self.live_open, current_price),
            "close": current_price,
            "direction": "up" if is_up else "down",
            "tick_count": self.live_tick_count,
            "bid": current_bid,
            "ask": current_ask,
            "is_live": True
        }

    def _reset_live_from_close(self) -> None:
        assert self.last_close is not None
        self.live_open = self.last_close
        self.live_high = self.last_close
        self.live_low = self.last_close
        self.live_tick_count = 0


@numba.njit(nogil=True, cache=True)
def _build_renko_numba(
    prices: np.ndarray,
    brick_size: float,
    reversal_boxes: int,
    anchor_mode_int: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, int]:
    n = len(prices)
    
    init_size = max(1024, n)
    out_opens = np.zeros(init_size, dtype=np.float64)
    out_closes = np.zeros(init_size, dtype=np.float64)
    out_highs = np.zeros(init_size, dtype=np.float64)
    out_lows = np.zeros(init_size, dtype=np.float64)
    out_directions = np.zeros(init_size, dtype=np.int8)  # 1 = Up, -1 = Down
    out_times_idx = np.zeros(init_size, dtype=np.int64)
    out_ticks = np.zeros(init_size, dtype=np.int32)
    
    if n == 0:
        return out_opens[:0], out_closes[:0], out_highs[:0], out_lows[:0], out_directions[:0], out_times_idx[:0], out_ticks[:0], 0
        
    last_close = 0.0
    direction = 0
    brick_idx = 0
    
    live_open = 0.0
    live_high = 0.0
    live_low = 0.0
    live_tick_count = 0
    
    has_first = False
    eps = brick_size / 1000000.0
    
    for i in range(n):
        price = prices[i]
        
        if not has_first:
            if anchor_mode_int == 2:
                last_close = price
            elif anchor_mode_int == 1:
                last_close = round(price / brick_size) * brick_size
            else:
                last_close = math.floor(price / brick_size) * brick_size
            live_open = last_close
            live_high = price
            live_low = price
            live_tick_count = 1
            has_first = True
            continue
            
        live_high = max(live_high, price)
        live_low = min(live_low, price)
        live_tick_count += 1
        
        while True:
            up_trigger, down_trigger = get_triggers(last_close, direction, reversal_boxes, brick_size)
            
            if price >= up_trigger - eps:
                brick_open, brick_close = get_up_brick_bounds(last_close, direction, reversal_boxes, brick_size)
                
                # Check for array capacity and double if needed
                if brick_idx >= len(out_opens):
                    new_size = len(out_opens) * 2
                    
                    new_opens = np.zeros(new_size, dtype=np.float64)
                    new_opens[:brick_idx] = out_opens[:brick_idx]
                    out_opens = new_opens
                    
                    new_closes = np.zeros(new_size, dtype=np.float64)
                    new_closes[:brick_idx] = out_closes[:brick_idx]
                    out_closes = new_closes
                    
                    new_highs = np.zeros(new_size, dtype=np.float64)
                    new_highs[:brick_idx] = out_highs[:brick_idx]
                    out_highs = new_highs
                    
                    new_lows = np.zeros(new_size, dtype=np.float64)
                    new_lows[:brick_idx] = out_lows[:brick_idx]
                    out_lows = new_lows
                    
                    new_directions = np.zeros(new_size, dtype=np.int8)
                    new_directions[:brick_idx] = out_directions[:brick_idx]
                    out_directions = new_directions
                    
                    new_times_idx = np.zeros(new_size, dtype=np.int64)
                    new_times_idx[:brick_idx] = out_times_idx[:brick_idx]
                    out_times_idx = new_times_idx
                    
                    new_ticks = np.zeros(new_size, dtype=np.int32)
                    new_ticks[:brick_idx] = out_ticks[:brick_idx]
                    out_ticks = new_ticks
                
                out_opens[brick_idx] = brick_open
                out_closes[brick_idx] = brick_close
                out_highs[brick_idx] = brick_close
                out_lows[brick_idx] = brick_open
                out_directions[brick_idx] = 1
                out_times_idx[brick_idx] = i
                out_ticks[brick_idx] = live_tick_count
                
                brick_idx += 1
                last_close = brick_close
                direction = 1
                
                live_open = last_close
                live_high = last_close
                live_low = last_close
                live_tick_count = 0
                continue
                
            if price <= down_trigger + eps:
                brick_open, brick_close = get_down_brick_bounds(last_close, direction, reversal_boxes, brick_size)
                
                # Check for array capacity and double if needed
                if brick_idx >= len(out_opens):
                    new_size = len(out_opens) * 2
                    
                    new_opens = np.zeros(new_size, dtype=np.float64)
                    new_opens[:brick_idx] = out_opens[:brick_idx]
                    out_opens = new_opens
                    
                    new_closes = np.zeros(new_size, dtype=np.float64)
                    new_closes[:brick_idx] = out_closes[:brick_idx]
                    out_closes = new_closes
                    
                    new_highs = np.zeros(new_size, dtype=np.float64)
                    new_highs[:brick_idx] = out_highs[:brick_idx]
                    out_highs = new_highs
                    
                    new_lows = np.zeros(new_size, dtype=np.float64)
                    new_lows[:brick_idx] = out_lows[:brick_idx]
                    out_lows = new_lows
                    
                    new_directions = np.zeros(new_size, dtype=np.int8)
                    new_directions[:brick_idx] = out_directions[:brick_idx]
                    out_directions = new_directions
                    
                    new_times_idx = np.zeros(new_size, dtype=np.int64)
                    new_times_idx[:brick_idx] = out_times_idx[:brick_idx]
                    out_times_idx = new_times_idx
                    
                    new_ticks = np.zeros(new_size, dtype=np.int32)
                    new_ticks[:brick_idx] = out_ticks[:brick_idx]
                    out_ticks = new_ticks
                
                out_opens[brick_idx] = brick_open
                out_closes[brick_idx] = brick_close
                out_highs[brick_idx] = brick_open
                out_lows[brick_idx] = brick_close
                out_directions[brick_idx] = -1
                out_times_idx[brick_idx] = i
                out_ticks[brick_idx] = live_tick_count
                
                brick_idx += 1
                last_close = brick_close
                direction = -1
                
                live_open = last_close
                live_high = last_close
                live_low = last_close
                live_tick_count = 0
                continue
                
            break
            
    return (
        out_opens[:brick_idx],
        out_closes[:brick_idx],
        out_highs[:brick_idx],
        out_lows[:brick_idx],
        out_directions[:brick_idx],
        out_times_idx[:brick_idx],
        out_ticks[:brick_idx],
        brick_idx
    )


def build_renko_cpu(
    prices: np.ndarray,
    times: np.ndarray,
    brick_pips: float,
    reversal_boxes: int,
    pip_size: float,
    anchor_mode: str,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    brick_size = brick_pips * pip_size
    n = len(prices)
    if n == 0:
        empty = np.array([])
        return (empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty)
        
    anchor_mode_int = 0
    if anchor_mode == "round":
        anchor_mode_int = 1
    elif anchor_mode == "first":
        anchor_mode_int = 2
        
    (
        opens, closes, highs, lows, directions, times_idx, ticks, brick_count
    ) = _build_renko_numba(prices, brick_size, reversal_boxes, anchor_mode_int)
    
    if brick_count == 0:
        empty = np.array([])
        return (empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty, empty)
        
    tops = np.maximum(opens, closes)
    bottoms = np.minimum(opens, closes)
    indices = np.arange(brick_count, dtype=np.int32)
    
    out_colors = np.where(directions == 1, UP_FILL, DOWN_FILL)
    out_borders = np.where(directions == 1, UP_LINE, DOWN_LINE)
    out_directions_str = np.where(directions == 1, "up", "down")
    out_times = times[times_idx]
    
    return (
        indices,
        opens,
        closes,
        tops,
        bottoms,
        highs,
        lows,
        out_colors,
        out_borders,
        out_times,
        ticks,
        out_directions_str,
        times_idx,
    )
