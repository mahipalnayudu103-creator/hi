"""
renko_state.py — Streaming Renko engine (one tick at a time).

Key design rules:
  - process_tick() receives ONE tick and returns a list of confirmed bricks.
  - One tick can confirm 0, 1, or many bricks (gap jumps).
  - Only 7 scalar fields track forming state — no lists of raw ticks.
  - Confirmed bricks contain only OHLC metadata.
  - time = tick_index * 1000 + sequence_in_tick (unique, cross-chart sync safe).
"""

import math
import logging
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import numba

from services.renko.rules import get_triggers, get_up_brick_bounds, get_down_brick_bounds

logger = logging.getLogger("renko_playback.renko_state")


class RenkoState:
    """
    Streaming, memory-minimal Renko engine.

    RAM footprint: 7 scalars + configuration constants.
    No accumulation of confirmed bricks.
    No storage of raw ticks.
    """

    __slots__ = (
        "brick_pips", "pip_size", "brick_size", "reversal_boxes", "anchor",
        "initialized", "last_close", "direction",
        "forming_open", "forming_high", "forming_low",
        "forming_tick_count", "_eps",
        "total_bricks_confirmed",
    )

    def __init__(
        self,
        brick_pips: float,
        pip_size: float = 0.0001,
        reversal_boxes: int = 2,
        anchor: str = "floor",
    ) -> None:
        self.brick_pips      = float(brick_pips)
        self.pip_size        = float(pip_size)
        self.brick_size      = self.brick_pips * self.pip_size
        self.reversal_boxes  = max(1, int(reversal_boxes))
        self.anchor          = anchor
        self._eps            = self.brick_size / 1_000_000.0

        # Forming state (7 scalars)
        self.initialized          = False
        self.last_close: Optional[float] = None
        self.direction            = 0   # 0=undecided, 1=up, -1=down
        self.forming_open: Optional[float] = None
        self.forming_high: Optional[float] = None
        self.forming_low:  Optional[float] = None
        self.forming_tick_count   = 0

        # Counter only — not a list
        self.total_bricks_confirmed = 0

    # ── Public API ─────────────────────────────────────────────────────────────

    def process_tick(
        self,
        price: float,
        tick_time: str,
        tick_index: int,
        bid: float = 0.0,
        ask: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """
        Feed one tick into the engine.

        Returns a list of confirmed brick dicts (may be empty).
        Each brick dict contains only OHLC metadata — no raw ticks.

        time field = tick_index * 1000 + sequence_within_this_tick
        This guarantees unique x-values even when one tick fires multiple bricks.
        """
        if not math.isfinite(price) or price <= 0:
            return []

        # ── First tick: initialise anchor close ───────────────────────────────
        if not self.initialized:
            self.last_close     = self._anchor_close(price)
            self.forming_open   = self.last_close
            self.forming_high   = price
            self.forming_low    = price
            self.forming_tick_count = 1
            self.initialized    = True
            return []

        # ── Update forming high/low/tick_count ───────────────────────────────
        if self.forming_high is None or self.forming_low is None:
            self.forming_high = price
            self.forming_low  = price
        else:
            if price > self.forming_high:
                self.forming_high = price
            if price < self.forming_low:
                self.forming_low = price
        self.forming_tick_count += 1

        # ── Check for brick confirmations ─────────────────────────────────────
        confirmed: List[Dict[str, Any]] = []
        seq = 0  # sub-index within this tick

        while True:
            up_trigger, down_trigger = get_triggers(self.last_close, self.direction, self.reversal_boxes, self.brick_size)

            if price >= up_trigger - self._eps:
                # ── Upward brick ─────────────────────────────────────────────
                brick_open, brick_close = get_up_brick_bounds(self.last_close, self.direction, self.reversal_boxes, self.brick_size)
                brick_high = brick_close
                brick_low  = brick_open

                confirmed.append(self._make_brick(
                    brick_open  = brick_open,
                    brick_close = brick_close,
                    brick_high  = brick_high,
                    brick_low   = brick_low,
                    direction   = "up",
                    tick_time   = tick_time,
                    tick_index  = tick_index,
                    seq         = seq,
                    bid         = bid,
                    ask         = ask,
                ))
                self.last_close = brick_close
                self.direction  = 1
                self._reset_forming()
                seq += 1
                continue

            if price <= down_trigger + self._eps:
                # ── Downward brick ───────────────────────────────────────────
                brick_open, brick_close = get_down_brick_bounds(self.last_close, self.direction, self.reversal_boxes, self.brick_size)
                brick_high = brick_open
                brick_low  = brick_close

                confirmed.append(self._make_brick(
                    brick_open  = brick_open,
                    brick_close = brick_close,
                    brick_high  = brick_high,
                    brick_low   = brick_low,
                    direction   = "down",
                    tick_time   = tick_time,
                    tick_index  = tick_index,
                    seq         = seq,
                    bid         = bid,
                    ask         = ask,
                ))
                self.last_close = brick_close
                self.direction  = -1
                self._reset_forming()
                seq += 1
                continue

            break  # no more bricks this tick

        return confirmed

    def process_ticks_batch(
        self,
        prices: np.ndarray,
        times: np.ndarray,
        bids: np.ndarray,
        asks: np.ndarray,
        global_tick_start: int,
    ) -> List[Dict[str, Any]]:
        """
        Process a batch of ticks at C-speed using Numba JIT.
        GIL is released, allowing parallel multi-chart execution.
        """
        if len(prices) == 0:
            return []

        anchor_mode_int = 0
        if self.anchor == "round":
            anchor_mode_int = 1
        elif self.anchor == "first":
            anchor_mode_int = 2

        last_close_val = self.last_close if self.last_close is not None else np.nan
        forming_open_val = self.forming_open if self.forming_open is not None else np.nan
        forming_high_val = self.forming_high if self.forming_high is not None else np.nan
        forming_low_val = self.forming_low if self.forming_low is not None else np.nan

        (
            initialized, last_close, direction, forming_open, forming_high, forming_low, forming_tick_count, total_bricks_confirmed,
            out_opens, out_closes, out_highs, out_lows, out_directions, out_tick_indices, out_tick_counts, out_seqs, brick_count
        ) = _process_ticks_numba_core(
            prices=prices,
            bids=bids,
            asks=asks,
            global_tick_start=global_tick_start,
            brick_size=self.brick_size,
            reversal_boxes=self.reversal_boxes,
            anchor_mode=anchor_mode_int,
            eps=self._eps,
            initialized=self.initialized,
            last_close=last_close_val,
            direction=self.direction,
            forming_open=forming_open_val,
            forming_high=forming_high_val,
            forming_low=forming_low_val,
            forming_tick_count=self.forming_tick_count,
            total_bricks_confirmed=self.total_bricks_confirmed,
        )

        # Update state
        self.initialized = initialized
        self.last_close = last_close if not np.isnan(last_close) else None
        self.direction = direction
        self.forming_open = forming_open if not np.isnan(forming_open) else None
        self.forming_high = forming_high if not np.isnan(forming_high) else None
        self.forming_low = forming_low if not np.isnan(forming_low) else None
        self.forming_tick_count = forming_tick_count
        self.total_bricks_confirmed = total_bricks_confirmed

        # Reconstruct list of bricks
        confirmed = []
        for idx in range(brick_count):
            tick_idx = int(out_tick_indices[idx])
            seq = int(out_seqs[idx])
            chart_time = tick_idx * 1000 + seq
            
            confirmed.append({
                "time":               chart_time,
                "confirm_tick_index": tick_idx,
                "confirm_time":       str(times[tick_idx - global_tick_start]).replace("T", " "),
                "open":               round(float(out_opens[idx]),  10),
                "high":               round(float(out_highs[idx]),  10),
                "low":                round(float(out_lows[idx]),   10),
                "close":              round(float(out_closes[idx]), 10),
                "direction":          "up" if out_directions[idx] == 1 else "down",
                "tick_count":         int(out_tick_counts[idx]),
                "brick_size_pips":    self.brick_pips,
                "bid":                float(bids[tick_idx - global_tick_start]),
                "ask":                float(asks[tick_idx - global_tick_start]),
            })

        return confirmed

    def get_live_brick(
        self,
        current_price: float,
        bid: float = 0.0,
        ask: float = 0.0,
        tick_index: int = 0,
        seq: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """
        Return the current forming (unconfirmed) brick for live display.
        Returns None if engine is not yet initialised.
        """
        if not self.initialized or self.forming_open is None:
            return None

        is_up = current_price >= self.forming_open
        live_high = max(self.forming_open, current_price)
        live_low  = min(self.forming_open, current_price)

        # Use the same unique x encoding as confirmed bricks: tick_index * 1000 + seq
        live_time = tick_index * 1000 + seq

        return {
            "time":              live_time,
            "confirm_time":      "",
            "confirm_tick_index": tick_index,
            "open":              self.forming_open,
            "high":              live_high,
            "low":               live_low,
            "close":             current_price,
            "direction":         "up" if is_up else "down",
            "tick_count":        self.forming_tick_count,
            "brick_size_pips":   self.brick_pips,
            "bid":               bid,
            "ask":               ask,
            "is_live":           True,
        }

    def configure(
        self,
        brick_pips: float,
        pip_size: float,
        reversal_boxes: int,
        anchor: str,
    ) -> None:
        """Reconfigure and reset the engine."""
        self.brick_pips     = float(brick_pips)
        self.pip_size       = float(pip_size)
        self.brick_size     = self.brick_pips * self.pip_size
        self.reversal_boxes = max(1, int(reversal_boxes))
        self.anchor         = anchor
        self._eps           = self.brick_size / 1_000_000.0
        self._full_reset()

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _anchor_close(self, price: float) -> float:
        if self.anchor == "first":
            return price
        if self.anchor == "round":
            return round(price / self.brick_size) * self.brick_size
        # default: floor
        return math.floor(price / self.brick_size) * self.brick_size

    def _make_brick(
        self,
        brick_open:  float,
        brick_close: float,
        brick_high:  float,
        brick_low:   float,
        direction:   str,
        tick_time:   str,
        tick_index:  int,
        seq:         int,
        bid:         float,
        ask:         float,
    ) -> Dict[str, Any]:
        """Create a confirmed brick metadata dict. No raw tick data stored."""
        self.total_bricks_confirmed += 1
        # Unique x: encode tick_index + intra-tick sequence
        chart_time = tick_index * 1000 + seq
        return {
            "time":               chart_time,
            "confirm_tick_index": tick_index,
            "confirm_time":       tick_time,
            "open":               round(brick_open,  10),
            "high":               round(brick_high,  10),
            "low":                round(brick_low,   10),
            "close":              round(brick_close, 10),
            "direction":          direction,
            "tick_count":         self.forming_tick_count,
            "brick_size_pips":    self.brick_pips,
            "bid":                bid,
            "ask":                ask,
        }

    def _reset_forming(self) -> None:
        """Reset forming state after a brick confirms."""
        assert self.last_close is not None
        self.forming_open       = self.last_close
        self.forming_high       = self.last_close
        self.forming_low        = self.last_close
        self.forming_tick_count = 0

    def _full_reset(self) -> None:
        """Full engine reset."""
        self.initialized          = False
        self.last_close           = None
        self.direction            = 0
        self.forming_open         = None
        self.forming_high         = None
        self.forming_low          = None
        self.forming_tick_count   = 0
        self.total_bricks_confirmed = 0

    def reset(self) -> None:
        """Reset the engine state."""
        self._full_reset()


def build_streaming_engines(
    chart_pips: List[float],
    pip_size: float,
    reversal_boxes: int,
    anchor: str,
) -> Dict[str, RenkoState]:
    """
    Create one RenkoState per pip value.

    Returns dict keyed by str(pip) e.g. {"1.0": <RenkoState>, "2.0": ...}
    All engines share the same tick stream for cross-chart sync.
    """
    return {
        str(pip): RenkoState(
            brick_pips     = pip,
            pip_size       = pip_size,
            reversal_boxes = reversal_boxes,
            anchor         = anchor,
        )
        for pip in chart_pips
    }


@numba.njit(nogil=True, cache=True, fastmath=True)
def _process_ticks_numba_core(
    prices: np.ndarray,
    bids: np.ndarray,
    asks: np.ndarray,
    global_tick_start: int,
    brick_size: float,
    reversal_boxes: int,
    anchor_mode: int, # 0=floor, 1=round, 2=first
    eps: float,
    # state inputs
    initialized: bool,
    last_close: float,
    direction: int,
    forming_open: float,
    forming_high: float,
    forming_low: float,
    forming_tick_count: int,
    total_bricks_confirmed: int,
) -> Tuple[
    bool, float, int, float, float, float, int, int,
    np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, int
]:
    n = len(prices)
    # pre-allocate output arrays
    out_opens = np.zeros(n, dtype=np.float64)
    out_closes = np.zeros(n, dtype=np.float64)
    out_highs = np.zeros(n, dtype=np.float64)
    out_lows = np.zeros(n, dtype=np.float64)
    out_directions = np.zeros(n, dtype=np.int8)  # 1 = Up, -1 = Down
    out_tick_indices = np.zeros(n, dtype=np.int64)
    out_tick_counts = np.zeros(n, dtype=np.int32)
    out_seqs = np.zeros(n, dtype=np.int32)
    
    brick_idx = 0
    seq = 0
    
    for i in range(n):
        price = prices[i]
        bid = bids[i]
        ask = asks[i]
        tick_index = global_tick_start + i
        
        if not math.isfinite(price) or price <= 0:
            continue
            
        if not initialized:
            if anchor_mode == 2:
                last_close = price
            elif anchor_mode == 1:
                last_close = round(price / brick_size) * brick_size
            else:
                last_close = math.floor(price / brick_size) * brick_size
                
            forming_open = last_close
            forming_high = price
            forming_low = price
            forming_tick_count = 1
            initialized = True
            continue
            
        # Update forming high/low
        if math.isnan(forming_high) or math.isnan(forming_low):
            forming_high = price
            forming_low = price
        else:
            if price > forming_high:
                forming_high = price
            if price < forming_low:
                forming_low = price
        forming_tick_count += 1
        
        # Check for brick confirmations
        while True:
            up_trigger, down_trigger = get_triggers(last_close, direction, reversal_boxes, brick_size)
            
            if price >= up_trigger - eps:
                # Upward brick
                brick_open, brick_close = get_up_brick_bounds(last_close, direction, reversal_boxes, brick_size)
                brick_high = brick_close
                brick_low = brick_open
                
                # Append to pre-allocated arrays
                out_opens[brick_idx] = brick_open
                out_closes[brick_idx] = brick_close
                out_highs[brick_idx] = brick_high
                out_lows[brick_idx] = brick_low
                out_directions[brick_idx] = 1
                out_tick_indices[brick_idx] = tick_index
                out_tick_counts[brick_idx] = forming_tick_count
                out_seqs[brick_idx] = seq
                
                brick_idx += 1
                total_bricks_confirmed += 1
                last_close = brick_close
                direction = 1
                
                # Reset forming
                forming_open = last_close
                forming_high = last_close
                forming_low = last_close
                forming_tick_count = 0
                seq += 1
                continue
                
            if price <= down_trigger + eps:
                # Downward brick
                brick_open, brick_close = get_down_brick_bounds(last_close, direction, reversal_boxes, brick_size)
                brick_high = brick_open
                brick_low = brick_close
                
                # Append to pre-allocated arrays
                out_opens[brick_idx] = brick_open
                out_closes[brick_idx] = brick_close
                out_highs[brick_idx] = brick_high
                out_lows[brick_idx] = brick_low
                out_directions[brick_idx] = -1
                out_tick_indices[brick_idx] = tick_index
                out_tick_counts[brick_idx] = forming_tick_count
                out_seqs[brick_idx] = seq
                
                brick_idx += 1
                total_bricks_confirmed += 1
                last_close = brick_close
                direction = -1
                
                # Reset forming
                forming_open = last_close
                forming_high = last_close
                forming_low = last_close
                forming_tick_count = 0
                seq += 1
                continue
                
            break
            
    return (
        initialized, last_close, direction, forming_open, forming_high, forming_low, forming_tick_count, total_bricks_confirmed,
        out_opens[:brick_idx], out_closes[:brick_idx], out_highs[:brick_idx], out_lows[:brick_idx],
        out_directions[:brick_idx], out_tick_indices[:brick_idx], out_tick_counts[:brick_idx], out_seqs[:brick_idx], brick_idx
    )
