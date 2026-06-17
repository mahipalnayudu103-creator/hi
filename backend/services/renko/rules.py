"""
rules.py — Shared constants and visual parameters for Renko brick generation.
"""

UP_FILL   = "#22c55e"
UP_LINE   = "#16a34a"
DOWN_FILL = "#fb7185"
DOWN_LINE = "#e11d48"

RENKO_METHOD_LABEL = "cTrader body v3"
RENKO_METHOD_CACHE_VARIANT = "ctrader_body_v3"

import numba

@numba.njit(nogil=True, cache=True)
def get_triggers(last_close: float, direction: int, reversal_boxes: int, brick_size: float):
    reversal = max(1, reversal_boxes)
    up_distance = brick_size if direction >= 0 else reversal * brick_size
    down_distance = brick_size if direction <= 0 else reversal * brick_size
    up_trigger = last_close + up_distance
    down_trigger = last_close - down_distance
    return up_trigger, down_trigger

@numba.njit(nogil=True, cache=True)
def get_up_brick_bounds(last_close: float, direction: int, reversal_boxes: int, brick_size: float):
    if direction < 0:
        brick_open = last_close + (reversal_boxes - 1) * brick_size
    else:
        brick_open = last_close
    brick_close = brick_open + brick_size
    return brick_open, brick_close

@numba.njit(nogil=True, cache=True)
def get_down_brick_bounds(last_close: float, direction: int, reversal_boxes: int, brick_size: float):
    if direction > 0:
        brick_open = last_close - (reversal_boxes - 1) * brick_size
    else:
        brick_open = last_close
    brick_close = brick_open - brick_size
    return brick_open, brick_close

