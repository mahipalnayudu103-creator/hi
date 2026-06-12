"""
downsample.py — LTTB (Largest-Triangle-Three-Buckets) downsampling for Renko bricks.

When zoomed out, millions of bricks get downsampled to ~3,000 points while
preserving visual shape. Full data is returned when zoomed in.
"""

import math
from typing import List, Dict, Any

# Max bricks to send to browser before downsampling kicks in
LTTB_THRESHOLD = 5_000
# Target points when downsampling
LTTB_TARGET    = 3_000


def lttb_downsample(bricks: List[Dict[str, Any]], threshold: int = LTTB_TARGET) -> List[Dict[str, Any]]:
    """
    Downsample bricks list using LTTB algorithm.
    Uses 'time' as x-axis and 'close' as y-axis for triangle area calculation.
    Always keeps first and last brick intact.
    Returns original list if len <= threshold.
    """
    n = len(bricks)
    if n <= threshold or threshold <= 0:
        return bricks

    sampled: List[Dict[str, Any]] = []
    # Always include first point
    sampled.append(bricks[0])

    bucket_size = (n - 2) / (threshold - 2)
    a = 0  # index of last selected point

    for i in range(threshold - 2):
        # Bucket range for current bucket
        b_start = int(math.floor((i + 1) * bucket_size)) + 1
        b_end   = int(math.floor((i + 2) * bucket_size)) + 1
        b_end   = min(b_end, n - 1)

        # Compute average point in NEXT bucket (used as third triangle vertex)
        next_start = b_end
        next_end   = int(math.floor((i + 3) * bucket_size)) + 1 if i + 3 <= threshold - 1 else n
        next_end   = min(next_end, n)

        avg_x = 0.0
        avg_y = 0.0
        count = next_end - next_start
        if count > 0:
            for j in range(next_start, next_end):
                avg_x += bricks[j]["time"]
                avg_y += bricks[j]["close"]
            avg_x /= count
            avg_y /= count

        # Find point in current bucket with largest triangle area
        max_area   = -1.0
        max_index  = b_start
        ax = bricks[a]["time"]
        ay = bricks[a]["close"]

        for j in range(b_start, b_end):
            bx = bricks[j]["time"]
            by = bricks[j]["close"]
            area = abs((ax - avg_x) * (by - ay) - (ax - bx) * (avg_y - ay)) * 0.5
            if area > max_area:
                max_area  = area
                max_index = j

        sampled.append(bricks[max_index])
        a = max_index

    # Always include last point
    sampled.append(bricks[-1])
    return sampled


def maybe_downsample(bricks: List[Dict[str, Any]], max_points: int = LTTB_THRESHOLD) -> List[Dict[str, Any]]:
    """Downsample only if brick count exceeds max_points."""
    if len(bricks) > max_points:
        return lttb_downsample(bricks, threshold=LTTB_TARGET)
    return bricks
