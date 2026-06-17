# Live Bar — Tick-by-Tick Animation

The forming (current) Renko bar updates on **every single tick** during playback,
not once per render frame. This document explains how it works end-to-end.

---

## Problem

Renko bricks are built by batch-processing ticks with a Numba JIT kernel.
The kernel returns confirmed bricks and a single live-bar snapshot.
Without extra work the live bar only moves once per frame (~16 ms at 60 FPS),
which looks choppy at high tick-speeds.

---

## Solution: `tick_prices` side-channel

### Backend (`backend/routes/api.py`)

Each `playback_frame` WebSocket message now carries a `tick_prices` field:

```json
{
  "type": "playback_frame",
  "bricks_by_chart":      { "1": [...], "2": [...] },
  "live_bricks_by_chart": { "1": {...}, "2": {...} },
  "tick_prices": [1.10234, 1.10241, 1.10229, ...],
  "processed_ticks": 123456,
  ...
}
```

`tick_prices` contains **sampled prices** from the batch (max 120 per frame).
Stride = `max(1, batch_size // 120)` so the array is always small regardless of speed.
The last price in the batch is always appended to guarantee accuracy.

### Frontend (`frontend/static/js/charts.js`)

`RenkoChart.animateTickPrices(tickPrices, frameIntervalMs)` spreads the price
array across the frame interval using `requestAnimationFrame`:

```
Frame budget = 16.7 ms
tick_prices  = [p0, p1, p2, ..., p119]
ms per tick  = 16.7 / 120 ≈ 0.14 ms

RAF step: advance ceil(elapsed / msPerTick) prices
  → update live bar: close=latest, high=max(seen), low=min(seen)
  → update price line colour (green/red)
```

The animation:
- **Cancels** any in-flight animation from the previous frame before starting
- **Never** creates new series data — only calls `series.update()` on the live bar's fixed `time` slot
- **Stops** automatically when all prices are consumed

### Frontend (`frontend/static/js/main.js`)

Both WS-mode (`_wsScheduleRAF`) and worker-mode (`scheduleRAF`) call
`animateTickPrices` after rendering the frame's confirmed + live bricks.

---

## Data flow per frame

```
Backend (Numba batch)
  ├─ confirmed bricks  →  bricks_by_chart
  ├─ live bar snapshot →  live_bricks_by_chart
  └─ sampled prices    →  tick_prices   (max 120 floats)

WebSocket (MsgPack binary)

Frontend RAF
  ├─ appendBrick() for each confirmed brick
  ├─ appendBrick(live)          ← sets initial live bar
  └─ animateTickPrices(prices)  ← sub-frame tick animation loop
```

---

## Performance

| Item | Cost |
|---|---|
| tick_prices at 120 prices | 120 × 8 bytes = 960 bytes (negligible vs MsgPack frame) |
| `series.update()` per RAF step | < 0.1 ms (LightweightCharts internal update) |
| Animation RAF loop | shares browser's display refresh — zero extra threads |

---

## CPU / GPU Booster

When playback starts the process priority is raised to `ABOVE_NORMAL`
(`ABOVE_NORMAL_PRIORITY_CLASS` on Windows, `nice -5` on Linux).
It is restored to `NORMAL` on pause or cancel.

Defined in `backend/routes/api.py`:
- `_boost_playback_start()` — called on play and resume
- `_boost_playback_stop()`  — called on pause, cancel, and loop end
