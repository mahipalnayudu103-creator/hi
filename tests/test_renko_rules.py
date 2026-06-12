import os
import sys
import unittest
from pathlib import Path

# Add backend directory to sys.path
backend_dir = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(backend_dir))

from services.renko.state import RenkoState
from services.renko.cpu_engine import RenkoState as LegacyRenkoState

class TestRenkoRules(unittest.TestCase):
    def test_md_up_brick_and_reversal(self):
        """Verify the exact calculations from the MD reference."""
        # 1. Traditional/fixed pip brick size
        # Symbol = EURUSD, brick = 1 pip = 0.00010, reversal = 2 boxes
        engine = RenkoState(brick_pips=1.0, pip_size=0.0001, reversal_boxes=2, anchor="first")
        
        # Initial tick
        # Time                         Bid
        # 2026-01-01 22:00:00.000      1.17450
        ticks = [
            (1.17450, "2026-01-01 22:00:00.000", 0),
            (1.17447, "2026-01-01 22:00:01.000", 1),
            (1.17442, "2026-01-01 22:00:02.000", 2),
            (1.17455, "2026-01-01 22:00:03.000", 3),
            (1.17460, "2026-01-01 22:00:04.000", 4),
        ]
        
        confirmed_bricks = []
        for price, time_str, idx in ticks:
            res = engine.process_tick(price, time_str, idx, bid=price, ask=price + 0.0002)
            confirmed_bricks.extend(res)
            
        # Verify first confirmed brick (up brick)
        self.assertEqual(len(confirmed_bricks), 1)
        b = confirmed_bricks[0]
        self.assertEqual(b["direction"], "up")
        self.assertAlmostEqual(b["open"], 1.17450)
        self.assertAlmostEqual(b["close"], 1.17460)
        # Body-only OHLC check
        self.assertAlmostEqual(b["high"], 1.17460)
        self.assertAlmostEqual(b["low"], 1.17450)
        self.assertEqual(b["confirm_tick_index"], 4)
        self.assertEqual(b["tick_count"], 5) # 5 ticks processed (index 0 to 4)
        
        # 2. Reversal Down brick
        # Last Renko close is now 1.17460, direction is 1 (UP)
        # Down reversal trigger = 1.17460 - 2 * 0.0001 = 1.17440
        # Let's send a tick to 1.17440
        res = engine.process_tick(1.17440, "2026-01-01 22:00:05.000", 5, bid=1.17440, ask=1.17442)
        self.assertEqual(len(res), 1)
        rev_b = res[0]
        self.assertEqual(rev_b["direction"], "down")
        # Opposite direction confirmed -> printed brick still moves one brick from previous Renko close
        # i.e., Open = 1.17460 - 0.0001 = 1.17450
        # Close = 1.17460 - 2 * 0.0001 = 1.17440
        self.assertAlmostEqual(rev_b["open"], 1.17450)
        self.assertAlmostEqual(rev_b["close"], 1.17440)
        self.assertAlmostEqual(rev_b["high"], 1.17450)
        self.assertAlmostEqual(rev_b["low"], 1.17440)
        
    def test_live_forming_bar_movement(self):
        """Verify that the live/forming bar moves correctly tick-by-tick using body-only OHLC."""
        engine = RenkoState(brick_pips=1.0, pip_size=0.0001, reversal_boxes=2, anchor="first")
        
        # First tick initializes
        engine.process_tick(1.17450, "2026-01-01 22:00:00.000", 0)
        
        # Send a tick that does not confirm a brick
        engine.process_tick(1.17455, "2026-01-01 22:00:01.000", 1)
        
        live = engine.get_live_brick(1.17455, tick_index=1, seq=0)
        self.assertIsNotNone(live)
        self.assertAlmostEqual(live["open"], 1.17450)
        self.assertAlmostEqual(live["close"], 1.17455)
        # Body-only OHLC for live brick
        self.assertAlmostEqual(live["high"], 1.17455)
        self.assertAlmostEqual(live["low"], 1.17450)
        self.assertTrue(live["is_live"])

    def test_legacy_engine_rules_consistency(self):
        """Verify that the legacy cpu engine behaves exactly like the new streaming engine."""
        engine = LegacyRenkoState(brick_pips=1.0, pip_size=0.0001, reversal_boxes=2, anchor="first")
        
        ticks = [
            (1.17450, "2026-01-01 22:00:00.000"),
            (1.17447, "2026-01-01 22:00:01.000"),
            (1.17442, "2026-01-01 22:00:02.000"),
            (1.17455, "2026-01-01 22:00:03.000"),
            (1.17460, "2026-01-01 22:00:04.000"),
        ]
        
        confirmed = []
        for price, time_str in ticks:
            res = engine.process_tick(price, time_str, bid=price, ask=price)
            confirmed.extend(res)
            
        self.assertEqual(len(confirmed), 1)
        b = confirmed[0]
        self.assertEqual(b["direction"], "up")
        self.assertAlmostEqual(b["open"], 1.17450)
        self.assertAlmostEqual(b["close"], 1.17460)
        self.assertAlmostEqual(b["high"], 1.17460)
        self.assertAlmostEqual(b["low"], 1.17450)
        
        # Reversal Down
        res = engine.process_tick(1.17440, "2026-01-01 22:00:05.000", bid=1.17440, ask=1.17440)
        self.assertEqual(len(res), 1)
        rev_b = res[0]
        self.assertEqual(rev_b["direction"], "down")
        self.assertAlmostEqual(rev_b["open"], 1.17450)
        self.assertAlmostEqual(rev_b["close"], 1.17440)
        self.assertAlmostEqual(rev_b["high"], 1.17450)
        self.assertAlmostEqual(rev_b["low"], 1.17440)

if __name__ == "__main__":
    unittest.main()
