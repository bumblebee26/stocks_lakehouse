"""Tests for the technical scanner's pandas block in pipeline/research/gold.py (loaded on its own, no Spark needed)."""

import os
import re
import unittest

import numpy as np
import pandas as pd

SRC = open(os.path.join(os.path.dirname(__file__), "..", "pipeline", "research", "gold.py"), encoding="utf-8").read()
BLOCK = re.search(r"# --- technicals \(pure pandas\) ---\n(.*?)# --- end technicals ---", SRC, re.S).group(1)
T = {}
exec(compile(BLOCK, "research/gold.py:technicals", "exec"), T)  # noqa: S102 - our own source


def bars(closes, volume=1000.0, prev=None):
    closes = list(map(float, closes))
    n = len(closes)
    df = pd.DataFrame({
        "trade_date": pd.date_range("2025-01-01", periods=n, freq="B").date,
        "open": closes, "high": [c * 1.01 for c in closes], "low": [c * 0.99 for c in closes], "close": closes,
        "volume": [volume] * n if np.isscalar(volume) else list(volume),
    })
    df["prev_close"] = prev if prev is not None else df["close"].shift(1).fillna(df["close"].iloc[0])
    return df


class Indicators(unittest.TestCase):
    def test_sma_and_rsi_extremes(self):
        df = T["add_indicators"](bars(range(1, 61)))
        self.assertAlmostEqual(df["sma20"].iloc[-1], np.mean(range(41, 61)))
        self.assertAlmostEqual(df["rsi14"].iloc[-1], 100.0)   # only gains
        down = T["add_indicators"](bars(range(60, 0, -1)))
        self.assertAlmostEqual(down["rsi14"].iloc[-1], 0.0)

    def test_macd_positive_in_uptrend_and_adx_strong(self):
        df = T["add_indicators"](bars([100 * 1.01 ** i for i in range(80)]))
        self.assertGreater(df["macd"].iloc[-1], 0)
        self.assertGreater(df["adx14"].iloc[-1], 20)

    def test_volume_ratio_uses_previous_days(self):
        vol = [1000.0] * 40 + [3000.0]
        df = T["add_indicators"](bars(range(100, 141), volume=vol))
        self.assertAlmostEqual(df["vol_ratio"].iloc[-1], 3.0)

    def test_swing_points(self):
        closes = [100, 101, 102, 103, 104, 110, 104, 103, 102, 101, 100, 99]
        df = T["add_indicators"](bars(closes))
        self.assertEqual(df["swing_high"].dropna().index.tolist(), [5])


class SplitAdjust(unittest.TestCase):
    def test_one_for_one_bonus_halves_earlier_prices(self):
        closes = [200.0] * 5 + [101.0] * 5
        prev = [200.0] * 5 + [100.0] + [101.0] * 4   # NSE quotes yesterday's close adjusted on the ex-date
        df = T["adjust_prices"](bars(closes, prev=prev))
        self.assertAlmostEqual(df["close"].iloc[0], 100.0)
        self.assertAlmostEqual(df["volume"].iloc[0], 2000.0)
        self.assertAlmostEqual(df["close"].iloc[-1], 101.0)
        self.assertTrue(df["adjusted"].iloc[0])
        self.assertFalse(df["adjusted"].iloc[-1])

    def test_ordinary_move_not_adjusted(self):
        df = T["adjust_prices"](bars([100, 102, 101, 103]))
        self.assertEqual(df["close"].tolist(), [100, 102, 101, 103])


class Screen(unittest.TestCase):
    def test_short_history_gives_nothing(self):
        self.assertEqual(len(T["technicals"](bars(range(1, 20)))), 0)

    def test_extended_stock_is_not_strict(self):
        df = T["technicals"](bars([100 * 1.02 ** i for i in range(260)]))
        last = df.iloc[-1]
        self.assertEqual(last["setup"], "extended")      # RSI far above 70: no chasing
        self.assertEqual(last["stance"], "wait/avoid")
        self.assertFalse(last["strict"])
        self.assertIn("RSI outside 50-65", last["misses"])

    def test_breakout_levels(self):
        rng = np.random.default_rng(1)
        closes = list(100 + np.cumsum(rng.normal(0.08, 0.6, 259)))
        closes.append(max(closes[-20:]) * 1.02)
        vol = [1000.0] * 259 + [2500.0]
        df = T["technicals"](bars(closes, volume=vol))
        last = df.iloc[-1]
        if last["setup"] == "20-day breakout":
            self.assertEqual(last["stance"], "buy-on-dip")
            self.assertLess(last["stop"], last["entry_low"])
            self.assertGreater(last["target2"], last["target1"] - 1e-9)
            self.assertGreater(last["rr"], 0)
        else:   # random path ran extended; it must then say so
            self.assertEqual(last["setup"], "extended")
        self.assertTrue(last["volume_ok"])

    def test_levels_always_consistent(self):
        for seed in range(8):
            rng = np.random.default_rng(seed)
            closes = 100 + np.cumsum(rng.normal(0.05, 1.0, 300))
            vol = rng.uniform(500, 3000, 300)
            df = T["technicals"](bars(closes, volume=vol))
            lv = df[df["entry_high"].notna()]
            self.assertTrue(len(lv) > 0)
            self.assertTrue((lv["stop"] < lv["entry_low"]).all())
            self.assertTrue((lv["entry_low"] <= lv["entry_high"]).all())
            self.assertTrue((lv["target1"] > lv["entry_high"]).all())
            self.assertTrue((lv["target2"] > lv["target1"]).all())
            self.assertTrue((df.loc[df["strict"], "rr"] >= 2).all())

    def test_filters_count_and_misses(self):
        df = T["technicals"](bars([100 + 0.1 * i for i in range(260)]))
        last = df.iloc[-1]
        self.assertEqual(last["filters_passed"], int(sum(bool(last[k]) for k in T["TECH_FILTERS"])))
        self.assertIn("volume under 1.5x", last["misses"])
        self.assertFalse(last["strict"])


if __name__ == "__main__":
    unittest.main()
