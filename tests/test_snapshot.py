"""Tests for the pre-open snapshot (fetchers/snapshot.py). No network/AWS."""

import datetime as dt
import json
import os
import sys
import unittest
from unittest import mock
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetchers"))
sys.modules.setdefault("boto3", mock.MagicMock())

import snapshot  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
NOW = dt.datetime(2026, 11, 18, 8, 45, tzinfo=IST)
NOW_TS = int(NOW.timestamp())


def candles(n, start=100.0, step=1.0, last="2026-11-17"):
    end = dt.date.fromisoformat(last)
    days = [end - dt.timedelta(days=i) for i in range(n)][::-1]
    return [{"date": d.isoformat(), "open": 0, "high": 0, "low": 0, "close": start + i * step}
            for i, d in enumerate(days)]


def meta(tz="America/New_York", start=None, end=None):
    m = {"exchangeTimezoneName": tz}
    if start:
        m["currentTradingPeriod"] = {"regular": {"start": start, "end": end}}
    return m


class Row(unittest.TestCase):
    SPEC = ("^GSPC", "SP500", "S&P 500", "US", "equity_index", "index")

    def test_changes_from_yahoo_candles(self):
        r = snapshot.row(self.SPEC, candles(30), meta(), NOW_TS, "2026-11-18 08:45:00")
        self.assertEqual(r["instrument"], "SP500")
        self.assertEqual(r["obs_date"], "2026-11-17")
        self.assertEqual(r["value"], 129.0)
        self.assertAlmostEqual(r["change_pct"], (129 / 128 - 1) * 100)
        self.assertAlmostEqual(r["change_5d_pct"], (129 / 124 - 1) * 100)
        self.assertAlmostEqual(r["change_20d_pct"], (129 / 109 - 1) * 100)
        self.assertEqual(r["reading"], "close")

    def test_short_history_leaves_long_changes_empty(self):
        r = snapshot.row(self.SPEC, candles(4), meta(), NOW_TS, "x")
        self.assertIsNone(r["change_5d_pct"])
        self.assertIsNone(r["change_20d_pct"])
        self.assertIsNone(snapshot.row(self.SPEC, candles(1), meta(), NOW_TS, "x"))

    def test_repeated_last_date_keeps_the_later_candle(self):
        bars = candles(10) + [{"date": "2026-11-17", "open": 0, "high": 0, "low": 0, "close": 200.0}]
        r = snapshot.row(self.SPEC, bars, meta(), NOW_TS, "x")
        self.assertEqual(r["value"], 200.0)
        self.assertAlmostEqual(r["change_pct"], (200 / 108 - 1) * 100)

    def test_tokyo_session_in_progress_is_live(self):
        tokyo = ZoneInfo("Asia/Tokyo")
        start = int(dt.datetime(2026, 11, 18, 9, 0, tzinfo=tokyo).timestamp())
        end = int(dt.datetime(2026, 11, 18, 15, 30, tzinfo=tokyo).timestamp())
        spec = ("^N225", "NIKKEI225", "Nikkei 225", "Japan", "equity_index", "index")
        r = snapshot.row(spec, candles(25, last="2026-11-18"), meta("Asia/Tokyo", start, end), NOW_TS, "x")
        self.assertEqual(r["reading"], "live")
        self.assertEqual(r["obs_date"], "2026-11-18")

    def test_before_the_open_the_last_close_is_not_live(self):
        ny = ZoneInfo("America/New_York")
        start = int(dt.datetime(2026, 11, 18, 9, 30, tzinfo=ny).timestamp())  # later today in New York
        end = int(dt.datetime(2026, 11, 18, 16, 0, tzinfo=ny).timestamp())
        r = snapshot.row(self.SPEC, candles(25), meta("America/New_York", start, end), NOW_TS, "x")
        self.assertEqual(r["reading"], "close")

    def test_old_style_yield_quote_is_scaled(self):
        spec = ("^TNX", "DGS10", "US 10-year Treasury yield", "US", "rates", "percent")
        r = snapshot.row(spec, candles(25, start=42.0, step=0.1), meta(), NOW_TS, "x")
        self.assertAlmostEqual(r["value"], 4.44)


class Collect(unittest.TestCase):
    def test_fred_ids_take_fred_metadata(self):
        specs = {s[1]: s for s in snapshot.instruments()}
        self.assertEqual(specs["DEXINUS"][2:], ("USD/INR", "India", "fx", "INR per USD"))
        self.assertEqual(specs["DCOILBRENTEU"][0], "BZ=F")
        self.assertIn("KOSPI", specs)
        self.assertEqual(len(specs), len(snapshot.instruments()))  # ids are unique

    def test_a_failed_symbol_is_skipped(self):
        def fetch(sym, rng):
            if sym == "^HSI":
                raise RuntimeError("HTTP 500")
            return candles(25), meta()

        rows = snapshot.collect(NOW, fetch=fetch, workers=2)
        self.assertEqual(len(rows), len(snapshot.instruments()) - 1)
        self.assertNotIn("HSI", {r["instrument"] for r in rows})

    def test_save_writes_ndjson_to_premarket(self):
        rows = [{"instrument": "SP500", "value": 1.0}]
        with mock.patch.object(snapshot, "collect", return_value=rows), \
             mock.patch.object(snapshot, "put_landing", return_value="landing/premarket/x") as put:
            out = snapshot.save(NOW)
        source, day, name, body, ctype = put.call_args.args
        self.assertEqual((source, day, name, ctype), ("premarket", NOW.date(), "yahoo_0845.jsonl", "application/x-ndjson"))
        self.assertEqual(json.loads(body.decode().splitlines()[0])["instrument"], "SP500")
        self.assertEqual(out["rows"], 1)

    def test_nothing_from_yahoo_writes_nothing(self):
        with mock.patch.object(snapshot, "collect", return_value=[]), \
             mock.patch.object(snapshot, "put_landing") as put:
            out = snapshot.save(NOW)
        put.assert_not_called()
        self.assertEqual(out["rows"], 0)


if __name__ == "__main__":
    unittest.main()
