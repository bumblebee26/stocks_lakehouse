"""Tests for the mobile app export (fetchers/app_export.py). No network/AWS."""

import io
import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetchers"))
sys.modules.setdefault("boto3", mock.MagicMock())

import app_export  # noqa: E402

QUERIES = {"desk_kpis": {"sql": "SELECT 1", "fields": ["portfolio_rs"]},
           "world_moves": {"sql": "SELECT 2", "fields": ["market"]}}


class FakeS3:
    def __init__(self, objects):
        self.objects, self.puts = dict(objects), []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, **kw):
        self.puts.append(kw)


class Export(unittest.TestCase):
    def test_failed_query_keeps_previous_rows(self):
        previous = {"datasets": {"world_moves": {"fields": ["market"], "rows": [{"market": "S&P 500"}]}}}

        def query(sql):
            if sql == "SELECT 2":
                raise RuntimeError("warehouse cold")
            return [{"portfolio_rs": 199954.0}]

        datasets, failed = app_export.run_all(QUERIES, previous, query=query)
        self.assertEqual(datasets["desk_kpis"]["rows"], [{"portfolio_rs": 199954.0}])
        self.assertEqual(datasets["world_moves"]["rows"], [{"market": "S&P 500"}])
        self.assertEqual(failed, ["world_moves"])

    def test_handler_writes_under_the_secret_path(self):
        s3 = FakeS3({"config/queries.json": json.dumps(QUERIES).encode()})
        with mock.patch.dict(os.environ, {"APP_BUCKET": "b", "APP_PREFIX": "abc123"}), \
             mock.patch.object(app_export, "_s3", return_value=s3), \
             mock.patch.object(app_export.databricks_sql, "query", return_value=[{"x": 1.0}]), \
             mock.patch.object(app_export, "yahoo_series", return_value=({}, [])), \
             mock.patch.object(app_export, "http_get", side_effect=RuntimeError("offline")), \
             mock.patch.object(app_export.nse_holidays, "http_get", side_effect=RuntimeError("offline")):
            out = app_export.handler({}, None)
        self.assertTrue(out["written"])
        put = s3.puts[0]
        self.assertEqual(put["Key"], "abc123/data/desk.json")
        body = json.loads(put["Body"])
        self.assertEqual(set(body["datasets"]), set(QUERIES) | {"fuel_mumbai", "nse_holidays", "market_status"})
        self.assertEqual(body["failed"], ["nse_holidays", "fuel_mumbai"])
        fuel = body["datasets"]["fuel_mumbai"]["rows"]
        self.assertEqual([r["item"] for r in fuel], ["Petrol", "Diesel", "CNG"])

    def test_old_history_is_weekly(self):
        import datetime as dt
        days = [dt.date(2015, 1, 1) + dt.timedelta(days=i) for i in range(0, 4400)]
        rows = app_export.thin_old([{"date": d.isoformat(), "close": 1.0} for d in days])
        cut = (days[-1].replace(year=days[-1].year - 5)).isoformat()
        old = [r for r in rows if r["date"] < cut]
        self.assertLess(len(old), len([d for d in days if d.isoformat() < cut]) / 6)   # about one a week
        self.assertEqual(len([r for r in rows if r["date"] >= cut]), len([d for d in days if d.isoformat() >= cut]))
        self.assertEqual(rows[-1]["date"], days[-1].isoformat())

    def test_market_status_on_a_holiday_and_after(self):
        import datetime as dt
        payload = {"CM": [{"tradingDate": "24-Nov-2026", "description": "Guru Nanak Jayanti"}]}
        hol, st, failed = app_export.market_status({}, fetch=lambda: payload, today=dt.date(2026, 11, 24))
        row = st["rows"][0]
        self.assertEqual((row["holiday"], row["title"], row["next_open"], failed),
                         ("Guru Nanak Jayanti", "Happy Guru Nanak Jayanti!", "2026-11-25", False))
        self.assertIn("will open at 9:15 AM on 25 Nov", row["message"])

        def down():
            raise RuntimeError("403")
        prev = {"datasets": {"nse_holidays": hol}}
        _, st2, failed2 = app_export.market_status(prev, fetch=down, today=dt.date(2026, 11, 24))
        self.assertEqual((st2["rows"][0]["holiday"], failed2), ("Guru Nanak Jayanti", True))   # last good list
        _, st3, _ = app_export.market_status(prev, fetch=down, today=dt.date(2026, 11, 25))
        self.assertEqual(st3["rows"][0]["holiday"], "")                                        # no card

    def test_fuel_prices_from_the_page_within_the_band(self):
        page = ("<a>Petrol cars from ₹5.99 Lakh</a><h2>Petrol Price in Mumbai</h2><td>&#8377; 111.50</td>"
                "<h2>Diesel</h2> <span>₹97.90/L</span><p>CNG price today: Rs. 9150</p>")
        ds, bad = app_export.fuel_rates({}, fetch=lambda: page.encode(), today="2026-11-24")
        rows = {r["item"]: r for r in ds["rows"]}
        self.assertEqual((rows["Petrol"]["rate"], rows["Petrol"]["as_of"]), ("111.50", "2026-11-24"))
        self.assertEqual(rows["Diesel"]["rate"], "97.90")
        self.assertEqual((rows["CNG"]["rate"], rows["CNG"]["as_of"]), ("91.50", "2026-11-15"))  # 915 rejected
        self.assertTrue(bad)

    def test_fuel_page_down_keeps_the_last_good_prices(self):
        prev = {"datasets": {"fuel_mumbai": {"rows": [{"item": "Petrol", "rate": "112.00", "unit": "Rs / litre",
                                                       "as_of": "2026-11-20", "source": "CarDekho"}]}}}

        def down():
            raise RuntimeError("blocked")
        ds, bad = app_export.fuel_rates(prev, fetch=down)
        self.assertEqual((ds["rows"][0]["rate"], ds["rows"][0]["as_of"]), ("112.00", "2026-11-20"))
        self.assertTrue(bad)

    def test_dry_run_writes_nothing(self):
        s3 = FakeS3({"config/queries.json": json.dumps(QUERIES).encode()})
        with mock.patch.dict(os.environ, {"APP_BUCKET": "b", "APP_PREFIX": "p"}), \
             mock.patch.object(app_export, "_s3", return_value=s3), \
             mock.patch.object(app_export.databricks_sql, "query", return_value=[]), \
             mock.patch.object(app_export, "yahoo_series", return_value=({}, [])), \
             mock.patch.object(app_export, "fuel_rates", return_value=({"fields": [], "rows": []}, False)), \
             mock.patch.object(app_export, "market_status", return_value=({}, {}, False)):
            out = app_export.handler({"dry_run": True}, None)
        self.assertFalse(out["written"])
        self.assertEqual(s3.puts, [])


class YahooSeries(unittest.TestCase):
    def test_series_and_failure_keeps_previous(self):
        bars = [{"date": "2021-11-24", "close": 74.5}, {"date": "2026-11-23", "close": 93.47}]
        ok = lambda sym, rng: (bars, {})  # noqa: E731
        out, failed = app_export.yahoo_series({}, fetch=ok)
        self.assertEqual(failed, [])
        self.assertEqual(out["usdinr_5y"]["rows"][-1], {"date": "2026-11-23", "close": 93.47})
        self.assertIn("Accenture", out["acn_5y"]["label"])

        def boom(sym, rng):
            raise RuntimeError("yahoo down")
        prev = {"datasets": {"usdinr_5y": out["usdinr_5y"]}}
        out2, failed2 = app_export.yahoo_series(prev, fetch=boom)
        self.assertEqual(sorted(failed2), ["acn_5y", "usdinr_5y"])
        self.assertEqual(out2["usdinr_5y"]["rows"], out["usdinr_5y"]["rows"])
        self.assertEqual(out2["acn_5y"]["rows"], [])


if __name__ == "__main__":
    unittest.main()
