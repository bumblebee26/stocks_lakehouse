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
             mock.patch.object(app_export, "yahoo_series", return_value=({}, [])):
            out = app_export.handler({}, None)
        self.assertTrue(out["written"])
        put = s3.puts[0]
        self.assertEqual(put["Key"], "abc123/data/desk.json")
        body = json.loads(put["Body"])
        self.assertEqual(set(body["datasets"]), set(QUERIES) | {"fuel_mumbai"})
        self.assertEqual(body["failed"], [])
        fuel = body["datasets"]["fuel_mumbai"]["rows"]
        self.assertEqual([r["item"] for r in fuel[:3]], ["Petrol", "Diesel", "CNG"])

    def test_fuel_rates_from_the_bucket_win(self):
        own = [{"item": "Petrol", "rate": "105.00", "unit": "Rs / litre", "as_of": "2026-12-01", "source": "you"}]
        s3 = FakeS3({app_export.FUEL_KEY: json.dumps(own).encode()})
        self.assertEqual(app_export.fuel_rates(s3, "b")["rows"], own)
        self.assertEqual(app_export.fuel_rates(FakeS3({}), "b")["rows"][0]["item"], "Petrol")

    def test_dry_run_writes_nothing(self):
        s3 = FakeS3({"config/queries.json": json.dumps(QUERIES).encode()})
        with mock.patch.dict(os.environ, {"APP_BUCKET": "b", "APP_PREFIX": "p"}), \
             mock.patch.object(app_export, "_s3", return_value=s3), \
             mock.patch.object(app_export.databricks_sql, "query", return_value=[]), \
             mock.patch.object(app_export, "yahoo_series", return_value=({}, [])):
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
