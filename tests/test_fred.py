"""Tests for the FRED fetcher. No network or AWS."""

import datetime as dt
import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetchers"))
sys.modules.setdefault("boto3", mock.MagicMock())

import common  # noqa: E402
import fred  # noqa: E402


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kw):
        self.puts.append(kw)


class ResolveWindow(unittest.TestCase):
    def test_default_is_rolling_window_to_today(self):
        start, end = fred.resolve_window({}, today=dt.date(2026, 11, 8))
        self.assertEqual(end, dt.date(2026, 11, 8))
        self.assertEqual((end - start).days, fred.WINDOW_DAYS)

    def test_explicit_range(self):
        self.assertEqual(fred.resolve_window({"start": "2024-07-01", "end": "2026-11-07"}),
                         (dt.date(2024, 7, 1), dt.date(2026, 11, 7)))

    def test_backwards_range_rejected(self):
        with self.assertRaises(ValueError):
            fred.resolve_window({"start": "2026-11-07", "end": "2026-11-01"})


class Handler(unittest.TestCase):
    def setUp(self):
        os.environ["LANDING_BUCKET"] = "test-bucket"
        os.environ.pop("PUSH_TO_DATABRICKS", None)
        self.s3 = FakeS3()
        common._s3 = self.s3
        fred._api_key = "FREDSECRET"

    def fake_get(self, url, log_url=None):
        self.assertIn("api_key=FREDSECRET", url)
        self.assertNotIn("FREDSECRET", log_url)
        sid = url.split("series_id=")[1].split("&")[0]
        obs = [{"date": "2026-11-05", "value": "100.5"}, {"date": "2026-11-06", "value": "."}]
        if sid == "DEXINUS":
            obs = [{"date": "2026-10-30", "value": "88.12"}]
        return json.dumps({"observations": obs}).encode()

    def test_writes_ndjson_with_metadata_and_skips_missing(self):
        with mock.patch.object(fred, "http_get", side_effect=self.fake_get):
            out = fred.handler({"start": "2026-10-20", "end": "2026-11-07"}, None)
        self.assertEqual(out["status"], "written")
        put = self.s3.puts[0]
        self.assertEqual(put["Key"], "landing/fred/date=2026-11-07/fred_20261020_20261107.json")
        rows = [json.loads(line) for line in put["Body"].decode().splitlines()]
        self.assertEqual(len(rows), len(fred.SERIES))  # one valid value per series, "." skipped
        inr = next(r for r in rows if r["series_id"] == "DEXINUS")
        self.assertEqual(inr, {"series_id": "DEXINUS", "date": "2026-10-30", "value": 88.12,
                               "name": "USD/INR", "region": "India", "asset_class": "fx",
                               "unit": "INR per USD"})

    def test_no_data_writes_nothing(self):
        empty = json.dumps({"observations": []}).encode()
        with mock.patch.object(fred, "http_get", return_value=empty):
            out = fred.handler({"start": "2026-11-01", "end": "2026-11-07"}, None)
        self.assertEqual(out["status"], "no_data")
        self.assertEqual(self.s3.puts, [])


if __name__ == "__main__":
    unittest.main()
