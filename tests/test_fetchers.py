"""Unit tests for the fetchers. No network or AWS needed.

Run from the repo root:  python -m unittest discover -s tests -v
"""

import datetime as dt
import io
import json
import os
import sys
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetchers"))
sys.modules.setdefault("boto3", mock.MagicMock())  # tests don't need the real SDK

import common  # noqa: E402
import databricks_volume  # noqa: E402
import massive  # noqa: E402
import nse  # noqa: E402
import sync  # noqa: E402

D = dt.date


class ResolveDates(unittest.TestCase):
    def test_default_when_event_empty(self):
        self.assertEqual(common.resolve_dates({}, lambda: D(2026, 11, 6)), [D(2026, 11, 6)])
        self.assertEqual(common.resolve_dates(None, lambda: D(2026, 11, 6)), [D(2026, 11, 6)])

    def test_single_date(self):
        self.assertEqual(common.resolve_dates({"date": "2026-11-05"}, None), [D(2026, 11, 5)])

    def test_range_skips_weekends(self):
        got = common.resolve_dates({"start": "2026-11-06", "end": "2026-11-10"}, None)
        self.assertEqual(got, [D(2026, 11, 6), D(2026, 11, 9), D(2026, 11, 10)])

    def test_range_backwards_rejected(self):
        with self.assertRaises(ValueError):
            common.resolve_dates({"start": "2026-11-10", "end": "2026-11-06"}, None)


class MassiveDefaultDate(unittest.TestCase):
    NY = massive.NEW_YORK

    def test_after_close_uses_same_day(self):
        now = dt.datetime(2026, 11, 6, 18, 0, tzinfo=self.NY)  # Friday 6pm ET
        self.assertEqual(massive.default_date(now), D(2026, 11, 6))

    def test_before_ready_uses_previous_day(self):
        now = dt.datetime(2026, 11, 6, 10, 0, tzinfo=self.NY)  # Friday 10am ET
        self.assertEqual(massive.default_date(now), D(2026, 11, 5))

    def test_scheduled_saturday_run_gets_friday(self):
        # Schedule fires Sat 04:30 IST = Fri 18:00 ET (EST, UTC-5)
        ist = dt.datetime(2026, 11, 7, 4, 30, tzinfo=massive.ZoneInfo("Asia/Kolkata"))
        self.assertEqual(massive.default_date(ist.astimezone(self.NY)), D(2026, 11, 6))

    def test_tuesday_run_gets_monday(self):
        ist = dt.datetime(2026, 11, 10, 4, 30, tzinfo=massive.ZoneInfo("Asia/Kolkata"))
        self.assertEqual(massive.default_date(ist.astimezone(self.NY)), D(2026, 11, 9))

    def test_weekend_rolls_back_to_friday(self):
        now = dt.datetime(2026, 11, 8, 12, 0, tzinfo=self.NY)  # Sunday
        self.assertEqual(massive.default_date(now), D(2026, 11, 6))


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kw):
        self.puts.append(kw)


class NseFetch(unittest.TestCase):
    def setUp(self):
        os.environ["LANDING_BUCKET"] = "test-bucket"
        self.s3 = FakeS3()
        common._s3 = self.s3

    def test_writes_zip_to_dated_prefix(self):
        with mock.patch.object(nse, "http_get", return_value=b"PK\x03\x04data") as get:
            out = nse.handler({"date": "2026-11-06"}, None)
        self.assertIn("20261106", get.call_args_list[0].args[0])
        self.assertEqual(out["written"], 1)
        self.assertEqual(self.s3.puts[0]["Key"],
                         "landing/nse/date=2026-11-06/bhavcopy_cm_20261106.csv.zip")
        self.assertEqual(out["results"][0]["delivery"], "empty")     # the fake body isn't a delivery CSV

    def test_delivery_file(self):
        csv_text = ("SYMBOL, SERIES, DATE1, PREV_CLOSE, CLOSE_PRICE, TTL_TRD_QNTY, DELIV_QTY, DELIV_PER\n"
                    "TCS, EQ, 06-Nov-2026, 2400, 2410, 1000000, 550000, 55.00\n"
                    "SOMEBOND, N1, 06-Nov-2026, 100, 100, 10, -, -\n")
        rows = nse.parse_delivery(csv_text)
        self.assertEqual(rows, [{"symbol": "TCS", "series": "EQ", "deliv_qty": 550000.0, "deliv_per": 55.0}])
        bodies = {"cm": b"PK\x03\x04data", "sec": csv_text.encode()}
        with mock.patch.object(nse, "http_get", side_effect=lambda url, h: bodies["sec" if "sec_" in url else "cm"]):
            out = nse.handler({"date": "2026-11-06"}, None)
        self.assertEqual(out["results"][0]["delivery"], "written (1 rows)")
        self.assertEqual(self.s3.puts[1]["Key"], "landing/nse_delivery/date=2026-11-06/delivery_20261106.jsonl")

    def test_index_file(self):
        csv_text = ("Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,"
                    "Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield\n"
                    "Nifty 50,19-11-2026,\"26,100.50\",\"26,210.00\",\"26,050.25\",\"26,180.75\",80.25,0.31,-,-,22.1,3.5,1.2\n"
                    "India VIX,19-11-2026,14.20,15.10,13.90,14.85,0.65,4.58,-,-,-,-,-\n"
                    "Broken,19-11-2026,-,-,-,-,-,-,-,-,-,-,-\n")
        rows = nse.parse_indices(csv_text)
        self.assertEqual(rows[0], {"index_name": "Nifty 50", "open": 26100.5, "high": 26210.0, "low": 26050.25,
                                   "close": 26180.75, "change_pct": 0.31})
        self.assertEqual(rows[1]["close"], 14.85)
        self.assertEqual(len(rows), 2)                                   # no close: dropped
        bodies = {"cm": b"PK\x03\x04data", "ind": csv_text.encode(), "sec": b""}
        with mock.patch.object(nse, "http_get",
                               side_effect=lambda url, h: bodies["ind" if "ind_close" in url else
                                                                 "sec" if "sec_" in url else "cm"]):
            out = nse.handler({"date": "2026-11-19"}, None)
        self.assertEqual(out["results"][0]["indices"], "written (2 rows, India VIX 14.85)")
        self.assertIn("landing/nse_indices/date=2026-11-19/indices_20261119.jsonl", [p["Key"] for p in self.s3.puts])

    def test_fno_list(self):
        import nse_fo
        text = ("UNDERLYING                    ,SYMBOL    ,NOV-26,DEC-26\n"
                "Derivatives on Individual Securities,,,\n"
                "NIFTY 50                      ,NIFTY     ,75,75\n"
                "TATA CONSULTANCY SERV LT      ,TCS       ,175,175\n")
        rows = nse_fo.parse(text, "2026-11-16")
        self.assertEqual(rows, [{"symbol": "TCS", "underlying": "TATA CONSULTANCY SERV LT", "as_of": "2026-11-16"}])

    def test_fno_list_real_layout(self):
        # NSE's file as of 16 Nov 2026: index rows (some not in INDEX_ROWS) above a "Symbol" divider row
        import nse_fo
        text = ("UNDERLYING                         ,SYMBOL    ,NOV-26    ,DEC-26\n"
                "NIFTY 50                           ,NIFTY     ,65        ,65\n"
                "NIFTY FPI 150                      ,NIFTYFPI  ,1100      ,1100\n"
                "Derivatives on Individual Securities,Symbol    ,NOV-26    ,DEC-26\n"
                "ADANI ENERGY SOLUTION LTD          ,ADANIENSOL,675       ,675\n")
        self.assertEqual([r["symbol"] for r in nse_fo.parse(text, "2026-11-16")], ["ADANIENSOL"])

    def test_holiday_404_is_not_an_error(self):
        with mock.patch.object(nse, "http_get", side_effect=common.NotFound("x")):
            out = nse.handler({"date": "2026-11-06"}, None)
        self.assertEqual(out["results"][0]["status"], "no_file")
        self.assertEqual(self.s3.puts, [])

    def test_html_block_page_raises(self):
        with mock.patch.object(nse, "http_get", return_value=b"<html>Access Denied</html>"):
            with self.assertRaises(RuntimeError):
                nse.handler({"date": "2026-11-06"}, None)

    def test_weekend_skipped_without_request(self):
        with mock.patch.object(nse, "http_get") as get:
            out = nse.handler({"date": "2026-11-07"}, None)
        get.assert_not_called()
        self.assertEqual(out["results"][0]["status"], "skipped_weekend")


class MassiveFetch(unittest.TestCase):
    def setUp(self):
        os.environ["LANDING_BUCKET"] = "test-bucket"
        self.s3 = FakeS3()
        common._s3 = self.s3
        massive._api_key = "SECRET123"

    def test_writes_json_and_redacts_key_in_logs(self):
        body = json.dumps({"status": "OK", "resultsCount": 2, "results": [{}, {}]}).encode()
        with mock.patch.object(massive, "http_get", return_value=body) as get:
            out = massive.handler({"date": "2026-11-06"}, None)
        url = get.call_args.args[0]
        self.assertIn("/2026-11-06?", url)
        self.assertIn("apiKey=SECRET123", url)
        self.assertNotIn("SECRET123", get.call_args.kwargs["log_url"])
        self.assertEqual(out["results"][0]["tickers"], 2)
        self.assertEqual(self.s3.puts[0]["Key"],
                         "landing/massive/date=2026-11-06/grouped_daily_20261106.json")

    def test_scheduled_run_catches_up_missing_days(self):
        landed = {"massive/date=2026-11-05/grouped_daily_20261105.json"}
        with mock.patch.object(databricks_volume, "enabled", return_value=True), \
                mock.patch.object(databricks_volume, "exists", side_effect=lambda rel: rel in landed):
            self.assertEqual(massive.scheduled_dates(D(2026, 11, 9)), [D(2026, 11, 6), D(2026, 11, 9)])
            landed |= {"massive/date=2026-11-06/grouped_daily_20261106.json",
                       "massive/date=2026-11-09/grouped_daily_20261109.json"}
            self.assertEqual(massive.scheduled_dates(D(2026, 11, 9)), [])  # 07:30 retry after a good 04:30

    def test_scheduled_run_without_volume_or_check_fetches_latest(self):
        with mock.patch.object(databricks_volume, "enabled", return_value=False):
            self.assertEqual(massive.scheduled_dates(D(2026, 11, 9)), [D(2026, 11, 9)])
        with mock.patch.object(databricks_volume, "enabled", return_value=True), \
                mock.patch.object(databricks_volume, "exists", side_effect=RuntimeError("401")):
            self.assertEqual(massive.scheduled_dates(D(2026, 11, 9)), [D(2026, 11, 9)])

    def test_zero_results_not_written(self):
        body = json.dumps({"status": "OK", "resultsCount": 0}).encode()
        with mock.patch.object(massive, "http_get", return_value=body):
            out = massive.handler({"date": "2026-11-06"}, None)
        self.assertEqual(out["results"][0]["status"], "no_data")
        self.assertEqual(self.s3.puts, [])


class FakeContext:
    """Lambda context whose remaining time drops by step_ms on every check."""

    function_name = "stocks-fetch-test"

    def __init__(self, start_ms, step_ms):
        self.ms, self.step = start_ms, step_ms

    def get_remaining_time_in_millis(self):
        self.ms -= self.step
        return self.ms


class SelfContinuation(unittest.TestCase):
    def setUp(self):
        self.invoked = mock.MagicMock()
        common._lambda = self.invoked
        self.fetch = lambda d: {"date": d.isoformat(), "status": "written"}
        self.dates = common.resolve_dates({"start": "2026-11-02", "end": "2026-11-13"}, None)

    def test_hands_off_remaining_dates_when_time_runs_low(self):
        # 100s left, each check costs 30s, reserve 20s -> processes 3 dates, then hands off
        with mock.patch("time.sleep"):
            out = common.run_dates("t", self.dates, self.fetch, delay_seconds=0,
                                   context=FakeContext(100_000, 30_000), reserve_seconds=20)
        self.assertEqual(out["processed"], 3)
        self.assertEqual(out["continued_from"], "2026-11-05")
        call = self.invoked.invoke.call_args.kwargs
        self.assertEqual(call["FunctionName"], "stocks-fetch-test")
        self.assertEqual(call["InvocationType"], "Event")
        self.assertEqual(json.loads(call["Payload"]), {"start": "2026-11-05", "end": "2026-11-13"})

    def test_no_handoff_when_time_is_plenty(self):
        with mock.patch("time.sleep"):
            out = common.run_dates("t", self.dates, self.fetch, delay_seconds=0,
                                   context=FakeContext(900_000, 1_000), reserve_seconds=20)
        self.assertEqual(out["processed"], len(self.dates))
        self.assertIsNone(out["continued_from"])
        self.invoked.invoke.assert_not_called()

    def test_always_processes_at_least_one_date(self):
        with mock.patch("time.sleep"):
            out = common.run_dates("t", self.dates, self.fetch, delay_seconds=0,
                                   context=FakeContext(1_000, 1_000), reserve_seconds=60)
        self.assertEqual(out["processed"], 1)


class HttpGet(unittest.TestCase):
    def _http_error(self, code):
        return urllib.error.HTTPError("u", code, "x", {}, io.BytesIO(b""))

    def test_404_raises_not_found(self):
        with mock.patch("urllib.request.urlopen", side_effect=self._http_error(404)):
            with self.assertRaises(common.NotFound):
                common.http_get("https://example.test/f")

    def test_retries_then_succeeds_on_429(self):
        ok = mock.MagicMock()
        ok.__enter__.return_value.read.return_value = b"done"
        with mock.patch("urllib.request.urlopen", side_effect=[self._http_error(429), ok]), \
             mock.patch("time.sleep") as sleep:
            self.assertEqual(common.http_get("https://example.test/f", backoff_seconds=1), b"done")
        sleep.assert_called_once()

    def test_403_fails_fast(self):
        with mock.patch("urllib.request.urlopen", side_effect=self._http_error(403)), \
             mock.patch("time.sleep") as sleep:
            with self.assertRaises(RuntimeError):
                common.http_get("https://example.test/f")
        sleep.assert_not_called()


class DatabricksPush(unittest.TestCase):
    def setUp(self):
        databricks_volume._config = {"host": "https://dbc-x.cloud.databricks.com",
                                     "token": "TOKEN", "volume": "/Volumes/stocks/bronze/landing"}
        databricks_volume._config_loaded_at = float("inf")  # never expire during tests
        os.environ["LANDING_BUCKET"] = "test-bucket"
        self.s3 = FakeS3()
        common._s3 = self.s3

    def tearDown(self):
        os.environ.pop("PUSH_TO_DATABRICKS", None)

    def test_upload_creates_dir_then_puts_file(self):
        with mock.patch.object(databricks_volume, "http_request") as req:
            path = databricks_volume.upload("nse/date=2026-11-06/f.zip", b"PK..")
        self.assertEqual(path, "/Volumes/stocks/bronze/landing/nse/date=2026-11-06/f.zip")
        (m1, u1), (m2, u2) = [(c.args[0], c.args[1]) for c in req.call_args_list]
        self.assertEqual((m1, u1), ("PUT", "https://dbc-x.cloud.databricks.com/api/2.0/fs/directories"
                                           "/Volumes/stocks/bronze/landing/nse/date=2026-11-06"))
        self.assertEqual(m2, "PUT")
        self.assertTrue(u2.endswith("/api/2.0/fs/files/Volumes/stocks/bronze/landing/nse/"
                                    "date=2026-11-06/f.zip?overwrite=true"))
        self.assertEqual(req.call_args_list[1].kwargs["data"], b"PK..")
        self.assertEqual(req.call_args_list[1].kwargs["headers"]["Authorization"], "Bearer TOKEN")

    def test_csv_zip_is_uploaded_unzipped(self):
        import zipfile
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("BhavCopy_NSE_CM_0_0_0_20261106_F_0000.csv", "TckrSymb,ClsPric\nINFY,1500\n")
        with mock.patch.object(databricks_volume, "http_request") as req:
            path = databricks_volume.upload("nse/date=2026-11-06/bhavcopy_cm_20261106.csv.zip",
                                            buf.getvalue())
        self.assertTrue(path.endswith("nse/date=2026-11-06/bhavcopy_cm_20261106.csv"))
        self.assertEqual(req.call_args_list[1].kwargs["data"], b"TckrSymb,ClsPric\nINFY,1500\n")

    def test_exists_checks_unzipped_name(self):
        with mock.patch.object(databricks_volume, "http_request", return_value=b"") as req:
            databricks_volume.exists("nse/date=2026-11-06/b.csv.zip")
        self.assertTrue(req.call_args.args[1].endswith("/nse/date=2026-11-06/b.csv"))

    def test_exists_maps_404_to_false(self):
        with mock.patch.object(databricks_volume, "http_request", side_effect=common.NotFound("x")):
            self.assertFalse(databricks_volume.exists("nse/a.zip"))
        with mock.patch.object(databricks_volume, "http_request", return_value=b""):
            self.assertTrue(databricks_volume.exists("nse/a.zip"))

    def test_put_landing_pushes_only_when_enabled(self):
        with mock.patch.object(databricks_volume, "upload") as up:
            common.put_landing("nse", D(2026, 11, 6), "f.zip", b"x", "application/zip")
            up.assert_not_called()
            os.environ["PUSH_TO_DATABRICKS"] = "true"
            common.put_landing("nse", D(2026, 11, 6), "f.zip", b"x", "application/zip")
            up.assert_called_once_with("nse/date=2026-11-06/f.zip", b"x")
        self.assertEqual(self.s3.puts[0]["Key"], "landing/nse/date=2026-11-06/f.zip")


class FakeListingS3:
    def __init__(self, keys):
        self.keys = keys
        self.list_args = None

    def get_paginator(self, name):
        outer = self

        class P:
            def paginate(self, **kw):
                outer.list_args = kw
                after = kw.get("StartAfter", "")
                yield {"Contents": [{"Key": k} for k in outer.keys if k > after]}
        return P()

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(Key.encode())}


class SyncToVolume(unittest.TestCase):
    KEYS = ["landing/massive/date=2026-11-06/g.json",
            "landing/nse/date=2026-11-05/a.zip",
            "landing/nse/date=2026-11-06/b.zip"]

    def setUp(self):
        os.environ["LANDING_BUCKET"] = "test-bucket"
        common._s3 = FakeListingS3(self.KEYS)
        self.lam = mock.MagicMock()
        common._lambda = self.lam

    def test_copies_missing_skips_existing(self):
        existing = {"nse/date=2026-11-05/a.zip"}
        with mock.patch.object(databricks_volume, "exists", side_effect=lambda r: r in existing), \
             mock.patch.object(databricks_volume, "upload") as up:
            out = sync.handler({}, None)
        self.assertEqual(out["copied"], 2)
        self.assertEqual(out["skipped_existing"], 1)
        self.assertEqual([c.args[0] for c in up.call_args_list],
                         ["massive/date=2026-11-06/g.json", "nse/date=2026-11-06/b.zip"])

    def test_continues_after_last_done_key(self):
        # No check before the first key; checks before keys 2 and 3 see 120s then 40s left.
        ctx = FakeContext(200_000, 80_000)
        with mock.patch.object(databricks_volume, "exists", return_value=False), \
             mock.patch.object(databricks_volume, "upload"):
            out = sync.handler({"prefix": "landing/"}, ctx)
        self.assertEqual(out["copied"], 2)
        self.assertEqual(out["continued_after"], self.KEYS[1])
        payload = json.loads(self.lam.invoke.call_args.kwargs["Payload"])
        self.assertEqual(payload["start_after"], self.KEYS[1])

    def test_rejects_prefix_outside_landing(self):
        with self.assertRaises(ValueError):
            sync.handler({"prefix": "other/"}, None)


if __name__ == "__main__":
    unittest.main()
