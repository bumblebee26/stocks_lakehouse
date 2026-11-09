"""News fetcher: parsing NSE RSS, symbol matching, categories, where files land. No network."""
import datetime as dt
import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetchers"))
sys.modules.setdefault("boto3", mock.MagicMock())

import common  # noqa: E402
import databricks_volume  # noqa: E402
import news  # noqa: E402

EQUITY_CSV = (b"SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING\n"
              b"HDFCBANK,HDFC Bank Limited,EQ,08-NOV-1995\n"
              b"M&M,Mahindra & Mahindra Limited,EQ,17-NOV-1995\n"
              b"RELIANCE,Reliance Industries Limited,EQ,29-NOV-1995\n")

ANNOUNCEMENTS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>NSE</title>
<item><title>Reliance Industries Limited</title>
<link>https://nsearchives.nseindia.com/corporate/RELIANCE_09112026141255_SE.pdf</link>
<description>Reliance Industries Limited has informed the Exchange about Credit Rating</description>
<pubDate>09-Nov-2026 14:12:55</pubDate></item>
<item><title>Mahindra &amp; Mahindra Limited</title>
<link>https://nsearchives.nseindia.com/corporate/xyz.pdf</link>
<description>Mahindra &amp; Mahindra Limited has informed the Exchange regarding receipt of an order</description>
<pubDate>Mon, 09 Nov 2026 09:30:00 +0530</pubDate></item>
<item><title>Unknown Small Co Ltd</title><link>https://x/y.pdf</link>
<description>Change in Management</description><pubDate>bad date</pubDate></item>
</channel></rss>"""

BOARD = b"""<rss><channel><item><title>HDFC Bank Limited</title><link>https://x/HDFCBANK_10112026.pdf</link>
<description>HDFC Bank Limited has informed the Exchange that a meeting of the Board of Directors will be
held on 18-Jan-2027 to consider financial results</description><pubDate>10-Nov-2026 10:00:00</pubDate></item>
</channel></rss>"""


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[Key] = Body


class NewsTest(unittest.TestCase):
    def setUp(self):
        news._equity.update(date=None, names={}, symbols=set())
        os.environ["LANDING_BUCKET"] = "test-bucket"
        os.environ["NEWS_VOLUME_PATH"] = "/Volumes/stocks/bronze/news"
        self.s3 = FakeS3()
        common._s3 = self.s3

    def tearDown(self):
        os.environ.pop("PUSH_TO_DATABRICKS", None)

    def fake_get(self, url, headers=None, **kw):
        if url == news.EQUITY_LIST_URL:
            return EQUITY_CSV
        if url.endswith("Online_announcements.xml"):
            return ANNOUNCEMENTS
        if url.endswith("Board_Meetings.xml"):
            return BOARD
        raise RuntimeError("HTTP 403")

    def test_parse_symbols_categories_times(self):
        eq = {"date": None, "names": {}, "symbols": set()}
        with mock.patch.object(news, "http_get", self.fake_get):
            eq = news.equity_map(dt.date(2026, 11, 9))
        items = news.parse_feed("announcement", ANNOUNCEMENTS, eq, "2026-11-09 14:30:00")
        rel, mm, unknown = items
        self.assertEqual((rel["symbol"], rel["category"], rel["published_ist"]),
                         ("RELIANCE", "rating", "2026-11-09 14:12:55"))
        self.assertEqual((mm["symbol"], mm["category"], mm["published_ist"]),
                         ("M&M", "order", "2026-11-09 09:30:00"))   # matched by company name
        self.assertIsNone(unknown["symbol"])
        self.assertEqual(unknown["category"], "management")
        self.assertIsNone(unknown["published_ist"])
        self.assertEqual(len({rel["id"], mm["id"], unknown["id"]}), 3)

    def test_handler_lands_snapshot_and_latest_and_survives_failed_feeds(self):
        with mock.patch.object(news, "http_get", self.fake_get), \
                mock.patch.object(databricks_volume, "upload") as up:
            os.environ["PUSH_TO_DATABRICKS"] = "true"
            out = news.handler({}, None)
        self.assertEqual(out["items"], 4)
        self.assertEqual(out["with_symbol"], 3)
        self.assertEqual(sorted(out["failed_feeds"]), ["corporate_action", "results"])
        keys = sorted(self.s3.objects)
        self.assertEqual(len(keys), 2)
        self.assertTrue(keys[0].startswith("landing/news/date="))
        self.assertTrue(any(k.endswith("/latest.jsonl") for k in keys))
        self.assertTrue(any("/snap_" in k for k in keys))
        lines = [json.loads(x) for x in self.s3.objects[keys[0]].decode().splitlines()]
        self.assertEqual({i["feed"] for i in lines}, {"announcement", "board_meeting"})
        board = [i for i in lines if i["feed"] == "board_meeting"][0]
        self.assertEqual((board["symbol"], board["category"]), ("HDFCBANK", "board_meeting"))
        self.assertEqual(up.call_count, 2)
        self.assertEqual(up.call_args.kwargs["volume"], "/Volumes/stocks/bronze/news")

    def test_all_feeds_failing_is_an_error(self):
        with mock.patch.object(news, "http_get", side_effect=RuntimeError("HTTP 403")):
            with self.assertRaises(RuntimeError):
                news.handler({}, None)

    def test_upload_to_another_volume(self):
        databricks_volume._config = {"host": "https://dbc-x", "token": "T", "volume": "/Volumes/stocks/bronze/landing"}
        databricks_volume._config_loaded_at = float("inf")
        with mock.patch.object(databricks_volume, "http_request") as req:
            path = databricks_volume.upload("date=2026-11-09/latest.jsonl", b"{}", volume="/Volumes/stocks/bronze/news")
        self.assertEqual(path, "/Volumes/stocks/bronze/news/date=2026-11-09/latest.jsonl")
        self.assertTrue(req.call_args_list[0].args[1].endswith("/directories/Volumes/stocks/bronze/news/date=2026-11-09"))


if __name__ == "__main__":
    unittest.main()
