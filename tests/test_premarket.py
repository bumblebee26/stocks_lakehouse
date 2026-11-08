"""Tests for the pre-market brief, Databricks SQL client and Telegram sender. No network/AWS."""

import datetime as dt
import json
import os
import re
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetchers"))
sys.modules.setdefault("boto3", mock.MagicMock())

import databricks_sql  # noqa: E402
import premarket  # noqa: E402
import telegram  # noqa: E402


def g(instrument, obs_date, value, change_pct, change_5d_pct=None, change_abs=None):
    return {"instrument": instrument, "obs_date": obs_date, "value": value, "change_pct": change_pct,
            "change_5d_pct": change_5d_pct, "change_abs": change_abs}


SAMPLE = {
    "us_bars": [
        {"trade_date": "2026-11-06", "symbol": "SPY", "close": 650.0, "return_pct": 1.42},
        {"trade_date": "2026-11-06", "symbol": "QQQ", "close": 560.0, "return_pct": 1.91},
        {"trade_date": "2026-11-06", "symbol": "INFY", "close": 21.0, "return_pct": 2.3},
        {"trade_date": "2026-11-06", "symbol": "HDB", "close": 70.0, "return_pct": -0.4},
    ],
    "us_breadth": [{"trade_date": "2026-11-06", "advances": 3210.0, "declines": 1980.0,
                    "pct_above_ma20": 61.6, "median_return_pct": 0.5}],
    "global": [
        g("NIKKEI225", "2026-11-06", 50120.0, -5.96, -3.1, -3180.0),
        g("EWY", "2026-11-06", 71.0, 4.07, 6.2, 2.8),
        g("FXI", "2026-11-06", 38.0, 0.03, 1.1, 0.01),
        g("VGK", "2026-11-05", 72.1, -1.26, -2.0, -0.9),
        g("INDA", "2026-11-06", 52.0, 0.4, 0.9, 0.2),
        g("VIXCLS", "2026-11-06", 28.2, 70.0, None, 11.6),
        g("DGS10", "2026-11-06", 4.84, -1.2, None, -0.06),
        g("DCOILBRENTEU", "2026-11-03", 107.0, -0.3, None, -0.3),
        g("DEXINUS", "2026-10-30", 94.94, 0.1, None, 0.09),
        g("DEXUSEU", "2026-10-30", 1.0712, -0.2, None, -0.002),
    ],
    "linked": [
        {"instrument": "EWY", "corr_with_india_gap": 0.41, "sessions": 380.0},
        {"instrument": "NIKKEI225", "corr_with_india_gap": 0.37, "sessions": 370.0},
    ],
    "summary": [
        {"us_move": "US up big", "sessions": 48.0, "avg_us_return_pct": 1.6, "avg_india_gap_pct": 0.35,
         "avg_india_return_pct": 0.2, "pct_india_up_days": 64.0, "pct_gap_same_direction_as_us": 71.0,
         "corr_us_return_india_gap": 0.4},
        {"us_move": "US flat", "sessions": 300.0, "avg_us_return_pct": 0.1, "avg_india_gap_pct": 0.02,
         "avg_india_return_pct": 0.05, "pct_india_up_days": 52.0, "pct_gap_same_direction_as_us": 55.0,
         "corr_us_return_india_gap": 0.2},
    ],
    "study_start": [{"first_day": "2024-11-12"}],
    "unusual_in": [
        {"symbol": "ABC&CO", "trade_date": "2026-11-06", "close": 120.0, "return_pct": 4.3, "volume_ratio": 5.2},
        {"symbol": "XYZ", "trade_date": "2026-11-06", "close": 50.0, "return_pct": -2.1, "volume_ratio": 3.4},
    ],
}


def pre_blocks(msg):
    return re.findall(r"<pre>(.*?)</pre>", msg, re.S)


class BuildMessage(unittest.TestCase):
    def setUp(self):
        self.msg = premarket.build_message(SAMPLE, dt.date(2026, 11, 9))

    def test_header_and_takeaways(self):
        m = self.msg
        self.assertIn("📊 <b>Pre-market brief</b> · Mon 09 Nov 2026", m)
        self.assertIn("<i>Markets as of the Fri 06 Nov close</i>", m)
        self.assertIn("• <b>US:</b> S&amp;P +1.42%, Nasdaq +1.91%, a big US up day.", m)
        self.assertIn("• <b>World:</b> weakest Japan -5.96%, strongest Korea +4.07%.", m)
        self.assertIn("• <b>Risk:</b> VIX 28.2 (+11.6), elevated · Brent $107 · USD/INR 94.94.", m)
        self.assertIn("• <b>India:</b> after similar US days, NIFTYBEES opened +0.35% on average.", m)
        self.assertNotIn("⚠️", m)

    def test_tables_are_aligned_and_escaped(self):
        blocks = pre_blocks(self.msg)
        self.assertEqual(len(blocks), 6)  # US, world, macro, India stats, ADRs, unusual
        for block in blocks:
            rows = block.split("\n")
            self.assertLessEqual(max(len(r) for r in rows), 40, block)  # fits a phone screen
        world = blocks[1]
        self.assertIn("Japan", world)
        self.assertIn("Europe*", world)  # earlier session than the US close
        self.assertRegex(world, r"Korea\s+\+4\.07\s+\+6\.20")
        self.assertIn("ABC&amp;CO", blocks[5])  # escaped inside <pre>
        self.assertIn("* = earlier session.", self.msg)

    def test_macro_formats(self):
        macro = pre_blocks(self.msg)[2]
        self.assertRegex(macro, r"VIX\s+28\.20\s+\+11\.6\s+06 Nov")
        self.assertRegex(macro, r"US 10Y\s+4\.84%\s+-6bp\s+06 Nov")
        self.assertRegex(macro, r"USD/INR\s+94\.94\s+\+0\.1%\s+30 Oct")
        self.assertRegex(macro, r"EUR/USD\s+1\.0712\s+-0\.2%\s+30 Oct")

    def test_india_setup_and_linked(self):
        self.assertIn("After a <i>big US up day</i> (48 sessions since Nov 2024):", self.msg)
        self.assertRegex(pre_blocks(self.msg)[3], r"Avg opening gap\s+\+0\.35%")
        self.assertIn("Most linked to India's open: Korea +4.1% (r=0.41) · Japan -6.0% (r=0.37)", self.msg)

    def test_flat_day_uses_flat_bucket(self):
        data = json.loads(json.dumps(SAMPLE))
        data["us_bars"][0]["return_pct"] = 0.3
        msg = premarket.build_message(data, dt.date(2026, 11, 9))
        self.assertIn("After a <i>flat US day</i> (300 sessions", msg)

    def test_stale_data_warning(self):
        msg = premarket.build_message(SAMPLE, dt.date(2026, 11, 16))
        self.assertIn("⚠️ Latest US data is from Fri 06 Nov", msg)

    def test_empty_tables_do_not_crash(self):
        empty = {k: [] for k in SAMPLE}
        msg = premarket.build_message(empty, dt.date(2026, 11, 9))
        self.assertIn("No US index data.", msg)
        self.assertIn("Not enough history", msg)
        self.assertIn("None above the threshold.", msg)
        self.assertNotIn("World markets", msg)

    def test_fits_one_telegram_message(self):
        self.assertLess(len(self.msg), telegram.MAX_LENGTH)

    def test_handler_dry_run_does_not_send(self):
        with mock.patch.object(databricks_sql, "query", side_effect=lambda sql: []), \
             mock.patch.object(telegram, "send") as send:
            out = premarket.handler({"dry_run": True}, None)
        send.assert_not_called()
        self.assertFalse(out["sent"])
        self.assertIn("Pre-market brief", out["message"])


class SoftFailure(unittest.TestCase):
    def test_one_failed_query_still_sends_with_warning(self):
        def fake_query(sql):
            if "global_snapshot" in sql:
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND stocks.gold.global_snapshot")
            if "daily_bars" in sql:
                return SAMPLE["us_bars"]
            return []

        with mock.patch.object(databricks_sql, "query", side_effect=fake_query), \
             mock.patch.object(telegram, "send") as send:
            out = premarket.handler({}, None)
        send.assert_called_once()
        self.assertEqual(out["failed_queries"], ["global"])
        msg = send.call_args.args[0]
        self.assertIn("⚠️ Unavailable today: world markets", msg)
        self.assertIn("S&amp;P 500", msg)  # the rest of the brief is intact
        self.assertNotIn("World markets", msg)


class DatabricksSql(unittest.TestCase):
    def setUp(self):
        databricks_sql._config = {"host": "https://dbc-x.cloud.databricks.com", "token": "T", "warehouse_id": "W1"}

    def _resp(self, state, rows=None, sid="s1"):
        return {
            "statement_id": sid,
            "status": {"state": state},
            "manifest": {"schema": {"columns": [{"name": "symbol", "type_name": "STRING"},
                                                {"name": "ret", "type_name": "DOUBLE"},
                                                {"name": "d", "type_name": "DATE"}]}},
            "result": {"data_array": rows or []},
        }

    def test_polls_until_succeeded_and_types_numbers(self):
        replies = [self._resp("PENDING"), self._resp("RUNNING"),
                   self._resp("SUCCEEDED", [["SPY", "1.5", "2026-11-06"], ["QQQ", None, "2026-11-06"]])]
        calls = []

        def fake_call(method, path, body=None):
            calls.append((method, path, body))
            return replies.pop(0)

        with mock.patch.object(databricks_sql, "_call", side_effect=fake_call), mock.patch("time.sleep"):
            rows = databricks_sql.query("SELECT 1")
        self.assertEqual(rows, [{"symbol": "SPY", "ret": 1.5, "d": "2026-11-06"},
                                {"symbol": "QQQ", "ret": None, "d": "2026-11-06"}])
        self.assertEqual(calls[0][0], "POST")
        self.assertEqual(calls[0][2]["warehouse_id"], "W1")
        self.assertEqual(calls[1], ("GET", "/api/2.0/sql/statements/s1", None))

    def test_failed_statement_raises(self):
        failed = self._resp("FAILED")
        failed["status"]["error"] = {"message": "TABLE_OR_VIEW_NOT_FOUND"}
        with mock.patch.object(databricks_sql, "_call", return_value=failed):
            with self.assertRaisesRegex(RuntimeError, "TABLE_OR_VIEW_NOT_FOUND"):
                databricks_sql.query("SELECT 1")


class TelegramSend(unittest.TestCase):
    def test_sends_html_and_redacts_token(self):
        ssm = mock.MagicMock()
        ssm.get_parameter.side_effect = lambda Name, WithDecryption: {
            "Parameter": {"Value": "BOT:SECRET" if "token" in Name else "12345"}}
        os.environ.update(TELEGRAM_TOKEN_PARAM="/stocks/telegram-bot-token",
                          TELEGRAM_CHAT_ID_PARAM="/stocks/telegram-chat-id")
        with mock.patch.object(telegram.boto3, "client", return_value=ssm), \
             mock.patch.object(telegram, "http_request") as req:
            telegram.send("x" * 5000)
        args, kwargs = req.call_args
        self.assertEqual(args[0], "POST")
        self.assertIn("botBOT:SECRET/sendMessage", args[1])
        self.assertNotIn("SECRET", kwargs["log_url"])
        body = json.loads(kwargs["data"])
        self.assertEqual(body["chat_id"], "12345")
        self.assertEqual(body["parse_mode"], "HTML")
        self.assertLessEqual(len(body["text"]), telegram.MAX_LENGTH)


if __name__ == "__main__":
    unittest.main()
