"""Tests for the pre-market brief, Databricks SQL client and Telegram sender. No network/AWS."""

import datetime as dt
import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetchers"))
sys.modules.setdefault("boto3", mock.MagicMock())

import databricks_sql  # noqa: E402
import premarket  # noqa: E402
import telegram  # noqa: E402

SAMPLE = {
    "us_bars": [
        {"trade_date": "2026-11-06", "symbol": "SPY", "close": 650.0, "return_pct": 1.42},
        {"trade_date": "2026-11-06", "symbol": "QQQ", "close": 560.0, "return_pct": 1.91},
        {"trade_date": "2026-11-06", "symbol": "INFY", "close": 21.0, "return_pct": 2.3},
        {"trade_date": "2026-11-06", "symbol": "HDB", "close": 70.0, "return_pct": -0.4},
    ],
    "us_breadth": [{"trade_date": "2026-11-06", "advances": 3210.0, "declines": 1980.0,
                    "pct_above_ma20": 61.6, "median_return_pct": 0.5}],
    "summary": [
        {"us_move": "US up big", "sessions": 48.0, "avg_us_return_pct": 1.6, "avg_india_gap_pct": 0.35,
         "avg_india_return_pct": 0.2, "pct_india_up_days": 64.0, "pct_gap_same_direction_as_us": 71.0,
         "corr_us_return_india_gap": 0.4},
        {"us_move": "US flat", "sessions": 300.0, "avg_us_return_pct": 0.1, "avg_india_gap_pct": 0.02,
         "avg_india_return_pct": 0.05, "pct_india_up_days": 52.0, "pct_gap_same_direction_as_us": 55.0,
         "corr_us_return_india_gap": 0.2},
    ],
    "study_start": [{"first_day": "2024-11-12"}],
    "global": [
        {"instrument": "NIKKEI225", "obs_date": "2026-11-06", "value": 50120.0, "change_pct": 0.84, "change_abs": 418.0},
        {"instrument": "VGK", "obs_date": "2026-11-06", "value": 72.1, "change_pct": -0.31, "change_abs": -0.22},
        {"instrument": "FXI", "obs_date": "2026-11-06", "value": 38.0, "change_pct": 1.2, "change_abs": 0.45},
        {"instrument": "VIXCLS", "obs_date": "2026-11-06", "value": 17.4, "change_pct": -6.0, "change_abs": -1.1},
        {"instrument": "DCOILBRENTEU", "obs_date": "2026-11-02", "value": 68.25, "change_pct": -0.9, "change_abs": -0.6},
        {"instrument": "DGS10", "obs_date": "2026-11-06", "value": 4.12, "change_pct": 0.7, "change_abs": 0.03},
        {"instrument": "DEXINUS", "obs_date": "2026-10-30", "value": 88.12, "change_pct": 0.1, "change_abs": 0.09},
    ],
    "unusual_in": [
        {"symbol": "ABC&CO", "trade_date": "2026-11-06", "close": 120.0, "return_pct": 4.3, "volume_ratio": 5.2},
        {"symbol": "XYZ", "trade_date": "2026-11-06", "close": 50.0, "return_pct": -2.1, "volume_ratio": 3.4},
    ],
}


class BuildMessage(unittest.TestCase):
    def test_full_message(self):
        msg = premarket.build_message(SAMPLE, dt.date(2026, 11, 9))
        self.assertIn("Pre-market brief · Mon 09 Nov 2026", msg)
        self.assertIn("US session (Fri 06 Nov)", msg)
        self.assertIn("S&amp;P 500 (SPY) +1.42%", msg)
        self.assertIn("3,210 up / 1,980 down", msg)
        self.assertIn("After a <i>big US up day</i> (48 sessions since Tue 12 Nov)", msg)
        self.assertIn("NIFTYBEES opened +0.35%", msg)
        self.assertIn("INFY +2.30% · HDB -0.40%", msg)
        self.assertIn("1. ABC&amp;CO · 5.2× avg volume · +4.3%", msg)  # symbol HTML-escaped
        self.assertNotIn("⚠️", msg)
        self.assertIn("Nikkei 225 +0.84% · Europe -0.31% · China +1.20%", msg)
        self.assertIn("VIX 17.4 (-1.1) · Brent $68.2 (-0.9%) (as of Mon 02 Nov) · US 10Y 4.12% (+3 bp)", msg)
        self.assertIn("USD/INR 88.12 (as of Fri 30 Oct)", msg)

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
        self.assertNotIn("<b>Global</b>", msg)  # section hidden when there is no global data

    def test_handler_dry_run_does_not_send(self):
        with mock.patch.object(databricks_sql, "query", side_effect=lambda sql: []), \
             mock.patch.object(telegram, "send") as send:
            out = premarket.handler({"dry_run": True}, None)
        send.assert_not_called()
        self.assertFalse(out["sent"])
        self.assertIn("Pre-market brief", out["message"])


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
