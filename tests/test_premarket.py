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


D = dt.date
TODAY = D(2026, 11, 16)  # Monday
SESSIONS = {"NSE": "2026-11-13", "US": "2026-11-13"}


def live(**over):
    base = {"^GSPC": (0.4, 6800.0), "^IXIC": (0.6, 23000.0), "BZ=F": (-1.2, 72.3), "INR=X": (0.1, 84.12),
            "^INDIAVIX": (3.1, 13.4), "INFY": (2.3, 21.0), "HDB": (-0.4, 70.0)}
    base.update(over)
    return {k: {"date": "2026-11-13", "close": v[1], "change_pct": v[0]} for k, v in base.items() if v}


def pick(sym, list_type="strict", **over):
    r = {"symbol": sym, "company": f"{sym.title()} Industries Ltd.", "trade_date": "2026-11-13",
         "list_type": list_type, "rank": 1, "close": 1234.5, "setup": "20-day breakout", "stance": "buy-on-dip",
         "rsi14": 58.2, "macd": 12.1, "macd_signal": 10.3, "macd_cross_up": True, "adx14": 24.5, "above_mas": True,
         "above20": True, "above50": True, "above200": True, "vol_ratio": 2.1, "deliv_per": 54.0, "fno": True,
         "entry_low": 1220.0, "entry_high": 1240.0, "trigger": 1220.0, "stop": 1180.0, "stop_basis": "swing low",
         "target1": 1300.0, "target1_basis": "resistance", "target2": 1420.0, "rr": 2.4, "misses": "",
         "filters_passed": 6, "nifty200_as_of": "2026-10-01", "fno_as_of": "2026-11-09",
         "regime_close": 268.4, "regime_rsi": 57.0, "regime_sma20": 265.0, "regime_sma50": 260.0,
         "regime_sma200": 250.0, "regime_adx": 22.0}
    r.update(over)
    return r


def sample(**over):
    d = {
        "freshness": [{"source": "nse_prices", "latest_date": "2026-11-13"},
                      {"source": "nse_delivery", "latest_date": "2026-11-13"},
                      {"source": "technical_scanner", "latest_date": "2026-11-13"}],
        "summary": [{"us_move": "US down big", "sessions": 14, "avg_india_gap_pct": -0.62}],
        "unusual_in": [{"symbol": "M&M", "trade_date": "2026-11-13", "close": 3000.0, "return_pct": 3.2,
                        "volume_ratio": 3.4}],
        "screen": [pick("ABC"), pick("M&M"), pick("XYZ", "near-miss", misses="volume under 1.5x its 20-day average",
                                                  stance="wait")],
    }
    d.update(over)
    return d


def build(data=None, lv=None, sessions=SESSIONS, today=TODAY, failed=()):
    data = data or sample()
    exp = premarket.expected_dates(today, sessions)
    fresh = premarket.freshness(data, exp)
    lv = live() if lv is None else lv
    return (premarket.build_brief(data, lv, exp, fresh, today, failed),
            *premarket.build_scanner(data, exp, fresh, failed))


class Calendar(unittest.TestCase):
    def test_holiday_is_stated(self):
        exp = premarket.expected_dates(D(2026, 11, 17), {"NSE": "2026-11-13", "US": "2026-11-16"})
        self.assertEqual(exp["NSE"][0], D(2026, 11, 13))
        self.assertEqual(exp["NSE"][1], [D(2026, 11, 16)])
        msg, _, _ = build(sessions={"NSE": "2026-11-13", "US": "2026-11-16"}, today=D(2026, 11, 17),
                          lv=live(**{"^GSPC": None, "^IXIC": None, "BZ=F": None, "INR=X": None}))
        self.assertIn("NSE closed Mon 16 Nov (holiday)", msg)

    def test_no_calendar_falls_back_to_previous_weekday(self):
        exp = premarket.expected_dates(TODAY, {})
        self.assertEqual(exp["US"], (D(2026, 11, 13), [], False))


class Brief(unittest.TestCase):
    def test_india_first_then_global_supporting(self):
        msg, _, _ = build()
        self.assertIn("India VIX 13.4", msg)
        self.assertIn("1% of the pool at risk", msg)
        self.assertIn("above its 20/50/200-day averages", msg)
        self.assertIn("2 strict picks, 1 near-miss", msg)
        self.assertLess(msg.index("Risk:"), msg.index("Global"))
        self.assertIn("S&amp;P +0.40%", msg)
        self.assertNotIn("Global shock", msg)
        self.assertIn("M&amp;M", msg)                 # escaped
        self.assertNotIn("World markets", msg)       # no US/world tables any more

    def test_vix_sets_risk(self):
        self.assertEqual(premarket.vix_risk(22), ("elevated", 0.75))
        self.assertEqual(premarket.vix_risk(31), ("high", 0.5))
        self.assertEqual(premarket.vix_risk(12)[1], 1.0)

    def test_global_shock_line(self):
        msg, _, _ = build(lv=live(**{"^GSPC": (-2.4, 6500.0)}))
        self.assertIn("Global shock:</b> S&amp;P 500 -2.40%", msg)
        self.assertIn("opened -0.62% on average (14 sessions)", msg)

    def test_stale_us_is_left_out_not_used(self):
        stale = live()
        for k in stale:
            stale[k]["date"] = "2026-11-12"
        stale["^INDIAVIX"]["date"] = "2026-11-13"
        msg, _, _ = build(lv=stale)
        self.assertNotIn("S&amp;P +", msg)
        self.assertNotIn("Indian ADRs", msg)
        self.assertIn("Not available this morning: S&amp;P 500, Nasdaq, Brent, USD/INR: left out", msg)
        self.assertIn("India VIX 13.4", msg)

    def test_stale_nse_is_left_out(self):
        data = sample(freshness=[{"source": "nse_prices", "latest_date": "2026-11-12"},
                                 {"source": "technical_scanner", "latest_date": "2026-11-12"}])
        msg, scan, buttons = build(data)
        self.assertIn("NSE prices for Fri 13 Nov hasn't arrived (latest Thu 12 Nov): left out", msg)
        self.assertNotIn("Unusual volume", msg)
        self.assertNotIn("Scanner:", msg)
        self.assertIn("hasn't run", scan)
        self.assertEqual(buttons, [])

    def test_fits_telegram(self):
        msg, scan, _ = build(sample(screen=[pick(f"S{i}") for i in range(10)] +
                                    [pick(f"N{i}", "near-miss", misses="x") for i in range(5)]))
        self.assertLessEqual(len(msg), telegram.MAX_LENGTH)
        for part in premarket.chunks(scan):
            self.assertLessEqual(len(part) + 12, telegram.MAX_LENGTH)
        self.assertGreater(len(premarket.chunks(scan)), 1)
        for block in re.findall(r"<pre>(.*?)</pre>", msg, re.S):
            self.assertLessEqual(max(len(x) for x in block.splitlines()), 40)


class Scanner(unittest.TestCase):
    def test_picks_levels_links_and_buttons(self):
        _, scan, buttons = build()
        self.assertIn("<b>1. ABC</b> Rs 1,234.5 · 20-day breakout · <b>buy-on-dip</b>", scan)
        self.assertIn("MACD 12.10/10.30 crossed ↑", scan)
        self.assertIn("stop 1,180.0 (swing low)", scan)
        self.assertIn("R:R 2.4", scan)
        self.assertIn("symbol=M%26M", scan)
        self.assertIn("groww.in/stocks/abc-industries-ltd", scan)
        self.assertIn("XYZ Rs 1,234.5, wait: missed volume under 1.5x", scan)
        self.assertIn("never a guaranteed return", scan)
        self.assertEqual(buttons[0], [("Swing buy ABC (asks first)", "sw:ABC:1180.00:1420.00")])
        self.assertEqual(len(buttons), 2)

    def test_empty_list_says_so(self):
        _, scan, buttons = build(sample(screen=[]))
        self.assertIn("No stock passed all six filters today; none forced.", scan)
        self.assertEqual(buttons, [])

    def test_groww_slug(self):
        self.assertEqual(premarket.groww_slug("Dr. Reddy's Laboratories Ltd."), "dr-reddys-laboratories-ltd")
        self.assertEqual(premarket.groww_slug("Larsen & Toubro Limited"), "larsen-toubro-ltd")


class Handler(unittest.TestCase):
    def _run(self, fake_query, event=None):
        with mock.patch.object(databricks_sql, "query", side_effect=fake_query), \
             mock.patch.object(premarket, "fetch_live", return_value=(SESSIONS, live())), \
             mock.patch.object(premarket.dt, "datetime", wraps=dt.datetime) as fake_dt, \
             mock.patch.object(telegram, "send") as send:
            fake_dt.now.return_value = dt.datetime(2026, 11, 16, 8, 45, tzinfo=premarket.IST)
            return premarket.handler(event or {}, None), send

    def test_dry_run_does_not_send(self):
        out, send = self._run(lambda sql: [], {"dry_run": True})
        send.assert_not_called()
        self.assertIn("Technical scanner", out["scanner"])

    def test_one_failed_query_still_sends_both(self):
        data = sample()

        def fake_query(sql):
            if "technical_screen" in sql:
                raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND stocks.gold.technical_screen")
            if "data_freshness" in sql:
                return data["freshness"]
            return []

        out, send = self._run(fake_query)
        self.assertEqual(send.call_count, 2)
        self.assertEqual(out["failed_queries"], ["screen"])
        self.assertIn("Unavailable today: technical scanner", send.call_args_list[0].args[0])
        self.assertIn("Scanner unavailable today", send.call_args_list[1].args[0])


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
        self.assertNotIn("reply_markup", body)

    def test_buttons(self):
        ssm = mock.MagicMock()
        ssm.get_parameter.return_value = {"Parameter": {"Value": "1"}}
        os.environ.update(TELEGRAM_TOKEN_PARAM="/t", TELEGRAM_CHAT_ID_PARAM="/c")
        with mock.patch.object(telegram.boto3, "client", return_value=ssm), \
             mock.patch.object(telegram, "http_request") as req:
            telegram.send("hi", buttons=[[("Swing buy ABC", "sw:ABC:1.00:2.00")]])
        body = json.loads(req.call_args.kwargs["data"])
        self.assertEqual(body["reply_markup"]["inline_keyboard"][0][0]["callback_data"], "sw:ABC:1.00:2.00")


if __name__ == "__main__":
    unittest.main()
