"""Tests for the mobile app export (fetchers/app_export.py). No network/AWS."""

import datetime as dt
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
        self.assertEqual(set(body["datasets"]), set(QUERIES) | {"fuel_mumbai", "nse_holidays", "market_status",
                                                         "morning", "pool_auto", "bullion_in", "council"})
        self.assertEqual(body["failed"], ["nse_holidays", "bullion_in", "fuel_mumbai"])
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

    def test_fuel_remembers_the_price_before_a_change(self):
        prev = {"datasets": {"fuel_mumbai": {"rows": [
            {"item": "Petrol", "rate": "111.21", "as_of": "2026-11-24", "source": "CarDekho"},
            {"item": "Diesel", "rate": "97.83", "as_of": "2026-11-24", "source": "CarDekho",
             "prev_rate": "99.00", "changed_on": "2026-11-01"}]}}}
        page = "<h2>Petrol</h2><td>₹109.21</td><h2>Diesel</h2><td>₹97.83</td><p>CNG ₹91.50</p>"
        ds, _ = app_export.fuel_rates(prev, fetch=lambda: page.encode(), today="2026-11-26")
        rows = {r["item"]: r for r in ds["rows"]}
        self.assertEqual((rows["Petrol"]["prev_rate"], rows["Petrol"]["changed_on"]), ("111.21", "2026-11-26"))
        self.assertEqual((rows["Diesel"]["prev_rate"], rows["Diesel"]["changed_on"]), ("99.00", "2026-11-01"))
        self.assertEqual(rows["CNG"]["prev_rate"], "")                                         # no change yet

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
    def test_whole_history_is_asked_for_by_dates_so_it_stays_daily(self):
        import yahoo
        seen = []
        res = {"chart": {"result": [{"meta": {}, "timestamp": [1795392000],
                                      "indicators": {"quote": [{"open": [1], "high": [1], "low": [1], "close": [1]}]}}]}}

        def fake(method, url, **kw):
            seen.append(url)
            return json.dumps(res).encode()
        with mock.patch.object(yahoo, "http_request", side_effect=fake):
            yahoo.daily("INR=X", "all")
            yahoo.daily("^NSEI", "5y")
        self.assertIn("period1=0&period2=", seen[0])
        self.assertNotIn("range=max", seen[0])
        self.assertIn("%5ENSEI?range=5y&interval=1d", seen[1])
        calls = []
        app_export.yahoo_series({}, fetch=lambda sym, rng: (calls.append(rng), ([], {}))[1])
        self.assertEqual(set(calls), {"all"})   # no history: failed, no second call

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
        self.assertEqual(sorted(failed2), sorted(app_export.YAHOO_SERIES))
        self.assertEqual(out2["usdinr_5y"]["rows"], out["usdinr_5y"]["rows"])
        self.assertEqual(out2["acn_5y"]["rows"], [])

    def test_series_adds_the_days_the_whole_history_lacks(self):
        whole = [{"date": "2026-11-20", "close": 80800.0}, {"date": "2026-11-23", "close": 80900.0}]
        month = [{"date": "2026-11-23", "close": 80900.0}, {"date": "2026-11-24", "close": 81000.0},
                 {"date": "2026-11-25", "close": 81500.0}]
        out, _ = app_export.yahoo_series({}, fetch=lambda sym, rng: (whole if rng == "all" else month, {}))
        self.assertEqual([r["date"] for r in out["sensex_5y"]["rows"]][-3:], ["2026-11-23", "2026-11-24", "2026-11-25"])

    def test_series_never_loses_the_last_day(self):
        prev = {"datasets": {"sensex_5y": {"rows": [{"date": "2026-11-24", "close": 81000.0},
                                                    {"date": "2026-11-25", "close": 81500.0}]}}}
        late = lambda sym, rng: ([{"date": "2026-11-23", "close": 80900.0},  # noqa: E731
                                  {"date": "2026-11-24", "close": 81000.0}], {})
        out, _ = app_export.yahoo_series(prev, fetch=late)
        self.assertEqual(out["sensex_5y"]["rows"][-1], {"date": "2026-11-25", "close": 81500.0})

    def test_trader_files_latest_opening_read_and_auto_mode(self):
        read = {"read": {"nifty_gap_pct": 0.4}, "stocks": [
            {"symbol": f"S{i}", "score": 10 - i, "rvol": 2.0, "preopen": {"gap_pct": 1.0, "buy_sell": 1.5}}
            for i in range(7)]}
        s3 = FakeS3({"opening/2026-11-23.json": json.dumps(read).encode(),
                     "state/pool_auto.json": json.dumps({"long_term": True}).encode()})
        morning, auto = app_export.trader_files(s3, "t", today=dt.date(2026, 11, 24))
        row = morning["rows"][0]
        self.assertEqual(row["date"], "2026-11-23")
        self.assertEqual([t["symbol"] for t in row["top"]], ["S0", "S1", "S2", "S3", "S4"])
        self.assertEqual(row["top"][0]["preopen_gap"], 1.0)
        self.assertEqual({r["pool"]: r["on"] for r in auto["rows"]}, {"swing": True, "long_term": True})
        morning2, auto2 = app_export.trader_files(FakeS3({}), "t", today=dt.date(2026, 11, 24))
        self.assertEqual(morning2["rows"], [])
        self.assertEqual({r["pool"]: r["on"] for r in auto2["rows"]}, {"swing": True, "long_term": False})

    IBJA = ("<div>Gold rates per 10gm &amp; Silver rate per 1kg</div><div id='tab-am'><table>"
            "<tr><td>23/11/2026</td><td>125123</td><td>124622</td><td>114613</td><td>93842</td><td>73197</td>"
            "<td>152941</td><td>48525</td></tr>"
            "<tr><td>20/11/2026</td><td>123584</td><td>123089</td><td>113203</td><td>92688</td><td>72297</td>"
            "<td>150650</td><td>48880</td></tr></table></div><div id='tab-pm'><table>"
            "<tr><td>23/11/2026</td><td>124945</td><td>124445</td><td>114450</td><td>93709</td><td>73093</td>"
            "<td>152625</td><td>46900</td></tr>"
            "<tr><td>20/11/2026</td><td>123893</td><td>123397</td><td>113486</td><td>92920</td><td>72477</td>"
            "<td>152528</td><td>48730</td></tr></table></div>")

    def test_morning_has_plan_focus_preopen_gaps_and_dashboard_lines(self):
        plan = {"plan": {"india_vix": 17.7, "risk_per_trade_pct": 1.0, "bias": "uptrend"}, "focus": ["HDFCBANK", "INFY"]}
        pre = {"stocks": {"POLICYBZR": {"gap_pct": 5.9, "buy_sell": 1.38}, "INFY": {"gap_pct": 3.5, "buy_sell": 0.51},
                          "APLAPOLLO": {"gap_pct": -10.0, "buy_sell": None}, "TCS": {"gap_pct": 0.0}}}
        opening = {"read": {"nifty_gap_pct": 1.15, "nifty_first10_pct": 0.28, "nifty_gap": "kept",
                            "up_from_open": "21/44", "median_or_vs_day_range": 0.54, "india_vix_change_pct": -4.2},
                   "stocks": [{"symbol": "APLAPOLLO", "score": 29.03, "rvol": 28.03, "gap_pct": -10.0, "news": True,
                               "preopen": {"gap_pct": -10.0}}]}
        s3 = FakeS3({"plans/2026-11-25.json": json.dumps(plan).encode(), "preopen/2026-11-25.json": json.dumps(pre).encode(),
                     "opening/2026-11-25.json": json.dumps(opening).encode()})
        morning, _ = app_export.trader_files(s3, "t", today=dt.date(2026, 11, 25))
        row = morning["rows"][0]
        self.assertEqual(row["plan"], {"vix": 17.7, "risk_pct": 1.0, "bias": "uptrend", "regime": None})
        self.assertEqual([g["symbol"] for g in row["gaps_up"]], ["POLICYBZR", "INFY"])
        self.assertEqual([g["symbol"] for g in row["gaps_down"]], ["APLAPOLLO"])
        self.assertTrue(row["top"][0]["news"])
        lines = app_export.morning_lines(row)
        self.assertIn({"date": "2026-11-25", "page": "desk", "section": "1 Market", "item": "India VIX 17.7",
                       "detail": "bias: uptrend"}, lines)
        self.assertIn({"date": "2026-11-25", "page": "intraday", "section": "1 Plan", "item": "Risk 1.0% per trade",
                       "detail": "set from India VIX 17.7"}, lines)
        self.assertEqual({x["page"] for x in lines if x["section"] == "2 Focus"}, {"intraday"})
        up = [x for x in lines if x["section"] == "2 Pre-open gaps up"]
        self.assertEqual((up[0]["page"], up[0]["detail"]), ("desk", "+5.9%, buy/sell 1.38x"))
        self.assertEqual({x["page"] for x in lines if x["section"] == "4 Opening read"}, {"desk"})
        play = [x for x in lines if x["section"] == "3 In play at 09:25"][0]
        self.assertEqual(play["item"], "1. APLAPOLLO")
        self.assertIn("news", play["detail"])
        # plan only (before 09:25) still makes a morning
        s3b = FakeS3({"plans/2026-11-25.json": json.dumps(plan).encode()})
        mb, _ = app_export.trader_files(s3b, "t", today=dt.date(2026, 11, 25))
        self.assertEqual((mb["rows"][0]["top"], mb["rows"][0]["focus"]), ([], ["HDFCBANK", "INFY"]))

    def test_council_scorecard_rows(self):
        card = {"as_of": "2026-11-25", "real": {"n": 9, "rupees": -120, "hit_pct": 44.4},
                "council": {"n": 9, "rupees": 80, "hit_pct": 55.6, "brier": 0.231},
                "disagreements": {"n": 3, "council_rupees": 60, "reviewer_rupees": -40}}
        data = app_export.council_card(FakeS3({"council/scorecard.json": json.dumps(card).encode()}), "t")
        self.assertEqual([r["method"] for r in data["rows"]], ["Real decisions", "Council (reviewer + tape + base rate)"])
        self.assertEqual((data["rows"][1]["brier"], data["rows"][1]["disagree_n"]), (0.231, 3))
        self.assertEqual(app_export.council_card(FakeS3({}), "t")["rows"], [])

    def test_bullion_takes_the_latest_pm_rates_with_the_change(self):
        data, failed = app_export.bullion_rates({}, fetch=lambda: self.IBJA)
        self.assertFalse(failed)
        rows = {r["item"]: r for r in data["rows"]}
        g = rows["Gold 24K (999)"]
        self.assertEqual((g["rate"], g["as_of"], g["session"], g["unit"]), (124945.0, "2026-11-23", "PM", "Rs / 10 g"))
        self.assertEqual(g["change_pct"], round((124945 / 123893 - 1) * 100, 2))
        self.assertEqual(rows["Gold 22K (916)"]["rate"], 114450.0)
        self.assertEqual((rows["Silver (999)"]["rate"], rows["Silver (999)"]["unit"]), (152625.0, "Rs / kg"))

    def test_bullion_only_am_and_failures_keep_the_last(self):
        am_only = self.IBJA.split("<div id='tab-pm'>")[0]
        data, _ = app_export.bullion_rates({}, fetch=lambda: am_only)
        self.assertEqual((data["rows"][0]["rate"], data["rows"][0]["session"]), (125123.0, "AM"))
        prev = {"datasets": {"bullion_in": data}}
        kept, failed = app_export.bullion_rates(prev, fetch=lambda: "<html>blocked</html>")
        self.assertTrue(failed)
        self.assertEqual(kept, data)

    def test_empty_sector_tables_keep_the_last_trading_day(self):
        prev = {"datasets": {"sector_board": {"fields": [], "rows": [{"sector": "IT"}]}}}
        s3 = FakeS3({"config/queries.json": json.dumps({"sector_board": {"sql": "x"}}).encode(),
                     "abc/data/desk.json": json.dumps(prev).encode()})
        with mock.patch.dict(os.environ, {"APP_BUCKET": "b", "APP_PREFIX": "abc"}), \
             mock.patch.object(app_export, "_s3", return_value=s3), \
             mock.patch.object(app_export.databricks_sql, "query", return_value=[]), \
             mock.patch.object(app_export, "yahoo_series", return_value=({}, [])), \
             mock.patch.object(app_export, "http_get", side_effect=RuntimeError("offline")), \
             mock.patch.object(app_export.nse_holidays, "http_get", side_effect=RuntimeError("offline")):
            app_export.handler({}, None)
        body = json.loads(s3.puts[0]["Body"])
        self.assertEqual(body["datasets"]["sector_board"]["rows"], [{"sector": "IT"}])


if __name__ == "__main__":
    unittest.main()
