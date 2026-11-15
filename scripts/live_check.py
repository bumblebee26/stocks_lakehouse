"""Temporary: run the new code against the live sources (no AWS, no Databricks, nothing sent)."""
import datetime as dt
import json
import sys
from unittest import mock
from zoneinfo import ZoneInfo

sys.path.insert(0, "fetchers")
sys.modules["boto3"] = mock.MagicMock()
import common, news, nse, nse_fo, premarket, yahoo  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
today = dt.datetime.now(IST).date()
ok = True

print("== Yahoo: calendars and live figures (as the brief reads them)")
sessions, live = premarket.fetch_live(today)
print("sessions:", sessions)
for sym, label in premarket.LIVE:
    r = live.get(sym)
    print(f"  {label:10} {sym:10} {r}")
    ok &= r is not None
exp = premarket.expected_dates(today, sessions)
print("expected:", {k: (v[0].isoformat(), [d.isoformat() for d in v[1]], v[2]) for k, v in exp.items()})

print("\n== NSE delivery file (last NSE session)")
d = dt.date.fromisoformat(sessions["NSE"]) if sessions.get("NSE") else premarket.previous_weekday(today)
try:
    rows = nse.parse_delivery(common.http_get(nse.DELIVERY_URL.format(d=d), nse.HEADERS).decode("utf-8", "replace"))
    eq = [r for r in rows if r["series"] == "EQ"]
    print(f"  {len(rows)} rows, {len(eq)} EQ; sample", [r for r in eq if r["symbol"] in ("TCS", "RELIANCE", "NIFTYBEES")])
    ok &= len(eq) > 1000
except Exception as err:  # noqa: BLE001
    print("  FAILED:", err); ok = False

print("\n== NSE F&O list")
try:
    fo = nse_fo.parse(common.http_get(nse_fo.URL, nse_fo.HEADERS).decode("utf-8", "replace"), today.isoformat())
    syms = [r["symbol"] for r in fo]
    print(f"  {len(fo)} stocks; first {syms[:5]}; TCS in list: {'TCS' in syms}; index rows leaked:",
          [s for s in syms if s.startswith("NIFTY") or s in ("BANKNIFTY", "SENSEX")])
    ok &= len(fo) >= 100
except Exception as err:  # noqa: BLE001
    print("  FAILED:", err); ok = False

print("\n== Extra news feeds")
eqnames = {"names": {news.norm_name(n): s for n, s in {"Infosys Ltd.": "INFY", "Tata Consultancy Services Ltd.": "TCS",
           "Reliance Industries Ltd.": "RELIANCE", "HDFC Bank Ltd.": "HDFCBANK", "State Bank of India": "SBIN"}.items()},
           "symbols": {"INFY", "TCS", "RELIANCE", "HDFCBANK", "SBIN"}}
now = dt.datetime.now(IST)
items = news.extra_items(now.replace(minute=0), eqnames, now.strftime("%Y-%m-%d %H:%M:%S"))
by = {}
for i in items:
    by.setdefault(i["feed"], []).append(i)
for feed in news.EXTRA_FEEDS:
    got = by.get(feed, [])
    print(f"  {feed}: {len(got)} items")
    for i in got[:2]:
        print(f"    [{i['category']}/{i['symbol']}] {i['published_ist']} {i['title'][:90]}")
ok &= len(by.get("bs_markets", [])) > 0 and len(by.get("rbi", [])) > 0

print("\n== Brief built from live Yahoo data and sample lakehouse rows (not sent)")
data = {"freshness": [{"source": s, "latest_date": exp["NSE"][0].isoformat()} for s in
                      ("nse_prices", "nse_delivery", "technical_scanner")],
        "summary": [], "unusual_in": [], "screen": []}
fresh = premarket.freshness(data, exp)
print(premarket.build_brief(data, live, exp, fresh, today))
print("\n--- scanner message with an empty list:")
print(premarket.build_scanner(data, exp, fresh)[0])
print("\nRESULT:", "OK" if ok else "PROBLEMS")
