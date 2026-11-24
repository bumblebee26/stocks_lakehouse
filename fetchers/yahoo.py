"""Live quotes and daily candles from Yahoo Finance's public chart API (no key).

Used by the morning brief for the few figures it needs at 08:30 IST, straight from the market (the Massive free
plan only releases a US day at ~10:30 IST, and FRED a day or two later), and for the calendar: the dates of an
index's own daily candles show which sessions actually happened, so holidays need no list.
"""

import datetime as dt
import json
import time
from zoneinfo import ZoneInfo

from common import http_request

URL = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range={rng}&interval=1d"
# Whole history by dates: Yahoo turns range=max into monthly candles even with interval=1d, so "all" asks for
# 1 Jan 1970 to now instead, which stays daily.
URL_ALL = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}?period1=0&period2={now}&interval=1d"
HEADERS = {"User-Agent": "Mozilla/5.0 (stocks-lakehouse morning brief)", "Accept": "application/json"}


def daily(symbol, rng="1mo"):
    """[{'date': 'YYYY-MM-DD' (exchange's own date), 'open', 'high', 'low', 'close'}], oldest first, plus meta.
    rng is a Yahoo range ('1mo', '5y') or 'all' for the whole daily history."""
    sym = symbol.replace("^", "%5E")
    url = URL_ALL.format(sym=sym, now=int(time.time())) if rng == "all" else URL.format(sym=sym, rng=rng)
    body = json.loads(http_request("GET", url, headers=HEADERS, retries=2, backoff_seconds=2, timeout=30))
    res = body["chart"]["result"][0]
    meta = res.get("meta") or {}
    tz = ZoneInfo(meta.get("exchangeTimezoneName") or "UTC")
    q = res["indicators"]["quote"][0]
    out = []
    for i, ts in enumerate(res.get("timestamp") or []):
        if q["close"][i] is None:
            continue
        out.append({"date": dt.datetime.fromtimestamp(ts, tz).date().isoformat(), "open": q["open"][i],
                    "high": q["high"][i], "low": q["low"][i], "close": q["close"][i]})
    return out, meta


def last_session(symbol, before):
    """The last completed daily candle dated before `before` (a date), and the one before it, or (None, None)."""
    bars, _ = daily(symbol)
    done = [b for b in bars if b["date"] < before.isoformat()]
    if not done:
        return None, None
    return done[-1], (done[-2] if len(done) > 1 else None)


def change(symbol, before):
    """{'date', 'close', 'change_pct'} for the last session before `before`, or None."""
    last, prev = last_session(symbol, before)
    if not last or not prev:
        return None
    return {"date": last["date"], "close": last["close"], "change_pct": (last["close"] / prev["close"] - 1) * 100}
