"""Pre-open snapshot of the world markets, saved to the lakehouse before NSE opens.

Written by the 08:45 IST brief (premarket.py) to landing/premarket/date=YYYY-MM-DD/yahoo_HHMM.jsonl. Its arrival
starts the stocks-market-preopen job (market pipeline, then the dashboard summaries), so the Global markets page
is current by about 09:00 instead of waiting for Massive (~10:30 IST) and FRED (a day or more later).

One JSON line per instrument: its latest Yahoo daily candle and the changes Yahoo's own candles give (1, 5 and 20
sessions), so a reading never mixes two sources. Instruments FRED also carries keep FRED's id (SP500, DEXINUS...)
and the dashboard shows whichever reading is newer; on the same date FRED wins. `reading` is 'live' for a session
still trading (Tokyo, Seoul, Hong Kong and Shanghai are open at 08:45 IST; futures and currencies trade overnight)
and 'close' for a finished one.
"""

import datetime as dt
import json
import time
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import fred
import yahoo
from common import log, put_landing

# (Yahoo symbol, instrument id). Ids FRED uses take their name, region, asset class and unit from fred.SERIES.
SAME_AS_FRED = [
    ("^GSPC", "SP500"), ("^DJI", "DJIA"), ("^IXIC", "NASDAQCOM"), ("^N225", "NIKKEI225"), ("^VIX", "VIXCLS"),
    ("INR=X", "DEXINUS"), ("JPY=X", "DEXJPUS"), ("EURUSD=X", "DEXUSEU"), ("GBPUSD=X", "DEXUSUK"),
    ("CNY=X", "DEXCHUS"), ("BZ=F", "DCOILBRENTEU"), ("CL=F", "DCOILWTICO"), ("^TNX", "DGS10"),
]
# Markets FRED doesn't carry: (Yahoo symbol, instrument id, name, region, asset class, unit)
YAHOO_ONLY = [
    ("^KS11", "KOSPI", "Kospi", "South Korea", "equity_index", "index"),
    ("^HSI", "HSI", "Hang Seng", "Hong Kong", "equity_index", "index"),
    ("000001.SS", "SSEC", "Shanghai Composite", "China", "equity_index", "index"),
    ("^GDAXI", "DAX", "DAX", "Germany", "equity_index", "index"),
    ("^FTSE", "FTSE100", "FTSE 100", "UK", "equity_index", "index"),
    ("ES=F", "SP500_FUT", "S&P 500 futures", "US", "equity_index", "index"),
    ("GC=F", "GOLD", "Gold", "Global", "commodity", "USD per ounce"),
    ("^INDIAVIX", "INDIAVIX", "India VIX", "India", "volatility", "index"),
]


def instruments():
    """[(yahoo symbol, instrument, name, region, asset_class, unit)]"""
    same = [(sym, inst, *fred.SERIES[inst]) for sym, inst in SAME_AS_FRED]
    return same + YAHOO_ONLY


def _pct(now, then):
    return (now / then - 1) * 100 if then else None


def is_live(meta, last_date, now_ts):
    """True when the last candle is a session still trading: its regular period has started and not ended."""
    regular = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
    start, end = regular.get("start"), regular.get("end")
    if not (start and end and start <= now_ts < end):
        return False
    tz = ZoneInfo(meta.get("exchangeTimezoneName") or "UTC")
    return dt.datetime.fromtimestamp(start, tz).date().isoformat() == last_date


def row(spec, bars, meta, now_ts, fetched_at):
    """One snapshot line from a symbol's daily candles (oldest first), or None when there is too little."""
    sym, inst, name, region, asset_class, unit = spec
    by_date = {}
    for b in bars:  # Yahoo can repeat today's date with a live timestamp; the last one wins
        by_date[b["date"]] = b
    closes = [by_date[d]["close"] for d in sorted(by_date)]
    if len(closes) < 2:
        return None
    if unit == "percent" and closes[-1] > 20:  # an old-style yield quote (10 x the yield): back to percent
        closes = [c / 10 for c in closes]
    last_date, value = max(by_date), closes[-1]
    return {
        "instrument": inst, "name": name, "region": region, "asset_class": asset_class, "unit": unit,
        "yahoo_symbol": sym, "obs_date": last_date, "value": value,
        "change_abs": value - closes[-2],
        "change_pct": _pct(value, closes[-2]),
        "change_5d_pct": _pct(value, closes[-6]) if len(closes) > 5 else None,
        "change_20d_pct": _pct(value, closes[-21]) if len(closes) > 20 else None,
        "reading": "live" if is_live(meta, last_date, now_ts) else "close",
        "fetched_at_ist": fetched_at,
    }


def collect(now, fetch=yahoo.daily, workers=8):
    """Snapshot lines for every instrument; a symbol Yahoo can't give is skipped and logged."""
    now_ts = int(now.timestamp())
    fetched_at = now.strftime("%Y-%m-%d %H:%M:%S")

    def one(spec):
        try:
            bars, meta = fetch(spec[0], "3mo")
            return row(spec, bars, meta, now_ts, fetched_at)
        except Exception as err:  # noqa: BLE001 - one missing market can't stop the rest
            log.warning("snapshot %s unavailable: %s", spec[0], err)
            return None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        rows = list(pool.map(one, instruments()))
    return [r for r in rows if r]


def save(now):
    """Collect and write the snapshot; returns {'rows', 'key', 'seconds'}."""
    started = time.monotonic()
    rows = collect(now)
    if not rows:
        log.warning("snapshot: Yahoo gave nothing; nothing written")
        return {"rows": 0, "key": None, "seconds": round(time.monotonic() - started, 1)}
    body = ("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n").encode()
    key = put_landing("premarket", now.date(), f"yahoo_{now:%H%M}.jsonl", body, "application/x-ndjson")
    return {"rows": len(rows), "key": key, "seconds": round(time.monotonic() - started, 1)}

