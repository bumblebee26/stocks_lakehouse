"""Fetch global market series from FRED (Federal Reserve Bank of St. Louis).

API: GET https://api.stlouisfed.org/fred/series/observations  (free API key, 120 requests/minute)
The key is read from SSM Parameter Store (FRED_KEY_PARAM), never from code or env vars.

Every run fetches a rolling window for all series, because some series are
published late (e.g. exchange rates are released weekly) and FRED revises
recent values. Silver keeps the latest file's value per series and date.

Event:
  {}                                        -> last WINDOW_DAYS days up to today (scheduled run)
  {"start": "2024-07-01", "end": "2026-11-07"} -> that whole range in one run (backfill)

Lands at: landing/fred/date=<end>/fred_<start>_<end>.json
Format: newline-delimited JSON, one observation per line:
  {"series_id": "NIKKEI225", "date": "2026-11-06", "value": 50123.4,
   "name": "Nikkei 225", "region": "Japan", "asset_class": "equity_index", "unit": "index"}
"""

import datetime as dt
import json
import os
import urllib.parse

import boto3

from common import http_get, log, parse_date, put_landing

URL = "https://api.stlouisfed.org/fred/series/observations"
WINDOW_DAYS = 21

# series_id: (name, region, asset_class, unit)
SERIES = {
    # Equity indices
    "SP500": ("S&P 500", "US", "equity_index", "index"),
    "DJIA": ("Dow Jones Industrial Average", "US", "equity_index", "index"),
    "NASDAQCOM": ("Nasdaq Composite", "US", "equity_index", "index"),
    "NIKKEI225": ("Nikkei 225", "Japan", "equity_index", "index"),
    # Volatility
    "VIXCLS": ("CBOE VIX", "US", "volatility", "index"),
    # Currencies (H.10 noon rates, released weekly)
    "DEXINUS": ("USD/INR", "India", "fx", "INR per USD"),
    "DEXJPUS": ("USD/JPY", "Japan", "fx", "JPY per USD"),
    "DEXUSEU": ("EUR/USD", "Eurozone", "fx", "USD per EUR"),
    "DEXUSUK": ("GBP/USD", "UK", "fx", "USD per GBP"),
    "DEXCHUS": ("USD/CNY", "China", "fx", "CNY per USD"),
    "DTWEXBGS": ("US dollar index (broad)", "Global", "fx", "index"),
    # Commodities
    "DCOILBRENTEU": ("Brent crude", "Global", "commodity", "USD per barrel"),
    "DCOILWTICO": ("WTI crude", "US", "commodity", "USD per barrel"),
    # Rates
    "DGS10": ("US 10-year Treasury yield", "US", "rates", "percent"),
}

_api_key = None


def api_key():
    global _api_key
    if _api_key is None:
        ssm = boto3.client("ssm")
        _api_key = ssm.get_parameter(Name=os.environ["FRED_KEY_PARAM"],
                                     WithDecryption=True)["Parameter"]["Value"]
    return _api_key


def resolve_window(event, today=None):
    event = event or {}
    end = parse_date(event["end"]) if "end" in event else (today or dt.date.today())
    start = parse_date(event["start"]) if "start" in event else end - dt.timedelta(days=WINDOW_DAYS)
    if end < start:
        raise ValueError("end must be on or after start")
    return start, end


def fetch_series(series_id, start, end):
    """Return [(date, value)] for one series; FRED marks missing values as '.'."""
    params = {
        "series_id": series_id,
        "api_key": api_key(),
        "file_type": "json",
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
    }
    redacted = f"{URL}?series_id={series_id}&api_key=***"
    payload = json.loads(http_get(f"{URL}?{urllib.parse.urlencode(params)}", log_url=redacted))
    return [(o["date"], float(o["value"])) for o in payload.get("observations", []) if o["value"] != "."]


def handler(event, context):
    start, end = resolve_window(event)
    lines, counts = [], {}
    for series_id, (name, region, asset_class, unit) in SERIES.items():
        observations = fetch_series(series_id, start, end)
        counts[series_id] = len(observations)
        for date, value in observations:
            lines.append(json.dumps({
                "series_id": series_id, "date": date, "value": value,
                "name": name, "region": region, "asset_class": asset_class, "unit": unit,
            }))

    if not lines:
        log.warning("FRED returned no observations for %s..%s", start, end)
        return {"source": "fred", "start": start.isoformat(), "end": end.isoformat(),
                "status": "no_data", "observations": counts}

    key = put_landing("fred", end, f"fred_{start:%Y%m%d}_{end:%Y%m%d}.json",
                      ("\n".join(lines) + "\n").encode(), "application/x-ndjson")
    summary = {"source": "fred", "start": start.isoformat(), "end": end.isoformat(),
               "status": "written", "key": key, "rows": len(lines), "observations": counts}
    log.info("Summary: %s", summary)
    return summary
