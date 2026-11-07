"""Fetch Massive (formerly Polygon.io) grouped daily bars: OHLCV for every US stock for one date.

API: GET https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{YYYY-MM-DD}
The API key is read from SSM Parameter Store (MASSIVE_KEY_PARAM), never from code or env vars.
Scheduled for 04:30 IST (after the US close the previous evening, New York time).

Lands at: landing/massive/date=YYYY-MM-DD/grouped_daily_YYYYMMDD.json (raw API response)
"""

import datetime as dt
import json
import os
import time
import urllib.parse
from zoneinfo import ZoneInfo

import boto3

from common import NotFound, http_get, log, previous_weekday, put_landing, resolve_dates

NEW_YORK = ZoneInfo("America/New_York")
URL = "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{d}"

# The free Basic plan is rate limited; keep backfills well under the limit.
BACKFILL_DELAY_SECONDS = 13

# US equities close at 16:00 ET; give the daily aggregates an hour to settle.
DATA_READY_HOUR_ET = 17

_api_key = None


def api_key():
    global _api_key
    if _api_key is None:
        ssm = boto3.client("ssm")
        _api_key = ssm.get_parameter(Name=os.environ["MASSIVE_KEY_PARAM"],
                                     WithDecryption=True)["Parameter"]["Value"]
    return _api_key


def default_date(now=None):
    """Most recent US trading day whose data should be ready (weekends skipped, not holidays)."""
    now = now or dt.datetime.now(NEW_YORK)
    day = now.date()
    if now.hour < DATA_READY_HOUR_ET:
        day -= dt.timedelta(days=1)
    return previous_weekday(day)


def fetch_one(trade_date):
    if trade_date.weekday() >= 5:
        return {"date": trade_date.isoformat(), "status": "skipped_weekend"}

    query = urllib.parse.urlencode({"adjusted": "true", "apiKey": api_key()})
    url = f"{URL.format(d=trade_date.isoformat())}?{query}"
    redacted = f"{URL.format(d=trade_date.isoformat())}?adjusted=true&apiKey=***"

    try:
        body = http_get(url, log_url=redacted)
    except NotFound:
        return {"date": trade_date.isoformat(), "status": "no_file"}

    payload = json.loads(body)
    count = payload.get("resultsCount") or 0
    if count == 0:
        # Market holiday, or data not available yet / outside your plan's history window.
        log.warning("Massive returned 0 results for %s (status=%s)", trade_date, payload.get("status"))
        return {"date": trade_date.isoformat(), "status": "no_data", "api_status": payload.get("status")}

    key = put_landing("massive", trade_date, f"grouped_daily_{trade_date:%Y%m%d}.json",
                      body, "application/json")
    return {"date": trade_date.isoformat(), "status": "written", "key": key, "tickers": count}


def handler(event, context):
    dates = resolve_dates(event, default_date)
    results = []
    for i, trade_date in enumerate(dates):
        if i:
            time.sleep(BACKFILL_DELAY_SECONDS)
        results.append(fetch_one(trade_date))

    summary = {"source": "massive", "requested": len(dates),
               "written": sum(r["status"] == "written" for r in results), "results": results}
    log.info("Summary: %s", {k: v for k, v in summary.items() if k != "results"})
    return summary
