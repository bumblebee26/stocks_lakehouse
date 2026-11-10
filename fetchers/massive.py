"""Fetch Massive (formerly Polygon.io) grouped daily bars: OHLCV for every US stock for one date.

API: GET https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{YYYY-MM-DD}
The API key is read from SSM Parameter Store (MASSIVE_KEY_PARAM), never from code or env vars.
Scheduled for 04:30 IST (after the US close the previous evening, New York time) and again at 07:30 IST.
Each scheduled run also catches up: it fetches any of the last CATCH_UP_DAYS weekdays whose file isn't in
the Databricks landing volume yet (a failed or empty run, e.g. data not yet published), and skips days
already there, so the 07:30 retry costs one check when 04:30 worked.

Lands at: landing/massive/date=YYYY-MM-DD/grouped_daily_YYYYMMDD.json (raw API response)
"""

import datetime as dt
import json
import os
import urllib.parse
from zoneinfo import ZoneInfo

import boto3

from common import NotFound, http_get, log, previous_weekday, put_landing, resolve_dates, run_dates

NEW_YORK = ZoneInfo("America/New_York")
URL = "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{d}"

# The free Basic plan is rate limited; keep backfills well under the limit.
BACKFILL_DELAY_SECONDS = 13

# US equities close at 16:00 ET; give the daily aggregates an hour to settle.
DATA_READY_HOUR_ET = 17
CATCH_UP_DAYS = 3

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

    key = put_landing("massive", trade_date, landing_file(trade_date).rsplit("/", 1)[1],
                      body, "application/json")
    return {"date": trade_date.isoformat(), "status": "written", "key": key, "tickers": count}


def landing_file(trade_date):
    return f"massive/date={trade_date.isoformat()}/grouped_daily_{trade_date:%Y%m%d}.json"


def scheduled_dates(latest=None):
    """The latest ready day plus earlier weekdays, oldest first, keeping only days not yet landed."""
    import databricks_volume  # local import: tests stub it

    day = latest or default_date()
    days = []
    while len(days) < CATCH_UP_DAYS:
        if day.weekday() < 5:
            days.append(day)
        day -= dt.timedelta(days=1)
    if not databricks_volume.enabled():
        return [days[0]]
    missing = []
    for d in reversed(days):
        try:
            if not databricks_volume.exists(landing_file(d)):
                missing.append(d)
        except Exception as err:  # noqa: BLE001 - if the check fails, fetch the latest day as before
            log.warning("Could not check the volume for %s: %s", d, err)
            return [days[0]]
    if not missing:
        log.info("Last %d weekdays already landed; nothing to fetch", CATCH_UP_DAYS)
    return missing


def handler(event, context):
    dates = resolve_dates(event, default_date) if event else scheduled_dates()
    return run_dates("massive", dates, fetch_one, delay_seconds=BACKFILL_DELAY_SECONDS,
                     context=context, reserve_seconds=60)
