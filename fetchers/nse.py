"""Fetch the NSE capital-market (equities) bhavcopy in UDiFF format.

File: https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_<YYYYMMDD>_F_0000.csv.zip
Published on trading days, usually by early evening IST. Scheduled for 19:30 IST.

Lands at: landing/nse/date=YYYY-MM-DD/bhavcopy_cm_YYYYMMDD.csv.zip (the original zip, untouched)
"""

import datetime as dt
from zoneinfo import ZoneInfo

from common import NotFound, http_get, log, put_landing, resolve_dates, run_dates

IST = ZoneInfo("Asia/Kolkata")
URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"

# NSE rejects requests that don't look like a browser.
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"),
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
}

# Be polite during backfills; NSE may block aggressive clients.
BACKFILL_DELAY_SECONDS = 2


def default_date():
    """Today in India. The schedule only fires Mon-Fri."""
    return dt.datetime.now(IST).date()


def fetch_one(trade_date):
    if trade_date.weekday() >= 5:
        return {"date": trade_date.isoformat(), "status": "skipped_weekend"}

    try:
        body = http_get(URL.format(d=trade_date), HEADERS)
    except NotFound:
        # Market holiday, or not published yet.
        log.warning("No NSE bhavcopy for %s (holiday or not yet published)", trade_date)
        return {"date": trade_date.isoformat(), "status": "no_file"}

    if not body.startswith(b"PK"):
        raise RuntimeError(f"NSE returned a non-zip response for {trade_date} "
                           f"({len(body)} bytes): {body[:120]!r}")

    key = put_landing("nse", trade_date, f"bhavcopy_cm_{trade_date:%Y%m%d}.csv.zip",
                      body, "application/zip")
    return {"date": trade_date.isoformat(), "status": "written", "key": key, "bytes": len(body)}


def handler(event, context):
    dates = resolve_dates(event, default_date)
    return run_dates("nse", dates, fetch_one, delay_seconds=BACKFILL_DELAY_SECONDS,
                     context=context, reserve_seconds=30)
