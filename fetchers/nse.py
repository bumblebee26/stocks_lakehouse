"""Fetch the NSE capital-market (equities) bhavcopy in UDiFF format, and the day's delivery figures.

File: https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_<YYYYMMDD>_F_0000.csv.zip
Delivery: https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_<DDMMYYYY>.csv (DELIV_QTY, DELIV_PER)
Indices:  https://nsearchives.nseindia.com/content/indices/ind_close_all_<DDMMYYYY>.csv (every NSE index's day,
          India VIX included: the trader's risk per trade is set from it each morning)
Published on trading days, usually by early evening IST. Scheduled for 19:30 IST.

Lands at: landing/nse/date=YYYY-MM-DD/bhavcopy_cm_YYYYMMDD.csv.zip (the original zip, untouched)
          landing/nse_delivery/date=YYYY-MM-DD/delivery_YYYYMMDD.jsonl (symbol, series, deliv_qty, deliv_per)
          landing/nse_indices/date=YYYY-MM-DD/indices_YYYYMMDD.jsonl (index_name, open, high, low, close,
          change_pct)
The delivery and index files are optional: if one isn't there, the bhavcopy still lands and that part is missing.
"""
import csv
import io
import json

import datetime as dt
from zoneinfo import ZoneInfo

from common import NotFound, http_get, log, put_landing, resolve_dates, run_dates

IST = ZoneInfo("Asia/Kolkata")
URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"
DELIVERY_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"
INDICES_URL = "https://nsearchives.nseindia.com/content/indices/ind_close_all_{d:%d%m%Y}.csv"

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
    return {"date": trade_date.isoformat(), "status": "written", "key": key, "bytes": len(body),
            "delivery": fetch_delivery(trade_date), "indices": fetch_indices(trade_date)}


def parse_delivery(text):
    """sec_bhavdata_full CSV (headers and values padded with spaces) -> clean rows for EQ/BE/BZ series."""
    reader = csv.reader(io.StringIO(text))
    header = [h.strip().upper() for h in next(reader, [])]
    out = []
    for row in reader:
        rec = dict(zip(header, (v.strip() for v in row)))
        if rec.get("SERIES") not in ("EQ", "BE", "BZ", "SM", "ST"):
            continue
        try:
            qty = float(rec["DELIV_QTY"]) if rec.get("DELIV_QTY") not in (None, "", "-") else None
            pct = float(rec["DELIV_PER"]) if rec.get("DELIV_PER") not in (None, "", "-") else None
        except ValueError:
            qty = pct = None
        out.append({"symbol": rec.get("SYMBOL"), "series": rec.get("SERIES"), "deliv_qty": qty, "deliv_per": pct})
    return out


def fetch_delivery(trade_date):
    try:
        body = http_get(DELIVERY_URL.format(d=trade_date), HEADERS)
    except NotFound:
        log.warning("No NSE delivery file for %s yet", trade_date)
        return "no_file"
    except Exception as err:  # noqa: BLE001 - optional data: never fail the bhavcopy run over it
        log.warning("NSE delivery file for %s failed: %s", trade_date, err)
        return "error"
    rows = parse_delivery(body.decode("utf-8", "replace"))
    if not rows:
        return "empty"
    put_landing("nse_delivery", trade_date, f"delivery_{trade_date:%Y%m%d}.jsonl",
                "\n".join(json.dumps(r) for r in rows).encode(), "application/x-ndjson")
    return f"written ({len(rows)} rows)"


def _num(v):
    v = (v or "").strip().replace(",", "")
    try:
        return float(v) if v not in ("", "-") else None
    except ValueError:
        return None


def parse_indices(text):
    """ind_close_all CSV -> one row per index: index_name, open, high, low, close, change_pct."""
    reader = csv.reader(io.StringIO(text))
    header = [h.strip().lower() for h in next(reader, [])]
    col = {name: next((i for i, h in enumerate(header) if h.startswith(name)), None)
           for name in ("index name", "open", "high", "low", "closing", "change(%)")}
    if col["index name"] is None or col["closing"] is None:
        return []
    out = []
    for row in reader:
        if len(row) <= col["closing"]:
            continue
        get = lambda k: _num(row[col[k]]) if col[k] is not None and col[k] < len(row) else None  # noqa: E731
        close = get("closing")
        name = row[col["index name"]].strip()
        if name and close is not None:
            out.append({"index_name": name, "open": get("open"), "high": get("high"), "low": get("low"),
                        "close": close, "change_pct": get("change(%)")})
    return out


def fetch_indices(trade_date):
    try:
        body = http_get(INDICES_URL.format(d=trade_date), HEADERS)
    except NotFound:
        log.warning("No NSE index file for %s yet", trade_date)
        return "no_file"
    except Exception as err:  # noqa: BLE001 - optional data: never fail the bhavcopy run over it
        log.warning("NSE index file for %s failed: %s", trade_date, err)
        return "error"
    rows = parse_indices(body.decode("utf-8", "replace"))
    if not rows:
        return "empty"
    put_landing("nse_indices", trade_date, f"indices_{trade_date:%Y%m%d}.jsonl",
                "\n".join(json.dumps(r) for r in rows).encode(), "application/x-ndjson")
    vix = next((r for r in rows if r["index_name"].lower() == "india vix"), None)
    return f"written ({len(rows)} rows, India VIX {vix['close'] if vix else 'missing'})"


def handler(event, context):
    dates = resolve_dates(event, default_date)
    return run_dates("nse", dates, fetch_one, delay_seconds=BACKFILL_DELAY_SECONDS,
                     context=context, reserve_seconds=30)
