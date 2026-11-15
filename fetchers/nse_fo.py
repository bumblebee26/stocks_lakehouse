"""NSE's list of stocks with futures & options (F&O), refreshed weekly.

File: https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv (market lots per F&O underlying; index rows such
as NIFTY are dropped). Scheduled Mondays 08:00 IST.

Lands at: landing/nse_fo/date=YYYY-MM-DD/fno_symbols.jsonl ({"symbol", "underlying", "as_of"} per stock)
"""
import csv
import datetime as dt
import io
import json
from zoneinfo import ZoneInfo

from common import http_get, log, put_landing
from nse import HEADERS

IST = ZoneInfo("Asia/Kolkata")
URL = "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv"
INDEX_ROWS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "SENSEX", "BANKEX"}


def parse(text, as_of):
    reader = csv.reader(io.StringIO(text))
    header = [h.strip().upper() for h in next(reader, [])]
    try:
        i_sym, i_und = header.index("SYMBOL"), header.index("UNDERLYING")
    except ValueError:
        raise RuntimeError(f"unexpected fo_mktlots header: {header[:4]}")
    rows = [r for r in reader if len(r) > max(i_sym, i_und)]
    # NSE lists index derivatives first, then a divider row ("Derivatives on Individual Securities", "Symbol");
    # only rows after it are stocks. INDEX_ROWS still guards a file without the divider.
    divider = next((n for n, r in enumerate(rows) if r[i_sym].strip().upper() == "SYMBOL"), None)
    if divider is not None:
        rows = rows[divider + 1:]
    out = []
    for row in rows:
        sym = row[i_sym].strip().upper()
        if not sym or sym == "SYMBOL" or sym in INDEX_ROWS:
            continue
        out.append({"symbol": sym, "underlying": row[i_und].strip(), "as_of": as_of})
    return out


def handler(event, context):
    today = dt.datetime.now(IST).date()
    rows = parse(http_get(URL, HEADERS).decode("utf-8", "replace"), today.isoformat())
    if len(rows) < 100:
        raise RuntimeError(f"only {len(rows)} F&O stocks parsed; NSE's file may have changed")
    key = put_landing("nse_fo", today, "fno_symbols.jsonl", "\n".join(json.dumps(r) for r in rows).encode(),
                      "application/x-ndjson")
    log.info("F&O list: %d stocks", len(rows))
    return {"stocks": len(rows), "key": key}
