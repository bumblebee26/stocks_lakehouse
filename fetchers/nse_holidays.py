"""NSE trading holidays (cash market), from NSE's own list (from 24 Nov 2026).

Used by the 08:30 brief (a "closed today" notice, no buy buttons), which also lands the list at
landing/nse_holidays/date=YYYY-MM-DD/holidays.jsonl for stocks.silver.nse_holidays (the dashboard's holiday banner),
and by the app export (the phone app's holiday card). NSE sometimes blocks cloud servers: callers keep their last
good list, or treat the day as a trading day.
"""
import datetime as dt
import json

from common import http_get, log

URL = "https://www.nseindia.com/api/holiday-master?type=trading"
HEADERS = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0 Safari/537.36"),
           "Accept": "application/json, */*", "Accept-Language": "en-US,en;q=0.9",
           "Referer": "https://www.nseindia.com/resources/exchange-communication-holidays"}

# "Happy ...!" for festivals and national days; a plain "Market holiday" for the rest (elections, Good Friday,
# Muharram).
FESTIVE = ("jayanti", "gurpurb", "diwali", "deepavali", "laxmi", "balipratipada", "holi", "christmas", "eid", "id-",
           "id ", "ramzan", "bakri", "dussehra", "dasara", "ganesh", "shivratri", "navami", "mahavir", "republic day",
           "independence day", "maharashtra day", "pongal", "new year")
SOMBER = ("good friday", "muharram", "election")


def parse(payload):
    """[{holiday_date 'YYYY-MM-DD', description}] from NSE's response, oldest first."""
    out = {}
    for row in (payload or {}).get("CM", []):
        try:
            day = dt.datetime.strptime(row["tradingDate"], "%d-%b-%Y").date().isoformat()
        except (KeyError, TypeError, ValueError):
            continue
        out[day] = (row.get("description") or "NSE holiday").rstrip("*").strip()
    return [{"holiday_date": d, "description": out[d]} for d in sorted(out)]


def fetch(fetch_json=None):
    """The list, or None if NSE can't be read."""
    try:
        payload = (fetch_json or (lambda: json.loads(http_get(URL, HEADERS, retries=2))))()
    except Exception as err:  # noqa: BLE001
        log.warning("NSE holiday list unavailable: %s", err)
        return None
    return parse(payload) or None


def holiday_on(day, rows):
    """The holiday's name if `day` is an NSE holiday, else None."""
    iso = day.isoformat()
    return next((r["description"] for r in rows or [] if r["holiday_date"] == iso), None)


def next_open(day, rows):
    """The next trading day after `day` (weekdays that aren't holidays)."""
    closed = {r["holiday_date"] for r in rows or []}
    d = day + dt.timedelta(days=1)
    while d.weekday() >= 5 or d.isoformat() in closed:
        d += dt.timedelta(days=1)
    return d


def next_holiday(day, rows):
    """The first weekday holiday after `day`: (date, name) or None."""
    for r in rows or []:
        d = dt.date.fromisoformat(r["holiday_date"])
        if d > day and d.weekday() < 5:
            return d, r["description"]
    return None


def status(day, rows):
    """{'holiday': name or None, 'title', 'message', 'next_open'} for the app and the dashboard."""
    name = holiday_on(day, rows)
    opens = next_open(day, rows)
    if name:
        festive = any(w in name.lower() for w in FESTIVE) and not any(w in name.lower() for w in SOMBER)
        return {"holiday": name, "title": f"Happy {name}!" if festive else f"Market holiday: {name}",
                "message": f"Markets are closed on {day.day} {day:%b} on account of {name} and will open at "
                           f"9:15 AM on {opens.day} {opens:%b}.",
                "next_open": opens.isoformat()}
    nxt = next_holiday(day, rows)
    return {"holiday": None, "title": "", "next_open": opens.isoformat(),
            "message": (f"Next market holiday: {nxt[0]:%a} {nxt[0].day} {nxt[0]:%b} ({nxt[1]})" if nxt else "")}
