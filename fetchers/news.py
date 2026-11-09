"""Company news from NSE's official RSS feeds, every 30 minutes on weekdays 08:00-19:30 IST.

Feeds (https://www.nseindia.com/rss-feed): announcements, financial results, board meetings,
corporate actions. Each item is tagged with its NSE symbol (from the attachment link, else the
company name matched against NSE's equity list) and a category (results, order, management, ...).

Lands in the NEWS volume, not the landing volume: the pipeline job refreshes whenever a file
lands in the landing volume, and a refresh every 30 minutes would use up Databricks Free
Edition's daily compute. The pipeline still reads the news volume at every normal refresh.

  landing/news/date=YYYY-MM-DD/snap_HHMM.jsonl   S3 copy of each run (bronze reads the volume copy)
  <news volume>/date=YYYY-MM-DD/snap_HHMM.jsonl  same file; bronze.news_raw ingests these
  <news volume>/date=YYYY-MM-DD/latest.jsonl     the newest run, read directly by the paper trader

A feed that fails is skipped and reported; the run fails only if every feed fails.
"""
import csv
import datetime as dt
import email.utils
import hashlib
import io
import json
import os
import re
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

from common import http_get, log, s3

IST = ZoneInfo("Asia/Kolkata")
BASE = "https://nsearchives.nseindia.com/content/RSS/"
FEEDS = {
    "announcement": BASE + "Online_announcements.xml",
    "results": BASE + "Financial_Results.xml",
    "board_meeting": BASE + "Board_Meetings.xml",
    "corporate_action": BASE + "Corporate_action.xml",
}
EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"),
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
}

# First match wins, so specific phrases come before general words ("order" is last of the risky ones).
CATEGORIES = [
    ("results", ("financial result", "quarterly result", "audited result", "unaudited result")),
    ("rating", ("credit rating",)),
    ("regulatory", ("sebi", "penalty", "show cause", "litigation", "tax demand", "adjudication", "fine ")),
    ("pledge", ("pledge", "encumbrance")),
    ("insider", ("insider trading", "sast", "regulation 29", "regulation 31")),
    ("management", ("resignation", "appointment", "cessation", "change in management", "managing director",
                    "chief executive", "chief financial", "key managerial")),
    ("dividend_split_bonus", ("dividend", "bonus", "split", "sub-division", "buyback", "buy back",
                              "rights issue", "record date")),
    ("mna", ("acquisition", "merger", "amalgamation", "demerger", "scheme of arrangement", "stake")),
    ("meeting", ("analyst", "investor meet", "earnings call", "con. call", "conference call")),
    ("order", ("order", "contract", "bagging", "letter of award", "loa ")),
]
LINK_SYMBOL = re.compile(r"/([A-Z0-9&\-]{2,20})_\d{6,}")
NAME_DROP = re.compile(r"\b(LIMITED|LTD|THE|CO|COMPANY|INDIA|OF)\b")

_equity = {"date": None, "names": {}, "symbols": set()}


def norm_name(name):
    s = (name or "").upper().replace("&", " AND ")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = NAME_DROP.sub(" ", s)
    return " ".join(s.split())


def equity_map(today):
    """NSE symbol <-> company name, fetched once a day per warm Lambda. Empty map if NSE fails."""
    if _equity["date"] != today:
        try:
            text = http_get(EQUITY_LIST_URL, HEADERS, retries=2).decode("utf-8", "replace")
            names, symbols = {}, set()
            for row in csv.DictReader(io.StringIO(text)):
                row = {k.strip(): (v or "").strip() for k, v in row.items() if k}
                sym = row.get("SYMBOL")
                if sym:
                    symbols.add(sym)
                    names[norm_name(row.get("NAME OF COMPANY"))] = sym
            _equity.update(date=today, names=names, symbols=symbols)
            log.info("Equity list: %d symbols", len(symbols))
        except Exception as err:  # noqa: BLE001 - link-based symbols still work
            log.warning("Equity list unavailable: %s", err)
            _equity.update(date=today, names={}, symbols=set())
    return _equity


def symbol_for(item, eq):
    m = LINK_SYMBOL.search(item.get("link") or "")
    if m and (not eq["symbols"] or m.group(1) in eq["symbols"]):
        return m.group(1)
    for text in (item.get("title"), (item.get("description") or "").split(" has ")[0]):
        sym = eq["names"].get(norm_name(text))
        if sym:
            return sym
    return None


def category_for(feed, item):
    if feed == "results":
        return "results"
    if feed == "board_meeting":
        return "board_meeting"
    text = f" {item.get('title', '')} {item.get('description', '')} ".lower()
    for name, words in CATEGORIES:
        if any(w in text for w in words):
            return name
    return "corporate_action" if feed == "corporate_action" else "other"


def parse_time(raw):
    """NSE uses '09-Nov-2026 14:12:55' or RFC 822 dates. Returns 'YYYY-MM-DD HH:MM:SS' IST or None."""
    raw = (raw or "").strip()
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%Y"):
        try:
            return dt.datetime.strptime(raw, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    try:
        t = email.utils.parsedate_to_datetime(raw)
        if t.tzinfo:
            t = t.astimezone(IST)
        return t.strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


def parse_feed(feed, body, eq, fetched_at):
    root = ET.fromstring(body)
    out = []
    for node in root.iter("item"):
        item = {child.tag.split("}")[-1].lower(): " ".join((child.text or "").split()) for child in node}
        title, link = item.get("title", ""), item.get("link", "")
        published_raw = item.get("pubdate", "")
        uid = hashlib.sha1(f"{feed}|{link}|{title}|{published_raw}".encode()).hexdigest()[:16]
        out.append({
            "id": uid, "feed": feed, "symbol": symbol_for(item, eq), "company": title,
            "category": category_for(feed, item), "title": title,
            "description": item.get("description", "")[:2000], "link": link,
            "published_ist": parse_time(published_raw), "published_raw": published_raw,
            "fetched_at_ist": fetched_at,
        })
    return out


def put_news(relative, body):
    """S3 copy under landing/news/, then the news volume (when pushing is on)."""
    bucket = os.environ["LANDING_BUCKET"]
    s3().put_object(Bucket=bucket, Key=f"landing/news/{relative}", Body=body,
                    ContentType="application/x-ndjson")
    import databricks_volume
    if databricks_volume.enabled():
        databricks_volume.upload(relative, body, volume=os.environ.get("NEWS_VOLUME_PATH"))


def handler(event, context):
    now = dt.datetime.now(IST)
    fetched_at = now.strftime("%Y-%m-%d %H:%M:%S")
    eq = equity_map(now.date())
    items, failed = [], {}
    for feed, url in FEEDS.items():
        try:
            items += parse_feed(feed, http_get(url, HEADERS, retries=2), eq, fetched_at)
        except Exception as err:  # noqa: BLE001 - one bad feed shouldn't stop the others
            log.warning("Feed %s failed: %s", feed, err)
            failed[feed] = str(err)[:200]
    if len(failed) == len(FEEDS):
        raise RuntimeError(f"All NSE news feeds failed: {failed}")
    body = "".join(json.dumps(i, ensure_ascii=False) + "\n" for i in items).encode()
    day = f"date={now:%Y-%m-%d}"
    put_news(f"{day}/snap_{now:%H%M}.jsonl", body)
    put_news(f"{day}/latest.jsonl", body)
    mapped = sum(1 for i in items if i["symbol"])
    log.info("News: %d items (%d with a symbol), failed feeds: %s", len(items), mapped, list(failed))
    return {"items": len(items), "with_symbol": mapped, "failed_feeds": failed}
