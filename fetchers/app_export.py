"""Mobile app export: the dashboard's numbers as one JSON file for the phone app (from 22 Nov 2026).

Runs a few minutes after each Databricks job (EventBridge Scheduler). Reads the dashboard's own SQL, rendered by
the databricks stack into s3://<app bucket>/config/queries.json ({dataset: {sql, fields}}), runs each query on the
SQL warehouse and writes s3://<app bucket>/<secret path>/data/desk.json: {updated_at, datasets: {name: {fields,
rows}}, failed}. A query that fails keeps its previous rows (and is listed in failed), so one slow table never
blanks the app. The app (app/index.html) is served by CloudFront from the same secret path.

Event: {} normally; {"dry_run": true} returns the payload without writing it.

From 24 Nov it also adds Yahoo's full daily history (weekly before the last five years) for the app's charts that have no lakehouse table:
USD/INR (Markets tab) and Accenture, NYSE: ACN (Desk tab). A failed fetch keeps the previous series.

From 24 Nov the Markets tab also shows Mumbai petrol, diesel and CNG prices (dataset fuel_mumbai), read on every
export from one source, CarDekho's Mumbai fuel page (all three on one page; prices revise at 6 a.m.). A new price is
accepted only within 15% of the last good one; otherwise, or if the page fails, the last good price stays with its
date (the starting values are from 15-16 Nov 2026).
"""

import datetime as dt
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

import boto3

import databricks_sql
from common import http_get, log

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
QUERIES_KEY = "config/queries.json"
WORKERS = 4
# app dataset -> (Yahoo symbol, label); five years of daily closes as rows {date, close}
YAHOO_SERIES = {"usdinr_5y": ("INR=X", "USD/INR"), "acn_5y": ("ACN", "Accenture (NYSE: ACN, USD)")}
FUEL_URL = "https://www.cardekho.com/fuel-price-in-mumbai-city"
FUEL_SOURCE = "CarDekho"
FUEL_FIELDS = ["item", "rate", "unit", "as_of", "source"]
FUEL_UNITS = {"Petrol": "Rs / litre", "Diesel": "Rs / litre", "CNG": "Rs / kg"}
FUEL_START = {"Petrol": (111.21, "2026-11-16"), "Diesel": (97.83, "2026-11-16"), "CNG": (91.50, "2026-11-15")}
FUEL_BAND = 0.15


def _s3():
    return boto3.client("s3")


def load_previous(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001 - first run, or unreadable: start empty
        return {}


def run_all(queries, previous, query=None):
    """{name: {fields, rows}} and the names that failed (those keep the previous rows)."""
    query = query or databricks_sql.query
    old = (previous or {}).get("datasets") or {}

    def one(item):
        name, spec = item
        try:
            return name, {"fields": spec.get("fields") or [], "rows": query(spec["sql"])}, None
        except Exception as err:  # noqa: BLE001 - keep the app showing the last good numbers
            log.error("App dataset %s failed: %s", name, err)
            return name, old.get(name) or {"fields": spec.get("fields") or [], "rows": []}, name

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(one, sorted(queries.items())))
    datasets = {name: data for name, data, _ in results}
    failed = [name for _, _, name in results if name]
    return datasets, failed


def thin_old(rows, years=5):
    """Full history for the app's "All" range, kept light: daily for the last `years` years, one close per week
    (the week's last) before that."""
    if not rows:
        return rows
    last = dt.date.fromisoformat(rows[-1]["date"])
    cut = last.replace(year=last.year - years).isoformat()
    old, recent = [r for r in rows if r["date"] < cut], [r for r in rows if r["date"] >= cut]
    weekly = {}
    for r in old:
        weekly[dt.date.fromisoformat(r["date"]).isocalendar()[:2]] = r
    return [weekly[k] for k in sorted(weekly)] + recent


def yahoo_series(previous, fetch=None):
    """{dataset: {fields, rows, label}} for YAHOO_SERIES and the names that failed (those keep the previous rows)."""
    if fetch is None:
        import yahoo

        fetch = yahoo.daily
    old = (previous or {}).get("datasets") or {}
    out, failed = {}, []
    for name, (sym, label) in YAHOO_SERIES.items():
        try:
            bars, _ = fetch(sym, "max")
            rows = thin_old([{"date": b["date"], "close": round(float(b["close"]), 4)} for b in bars if b.get("close")])
            if not rows:
                raise ValueError("no closes")
            out[name] = {"fields": ["date", "close"], "rows": rows, "label": label}
        except Exception as err:  # noqa: BLE001 - keep the last good series
            log.error("Yahoo series %s failed: %s", name, err)
            out[name] = old.get(name) or {"fields": ["date", "close"], "rows": [], "label": label}
            failed.append(name)
    return out, failed


def parse_fuel(html):
    """{fuel: [prices]} from the page text: every rupee amount just after the fuel's name (e.g. "CNG ... ₹91.5"),
    in page order. Menus and car prices can match too, so fuel_rates takes the first one near the last good price."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&#8377;|&#x20[bB]9;", "₹", text)
    text = re.sub(r"\s+", " ", text)
    out = {}
    for fuel in FUEL_UNITS:
        pattern = fuel + r"\b[^₹]{0,160}?(?:₹|Rs\.?)\s*([0-9]{2,3}(?:\.[0-9]{1,2})?)"
        out[fuel] = [float(v) for v in re.findall(pattern, text, flags=re.I)]
    return out


def fuel_rates(previous, fetch=None, today=None):
    """The fuel_mumbai dataset and whether anything failed. Each price keeps its last good value and date unless the
    page gives one within FUEL_BAND of it."""
    today = today or dt.datetime.now(IST).date().isoformat()
    prev_rows = (((previous or {}).get("datasets") or {}).get("fuel_mumbai") or {}).get("rows") or []
    old = {r["item"]: r for r in prev_rows if r.get("item") in FUEL_UNITS}
    try:
        html = (fetch or (lambda: http_get(FUEL_URL, headers={"User-Agent": "Mozilla/5.0"}, retries=2)))()
        got = parse_fuel(html.decode("utf-8", "replace") if isinstance(html, bytes) else html)
    except Exception as err:  # noqa: BLE001 - keep the last good prices
        log.error("Fuel prices failed: %s", err)
        got = {}
    rows, bad = [], False
    for fuel, unit in FUEL_UNITS.items():
        last = old.get(fuel) or {"rate": f"{FUEL_START[fuel][0]:.2f}", "as_of": FUEL_START[fuel][1],
                                 "source": FUEL_SOURCE}
        near = [v for v in got.get(fuel, []) if abs(v / float(last["rate"]) - 1) <= FUEL_BAND]
        if near:
            last = {"rate": f"{near[0]:.2f}", "as_of": today, "source": FUEL_SOURCE}
        else:
            bad = True
            log.error("No %s price within 15%% of %s on the page (found %s); kept the last one", fuel,
                      last["rate"], got.get(fuel))
        rows.append({"item": fuel, "rate": last["rate"], "unit": unit, "as_of": last["as_of"],
                     "source": last["source"]})
    return {"fields": FUEL_FIELDS, "rows": rows}, bad


def handler(event, context):
    event = event or {}
    bucket, prefix = os.environ["APP_BUCKET"], os.environ["APP_PREFIX"]
    s3 = _s3()
    queries = json.loads(s3.get_object(Bucket=bucket, Key=QUERIES_KEY)["Body"].read())
    data_key = f"{prefix}/data/desk.json"
    previous = load_previous(s3, bucket, data_key)
    datasets, failed = run_all(queries, previous)
    series, series_failed = yahoo_series(previous)
    datasets.update(series)
    datasets["fuel_mumbai"], fuel_failed = fuel_rates(previous)
    failed += ["fuel_mumbai"] if fuel_failed else []
    failed += series_failed
    payload = {"updated_at": dt.datetime.now(IST).strftime("%Y-%m-%d %H:%M"), "datasets": datasets,
               "failed": failed}
    body = json.dumps(payload, separators=(",", ":"), default=str).encode()
    if event.get("dry_run"):
        return {"written": False, "bytes": len(body), "failed": failed, "payload": payload}
    s3.put_object(Bucket=bucket, Key=data_key, Body=body, ContentType="application/json",
                  CacheControl="max-age=60")
    log.info("App data written: %d datasets, %d bytes, failed %s", len(datasets), len(body), failed)
    return {"written": True, "bytes": len(body), "failed": failed}
