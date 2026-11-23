"""Mobile app export: the dashboard's numbers as one JSON file for the phone app (from 22 Nov 2026).

Runs a few minutes after each Databricks job (EventBridge Scheduler). Reads the dashboard's own SQL, rendered by
the databricks stack into s3://<app bucket>/config/queries.json ({dataset: {sql, fields}}), runs each query on the
SQL warehouse and writes s3://<app bucket>/<secret path>/data/desk.json: {updated_at, datasets: {name: {fields,
rows}}, failed}. A query that fails keeps its previous rows (and is listed in failed), so one slow table never
blanks the app. The app (app/index.html) is served by CloudFront from the same secret path.

Event: {} normally; {"dry_run": true} returns the payload without writing it.

From 24 Nov it also adds five years of daily closes from Yahoo for the app's charts that have no lakehouse table:
USD/INR (Markets tab) and Accenture, NYSE: ACN (Custom tab). A failed fetch keeps the previous series.
"""

import datetime as dt
import json
import os
from concurrent.futures import ThreadPoolExecutor

import boto3

import databricks_sql
from common import log

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
QUERIES_KEY = "config/queries.json"
WORKERS = 4
# app dataset -> (Yahoo symbol, label); five years of daily closes as rows {date, close}
YAHOO_SERIES = {"usdinr_5y": ("INR=X", "USD/INR"), "acn_5y": ("ACN", "Accenture (NYSE: ACN, USD)")}


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


def yahoo_series(previous, fetch=None):
    """{dataset: {fields, rows, label}} for YAHOO_SERIES and the names that failed (those keep the previous rows)."""
    if fetch is None:
        import yahoo

        fetch = yahoo.daily
    old = (previous or {}).get("datasets") or {}
    out, failed = {}, []
    for name, (sym, label) in YAHOO_SERIES.items():
        try:
            bars, _ = fetch(sym, "5y")
            rows = [{"date": b["date"], "close": round(float(b["close"]), 4)} for b in bars if b.get("close")]
            if not rows:
                raise ValueError("no closes")
            out[name] = {"fields": ["date", "close"], "rows": rows, "label": label}
        except Exception as err:  # noqa: BLE001 - keep the last good series
            log.error("Yahoo series %s failed: %s", name, err)
            out[name] = old.get(name) or {"fields": ["date", "close"], "rows": [], "label": label}
            failed.append(name)
    return out, failed


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
