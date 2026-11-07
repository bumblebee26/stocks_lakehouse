"""Shared helpers for the Lambda fetchers.

Standard library + boto3 only (boto3 ships with the Lambda Python runtime),
so the deployment zip is just these .py files with no dependencies to package.
"""

import datetime as dt
import logging
import os
import time
import urllib.error
import urllib.request

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

_s3 = None


def s3():
    """Lazily create the S3 client so unit tests can import without AWS."""
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


class NotFound(Exception):
    """The source has no file for that date (holiday, weekend, or not yet published)."""


def http_get(url, headers=None, *, retries=3, backoff_seconds=5, timeout=60, log_url=None):
    """GET a URL and return the body as bytes.

    Retries on network errors, 429 and 5xx with linear backoff. Raises NotFound on 404.
    Pass log_url to log a redacted URL (for example without an API key).
    """
    shown = log_url or url
    request = urllib.request.Request(url, headers=headers or {})
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as err:
            if err.code == 404:
                raise NotFound(shown) from err
            if err.code not in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {err.code} from {shown}") from err
            last_error = err
        except urllib.error.URLError as err:
            last_error = err

        wait = backoff_seconds * attempt
        log.warning("GET %s failed (attempt %d/%d): %s; retrying in %ss",
                    shown, attempt, retries, last_error, wait)
        time.sleep(wait)

    raise RuntimeError(f"GET {shown} failed after {retries} attempts: {last_error}")


def put_landing(source, trade_date, filename, body, content_type):
    """Write one raw file to s3://<bucket>/landing/<source>/date=YYYY-MM-DD/<filename>."""
    bucket = os.environ["LANDING_BUCKET"]
    key = f"landing/{source}/date={trade_date.isoformat()}/{filename}"
    s3().put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    log.info("Wrote s3://%s/%s (%d bytes)", bucket, key, len(body))
    return key


def parse_date(value):
    return dt.date.fromisoformat(value)


def resolve_dates(event, default_date):
    """Decide which trading dates to fetch.

    event = {}                                      -> [default_date()]  (scheduled run)
    event = {"date": "2026-11-06"}                  -> that one date
    event = {"start": "2026-10-01", "end": "2026-10-31"} -> every weekday in range
    """
    event = event or {}
    if "date" in event:
        return [parse_date(event["date"])]
    if "start" in event and "end" in event:
        start, end = parse_date(event["start"]), parse_date(event["end"])
        if end < start:
            raise ValueError("end must be on or after start")
        days = (end - start).days + 1
        return [d for d in (start + dt.timedelta(n) for n in range(days)) if d.weekday() < 5]
    return [default_date()]


def previous_weekday(day):
    """Step back from `day` to the nearest Monday-Friday (inclusive)."""
    while day.weekday() >= 5:
        day -= dt.timedelta(days=1)
    return day
