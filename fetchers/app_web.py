"""Serves the desk mobile app from its private bucket through a Lambda function URL.

Used while CloudFront is unavailable on the account (infra/aws/app.tf, var.app_use_cloudfront).
Only /<secret path>/<known file> is served; anything else is 404, so the bare function URL shows nothing.
"""

import base64
import hmac
import os

import boto3

FILES = {
    "index.html": "text/html; charset=utf-8",
    "manifest.webmanifest": "application/manifest+json",
    "icon.svg": "image/svg+xml",
    "data/desk.json": "application/json",
}

SECURITY_HEADERS = {
    "strict-transport-security": "max-age=31536000; includeSubDomains",
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "no-referrer",
}

_s3 = boto3.client("s3")


def _response(status, body=b"", content_type="text/plain; charset=utf-8", extra=None):
    headers = {"content-type": content_type, **SECURITY_HEADERS, **(extra or {})}
    return {
        "statusCode": status,
        "headers": headers,
        "body": base64.b64encode(body).decode(),
        "isBase64Encoded": True,
    }


def route(raw_path, secret):
    """Returns (status, file name or redirect target or None)."""
    parts = raw_path.lstrip("/").split("/", 1)
    if not secret or not hmac.compare_digest(parts[0], secret):
        return 404, None
    if len(parts) == 1:
        return 302, f"/{secret}/"
    name = parts[1] or "index.html"
    if name not in FILES:
        return 404, None
    return 200, name


def handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    if method not in ("GET", "HEAD"):
        return _response(405, b"Method not allowed")

    status, target = route(event.get("rawPath", "/"), os.environ.get("APP_PATH", ""))
    if status == 404:
        return _response(404, b"Not found")
    if status == 302:
        return _response(302, extra={"location": target})

    try:
        obj = _s3.get_object(Bucket=os.environ["APP_BUCKET"], Key=f"{os.environ['APP_PREFIX']}/{target}")
    except _s3.exceptions.NoSuchKey:
        return _response(404, b"Not found")
    body = obj["Body"].read() if method == "GET" else b""
    return _response(
        200,
        body,
        FILES[target],
        {"cache-control": obj.get("CacheControl") or "max-age=60"},
    )
