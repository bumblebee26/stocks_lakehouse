"""Run SQL on Databricks through the SQL Statement Execution API.

Config (Lambda environment variables -> SSM parameter names):
  DATABRICKS_URL_PARAM        workspace URL
  DATABRICKS_TOKEN_PARAM      token (SecureString)
  DATABRICKS_WAREHOUSE_PARAM  SQL warehouse ID (written by the databricks stack)
"""

import json
import os
import time

import boto3

from common import http_request, log

NUMERIC_TYPES = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DECIMAL"}
_config = None


def config():
    global _config
    if _config is None:
        ssm = boto3.client("ssm")

        def param(name, decrypt=False):
            return ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]

        _config = {
            "host": param(os.environ["DATABRICKS_URL_PARAM"]).rstrip("/"),
            "token": param(os.environ["DATABRICKS_TOKEN_PARAM"], decrypt=True),
            "warehouse_id": param(os.environ["DATABRICKS_WAREHOUSE_PARAM"]),
        }
    return _config


def _call(method, path, body=None):
    cfg = config()
    headers = {"Authorization": f"Bearer {cfg['token']}", "Content-Type": "application/json"}
    data = json.dumps(body).encode() if body is not None else None
    return json.loads(http_request(method, f"{cfg['host']}{path}", headers=headers, data=data))


def _to_rows(response):
    columns = response["manifest"]["schema"]["columns"]
    rows = []
    for raw in (response.get("result") or {}).get("data_array") or []:
        row = {}
        for col, value in zip(columns, raw):
            if value is not None and col.get("type_name") in NUMERIC_TYPES:
                value = float(value)
            row[col["name"]] = value
        rows.append(row)
    return rows


def query(sql, *, max_wait_seconds=240, poll_seconds=5):
    """Run one statement and return a list of dicts (numbers as float, dates as 'YYYY-MM-DD').

    A cold serverless warehouse can take a little while to start, so pending
    statements are polled until they finish or max_wait_seconds passes.
    """
    response = _call("POST", "/api/2.0/sql/statements", {
        "warehouse_id": config()["warehouse_id"],
        "statement": sql,
        "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE",
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
    })
    deadline = time.monotonic() + max_wait_seconds
    while response["status"]["state"] in ("PENDING", "RUNNING"):
        if time.monotonic() > deadline:
            raise TimeoutError(f"Databricks statement {response['statement_id']} still running")
        time.sleep(poll_seconds)
        response = _call("GET", f"/api/2.0/sql/statements/{response['statement_id']}")

    state = response["status"]["state"]
    if state != "SUCCEEDED":
        error = response["status"].get("error", {})
        raise RuntimeError(f"Databricks statement {state}: {error.get('message', error)}")
    rows = _to_rows(response)
    log.info("Query returned %d rows", len(rows))
    return rows
