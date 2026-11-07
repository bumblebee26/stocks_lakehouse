"""Upload files to the Databricks Unity Catalog landing volume via the Files API.

Config (set by Terraform as Lambda environment variables):
  PUSH_TO_DATABRICKS      "true" to push after every S3 write
  DATABRICKS_URL_PARAM    SSM parameter holding the workspace URL
  DATABRICKS_TOKEN_PARAM  SSM SecureString holding the stocks-lambda-pusher token
  LANDING_VOLUME_PATH     e.g. /Volumes/stocks/bronze/landing

The token belongs to a service principal that may only read/write this one volume.
"""

import os
import urllib.parse

import boto3

from common import NotFound, http_request, log

_config = None


def enabled():
    return os.environ.get("PUSH_TO_DATABRICKS", "false").lower() == "true"


def config():
    global _config
    if _config is None:
        ssm = boto3.client("ssm")

        def param(name, decrypt):
            return ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]

        _config = {
            "host": param(os.environ["DATABRICKS_URL_PARAM"], False).rstrip("/"),
            "token": param(os.environ["DATABRICKS_TOKEN_PARAM"], True),
            "volume": os.environ["LANDING_VOLUME_PATH"].rstrip("/"),
        }
    return _config


def _url(api, path):
    cfg = config()
    return f"{cfg['host']}/api/2.0/fs/{api}{urllib.parse.quote(path, safe='/=')}"


def _auth():
    return {"Authorization": f"Bearer {config()['token']}"}


def volume_path(relative):
    return f"{config()['volume']}/{relative.lstrip('/')}"


def exists(relative):
    """True if the file is already in the volume."""
    try:
        http_request("HEAD", _url("files", volume_path(relative)), headers=_auth())
        return True
    except NotFound:
        return False


def upload(relative, body):
    """Upload bytes to <volume>/<relative>, creating parent directories, overwriting if present."""
    path = volume_path(relative)
    parent = path.rsplit("/", 1)[0]

    # Idempotent: succeeds if the directory already exists.
    http_request("PUT", _url("directories", parent), headers=_auth())
    http_request(
        "PUT",
        _url("files", path) + "?overwrite=true",
        headers={**_auth(), "Content-Type": "application/octet-stream"},
        data=body,
        timeout=120,
    )
    log.info("Pushed %s (%d bytes) to Databricks", path, len(body))
    return path
