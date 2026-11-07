"""Upload files to the Databricks Unity Catalog landing volume via the Files API.

Config (set by Terraform as Lambda environment variables):
  PUSH_TO_DATABRICKS      "true" to push after every S3 write
  DATABRICKS_URL_PARAM    SSM parameter holding the workspace URL
  DATABRICKS_TOKEN_PARAM  SSM SecureString holding the stocks-lambda-pusher token
  LANDING_VOLUME_PATH     e.g. /Volumes/stocks/bronze/landing

The token is a dedicated, expiring token stored only in SSM (see infra/databricks/pusher.tf).
"""

import io
import os
import time
import urllib.parse
import zipfile

import boto3

from common import NotFound, http_request, log

_config = None
_config_loaded_at = 0.0
CONFIG_TTL_SECONDS = 300  # pick up a rotated token within 5 minutes


def enabled():
    return os.environ.get("PUSH_TO_DATABRICKS", "false").lower() == "true"


def config():
    global _config, _config_loaded_at
    if _config is None or time.monotonic() - _config_loaded_at > CONFIG_TTL_SECONDS:
        ssm = boto3.client("ssm")

        def param(name, decrypt):
            return ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]

        _config = {
            "host": param(os.environ["DATABRICKS_URL_PARAM"], False).rstrip("/"),
            "token": param(os.environ["DATABRICKS_TOKEN_PARAM"], True),
            "volume": os.environ["LANDING_VOLUME_PATH"].rstrip("/"),
        }
        _config_loaded_at = time.monotonic()
    return _config


def _url(api, path):
    cfg = config()
    return f"{cfg['host']}/api/2.0/fs/{api}{urllib.parse.quote(path, safe='/=')}"


def _auth():
    return {"Authorization": f"Bearer {config()['token']}"}


def volume_path(relative):
    return f"{config()['volume']}/{relative.lstrip('/')}"


def volume_relative(relative):
    """Name the file gets in the volume: NSE .csv.zip archives are stored unzipped as .csv,
    because Spark/Auto Loader can read gzip but not zip. S3 keeps the original zip."""
    return relative[: -len(".zip")] if relative.endswith(".csv.zip") else relative


def prepare(relative, body):
    """Return (volume relative path, bytes to upload), unzipping .csv.zip archives."""
    if not relative.endswith(".csv.zip"):
        return relative, body
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        csvs = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise RuntimeError(f"Expected exactly one CSV in {relative}, found {archive.namelist()}")
        return volume_relative(relative), archive.read(csvs[0])


def exists(relative):
    """True if the file (under its volume name) is already in the volume."""
    try:
        http_request("HEAD", _url("files", volume_path(volume_relative(relative))), headers=_auth())
        return True
    except NotFound:
        return False


def upload(relative, body):
    """Upload to <volume>/<relative> (unzipping .csv.zip), creating parent dirs, overwriting."""
    relative, body = prepare(relative, body)
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
