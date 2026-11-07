"""Copy files from S3 landing/ into the Databricks landing volume.

Use it once for files fetched before the Databricks push existed (the backfill),
and any time later to repair gaps. Files already in the volume are skipped unless
{"overwrite": true}. Long runs continue themselves in a new invocation.

Event (all optional):
  {"prefix": "landing/nse/", "overwrite": false, "start_after": "<s3 key>"}
"""

import json
import os

from common import lambda_client, log, s3

import databricks_volume

RESERVE_SECONDS = 60


def handler(event, context):
    event = event or {}
    bucket = os.environ["LANDING_BUCKET"]
    prefix = event.get("prefix", "landing/")
    if not prefix.startswith("landing/"):
        raise ValueError("prefix must start with landing/")
    overwrite = bool(event.get("overwrite", False))

    list_args = {"Bucket": bucket, "Prefix": prefix}
    if event.get("start_after"):
        list_args["StartAfter"] = event["start_after"]

    copied = skipped = 0
    last_done = event.get("start_after")
    continued_after = None

    for page in s3().get_paginator("list_objects_v2").paginate(**list_args):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue

            if (context is not None and (copied + skipped) > 0
                    and context.get_remaining_time_in_millis() / 1000 < RESERVE_SECONDS):
                payload = {"prefix": prefix, "overwrite": overwrite, "start_after": last_done}
                lambda_client().invoke(FunctionName=context.function_name,
                                       InvocationType="Event",
                                       Payload=json.dumps(payload).encode())
                continued_after = last_done
                break

            relative = key[len("landing/"):]
            if not overwrite and databricks_volume.exists(relative):
                skipped += 1
            else:
                body = s3().get_object(Bucket=bucket, Key=key)["Body"].read()
                databricks_volume.upload(relative, body)
                copied += 1
            last_done = key

        if continued_after:
            break

    summary = {"prefix": prefix, "copied": copied, "skipped_existing": skipped,
               "continued_after": continued_after}
    log.info("Sync summary: %s", summary)
    return summary
