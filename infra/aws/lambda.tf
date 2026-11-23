# Lambda fetchers + EventBridge Scheduler.
# All functions ship in one zip built from ../../fetchers (stdlib + boto3 only).

locals {
  functions = {
    nse = {
      # timeout 15 min: a self-continuing backfill does ~80 days a run, staying under AWS's ~16-hop recursion limit
      handler     = "nse.handler"
      role_key    = "nse" # key in var.fetchers / aws_iam_role.fetcher
      timeout     = 900
      description = "NSE equities bhavcopy (UDiFF) -> landing/nse/"
      # 19:30 IST, Monday-Friday (NSE publishes in the early evening)
      schedule = "cron(30 19 ? * MON-FRI *)"
      enabled  = true
    }
    massive = {
      handler     = "massive.handler"
      role_key    = "us"
      timeout     = 900 # backfills sleep between calls for the free-plan rate limit
      description = "Massive grouped daily (all US stocks) -> landing/massive/"
      # 11:00 IST Tue-Sat: the free plan releases a US day just after midnight New York time (~10:30 IST).
      # History only: the morning brief reads the few US figures it needs from Yahoo, live.
      # (each run also fetches any of the last 3 weekdays still missing; see massive.py)
      schedule = "cron(0 11 ? * TUE-SAT *)"
      enabled  = true
    }
    nse_fo = {
      handler     = "nse_fo.handler"
      role_key    = "nse"
      timeout     = 120
      description = "NSE F&O stock list (fo_mktlots.csv) -> landing/nse_fo/"
      # Mondays 08:00 IST
      schedule = "cron(0 8 ? * MON *)"
      enabled  = true
    }
    news = {
      handler     = "news.handler"
      role_key    = "news"
      timeout     = 120
      description = "NSE company news RSS (announcements, results, board meetings, corporate actions) -> news volume"
      # Every 30 minutes, 08:00-19:30 IST, Monday-Friday; plus every minute 09:00-15:59 (news_live below)
      schedule = "cron(0/30 8-19 ? * MON-FRI *)"
      enabled  = var.news_enabled
    }
    fred = {
      handler     = "fred.handler"
      role_key    = "global"
      timeout     = 300
      description = "FRED global markets (indices, FX, VIX, oil, rates) -> landing/fred/"
      # 06:00 IST, Tuesday-Saturday = after the previous US day's data is posted
      schedule = "cron(0 6 ? * TUE-SAT *)"
      enabled  = var.fred_enabled
    }
  }
}

data "archive_file" "fetchers" {
  type        = "zip"
  source_dir  = "${path.module}/../../fetchers"
  output_path = "${path.module}/build/fetchers.zip"
  excludes    = ["__pycache__"]

  # Same permissions on Windows and Linux so the zip hash matches across machines.
  output_file_mode = "0644"
}

resource "aws_cloudwatch_log_group" "fetcher" {
  for_each = local.functions

  name              = "/aws/lambda/${var.name_prefix}-fetch-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "fetcher" {
  for_each = local.functions

  function_name = "${var.name_prefix}-fetch-${each.key}"
  description   = each.value.description
  role          = aws_iam_role.fetcher[each.value.role_key].arn

  runtime       = var.lambda_python_runtime
  architectures = ["arm64"] # cheaper; pure Python, so architecture doesn't matter
  handler       = each.value.handler
  timeout       = each.value.timeout
  memory_size   = 256

  filename         = data.archive_file.fetchers.output_path
  source_code_hash = data.archive_file.fetchers.output_base64sha256

  environment {
    variables = {
      LANDING_BUCKET         = module.landing_bucket.id
      MASSIVE_KEY_PARAM      = aws_ssm_parameter.massive_api_key.name
      FRED_KEY_PARAM         = aws_ssm_parameter.fred_api_key.name
      PUSH_TO_DATABRICKS     = var.databricks_push_enabled ? "true" : "false"
      DATABRICKS_URL_PARAM   = local.databricks_url_param
      DATABRICKS_TOKEN_PARAM = local.databricks_token_param
      LANDING_VOLUME_PATH    = var.databricks_volume_path
      NEWS_VOLUME_PATH       = var.databricks_news_volume_path
      BSE_NEWS_ENABLED       = var.bse_news_enabled ? "true" : "false"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.fetcher,
    aws_iam_role_policy_attachment.fetcher_logs,
    aws_iam_role_policy.fetcher,
  ]
}

# ---------------------------------------------------------------------------
# Scheduler: one role that may only invoke these two functions
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    sid     = "SchedulerInThisAccount"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.name_prefix}-scheduler-role"
  description        = "EventBridge Scheduler: invoke the stocks fetchers only."
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

data "aws_iam_policy_document" "scheduler_invoke" {
  statement {
    sid       = "InvokeFetchersOnly"
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [for f in aws_lambda_function.fetcher : f.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name   = "invoke-fetchers"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_invoke.json
}

resource "aws_scheduler_schedule" "fetcher" {
  for_each = local.functions

  name        = "${var.name_prefix}-fetch-${each.key}"
  description = each.value.description
  state       = var.schedules_enabled && each.value.enabled ? "ENABLED" : "DISABLED"

  schedule_expression          = each.value.schedule
  schedule_expression_timezone = "Asia/Kolkata"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.fetcher[each.key].arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}

# Market hours: company news every minute (09:00-15:59 IST, Monday-Friday), so the paper trader can act on a
# filing within about a minute. Same Lambda; it rewrites latest.jsonl each run and snapshots only when needed.
resource "aws_scheduler_schedule" "news_live" {
  name        = "${var.name_prefix}-fetch-news-live"
  description = "NSE company news every minute during market hours"
  state       = var.schedules_enabled && var.news_enabled ? "ENABLED" : "DISABLED"

  schedule_expression          = "cron(* 9-15 ? * MON-FRI *)"
  schedule_expression_timezone = "Asia/Kolkata"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.fetcher["news"].arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    # No retries: the next minute's run replaces a failed one.
    retry_policy {
      maximum_retry_attempts       = 0
      maximum_event_age_in_seconds = 60
    }
  }
}

# One-off backfill (24 Nov 2026, approved by the owner): NSE's older bhavcopy format from 2019 to the day before
# UDiFF began (8 Jul 2024), for multi-year swing and long-term backtests. First run 23 Nov 22:00 stopped near late
# 2020: the chain of self-invocations hit AWS's recursive-loop limit (~16 hops). Resumed from 2 Nov 2020 at 01:45 IST
# on 24 Nov with a 15-minute timeout, stopped again by the same protection at 02:17; restarted at 03:00 with
# loop detection off for this function (below), after
# the day's 19:30 fetch; the Lambda continues itself until the range is done (~2 hours, 2 s between files). To run
# it again, change the time and apply.
resource "aws_scheduler_schedule" "nse_history_backfill" {
  name        = "${var.name_prefix}-fetch-nse-history-backfill"
  description = "One-off: NSE bhavcopy 2020-11-02 to 2024-07-05 (older format, converted to UDiFF columns); resumes the 23 Nov run."
  state       = var.schedules_enabled ? "ENABLED" : "DISABLED"

  schedule_expression          = "at(2026-11-24T03:00:00)"
  schedule_expression_timezone = "Asia/Kolkata"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.fetcher["nse"].arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ start = "2020-11-02", end = "2024-07-05" })

    retry_policy {
      maximum_retry_attempts       = 0
      maximum_event_age_in_seconds = 3600
    }
  }
}

# The NSE fetcher continues long backfills by invoking itself (common.run_dates). AWS stopped that chain as a
# "recursive loop" on 23-24 Nov 2026 (Health event AWS_LAMBDA_RUNAWAY_TERMINATION_NOTIFICATION), so at the owner's
# decision loop detection is off for this one function. The chain is bounded: each hop passes only the remaining
# dates of a fixed range and stops at its end.
resource "aws_lambda_function_recursion_config" "nse" {
  function_name  = aws_lambda_function.fetcher["nse"].function_name
  recursive_loop = "Allow"
}
