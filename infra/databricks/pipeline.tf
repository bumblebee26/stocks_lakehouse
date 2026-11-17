# ---------------------------------------------------------------------------
# Three Lakeflow pipelines and the jobs that run them (split on 17 Nov from one 61-table "medallion" pipeline).
# Source code lives in ../../pipeline/<pipeline>/*.py and is uploaded to your workspace home folder, so
# `terraform apply` after a code change redeploys it.
#
#   market    market data in, cleaned, and the light market summaries (33 tables)
#   research  the heavy daily research: technical scanner, momentum ranks, signal record (5 tables, pandas)
#   trading   the paper-trader's journal and portfolio exports and everything built on them (23 tables)
#
# Free Edition allows ONE ACTIVE PIPELINE at a time (starting a second while one runs fails with a quota error), so:
# each job runs its pipelines one after another, the jobs' start times never overlap, and every pipeline task
# retries after 15 minutes if it ever collides with another job's pipeline.
#
#   job stocks-market-close    when the NSE evening file lands (landing/nse/, ~19:30 IST Mon-Fri):
#                              market -> research -> dashboard summary. Picks up everything else that landed during
#                              the day too (US prices, FRED, the F&O list).
#   job stocks-market-weekend  Saturday 08:00 and 12:00 IST: market -> research -> dashboard summary (the 07:15
#                              fundamentals and Nifty 200 list, so the momentum ranks are ready for the 10:15
#                              long-term shadow job; Friday's US and FRED files).
#   job stocks-trading         when the paper trader's export lands (landing/trades/, 15:45 and 23:30 IST):
#                              trading -> dashboard summary.
# ---------------------------------------------------------------------------

data "databricks_current_user" "me" {}

locals {
  pipelines = {
    market   = ["bronze", "silver", "gold"]
    research = ["gold"]
    trading  = ["bronze", "silver", "gold"]
  }
  pipeline_file_list = flatten([for p, files in local.pipelines : [for f in files : "${p}/${f}"]])
  pipeline_workspace = "${data.databricks_current_user.me.home}/stocks_lakehouse/pipeline"
  landing_path       = "/Volumes/${databricks_volume.landing.catalog_name}/${databricks_volume.landing.schema_name}/${databricks_volume.landing.name}"
  pipeline_configuration = {
    "stocks.landing_path"       = local.landing_path
    "stocks.news_path"          = "/Volumes/${databricks_volume.news.catalog_name}/${databricks_volume.news.schema_name}/${databricks_volume.news.name}"
    "stocks.india_proxy"        = var.india_proxy_symbol
    "stocks.us_proxy"           = var.us_proxy_symbol
    "stocks.big_move_pct"       = tostring(var.big_move_pct)
    "stocks.volume_spike_ratio" = tostring(var.volume_spike_ratio)
    "stocks.min_avg_volume"     = tostring(var.min_avg_volume)
    "stocks.ledger_start"       = var.ledger_start
  }
  retry_minutes = 15
  retries       = 4
}

resource "databricks_workspace_file" "pipeline" {
  for_each = toset(local.pipeline_file_list)

  source = "${path.module}/../../pipeline/${each.value}.py"
  path   = "${local.pipeline_workspace}/${each.value}.py"
}

resource "databricks_pipeline" "stocks" {
  for_each = local.pipelines

  name        = "${var.name_prefix}-${each.key}"
  catalog     = databricks_catalog.stocks.name
  schema      = databricks_schema.layer["bronze"].name # default schema; tables use full names
  serverless  = true
  continuous  = false # run by the jobs below
  development = var.pipeline_development_mode
  channel     = "CURRENT"

  configuration = local.pipeline_configuration

  dynamic "library" {
    for_each = each.value

    content {
      file {
        path = databricks_workspace_file.pipeline["${each.key}/${library.value}"].path
      }
    }
  }

  depends_on = [databricks_schema.layer, databricks_volume.landing, databricks_volume.news]
}

# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------
resource "databricks_job" "market_close" {
  name                = "${var.name_prefix}-market-close"
  description         = "After the NSE evening file: market pipeline, then research, then the dashboard summaries. Managed by Terraform."
  max_concurrent_runs = 1

  task {
    task_key                  = "market"
    max_retries               = local.retries
    min_retry_interval_millis = local.retry_minutes * 60000

    pipeline_task {
      pipeline_id = databricks_pipeline.stocks["market"].id
    }
  }

  task {
    task_key                  = "research"
    max_retries               = local.retries
    min_retry_interval_millis = local.retry_minutes * 60000
    depends_on {
      task_key = "market"
    }

    pipeline_task {
      pipeline_id = databricks_pipeline.stocks["research"].id
    }
  }

  task {
    task_key = "dashboard_summary"
    depends_on {
      task_key = "research"
    }

    sql_task {
      warehouse_id = data.databricks_sql_warehouse.main.id
      file {
        path   = databricks_workspace_file.dashboard_summary.workspace_path
        source = "WORKSPACE"
      }
    }
  }

  dynamic "trigger" {
    for_each = var.job_trigger == "file_arrival" ? [1] : []

    content {
      pause_status = "UNPAUSED"

      file_arrival {
        url                               = "${local.landing_path}/nse/"
        min_time_between_triggers_seconds = 900
        wait_after_last_change_seconds    = 300 # the delivery file lands a few seconds after the bhavcopy
      }
    }
  }

  # Fallback if file-arrival triggers aren't available: 20:15 IST Mon-Fri
  dynamic "schedule" {
    for_each = var.job_trigger == "schedule" ? [1] : []

    content {
      quartz_cron_expression = "0 15 20 ? * MON-FRI"
      timezone_id            = "Asia/Kolkata"
      pause_status           = "UNPAUSED"
    }
  }

  email_notifications {
    on_failure = [data.databricks_current_user.me.user_name]
  }
}

resource "databricks_job" "market_weekend" {
  name                = "${var.name_prefix}-market-weekend"
  description         = "Saturday 08:00 and 12:00 IST: market pipeline (fundamentals, Nifty 200 list, US, FRED), research (momentum ranks for the 10:15 long-term job), then the dashboard summaries. Managed by Terraform."
  max_concurrent_runs = 1

  task {
    task_key                  = "market"
    max_retries               = local.retries
    min_retry_interval_millis = local.retry_minutes * 60000

    pipeline_task {
      pipeline_id = databricks_pipeline.stocks["market"].id
    }
  }

  task {
    task_key                  = "research"
    max_retries               = local.retries
    min_retry_interval_millis = local.retry_minutes * 60000
    depends_on {
      task_key = "market"
    }

    pipeline_task {
      pipeline_id = databricks_pipeline.stocks["research"].id
    }
  }

  task {
    task_key = "dashboard_summary"
    depends_on {
      task_key = "research"
    }

    sql_task {
      warehouse_id = data.databricks_sql_warehouse.main.id
      file {
        path   = databricks_workspace_file.dashboard_summary.workspace_path
        source = "WORKSPACE"
      }
    }
  }

  schedule {
    quartz_cron_expression = "0 0 8,12 ? * SAT"
    timezone_id            = "Asia/Kolkata"
    pause_status           = "UNPAUSED"
  }

  email_notifications {
    on_failure = [data.databricks_current_user.me.user_name]
  }
}

resource "databricks_job" "trading" {
  name                = "${var.name_prefix}-trading"
  description         = "When the paper trader's export lands: trading pipeline, then the dashboard summaries. Managed by Terraform."
  max_concurrent_runs = 1

  task {
    task_key                  = "trading"
    max_retries               = local.retries
    min_retry_interval_millis = local.retry_minutes * 60000

    pipeline_task {
      pipeline_id = databricks_pipeline.stocks["trading"].id
    }
  }

  task {
    task_key = "dashboard_summary"
    depends_on {
      task_key = "trading"
    }

    sql_task {
      warehouse_id = data.databricks_sql_warehouse.main.id
      file {
        path   = databricks_workspace_file.dashboard_summary.workspace_path
        source = "WORKSPACE"
      }
    }
  }

  dynamic "trigger" {
    for_each = var.job_trigger == "file_arrival" ? [1] : []

    content {
      pause_status = "UNPAUSED"

      file_arrival {
        url                               = "${local.landing_path}/trades/"
        min_time_between_triggers_seconds = 900
        wait_after_last_change_seconds    = 300 # the portfolio snapshot lands with the journal
      }
    }
  }

  # Fallback if file-arrival triggers aren't available: 15:55 and 23:55 IST Mon-Fri
  dynamic "schedule" {
    for_each = var.job_trigger == "schedule" ? [1] : []

    content {
      quartz_cron_expression = "0 55 15,23 ? * MON-FRI"
      timezone_id            = "Asia/Kolkata"
      pause_status           = "UNPAUSED"
    }
  }

  email_notifications {
    on_failure = [data.databricks_current_user.me.user_name]
  }
}

resource "databricks_workspace_file" "dashboard_summary" {
  source = "${path.module}/../../sql/dashboard_summary.sql"
  path   = "${data.databricks_current_user.me.home}/stocks_lakehouse/dashboard_summary.sql"
}
