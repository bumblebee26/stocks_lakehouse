# ---------------------------------------------------------------------------
# Lakeflow pipeline (bronze -> silver -> gold) and the job that runs it.
# Source code lives in ../../pipeline/*.py and is uploaded to your workspace
# home folder, so `terraform apply` after a code change redeploys it.
# ---------------------------------------------------------------------------

data "databricks_current_user" "me" {}

locals {
  pipeline_files     = ["bronze", "silver", "gold"]
  pipeline_workspace = "${data.databricks_current_user.me.home}/stocks_lakehouse/pipeline"
  landing_path       = "/Volumes/${databricks_volume.landing.catalog_name}/${databricks_volume.landing.schema_name}/${databricks_volume.landing.name}"
}

resource "databricks_workspace_file" "pipeline" {
  for_each = toset(local.pipeline_files)

  source = "${path.module}/../../pipeline/${each.value}.py"
  path   = "${local.pipeline_workspace}/${each.value}.py"
}

resource "databricks_pipeline" "medallion" {
  name        = "${var.name_prefix}-medallion"
  catalog     = databricks_catalog.stocks.name
  schema      = databricks_schema.layer["bronze"].name # default schema; tables use full names
  serverless  = true
  continuous  = false # triggered by the job below
  development = var.pipeline_development_mode
  channel     = "CURRENT"

  configuration = {
    "stocks.landing_path"       = local.landing_path
    "stocks.news_path"          = "/Volumes/${databricks_volume.news.catalog_name}/${databricks_volume.news.schema_name}/${databricks_volume.news.name}"
    "stocks.india_proxy"        = var.india_proxy_symbol
    "stocks.us_proxy"           = var.us_proxy_symbol
    "stocks.big_move_pct"       = tostring(var.big_move_pct)
    "stocks.volume_spike_ratio" = tostring(var.volume_spike_ratio)
    "stocks.min_avg_volume"     = tostring(var.min_avg_volume)
  }

  dynamic "library" {
    for_each = local.pipeline_files

    content {
      file {
        path = databricks_workspace_file.pipeline[library.value].path
      }
    }
  }

  depends_on = [databricks_schema.layer, databricks_volume.landing, databricks_volume.news]
}

# ---------------------------------------------------------------------------
# Job: refresh the pipeline when new files land (or on a schedule)
# ---------------------------------------------------------------------------
resource "databricks_job" "refresh" {
  name                = "${var.name_prefix}-refresh"
  description         = "Refresh the stocks medallion pipeline. Managed by Terraform."
  max_concurrent_runs = 1

  task {
    task_key = "refresh_medallion"

    pipeline_task {
      pipeline_id = databricks_pipeline.medallion.id
    }
  }

  # Plain-words page summaries for the dashboard (facts + ai_summarize), after every refresh.
  task {
    task_key = "dashboard_summary"
    depends_on {
      task_key = "refresh_medallion"
    }

    sql_task {
      warehouse_id = data.databricks_sql_warehouse.main.id
      file {
        path   = databricks_workspace_file.dashboard_summary.workspace_path
        source = "WORKSPACE"
      }
    }
  }

  # New NSE file ~19:30 IST and new US file ~04:30 IST -> two refreshes a day.
  # The wait lets a burst of files (e.g. a backfill) settle into one run.
  dynamic "trigger" {
    for_each = var.job_trigger == "file_arrival" ? [1] : []

    content {
      pause_status = "UNPAUSED"

      file_arrival {
        url                               = "${local.landing_path}/"
        min_time_between_triggers_seconds = 900
        wait_after_last_change_seconds    = 300
      }
    }
  }

  # Fallback if file-arrival triggers aren't available: 06:00 IST Tue-Sat
  # (after both the NSE evening file and the US early-morning file).
  dynamic "schedule" {
    for_each = var.job_trigger == "schedule" ? [1] : []

    content {
      quartz_cron_expression = "0 0 6 ? * TUE-SAT"
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
