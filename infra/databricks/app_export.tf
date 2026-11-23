# ---------------------------------------------------------------------------
# Mobile app (infra/aws/app.tf): the dashboard's own SQL for the phone app, so both always show the same numbers.
# Rendered from local.dashboard_datasets and local.raw_fields into s3://<app bucket>/config/queries.json; the
# app-export Lambda runs each query after every Databricks job and writes the app's data file. The app bucket is
# made by the aws stack, which applies first.
# ---------------------------------------------------------------------------

data "aws_caller_identity" "app" {}

locals {
  app_datasets = [
    "desk_status", "desk_kpis", "desk_pools", "desk_decisions", "desk_board", "portfolio_vs_nifty",
    "intraday_kpis", "intra_board", "intra_bar_ladder", "intra_selector", "intra_lines", "swing_returns",
    "longterm_returns",
    "swing_kpis", "swing_scoreboard", "swing_trades", "longterm_kpis", "longterm_scoreboard", "longterm_trades",
    "calls_kpis", "markets_kpis", "market_read", "world_moves",
  ]
}

resource "aws_s3_object" "app_queries" {
  bucket       = "${var.name_prefix}-desk-app-${data.aws_caller_identity.app.account_id}"
  key          = "config/queries.json"
  content_type = "application/json"
  content = jsonencode({
    for name in local.app_datasets : name => {
      sql    = local.dashboard_datasets[name]
      fields = [for f in local.raw_fields[name] : f.name]
    }
  })
}
