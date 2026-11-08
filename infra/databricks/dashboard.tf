# ---------------------------------------------------------------------------
# AI/BI (Lakeview) dashboard on the gold tables.
# Layout grid is 12 columns wide; every row's widgets add up to width 12.
# Edits made in the UI are overwritten on the next apply; change them here.
# ---------------------------------------------------------------------------

locals {
  # Dataset name -> SQL. Derived columns are computed in SQL, not in widgets.
  dashboard_datasets = {
    breadth = <<-SQL
      SELECT market, trade_date, pct_above_ma20, advance_decline_ratio, median_return_pct
      FROM stocks.gold.market_breadth
      WHERE trade_date >= date_sub(current_date(), 365)
    SQL

    us_vs_india = <<-SQL
      SELECT india_date, us_date, us_move, us_return_pct, india_gap_pct, india_return_pct
      FROM stocks.gold.nifty_after_us_moves
    SQL

    us_vs_india_summary = <<-SQL
      SELECT us_move, CAST(sessions AS INT) AS sessions,
             round(avg_us_return_pct, 2)            AS avg_us_return_pct,
             round(avg_india_gap_pct, 2)            AS avg_india_gap_pct,
             round(avg_india_return_pct, 2)         AS avg_india_return_pct,
             round(pct_india_up_days, 0)            AS pct_india_up_days,
             round(pct_gap_same_direction_as_us, 0) AS pct_gap_same_direction_as_us,
             round(corr_us_return_india_gap, 2)     AS corr_us_return_india_gap
      FROM stocks.gold.nifty_after_us_moves_summary
      ORDER BY CASE us_move WHEN 'US down big' THEN 1 WHEN 'US flat' THEN 2 ELSE 3 END
    SQL

    unusual_latest = <<-SQL
      SELECT u.market, u.symbol, u.trade_date,
             round(u.volume_ratio, 1) AS volume_ratio,
             round(u.return_pct, 2)   AS return_pct,
             u.close, u.currency
      FROM stocks.gold.unusual_volume u
      JOIN (SELECT market, max(trade_date) AS last_day
            FROM stocks.silver.trading_days GROUP BY market) d
        ON u.market = d.market AND u.trade_date = d.last_day
      ORDER BY u.volume_ratio DESC
      LIMIT 50
    SQL

    adr_latest = <<-SQL
      SELECT adr, nse_symbol, trade_date, adr_close_usd, nse_close_inr,
             round(implied_inr_per_usd, 2)   AS implied_inr_per_usd,
             round(relative_premium_pct, 2)  AS relative_premium_pct
      FROM stocks.gold.adr_premium
      WHERE trade_date = (SELECT max(trade_date) FROM stocks.gold.adr_premium)
      ORDER BY relative_premium_pct DESC
    SQL
  }

  # Helpers to keep widget definitions short.
  raw_fields = {
    for ds, cols in {
      us_vs_india         = ["us_return_pct", "india_gap_pct", "us_move"]
      us_vs_india_summary = ["us_move", "sessions", "avg_us_return_pct", "avg_india_gap_pct", "avg_india_return_pct", "pct_india_up_days", "pct_gap_same_direction_as_us", "corr_us_return_india_gap"]
      unusual_latest      = ["market", "symbol", "trade_date", "volume_ratio", "return_pct", "close", "currency"]
      adr_latest          = ["adr", "nse_symbol", "trade_date", "adr_close_usd", "nse_close_inr", "implied_inr_per_usd", "relative_premium_pct"]
    } : ds => [for c in cols : { name = c, expression = "`${c}`" }]
  }

  table_columns = {
    for ds, fields in local.raw_fields : ds => [for f in fields : { fieldName = f.name, displayName = replace(f.name, "_", " ") }]
  }

  dashboard = {
    datasets = [
      for name, sql in local.dashboard_datasets : {
        name        = name
        displayName = replace(name, "_", " ")
        queryLines  = [sql]
      }
    ]

    pages = [{
      name          = "overview"
      displayName   = "Overview"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = [
        {
          widget = {
            name                 = "title"
            multilineTextboxSpec = { lines = ["## Stocks lakehouse: India x US", "Daily NSE + US data. Proxies: NIFTYBEES (Nifty 50) and SPY (S&P 500). Historical tendencies, not advice."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        },
        {
          widget = {
            name = "breadth_chart"
            queries = [{
              name = "main_query"
              query = {
                datasetName   = "breadth"
                disaggregated = false
                fields = [
                  { name = "trade_date", expression = "`trade_date`" },
                  { name = "market", expression = "`market`" },
                  { name = "avg(pct_above_ma20)", expression = "AVG(`pct_above_ma20`)" },
                ]
              }
            }]
            spec = {
              version    = 3
              widgetType = "line"
              encodings = {
                x     = { fieldName = "trade_date", scale = { type = "temporal" }, displayName = "Date" }
                y     = { fieldName = "avg(pct_above_ma20)", scale = { type = "quantitative" }, displayName = "% of stocks above 20-day average" }
                color = { fieldName = "market", scale = { type = "categorical" }, displayName = "Market" }
              }
              frame = { showTitle = true, title = "Market breadth: % of stocks above their 20-day average" }
            }
          }
          position = { x = 0, y = 2, width = 6, height = 6 }
        },
        {
          widget = {
            name = "us_vs_india_scatter"
            queries = [{
              name  = "main_query"
              query = { datasetName = "us_vs_india", disaggregated = true, fields = local.raw_fields.us_vs_india }
            }]
            spec = {
              version    = 3
              widgetType = "scatter"
              encodings = {
                x     = { fieldName = "us_return_pct", scale = { type = "quantitative" }, displayName = "US session return % (SPY)" }
                y     = { fieldName = "india_gap_pct", scale = { type = "quantitative" }, displayName = "Next India open gap % (NIFTYBEES)" }
                color = { fieldName = "us_move", scale = { type = "categorical" }, displayName = "US move" }
              }
              frame = { showTitle = true, title = "Does India's open follow the US close?" }
            }
          }
          position = { x = 6, y = 2, width = 6, height = 6 }
        },
        {
          widget = {
            name = "us_vs_india_summary_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "us_vs_india_summary", disaggregated = true, fields = local.raw_fields.us_vs_india_summary }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.us_vs_india_summary }
              frame      = { showTitle = true, title = "How India trades after US down / flat / up days" }
            }
          }
          position = { x = 0, y = 8, width = 12, height = 4 }
        },
        {
          widget = {
            name = "unusual_volume_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "unusual_latest", disaggregated = true, fields = local.raw_fields.unusual_latest }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.unusual_latest }
              frame      = { showTitle = true, title = "Unusual volume on the latest session (>= 3x 20-day average)" }
            }
          }
          position = { x = 0, y = 12, width = 12, height = 7 }
        },
        {
          widget = {
            name = "adr_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "adr_latest", disaggregated = true, fields = local.raw_fields.adr_latest }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.adr_latest }
              frame      = { showTitle = true, title = "Indian ADRs vs NSE shares (relative premium vs the day's median pair)" }
            }
          }
          position = { x = 0, y = 19, width = 12, height = 5 }
        },
      ]
    }]
  }
}

resource "databricks_dashboard" "stocks" {
  display_name         = "Stocks lakehouse: India x US"
  parent_path          = "${data.databricks_current_user.me.home}/stocks_lakehouse"
  warehouse_id         = data.databricks_sql_warehouse.main.id
  serialized_dashboard = jsonencode(local.dashboard)
  embed_credentials    = true

  depends_on = [databricks_pipeline.medallion]
}

output "dashboard_url" {
  value = "${trimsuffix(var.databricks_workspace_url, "/")}/dashboardsv3/${databricks_dashboard.stocks.id}/published"
}
