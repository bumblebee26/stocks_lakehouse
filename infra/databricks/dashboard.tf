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
             round(relative_premium_pct, 2)  AS relative_premium_pct,
             round(fx_inr_per_usd, 2)        AS fx_inr_per_usd,
             round(fx_premium_pct, 2)        AS fx_premium_pct
      FROM stocks.gold.adr_premium
      WHERE trade_date = (SELECT max(trade_date) FROM stocks.gold.adr_premium)
      ORDER BY relative_premium_pct DESC
    SQL

    global_latest = <<-SQL
      SELECT region, name, instrument, asset_class, obs_date AS as_of,
             round(value, 2)          AS value,
             round(change_pct, 2)     AS change_pct,
             round(change_5d_pct, 2)  AS change_5d_pct,
             round(change_20d_pct, 2) AS change_20d_pct,
             source
      FROM stocks.gold.global_snapshot
      ORDER BY CASE asset_class WHEN 'equity_index' THEN 1 WHEN 'equity_etf' THEN 2 WHEN 'volatility' THEN 3
                                WHEN 'commodity' THEN 4 WHEN 'rates' THEN 5 ELSE 6 END, region
    SQL

    india_vs_global = <<-SQL
      SELECT name, instrument, region, asset_class, CAST(sessions AS INT) AS sessions,
             round(corr_with_india_gap, 2)    AS corr_with_india_gap,
             round(corr_with_india_return, 2) AS corr_with_india_return,
             round(pct_same_direction_gap, 0) AS pct_same_direction_gap
      FROM stocks.gold.india_vs_global
      ORDER BY abs(corr_with_india_gap) DESC
    SQL

    # --- Paper trading (dummy money, bumblebee26/paper-trader) ---
    trading_daily = <<-SQL
      SELECT trade_date,
             CAST(proposals AS INT) AS proposals, CAST(trades_opened AS INT) AS trades,
             CAST(rejected AS INT) AS said_no, CAST(expired AS INT) AS no_reply,
             round(win_rate_pct, 0)             AS win_rate_pct,
             round(net_pnl_rs, 0)               AS net_pnl_rs,
             round(passed_hypothetical_pnl_rs, 0) AS skipped_would_have_made_rs,
             round(end_balance_rs, 0)           AS end_balance_rs,
             reconciliation
      FROM stocks.gold.trading_daily
      ORDER BY trade_date DESC
    SQL

    trading_equity = <<-SQL
      SELECT trade_date, end_balance_rs
      FROM stocks.gold.trading_daily
      WHERE end_balance_rs IS NOT NULL
    SQL

    trading_by_setup = <<-SQL
      SELECT setup, side, CAST(trades AS INT) AS trades,
             round(win_rate_pct, 0)          AS win_rate_pct,
             round(net_pnl_rs, 0)            AS net_pnl_rs,
             round(avg_pnl_per_trade_rs, 0)  AS avg_pnl_per_trade_rs,
             round(avg_r_multiple, 2)        AS avg_r_multiple
      FROM stocks.gold.trading_by_setup
      ORDER BY net_pnl_rs DESC
    SQL

    trading_by_hour = <<-SQL
      SELECT entry_hour, CAST(trades AS INT) AS trades, round(win_rate_pct, 0) AS win_rate_pct,
             round(net_pnl_rs, 0) AS net_pnl_rs, round(avg_r_multiple, 2) AS avg_r_multiple
      FROM stocks.gold.trading_by_hour
      ORDER BY entry_hour
    SQL

    trading_decisions = <<-SQL
      SELECT CAST(sum(taken) AS INT)                     AS trades_taken,
             round(sum(taken_net_pnl_rs), 0)             AS taken_net_pnl_rs,
             CAST(sum(passed) AS INT)                    AS proposals_skipped,
             CAST(sum(passed_would_have_won) AS INT)     AS skipped_that_would_have_won,
             round(sum(passed_would_have_made_rs), 0)    AS skipped_would_have_made_rs,
             round(sum(saved_by_skipping_rs), 0)         AS saved_by_skipping_rs
      FROM stocks.gold.trading_decisions
    SQL

    trading_recent = <<-SQL
      SELECT trade_date, symbol, side, setup, CAST(qty AS INT) AS qty,
             entry_price, exit_price, exit_reason,
             round(net_pnl_rs, 0) AS net_pnl_rs, round(r_multiple, 2) AS r_multiple,
             round(holding_minutes, 0) AS holding_minutes
      FROM stocks.gold.trades
      ORDER BY entry_time DESC
      LIMIT 50
    SQL
  }

  # Helpers to keep widget definitions short.
  raw_fields = {
    for ds, cols in {
      us_vs_india         = ["us_return_pct", "india_gap_pct", "us_move"]
      us_vs_india_summary = ["us_move", "sessions", "avg_us_return_pct", "avg_india_gap_pct", "avg_india_return_pct", "pct_india_up_days", "pct_gap_same_direction_as_us", "corr_us_return_india_gap"]
      unusual_latest      = ["market", "symbol", "trade_date", "volume_ratio", "return_pct", "close", "currency"]
      adr_latest          = ["adr", "nse_symbol", "trade_date", "adr_close_usd", "nse_close_inr", "implied_inr_per_usd", "relative_premium_pct", "fx_inr_per_usd", "fx_premium_pct"]
      global_latest       = ["region", "name", "instrument", "asset_class", "as_of", "value", "change_pct", "change_5d_pct", "change_20d_pct", "source"]
      india_vs_global     = ["name", "instrument", "region", "asset_class", "sessions", "corr_with_india_gap", "corr_with_india_return", "pct_same_direction_gap"]
      trading_daily       = ["trade_date", "proposals", "trades", "said_no", "no_reply", "win_rate_pct", "net_pnl_rs", "skipped_would_have_made_rs", "end_balance_rs", "reconciliation"]
      trading_equity      = ["trade_date", "end_balance_rs"]
      trading_by_setup    = ["setup", "side", "trades", "win_rate_pct", "net_pnl_rs", "avg_pnl_per_trade_rs", "avg_r_multiple"]
      trading_by_hour     = ["entry_hour", "trades", "win_rate_pct", "net_pnl_rs", "avg_r_multiple"]
      trading_decisions   = ["trades_taken", "taken_net_pnl_rs", "proposals_skipped", "skipped_that_would_have_won", "skipped_would_have_made_rs", "saved_by_skipping_rs"]
      trading_recent      = ["trade_date", "symbol", "side", "setup", "qty", "entry_price", "exit_price", "exit_reason", "net_pnl_rs", "r_multiple", "holding_minutes"]
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
            multilineTextboxSpec = { lines = ["## Stocks lakehouse: India x US", "Daily NSE, US (Massive) and global (FRED) data. Proxies: NIFTYBEES (Nifty 50) and SPY (S&P 500). Historical tendencies, not advice."] }
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
              frame      = { showTitle = true, title = "Indian ADRs vs NSE shares (premium vs median pair, and vs USD/INR)" }
            }
          }
          position = { x = 0, y = 19, width = 12, height = 5 }
        },
        {
          widget = {
            name = "global_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "global_latest", disaggregated = true, fields = local.raw_fields.global_latest }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.global_latest }
              frame      = { showTitle = true, title = "Global markets: latest reading (FRED + US-listed country ETFs)" }
            }
          }
          position = { x = 0, y = 24, width = 12, height = 8 }
        },
        {
          widget = {
            name = "india_vs_global_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "india_vs_global", disaggregated = true, fields = local.raw_fields.india_vs_global }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.india_vs_global }
              frame      = { showTitle = true, title = "Which global moves line up with India's next open? (correlation with NIFTYBEES gap)" }
            }
          }
          position = { x = 0, y = 32, width = 12, height = 7 }
        },
      ]
      }, {
      name          = "paper_trading"
      displayName   = "Paper trading"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = [
        {
          widget = {
            name                 = "trading_title"
            multilineTextboxSpec = { lines = ["## Paper trading (dummy money)", "Mock NSE intraday trades from bumblebee26/paper-trader, approved on Telegram. Exported daily at 15:45 IST. Reconciliation compares P&L rebuilt from events with the trader's ledger."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        },
        {
          widget = {
            name = "trading_equity_chart"
            queries = [{
              name  = "main_query"
              query = { datasetName = "trading_equity", disaggregated = true, fields = local.raw_fields.trading_equity }
            }]
            spec = {
              version    = 3
              widgetType = "line"
              encodings = {
                x = { fieldName = "trade_date", scale = { type = "temporal" }, displayName = "Date" }
                y = { fieldName = "end_balance_rs", scale = { type = "quantitative" }, displayName = "Dummy balance (Rs)" }
              }
              frame = { showTitle = true, title = "Equity curve: dummy balance at the end of each day" }
            }
          }
          position = { x = 0, y = 2, width = 12, height = 6 }
        },
        {
          widget = {
            name = "trading_decisions_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "trading_decisions", disaggregated = true, fields = local.raw_fields.trading_decisions }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.trading_decisions }
              frame      = { showTitle = true, title = "Did your YES/NO choices help? (all days)" }
            }
          }
          position = { x = 0, y = 8, width = 12, height = 3 }
        },
        {
          widget = {
            name = "trading_daily_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "trading_daily", disaggregated = true, fields = local.raw_fields.trading_daily }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.trading_daily }
              frame      = { showTitle = true, title = "Day by day (reconciliation: ok = events match the ledger)" }
            }
          }
          position = { x = 0, y = 11, width = 12, height = 7 }
        },
        {
          widget = {
            name = "trading_by_setup_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "trading_by_setup", disaggregated = true, fields = local.raw_fields.trading_by_setup }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.trading_by_setup }
              frame      = { showTitle = true, title = "Results by setup" }
            }
          }
          position = { x = 0, y = 18, width = 12, height = 5 }
        },
        {
          widget = {
            name = "trading_by_hour_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "trading_by_hour", disaggregated = true, fields = local.raw_fields.trading_by_hour }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.trading_by_hour }
              frame      = { showTitle = true, title = "Results by entry hour (IST)" }
            }
          }
          position = { x = 0, y = 23, width = 12, height = 5 }
        },
        {
          widget = {
            name = "trading_recent_table"
            queries = [{
              name  = "main_query"
              query = { datasetName = "trading_recent", disaggregated = true, fields = local.raw_fields.trading_recent }
            }]
            spec = {
              version    = 2
              widgetType = "table"
              encodings  = { columns = local.table_columns.trading_recent }
              frame      = { showTitle = true, title = "Latest trades" }
            }
          }
          position = { x = 0, y = 28, width = 12, height = 8 }
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
