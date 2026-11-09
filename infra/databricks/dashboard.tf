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
      LIMIT 15
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

    # --- "In plain words" summaries (sql/dashboard_summary.sql, rebuilt after each refresh) ---
    summary_overview = <<-SQL
      SELECT explode(split(coalesce(nullif(ai_text, ''), facts), '(?<=[.!?])\\s+')) AS in_plain_words
      FROM stocks.gold.dashboard_summary
      WHERE page = 'overview'
    SQL

    summary_portfolio = <<-SQL
      SELECT explode(split(coalesce(nullif(ai_text, ''), facts), '(?<=[.!?])\\s+')) AS in_plain_words
      FROM stocks.gold.dashboard_summary
      WHERE page = 'portfolio'
    SQL

    summary_intraday = <<-SQL
      SELECT explode(split(coalesce(nullif(ai_text, ''), facts), '(?<=[.!?])\\s+')) AS in_plain_words
      FROM stocks.gold.dashboard_summary
      WHERE page = 'intraday'
    SQL

    summary_quality = <<-SQL
      SELECT explode(split(coalesce(nullif(ai_text, ''), facts), '(?<=[.!?])\\s+')) AS in_plain_words
      FROM stocks.gold.dashboard_summary
      WHERE page = 'quality'
    SQL

    # --- Portfolio (all dummy pools) ---
    portfolio_kpis = <<-SQL
      WITH d AS (SELECT * FROM stocks.gold.portfolio_daily),
           l AS (SELECT * FROM d ORDER BY snapshot_date DESC LIMIT 1)
      SELECT round(l.total_value_rs, 0)                      AS total_value_rs,
             round(l.pnl_rs, 0)                              AS total_pnl_rs,
             round(l.portfolio_index - 100, 2)               AS since_start_pct,
             round(l.nifty_index - 100, 2)                   AS nifty_same_period_pct,
             l.drawdown_pct                                  AS below_peak_pct,
             round(100 * l.cash_rs / l.total_value_rs, 1)    AS cash_pct
      FROM l
    SQL

    portfolio_vs_nifty = <<-SQL
      SELECT snapshot_date, 'Your portfolio' AS series, portfolio_index AS index_value FROM stocks.gold.portfolio_daily
      UNION ALL
      SELECT snapshot_date, 'NIFTYBEES (index)' AS series, nifty_index AS index_value
      FROM stocks.gold.portfolio_daily WHERE nifty_index IS NOT NULL
    SQL

    portfolio_drawdown = <<-SQL
      SELECT snapshot_date, drawdown_pct FROM stocks.gold.portfolio_daily
    SQL

    portfolio_pools = <<-SQL
      SELECT snapshot_date,
             CASE pool WHEN 'long_term' THEN 'Long-term' WHEN 'swing' THEN 'Swing' ELSE 'Intraday' END AS pool,
             round(equity, 0) AS value_rs
      FROM stocks.silver.portfolio_pools
    SQL

    allocation_pool = <<-SQL
      SELECT CASE pool WHEN 'long_term' THEN 'Long-term' WHEN 'swing' THEN 'Swing' ELSE 'Intraday' END AS pool,
             round(equity, 0) AS value_rs
      FROM stocks.silver.portfolio_pools
      WHERE snapshot_date = (SELECT max(snapshot_date) FROM stocks.silver.portfolio_pools)
    SQL

    allocation_sector = <<-SQL
      SELECT sector, round(sum(value), 0) AS value_rs FROM stocks.gold.holdings_latest GROUP BY sector
      UNION ALL
      SELECT 'Cash' AS sector, round(sum(cash), 0) AS value_rs FROM stocks.silver.portfolio_pools
      WHERE snapshot_date = (SELECT max(snapshot_date) FROM stocks.silver.portfolio_pools)
    SQL

    holdings = <<-SQL
      SELECT CASE pool WHEN 'long_term' THEN 'Long-term' WHEN 'swing' THEN 'Swing' ELSE 'Intraday' END AS pool,
             symbol, sector, CAST(qty AS INT) AS qty, round(avg_cost, 2) AS avg_cost, round(last_price, 2) AS last_price,
             round(value, 0) AS value_rs, round(pnl, 0) AS pnl_rs, pnl_pct, weight_pct, days_held
      FROM stocks.gold.holdings_latest
      ORDER BY value DESC
    SQL

    # --- Intraday KPIs and charts ---
    intraday_kpis = <<-SQL
      SELECT CAST(count(*) AS INT)                                                     AS trades,
             round(sum(net_pnl_rs), 0)                                                  AS net_pnl_rs,
             round(100 * avg(CAST(is_win AS INT)), 0)                                   AS win_rate_pct,
             round(sum(CASE WHEN net_pnl_rs > 0 THEN net_pnl_rs END)
                   / nullif(-sum(CASE WHEN net_pnl_rs < 0 THEN net_pnl_rs END), 0), 2)  AS profit_factor,
             round(avg(r_multiple), 2)                                                  AS expectancy_r,
             round(avg(CASE WHEN net_pnl_rs > 0 THEN net_pnl_rs END)
                   / nullif(-avg(CASE WHEN net_pnl_rs <= 0 THEN net_pnl_rs END), 0), 2) AS win_loss_ratio
      FROM stocks.gold.trades
      WHERE net_pnl_rs IS NOT NULL
    SQL

    intraday_curve = <<-SQL
      SELECT trade_date, round(total_pnl_since_start_rs, 0) AS total_pnl_rs,
             round(total_pnl_since_start_rs - max(total_pnl_since_start_rs) OVER (ORDER BY trade_date), 0) AS below_peak_rs
      FROM stocks.gold.trading_daily
      WHERE total_pnl_since_start_rs IS NOT NULL
    SQL

    intraday_daily_pnl = <<-SQL
      SELECT trade_date, round(coalesce(ledger_realized_rs, net_pnl_rs), 0) AS day_pnl_rs,
             CASE WHEN coalesce(ledger_realized_rs, net_pnl_rs) >= 0 THEN 'Gain' ELSE 'Loss' END AS result
      FROM stocks.gold.trading_daily
      WHERE coalesce(ledger_realized_rs, net_pnl_rs) IS NOT NULL
    SQL

    intraday_setup_chart = <<-SQL
      SELECT concat(setup, ' ', side) AS setup, round(net_pnl_rs, 0) AS net_pnl_rs, round(win_rate_pct, 0) AS win_rate_pct
      FROM stocks.gold.trading_by_setup
    SQL

    intraday_hour_chart = <<-SQL
      SELECT lpad(CAST(entry_hour AS STRING), 2, '0') || ':00' AS entry_hour, round(net_pnl_rs, 0) AS net_pnl_rs
      FROM stocks.gold.trading_by_hour
    SQL

    # --- Company quality (long-term pool) ---
    quality_top = <<-SQL
      SELECT symbol, quality_score, CAST(tests_passed AS INT) AS tests_passed, CAST(tests_available AS INT) AS tests_available,
             coalesce(roce_pct, roa_pct) AS return_on_capital_pct, roe_pct, debt_to_equity,
             revenue_cagr_3y_pct, profit_cagr_3y_pct, pe, pe_percentile, profit_down_2q
      FROM stocks.gold.company_fundamentals
      WHERE tests_available >= 3
      ORDER BY quality_score DESC, pe_percentile ASC
      LIMIT 50
    SQL

    quality_scatter = <<-SQL
      SELECT symbol, roce_pct, pe, CAST(tests_passed AS STRING) || ' of ' || CAST(tests_available AS STRING) AS tests
      FROM stocks.gold.company_fundamentals
      WHERE NOT is_financial AND roce_pct BETWEEN -20 AND 80 AND pe BETWEEN 0 AND 120
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
      us_vs_india          = ["us_return_pct", "india_gap_pct", "us_move"]
      us_vs_india_summary  = ["us_move", "sessions", "avg_us_return_pct", "avg_india_gap_pct", "avg_india_return_pct", "pct_india_up_days", "pct_gap_same_direction_as_us", "corr_us_return_india_gap"]
      unusual_latest       = ["market", "symbol", "trade_date", "volume_ratio", "return_pct", "close", "currency"]
      adr_latest           = ["adr", "nse_symbol", "trade_date", "adr_close_usd", "nse_close_inr", "implied_inr_per_usd", "relative_premium_pct", "fx_inr_per_usd", "fx_premium_pct"]
      global_latest        = ["region", "name", "instrument", "asset_class", "as_of", "value", "change_pct", "change_5d_pct", "change_20d_pct", "source"]
      india_vs_global      = ["name", "instrument", "region", "asset_class", "sessions", "corr_with_india_gap", "corr_with_india_return", "pct_same_direction_gap"]
      trading_daily        = ["trade_date", "proposals", "trades", "said_no", "no_reply", "win_rate_pct", "net_pnl_rs", "skipped_would_have_made_rs", "end_balance_rs", "reconciliation"]
      trading_by_setup     = ["setup", "side", "trades", "win_rate_pct", "net_pnl_rs", "avg_pnl_per_trade_rs", "avg_r_multiple"]
      trading_by_hour      = ["entry_hour", "trades", "win_rate_pct", "net_pnl_rs", "avg_r_multiple"]
      trading_decisions    = ["trades_taken", "taken_net_pnl_rs", "proposals_skipped", "skipped_that_would_have_won", "skipped_would_have_made_rs", "saved_by_skipping_rs"]
      trading_recent       = ["trade_date", "symbol", "side", "setup", "qty", "entry_price", "exit_price", "exit_reason", "net_pnl_rs", "r_multiple", "holding_minutes"]
      holdings             = ["pool", "symbol", "sector", "qty", "avg_cost", "last_price", "value_rs", "pnl_rs", "pnl_pct", "weight_pct", "days_held"]
      quality_top          = ["symbol", "quality_score", "tests_passed", "tests_available", "return_on_capital_pct", "roe_pct", "debt_to_equity", "revenue_cagr_3y_pct", "profit_cagr_3y_pct", "pe", "pe_percentile", "profit_down_2q"]
      portfolio_vs_nifty   = ["snapshot_date", "series", "index_value"]
      portfolio_drawdown   = ["snapshot_date", "drawdown_pct"]
      portfolio_pools      = ["snapshot_date", "pool", "value_rs"]
      allocation_pool      = ["pool", "value_rs"]
      allocation_sector    = ["sector", "value_rs"]
      intraday_curve       = ["trade_date", "total_pnl_rs", "below_peak_rs"]
      intraday_daily_pnl   = ["trade_date", "day_pnl_rs", "result"]
      intraday_setup_chart = ["setup", "net_pnl_rs", "win_rate_pct"]
      intraday_hour_chart  = ["entry_hour", "net_pnl_rs"]
      quality_scatter      = ["symbol", "roce_pct", "pe", "tests"]
      summary_overview     = ["in_plain_words"]
      summary_portfolio    = ["in_plain_words"]
      summary_intraday     = ["in_plain_words"]
      summary_quality      = ["in_plain_words"]
    } : ds => [for c in cols : { name = c, expression = "`${c}`" }]
  }

  table_columns = {
    for ds, fields in local.raw_fields : ds => [for f in fields : { fieldName = f.name, displayName = replace(f.name, "_", " ") }]
  }

  # KPI tiles: one counter per field of a one-row dataset.
  portfolio_counters = [
    { field = "total_value_rs", title = "Total value (Rs)" },
    { field = "total_pnl_rs", title = "Profit or loss (Rs)" },
    { field = "since_start_pct", title = "Return since tracking began (%)" },
    { field = "nifty_same_period_pct", title = "NIFTYBEES, same period (%)" },
    { field = "below_peak_pct", title = "Below peak now (%)" },
    { field = "cash_pct", title = "Cash (% of value)" },
  ]
  intraday_counters = [
    { field = "net_pnl_rs", title = "Net P&L (Rs)" },
    { field = "trades", title = "Trades" },
    { field = "win_rate_pct", title = "Win rate (%)" },
    { field = "profit_factor", title = "Profit factor (gains / losses)" },
    { field = "expectancy_r", title = "Expectancy (R per trade)" },
    { field = "win_loss_ratio", title = "Average win / average loss" },
  ]

  counter_widgets = {
    for ds, list in { portfolio_kpis = local.portfolio_counters, intraday_kpis = local.intraday_counters } : ds => [
      for i, c in list : {
        widget = {
          name = "${ds}_${c.field}"
          queries = [{
            name  = "main_query"
            query = { datasetName = ds, disaggregated = true, fields = [{ name = c.field, expression = "`${c.field}`" }] }
          }]
          spec = {
            version    = 2
            widgetType = "counter"
            encodings  = { value = { fieldName = c.field, displayName = c.title } }
            frame      = { showTitle = true, title = c.title }
          }
        }
        position = { x = i * 2, y = 6, width = 2, height = 3 }
      }
    ]
  }

  # Chart widget: dataset, type, x/y (and optional color) fields, title, position.
  chart = {
    for k, c in {
      ov_unusual        = { ds = "unusual_latest", type = "bar", x = "symbol", xt = "categorical", y = "volume_ratio", color = "market", title = "Unusual volume in the latest session (times the 20-day average)", desc = "Stocks that traded far more than usual (3x or more). Heavy volume often means news or big buyers and sellers.", pos = [0, 12, 6, 6] }
      ov_global         = { ds = "global_latest", type = "bar", x = "name", xt = "categorical", y = "change_pct", color = "asset_class", title = "World markets: latest daily change (%)", desc = "Indices, currencies, oil and rates. Big moves overnight often show up in India's open.", pos = [6, 12, 6, 6] }
      ov_corr           = { ds = "india_vs_global", type = "bar", x = "name", xt = "categorical", y = "corr_with_india_gap", color = "region", title = "Which world markets India's open follows (correlation)", desc = "Near +1 = India's next open usually moves the same way; near 0 = no link; below 0 = the opposite way.", pos = [0, 22, 6, 6] }
      ov_adr            = { ds = "adr_latest", type = "bar", x = "adr", xt = "categorical", y = "relative_premium_pct", color = null, title = "Indian ADRs: US price premium vs NSE (%)", desc = "Indian companies also listed in the US. Above 0 = the US share costs more than the NSE share after currency conversion.", pos = [6, 22, 6, 6] }
      port_vs_nifty     = { ds = "portfolio_vs_nifty", type = "line", x = "snapshot_date", xt = "temporal", y = "index_value", color = "series", title = "Your portfolio vs simply holding the index (both start at 100)", desc = "Both lines start at 100. When yours is above the NIFTYBEES line, the portfolio is beating simply holding the index.", pos = [0, 9, 8, 6] }
      port_alloc_pool   = { ds = "allocation_pool", type = "pie", x = "pool", xt = "categorical", y = "value_rs", color = "pool", title = "Where the money is: by pool", desc = "How the Rs 2 lakh is split across the three pools today.", pos = [8, 9, 4, 6] }
      port_drawdown     = { ds = "portfolio_drawdown", type = "area", x = "snapshot_date", xt = "temporal", y = "drawdown_pct", color = null, title = "How far below its best value the portfolio is (%)", desc = "0 means at its best value so far; -5 means 5% below the best. Shallow, short dips are what you want.", pos = [0, 15, 8, 5] }
      port_alloc_sector = { ds = "allocation_sector", type = "pie", x = "sector", xt = "categorical", y = "value_rs", color = "sector", title = "By sector (index funds and cash shown separately)", desc = "What the money is invested in. Index fund = NIFTYBEES, which holds the whole Nifty 50.", pos = [8, 15, 4, 5] }
      port_pools        = { ds = "portfolio_pools", type = "area", x = "snapshot_date", xt = "temporal", y = "value_rs", color = "pool", title = "Value of each pool over time (Rs)", desc = "Value of each pool day by day.", pos = [0, 20, 12, 5] }
      intra_curve       = { ds = "intraday_curve", type = "line", x = "trade_date", xt = "temporal", y = "total_pnl_rs", color = null, title = "Equity curve: total intraday P&L since the start (Rs)", desc = "Total intraday profit or loss since the start. A line that keeps rising means the strategy makes money.", pos = [0, 9, 8, 6] }
      intra_daily       = { ds = "intraday_daily_pnl", type = "bar", x = "trade_date", xt = "temporal", y = "day_pnl_rs", color = "result", title = "P&L each day (Rs)", desc = "One bar per trading day, coloured by gain or loss.", pos = [8, 9, 4, 6] }
      intra_drawdown    = { ds = "intraday_curve", type = "area", x = "trade_date", xt = "temporal", y = "below_peak_rs", color = null, title = "Below the best P&L so far (Rs)", desc = "How far the P&L is below its best total (Rs). Deep or long dips mean risky trading.", pos = [0, 15, 4, 5] }
      intra_setup       = { ds = "intraday_setup_chart", type = "bar", x = "setup", xt = "categorical", y = "net_pnl_rs", color = null, title = "P&L by setup (Rs)", desc = "Which kinds of trade made or lost money.", pos = [4, 15, 4, 5] }
      intra_hour        = { ds = "intraday_hour_chart", type = "bar", x = "entry_hour", xt = "categorical", y = "net_pnl_rs", color = null, title = "P&L by entry hour, IST (Rs)", desc = "Which entry times made or lost money (IST).", pos = [8, 15, 4, 5] }
      quality_scatter   = { ds = "quality_scatter", type = "scatter", x = "roce_pct", xt = "quantitative", y = "pe", color = "tests", title = "Quality vs price: return on capital (%) against P/E, coloured by tests passed (excl. banks)", desc = "Each dot is a company (banks left out). Further right = more profitable; lower = cheaper. Bottom right is the sweet spot.", pos = [0, 6, 12, 7] }
      } : k => {
      widget = {
        name = k
        queries = [{
          name  = "main_query"
          query = { datasetName = c.ds, disaggregated = true, fields = local.raw_fields[c.ds] }
        }]
        spec = {
          version    = 3
          widgetType = c.type
          encodings = {
            for k, v in(c.type == "pie" ? tomap({
              angle = { fieldName = c.y, scale = { type = "quantitative" }, displayName = replace(c.y, "_", " ") }
              color = { fieldName = c.color, scale = { type = "categorical" }, displayName = replace(coalesce(c.color, "-"), "_", " ") }
              }) : tomap({
              x     = { fieldName = c.x, scale = { type = c.xt }, displayName = replace(c.x, "_", " ") }
              y     = { fieldName = c.y, scale = { type = "quantitative" }, displayName = replace(c.y, "_", " ") }
              color = { fieldName = c.color, scale = { type = "categorical" }, displayName = replace(coalesce(c.color, "-"), "_", " ") }
            })) : k => v if v.fieldName != null
          }
          frame = { showTitle = true, title = c.title, showDescription = true, description = c.desc }
        }
      }
      position = { x = c.pos[0], y = c.pos[1], width = c.pos[2], height = c.pos[3] }
    }
  }

  table_widget = {
    for k, t in {
      holdings          = { ds = "holdings", title = "Current holdings (all pools)", desc = "Everything held right now, largest first. Weight = share of the whole portfolio; P&L includes buying costs.", pos = [0, 25, 12, 6] }
      quality_top       = { ds = "quality_top", title = "Top 50 companies by quality score", desc = "Long-term pool candidates. Score = share of the tests passed (return on capital, debt, cash flow, growth, price). Profit down 2q = profit fell two quarters in a row.", pos = [0, 13, 12, 10] }
      summary_overview  = { ds = "summary_overview", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 2, 12, 4] }
      summary_portfolio = { ds = "summary_portfolio", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 2, 12, 4] }
      summary_intraday  = { ds = "summary_intraday", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 2, 12, 4] }
      summary_quality   = { ds = "summary_quality", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 2, 12, 4] }
      details_unusual   = { ds = "unusual_latest", title = "Unusual volume, latest session (top 15)", desc = "Full numbers behind the Overview chart.", pos = [0, 2, 12, 6] }
      details_global    = { ds = "global_latest", title = "Global markets: latest reading", desc = "FRED series and US-listed country ETFs.", pos = [0, 8, 12, 8] }
      details_ivg       = { ds = "india_vs_global", title = "India's next open vs global moves", desc = "Correlation over all sessions; same direction = how often the gap matched the global move.", pos = [0, 16, 12, 7] }
      details_adr       = { ds = "adr_latest", title = "Indian ADRs vs NSE shares", desc = "Premium vs the pair's median, and vs USD/INR.", pos = [0, 23, 12, 5] }
      } : k => {
      widget = {
        name = "${k}_table"
        queries = [{
          name  = "main_query"
          query = { datasetName = t.ds, disaggregated = true, fields = local.raw_fields[t.ds] }
        }]
        spec = {
          version    = 2
          widgetType = "table"
          encodings  = { columns = local.table_columns[t.ds] }
          frame      = { showTitle = true, title = t.title, showDescription = true, description = t.desc }
        }
      }
      position = { x = t.pos[0], y = t.pos[1], width = t.pos[2], height = t.pos[3] }
    }
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
      layout = concat([
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
              frame = { showTitle = true, title = "Market breadth: % of stocks above their 20-day average", showDescription = true, description = "Above 60% = most stocks are rising (broad strength); below 40% = most are falling." }
            }
          }
          position = { x = 0, y = 6, width = 6, height = 6 }
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
              frame = { showTitle = true, title = "Does India's open follow the US close?", showDescription = true, description = "Each dot is one day: the US move (across) and India's opening gap next morning (up). Dots along a rising line mean India tends to follow the US." }
            }
          }
          position = { x = 6, y = 6, width = 6, height = 6 }
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
              frame      = { showTitle = true, title = "How India trades after US down / flat / up days", showDescription = true, description = "The averages behind the dots above, grouped by the size of the US move." }
            }
          }
          position = { x = 0, y = 18, width = 12, height = 4 }
        },
        ], [local.table_widget.summary_overview, local.chart.ov_unusual, local.chart.ov_global, local.chart.ov_corr, local.chart.ov_adr])
      }, {
      name          = "portfolio"
      displayName   = "Portfolio"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = concat(
        [{
          widget = {
            name                 = "portfolio_title"
            multilineTextboxSpec = { lines = ["## Portfolio (dummy money, Rs 2 lakh)", "All three pools from bumblebee26/paper-trader: intraday (trading), swing (held in NIFTYBEES while its rules are on hold) and long-term. Updated daily at 15:45 IST. Not advice."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        }],
        [local.table_widget.summary_portfolio],
        local.counter_widgets.portfolio_kpis,
        [local.chart.port_vs_nifty, local.chart.port_alloc_pool, local.chart.port_drawdown, local.chart.port_alloc_sector,
        local.chart.port_pools, local.table_widget.holdings],
      )
      }, {
      name          = "paper_trading"
      displayName   = "Intraday trading"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = concat(
        [{
          widget = {
            name                 = "trading_title"
            multilineTextboxSpec = { lines = ["## Intraday trading (dummy money)", "Mock NSE intraday trades approved on Telegram. Profit factor = total gains / total losses (above 1.5 is solid). Expectancy = average result per trade in units of risk (R). Exported daily at 15:45 IST."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        }],
        [local.table_widget.summary_intraday],
        local.counter_widgets.intraday_kpis,
        [local.chart.intra_curve, local.chart.intra_daily, local.chart.intra_drawdown, local.chart.intra_setup, local.chart.intra_hour],
        [
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
            position = { x = 0, y = 20, width = 12, height = 3 }
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
            position = { x = 0, y = 23, width = 12, height = 5 }
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
            position = { x = 0, y = 28, width = 12, height = 5 }
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
            position = { x = 0, y = 33, width = 12, height = 7 }
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
            position = { x = 0, y = 40, width = 12, height = 8 }
          },
        ],
      )
      }, {
      name          = "company_quality"
      displayName   = "Company quality"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = [
        {
          widget = {
            name                 = "quality_title"
            multilineTextboxSpec = { lines = ["## Company quality (long-term pool candidates)", "About 250 large NSE companies, refreshed weekly (Saturday). Tests: return on capital above 15% (banks: return on assets above 1.2%), debt below half of equity, operating cash flow positive in most years, revenue and profit growing over 10% a year, P/E in the cheaper half. Not advice."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        },
        local.chart.quality_scatter,
        local.table_widget.summary_quality,
        local.table_widget.quality_top,
      ]
      }, {
      name          = "details"
      displayName   = "Details (full tables)"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = [
        {
          widget = {
            name                 = "details_title"
            multilineTextboxSpec = { lines = ["## Details", "The full numbers behind the Overview charts, for when you want to dig in."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        },
        local.table_widget.details_unusual,
        local.table_widget.details_global,
        local.table_widget.details_ivg,
        local.table_widget.details_adr,
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
