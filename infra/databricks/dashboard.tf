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
        AND trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily) -- since the ledger reset
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
    longterm_picks = <<-SQL
      SELECT CAST(rank AS INT) AS rank, CASE WHEN is_pick THEN 'Pick' ELSE '' END AS pick, symbol, company, industry,
             score, quality_score, target_weight_pct, coalesce(roce_pct, roa_pct) AS return_on_capital_pct,
             debt_to_equity, profit_cagr_3y_pct, pe, reasons
      FROM stocks.gold.longterm_candidates
      ORDER BY rank
      LIMIT 30
    SQL

    quality_scatter = <<-SQL
      SELECT symbol, roce_pct, pe, CAST(tests_passed AS STRING) || ' of ' || CAST(tests_available AS STRING) AS tests
      FROM stocks.gold.company_fundamentals
      WHERE NOT is_financial AND roce_pct BETWEEN -20 AND 80 AND pe BETWEEN 0 AND 120
    SQL


    # --- Strategies: every real pool and every shadow pool, with NIFTYBEES over the same days ---
    strategy_scoreboard = <<-SQL
      ${local.strategy_scoreboard_sql}
    SQL

    swing_scoreboard = <<-SQL
      SELECT * FROM (${local.strategy_scoreboard_sql}) WHERE grp = 'Swing'
    SQL

    longterm_scoreboard = <<-SQL
      SELECT * FROM (${local.strategy_scoreboard_sql}) WHERE grp = 'Long-term'
    SQL

    strategy_returns = <<-SQL
      ${local.strategy_returns_sql}
    SQL

    swing_returns = <<-SQL
      SELECT * FROM (${local.strategy_returns_sql}) WHERE grp IN ('Swing', 'Benchmark')
    SQL

    longterm_returns = <<-SQL
      SELECT * FROM (${local.strategy_returns_sql}) WHERE grp IN ('Long-term', 'Benchmark')
    SQL

    swing_holdings = <<-SQL
      ${replace(local.strategy_holdings_sql, "__POOLS__", "'swing', 'swing_shadow', 'swing_tech_shadow', 'swing_mom_shadow', 'swing_mom_conc_shadow'")}
    SQL

    longterm_holdings = <<-SQL
      ${replace(local.strategy_holdings_sql, "__POOLS__", "'long_term', 'long_term_shadow', 'long_term_mom_shadow', 'long_term_mom_conc_shadow'")}
    SQL

    scanner_today = <<-SQL
      SELECT list_type, CAST(rank AS INT) AS rank, symbol, company, round(close, 2) AS close, setup, stance,
             round(rsi14, 0) AS rsi, round(adx14, 0) AS adx, round(vol_ratio, 1) AS volume_x,
             round(deliv_per, 0) AS delivery_pct, fno, round(entry_low, 2) AS entry_low, round(entry_high, 2) AS entry_high,
             round(stop, 2) AS stop, round(target2, 2) AS target2, round(rr, 1) AS rr, misses, trade_date
      FROM stocks.gold.technical_screen
      WHERE list_type IN ('strict', 'near-miss')
      ORDER BY list_type DESC, rank
    SQL

    momentum_top = <<-SQL
      SELECT CAST(swing_rank AS INT) AS swing_rank, CAST(lt_rank AS INT) AS lt_rank, symbol, company, industry, close,
             ret_1m_pct, ret_3m_pct, ret_6m_pct, ret_12m_pct, vol_1y_pct, pct_of_52w_high, rsi14,
             CAST(news_pos_5d AS INT) AS good_news_5d, latest_good_news, scanner_strict, swing_score, lt_score, trade_date
      FROM stocks.gold.momentum_ranks
      WHERE swing_rank <= 12 OR lt_rank <= 30
      ORDER BY coalesce(swing_rank, 999), lt_rank
    SQL

    scanner_record = <<-SQL
      SELECT signal_label AS signal, CAST(horizon_days AS INT) AS days_after, CAST(signals AS INT) AS signals,
             avg_return_pct, avg_excess_pct AS vs_niftybees_pct, beat_niftybees_pct
      FROM stocks.gold.signal_performance
      ORDER BY signal, horizon_days
    SQL

    # --- Intraday: what other rules and the shadow models would have done (paper only) ---
    shadow_exit_rules = <<-SQL
      SELECT rule_label AS rule, CAST(trades AS INT) AS trades, would_have_made_rs, actual_rs, vs_actual_rs, beat_actual_pct
      FROM stocks.gold.shadow_rules_summary WHERE test = 'exit rules' AND rule <> 'current'
      ORDER BY vs_actual_rs DESC
    SQL

    shadow_models_real = <<-SQL
      SELECT rule_label AS rule, CAST(trades AS INT) AS trades, would_have_made_rs, actual_rs, vs_actual_rs, beat_actual_pct
      FROM stocks.gold.shadow_rules_summary WHERE test = 'models' AND rule <> 'current'
      ORDER BY vs_actual_rs DESC
    SQL

    # Real vs shadow in one chart: cumulative Rs by day on the same real trades, since the ledger reset (18 Nov)
    intra_vs_exit_rules = <<-SQL
      WITH t AS (
             SELECT trade_date, proposal_id, rule_label, would_have_made_rs, actual_rs FROM stocks.gold.shadow_rules_trades
             WHERE test = 'exit rules' AND rule <> 'current'
               AND trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily) -- since the ledger reset
           ),
           d AS (
             SELECT trade_date, rule_label AS rule, sum(would_have_made_rs) AS day_rs FROM t GROUP BY trade_date, rule_label
             UNION ALL
             SELECT trade_date, 'Real (what happened)' AS rule, sum(actual_rs) AS day_rs
             FROM (SELECT DISTINCT trade_date, proposal_id, actual_rs FROM t) GROUP BY trade_date
           )
      SELECT trade_date, rule, round(sum(day_rs) OVER (PARTITION BY rule ORDER BY trade_date), 0) AS total_rs
      FROM d ORDER BY trade_date, rule
    SQL

    intra_vs_models = <<-SQL
      WITH t AS (
             SELECT trade_date, proposal_id, rule_label, would_have_made_rs, actual_rs FROM stocks.gold.shadow_rules_trades
             WHERE test = 'models' AND rule <> 'current'
               AND trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily) -- since the ledger reset
           ),
           d AS (
             SELECT trade_date, rule_label AS rule, sum(would_have_made_rs) AS day_rs FROM t GROUP BY trade_date, rule_label
             UNION ALL
             SELECT trade_date, 'Real (what happened)' AS rule, sum(actual_rs) AS day_rs
             FROM (SELECT DISTINCT trade_date, proposal_id, actual_rs FROM t) GROUP BY trade_date
           )
      SELECT trade_date, rule, round(sum(day_rs) OVER (PARTITION BY rule ORDER BY trade_date), 0) AS total_rs
      FROM d ORDER BY trade_date, rule
    SQL

    backtest_latest = <<-SQL
      SELECT rule_label AS rule, CAST(traded AS INT) AS trades, total_r, vs_current_r, avg_r, win_pct, days_up_pct,
             worst_day_r, CAST(days AS INT) AS days, run_date
      FROM stocks.gold.model_backtest WHERE is_latest
      ORDER BY total_r DESC
    SQL

    backtest_trend = <<-SQL
      SELECT run_date, rule_label AS rule, vs_current_r FROM stocks.gold.model_backtest
      WHERE rule IN ('filter', 'vol_stop', 'early_cut', 'let_run', 'combined', 'wide_2')
    SQL

    orb_summary = <<-SQL
      SELECT rule_label AS variant, CAST(count(*) AS INT) AS trades, round(sum(would_have_made_rs), 0) AS would_have_made_rs,
             round(100 * avg(CASE WHEN would_have_made_rs > 0 THEN 1 ELSE 0 END), 0) AS win_rate_pct
      FROM stocks.gold.orb_test GROUP BY rule_label ORDER BY would_have_made_rs DESC
    SQL

    shadow_trades_recent = <<-SQL
      SELECT trade_date, symbol, side, setup, test, rule_label AS rule, round(actual_rs, 0) AS actual_rs,
             round(would_have_made_rs, 0) AS would_have_made_rs, round(vs_actual_rs, 0) AS vs_actual_rs
      FROM stocks.gold.shadow_rules_trades
      WHERE trade_date >= date_sub(current_date(), 14) AND rule <> 'current'
      ORDER BY trade_date DESC, symbol, test, rule
    SQL

    # --- Your ratings: how your 👍/👎 calls really did (ratings never train the trader) ---
    rating_summary = <<-SQL
      SELECT CASE rating WHEN 'like' THEN '👍 liked' WHEN 'dislike' THEN '👎 disliked' ELSE 'not rated' END AS rating,
             decision_group AS decisions, CAST(proposals AS INT) AS proposals, CAST(wins AS INT) AS wins, win_rate_pct,
             round(total_outcome_rs, 0) AS total_rs, round(avg_outcome_rs, 0) AS avg_rs, avg_outcome_r AS avg_r,
             CAST(right_calls AS INT) AS right_calls, right_pct
      FROM stocks.gold.rating_review
      ORDER BY CASE rating WHEN 'like' THEN 1 WHEN 'dislike' THEN 2 ELSE 3 END,
               CASE decision_group WHEN 'all' THEN 1 WHEN 'taken' THEN 2 ELSE 3 END
    SQL

    rating_chart = <<-SQL
      SELECT CASE rating WHEN 'like' THEN '👍 liked' WHEN 'dislike' THEN '👎 disliked' ELSE 'not rated' END AS rating,
             decision_group AS decisions, round(avg_outcome_rs, 0) AS avg_rs, win_rate_pct
      FROM stocks.gold.rating_review WHERE decision_group IN ('taken', 'skipped')
    SQL

    rating_trades = <<-SQL
      SELECT trade_date, symbol, setup, side,
             CASE feedback WHEN 'like' THEN '👍' WHEN 'dislike' THEN '👎' END AS your_rating,
             decision, suggested_yes_pct AS suggestion_pct, round(outcome_rs, 0) AS outcome_rs, round(outcome_r, 2) AS outcome_r,
             CASE WHEN (feedback = 'like' AND won = 1) OR (feedback = 'dislike' AND won = 0) THEN 'right' ELSE 'wrong' END AS your_call
      FROM stocks.gold.proposal_outcomes
      WHERE feedback IS NOT NULL AND outcome_rs IS NOT NULL
      ORDER BY proposed_at DESC
      LIMIT 200
    SQL

    trading_recent = <<-SQL
      SELECT trade_date, symbol, side, setup, CAST(qty AS INT) AS qty,
             entry_price, exit_price, exit_reason,
             round(net_pnl_rs, 0) AS net_pnl_rs, round(r_multiple, 2) AS r_multiple,
             round(holding_minutes, 0) AS holding_minutes
      FROM stocks.gold.trades
      WHERE trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily) -- since the ledger reset
      ORDER BY entry_time DESC
      LIMIT 50
    SQL
  }

  # Every strategy (real pools and shadow pools) in one shape: label, group, start, latest return, NIFTYBEES over the
  # same days (from the pool's first snapshot to its latest), and the difference.
  strategy_label_sql = <<-SQL
    CASE pool WHEN 'intraday' THEN 'Intraday (real)' WHEN 'swing' THEN 'Swing (real: held in NIFTYBEES)'
              WHEN 'long_term' THEN 'Long-term (real: cash until buying starts)'
              WHEN 'swing_shadow' THEN 'Swing shadow: news breakouts' WHEN 'swing_tech_shadow' THEN 'Swing shadow: technical scanner'
              WHEN 'long_term_shadow' THEN 'Long-term shadow: quality picks'
              WHEN 'swing_mom_shadow' THEN 'Swing shadow: momentum + news'
              WHEN 'long_term_mom_shadow' THEN 'Long-term shadow: momentum (top 15)'
              WHEN 'swing_mom_conc_shadow' THEN 'Swing shadow: momentum, concentrated (top 3)'
              WHEN 'long_term_mom_conc_shadow' THEN 'Long-term shadow: momentum, concentrated (top 5)' ELSE pool END
  SQL
  strategy_group_sql = "CASE WHEN pool LIKE 'swing%' THEN 'Swing' WHEN pool LIKE 'long_term%' THEN 'Long-term' ELSE 'Intraday' END"

  strategy_daily_sql = <<-SQL
    SELECT snapshot_date, pool, capital, equity, 'real' AS kind FROM stocks.silver.portfolio_pools
    UNION ALL
    SELECT snapshot_date, pool, capital, equity, 'shadow' AS kind FROM stocks.silver.shadow_pools
  SQL

  strategy_scoreboard_sql = <<-SQL
    WITH d AS (${local.strategy_daily_sql}),
         nb AS (SELECT trade_date, close FROM stocks.silver.nse_eq_daily WHERE symbol = 'NIFTYBEES' AND series = 'EQ'),
         span AS (SELECT pool, kind, min(snapshot_date) AS start_date, max(snapshot_date) AS last_date FROM d GROUP BY pool, kind),
         latest AS (SELECT d.pool, d.equity, d.capital FROM d JOIN span s ON d.pool = s.pool AND d.snapshot_date = s.last_date),
         nb_span AS (
           SELECT s.pool,
                  max_by(CASE WHEN nb.trade_date <= s.start_date THEN nb.close END,
                         CASE WHEN nb.trade_date <= s.start_date THEN nb.trade_date END) AS nb_start,
                  max_by(nb.close, nb.trade_date) AS nb_last
           FROM span s JOIN nb ON nb.trade_date <= s.last_date
           GROUP BY s.pool)
    SELECT ${replace(local.strategy_group_sql, "pool", "s.pool")} AS grp,
           ${replace(local.strategy_label_sql, "pool", "s.pool")} AS strategy,
           s.kind, s.start_date, s.last_date, round(l.equity, 0) AS value_rs,
           round((l.equity / l.capital - 1) * 100, 2) AS return_pct,
           round((n.nb_last / n.nb_start - 1) * 100, 2) AS niftybees_pct,
           round((l.equity / l.capital - 1) * 100 - (n.nb_last / n.nb_start - 1) * 100, 2) AS vs_niftybees_pct
    FROM span s JOIN latest l ON l.pool = s.pool LEFT JOIN nb_span n ON n.pool = s.pool
  SQL

  strategy_returns_sql = <<-SQL
    WITH d AS (${local.strategy_daily_sql})
    SELECT snapshot_date, ${local.strategy_label_sql} AS strategy, ${local.strategy_group_sql} AS grp,
           round((equity / capital - 1) * 100, 2) AS return_pct
    FROM d
    UNION ALL
    SELECT snapshot_date, 'NIFTYBEES (since the portfolio started)' AS strategy, 'Benchmark' AS grp,
           round(nifty_index - 100, 2) AS return_pct
    FROM stocks.gold.portfolio_daily WHERE nifty_index IS NOT NULL
  SQL

  strategy_holdings_sql = <<-SQL
    WITH h AS (
      SELECT pool, symbol, sector, qty, avg_cost, last_price, value, pnl, opened, snapshot_date FROM stocks.silver.portfolio_holdings
      UNION ALL
      SELECT pool, symbol, sector, qty, avg_cost, last_price, value, pnl, opened, snapshot_date FROM stocks.silver.shadow_holdings)
    SELECT ${local.strategy_label_sql} AS strategy, symbol, sector, CAST(qty AS INT) AS qty, round(avg_cost, 2) AS avg_cost,
           round(last_price, 2) AS last_price, round(value, 0) AS value_rs, round(pnl, 0) AS pnl_rs,
           round(100 * pnl / nullif(value - pnl, 0), 2) AS pnl_pct, opened
    FROM h
    WHERE pool IN (__POOLS__) AND snapshot_date = (SELECT max(snapshot_date) FROM h)
    ORDER BY strategy, value DESC
  SQL

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
      longterm_picks       = ["rank", "pick", "symbol", "company", "industry", "score", "quality_score", "target_weight_pct", "return_on_capital_pct", "debt_to_equity", "profit_cagr_3y_pct", "pe", "reasons"]
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
      strategy_scoreboard  = ["grp", "strategy", "kind", "start_date", "last_date", "value_rs", "return_pct", "niftybees_pct", "vs_niftybees_pct"]
      swing_scoreboard     = ["strategy", "kind", "start_date", "last_date", "value_rs", "return_pct", "niftybees_pct", "vs_niftybees_pct"]
      longterm_scoreboard  = ["strategy", "kind", "start_date", "last_date", "value_rs", "return_pct", "niftybees_pct", "vs_niftybees_pct"]
      strategy_returns     = ["snapshot_date", "strategy", "grp", "return_pct"]
      swing_returns        = ["snapshot_date", "strategy", "return_pct"]
      longterm_returns     = ["snapshot_date", "strategy", "return_pct"]
      swing_holdings       = ["strategy", "symbol", "sector", "qty", "avg_cost", "last_price", "value_rs", "pnl_rs", "pnl_pct", "opened"]
      longterm_holdings    = ["strategy", "symbol", "sector", "qty", "avg_cost", "last_price", "value_rs", "pnl_rs", "pnl_pct", "opened"]
      scanner_today        = ["list_type", "rank", "symbol", "company", "close", "setup", "stance", "rsi", "adx", "volume_x", "delivery_pct", "fno", "entry_low", "entry_high", "stop", "target2", "rr", "misses", "trade_date"]
      momentum_top         = ["swing_rank", "lt_rank", "symbol", "company", "industry", "close", "ret_1m_pct", "ret_3m_pct", "ret_6m_pct", "ret_12m_pct", "vol_1y_pct", "pct_of_52w_high", "rsi14", "good_news_5d", "latest_good_news", "scanner_strict", "swing_score", "lt_score", "trade_date"]
      scanner_record       = ["signal", "days_after", "signals", "avg_return_pct", "vs_niftybees_pct", "beat_niftybees_pct"]
      shadow_exit_rules    = ["rule", "trades", "would_have_made_rs", "actual_rs", "vs_actual_rs", "beat_actual_pct"]
      shadow_models_real   = ["rule", "trades", "would_have_made_rs", "actual_rs", "vs_actual_rs", "beat_actual_pct"]
      intra_vs_exit_rules  = ["trade_date", "rule", "total_rs"]
      intra_vs_models      = ["trade_date", "rule", "total_rs"]
      backtest_latest      = ["rule", "trades", "total_r", "vs_current_r", "avg_r", "win_pct", "days_up_pct", "worst_day_r", "days", "run_date"]
      backtest_trend       = ["run_date", "rule", "vs_current_r"]
      orb_summary          = ["variant", "trades", "would_have_made_rs", "win_rate_pct"]
      shadow_trades_recent = ["trade_date", "symbol", "side", "setup", "test", "rule", "actual_rs", "would_have_made_rs", "vs_actual_rs"]
      rating_summary       = ["rating", "decisions", "proposals", "wins", "win_rate_pct", "total_rs", "avg_rs", "avg_r", "right_calls", "right_pct"]
      rating_chart         = ["rating", "decisions", "avg_rs", "win_rate_pct"]
      rating_trades        = ["trade_date", "symbol", "setup", "side", "your_rating", "decision", "suggestion_pct", "outcome_rs", "outcome_r", "your_call"]
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
      port_strategies   = { ds = "strategy_returns", type = "line", x = "snapshot_date", xt = "temporal", y = "return_pct", color = "strategy", title = "Every strategy: return since it started (%), real and shadow", desc = "Real pools hold real dummy money; shadow pools test other rules on paper with the same costs. Each line starts at 0 on its own first day.", pos = [0, 31, 12, 7] }
      intra_exit_curve  = { ds = "intra_vs_exit_rules", type = "line", x = "trade_date", xt = "temporal", y = "total_rs", color = "rule", title = "Real vs other exit rules: total since 18 Nov (Rs)", desc = "Your real result (what happened) and each exit rule on the same real trades, added up day by day. A rule's line above the real line = it would have made more.", pos = [0, 49, 6, 6] }
      intra_model_curve = { ds = "intra_vs_models", type = "line", x = "trade_date", xt = "temporal", y = "total_rs", color = "rule", title = "Real vs the shadow models: total since 18 Nov (Rs)", desc = "Your real result and each model's decision on the same real trades, added up day by day (models trained only on earlier days). Above the real line = the model would have made more.", pos = [6, 49, 6, 6] }
      intra_exit_rules  = { ds = "shadow_exit_rules", type = "bar", x = "rule", xt = "categorical", y = "vs_actual_rs", color = null, title = "Exit rules on your real trades: gain vs what happened (Rs)", desc = "Each closed trade replayed on its 1-minute candles under another exit rule. Above 0 = that rule would have made more than what really happened.", pos = [0, 55, 6, 6] }
      intra_models_real = { ds = "shadow_models_real", type = "bar", x = "rule", xt = "categorical", y = "vs_actual_rs", color = null, title = "Shadow models on your real trades: gain vs what happened (Rs)", desc = "Each real trade scored by models trained only on earlier days. Above 0 = the model's decision would have made more.", pos = [6, 55, 6, 6] }
      intra_backtest    = { ds = "backtest_latest", type = "bar", x = "rule", xt = "categorical", y = "vs_current_r", color = null, title = "Backtest, all setups on ~200 stocks: each rule vs current rules (R)", desc = "Latest walk-forward run (each day judged by models trained only on earlier days). R = one unit of risk, about Rs 400. Above 0 = better than the current rules.", pos = [0, 61, 6, 6] }
      intra_bt_trend    = { ds = "backtest_trend", type = "line", x = "run_date", xt = "temporal", y = "vs_current_r", color = "rule", title = "Is each model's edge holding up? (R vs current rules, by run day)", desc = "One point per daily run. A line that stays above 0 run after run is a model worth reviewing for go-live.", pos = [6, 61, 6, 6] }
      intra_orb         = { ds = "orb_summary", type = "bar", x = "variant", xt = "categorical", y = "would_have_made_rs", color = null, title = "Opening-range test on paper (Rs)", desc = "The top stocks in play each day, traded on paper with a 5- or 15-minute opening range, a 2R target or held to 15:10.", pos = [0, 67, 6, 5] }
      swing_curve       = { ds = "swing_returns", type = "line", x = "snapshot_date", xt = "temporal", y = "return_pct", color = "strategy", title = "Swing: real pool vs shadow strategies vs NIFTYBEES (return %)", desc = "The real swing pool holds NIFTYBEES; the shadow pools trade stocks on paper: news breakouts, technical scanner, momentum + news (6 stocks) and its concentrated copy (top 3). Each line starts at 0 on its own first day.", pos = [0, 6, 12, 7] }
      lt_curve          = { ds = "longterm_returns", type = "line", x = "snapshot_date", xt = "temporal", y = "return_pct", color = "strategy", title = "Long-term: real pool vs shadows vs NIFTYBEES (return %)", desc = "The real pool waits in cash until its go/no-go review; the shadows buy on paper: the weekly quality picks, momentum (top 15) and its concentrated copy (top 5).", pos = [0, 6, 12, 7] }
      rating_bars       = { ds = "rating_chart", type = "bar", x = "rating", xt = "categorical", y = "avg_rs", color = "decisions", title = "Average result per proposal, by your rating (Rs)", desc = "Taken = the real net result; skipped = what it would have made. If 👍 bars are above 👎 bars, your instinct is adding something.", pos = [0, 9, 12, 6] }
      quality_scatter   = { ds = "quality_scatter", type = "scatter", x = "roce_pct", xt = "quantitative", y = "pe", color = "tests", title = "Quality vs price: return on capital (%) against P/E, coloured by tests passed (excl. banks)", desc = "Each dot is a company (banks left out). Further right = more profitable; lower = cheaper. Bottom right is the sweet spot.", pos = [0, 23, 12, 7] }
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
      quality_top       = { ds = "longterm_picks", title = "Long-term pool: this week's 10 picks (max 2 per industry) and the next 20", desc = "Eligible Nifty 200 companies: quality score 0.6 or more, profitable, no two-quarter profit fall. Score = 100 x quality + growth bonus - price penalty. Target weight = share of the Rs 1 lakh pool; 10% stays in cash. Shadow mode: nothing is bought yet.", pos = [0, 30, 12, 10] }
      summary_overview  = { ds = "summary_overview", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 2, 12, 4] }
      summary_portfolio = { ds = "summary_portfolio", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 2, 12, 4] }
      summary_intraday  = { ds = "summary_intraday", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 2, 12, 4] }
      summary_quality   = { ds = "summary_quality", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 19, 12, 4] }
      port_scoreboard   = { ds = "strategy_scoreboard", title = "Scoreboard: every strategy vs NIFTYBEES over the same days", desc = "Real pools and shadow pools (paper). Return since each one started; NIFTYBEES over exactly the same days; the difference.", pos = [0, 38, 12, 6] }
      intra_bt_table    = { ds = "backtest_latest", title = "Backtest details (latest run)", desc = "All rules on the same setups. Trades = setups taken (the filter skips some). Worst day = the worst single day's total.", pos = [0, 72, 12, 7] }
      intra_shadow_list = { ds = "shadow_trades_recent", title = "Your trades under other rules (last 14 days)", desc = "Every real trade, what really happened, and what each rule or model would have made instead.", pos = [6, 67, 6, 5] }
      swing_board       = { ds = "swing_scoreboard", title = "Swing scoreboard", desc = "Return since each started vs NIFTYBEES over the same days.", pos = [0, 2, 12, 4] }
      swing_hold        = { ds = "swing_holdings", title = "What each swing strategy holds now", desc = "Real pool and shadow pools (paper).", pos = [0, 13, 12, 6] }
      swing_scan        = { ds = "scanner_today", title = "Today's technical scanner (Nifty 200): strict picks and near-misses", desc = "What the scanner shadow pool buys from (next day, only at the scanner's level). Indicative levels, never a guaranteed return.", pos = [0, 19, 12, 7] }
      swing_signals     = { ds = "scanner_record", title = "How each scanner signal did afterwards (vs NIFTYBEES)", desc = "Average return 5, 10 and 20 trading days after each signal over ~6 months, and how often it beat NIFTYBEES. Past results only.", pos = [0, 26, 12, 7] }
      swing_mom         = { ds = "momentum_top", title = "Momentum ranks (Nifty 200): swing top 12 and long-term top 30", desc = "What the momentum shadow pools buy from. Swing: 1- and 3-month strength, near the 52-week high, fresh good news, scanner picks. Long term: volatility-adjusted 6- and 12-month momentum (the Momentum 30 method). Past strength only.", pos = [0, 33, 12, 8] }
      lt_board          = { ds = "longterm_scoreboard", title = "Long-term scoreboard", desc = "Return since each started vs NIFTYBEES over the same days.", pos = [0, 2, 12, 4] }
      lt_hold           = { ds = "longterm_holdings", title = "What each long-term strategy holds now", desc = "Real pool and shadow pool (paper).", pos = [0, 13, 12, 6] }
      rating_table      = { ds = "rating_summary", title = "Your 👍/👎 vs what really happened", desc = "For your own learning: ratings never train the trader. Right calls = a 👍 that made money or a 👎 that lost (taken: real result; skipped: what it would have made).", pos = [0, 2, 12, 7] }
      rating_list       = { ds = "rating_trades", title = "Every rated proposal (newest first)", desc = "Your rating, what the trader did, and the outcome.", pos = [0, 15, 12, 9] }
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
      name          = "portfolio"
      displayName   = "Portfolio"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = concat(
        [{
          widget = {
            name                 = "portfolio_title"
            multilineTextboxSpec = { lines = ["## Portfolio (dummy money, Rs 2 lakh)", "All three pools from bumblebee26/paper-trader: intraday (trading), swing (held in NIFTYBEES while its rules are on hold) and long-term. Bottom: every real and shadow strategy side by side. Updated daily at 15:45 IST. Not advice."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        }],
        [local.table_widget.summary_portfolio],
        local.counter_widgets.portfolio_kpis,
        [local.chart.port_vs_nifty, local.chart.port_alloc_pool, local.chart.port_drawdown, local.chart.port_alloc_sector,
        local.chart.port_pools, local.table_widget.holdings, local.chart.port_strategies, local.table_widget.port_scoreboard],
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
          {
            widget = {
              name                 = "intraday_shadow_title"
              multilineTextboxSpec = { lines = ["### Shadow tests (paper only: nothing here changes a real trade)", "Exit rules and the shadow models (volatility stop, trade filter, early cut, let winners run) on your real trades (first two charts: real and shadow lines together) and on every scanner setup across ~200 stocks. Reviewed with you before anything goes live (before December)."] }
            }
            position = { x = 0, y = 48, width = 12, height = 1 }
          },
        ],
        [local.chart.intra_exit_curve, local.chart.intra_model_curve, local.chart.intra_exit_rules, local.chart.intra_models_real,
        local.chart.intra_backtest, local.chart.intra_bt_trend, local.chart.intra_orb, local.table_widget.intra_shadow_list,
        local.table_widget.intra_bt_table],
      )
      }, {
      name          = "swing"
      displayName   = "Swing"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = [
        {
          widget = {
            name                 = "swing_title"
            multilineTextboxSpec = { lines = ["## Swing pool: real vs shadow strategies (dummy money)", "Real pool: Rs 60,000 held in NIFTYBEES while new rules are tested. Shadow pools (paper, same costs): news breakouts (from 16 Nov), the technical scanner's strict picks (from 17 Nov) momentum + news (from 18 Nov, 6 stocks, 2% risk per trade) and its concentrated copy (top 3, up to a third each, 3% risk). Not advice."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        },
        local.table_widget.swing_board,
        local.chart.swing_curve,
        local.table_widget.swing_hold,
        local.table_widget.swing_scan,
        local.table_widget.swing_signals,
        local.table_widget.swing_mom,
      ]
      }, {
      name          = "long_term"
      displayName   = "Long term"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = [
        {
          widget = {
            name                 = "quality_title"
            multilineTextboxSpec = { lines = ["## Long-term pool: real vs shadow (dummy money)", "Real pool: Rs 1 lakh in cash until its go/no-go review (18 Dec). Shadow pools (paper): the weekly quality picks, momentum (top 15 of the Nifty 200, rebalanced monthly, from 21 Nov) and its concentrated copy (top 5). Below: company quality, refreshed every Saturday. Tests: return on capital above 15% (banks: return on assets above 1.2%), debt below half of equity, operating cash flow positive in most years, revenue and profit growing over 10% a year, P/E in the cheaper half. Not advice."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        },
        local.table_widget.lt_board,
        local.chart.lt_curve,
        local.table_widget.lt_hold,
        local.table_widget.summary_quality,
        local.chart.quality_scatter,
        local.table_widget.quality_top,
      ]
      }, {
      name          = "ratings"
      displayName   = "Your ratings"
      pageType      = "PAGE_TYPE_CANVAS"
      layoutVersion = "GRID_V1"
      layout = [
        {
          widget = {
            name                 = "ratings_title"
            multilineTextboxSpec = { lines = ["## Your 👍/👎: how your calls really did", "For you to learn from: ratings are recorded on Telegram but never train the trader (from 17 Nov). Outcome = the real net result if the trade was taken, what it would have made if skipped."] }
          }
          position = { x = 0, y = 0, width = 12, height = 2 }
        },
        local.table_widget.rating_table,
        local.chart.rating_bars,
        local.table_widget.rating_list,
      ]
      }, {
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

  depends_on = [databricks_pipeline.stocks]
}

output "dashboard_url" {
  value = "${trimsuffix(var.databricks_workspace_url, "/")}/dashboardsv3/${databricks_dashboard.stocks.id}/published"
}
