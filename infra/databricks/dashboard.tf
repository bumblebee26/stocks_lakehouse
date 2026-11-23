# ---------------------------------------------------------------------------
# AI/BI (Lakeview) dashboard on the gold tables. Six pages, each in the same order: headline tiles, one main chart,
# breakdowns, then detail tables (redesigned 18 Nov 2026; mockups in the "Trading Dashboard Revamp" canvas).
#   Desk · Intraday · Swing · Long term · Your calls · Global markets
# Intraday, Swing and Long term have a "Show" filter (real / shadow / benchmark) that filters their chart, scoreboard
# and holdings together. Bars and arrows in tables (█░, ▲▼, ✓ ✕) are text made in the queries.
# Layout grid is 12 columns wide; every row's widgets add up to width 12.
# Edits made in the UI are overwritten on the next apply; change them here.
# ---------------------------------------------------------------------------

locals {
  # Dataset name -> SQL. Derived columns are computed in SQL, not in widgets.
  dashboard_datasets = {
    # --- Desk ---
    desk_status = <<-SQL
      WITH s AS (SELECT trade_date, str_to_map(note, ';', '=') AS kv FROM stocks.silver.trade_events
                 WHERE event = 'ledger_snapshot' ORDER BY trade_date DESC, event_ts DESC LIMIT 1),
           m AS (SELECT mood FROM stocks.gold.proposal_outcomes WHERE mood IS NOT NULL ORDER BY proposed_at DESC LIMIT 1)
      SELECT CASE try_element_at(kv, 'auto_mode') WHEN 'on' THEN '● Auto mode ON' WHEN 'off' THEN '○ Auto mode OFF' ELSE '–' END AS auto_mode,
             coalesce(concat('India VIX ', try_element_at(kv, 'vix')), '–') AS india_vix,
             coalesce((SELECT mood FROM m), '–') AS mood,
             coalesce(concat(try_element_at(kv, 'risk_pct'), '% a trade'), '–') AS risk,
             date_format(trade_date, 'EEE d MMM') AS as_of
      FROM s
    SQL

    desk_kpis = <<-SQL
      WITH l AS (SELECT * FROM stocks.gold.portfolio_daily ORDER BY snapshot_date DESC LIMIT 1),
           t AS (SELECT trades_opened FROM stocks.gold.trading_daily ORDER BY trade_date DESC LIMIT 1),
           r AS (SELECT CASE WHEN (feedback = 'like' AND won = 1) OR (feedback = 'dislike' AND won = 0) THEN 1 ELSE 0 END AS right_call
                 FROM stocks.gold.proposal_outcomes WHERE feedback IN ('like', 'dislike') AND won IS NOT NULL)
      SELECT round(l.total_value_rs, 0)                    AS portfolio_rs,
             round(l.return_pct, 2)                        AS since_reset_pct,
             round(coalesce(l.day_change_rs, l.pnl_rs), 0) AS today_pnl_rs,
             CAST((SELECT trades_opened FROM t) AS INT)    AS trades_today,
             CAST((SELECT count(*) FROM stocks.gold.proposal_outcomes
                   WHERE trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily)) AS INT) AS proposals,
             (SELECT round(100 * avg(right_call), 0) FROM r) AS calls_right_pct
      FROM l
    SQL

    desk_setups = <<-SQL
      SELECT setup,
             concat(repeat('█', CAST(round(win_pct / 10) AS INT)), repeat('░', 10 - CAST(round(win_pct / 10) AS INT)),
                    ' ', CAST(CAST(round(win_pct) AS INT) AS STRING), '%') AS win_rate,
             avg_r, CAST(n AS INT) AS proposals
      FROM (SELECT setup, 100 * avg(won) AS win_pct, round(avg(outcome_r), 2) AS avg_r, count(*) AS n
            FROM stocks.gold.proposal_outcomes
            WHERE outcome_rs IS NOT NULL AND source <> 'custom' AND trade_date >= date_sub(current_date(), 30)
            GROUP BY setup)
      ORDER BY avg_r DESC
    SQL

    desk_decisions = <<-SQL
      SELECT date_format(proposed_at, 'HH:mm') AS time, symbol, concat(setup, ' ', lower(side)) AS setup,
             coalesce(concat(CAST(suggested_yes_pct AS STRING), '%'), '–') AS score,
             CASE reviewer_verdict WHEN 'YES' THEN 'Agrees' WHEN 'NO' THEN 'Objects' WHEN 'UNSURE' THEN 'Unsure' ELSE '–' END AS reviewer,
             CASE decision WHEN 'taken' THEN '✓ Placed' WHEN 'skipped_auto' THEN '– Skipped (score)' WHEN 'skipped_by_you' THEN '– You said no'
                           WHEN 'expired' THEN '– No reply' WHEN 'not_filled' THEN '✕ Not filled' ELSE '… Open' END AS decision,
             CASE feedback WHEN 'like' THEN '👍' WHEN 'dislike' THEN '👎' ELSE '–' END AS you,
             CASE WHEN outcome_r IS NULL THEN '–'
                  WHEN decision = 'taken' THEN concat(CASE WHEN outcome_r >= 0 THEN '▲ +' ELSE '▼ −' END, format_number(abs(outcome_r), 1), 'R')
                  ELSE concat('would be ', CASE WHEN outcome_r >= 0 THEN '+' ELSE '−' END, format_number(abs(outcome_r), 1), 'R') END AS result
      FROM stocks.gold.proposal_outcomes
      WHERE trade_date = (SELECT max(trade_date) FROM stocks.gold.proposal_outcomes)
      ORDER BY proposed_at
    SQL

    # Each real pool: value now, P&L since 18 Nov (value minus starting cash), return (Desk and the phone app).
    desk_pools = <<-SQL
      SELECT CASE grp WHEN 'Intraday' THEN 'Intraday' WHEN 'Swing' THEN 'Swing' ELSE 'Long term' END AS pool,
             value_rs, pnl_rs, return_pct, start_date AS since, last_date AS as_of
      FROM (${local.strategy_scoreboard_sql})
      WHERE kind = 'real'
      ORDER BY CASE grp WHEN 'Intraday' THEN 1 WHEN 'Swing' THEN 2 ELSE 3 END
    SQL

    desk_board = <<-SQL
      SELECT strategy, initcap(kind) AS kind,
             ${replace(local.signed_pct_sql, "__V__", "return_pct")} AS return_since_start,
             ${replace(local.signed_pts_sql, "__V__", "vs_niftybees_pct")} AS vs_niftybees
      FROM (${local.strategy_scoreboard_sql})
      ORDER BY return_pct DESC
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

    allocation_pool = <<-SQL
      SELECT CASE pool WHEN 'long_term' THEN 'Long-term' WHEN 'swing' THEN 'Swing' ELSE 'Intraday' END AS pool,
             round(equity, 0) AS value_rs
      FROM stocks.silver.portfolio_pools
      WHERE snapshot_date = (SELECT max(snapshot_date) FROM stocks.silver.portfolio_pools)
      ORDER BY value_rs DESC
    SQL

    holdings = <<-SQL
      SELECT CASE pool WHEN 'long_term' THEN 'Long-term' WHEN 'swing' THEN 'Swing' ELSE 'Intraday' END AS pool,
             symbol, sector, CAST(qty AS INT) AS qty, round(avg_cost, 2) AS avg_cost, round(last_price, 2) AS last_price,
             round(value, 0) AS value_rs, round(pnl, 0) AS pnl_rs, pnl_pct, weight_pct, days_held
      FROM stocks.gold.holdings_latest
      ORDER BY value DESC
    SQL

    # --- "In plain words" summaries (sql/dashboard_summary.sql, rebuilt after each job) ---
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

    # --- Intraday (since the 18 Nov ledger reset) ---
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

    # Real and every shadow line in one chart: cumulative Rs by day since the reset. Exit rules and models replay the
    # same real trades (the real line is those trades as they happened); the opening-range test is its own paper trades.
    intra_lines = <<-SQL
      WITH t AS (${local.intra_shadow_sql}),
           d AS (
             SELECT trade_date, rule_label AS line, CASE test WHEN 'models' THEN 'Models' ELSE 'Exit rules' END AS kind,
                    sum(would_have_made_rs) AS day_rs
             FROM t GROUP BY trade_date, rule_label, test
             UNION ALL
             SELECT trade_date, 'Real (what happened)' AS line, 'Real' AS kind, sum(actual_rs) AS day_rs
             FROM (SELECT DISTINCT trade_date, proposal_id, actual_rs FROM t WHERE test = 'exit rules') GROUP BY trade_date
             UNION ALL
             SELECT trade_date, rule_label AS line, 'Opening-range test' AS kind, sum(would_have_made_rs) AS day_rs
             FROM stocks.gold.orb_test
             WHERE trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily)
             GROUP BY trade_date, rule_label)
      SELECT trade_date, line, kind, round(sum(day_rs) OVER (PARTITION BY line ORDER BY trade_date), 0) AS total_rs
      FROM d ORDER BY trade_date, line
    SQL

    intra_board = <<-SQL
      WITH t AS (${local.intra_shadow_sql}),
           real_trades AS (SELECT DISTINCT proposal_id, actual_rs FROM t WHERE test = 'exit rules')
      SELECT rule_label AS rule, CASE test WHEN 'models' THEN 'Models' ELSE 'Exit rules' END AS kind, CAST(count(*) AS INT) AS trades,
             round(sum(would_have_made_rs), 0) AS total_rs,
             round(sum(vs_actual_rs), 0) AS vs_real_rs,
             round(100 * avg(CASE WHEN vs_actual_rs > 0 THEN 1 ELSE 0 END), 0) AS beat_real_pct
      FROM t GROUP BY rule_label, test
      UNION ALL
      SELECT 'Real (what happened)' AS rule, 'Real' AS kind, CAST(count(*) AS INT) AS trades, round(sum(actual_rs), 0) AS total_rs,
             CAST(0 AS DOUBLE) AS vs_real_rs, CAST(NULL AS DOUBLE) AS beat_real_pct
      FROM real_trades
      UNION ALL
      SELECT rule_label AS rule, 'Opening-range test' AS kind, CAST(count(*) AS INT) AS trades, round(sum(would_have_made_rs), 0) AS total_rs,
             CAST(NULL AS DOUBLE) AS vs_real_rs, CAST(NULL AS DOUBLE) AS beat_real_pct
      FROM stocks.gold.orb_test
      WHERE trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily)
      GROUP BY rule_label
      ORDER BY total_rs DESC
    SQL

    # Auto-bar ladder and score-sized trades (paper, from 21 Nov): every proposal with a known outcome (placed: the
    # real result; skipped: what it would have made) at each bar, and sized 0.5x / 1x / 1.5x by its suggestion.
    intra_bar_ladder = <<-SQL
      WITH o AS (SELECT suggested_yes_pct AS pct, outcome_rs FROM stocks.gold.proposal_outcomes
                 WHERE outcome_rs IS NOT NULL AND suggested_yes_pct IS NOT NULL
                   AND trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily)),
           bars AS (SELECT explode(array(50, 55, 60, 65, 70)) AS bar)
      SELECT concat('Take every proposal over ', bar, '%') AS version, CAST(count(o.pct) AS INT) AS trades,
             round(coalesce(sum(o.outcome_rs), 0), 0) AS total_rs,
             round(100 * avg(CASE WHEN o.outcome_rs > 0 THEN 1.0 ELSE 0.0 END), 0) AS won_pct
      FROM bars LEFT JOIN o ON o.pct > bars.bar GROUP BY bar
      UNION ALL
      SELECT 'Sized by score (0.5x 55-59%, 1x 60-69%, 1.5x 70%+)' AS version, CAST(count(*) AS INT) AS trades,
             round(sum(outcome_rs * CASE WHEN pct >= 70 THEN 1.5 WHEN pct >= 60 THEN 1.0 ELSE 0.5 END), 0) AS total_rs,
             round(100 * avg(CASE WHEN outcome_rs > 0 THEN 1.0 ELSE 0.0 END), 0) AS won_pct
      FROM o WHERE pct >= 55
      ORDER BY version
    SQL

    # Selector (paper, from 23 Nov): what its daily picks made vs the fixed rules, per family, and its latest picks.
    intra_selector = <<-SQL
      WITH s AS (SELECT trade_date, reasons, str_to_map(note, ';', '=') AS kv FROM stocks.silver.trade_events
                 WHERE event = 'selector'),
           fam AS (SELECT explode(array('exit', 'bar', 'entry')) AS family),
           latest AS (SELECT reasons FROM s ORDER BY trade_date DESC LIMIT 1)
      SELECT fam.family, CAST(count(s.trade_date) AS INT) AS days,
             round(sum(CAST(s.kv[concat(fam.family, '_rs')] AS DOUBLE)), 0) AS selector_rs,
             round(sum(CAST(s.kv[concat(fam.family, '_fixed_rs')] AS DOUBLE)), 0) AS fixed_rs,
             regexp_extract((SELECT reasons FROM latest), concat(fam.family, ': [^|]*'), 0) AS latest_pick
      FROM fam LEFT JOIN s ON true GROUP BY fam.family ORDER BY fam.family
    SQL

    # Paper fills against the real 1-minute candle of the fill minute (paper-trader fill_check rows, from 21 Nov).
    intra_fill_check = <<-SQL
      WITH f AS (SELECT trade_date, symbol, side, str_to_map(note, ';', '=') AS kv FROM stocks.silver.trade_events
                 WHERE event = 'fill_check' AND trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily))
      SELECT trade_date, symbol, side, CAST(kv['worse_bps'] AS DOUBLE) AS paper_worse_bps,
             CASE kv['inside'] WHEN '1' THEN 'yes' WHEN '1.0' THEN 'yes' ELSE 'no' END AS inside_minute_range
      FROM f ORDER BY trade_date DESC, symbol
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

    shadow_trades_recent = <<-SQL
      SELECT trade_date, symbol, side, setup, test, rule_label AS rule, round(actual_rs, 0) AS actual_rs,
             round(would_have_made_rs, 0) AS would_have_made_rs, round(vs_actual_rs, 0) AS vs_actual_rs
      FROM stocks.gold.shadow_rules_trades
      WHERE trade_date >= date_sub(current_date(), 14) AND rule <> 'current'
      ORDER BY trade_date DESC, symbol, test, rule
    SQL

    # --- Swing and Long term: real pool vs shadow pools vs NIFTYBEES ("Show" filter on kind) ---
    swing_kpis = <<-SQL
      ${replace(local.pool_kpis_sql, "__GRP__", "Swing")}
    SQL

    longterm_kpis = <<-SQL
      ${replace(local.pool_kpis_sql, "__GRP__", "Long-term")}
    SQL

    swing_scoreboard = <<-SQL
      ${replace(local.pool_board_sql, "__GRP__", "Swing")}
    SQL

    longterm_scoreboard = <<-SQL
      ${replace(local.pool_board_sql, "__GRP__", "Long-term")}
    SQL

    swing_best = <<-SQL
      ${replace(local.pool_best_sql, "__GRP__", "Swing")}
    SQL

    longterm_best = <<-SQL
      ${replace(local.pool_best_sql, "__GRP__", "Long-term")}
    SQL

    swing_trades = <<-SQL
      ${replace(local.pool_trades_sql, "__GRP__", "Swing")}
    SQL

    longterm_trades = <<-SQL
      ${replace(local.pool_trades_sql, "__GRP__", "Long-term")}
    SQL

    swing_returns = <<-SQL
      SELECT snapshot_date, strategy, kind, return_pct, pnl_rs FROM (${local.strategy_returns_sql}) WHERE grp IN ('Swing', 'Benchmark')
    SQL

    longterm_returns = <<-SQL
      SELECT snapshot_date, strategy, kind, return_pct, pnl_rs FROM (${local.strategy_returns_sql}) WHERE grp IN ('Long-term', 'Benchmark')
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

    # "Suggested today" on the Swing and Long-term pages and the phone app: information only, nothing is placed from it.
    # Swing: what the auto rules would buy next (momentum + news top 6, stop 2 x ATR under the close) and the
    # scanner's strict picks with their levels. Chart = TradingView (M&M -> M_M, BAJAJ-AUTO -> BAJAJ_AUTO).
    swing_suggest = <<-SQL
      SELECT strategy, CAST(rank AS INT) AS rank, symbol, company, close, entry, stop, target, rr, why, chart, trade_date
      FROM (
        SELECT 'Momentum + news (auto rules)' AS strategy, 1 AS grp, swing_rank AS rank, symbol, company, close,
               concat('near ', format_number(close, 2)) AS entry, round(close - 2 * atr14, 2) AS stop,
               CAST(NULL AS DOUBLE) AS target, CAST(NULL AS DOUBLE) AS rr,
               concat('1m ', format_number(ret_1m_pct, 1), '% · 3m ', format_number(ret_3m_pct, 1), '% · ',
                      format_number(pct_of_52w_high, 0), '% of 52-week high',
                      CASE WHEN news_pos_5d > 0 THEN ' · fresh good news' ELSE '' END,
                      ' · exit: 2xATR stop or out of the top 12') AS why,
               concat('https://www.tradingview.com/chart/?symbol=NSE%3A', translate(symbol, '&-', '__')) AS chart, trade_date
        FROM stocks.gold.momentum_ranks
        WHERE swing_rank <= 6
        UNION ALL
        SELECT 'Scanner strict pick', 2, rank, symbol, company, round(close, 2),
               concat(format_number(entry_low, 2), ' to ', format_number(entry_high, 2)), round(stop, 2),
               round(target2, 2), round(rr, 1), concat(setup, ' · RSI ', format_number(rsi14, 0), ' · ADX ', format_number(adx14, 0)),
               concat('https://www.tradingview.com/chart/?symbol=NSE%3A', translate(symbol, '&-', '__')), trade_date
        FROM stocks.gold.technical_screen
        WHERE list_type = 'strict'
      )
      ORDER BY grp, rank
    SQL

    # Long term: this week's quality picks (top 10) and the momentum top 15 (volatility-adjusted 6 and 12 months).
    longterm_suggest = <<-SQL
      SELECT strategy, CAST(rank AS INT) AS rank, symbol, company, close, why, chart
      FROM (
        SELECT 'Quality pick (weekly)' AS strategy, 1 AS grp, c.rank, c.symbol, c.company, m.close,
               concat('score ', format_number(c.score, 1),
                      coalesce(concat(' · weight ', format_number(c.target_weight_pct, 0), '%'), ''),
                      coalesce(concat(' · ', c.reasons), '')) AS why,
               concat('https://www.tradingview.com/chart/?symbol=NSE%3A', translate(c.symbol, '&-', '__')) AS chart
        FROM stocks.gold.longterm_candidates c
        LEFT JOIN stocks.gold.momentum_ranks m ON m.symbol = c.symbol
        WHERE c.rank <= 10
        UNION ALL
        SELECT 'Momentum top 15', 2, lt_rank, symbol, company, close,
               concat('6m ', format_number(ret_6m_pct, 1), '% · 12m ', format_number(ret_12m_pct, 1),
                      '% · volatility ', format_number(vol_1y_pct, 0), '%'),
               concat('https://www.tradingview.com/chart/?symbol=NSE%3A', translate(symbol, '&-', '__'))
        FROM stocks.gold.momentum_ranks
        WHERE lt_rank <= 15
      )
      ORDER BY grp, rank
    SQL

    # USD/INR, past five years (FRED DEXINUS; history loaded once on 24 Nov), for the Global markets page.
    usdinr_history = <<-SQL
      SELECT obs_date, round(value, 2) AS usd_inr
      FROM stocks.silver.global_daily
      WHERE instrument = 'DEXINUS' AND value IS NOT NULL AND obs_date >= add_months(current_date(), -60)
      ORDER BY obs_date
    SQL

    scanner_record = <<-SQL
      SELECT signal_label AS signal, CAST(horizon_days AS INT) AS days_after, CAST(signals AS INT) AS signals,
             avg_return_pct, avg_excess_pct AS vs_niftybees_pct, beat_niftybees_pct
      FROM stocks.gold.signal_performance
      ORDER BY signal, horizon_days
    SQL

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

    # --- Your calls: your 👍/👎 against what really happened (ratings never train the trader) ---
    calls_kpis = <<-SQL
      WITH r AS (${local.rated_sql})
      SELECT round(100 * avg(right_call), 0) AS calls_right_pct,
             round(avg(CASE WHEN feedback = 'like' THEN outcome_r END), 2) AS liked_avg_r,
             round(avg(CASE WHEN feedback = 'dislike' THEN outcome_r END), 2) AS disliked_avg_r,
             round(avg(CASE WHEN feedback = 'like' THEN outcome_r END) - avg(CASE WHEN feedback = 'dislike' THEN outcome_r END), 2) AS edge_r,
             CAST(count(*) AS INT) AS rated,
             CAST(greatest(30 - count(*), 0) AS INT) AS more_needed
      FROM r
    SQL

    calls_matrix = <<-SQL
      WITH r AS (${local.rated_sql})
      SELECT CASE feedback WHEN 'like' THEN '👍 Liked' ELSE '👎 Disliked' END AS your_rating,
             concat(CAST(sum(won) AS STRING), ' · ',
                    ${replace(local.signed_rs_sql, "__V__", "coalesce(sum(CASE WHEN won = 1 THEN outcome_rs END), 0)")},
                    CASE feedback WHEN 'like' THEN ' · ✓ right' ELSE ' · ✕ wrong' END) AS it_won,
             concat(CAST(sum(1 - won) AS STRING), ' · ',
                    ${replace(local.signed_rs_sql, "__V__", "coalesce(sum(CASE WHEN won = 0 THEN outcome_rs END), 0)")},
                    CASE feedback WHEN 'like' THEN ' · ✕ wrong' ELSE ' · ✓ right' END) AS it_lost
      FROM r GROUP BY feedback
      ORDER BY feedback DESC
    SQL

    calls_vs_trader = <<-SQL
      WITH r AS (${local.rated_sql})
      SELECT CASE feedback WHEN 'like' THEN '👍 Liked' ELSE '👎 Disliked' END AS you,
             CASE WHEN decision = 'taken' THEN 'took it' ELSE 'skipped it' END AS trader,
             CASE WHEN (feedback = 'like') = (decision = 'taken') THEN 'agreed' ELSE 'disagreed' END AS together,
             CAST(count(*) AS INT) AS proposals, round(100 * avg(won), 0) AS won_pct,
             round(avg(outcome_r), 2) AS avg_r, round(sum(outcome_rs), 0) AS total_rs
      FROM r GROUP BY 1, 2, 3
      ORDER BY 1, 2
    SQL

    calls_weekly = <<-SQL
      WITH r AS (${local.rated_sql})
      SELECT CAST(date_trunc('WEEK', trade_date) AS DATE) AS week, 'Your calls right' AS series,
             round(100 * avg(right_call), 0) AS right_pct
      FROM r GROUP BY 1
      UNION ALL
      SELECT DISTINCT CAST(date_trunc('WEEK', trade_date) AS DATE) AS week, 'Coin flip' AS series, CAST(50 AS DOUBLE) AS right_pct
      FROM r
    SQL

    calls_by_setup = <<-SQL
      WITH r AS (${local.rated_sql})
      SELECT setup, round(100 * avg(right_call), 0) AS right_pct, CAST(count(*) AS INT) AS rated
      FROM r GROUP BY setup HAVING count(*) >= 3
      ORDER BY right_pct DESC
    SQL

    rating_summary = <<-SQL
      SELECT CASE rating WHEN 'like' THEN '👍 liked' WHEN 'dislike' THEN '👎 disliked' ELSE 'not rated' END AS rating,
             decision_group AS decisions, CAST(proposals AS INT) AS proposals, CAST(wins AS INT) AS wins, win_rate_pct,
             round(total_outcome_rs, 0) AS total_rs, round(avg_outcome_rs, 0) AS avg_rs, avg_outcome_r AS avg_r,
             CAST(right_calls AS INT) AS right_calls, right_pct
      FROM stocks.gold.rating_review
      ORDER BY CASE rating WHEN 'like' THEN 1 WHEN 'dislike' THEN 2 ELSE 3 END,
               CASE decision_group WHEN 'all' THEN 1 WHEN 'taken' THEN 2 ELSE 3 END
    SQL

    rating_trades = <<-SQL
      SELECT trade_date, date_format(proposed_at, 'HH:mm') AS time, symbol, setup, side,
             CASE feedback WHEN 'like' THEN '👍' WHEN 'dislike' THEN '👎' END AS you,
             CASE WHEN decision = 'taken' THEN 'took it' ELSE 'skipped it' END AS trader,
             suggested_yes_pct AS score_pct, round(outcome_rs, 0) AS outcome_rs, round(outcome_r, 2) AS outcome_r,
             CASE WHEN (feedback = 'like' AND won = 1) OR (feedback = 'dislike' AND won = 0) THEN '✓ right' ELSE '✕ wrong' END AS your_call
      FROM stocks.gold.proposal_outcomes
      WHERE feedback IS NOT NULL AND outcome_rs IS NOT NULL
      ORDER BY proposed_at DESC
      LIMIT 200
    SQL

    # --- Global markets: what matters for India's next open ---
    # "Today's read": short findings written from the tables with fixed rules (not AI), one row each.
    market_read = <<-SQL
      WITH g AS (SELECT * FROM stocks.gold.global_snapshot),
           sp AS (SELECT change_pct,
                         CASE WHEN change_pct >= 1 THEN 'US up big' WHEN change_pct <= -1 THEN 'US down big' ELSE 'US flat' END AS bucket
                  FROM g WHERE instrument = 'SP500' AND change_pct IS NOT NULL),
           us AS (SELECT sp.change_pct, sp.bucket, s.sessions, s.pct_gap_same_direction_as_us, s.avg_india_gap_pct
                  FROM sp JOIN stocks.gold.nifty_after_us_moves_summary s ON s.us_move = sp.bucket),
           br AS (SELECT pct_above_ma20, row_number() OVER (ORDER BY trade_date DESC) AS rn
                  FROM stocks.gold.market_breadth WHERE market = 'IN'),
           inr AS (SELECT value, row_number() OVER (ORDER BY obs_date DESC) AS rn
                   FROM (SELECT obs_date, value FROM g WHERE instrument = 'DEXINUS'
                         UNION ALL
                         SELECT obs_date, value FROM stocks.silver.global_daily
                         WHERE instrument = 'DEXINUS' AND value IS NOT NULL
                           AND obs_date < (SELECT obs_date FROM g WHERE instrument = 'DEXINUS'))),
           inr20 AS (SELECT max(CASE WHEN rn = 1 THEN value END) AS now, max(value) AS hi, min(value) AS lo FROM inr WHERE rn <= 20),
           mover AS (SELECT name, change_pct FROM g
                     WHERE asset_class IN ('equity_index', 'equity_etf', 'commodity') AND change_pct IS NOT NULL
                     ORDER BY abs(change_pct) DESC LIMIT 1),
           uv AS (SELECT count(*) AS n, max_by(symbol, volume_ratio) AS top, max(volume_ratio) AS top_x
                  FROM stocks.gold.unusual_volume
                  WHERE market = 'IN' AND trade_date = (SELECT max(trade_date) FROM stocks.gold.unusual_volume WHERE market = 'IN'))
      SELECT 1 AS n, 'US overnight' AS topic,
             format_string('The S&P 500 moved %+.1f%%. After US days like that (%s, %d of them), the Nifty opened the same way %.0f%% of the time, by %+.2f%% on average.',
                           change_pct, CASE bucket WHEN 'US up big' THEN 'up 1% or more' WHEN 'US down big' THEN 'down 1% or more' ELSE 'within 1%' END,
                           CAST(sessions AS INT), pct_gap_same_direction_as_us, avg_india_gap_pct) AS what_the_data_says
      FROM us
      UNION ALL
      SELECT 2, 'Under the surface',
             format_string('%.0f%% of NSE stocks closed above their 20-day average, %s from %.0f%% ten sessions earlier.',
                           max(CASE WHEN rn = 1 THEN pct_above_ma20 END),
                           CASE WHEN max(CASE WHEN rn = 1 THEN pct_above_ma20 END) < max(CASE WHEN rn = 11 THEN pct_above_ma20 END) THEN 'down' ELSE 'up' END,
                           max(CASE WHEN rn = 11 THEN pct_above_ma20 END))
      FROM br WHERE rn IN (1, 11) HAVING count(*) = 2
      UNION ALL
      SELECT 3, 'Rupee',
             CASE WHEN now >= hi THEN format_string('USD/INR is %.2f: the weakest rupee of the last 20 readings.', now)
                  WHEN now <= lo THEN format_string('USD/INR is %.2f: the strongest rupee of the last 20 readings.', now)
                  ELSE format_string('USD/INR is %.2f, inside its 20-reading range of %.2f to %.2f.', now, lo, hi) END
      FROM inr20 WHERE now IS NOT NULL
      UNION ALL
      SELECT 4, 'Biggest world move', format_string('%s moved %+.1f%%, the biggest move among the world markets we follow.', name, change_pct)
      FROM mover
      UNION ALL
      SELECT 5, 'Unusual volume',
             format_string('%d NSE stocks traded far above their usual volume; the biggest was %s at %.1f times its 20-day average.', CAST(n AS INT), top, top_x)
      FROM uv WHERE n > 0
      ORDER BY n
    SQL

    markets_kpis = <<-SQL
      SELECT max(CASE WHEN instrument = 'SP500' THEN round(change_pct, 2) END)        AS sp500_pct,
             max(CASE WHEN instrument = 'NASDAQCOM' THEN round(change_pct, 2) END)    AS nasdaq_pct,
             max(CASE WHEN instrument = 'NIKKEI225' THEN round(change_pct, 2) END)    AS nikkei_pct,
             max(CASE WHEN instrument = 'DEXINUS' THEN round(value, 2) END)           AS usd_inr,
             max(CASE WHEN instrument = 'DCOILBRENTEU' THEN round(value, 1) END)      AS brent_usd,
             max(CASE WHEN instrument = 'VIXCLS' THEN round(value, 1) END)            AS cboe_vix
      FROM stocks.gold.global_snapshot
    SQL

    world_moves = <<-SQL
      SELECT name AS market,
             CASE asset_class WHEN 'equity_index' THEN 'Index' WHEN 'equity_etf' THEN 'Country fund' WHEN 'fx' THEN 'Currency'
                              WHEN 'commodity' THEN 'Commodity' WHEN 'rates' THEN 'Rates' WHEN 'volatility' THEN 'Fear gauge'
                              ELSE asset_class END AS type,
             region,
             ${replace(local.signed_pct_sql, "__V__", "change_pct")} AS last_move,
             ${replace(local.signed_pct_sql, "__V__", "change_5d_pct")} AS over_5,
             ${replace(local.signed_pct_sql, "__V__", "change_20d_pct")} AS over_20,
             concat(date_format(obs_date, 'EEE d MMM'),
                    CASE source WHEN 'Yahoo live' THEN ' · live (Yahoo)' WHEN 'Yahoo' THEN ' (Yahoo)' ELSE '' END) AS as_of
      FROM stocks.gold.global_snapshot
      ORDER BY CASE asset_class WHEN 'equity_index' THEN 1 WHEN 'equity_etf' THEN 2 WHEN 'fx' THEN 3 WHEN 'commodity' THEN 4
                                WHEN 'rates' THEN 5 ELSE 6 END, region, name
    SQL

    us_after_moves = <<-SQL
      WITH sp AS (SELECT CASE WHEN change_pct >= 1 THEN 'US up big' WHEN change_pct <= -1 THEN 'US down big' ELSE 'US flat' END AS bucket
                  FROM stocks.gold.global_snapshot WHERE instrument = 'SP500')
      SELECT CASE us_move WHEN 'US down big' THEN 'US fell 1% or more' WHEN 'US up big' THEN 'US rose 1% or more' ELSE 'US within 1%' END AS us_day,
             CASE WHEN us_move IN (SELECT bucket FROM sp) THEN '◀ last night' ELSE '' END AS now,
             CAST(sessions AS INT) AS sessions,
             round(avg_india_gap_pct, 2) AS avg_nifty_open_pct, round(avg_india_return_pct, 2) AS avg_nifty_day_pct,
             round(pct_gap_same_direction_as_us, 0) AS opened_same_way_pct
      FROM stocks.gold.nifty_after_us_moves_summary
      ORDER BY CASE us_move WHEN 'US down big' THEN 1 WHEN 'US flat' THEN 2 ELSE 3 END
    SQL

    breadth_nse = <<-SQL
      SELECT trade_date, round(pct_above_ma20, 0) AS pct_above_20d
      FROM stocks.gold.market_breadth
      WHERE market = 'IN' AND trade_date >= date_sub(current_date(), 120)
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
  }

  # Shared SQL pieces (names end in _sql; scripts/check_sql.py expands them the same way Terraform does).
  # Signed values as text with an arrow: ▲ 1.4% / ▼ 0.3% / ▲ 0.6 pts / +₹7,740.
  signed_pct_sql = "CASE WHEN __V__ IS NULL THEN '–' WHEN __V__ > 0 THEN concat('▲ ', format_number(__V__, 1), '%') WHEN __V__ < 0 THEN concat('▼ ', format_number(-(__V__), 1), '%') ELSE '0.0%' END"
  signed_pts_sql = "CASE WHEN __V__ IS NULL THEN '–' WHEN __V__ > 0 THEN concat('▲ ', format_number(__V__, 1), ' pts') WHEN __V__ < 0 THEN concat('▼ ', format_number(-(__V__), 1), ' pts') ELSE 'level' END"
  signed_rs_sql  = "concat(CASE WHEN __V__ < 0 THEN '−₹' ELSE '+₹' END, format_number(abs(__V__), 0))"

  # Rated proposals with a known outcome, and whether your call was right.
  rated_sql = <<-SQL
    SELECT *, CASE WHEN (feedback = 'like' AND won = 1) OR (feedback = 'dislike' AND won = 0) THEN 1 ELSE 0 END AS right_call
    FROM stocks.gold.proposal_outcomes
    WHERE feedback IN ('like', 'dislike') AND won IS NOT NULL
  SQL

  # Your real trades replayed under other exit rules and the shadow models, since the ledger reset.
  intra_shadow_sql = <<-SQL
    SELECT trade_date, proposal_id, test, rule_label, would_have_made_rs, actual_rs, vs_actual_rs
    FROM stocks.gold.shadow_rules_trades
    WHERE (rule <> 'current' OR test = 'exit rules') -- "stop and target only": real trades since 20 Nov may exit early
      AND trade_date >= (SELECT min(trade_date) FROM stocks.gold.trading_daily)
  SQL

  # Every strategy (real pools and shadow pools) in one shape: label, group, start, latest return, NIFTYBEES over the
  # same days (from the pool's first snapshot to its latest), and the difference.
  strategy_label_sql = <<-SQL
    CASE pool WHEN 'intraday' THEN 'Intraday (real)' WHEN 'swing' THEN 'Swing (real, auto: momentum + news)'
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
         tr AS (SELECT pool, count(*) AS trades, count_if(side = 'BUY') AS buys, count_if(side = 'SELL') AS sells,
                       sum(coalesce(pnl_rs, 0)) AS realized_rs
                FROM stocks.silver.pool_trades GROUP BY pool),
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
           s.kind, s.start_date, s.last_date, round(l.equity, 0) AS value_rs, round(l.equity - l.capital, 0) AS pnl_rs,
           CAST(coalesce(t.trades, 0) AS INT) AS trades, CAST(coalesce(t.buys, 0) AS INT) AS buys,
           CAST(coalesce(t.sells, 0) AS INT) AS sells, round(coalesce(t.realized_rs, 0), 0) AS realized_rs,
           round((l.equity / l.capital - 1) * 100, 2) AS return_pct,
           round((n.nb_last / n.nb_start - 1) * 100, 2) AS niftybees_pct,
           round((l.equity / l.capital - 1) * 100 - (n.nb_last / n.nb_start - 1) * 100, 2) AS vs_niftybees_pct
    FROM span s JOIN latest l ON l.pool = s.pool LEFT JOIN nb_span n ON n.pool = s.pool LEFT JOIN tr t ON t.pool = s.pool
  SQL

  strategy_returns_sql = <<-SQL
    WITH d AS (${local.strategy_daily_sql})
    SELECT snapshot_date, ${local.strategy_label_sql} AS strategy, ${local.strategy_group_sql} AS grp, initcap(kind) AS kind,
           round((equity / capital - 1) * 100, 2) AS return_pct, round(equity - capital, 0) AS pnl_rs
    FROM d
    UNION ALL
    SELECT snapshot_date, 'NIFTYBEES (since the portfolio started)' AS strategy, 'Benchmark' AS grp, 'NIFTYBEES' AS kind,
           round(nifty_index - 100, 2) AS return_pct, CAST(NULL AS DOUBLE) AS pnl_rs
    FROM stocks.gold.portfolio_daily WHERE nifty_index IS NOT NULL
  SQL

  strategy_holdings_sql = <<-SQL
    WITH h AS (
      SELECT pool, 'Real' AS kind, symbol, sector, qty, avg_cost, last_price, value, pnl, opened, snapshot_date FROM stocks.silver.portfolio_holdings
      UNION ALL
      SELECT pool, 'Shadow' AS kind, symbol, sector, qty, avg_cost, last_price, value, pnl, opened, snapshot_date FROM stocks.silver.shadow_holdings)
    SELECT ${local.strategy_label_sql} AS strategy, kind, symbol, sector, CAST(qty AS INT) AS qty, round(avg_cost, 2) AS avg_cost,
           round(last_price, 2) AS last_price, round(value, 0) AS value_rs, round(pnl, 0) AS pnl_rs,
           round(100 * pnl / nullif(value - pnl, 0), 2) AS pnl_pct, opened
    FROM h
    WHERE pool IN (__POOLS__) AND snapshot_date = (SELECT max(snapshot_date) FROM h)
    ORDER BY strategy, value DESC
  SQL

  # One pool group (__GRP__ = Swing or Long-term): its scoreboard, with a NIFTYBEES row over the real pool's days.
  pool_board_sql = <<-SQL
    WITH s AS (SELECT * FROM (${local.strategy_scoreboard_sql}) WHERE grp = '__GRP__')
    SELECT strategy, initcap(kind) AS kind, start_date AS since, value_rs, pnl_rs, return_pct, trades,
           ${replace(local.signed_pts_sql, "__V__", "vs_niftybees_pct")} AS vs_niftybees
    FROM s
    UNION ALL
    SELECT 'NIFTYBEES (same days as the real pool)' AS strategy, 'NIFTYBEES' AS kind, start_date AS since,
           CAST(NULL AS DOUBLE) AS value_rs, CAST(NULL AS DOUBLE) AS pnl_rs, niftybees_pct AS return_pct,
           CAST(NULL AS INT) AS trades, '–' AS vs_niftybees
    FROM s WHERE kind = 'real'
    ORDER BY return_pct DESC
  SQL

  # One pool group's headline tiles: the real pool, the best shadow, NIFTYBEES, and how many shadows beat it.
  pool_kpis_sql = <<-SQL
    SELECT * EXCEPT (best_shadow_raw), concat(upper(substr(best_shadow_raw, 1, 1)), substr(best_shadow_raw, 2)) AS best_shadow
    FROM (
    WITH s AS (SELECT * FROM (${local.strategy_scoreboard_sql}) WHERE grp = '__GRP__')
    SELECT max(CASE WHEN kind = 'real' THEN value_rs END)        AS real_value_rs,
           max(CASE WHEN kind = 'real' THEN pnl_rs END)          AS real_pnl_rs,
           max(CASE WHEN kind = 'real' THEN return_pct END)      AS real_return_pct,
           max(CASE WHEN kind = 'real' THEN trades END)          AS real_trades,
           CAST(sum(CASE WHEN kind = 'shadow' THEN trades ELSE 0 END) AS INT) AS shadow_trades,
           max(CASE WHEN kind = 'shadow' THEN return_pct END)    AS best_shadow_pct,
           max_by(CASE WHEN kind = 'shadow' THEN regexp_replace(strategy, '^(Swing|Long-term) shadow: ', '') END,
                  CASE WHEN kind = 'shadow' THEN return_pct END) AS best_shadow_raw,
           max_by(CASE WHEN kind = 'shadow' THEN value_rs END, CASE WHEN kind = 'shadow' THEN return_pct END) AS best_shadow_value_rs,
           max_by(CASE WHEN kind = 'shadow' THEN pnl_rs END, CASE WHEN kind = 'shadow' THEN return_pct END) AS best_shadow_pnl_rs,
           max(CASE WHEN kind = 'real' THEN niftybees_pct END)   AS niftybees_pct,
           CAST(count_if(kind = 'shadow' AND vs_niftybees_pct > 0) AS INT) AS shadows_beating
    FROM s
    )
  SQL

  # One pool group's "best shadow vs NIFTYBEES" card: which shadow leads, its value, P&L and return, and NIFTYBEES.
  pool_best_sql = <<-SQL
    WITH k AS (${local.pool_kpis_sql})
    SELECT 'Best shadow' AS line, coalesce(best_shadow, 'none yet') AS strategy, best_shadow_value_rs AS value_rs,
           best_shadow_pnl_rs AS pnl_rs, ${replace(local.signed_pct_sql, "__V__", "best_shadow_pct")} AS return_pct
    FROM k
    UNION ALL
    SELECT 'Bar to beat', 'NIFTYBEES, same days', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
           ${replace(local.signed_pct_sql, "__V__", "niftybees_pct")}
    FROM k
  SQL

  # One pool group's buys and sells (last 30 days), real and shadow; newest first.
  pool_trades_sql = <<-SQL
    SELECT trade_date, ${local.strategy_label_sql} AS strategy, initcap(kind) AS kind, side, symbol,
           CAST(qty AS INT) AS qty, round(price, 2) AS price, round(value_rs, 0) AS value_rs,
           round(pnl_rs, 0) AS pnl_rs, reason
    FROM stocks.silver.pool_trades
    WHERE ${local.strategy_group_sql} = '__GRP__' AND trade_date >= date_sub(current_date(), 30)
    ORDER BY trade_ts DESC
  SQL

  # Helpers to keep widget definitions short.
  raw_fields = {
    for ds, cols in {
      desk_status          = ["auto_mode", "india_vix", "mood", "risk", "as_of"]
      desk_kpis            = ["portfolio_rs", "since_reset_pct", "today_pnl_rs", "trades_today", "proposals", "calls_right_pct"]
      desk_setups          = ["setup", "win_rate", "avg_r", "proposals"]
      desk_decisions       = ["time", "symbol", "setup", "score", "reviewer", "decision", "you", "result"]
      desk_board           = ["strategy", "kind", "return_since_start", "vs_niftybees"]
      desk_pools           = ["pool", "value_rs", "pnl_rs", "return_pct", "since", "as_of"]
      portfolio_vs_nifty   = ["snapshot_date", "series", "index_value"]
      portfolio_drawdown   = ["snapshot_date", "drawdown_pct"]
      allocation_pool      = ["pool", "value_rs"]
      holdings             = ["pool", "symbol", "sector", "qty", "avg_cost", "last_price", "value_rs", "pnl_rs", "pnl_pct", "weight_pct", "days_held"]
      summary_overview     = ["in_plain_words"]
      summary_portfolio    = ["in_plain_words"]
      summary_intraday     = ["in_plain_words"]
      summary_quality      = ["in_plain_words"]
      intraday_kpis        = ["trades", "net_pnl_rs", "win_rate_pct", "profit_factor", "expectancy_r", "win_loss_ratio"]
      intra_lines          = ["trade_date", "line", "kind", "total_rs"]
      intra_board          = ["rule", "kind", "trades", "total_rs", "vs_real_rs", "beat_real_pct"]
      intra_bar_ladder     = ["version", "trades", "total_rs", "won_pct"]
      intra_fill_check     = ["trade_date", "symbol", "side", "paper_worse_bps", "inside_minute_range"]
      intra_selector       = ["family", "days", "selector_rs", "fixed_rs", "latest_pick"]
      intraday_curve       = ["trade_date", "total_pnl_rs", "below_peak_rs"]
      intraday_daily_pnl   = ["trade_date", "day_pnl_rs", "result"]
      intraday_setup_chart = ["setup", "net_pnl_rs", "win_rate_pct"]
      intraday_hour_chart  = ["entry_hour", "net_pnl_rs"]
      trading_recent       = ["trade_date", "symbol", "side", "setup", "qty", "entry_price", "exit_price", "exit_reason", "net_pnl_rs", "r_multiple", "holding_minutes"]
      backtest_latest      = ["rule", "trades", "total_r", "vs_current_r", "avg_r", "win_pct", "days_up_pct", "worst_day_r", "days", "run_date"]
      backtest_trend       = ["run_date", "rule", "vs_current_r"]
      shadow_trades_recent = ["trade_date", "symbol", "side", "setup", "test", "rule", "actual_rs", "would_have_made_rs", "vs_actual_rs"]
      swing_kpis           = ["real_value_rs", "real_pnl_rs", "real_return_pct", "real_trades", "shadow_trades", "best_shadow", "best_shadow_value_rs", "best_shadow_pnl_rs", "best_shadow_pct", "niftybees_pct", "shadows_beating"]
      longterm_kpis        = ["real_value_rs", "real_pnl_rs", "real_return_pct", "real_trades", "shadow_trades", "best_shadow", "best_shadow_value_rs", "best_shadow_pnl_rs", "best_shadow_pct", "niftybees_pct", "shadows_beating"]
      swing_scoreboard     = ["strategy", "kind", "since", "value_rs", "pnl_rs", "return_pct", "trades", "vs_niftybees"]
      longterm_scoreboard  = ["strategy", "kind", "since", "value_rs", "pnl_rs", "return_pct", "trades", "vs_niftybees"]
      swing_best           = ["line", "strategy", "value_rs", "pnl_rs", "return_pct"]
      longterm_best        = ["line", "strategy", "value_rs", "pnl_rs", "return_pct"]
      swing_trades         = ["trade_date", "strategy", "kind", "side", "symbol", "qty", "price", "value_rs", "pnl_rs", "reason"]
      longterm_trades      = ["trade_date", "strategy", "kind", "side", "symbol", "qty", "price", "value_rs", "pnl_rs", "reason"]
      swing_returns        = ["snapshot_date", "strategy", "kind", "return_pct", "pnl_rs"]
      longterm_returns     = ["snapshot_date", "strategy", "kind", "return_pct", "pnl_rs"]
      swing_holdings       = ["strategy", "kind", "symbol", "sector", "qty", "avg_cost", "last_price", "value_rs", "pnl_rs", "pnl_pct", "opened"]
      longterm_holdings    = ["strategy", "kind", "symbol", "sector", "qty", "avg_cost", "last_price", "value_rs", "pnl_rs", "pnl_pct", "opened"]
      scanner_today        = ["list_type", "rank", "symbol", "company", "close", "setup", "stance", "rsi", "adx", "volume_x", "delivery_pct", "fno", "entry_low", "entry_high", "stop", "target2", "rr", "misses", "trade_date"]
      momentum_top         = ["swing_rank", "lt_rank", "symbol", "company", "industry", "close", "ret_1m_pct", "ret_3m_pct", "ret_6m_pct", "ret_12m_pct", "vol_1y_pct", "pct_of_52w_high", "rsi14", "good_news_5d", "latest_good_news", "scanner_strict", "swing_score", "lt_score", "trade_date"]
      scanner_record       = ["signal", "days_after", "signals", "avg_return_pct", "vs_niftybees_pct", "beat_niftybees_pct"]
      swing_suggest        = ["strategy", "rank", "symbol", "company", "close", "entry", "stop", "target", "rr", "why", "chart", "trade_date"]
      longterm_suggest     = ["strategy", "rank", "symbol", "company", "close", "why", "chart"]
      usdinr_history       = ["obs_date", "usd_inr"]
      longterm_picks       = ["rank", "pick", "symbol", "company", "industry", "score", "quality_score", "target_weight_pct", "return_on_capital_pct", "debt_to_equity", "profit_cagr_3y_pct", "pe", "reasons"]
      quality_scatter      = ["symbol", "roce_pct", "pe", "tests"]
      calls_kpis           = ["calls_right_pct", "liked_avg_r", "disliked_avg_r", "edge_r", "rated", "more_needed"]
      calls_matrix         = ["your_rating", "it_won", "it_lost"]
      calls_vs_trader      = ["you", "trader", "together", "proposals", "won_pct", "avg_r", "total_rs"]
      calls_weekly         = ["week", "series", "right_pct"]
      calls_by_setup       = ["setup", "right_pct", "rated"]
      rating_summary       = ["rating", "decisions", "proposals", "wins", "win_rate_pct", "total_rs", "avg_rs", "avg_r", "right_calls", "right_pct"]
      rating_trades        = ["trade_date", "time", "symbol", "setup", "side", "you", "trader", "score_pct", "outcome_rs", "outcome_r", "your_call"]
      market_read          = ["topic", "what_the_data_says"]
      markets_kpis         = ["sp500_pct", "nasdaq_pct", "nikkei_pct", "usd_inr", "brent_usd", "cboe_vix"]
      world_moves          = ["market", "type", "region", "last_move", "over_5", "over_20", "as_of"]
      us_after_moves       = ["us_day", "now", "sessions", "avg_nifty_open_pct", "avg_nifty_day_pct", "opened_same_way_pct"]
      breadth_nse          = ["trade_date", "pct_above_20d"]
      unusual_latest       = ["market", "symbol", "trade_date", "volume_ratio", "return_pct", "close", "currency"]
      adr_latest           = ["adr", "nse_symbol", "trade_date", "adr_close_usd", "nse_close_inr", "implied_inr_per_usd", "relative_premium_pct", "fx_inr_per_usd", "fx_premium_pct"]
      global_latest        = ["region", "name", "instrument", "asset_class", "as_of", "value", "change_pct", "change_5d_pct", "change_20d_pct", "source"]
      india_vs_global      = ["name", "instrument", "region", "asset_class", "sessions", "corr_with_india_gap", "corr_with_india_return", "pct_same_direction_gap"]
    } : ds => [for c in cols : { name = c, expression = "`${c}`" }]
  }

  table_columns = {
    for ds, fields in local.raw_fields : ds => [for f in fields : { fieldName = f.name, displayName = replace(f.name, "_", " ") }]
  }

  # Headline tiles: one counter per field of a one-row dataset.
  counter = {
    for k, c in {
      desk_value     = { ds = "desk_kpis", field = "portfolio_rs", title = "Dummy portfolio (Rs)", desc = "All three real pools, today", pos = [0, 3] }
      desk_return    = { ds = "desk_kpis", field = "since_reset_pct", title = "Since 18 Nov (%)", desc = "Return since the ledger reset", pos = [2, 3] }
      desk_today     = { ds = "desk_kpis", field = "today_pnl_rs", title = "Today's P&L (Rs)", desc = "All pools, latest day", pos = [4, 3] }
      desk_trades    = { ds = "desk_kpis", field = "trades_today", title = "Trades, latest day", desc = "Auto mode takes at most 6 a day", pos = [6, 3] }
      desk_proposals = { ds = "desk_kpis", field = "proposals", title = "Proposals evaluated", desc = "Since 18 Nov: taken and skipped, judged on what happened", pos = [8, 3] }
      desk_calls     = { ds = "desk_kpis", field = "calls_right_pct", title = "Your calls right (%)", desc = "Your 👍/👎 vs what happened (never trains the trader)", pos = [10, 3] }
      intra_pnl      = { ds = "intraday_kpis", field = "net_pnl_rs", title = "P&L since 18 Nov (Rs)", desc = "After costs", pos = [0, 3] }
      intra_trades   = { ds = "intraday_kpis", field = "trades", title = "Trades", desc = "Closed since 18 Nov", pos = [2, 3] }
      intra_win      = { ds = "intraday_kpis", field = "win_rate_pct", title = "Win rate (%)", desc = "Read it with the win/loss size", pos = [4, 3] }
      intra_r        = { ds = "intraday_kpis", field = "expectancy_r", title = "Average R per trade", desc = "Above 0 = an edge after costs", pos = [6, 3] }
      intra_pf       = { ds = "intraday_kpis", field = "profit_factor", title = "Profit factor", desc = "Gains / losses; above 1 makes money", pos = [8, 3] }
      intra_wl       = { ds = "intraday_kpis", field = "win_loss_ratio", title = "Average win / average loss", desc = "How big wins are next to losses", pos = [10, 3] }
      swing_real     = { ds = "swing_kpis", field = "real_value_rs", title = "Real pool (Rs)", desc = "Auto mode: momentum + news; idle cash in NIFTYBEES", pos = [0, 3] }
      swing_pnl      = { ds = "swing_kpis", field = "real_pnl_rs", title = "Real P&L (Rs)", desc = "Value minus starting capital, after costs", pos = [2, 3] }
      swing_return   = { ds = "swing_kpis", field = "real_return_pct", title = "Real pool return (%)", desc = "Since 18 Nov", pos = [4, 3] }
      swing_trades   = { ds = "swing_kpis", field = "real_trades", title = "Trades placed (real)", desc = "Buys and sells since 18 Nov; shadows: scoreboard", pos = [6, 3] }
      lt_real        = { ds = "longterm_kpis", field = "real_value_rs", title = "Real pool (Rs)", desc = "Cash until the 18 Dec go/no-go review", pos = [0, 3] }
      lt_pnl         = { ds = "longterm_kpis", field = "real_pnl_rs", title = "Real P&L (Rs)", desc = "Value minus starting capital, after costs", pos = [2, 3] }
      lt_return      = { ds = "longterm_kpis", field = "real_return_pct", title = "Real pool return (%)", desc = "Since 18 Nov", pos = [4, 3] }
      lt_trades      = { ds = "longterm_kpis", field = "real_trades", title = "Trades placed (real)", desc = "Buys and sells since 18 Nov; shadows: scoreboard", pos = [6, 3] }
      calls_right    = { ds = "calls_kpis", field = "calls_right_pct", title = "Your calls right (%)", desc = "A coin flip gets 50", pos = [0, 3] }
      calls_liked    = { ds = "calls_kpis", field = "liked_avg_r", title = "When you liked it (avg R)", desc = "Average result per proposal", pos = [2, 3] }
      calls_disliked = { ds = "calls_kpis", field = "disliked_avg_r", title = "When you disliked it (avg R)", desc = "Average result per proposal", pos = [4, 3] }
      calls_edge     = { ds = "calls_kpis", field = "edge_r", title = "Your edge (R)", desc = "Liked minus disliked", pos = [6, 3] }
      calls_rated    = { ds = "calls_kpis", field = "rated", title = "Rated so far", desc = "With a known outcome", pos = [8, 3] }
      calls_needed   = { ds = "calls_kpis", field = "more_needed", title = "More needed to judge", desc = "Trust it from 30 rated", pos = [10, 3] }
      markets_sp500  = { ds = "markets_kpis", field = "sp500_pct", title = "S&P 500, last day (%)", desc = "US close before India's open", pos = [0, 7] }
      markets_nasdaq = { ds = "markets_kpis", field = "nasdaq_pct", title = "Nasdaq, last day (%)", desc = "Tech: leads Indian IT", pos = [2, 7] }
      markets_nikkei = { ds = "markets_kpis", field = "nikkei_pct", title = "Nikkei 225, last day (%)", desc = "Asia trades before India", pos = [4, 7] }
      markets_usdinr = { ds = "markets_kpis", field = "usd_inr", title = "USD/INR", desc = "Higher = weaker rupee", pos = [6, 7] }
      markets_brent  = { ds = "markets_kpis", field = "brent_usd", title = "Brent crude (USD)", desc = "India imports most of its oil", pos = [8, 7] }
      markets_vix    = { ds = "markets_kpis", field = "cboe_vix", title = "CBOE VIX", desc = "US fear gauge; above 20 = nervous", pos = [10, 7] }
      } : k => {
      widget = {
        name = k
        queries = [{
          name  = "main_query"
          query = { datasetName = c.ds, disaggregated = true, fields = [{ name = c.field, expression = "`${c.field}`" }] }
        }]
        spec = {
          version    = 2
          widgetType = "counter"
          encodings  = { value = { fieldName = c.field, displayName = c.title } }
          frame      = { showTitle = true, title = c.title, showDescription = true, description = c.desc }
        }
      }
      position = { x = c.pos[0], y = c.pos[1], width = try(c.w, 2), height = 3 }
    }
  }

  # The "Show" filters: one multi-select on `kind`, bound to every dataset listed (nothing picked = everything shown).
  show_filter = {
    for k, f in {
      intra_show = { datasets = ["intra_lines", "intra_board"], desc = "Real · Exit rules · Models · Opening-range test. Pick any; none picked = all." }
      swing_show = { datasets = ["swing_returns", "swing_scoreboard", "swing_holdings", "swing_trades"], desc = "Real · Shadow · NIFTYBEES. Pick any; none picked = all." }
      lt_show    = { datasets = ["longterm_returns", "longterm_scoreboard", "longterm_holdings", "longterm_trades"], desc = "Real · Shadow · NIFTYBEES. Pick any; none picked = all." }
      } : k => {
      widget = {
        name = k
        queries = [for ds in f.datasets : {
          name  = "${k}_${ds}"
          query = { datasetName = ds, disaggregated = false, fields = [{ name = "kind", expression = "`kind`" }] }
        }]
        spec = {
          version    = 2
          widgetType = "filter-multi-select"
          encodings  = { fields = [for ds in f.datasets : { fieldName = "kind", displayName = "Show", queryName = "${k}_${ds}" }] }
          frame      = { showTitle = true, title = "Show", showDescription = true, description = f.desc }
        }
      }
      position = { x = 8, y = 0, width = 4, height = 3 }
    }
  }

  # Chart widget: dataset, type, x/y (and optional color) fields, title, position.
  chart = {
    for k, c in {
      port_vs_nifty  = { ds = "portfolio_vs_nifty", type = "line", x = "snapshot_date", xt = "temporal", y = "index_value", color = "series", title = "Portfolio vs NIFTYBEES (both start at 100)", desc = "Above the NIFTYBEES line = beating simply holding the index.", pos = [0, 6, 7, 7] }
      port_alloc     = { ds = "allocation_pool", type = "bar", x = "pool", xt = "categorical", y = "value_rs", color = null, title = "Where the money is (Rs)", desc = "Value of each real pool today.", pos = [6, 24, 3, 6] }
      port_drawdown  = { ds = "portfolio_drawdown", type = "area", x = "snapshot_date", xt = "temporal", y = "drawdown_pct", color = null, title = "Below the best value so far (%)", desc = "0 = at its best; -5 = 5% below. Shallow, short dips are what you want.", pos = [9, 24, 3, 6] }
      intra_all      = { ds = "intra_lines", type = "line", x = "trade_date", xt = "temporal", y = "total_rs", color = "line", title = "Total since 18 Nov: real vs shadow (Rs)", desc = "Every version: exit rules (incl. the fixed 2R target, the trailing stop and the early exits) and models replay your real trades, so above the 'Real' line = it would have made more on the same trades. The opening-range test is separate paper trades. Use Show to pick lines.", pos = [0, 6, 12, 7] }
      intra_daily    = { ds = "intraday_daily_pnl", type = "bar", x = "trade_date", xt = "temporal", y = "day_pnl_rs", color = "result", title = "P&L each day (Rs)", desc = "One bar per trading day.", pos = [6, 13, 6, 7] }
      intra_setup    = { ds = "intraday_setup_chart", type = "bar", x = "setup", xt = "categorical", y = "net_pnl_rs", color = null, title = "By setup (Rs)", desc = "Which kinds of trade made or lost money.", pos = [0, 20, 4, 5] }
      intra_hour     = { ds = "intraday_hour_chart", type = "bar", x = "entry_hour", xt = "categorical", y = "net_pnl_rs", color = null, title = "By entry hour, IST (Rs)", desc = "Which entry times made or lost money.", pos = [4, 20, 4, 5] }
      intra_drawdown = { ds = "intraday_curve", type = "area", x = "trade_date", xt = "temporal", y = "below_peak_rs", color = null, title = "Below the best total so far (Rs)", desc = "Deep or long dips mean risky trading.", pos = [8, 20, 4, 5] }
      intra_backtest = { ds = "backtest_latest", type = "bar", x = "rule", xt = "categorical", y = "vs_current_r", color = null, title = "Backtest on ~200 stocks: each rule vs current rules (R)", desc = "Latest walk-forward run. R = one unit of risk, about Rs 400. Above 0 = better than the current rules.", pos = [0, 33, 6, 6] }
      intra_bt_trend = { ds = "backtest_trend", type = "line", x = "run_date", xt = "temporal", y = "vs_current_r", color = "rule", title = "Is each edge holding up? (R vs current, by run day)", desc = "A line that stays above 0 run after run is a rule worth reviewing for go-live.", pos = [6, 33, 6, 6] }
      swing_curve    = { ds = "swing_returns", type = "line", x = "snapshot_date", xt = "temporal", y = "return_pct", color = "strategy", title = "Return since each started (%)", desc = "Real pool, the four shadows (news breakouts, scanner, momentum + news, momentum top 3) and NIFTYBEES. Use Show to pick.", pos = [0, 6, 6, 7] }
      lt_curve       = { ds = "longterm_returns", type = "line", x = "snapshot_date", xt = "temporal", y = "return_pct", color = "strategy", title = "Return since each started (%)", desc = "Real pool, the three shadows (quality picks, momentum top 15, momentum top 5) and NIFTYBEES. Use Show to pick.", pos = [0, 6, 6, 7] }
      swing_pnl_rs   = { ds = "swing_returns", type = "line", x = "snapshot_date", xt = "temporal", y = "pnl_rs", color = "strategy", title = "Profit / loss since each started (Rs)", desc = "Same lines in rupees: value minus starting cash, after costs. Use Show to pick.", pos = [6, 6, 6, 7] }
      lt_pnl_rs      = { ds = "longterm_returns", type = "line", x = "snapshot_date", xt = "temporal", y = "pnl_rs", color = "strategy", title = "Profit / loss since each started (Rs)", desc = "Same lines in rupees: value minus starting cash, after costs. Use Show to pick.", pos = [6, 6, 6, 7] }
      mk_usdinr      = { ds = "usdinr_history", type = "line", x = "obs_date", xt = "temporal", y = "usd_inr", color = null, title = "USD/INR, past 5 years (rupees per dollar)", desc = "FRED daily reference rate (published weekly, so the last few days can lag). Up = weaker rupee.", pos = [0, 34, 12, 6] }
      quality_dots   = { ds = "quality_scatter", type = "scatter", x = "roce_pct", xt = "quantitative", y = "pe", color = "tests", title = "Quality vs price: return on capital (%) against P/E (excl. banks)", desc = "Further right = more profitable; lower = cheaper. Bottom right is the sweet spot.", pos = [0, 44, 12, 7] }
      calls_trend    = { ds = "calls_weekly", type = "line", x = "week", xt = "temporal", y = "right_pct", color = "series", title = "Calls right, week by week (%)", desc = "Above the coin-flip line = your instinct adds something.", pos = [0, 11, 7, 6] }
      calls_setup    = { ds = "calls_by_setup", type = "bar", x = "setup", xt = "categorical", y = "right_pct", color = null, title = "Where your instinct is strongest (% right)", desc = "Setups with 3 or more rated proposals. 50 = a coin flip.", pos = [7, 11, 5, 6] }
      mk_follow      = { ds = "india_vs_global", type = "bar", x = "name", xt = "categorical", y = "corr_with_india_gap", color = "region", title = "Who India's open follows (correlation)", desc = "How closely each market's last move matched the Nifty's next open. Near +1 = same way; 0 = no link; below 0 = opposite.", pos = [7, 10, 5, 12] }
      mk_breadth     = { ds = "breadth_nse", type = "line", x = "trade_date", xt = "temporal", y = "pct_above_20d", color = null, title = "NSE breadth: stocks above their 20-day average (%)", desc = "Falling while the index holds up = weakness under the surface.", pos = [6, 22, 6, 6] }
      mk_adr         = { ds = "adr_latest", type = "bar", x = "adr", xt = "categorical", y = "relative_premium_pct", color = null, title = "Indian ADRs in the US vs their NSE price (%)", desc = "A big premium or discount often closes at the next NSE open.", pos = [6, 28, 6, 6] }
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
            for k, v in tomap({
              x     = { fieldName = c.x, scale = { type = c.xt }, displayName = replace(c.x, "_", " ") }
              y     = { fieldName = c.y, scale = { type = "quantitative" }, displayName = replace(c.y, "_", " ") }
              color = { fieldName = c.color, scale = { type = "categorical" }, displayName = replace(coalesce(c.color, "-"), "_", " ") }
            }) : k => v if v.fieldName != null
          }
          frame = { showTitle = true, title = c.title, showDescription = true, description = c.desc }
        }
      }
      position = { x = c.pos[0], y = c.pos[1], width = c.pos[2], height = c.pos[3] }
    }
  }

  table_widget = {
    for k, t in {
      desk_status       = { ds = "desk_status", title = "Today", desc = "Auto mode, India VIX and risk from the trader's export; mood from its latest proposal.", pos = [8, 0, 4, 3] }
      desk_setups       = { ds = "desk_setups", title = "Track record by setup (last 30 days)", desc = "Every proposal, taken or skipped, judged on what really happened. Under 10 is too few to judge.", pos = [7, 6, 5, 7] }
      desk_pools        = { ds = "desk_pools", title = "Each pool: value and P&L since 18 Nov", desc = "Real pools only (dummy money). P&L = value now minus starting cash, after costs.", pos = [0, 13, 12, 3] }
      desk_decisions    = { ds = "desk_decisions", title = "Today's decisions", desc = "Score gate, the independent reviewer, what happened, your rating and the result (R = one unit of risk).", pos = [0, 16, 7, 7] }
      desk_board        = { ds = "desk_board", title = "Real and shadow pools", desc = "Return since each started, and vs NIFTYBEES over the same days.", pos = [7, 16, 5, 7] }
      holdings          = { ds = "holdings", title = "What the real pools hold", desc = "Largest first. Weight = share of the whole portfolio.", pos = [0, 24, 6, 6] }
      summary_portfolio = { ds = "summary_portfolio", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 30, 12, 4] }
      intra_board       = { ds = "intra_board", title = "Real vs every shadow, same trades (since 18 Nov)", desc = "Total = what each would have made. Beat real = how often it did better trade by trade.", pos = [0, 13, 6, 7] }
      intra_recent      = { ds = "trading_recent", title = "Latest trades", desc = "Since 18 Nov, newest first.", pos = [0, 25, 12, 7] }
      intra_bt_table    = { ds = "backtest_latest", title = "Backtest details (latest run)", desc = "All rules on the same setups. Worst day = the worst single day's total.", pos = [0, 39, 12, 6] }
      intra_shadow_list = { ds = "shadow_trades_recent", title = "Your trades under other rules (last 14 days)", desc = "Every real trade, what happened, and what each rule or model would have made.", pos = [0, 45, 12, 6] }
      intra_ladder      = { ds = "intra_bar_ladder", title = "Auto bar and sizing, on every proposal (since 18 Nov)", desc = "Placed trades count as they happened; skipped ones as they would have. The live bar is 55% (+5 for losing groups, 60% to flip a stock's direction).", pos = [0, 55, 6, 6] }
      intra_fills       = { ds = "intra_fill_check", title = "Paper fills vs the real minute", desc = "Positive bps = the paper fill was worse than that minute's close (stricter than the market).", pos = [6, 55, 6, 6] }
      intra_select      = { ds = "intra_selector", title = "Selector (paper): picks tomorrow's version from the last 20 days", desc = "Each day scored on results it couldn't see when it picked. Beats the fixed rules for weeks = worth a review; until then it is luck.", pos = [0, 61, 12, 5] }
      summary_intraday  = { ds = "summary_intraday", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 51, 12, 4] }
      swing_board       = { ds = "swing_scoreboard", title = "Scoreboard", desc = "Return since each started; vs NIFTYBEES over the same days.", pos = [0, 20, 6, 7] }
      swing_hold        = { ds = "swing_holdings", title = "What each one holds now", desc = "Real pool and shadow pools (paper).", pos = [6, 20, 6, 7] }
      swing_best_card   = { ds = "swing_best", title = "Best shadow vs the bar to beat", desc = "Which shadow leads, its value and P&L, and NIFTYBEES over the real pool's days.", pos = [8, 3, 4, 3] }
      swing_trades      = { ds = "swing_trades", title = "Trades placed (last 30 days)", desc = "Every buy and sell, real pool and shadows, after costs. P&L is realized on sells.", pos = [0, 27, 12, 7] }
      swing_suggest     = { ds = "swing_suggest", title = "Suggested today (information only)", desc = "What the auto rules would buy next and the scanner's strict picks, with levels. Nothing is placed from this list; chart = TradingView. Not advice.", pos = [0, 13, 12, 7] }
      lt_suggest        = { ds = "longterm_suggest", title = "Suggested today (information only)", desc = "This week's quality picks and the momentum top 15. Nothing is placed from this list; chart = TradingView. Not advice.", pos = [0, 13, 12, 7] }
      swing_scan        = { ds = "scanner_today", title = "Today's technical scanner (Nifty 200)", desc = "Strict picks and near-misses. Indicative levels, never a guaranteed return.", pos = [0, 34, 12, 7] }
      swing_mom         = { ds = "momentum_top", title = "Momentum ranks (Nifty 200)", desc = "What the momentum shadows buy from: swing top 12 and long-term top 30. Past strength only.", pos = [0, 41, 12, 8] }
      swing_signals     = { ds = "scanner_record", title = "How each scanner signal did afterwards", desc = "Average return 5, 10 and 20 trading days later, and how often it beat NIFTYBEES. Past results only.", pos = [0, 49, 12, 7] }
      lt_board          = { ds = "longterm_scoreboard", title = "Scoreboard", desc = "Return since each started; vs NIFTYBEES over the same days.", pos = [0, 20, 6, 7] }
      lt_hold           = { ds = "longterm_holdings", title = "What each one holds now", desc = "Real pool and shadow pools (paper).", pos = [6, 20, 6, 7] }
      lt_best_card      = { ds = "longterm_best", title = "Best shadow vs the bar to beat", desc = "Which shadow leads, its value and P&L, and NIFTYBEES over the real pool's days.", pos = [8, 3, 4, 3] }
      lt_trades         = { ds = "longterm_trades", title = "Trades placed (last 30 days)", desc = "Every buy and sell, real pool and shadows, after costs. P&L is realized on sells.", pos = [0, 27, 12, 7] }
      quality_top       = { ds = "longterm_picks", title = "This week's quality picks (max 2 per industry) and the next 20", desc = "Eligible Nifty 200 companies; score = 100 x quality + growth bonus - price penalty. Shadow mode: nothing real is bought yet.", pos = [0, 34, 12, 10] }
      summary_quality   = { ds = "summary_quality", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 51, 12, 4] }
      calls_matrix      = { ds = "calls_matrix", title = "Your rating vs what happened", desc = "Count · total Rs · whether your call was right. Skipped proposals count by what they would have made.", pos = [0, 6, 5, 5] }
      calls_vs_trader   = { ds = "calls_vs_trader", title = "You vs the trader", desc = "Where you disagreed shows what following you would have changed (ratings still never train it).", pos = [5, 6, 7, 5] }
      calls_list        = { ds = "rating_trades", title = "Every rated proposal (newest first)", desc = "Your rating, what the trader did, the outcome and whether your call was right.", pos = [0, 17, 12, 9] }
      calls_summary     = { ds = "rating_summary", title = "By decision", desc = "Taken = the real result; skipped = what it would have made.", pos = [0, 26, 12, 6] }
      mk_read           = { ds = "market_read", title = "Today's read", desc = "Written from the tables below with fixed rules (not AI), each evening and again by about 09:00 IST from the pre-open snapshot. Past patterns, not predictions.", pos = [0, 2, 12, 5] }
      mk_world          = { ds = "world_moves", title = "Around the world", desc = "Last move, and over the last 5 and 20 sessions. Newest reading wins: the 08:45 IST Yahoo snapshot or FRED/Massive; live = that market is still trading.", pos = [0, 10, 7, 12] }
      mk_us_after       = { ds = "us_after_moves", title = "What the Nifty did after the US moved", desc = "Since 2023. The open follows the US more than the close does.", pos = [0, 22, 6, 6] }
      mk_unusual        = { ds = "unusual_latest", title = "Unusual volume, latest session", desc = "Stocks trading far above their 20-day average volume.", pos = [0, 28, 6, 6] }
      details_global    = { ds = "global_latest", title = "Every global instrument: latest reading", desc = "FRED series, US-listed country ETFs and the 08:45 IST Yahoo snapshot (source column).", pos = [0, 41, 12, 8] }
      details_ivg       = { ds = "india_vs_global", title = "India's next open vs global moves", desc = "Correlation over all sessions; same direction = how often the gap matched the global move.", pos = [0, 49, 12, 7] }
      details_adr       = { ds = "adr_latest", title = "Indian ADRs vs NSE shares", desc = "Premium vs the pair's median, and vs USD/INR.", pos = [0, 56, 12, 5] }
      summary_overview  = { ds = "summary_overview", title = "In plain words", desc = "Written from the latest data after every refresh (Databricks AI). Not advice.", pos = [0, 61, 12, 4] }
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

  # Page titles and section headings.
  text = {
    for k, t in {
      desk_title     = { lines = ["## Paper Trader · Desk", "Whole portfolio, dummy money, since the 18 Nov reset. Updated after 15:45 and 23:30 IST. Not advice."], pos = [0, 0, 8, 3] }
      desk_details   = { lines = ["### Details"], pos = [0, 23, 12, 1] }
      intra_title    = { lines = ["## Intraday", "Real pool Rs 40,000 since 18 Nov. Shadow lines replay your real trades under other exit rules and the shadow models; the opening-range test trades on paper."], pos = [0, 0, 8, 3] }
      intra_tests    = { lines = ["### Model tests on ~200 stocks (paper)"], pos = [0, 32, 12, 1] }
      swing_title    = { lines = ["## Swing", "Real pool Rs 60,000 in auto mode from 24 Nov (momentum + news rules; idle cash in NIFTYBEES) vs four shadow strategies on paper, same costs. Not advice."], pos = [0, 0, 8, 3] }
      lt_title       = { lines = ["## Long term", "Real pool Rs 1 lakh in cash until the 18 Dec go/no-go review, vs three shadow strategies on paper. Company quality refreshed every Saturday. Not advice."], pos = [0, 0, 8, 3] }
      calls_title    = { lines = ["## Your calls", "Your 👍/👎 against what really happened, for your own learning: ratings never train the trader. All history; under 30 rated is too early to judge."], pos = [0, 0, 12, 3] }
      markets_title  = { lines = ["## Global markets: what matters for India's next open", "US, Asia, Europe, the rupee, crude, yields and the fear gauge, plus what the NSE did under the surface."], pos = [0, 0, 12, 2] }
      markets_tables = { lines = ["### Full tables"], pos = [0, 40, 12, 1] }
      } : k => {
      # one string with real line breaks: Databricks joins a text box's lines without a break
      widget   = { name = k, multilineTextboxSpec = { lines = [join("\n", t.lines)] } }
      position = { x = t.pos[0], y = t.pos[1], width = t.pos[2], height = t.pos[3] }
    }
  }

  dashboard = {
    datasets = [
      for name, sql in local.dashboard_datasets : {
        name        = name
        displayName = replace(name, "_", " ")
        # one entry per line, as Databricks stores it, so plans don't show a change that isn't there
        queryLines = [for line in split("\n", trimsuffix(sql, "\n")) : "${line}\n"]
      }
    ]

    pages = [
      {
        name          = "desk"
        displayName   = "Desk"
        pageType      = "PAGE_TYPE_CANVAS"
        layoutVersion = "GRID_V1"
        layout = [
          local.text.desk_title, local.table_widget.desk_status,
          local.counter.desk_value, local.counter.desk_return, local.counter.desk_today,
          local.counter.desk_trades, local.counter.desk_proposals, local.counter.desk_calls,
          local.chart.port_vs_nifty, local.table_widget.desk_setups,
          local.table_widget.desk_pools, local.table_widget.desk_decisions, local.table_widget.desk_board,
          local.text.desk_details, local.table_widget.holdings, local.chart.port_alloc, local.chart.port_drawdown,
          local.table_widget.summary_portfolio,
        ]
      },
      {
        name          = "intraday"
        displayName   = "Intraday"
        pageType      = "PAGE_TYPE_CANVAS"
        layoutVersion = "GRID_V1"
        layout = [
          local.text.intra_title, local.show_filter.intra_show,
          local.counter.intra_pnl, local.counter.intra_trades, local.counter.intra_win,
          local.counter.intra_r, local.counter.intra_pf, local.counter.intra_wl,
          local.chart.intra_all,
          local.table_widget.intra_board, local.chart.intra_daily,
          local.chart.intra_setup, local.chart.intra_hour, local.chart.intra_drawdown,
          local.table_widget.intra_recent,
          local.text.intra_tests, local.chart.intra_backtest, local.chart.intra_bt_trend,
          local.table_widget.intra_bt_table, local.table_widget.intra_shadow_list,
          local.table_widget.summary_intraday,
          local.table_widget.intra_ladder, local.table_widget.intra_fills,
          local.table_widget.intra_select,
        ]
      },
      {
        name          = "swing"
        displayName   = "Swing"
        pageType      = "PAGE_TYPE_CANVAS"
        layoutVersion = "GRID_V1"
        layout = [
          local.text.swing_title, local.show_filter.swing_show,
          local.counter.swing_real, local.counter.swing_pnl, local.counter.swing_return, local.counter.swing_trades,
          local.table_widget.swing_best_card,
          local.chart.swing_curve, local.chart.swing_pnl_rs, local.table_widget.swing_suggest,
          local.table_widget.swing_board, local.table_widget.swing_hold, local.table_widget.swing_trades,
          local.table_widget.swing_scan, local.table_widget.swing_mom, local.table_widget.swing_signals,
        ]
      },
      {
        name          = "long_term"
        displayName   = "Long term"
        pageType      = "PAGE_TYPE_CANVAS"
        layoutVersion = "GRID_V1"
        layout = [
          local.text.lt_title, local.show_filter.lt_show,
          local.counter.lt_real, local.counter.lt_pnl, local.counter.lt_return, local.counter.lt_trades,
          local.table_widget.lt_best_card,
          local.chart.lt_curve, local.chart.lt_pnl_rs, local.table_widget.lt_suggest,
          local.table_widget.lt_board, local.table_widget.lt_hold, local.table_widget.lt_trades,
          local.table_widget.quality_top, local.chart.quality_dots, local.table_widget.summary_quality,
        ]
      },
      {
        name          = "your_calls"
        displayName   = "Your calls"
        pageType      = "PAGE_TYPE_CANVAS"
        layoutVersion = "GRID_V1"
        layout = [
          local.text.calls_title,
          local.counter.calls_right, local.counter.calls_liked, local.counter.calls_disliked,
          local.counter.calls_edge, local.counter.calls_rated, local.counter.calls_needed,
          local.table_widget.calls_matrix, local.table_widget.calls_vs_trader,
          local.chart.calls_trend, local.chart.calls_setup,
          local.table_widget.calls_list, local.table_widget.calls_summary,
        ]
      },
      {
        name          = "markets"
        displayName   = "Global markets"
        pageType      = "PAGE_TYPE_CANVAS"
        layoutVersion = "GRID_V1"
        layout = [
          local.text.markets_title, local.table_widget.mk_read,
          local.counter.markets_sp500, local.counter.markets_nasdaq, local.counter.markets_nikkei,
          local.counter.markets_usdinr, local.counter.markets_brent, local.counter.markets_vix,
          local.table_widget.mk_world, local.chart.mk_follow,
          local.table_widget.mk_us_after, local.chart.mk_breadth,
          local.table_widget.mk_unusual, local.chart.mk_adr, local.chart.mk_usdinr,
          local.text.markets_tables, local.table_widget.details_global, local.table_widget.details_ivg,
          local.table_widget.details_adr, local.table_widget.summary_overview,
        ]
      },
    ]
  }
}

resource "databricks_dashboard" "stocks" {
  display_name         = "NSE Paper Trading Desk"
  parent_path          = "${data.databricks_current_user.me.home}/stocks_lakehouse"
  warehouse_id         = data.databricks_sql_warehouse.main.id
  serialized_dashboard = jsonencode(local.dashboard)
  embed_credentials    = true

  depends_on = [databricks_pipeline.stocks]
}

output "dashboard_url" {
  value = "${trimsuffix(var.databricks_workspace_url, "/")}/dashboardsv3/${databricks_dashboard.stocks.id}/published"
}
