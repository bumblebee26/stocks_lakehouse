-- "In plain words" summaries for the dashboard (one row per page).
-- Runs as the second task of the stocks-refresh job, after the pipeline (Databricks SQL warehouse).
-- 1) Facts: short sentences built from the latest numbers. A sentence whose data is missing is
--    simply left out (concat_ws skips NULLs), so a page never shows a broken sentence.
-- 2) AI text: ai_summarize rewrites the facts more fluently. If AI Functions fail, the dashboard
--    falls back to the facts (the dataset reads coalesce(ai_text, facts)).
-- Checked in CI with EXPLAIN (scripts/check_sql.py).

CREATE OR REPLACE TABLE stocks.gold.dashboard_summary_facts
COMMENT 'Plain-words facts per dashboard page, rebuilt after every pipeline refresh.'
AS
SELECT 'overview' AS page, concat_ws(' ',
  (SELECT 'In the last US session the S&P 500 moved ' || format_number(us_return_pct, 2) || '% (' || us_move || ').'
   FROM stocks.gold.nifty_after_us_moves ORDER BY us_date DESC LIMIT 1),
  (SELECT 'After similar US days, India has opened ' || format_number(s.avg_india_gap_pct, 2)
          || '% on average and ended higher on ' || CAST(round(s.pct_india_up_days) AS INT) || '% of those days.'
   FROM stocks.gold.nifty_after_us_moves_summary s
   WHERE s.us_move = (SELECT us_move FROM stocks.gold.nifty_after_us_moves ORDER BY us_date DESC LIMIT 1)),
  (SELECT CAST(round(pct_above_ma20) AS INT) || '% of NSE stocks are above their 20-day average'
          || CASE WHEN pct_above_ma20 >= 60 THEN ' (broad strength).' WHEN pct_above_ma20 <= 40 THEN ' (broad weakness).'
                  ELSE ' (mixed).' END
   FROM stocks.gold.market_breadth WHERE market = 'IN' ORDER BY trade_date DESC LIMIT 1),
  (SELECT 'Biggest volume spike on NSE: ' || symbol || ' traded ' || format_number(volume_ratio, 1)
          || 'x its usual volume and moved ' || format_number(return_pct, 1) || '% that day.'
   FROM stocks.gold.unusual_volume
   WHERE market = 'IN' AND trade_date = (SELECT max(trade_date) FROM stocks.gold.unusual_volume WHERE market = 'IN')
   ORDER BY volume_ratio DESC LIMIT 1),
  (SELECT 'US volatility index (VIX): ' || format_number(value, 1)
          || CASE WHEN value >= 25 THEN ', a nervous market.' WHEN value <= 15 THEN ', a calm market.' ELSE ', normal.' END
   FROM stocks.gold.global_snapshot WHERE upper(name) LIKE '%VIX%' ORDER BY obs_date DESC LIMIT 1)
) AS facts
UNION ALL
SELECT 'portfolio', concat_ws(' ',
  (SELECT 'Your dummy portfolio is worth Rs ' || format_number(total_value_rs, 0) || ', '
          || CASE WHEN pnl_rs >= 0 THEN 'up' ELSE 'down' END || ' Rs ' || format_number(abs(pnl_rs), 0)
          || ' (' || format_number(return_pct, 2) || '%) on Rs ' || format_number(capital_rs, 0) || ' of capital.'
   FROM stocks.gold.portfolio_daily ORDER BY snapshot_date DESC LIMIT 1),
  (SELECT 'Since tracking began it has ' || CASE WHEN portfolio_index >= 100 THEN 'gained ' ELSE 'lost ' END
          || format_number(abs(portfolio_index - 100), 2) || '%, while simply holding NIFTYBEES '
          || CASE WHEN nifty_index >= 100 THEN 'gained ' ELSE 'lost ' END || format_number(abs(nifty_index - 100), 2) || '%.'
   FROM stocks.gold.portfolio_daily WHERE nifty_index IS NOT NULL ORDER BY snapshot_date DESC LIMIT 1),
  (SELECT CASE WHEN drawdown_pct < 0 THEN 'It is ' || format_number(abs(drawdown_pct), 1) || '% below its best value so far.'
               ELSE 'It is at its best value so far.' END
   FROM stocks.gold.portfolio_daily ORDER BY snapshot_date DESC LIMIT 1),
  (SELECT 'Largest holding: ' || symbol || ' (' || CASE pool WHEN 'long_term' THEN 'long-term' ELSE pool END || ' pool), '
          || format_number(weight_pct, 1) || '% of the portfolio, ' || CASE WHEN pnl >= 0 THEN 'up' ELSE 'down' END
          || ' Rs ' || format_number(abs(pnl), 0) || '.'
   FROM stocks.gold.holdings_latest ORDER BY value DESC LIMIT 1)
)
UNION ALL
SELECT 'intraday', concat_ws(' ',
  (SELECT 'Intraday so far: ' || count(*) || ' trades, ' || CAST(round(100 * avg(CAST(is_win AS INT))) AS INT)
          || '% won, net ' || CASE WHEN sum(net_pnl_rs) >= 0 THEN '+' ELSE '-' END || 'Rs ' || format_number(abs(sum(net_pnl_rs)), 0) || '.'
   FROM stocks.gold.trades WHERE net_pnl_rs IS NOT NULL HAVING count(*) > 0),
  (SELECT 'Profit factor ' || format_number(pf, 2) || CASE WHEN pf >= 1.5 THEN ': gains comfortably outweigh losses.'
                                                         WHEN pf >= 1 THEN ': only just profitable.'
                                                         ELSE ': losses outweigh gains so far.' END
   FROM (SELECT sum(CASE WHEN net_pnl_rs > 0 THEN net_pnl_rs END)
                / nullif(-sum(CASE WHEN net_pnl_rs < 0 THEN net_pnl_rs END), 0) AS pf
         FROM stocks.gold.trades) WHERE pf IS NOT NULL),
  (SELECT 'Last trading day (' || date_format(trade_date, 'd MMM') || '): '
          || CASE WHEN coalesce(ledger_realized_rs, net_pnl_rs) >= 0 THEN '+' ELSE '-' END
          || 'Rs ' || format_number(abs(coalesce(ledger_realized_rs, net_pnl_rs)), 0) || '.'
   FROM stocks.gold.trading_daily WHERE coalesce(ledger_realized_rs, net_pnl_rs) IS NOT NULL
   ORDER BY trade_date DESC LIMIT 1),
  (SELECT 'Best setup so far: ' || setup || ' ' || side || ' (' || CASE WHEN net_pnl_rs >= 0 THEN '+' ELSE '-' END
          || 'Rs ' || format_number(abs(net_pnl_rs), 0) || ' over ' || CAST(trades AS INT) || ' trades).'
   FROM stocks.gold.trading_by_setup ORDER BY net_pnl_rs DESC LIMIT 1),
  (SELECT 'Weakest setup: ' || setup || ' ' || side || ' (' || CASE WHEN net_pnl_rs >= 0 THEN '+' ELSE '-' END
          || 'Rs ' || format_number(abs(net_pnl_rs), 0) || ' over ' || CAST(trades AS INT) || ' trades).'
   FROM stocks.gold.trading_by_setup WHERE trades >= 2 ORDER BY net_pnl_rs ASC LIMIT 1)
)
UNION ALL
SELECT 'quality', concat_ws(' ',
  (SELECT count(*) || ' large companies checked, and ' || count_if(tests_passed >= 4) || ' pass at least 4 of the quality tests.'
   FROM stocks.gold.company_fundamentals HAVING count(*) > 0),
  (SELECT 'Highest quality scores: ' || array_join(transform(sort_array(collect_list(
            struct(-quality_score AS k1, pe_percentile AS k2, symbol AS s))), x -> x.s), ', ') || '.'
   FROM (SELECT * FROM stocks.gold.company_fundamentals WHERE tests_available >= 3
         ORDER BY quality_score DESC, pe_percentile ASC LIMIT 5)),
  (SELECT count_if(profit_down_2q) || ' companies have seen profit fall two quarters in a row: worth avoiding for now.'
   FROM stocks.gold.company_fundamentals HAVING count_if(profit_down_2q) > 0)
);

CREATE OR REPLACE TABLE stocks.gold.dashboard_summary
COMMENT 'Dashboard summaries per page: the facts and an AI rewrite (ai_summarize), refreshed after every pipeline run.'
AS
SELECT page, facts, CASE WHEN facts <> '' THEN ai_summarize(facts, 70) END AS ai_text,
       current_timestamp() AS generated_at
FROM stocks.gold.dashboard_summary_facts;
