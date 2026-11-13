# Data model

Everything lives in the Unity Catalog catalog **`stocks`**. Each table's Python definition in `pipeline/` has
its equivalent Lakeflow SQL in a comment above it.

## Landing (raw files)

Same layout in S3 (`s3://stocks-lakehouse-landing-<account-id>/landing/`) and in the Databricks volume
(`/Volumes/stocks/bronze/landing/`):

| Path | Content | Format |
|---|---|---|
| `nse/date=YYYY-MM-DD/bhavcopy_cm_YYYYMMDD.csv.zip` | NSE UDiFF cash-market bhavcopy (S3) | zip |
| `nse/date=YYYY-MM-DD/bhavcopy_cm_YYYYMMDD.csv` | Same, unzipped (volume only) | CSV |
| `massive/date=YYYY-MM-DD/grouped_daily_YYYYMMDD.json` | Massive grouped daily, all US tickers | JSON |
| `fred/date=YYYY-MM-DD/fred_<start>_<end>.json` | FRED observations for a date range | NDJSON |
| `trades/date=YYYY-MM-DD/events.jsonl` | Paper-trader journal (dummy money), pushed 15:45 IST by `stocks-trader-guardian` (volume; S3 copy in the trader's own bucket) | NDJSON |
| `index_members/date=YYYY-MM-DD/nifty200.jsonl` | NSE's Nifty 200 list (symbol, company, industry, ISIN), pushed weekly with the fundamentals (volume only) | NDJSON |
| `portfolio/date=YYYY-MM-DD/portfolio.jsonl` | Paper-trader pools (capital, cash, equity) and holdings (qty, cost, last price, value, P&L), pushed daily at 15:45 IST with the journal (volume only) | NDJSON |
| `fundamentals/date=YYYY-MM-DD/fundamentals.jsonl` | Company financials for NSE's Nifty 200 list plus paper-trader's watchlist (the 250 most traded NSE stocks if the list can't be read), pushed weekly (Sat 07:15 IST) by paper-trader's *Company fundamentals* workflow (volume only) | NDJSON |
| `<source>/_README.txt` | Folder marker (volume only) | text |

Company news has **its own volume**, `/Volumes/stocks/bronze/news/` (S3 copy under `landing/news/`), so news
arriving every minute in market hours doesn't trigger a pipeline refresh each time:

| Path | Content | Format |
|---|---|---|
| `date=YYYY-MM-DD/snap_HHMM.jsonl` | Every item in NSE's announcements, results, board-meeting and corporate-action RSS feeds at that time | NDJSON |
| `date=YYYY-MM-DD/latest.jsonl` | Copy of the newest snapshot, read directly by paper-trader (not by the pipeline) | NDJSON |

## Bronze (streaming tables, Auto Loader)

Each new file is read once. Every bronze table has these lineage columns: `_source_file`,
`_file_modified_at`, `_ingested_at`, and `trade_date` (parsed from `date=` in the path).

| Table | Grain | Notes |
|---|---|---|
| `bronze.nse_bhavcopy_raw` | instrument × day × file | All UDiFF columns as strings (`TckrSymb`, `SctySrs`, `ISIN`, `OpnPric`, `ClsPric`, `TtlTradgVol`...). Unknown columns go to `_rescued_data`. |
| `bronze.massive_grouped_raw` | file (one US day) | `results` kept as **JSON text** (fields `T` and `t` clash, see ARCHITECTURE), plus `status`, `resultsCount`, `adjusted`. |
| `bronze.fred_observations_raw` | series × date × file | `series_id`, `date`, `value`, `name`, `region`, `asset_class`, `unit`. Explicit schema. |
| `bronze.news_raw` | news item × snapshot | `id`, `feed`, `symbol` (null when the company couldn't be matched), `company`, `category`, `title`, `description`, `link`, `published_ist`, `fetched_at_ist`. Explicit schema. |
| `bronze.index_members_raw` | company × file | `symbol`, `company`, `industry`, `isin`, `index_name`, `as_of`. Explicit schema. |
| `bronze.portfolio_raw` | record × file | `record_type` (`pool` or `holding`), `snapshot_date`, `pool`, pool fields (`capital`, `cash`, `holdings_value`, `equity`) or holding fields (`symbol`, `sector`, `qty`, `avg_cost`, `last_price`, `value`, `pnl`, `opened`). Explicit schema. |
| `bronze.fundamentals_raw` | company × metric × period × file | `symbol`, `metric` (TotalRevenue, NetIncome, EBIT, TotalDebt, StockholdersEquity, OperatingCashFlow, InvestedCapital, DilutedEPS, TotalAssets), `period_type` (`12M` annual, `3M` quarterly), `period_end`, `value` (INR), `source`. Explicit schema. |
| `bronze.trades_raw` | event × file | Paper-trader events: `event_id`, `proposal_id`, `timestamp_ist`, `event`, `symbol`, `setup`, `side`, prices, `qty`, `risk_rs`, `pnl_rs`, `fees_rs`, `user_reply`; snapshot fields `balance`, `starting_capital`, `realized_today`. Explicit schema. |

## Silver (materialized views)

| Table | Grain | Key columns | Quality rules (rows dropped) |
|---|---|---|---|
| `silver.nse_eq_daily` | symbol × series × day | `symbol`, `series`, `isin`, `name`, `instrument_type`, OHLC, `prev_close`, `volume`, `turnover`, `trades`, `currency='INR'` | close > 0, open > 0, high ≥ low, symbol and date present |
| `silver.us_daily` | ticker × day | `symbol`, OHLC, `prev_close` (previous bar), `volume`, `turnover` (= volume × VWAP), `vwap`, `trades`, `currency='USD'`. OTC excluded. | same |
| `silver.daily_bars` | market × symbol × (series) × day | **Unified India + US**: `market` (`IN`/`US`), `exchange`, `symbol`, `series` (NSE only), `isin`, `trade_date`, OHLC, `prev_close`, `volume`, `turnover`, `vwap`, `trades`, `currency` | — |
| `silver.trading_days` | market × day | `trade_date`, `prev_trade_date`, `symbols`. A day counts if the market published data. | — |
| `silver.fred_daily` | series × date | `series_id`, `obs_date`, `value`, `name`, `region`, `asset_class`, `unit` | series, date and value present |
| `silver.trade_events` | event | Typed paper-trader events (`event_ts` timestamp IST). Events: `proposed`, `blocked_by_risk_gate`, `rejected`, `expired`, `skipped_after_approval`, `filled`, `exited`, `squared_off`, `passed_outcome`, `day_plan`, `ledger_snapshot`. | event_id, event and time present |
| `silver.news` | news item | `id`, `feed`, `symbol`, `category` (results, board_meeting, order, management, regulatory, rating, pledge, insider, dividend_split_bonus, mna, meeting, corporate_action, other), `title`, `link`, `published_ts`, `first_seen_ts`, `news_ts`, `news_date`. Earliest snapshot of each item wins. | id and title present |
| `silver.index_members` | company | The latest Nifty 200 list: `symbol`, `company`, `industry` | symbol present |
| `silver.portfolio_pools` | day × pool | `capital`, `cash`, `holdings_value`, `equity` (newest file per day) | keys and equity present |
| `silver.portfolio_holdings` | day × pool × symbol | `sector`, `qty`, `avg_cost`, `last_price`, `value`, `pnl`, `opened` | keys present |
| `silver.fundamentals` | company × metric × period | `symbol`, `metric`, `period_type`, `period_end`, `value`, `currency`, `fetched_at`. Newest weekly fetch wins (restatements replace old figures). | keys and value present |
| `silver.country_etfs` | ticker | Reference list of the 12 country ETFs: `symbol`, `name`, `region` (from `COUNTRY_ETFS` in `silver.py`) | — |
| `silver.adr_pairs` | ADR | Reference list of Indian ADRs: `adr`, `nse_symbol`, `shares_per_adr` (from `ADR_PAIRS` in `gold.py`) | — |
| `silver.global_daily` | instrument × date | `instrument`, `name`, `region`, `asset_class`, `unit`, `source` (`FRED`/`Massive`), `obs_date`, `value`. FRED series + 12 country ETFs. | — |

Reference lists (`country_etfs`, `adr_pairs`) are pipeline tables rather than inline lists so the views that
join them read only Delta sources and can refresh incrementally.

De-duplication: when a day or observation is fetched more than once, the row from the most recently modified
file wins (for trades: per `event_id`, so a re-exported day replaces its old copy).

For NSE, `series = 'EQ'` is the main board. Gold uses `EQ` only.

## Gold (materialized views)

| Table | Grain | What it answers | Key columns |
|---|---|---|---|
| `gold.unusual_volume` | market × symbol × day | Which stocks traded ≥ 3× their prior 20-day average volume? | `volume_ratio`, `return_pct`, `avg_volume_20d` |
| `gold.market_breadth` | market × day | How broad was the move? | `advances`, `declines`, `advance_decline_ratio`, `median_return_pct`, `pct_above_ma20` |
| `gold.nifty_after_us_moves` | Indian session | What did India (NIFTYBEES) do after the preceding US session (SPY)? | `india_date`, `us_date`, `us_return_pct`, `us_move`, `india_gap_pct`, `india_return_pct` |
| `gold.nifty_after_us_moves_summary` | US move bucket | On average, after big US up/down/flat days | `sessions`, `avg_india_gap_pct`, `pct_india_up_days`, `pct_gap_same_direction_as_us`, `corr_us_return_india_gap` |
| `gold.adr_premium` | ADR × day | Are Indian ADRs (INFY, WIT, HDB, IBN, RDY) rich or cheap vs their NSE shares? | `implied_inr_per_usd`, `relative_premium_pct`, `fx_inr_per_usd`, `fx_date`, `fx_premium_pct` |
| `gold.global_markets_daily` | instrument × date | Global instruments with changes | `change_abs`, `change_pct`, `change_5d_pct`, `change_20d_pct` |
| `gold.global_snapshot` | instrument | The latest reading of every global instrument | same, latest row only |
| `gold.india_vs_global` | instrument | Which global moves line up with India's next open? | `sessions`, `corr_with_india_gap`, `corr_with_india_return`, `pct_same_direction_gap` |
| `gold.trades` | paper trade | Each dummy-money trade from entry to exit | `entry_price`, `exit_price`, `exit_reason`, `status`, `net_pnl_rs`, `fees_rs`, `r_multiple`, `holding_minutes`, `entry_hour`, `is_win` |
| `gold.trading_daily` | day | How did the day go, and does it match the ledger? | `proposals`, `rejected`, `expired`, `trades_opened`, `trades_closed`, `win_rate_pct`, `net_pnl_rs`, `end_balance_rs`, `reconciliation_diff_rs`, `reconciliation` |
| `gold.trading_by_setup` | setup × side | Which setups make money? | `trades`, `win_rate_pct`, `net_pnl_rs`, `avg_pnl_per_trade_rs`, `avg_r_multiple` |
| `gold.trading_by_hour` | entry hour (IST) | When in the day do trades work? | same as by setup |
| `gold.trading_decisions` | day | Did your YES/NO choices help? | `taken`, `taken_net_pnl_rs`, `passed`, `passed_would_have_made_rs`, `saved_by_skipping_rs` |
| `gold.proposal_outcomes` | proposal | Every proposal with its scores, decision, outcome and your 👍/👎 | `proposed_at`, `hour`, `setup`, `side`, `suggested_yes_pct`, `yes_band`, `checks_pct`, `agent_pct`, `reviewer_verdict`, `reviewer_pct`, `mood`, `mode` (auto/human), `decision` (taken, skipped_by_you, skipped_auto, expired, not_filled), `outcome_rs` (real if taken, would-have if skipped), `won`, `outcome_r`, `feedback` (like/dislike) |
| `gold.track_record` | dimension × value | How proposals turned out, last 90 days; read by paper-trader every session (the feedback loop) | `dimension` (all, setup, hour, mood, yes_band, reviewer, mode, decision, feedback), `value`, `trades`, `wins`, `win_rate_pct`, `avg_outcome_rs`, `total_outcome_rs`, `avg_outcome_r` |
| `gold.portfolio_daily` | day | How is the whole dummy portfolio doing? | `total_value_rs`, pool values, `pnl_rs`, `return_pct`, `drawdown_pct`, `portfolio_index` and `nifty_index` (both 100 on day one), `day_change_rs` |
| `gold.holdings_latest` | holding | What do I hold now? | `pool`, `symbol`, `sector`, `qty`, `avg_cost`, `last_price`, `value`, `pnl`, `pnl_pct`, `weight_pct`, `days_held` |
| `gold.intraday_universe` | Nifty 200 stock, latest NSE day | Which stocks may paper-trader trade intraday beyond its watchlist, and which have news today? | `eligible` (EQ series, close ≥ Rs 50, Rs 50 crore+ traded a day over 20 days), `flagged` (eligible and a fresh NSE filing, a 3%+ move or 2x volume), `reasons`, `latest_news_title` |
| `gold.swing_signals` | stock × day (last 45 days) | Which news breakouts does the swing shadow pool trade? | eligible stock within 2 days of an NSE filing, close above its 20-day high on 2x volume, up 2%+, above its 50-day average; `stop` (5-day low, at most 8% below) |
| `gold.shadow_pools_daily` | shadow pool × day | Are the rules tested on paper beating NIFTYBEES? | `pool` (swing_shadow, long_term_shadow), `equity`, `return_pct`, `nifty_return_pct` (NIFTYBEES since the pool's first day here), `vs_nifty_pct` |
| `gold.longterm_candidates` | company | What would the long-term pool buy this week? | `rank`, `is_pick` (top 10, max 2 per industry), `score` (100 × quality + growth bonus − price penalty), `target_weight_pct` (90% invested in equal parts, max 15%), `reasons` (plain words) |
| `gold.company_fundamentals` | company | Which large companies pass the long-term pool's quality tests? | `roce_pct` (`roa_pct` for banks/lenders), `debt_to_equity`, `ocf_positive_years`, `revenue_cagr_3y_pct`, `profit_cagr_3y_pct`, `eps_ttm`, `pe`, `pe_percentile` (0 = cheapest), `profit_down_2q`, `pass_capital/balance/cash/growth/price`, `tests_passed`, `quality_score`. Not in this source: promoter pledges, bad-loan ratios, P/E history |
| `gold.dashboard_summary_facts` | dashboard page | Short facts per page from the latest numbers (not a pipeline table: rebuilt by `sql/dashboard_summary.sql` after every refresh) | `page`, `facts` |
| `gold.dashboard_summary` | dashboard page | The "In plain words" text on each dashboard page | `page`, `facts`, `ai_text` (`ai_summarize` of the facts; the dashboard shows the facts when it is empty), `generated_at` |

### Definitions

- **Return:** `(close / prev_close - 1) × 100`.
- **India gap:** `(open / prev_close - 1) × 100` for NIFTYBEES.
- **US move bucket:** SPY return ≥ +1% = "US up big", ≤ −1% = "US down big", otherwise "US flat"
  (`big_move_pct`).
- **Matching India to the US / global:** each Indian date uses the latest observation dated **strictly before**
  it (within 7 days), i.e. information available before 09:15 IST.
- **ADR premium:** `implied_inr_per_usd = NSE close × shares per ADR / ADR close`.
  - `relative_premium_pct` compares it with the median implied rate across pairs that day.
  - `fx_premium_pct` compares it with FRED USD/INR (`DEXINUS`, published weekly; latest value up to 10 days
    old, shown in `fx_date`).
  - ADR ratios: INFY 1, WIT 1, HDB 3, IBN 2, RDY 1. Check them after splits.
- **Rates:** for `DGS10` (percent), read `change_abs` (`+0.05` = +5 basis points), not `change_pct`.
- **Paper trade net P&L:** `exit pnl_rs − entry fees_rs`. The exit's `pnl_rs` is already net of the exit
  brokerage; slippage is inside the fill prices. `r_multiple = net P&L / risk_rs` (−1 ≈ a normal stop-out).
- **Reconciliation:** each day, P&L rebuilt from events (exits − entry fees) is compared with the trader's own
  `realized_today` in the `ledger_snapshot` line. `ok` if they differ by less than Rs 1; `MISMATCH` means the
  two records disagree and the S3 ledger should be checked.
- **Skipped proposals:** `passed_outcome` events carry what a NO / no-reply proposal would have made (entry at
  the next minute's open, then stop, target or 15:10). `saved_by_skipping_rs > 0` means skipping helped.

### Global instruments

| From FRED | From Massive (US-listed ETFs) |
|---|---|
| SP500, DJIA, NASDAQCOM, NIKKEI225 | EWJ Japan, EWG Germany, EWU UK, EZU Eurozone, VGK Europe |
| VIXCLS | FXI China, EWH Hong Kong, EWY South Korea, EWT Taiwan |
| DEXINUS, DEXJPUS, DEXUSEU, DEXUSUK, DEXCHUS, DTWEXBGS | EWZ Brazil, EEM Emerging markets, INDA India |
| DCOILBRENTEU, DCOILWTICO, DGS10 | |

## Configuration and secrets

| SSM parameter | Type | Set by | Used by |
|---|---|---|---|
| `/stocks/massive-api-key` | SecureString | You (CLI) | Massive fetcher |
| `/stocks/fred-api-key` | SecureString | You (CLI) | FRED fetcher |
| `/stocks/telegram-bot-token`, `/stocks/telegram-chat-id` | SecureString | You (CLI) | Pre-market brief |
| `/stocks/databricks-pusher-token` | SecureString | Terraform (databricks stack) | Fetchers, sync, brief |
| `/stocks/databricks-workspace-url` | String | Terraform (databricks stack) | Fetchers, sync, brief |
| `/stocks/databricks-warehouse-id` | String | Terraform (databricks stack) | Pre-market brief |

Pipeline settings (Terraform variables → pipeline configuration): `stocks.india_proxy` (NIFTYBEES),
`stocks.us_proxy` (SPY), `stocks.big_move_pct` (1.0), `stocks.volume_spike_ratio` (3.0),
`stocks.min_avg_volume` (50000).
