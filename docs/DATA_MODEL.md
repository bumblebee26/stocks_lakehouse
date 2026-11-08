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
| `<source>/_README.txt` | Folder marker (volume only) | text |

## Bronze (streaming tables, Auto Loader)

Each new file is read once. Every bronze table has these lineage columns: `_source_file`,
`_file_modified_at`, `_ingested_at`, and `trade_date` (parsed from `date=` in the path).

| Table | Grain | Notes |
|---|---|---|
| `bronze.nse_bhavcopy_raw` | instrument × day × file | All UDiFF columns as strings (`TckrSymb`, `SctySrs`, `ISIN`, `OpnPric`, `ClsPric`, `TtlTradgVol`...). Unknown columns go to `_rescued_data`. |
| `bronze.massive_grouped_raw` | file (one US day) | `results` kept as **JSON text** (fields `T` and `t` clash, see ARCHITECTURE), plus `status`, `resultsCount`, `adjusted`. |
| `bronze.fred_observations_raw` | series × date × file | `series_id`, `date`, `value`, `name`, `region`, `asset_class`, `unit`. Explicit schema. |

## Silver (materialized views)

| Table | Grain | Key columns | Quality rules (rows dropped) |
|---|---|---|---|
| `silver.nse_eq_daily` | symbol × series × day | `symbol`, `series`, `isin`, `name`, `instrument_type`, OHLC, `prev_close`, `volume`, `turnover`, `trades`, `currency='INR'` | close > 0, open > 0, high ≥ low, symbol and date present |
| `silver.us_daily` | ticker × day | `symbol`, OHLC, `prev_close` (previous bar), `volume`, `turnover` (= volume × VWAP), `vwap`, `trades`, `currency='USD'`. OTC excluded. | same |
| `silver.daily_bars` | market × symbol × (series) × day | **Unified India + US**: `market` (`IN`/`US`), `exchange`, `symbol`, `series` (NSE only), `isin`, `trade_date`, OHLC, `prev_close`, `volume`, `turnover`, `vwap`, `trades`, `currency` | — |
| `silver.trading_days` | market × day | `trade_date`, `prev_trade_date`, `symbols`. A day counts if the market published data. | — |
| `silver.fred_daily` | series × date | `series_id`, `obs_date`, `value`, `name`, `region`, `asset_class`, `unit` | series, date and value present |
| `silver.global_daily` | instrument × date | `instrument`, `name`, `region`, `asset_class`, `unit`, `source` (`FRED`/`Massive`), `obs_date`, `value`. FRED series + 12 country ETFs. | — |

De-duplication: when a day or observation is fetched more than once, the row from the most recently modified
file wins.

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
