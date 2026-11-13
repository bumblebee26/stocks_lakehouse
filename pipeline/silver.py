"""SILVER: cleaned, typed, de-duplicated daily bars, unified across markets.

  stocks.silver.nse_eq_daily   NSE equities (typed, one row per symbol/series/day)
  stocks.silver.us_daily       US stocks from Massive (exploded, OTC excluded)
  stocks.silver.daily_bars     both markets in ONE schema  <- what gold reads
  stocks.silver.trading_days   each market's trading calendar, derived from the data
  stocks.silver.trade_events   paper-trader journal events (dummy money), one row per event_id
  stocks.silver.news           NSE company news, one row per item (id), typed
  stocks.silver.fundamentals   company financials, one row per symbol x metric x period (newest fetch wins)
  stocks.silver.portfolio_pools     paper-trader pools per day (capital, cash, equity)
  stocks.silver.portfolio_holdings  paper-trader holdings per day
  stocks.silver.shadow_pools   paper-trader shadow pools per day (rules tested on paper; never in portfolio totals)
  stocks.silver.index_members  the latest Nifty 200 list: symbol, company, industry

Materialized views: they recompute incrementally where possible, and make
de-duplication simple (a re-fetched day replaces the old copy: latest file wins).
Data-quality expectations drop bad rows and report counts in the pipeline UI.
"""

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

    dp.materialized_view = dp.table

from pyspark.sql import Window
from pyspark.sql import functions as F

VALID_OHLC = {
    "positive_close": "close > 0",
    "positive_open": "open > 0",
    "high_not_below_low": "high >= low",
    "has_symbol": "symbol IS NOT NULL AND symbol <> ''",
    "has_trade_date": "trade_date IS NOT NULL",
}


MASSIVE_RESULTS_SCHEMA = (
    "ARRAY<STRUCT<T: STRING, o: DOUBLE, h: DOUBLE, l: DOUBLE, c: DOUBLE, "
    "v: DOUBLE, vw: DOUBLE, n: BIGINT, ts: BIGINT, otc: BOOLEAN>>"
)


def latest_file_wins(df, keys):
    """Keep one row per key: the one from the most recently modified file."""
    w = Window.partitionBy(*keys).orderBy(F.col("_file_modified_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------
# NSE equities
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.nse_eq_daily (
#     CONSTRAINT positive_close     EXPECT (close > 0)           ON VIOLATION DROP ROW,
#     CONSTRAINT positive_open      EXPECT (open > 0)            ON VIOLATION DROP ROW,
#     CONSTRAINT high_not_below_low EXPECT (high >= low)         ON VIOLATION DROP ROW,
#     CONSTRAINT has_symbol         EXPECT (symbol IS NOT NULL AND symbol <> '') ON VIOLATION DROP ROW,
#     CONSTRAINT has_trade_date     EXPECT (trade_date IS NOT NULL) ON VIOLATION DROP ROW
#   ) AS
#   SELECT * EXCEPT (rn) FROM (
#     SELECT
#       'IN' AS market, 'NSE' AS exchange,
#       trim(TckrSymb) AS symbol, trim(SctySrs) AS series, trim(ISIN) AS isin, trim(FinInstrmNm) AS name,
#       trim(FinInstrmTp) AS instrument_type,
#       coalesce(to_date(TradDt), trade_date) AS trade_date,
#       CAST(OpnPric AS DOUBLE) AS open, CAST(HghPric AS DOUBLE) AS high,
#       CAST(LwPric AS DOUBLE) AS low,   CAST(ClsPric AS DOUBLE) AS close,
#       CAST(PrvsClsgPric AS DOUBLE) AS prev_close,
#       CAST(TtlTradgVol AS DOUBLE) AS volume, CAST(TtlTrfVal AS DOUBLE) AS turnover,
#       CAST(TtlNbOfTxsExctd AS BIGINT) AS trades,
#       'INR' AS currency, _source_file, _file_modified_at,
#       row_number() OVER (PARTITION BY trim(TckrSymb), trim(SctySrs), coalesce(to_date(TradDt), trade_date)
#                          ORDER BY _file_modified_at DESC) AS rn
#     FROM stocks.bronze.nse_bhavcopy_raw
#   ) WHERE rn = 1;
@dp.materialized_view(
    name="stocks.silver.nse_eq_daily",
    comment="NSE cash-market securities (stocks, ETFs), typed, one row per symbol/series/trading day.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(VALID_OHLC)
def nse_eq_daily():
    raw = spark.read.table("stocks.bronze.nse_bhavcopy_raw")  # noqa: F821
    # The cash-market file holds only cash-market securities (stocks, ETFs, etc.);
    # keep them all and let consumers filter by series (EQ = main board).
    typed = raw.select(
        F.lit("IN").alias("market"),
        F.lit("NSE").alias("exchange"),
        F.trim("TckrSymb").alias("symbol"),
        F.trim("SctySrs").alias("series"),
        F.trim("ISIN").alias("isin"),
        F.trim("FinInstrmNm").alias("name"),
        F.trim("FinInstrmTp").alias("instrument_type"),
        F.coalesce(F.to_date("TradDt"), F.col("trade_date")).alias("trade_date"),
        F.col("OpnPric").cast("double").alias("open"),
        F.col("HghPric").cast("double").alias("high"),
        F.col("LwPric").cast("double").alias("low"),
        F.col("ClsPric").cast("double").alias("close"),
        F.col("PrvsClsgPric").cast("double").alias("prev_close"),
        F.col("TtlTradgVol").cast("double").alias("volume"),
        F.col("TtlTrfVal").cast("double").alias("turnover"),
        F.col("TtlNbOfTxsExctd").cast("bigint").alias("trades"),
        F.lit("INR").alias("currency"),
        "_source_file",
        "_file_modified_at",
    )
    return latest_file_wins(typed, ["symbol", "series", "trade_date"])


# ---------------------------------------------------------------------------
# US stocks (Massive grouped daily)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.us_daily (
#     CONSTRAINT positive_close     EXPECT (close > 0)    ON VIOLATION DROP ROW,
#     CONSTRAINT positive_open      EXPECT (open > 0)     ON VIOLATION DROP ROW,
#     CONSTRAINT high_not_below_low EXPECT (high >= low)  ON VIOLATION DROP ROW,
#     CONSTRAINT has_symbol         EXPECT (symbol IS NOT NULL AND symbol <> '') ON VIOLATION DROP ROW,
#     CONSTRAINT has_trade_date     EXPECT (trade_date IS NOT NULL) ON VIOLATION DROP ROW
#   ) AS
#   WITH bars AS (
#     SELECT trade_date, _source_file, _file_modified_at,
#            explode(from_json(
#              regexp_replace(results, '"t":', '"ts":'),     -- "t" clashes with "T" (case-insensitive)
#              'ARRAY<STRUCT<T: STRING, o: DOUBLE, h: DOUBLE, l: DOUBLE, c: DOUBLE,
#                            v: DOUBLE, vw: DOUBLE, n: BIGINT, ts: BIGINT, otc: BOOLEAN>>'
#            )) AS r
#     FROM stocks.bronze.massive_grouped_raw
#   ), dedup AS (
#     SELECT *, row_number() OVER (PARTITION BY r.T, trade_date ORDER BY _file_modified_at DESC) AS rn
#     FROM bars WHERE coalesce(r.otc, false) = false
#   )
#   SELECT
#     'US' AS market, 'US' AS exchange, r.T AS symbol, trade_date,
#     r.o AS open, r.h AS high, r.l AS low, r.c AS close,
#     lag(r.c) OVER (PARTITION BY r.T ORDER BY trade_date) AS prev_close,
#     r.v AS volume, r.v * r.vw AS turnover, r.vw AS vwap, r.n AS trades,
#     'USD' AS currency, _source_file, _file_modified_at
#   FROM dedup WHERE rn = 1;
@dp.materialized_view(
    name="stocks.silver.us_daily",
    comment="US stocks from Massive grouped daily, one row per ticker per trading day (OTC excluded).",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(VALID_OHLC)
def us_daily():
    raw = spark.read.table("stocks.bronze.massive_grouped_raw")  # noqa: F821
    # Rename "t" (timestamp) to "ts" before parsing: it clashes with "T" (ticker) because
    # Spark/Delta column names are case-insensitive. The regex is case-sensitive, so "T" is untouched.
    parsed = F.from_json(F.regexp_replace("results", '"t":', '"ts":'), MASSIVE_RESULTS_SCHEMA)
    bars = raw.select("trade_date", "_source_file", "_file_modified_at", F.explode(parsed).alias("r"))
    bars = bars.filter(~F.coalesce(F.col("r.otc"), F.lit(False))).select(
        F.lit("US").alias("market"),
        F.lit("US").alias("exchange"),
        F.col("r.T").alias("symbol"),
        "trade_date",
        F.col("r.o").alias("open"),
        F.col("r.h").alias("high"),
        F.col("r.l").alias("low"),
        F.col("r.c").alias("close"),
        F.col("r.v").alias("volume"),
        (F.col("r.v") * F.col("r.vw")).alias("turnover"),
        F.col("r.vw").alias("vwap"),
        F.col("r.n").alias("trades"),
        F.lit("USD").alias("currency"),
        "_source_file",
        "_file_modified_at",
    )
    bars = latest_file_wins(bars, ["symbol", "trade_date"])
    by_symbol = Window.partitionBy("symbol").orderBy("trade_date")
    return bars.withColumn("prev_close", F.lag("close").over(by_symbol))


# ---------------------------------------------------------------------------
# Unified daily bars (what gold reads)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.daily_bars AS
#   SELECT market, exchange, symbol, series, isin, trade_date, open, high, low, close,
#          coalesce(prev_close, lag(close) OVER (PARTITION BY market, symbol, series ORDER BY trade_date)) AS prev_close,
#          volume, turnover, CAST(NULL AS DOUBLE) AS vwap, trades, currency, _source_file
#   FROM stocks.silver.nse_eq_daily
#   UNION ALL
#   SELECT market, exchange, symbol, CAST(NULL AS STRING) AS series, CAST(NULL AS STRING) AS isin,
#          trade_date, open, high, low, close, prev_close, volume, turnover, vwap, trades, currency, _source_file
#   FROM stocks.silver.us_daily;
UNIFIED_COLUMNS = [
    "market", "exchange", "symbol", "series", "isin", "trade_date",
    "open", "high", "low", "close", "prev_close",
    "volume", "turnover", "vwap", "trades", "currency", "_source_file",
]


@dp.materialized_view(
    name="stocks.silver.daily_bars",
    comment="Daily OHLCV for NSE equities and US stocks in one schema (prices in local currency).",
    table_properties={"quality": "silver"},
)
def daily_bars():
    nse = spark.read.table("stocks.silver.nse_eq_daily")  # noqa: F821
    nse_prev = Window.partitionBy("market", "symbol", "series").orderBy("trade_date")
    nse = nse.withColumn(
        "prev_close", F.coalesce(F.col("prev_close"), F.lag("close").over(nse_prev))
    ).withColumn("vwap", F.lit(None).cast("double"))

    us = (
        spark.read.table("stocks.silver.us_daily")  # noqa: F821
        .withColumn("series", F.lit(None).cast("string"))
        .withColumn("isin", F.lit(None).cast("string"))
    )
    return nse.select(*UNIFIED_COLUMNS).unionByName(us.select(*UNIFIED_COLUMNS))


# ---------------------------------------------------------------------------
# Trading calendar per market, derived from the data
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.trading_days AS
#   SELECT market, trade_date,
#          lag(trade_date) OVER (PARTITION BY market ORDER BY trade_date) AS prev_trade_date,
#          symbols
#   FROM (SELECT market, trade_date, count(*) AS symbols
#         FROM stocks.silver.daily_bars GROUP BY market, trade_date);
@dp.materialized_view(
    name="stocks.silver.trading_days",
    comment="Trading calendar per market (a day counts if the market published data).",
    table_properties={"quality": "silver"},
)
def trading_days():
    days = (
        spark.read.table("stocks.silver.daily_bars")  # noqa: F821
        .groupBy("market", "trade_date")
        .agg(F.count("*").alias("symbols"))
    )
    w = Window.partitionBy("market").orderBy("trade_date")
    return days.withColumn("prev_trade_date", F.lag("trade_date").over(w))


# ---------------------------------------------------------------------------
# FRED series, de-duplicated (each run re-fetches a window; latest file wins)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.fred_daily (
#     CONSTRAINT has_series EXPECT (series_id IS NOT NULL) ON VIOLATION DROP ROW,
#     CONSTRAINT has_date   EXPECT (obs_date IS NOT NULL)  ON VIOLATION DROP ROW,
#     CONSTRAINT has_value  EXPECT (value IS NOT NULL)     ON VIOLATION DROP ROW
#   ) AS
#   SELECT series_id, obs_date, value, name, region, asset_class, unit, _source_file, _file_modified_at
#   FROM (
#     SELECT *, to_date(date) AS obs_date,
#            row_number() OVER (PARTITION BY series_id, to_date(date) ORDER BY _file_modified_at DESC) AS rn
#     FROM stocks.bronze.fred_observations_raw
#   ) WHERE rn = 1;
@dp.materialized_view(
    name="stocks.silver.fred_daily",
    comment="FRED series (global indices, FX, VIX, oil, rates), one row per series per date.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "has_series": "series_id IS NOT NULL",
    "has_date": "obs_date IS NOT NULL",
    "has_value": "value IS NOT NULL",
})
def fred_daily():
    raw = spark.read.table("stocks.bronze.fred_observations_raw")  # noqa: F821
    typed = raw.select(
        "series_id",
        F.to_date("date").alias("obs_date"),
        F.col("value").cast("double").alias("value"),
        "name", "region", "asset_class", "unit",
        "_source_file", "_file_modified_at",
    )
    return latest_file_wins(typed, ["series_id", "obs_date"])


# ---------------------------------------------------------------------------
# Global instruments in one long table: FRED series + US-listed country ETFs
# ---------------------------------------------------------------------------
# Country ETFs come from the Massive data we already load, giving daily equity
# coverage for markets FRED doesn't carry (Europe, China, Korea, Brazil...).
COUNTRY_ETFS = [
    # (ticker, name, region)
    ("EWJ", "Japan (iShares MSCI Japan)", "Japan"),
    ("EWG", "Germany (iShares MSCI Germany)", "Germany"),
    ("EWU", "UK (iShares MSCI United Kingdom)", "UK"),
    ("EZU", "Eurozone (iShares MSCI Eurozone)", "Eurozone"),
    ("VGK", "Europe (Vanguard FTSE Europe)", "Europe"),
    ("FXI", "China large caps (iShares China Large-Cap)", "China"),
    ("EWH", "Hong Kong (iShares MSCI Hong Kong)", "Hong Kong"),
    ("EWY", "South Korea (iShares MSCI South Korea)", "South Korea"),
    ("EWT", "Taiwan (iShares MSCI Taiwan)", "Taiwan"),
    ("EWZ", "Brazil (iShares MSCI Brazil)", "Brazil"),
    ("EEM", "Emerging markets (iShares MSCI EM)", "Emerging markets"),
    ("INDA", "India (iShares MSCI India)", "India"),
]

GLOBAL_COLUMNS = ["instrument", "name", "region", "asset_class", "unit", "source", "obs_date", "value"]


# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.global_daily AS
#   SELECT series_id AS instrument, name, region, asset_class, unit, 'FRED' AS source, obs_date, value
#   FROM stocks.silver.fred_daily
#   UNION ALL
#   SELECT u.symbol, e.name, e.region, 'equity_etf', 'USD', 'Massive', u.trade_date, u.close
#   FROM stocks.silver.us_daily u
#   JOIN stocks.silver.country_etfs e ON u.symbol = e.symbol;
# Reference lists are pipeline tables (not inline DataFrames) so that the views reading them keep
# only Delta sources and can refresh incrementally instead of recomputing in full every update.
@dp.materialized_view(
    name="stocks.silver.country_etfs",
    comment="Reference: the US-listed country ETFs used as world-market gauges (ticker, name, region).",
    table_properties={"quality": "silver"},
)
def country_etfs():
    return spark.createDataFrame(COUNTRY_ETFS, "symbol STRING, name STRING, region STRING")  # noqa: F821


@dp.materialized_view(
    name="stocks.silver.global_daily",
    comment="Global instruments (FRED series + US-listed country ETFs), one row per instrument per date.",
    table_properties={"quality": "silver"},
)
def global_daily():
    fred = spark.read.table("stocks.silver.fred_daily").select(  # noqa: F821
        F.col("series_id").alias("instrument"), "name", "region", "asset_class", "unit",
        F.lit("FRED").alias("source"), "obs_date", "value",
    )
    etf_list = spark.read.table("stocks.silver.country_etfs")  # noqa: F821
    etfs = (
        spark.read.table("stocks.silver.us_daily")  # noqa: F821
        .join(etf_list, "symbol")
        .select(
            F.col("symbol").alias("instrument"), "name", "region",
            F.lit("equity_etf").alias("asset_class"), F.lit("USD").alias("unit"),
            F.lit("Massive").alias("source"),
            F.col("trade_date").alias("obs_date"), F.col("close").alias("value"),
        )
    )
    return fred.select(*GLOBAL_COLUMNS).unionByName(etfs.select(*GLOBAL_COLUMNS))


# ---------------------------------------------------------------------------
# Paper trades (dummy money) from bumblebee26/paper-trader
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.trade_events (
#     CONSTRAINT has_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW,
#     CONSTRAINT has_event    EXPECT (event IS NOT NULL)    ON VIOLATION DROP ROW,
#     CONSTRAINT has_time     EXPECT (event_ts IS NOT NULL) ON VIOLATION DROP ROW
#   ) AS
#   SELECT * EXCEPT (rn) FROM (
#     SELECT trade_date, event_id, proposal_id,
#            to_timestamp(timestamp_ist, 'yyyy-MM-dd HH:mm:ss') AS event_ts,
#            event, symbol, setup, side, entry, stop, target, CAST(qty AS INT) AS qty, risk_rs, rr,
#            reasons, user_reply, fill_price, exit_price, exit_reason, pnl_rs, fees_rs, note,
#            balance, starting_capital, realized_today, _source_file, _file_modified_at,
#            row_number() OVER (PARTITION BY event_id ORDER BY _file_modified_at DESC) AS rn
#     FROM stocks.bronze.trades_raw
#   ) WHERE rn = 1;
@dp.materialized_view(
    name="stocks.silver.trade_events",
    comment="Paper-trader journal events (dummy money), typed, one row per event_id (newest export wins).",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "has_event_id": "event_id IS NOT NULL",
    "has_event": "event IS NOT NULL",
    "has_time": "event_ts IS NOT NULL",
})
def trade_events():
    raw = spark.read.table("stocks.bronze.trades_raw")  # noqa: F821
    typed = raw.select(
        "trade_date", "event_id", "proposal_id",
        F.to_timestamp("timestamp_ist", "yyyy-MM-dd HH:mm:ss").alias("event_ts"),
        "event", "symbol", "setup", "side", "entry", "stop", "target",
        F.col("qty").cast("int").alias("qty"),
        "risk_rs", "rr", "reasons", "user_reply", "fill_price", "exit_price", "exit_reason",
        "pnl_rs", "fees_rs", "note", "balance", "starting_capital", "realized_today",
        "_source_file", "_file_modified_at",
    )
    return latest_file_wins(typed, ["event_id"])


# ---------------------------------------------------------------------------
# Company news (NSE RSS)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.news (
#     CONSTRAINT has_id    EXPECT (id IS NOT NULL)    ON VIOLATION DROP ROW,
#     CONSTRAINT has_title EXPECT (title IS NOT NULL) ON VIOLATION DROP ROW
#   ) AS
#   SELECT id, feed, symbol, company, category, title, description, link,
#          to_timestamp(published_ist, 'yyyy-MM-dd HH:mm:ss') AS published_ts,
#          to_timestamp(fetched_at_ist, 'yyyy-MM-dd HH:mm:ss') AS first_seen_ts, ...
#   FROM stocks.bronze.news_raw   -- earliest snapshot of each id wins
@dp.materialized_view(
    name="stocks.silver.news",
    comment="NSE company news, one row per item: symbol (where it could be matched), category, "
            "title, link, published and first-seen times (IST).",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "has_id": "id IS NOT NULL",
    "has_title": "title IS NOT NULL AND title <> ''",
})
def news():
    raw = spark.read.table("stocks.bronze.news_raw")  # noqa: F821
    typed = raw.select(
        "id", "feed", "symbol", "company", "category", "title", "description", "link",
        F.to_timestamp("published_ist", "yyyy-MM-dd HH:mm:ss").alias("published_ts"),
        F.to_timestamp("fetched_at_ist", "yyyy-MM-dd HH:mm:ss").alias("first_seen_ts"),
        "published_raw",
    )
    first = Window.partitionBy("id").orderBy(F.col("first_seen_ts").asc())
    return (
        typed.withColumn("rn", F.row_number().over(first))
        .filter("rn = 1").drop("rn")
        .withColumn("news_ts", F.coalesce("published_ts", "first_seen_ts"))
        .withColumn("news_date", F.to_date("news_ts"))
    )


# ---------------------------------------------------------------------------
# Company fundamentals
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.fundamentals (
#     CONSTRAINT has_keys  EXPECT (symbol IS NOT NULL AND metric IS NOT NULL AND period_end IS NOT NULL) ON VIOLATION DROP ROW,
#     CONSTRAINT has_value EXPECT (value IS NOT NULL) ON VIOLATION DROP ROW
#   ) AS SELECT symbol, metric, period_type, to_date(period_end) AS period_end, value, currency, source, fetched_at
#   FROM stocks.bronze.fundamentals_raw   -- newest file per symbol x metric x period wins
@dp.materialized_view(
    name="stocks.silver.fundamentals",
    comment="Company financials, one row per NSE symbol x metric x period (12M annual, 3M quarterly), INR. "
            "A restated figure from a newer weekly fetch replaces the older one.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "has_keys": "symbol IS NOT NULL AND metric IS NOT NULL AND period_end IS NOT NULL",
    "has_value": "value IS NOT NULL",
})
def fundamentals():
    raw = spark.read.table("stocks.bronze.fundamentals_raw")  # noqa: F821
    newest = Window.partitionBy("symbol", "metric", "period_type", "period_end").orderBy(
        F.col("_file_modified_at").desc())
    return (
        raw.withColumn("rn", F.row_number().over(newest)).filter("rn = 1")
        .select("symbol", "metric", "period_type", F.to_date("period_end").alias("period_end"), "value",
                "currency", "source", F.to_timestamp("fetched_at_ist", "yyyy-MM-dd HH:mm:ss").alias("fetched_at"))
    )


# ---------------------------------------------------------------------------
# Paper-trader portfolio (dummy money)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.portfolio_pools AS
#   SELECT to_date(snapshot_date) AS snapshot_date, pool, capital, cash, holdings_value, equity
#   FROM stocks.bronze.portfolio_raw WHERE record_type = 'pool'   -- newest file per day wins
def _latest_portfolio_file():
    raw = spark.read.table("stocks.bronze.portfolio_raw").withColumn(  # noqa: F821
        "snapshot_date", F.to_date("snapshot_date"))
    newest = Window.partitionBy("snapshot_date").orderBy(F.col("_file_modified_at").desc())
    files = (raw.select("snapshot_date", "_source_file", "_file_modified_at").distinct()
             .withColumn("rn", F.row_number().over(newest)).filter("rn = 1").select("snapshot_date", "_source_file"))
    return raw.join(files, ["snapshot_date", "_source_file"])


@dp.materialized_view(
    name="stocks.silver.portfolio_pools",
    comment="Paper-trader pools (intraday, swing, long_term) per day: capital, cash, holdings value, equity (dummy Rs).",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({"has_keys": "snapshot_date IS NOT NULL AND pool IS NOT NULL", "has_equity": "equity IS NOT NULL"})
def portfolio_pools():
    return (_latest_portfolio_file().filter("record_type = 'pool'")
            .select("snapshot_date", "pool", "capital", "cash", "holdings_value", "equity"))


# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.portfolio_holdings AS
#   SELECT to_date(snapshot_date) AS snapshot_date, pool, symbol, sector, qty, avg_cost, last_price, value, pnl,
#          to_date(opened) AS opened FROM stocks.bronze.portfolio_raw WHERE record_type = 'holding';
@dp.materialized_view(
    name="stocks.silver.portfolio_holdings",
    comment="Paper-trader holdings per day: pool, symbol, sector, quantity, average cost, last price, value, P&L (dummy Rs).",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({"has_keys": "snapshot_date IS NOT NULL AND pool IS NOT NULL AND symbol IS NOT NULL"})
def portfolio_holdings():
    return (_latest_portfolio_file().filter("record_type = 'holding'")
            .select("snapshot_date", "pool", "symbol", "sector", "qty", "avg_cost", "last_price", "value", "pnl",
                    F.to_date("opened").alias("opened")))


# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.shadow_pools AS
#   SELECT to_date(snapshot_date) AS snapshot_date, pool, capital, cash, holdings_value, equity
#   FROM stocks.bronze.portfolio_raw WHERE record_type = 'shadow_pool';   -- newest file per day wins
@dp.materialized_view(
    name="stocks.silver.shadow_pools",
    comment="Paper-trader SHADOW pools per day (swing_shadow, long_term_shadow): rules tested on paper, never part "
            "of the portfolio totals. Capital, cash, holdings value, equity (dummy Rs).",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({"has_keys": "snapshot_date IS NOT NULL AND pool IS NOT NULL", "has_equity": "equity IS NOT NULL"})
def shadow_pools():
    return (_latest_portfolio_file().filter("record_type = 'shadow_pool'")
            .select("snapshot_date", "pool", "capital", "cash", "holdings_value", "equity"))


# ---------------------------------------------------------------------------
# Index members (latest list only)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.index_members AS
#   SELECT symbol, company, industry, isin, index_name, to_date(as_of) AS as_of
#   FROM stocks.bronze.index_members_raw
#   WHERE as_of = (SELECT max(as_of) FROM stocks.bronze.index_members_raw);
@dp.materialized_view(
    name="stocks.silver.index_members",
    comment="The latest NSE index constituent list (Nifty 200): symbol, company name, industry, ISIN.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({"has_symbol": "symbol IS NOT NULL AND symbol <> ''"})
def index_members():
    raw = spark.read.table("stocks.bronze.index_members_raw").withColumn("as_of", F.to_date("as_of"))  # noqa: F821
    latest = raw.agg(F.max("as_of").alias("as_of"))
    return (raw.join(latest, "as_of").dropDuplicates(["symbol"])
            .select("symbol", "company", "industry", "isin", "index_name", "as_of"))
