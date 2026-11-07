"""SILVER: cleaned, typed, de-duplicated daily bars, unified across markets.

  stocks.silver.nse_eq_daily   NSE equities (typed, one row per symbol/series/day)
  stocks.silver.us_daily       US stocks from Massive (exploded, OTC excluded)
  stocks.silver.daily_bars     both markets in ONE schema  <- what gold reads
  stocks.silver.trading_days   each market's trading calendar, derived from the data

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
