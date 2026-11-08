"""GOLD: cross-market insights built on stocks.silver.daily_bars.

  stocks.gold.unusual_volume               volume spikes vs each stock's 20-day average
  stocks.gold.market_breadth               advances/declines and % above 20-day average, per market/day
  stocks.gold.nifty_after_us_moves         each Indian session next to the US session before it
  stocks.gold.nifty_after_us_moves_summary how India opens/closes after big US up/down days
  stocks.gold.adr_premium                  Indian ADRs in New York vs their NSE shares (+ USD/INR premium)
  stocks.gold.global_markets_daily         global instruments with 1/5/20-day changes
  stocks.gold.global_snapshot              latest reading per global instrument
  stocks.gold.india_vs_global              which global moves line up with India's next open

Market proxies (no index data in these sources): NIFTYBEES (Nifty 50 ETF on NSE)
and SPY (S&P 500 ETF). Change them with pipeline configuration keys
stocks.india_proxy / stocks.us_proxy.
"""

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

    dp.materialized_view = dp.table

from pyspark.sql import Window
from pyspark.sql import functions as F

INDIA_PROXY = spark.conf.get("stocks.india_proxy", "NIFTYBEES")  # noqa: F821
US_PROXY = spark.conf.get("stocks.us_proxy", "SPY")  # noqa: F821
BIG_MOVE_PCT = float(spark.conf.get("stocks.big_move_pct", "1.0"))  # noqa: F821
SPIKE_RATIO = float(spark.conf.get("stocks.volume_spike_ratio", "3.0"))  # noqa: F821
MIN_AVG_VOLUME = float(spark.conf.get("stocks.min_avg_volume", "50000"))  # noqa: F821

# Indian companies with US-listed ADRs: (ADR ticker, NSE symbol, NSE shares per ADR).
# Ratios change after splits; check them against the depositary bank before relying on them.
ADR_PAIRS = [
    ("INFY", "INFY", 1.0),
    ("WIT", "WIPRO", 1.0),
    ("HDB", "HDFCBANK", 3.0),
    ("IBN", "ICICIBANK", 2.0),
    ("RDY", "DRREDDY", 1.0),
]


def bars():
    """Daily bars with the main series only for NSE (EQ), plus daily return in %."""
    return (
        spark.read.table("stocks.silver.daily_bars")  # noqa: F821
        .filter((F.col("market") == "US") | (F.col("series") == "EQ"))
        .withColumn("return_pct", (F.col("close") / F.col("prev_close") - 1) * 100)
    )


# ---------------------------------------------------------------------------
# Unusual volume
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.unusual_volume AS
#   WITH b AS (
#     SELECT *, (close / prev_close - 1) * 100 AS return_pct,
#            avg(volume) OVER (PARTITION BY market, symbol ORDER BY trade_date
#                              ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS avg_volume_20d,
#            count(*)    OVER (PARTITION BY market, symbol ORDER BY trade_date
#                              ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS history_days
#     FROM stocks.silver.daily_bars WHERE market = 'US' OR series = 'EQ'
#   )
#   SELECT market, symbol, trade_date, close, return_pct, volume, avg_volume_20d,
#          volume / avg_volume_20d AS volume_ratio, turnover, currency
#   FROM b
#   WHERE history_days = 20 AND avg_volume_20d >= 50000 AND volume >= 3 * avg_volume_20d;
@dp.materialized_view(
    name="stocks.gold.unusual_volume",
    comment="Days when a stock traded at least volume_spike_ratio x its prior 20-day average volume.",
    table_properties={"quality": "gold"},
)
def unusual_volume():
    prior20 = (
        Window.partitionBy("market", "symbol").orderBy("trade_date").rowsBetween(-20, -1)
    )
    b = (
        bars()
        .withColumn("avg_volume_20d", F.avg("volume").over(prior20))
        .withColumn("history_days", F.count("*").over(prior20))
    )
    return b.filter(
        (F.col("history_days") == 20)
        & (F.col("avg_volume_20d") >= MIN_AVG_VOLUME)
        & (F.col("volume") >= SPIKE_RATIO * F.col("avg_volume_20d"))
    ).select(
        "market", "symbol", "trade_date", "close", "return_pct", "volume", "avg_volume_20d",
        (F.col("volume") / F.col("avg_volume_20d")).alias("volume_ratio"),
        "turnover", "currency",
    )


# ---------------------------------------------------------------------------
# Market breadth
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.market_breadth AS
#   WITH b AS (
#     SELECT *, (close / prev_close - 1) * 100 AS return_pct,
#            avg(close) OVER (PARTITION BY market, symbol ORDER BY trade_date
#                             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20
#     FROM stocks.silver.daily_bars WHERE market = 'US' OR series = 'EQ'
#   )
#   SELECT market, trade_date,
#          count_if(close > prev_close) AS advances,
#          count_if(close < prev_close) AS declines,
#          count_if(close = prev_close) AS unchanged,
#          count_if(close > prev_close) / nullif(count_if(close < prev_close), 0) AS advance_decline_ratio,
#          percentile_approx(return_pct, 0.5) AS median_return_pct,
#          100 * avg(CASE WHEN close > ma20 THEN 1 ELSE 0 END) AS pct_above_ma20,
#          count(*) AS stocks
#   FROM b WHERE prev_close IS NOT NULL
#   GROUP BY market, trade_date;
@dp.materialized_view(
    name="stocks.gold.market_breadth",
    comment="Per market and day: advances, declines, median return, % of stocks above their 20-day average.",
    table_properties={"quality": "gold"},
)
def market_breadth():
    ma20 = Window.partitionBy("market", "symbol").orderBy("trade_date").rowsBetween(-19, 0)
    b = bars().withColumn("ma20", F.avg("close").over(ma20)).filter(F.col("prev_close").isNotNull())
    advances = F.sum(F.when(F.col("close") > F.col("prev_close"), 1).otherwise(0))
    declines = F.sum(F.when(F.col("close") < F.col("prev_close"), 1).otherwise(0))
    return b.groupBy("market", "trade_date").agg(
        advances.alias("advances"),
        declines.alias("declines"),
        F.sum(F.when(F.col("close") == F.col("prev_close"), 1).otherwise(0)).alias("unchanged"),
        (advances / F.when(declines == 0, None).otherwise(declines)).alias("advance_decline_ratio"),
        F.percentile_approx("return_pct", 0.5).alias("median_return_pct"),
        (100 * F.avg(F.when(F.col("close") > F.col("ma20"), 1).otherwise(0))).alias("pct_above_ma20"),
        F.count("*").alias("stocks"),
    )


# ---------------------------------------------------------------------------
# How India trades after the US session before it
# ---------------------------------------------------------------------------
# The US closes at about 01:30-02:30 IST, so the US session dated D-1 (New York)
# finishes before India opens on D. Each Indian day is matched to the latest US
# trading day strictly before it.
#
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.nifty_after_us_moves AS
#   WITH india AS (
#     SELECT trade_date AS india_date, open, close, prev_close,
#            (open / prev_close - 1) * 100  AS india_gap_pct,
#            (close / prev_close - 1) * 100 AS india_return_pct,
#            (close / open - 1) * 100       AS india_intraday_pct
#     FROM stocks.silver.daily_bars
#     WHERE market = 'IN' AND series = 'EQ' AND symbol = 'NIFTYBEES'
#   ), us AS (
#     SELECT trade_date AS us_date, (close / prev_close - 1) * 100 AS us_return_pct
#     FROM stocks.silver.daily_bars WHERE market = 'US' AND symbol = 'SPY'
#   ), matched AS (
#     SELECT i.*, u.us_date, u.us_return_pct,
#            row_number() OVER (PARTITION BY i.india_date ORDER BY u.us_date DESC) AS rn
#     FROM india i JOIN us u ON u.us_date < i.india_date
#                           AND u.us_date >= date_sub(i.india_date, 7)
#   )
#   SELECT india_date, us_date, us_return_pct,
#          CASE WHEN us_return_pct >=  1.0 THEN 'US up big'
#               WHEN us_return_pct <= -1.0 THEN 'US down big'
#               ELSE 'US flat' END AS us_move,
#          india_gap_pct, india_return_pct, india_intraday_pct
#   FROM matched WHERE rn = 1;
@dp.materialized_view(
    name="stocks.gold.nifty_after_us_moves",
    comment="Each Indian session (Nifty proxy) next to the preceding US session (S&P 500 proxy).",
    table_properties={"quality": "gold"},
)
def nifty_after_us_moves():
    b = bars()
    india = b.filter((F.col("market") == "IN") & (F.col("symbol") == INDIA_PROXY)).select(
        F.col("trade_date").alias("india_date"),
        ((F.col("open") / F.col("prev_close") - 1) * 100).alias("india_gap_pct"),
        F.col("return_pct").alias("india_return_pct"),
        ((F.col("close") / F.col("open") - 1) * 100).alias("india_intraday_pct"),
    )
    us = b.filter((F.col("market") == "US") & (F.col("symbol") == US_PROXY)).select(
        F.col("trade_date").alias("us_date"),
        F.col("return_pct").alias("us_return_pct"),
    )
    matched = india.join(
        us,
        (F.col("us_date") < F.col("india_date"))
        & (F.col("us_date") >= F.date_sub(F.col("india_date"), 7)),
    )
    latest_us = Window.partitionBy("india_date").orderBy(F.col("us_date").desc())
    matched = matched.withColumn("_rn", F.row_number().over(latest_us)).filter("_rn = 1").drop("_rn")
    return matched.withColumn(
        "us_move",
        F.when(F.col("us_return_pct") >= BIG_MOVE_PCT, "US up big")
        .when(F.col("us_return_pct") <= -BIG_MOVE_PCT, "US down big")
        .otherwise("US flat"),
    ).filter(F.col("us_return_pct").isNotNull() & F.col("india_return_pct").isNotNull())


# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.nifty_after_us_moves_summary AS
#   SELECT us_move,
#          count(*)                                   AS sessions,
#          avg(us_return_pct)                         AS avg_us_return_pct,
#          avg(india_gap_pct)                         AS avg_india_gap_pct,
#          avg(india_return_pct)                      AS avg_india_return_pct,
#          100 * avg(CASE WHEN india_return_pct > 0 THEN 1 ELSE 0 END) AS pct_india_up_days,
#          100 * avg(CASE WHEN sign(india_gap_pct) = sign(us_return_pct) THEN 1 ELSE 0 END)
#                                                     AS pct_gap_same_direction_as_us,
#          corr(us_return_pct, india_gap_pct)         AS corr_us_return_india_gap
#   FROM stocks.gold.nifty_after_us_moves
#   GROUP BY us_move;
@dp.materialized_view(
    name="stocks.gold.nifty_after_us_moves_summary",
    comment="How the Nifty proxy opens and closes after US up/down/flat sessions.",
    table_properties={"quality": "gold"},
)
def nifty_after_us_moves_summary():
    m = spark.read.table("stocks.gold.nifty_after_us_moves")  # noqa: F821
    return m.groupBy("us_move").agg(
        F.count("*").alias("sessions"),
        F.avg("us_return_pct").alias("avg_us_return_pct"),
        F.avg("india_gap_pct").alias("avg_india_gap_pct"),
        F.avg("india_return_pct").alias("avg_india_return_pct"),
        (100 * F.avg(F.when(F.col("india_return_pct") > 0, 1).otherwise(0))).alias("pct_india_up_days"),
        (100 * F.avg(F.when(F.signum("india_gap_pct") == F.signum("us_return_pct"), 1).otherwise(0)))
        .alias("pct_gap_same_direction_as_us"),
        F.corr("us_return_pct", "india_gap_pct").alias("corr_us_return_india_gap"),
    )


# ---------------------------------------------------------------------------
# ADR premium (Indian ADRs in New York vs their NSE shares)
# ---------------------------------------------------------------------------
# For each pair, implied_inr_per_usd = NSE close x shares per ADR / ADR close.
#   relative_premium_pct: vs the median implied rate across pairs that day (needs no FX feed).
#   fx_premium_pct:       vs the actual USD/INR rate from FRED (DEXINUS, released weekly, so the
#                         latest rate up to 10 days old is used; fx_date shows which one).
# Positive = the ADR is expensive compared with buying the shares in India.
#
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.adr_premium AS
#   WITH pairs AS (
#     SELECT * FROM VALUES ('INFY','INFY',1.0), ('WIT','WIPRO',1.0), ('HDB','HDFCBANK',3.0),
#                          ('IBN','ICICIBANK',2.0), ('RDY','DRREDDY',1.0)
#            AS p(adr, nse_symbol, shares_per_adr)
#   ), joined AS (
#     SELECT p.adr, p.nse_symbol, p.shares_per_adr, u.trade_date,
#            u.close AS adr_close_usd, n.close AS nse_close_inr,
#            n.close * p.shares_per_adr / u.close AS implied_inr_per_usd
#     FROM pairs p
#     JOIN stocks.silver.daily_bars u ON u.market = 'US' AND u.symbol = p.adr
#     JOIN stocks.silver.daily_bars n ON n.market = 'IN' AND n.series = 'EQ'
#                                    AND n.symbol = p.nse_symbol AND n.trade_date = u.trade_date
#   )
#   ), fx AS (   -- latest USD/INR (FRED DEXINUS) on or before each trade date, at most 10 days old
#     SELECT j.adr, j.trade_date, f.value AS fx_inr_per_usd, f.obs_date AS fx_date,
#            row_number() OVER (PARTITION BY j.adr, j.trade_date ORDER BY f.obs_date DESC) AS rn
#     FROM joined j JOIN stocks.silver.fred_daily f
#       ON f.series_id = 'DEXINUS' AND f.obs_date <= j.trade_date AND f.obs_date >= date_sub(j.trade_date, 10)
#   )
#   SELECT j.*,
#          percentile_approx(implied_inr_per_usd, 0.5) OVER (PARTITION BY j.trade_date) AS reference_inr_per_usd,
#          (percentile_approx(implied_inr_per_usd, 0.5) OVER (PARTITION BY j.trade_date)
#             / implied_inr_per_usd - 1) * 100 AS relative_premium_pct,
#          fx.fx_inr_per_usd, fx.fx_date,
#          (fx.fx_inr_per_usd / implied_inr_per_usd - 1) * 100 AS fx_premium_pct
#   FROM joined j LEFT JOIN fx ON fx.adr = j.adr AND fx.trade_date = j.trade_date AND fx.rn = 1;
@dp.materialized_view(
    name="stocks.gold.adr_premium",
    comment="Indian ADRs vs NSE shares: implied INR/USD per pair and premium vs the day's median pair.",
    table_properties={"quality": "gold"},
)
def adr_premium():
    pairs = spark.createDataFrame(ADR_PAIRS, "adr STRING, nse_symbol STRING, shares_per_adr DOUBLE")  # noqa: F821
    b = bars()
    us = b.filter(F.col("market") == "US").select(
        F.col("symbol").alias("adr"), "trade_date", F.col("close").alias("adr_close_usd")
    )
    nse = b.filter(F.col("market") == "IN").select(
        F.col("symbol").alias("nse_symbol"), "trade_date", F.col("close").alias("nse_close_inr")
    )
    joined = (
        pairs.join(us, "adr")
        .join(nse, ["nse_symbol", "trade_date"])
        .withColumn(
            "implied_inr_per_usd",
            F.col("nse_close_inr") * F.col("shares_per_adr") / F.col("adr_close_usd"),
        )
    )
    day = Window.partitionBy("trade_date")
    joined = joined.withColumn(
        "reference_inr_per_usd", F.percentile_approx("implied_inr_per_usd", 0.5).over(day)
    ).withColumn(
        "relative_premium_pct",
        (F.col("reference_inr_per_usd") / F.col("implied_inr_per_usd") - 1) * 100,
    )

    fx = (
        spark.read.table("stocks.silver.fred_daily")  # noqa: F821
        .filter(F.col("series_id") == "DEXINUS")
        .select(F.col("obs_date").alias("fx_date"), F.col("value").alias("fx_inr_per_usd"))
    )
    with_fx = joined.join(
        fx,
        (F.col("fx_date") <= F.col("trade_date")) & (F.col("fx_date") >= F.date_sub(F.col("trade_date"), 10)),
        "left",
    )
    latest_fx = Window.partitionBy("adr", "trade_date").orderBy(F.col("fx_date").desc_nulls_last())
    return (
        with_fx.withColumn("_rn", F.row_number().over(latest_fx))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("fx_premium_pct", (F.col("fx_inr_per_usd") / F.col("implied_inr_per_usd") - 1) * 100)
    )


# ---------------------------------------------------------------------------
# Global markets with changes over 1, 5 and 20 observations
# ---------------------------------------------------------------------------
# change_pct suits prices and FX; for rates (asset_class = 'rates', in percent) use change_abs
# (e.g. +0.05 = +5 basis points).
#
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.global_markets_daily AS
#   SELECT *,
#          value - lag(value) OVER w                       AS change_abs,
#          (value / lag(value)     OVER w - 1) * 100       AS change_pct,
#          (value / lag(value, 5)  OVER w - 1) * 100       AS change_5d_pct,
#          (value / lag(value, 20) OVER w - 1) * 100       AS change_20d_pct
#   FROM stocks.silver.global_daily
#   WINDOW w AS (PARTITION BY instrument ORDER BY obs_date);
@dp.materialized_view(
    name="stocks.gold.global_markets_daily",
    comment="Global instruments (indices, country ETFs, FX, VIX, oil, rates) with 1/5/20-observation changes.",
    table_properties={"quality": "gold"},
)
def global_markets_daily():
    w = Window.partitionBy("instrument").orderBy("obs_date")
    g = spark.read.table("stocks.silver.global_daily")  # noqa: F821
    return (
        g.withColumn("change_abs", F.col("value") - F.lag("value").over(w))
        .withColumn("change_pct", (F.col("value") / F.lag("value").over(w) - 1) * 100)
        .withColumn("change_5d_pct", (F.col("value") / F.lag("value", 5).over(w) - 1) * 100)
        .withColumn("change_20d_pct", (F.col("value") / F.lag("value", 20).over(w) - 1) * 100)
    )


# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.global_snapshot AS
#   SELECT * EXCEPT (rn) FROM (
#     SELECT *, row_number() OVER (PARTITION BY instrument ORDER BY obs_date DESC) AS rn
#     FROM stocks.gold.global_markets_daily
#   ) WHERE rn = 1;
@dp.materialized_view(
    name="stocks.gold.global_snapshot",
    comment="Latest reading per global instrument, with its changes and as-of date.",
    table_properties={"quality": "gold"},
)
def global_snapshot():
    latest = Window.partitionBy("instrument").orderBy(F.col("obs_date").desc())
    return (
        spark.read.table("stocks.gold.global_markets_daily")  # noqa: F821
        .withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Which global moves line up with India's next open?
# ---------------------------------------------------------------------------
# For each Indian session (Nifty proxy), take each instrument's latest change dated strictly
# BEFORE the Indian date (information available before 09:15 IST), then measure over history:
# correlation with India's opening gap and full-day return, and how often they share a direction.
#
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.india_vs_global AS
#   WITH pairs AS (
#     SELECT i.india_date, i.india_gap_pct, i.india_return_pct,
#            g.instrument, g.name, g.region, g.asset_class, g.change_pct,
#            row_number() OVER (PARTITION BY i.india_date, g.instrument ORDER BY g.obs_date DESC) AS rn
#     FROM stocks.gold.nifty_after_us_moves i
#     JOIN stocks.gold.global_markets_daily g
#       ON g.obs_date < i.india_date AND g.obs_date >= date_sub(i.india_date, 7)
#     WHERE g.change_pct IS NOT NULL
#   )
#   SELECT instrument, name, region, asset_class,
#          count(*)                                AS sessions,
#          corr(change_pct, india_gap_pct)         AS corr_with_india_gap,
#          corr(change_pct, india_return_pct)      AS corr_with_india_return,
#          100 * avg(CASE WHEN sign(change_pct) = sign(india_gap_pct) THEN 1 ELSE 0 END) AS pct_same_direction_gap
#   FROM pairs WHERE rn = 1
#   GROUP BY instrument, name, region, asset_class
#   HAVING count(*) >= 30;
@dp.materialized_view(
    name="stocks.gold.india_vs_global",
    comment="Per global instrument: how its previous move relates to India's next open and day (Nifty proxy).",
    table_properties={"quality": "gold"},
)
def india_vs_global():
    india = spark.read.table("stocks.gold.nifty_after_us_moves").select(  # noqa: F821
        "india_date", "india_gap_pct", "india_return_pct"
    )
    g = (
        spark.read.table("stocks.gold.global_markets_daily")  # noqa: F821
        .filter(F.col("change_pct").isNotNull())
        .select("instrument", "name", "region", "asset_class", "obs_date", "change_pct")
    )
    pairs = india.join(
        g,
        (F.col("obs_date") < F.col("india_date")) & (F.col("obs_date") >= F.date_sub(F.col("india_date"), 7)),
    )
    latest = Window.partitionBy("india_date", "instrument").orderBy(F.col("obs_date").desc())
    pairs = pairs.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn")
    same_dir = F.when(F.signum("change_pct") == F.signum("india_gap_pct"), 1).otherwise(0)
    return (
        pairs.groupBy("instrument", "name", "region", "asset_class")
        .agg(
            F.count("*").alias("sessions"),
            F.corr("change_pct", "india_gap_pct").alias("corr_with_india_gap"),
            F.corr("change_pct", "india_return_pct").alias("corr_with_india_return"),
            (100 * F.avg(same_dir)).alias("pct_same_direction_gap"),
        )
        .filter(F.col("sessions") >= 30)
    )
