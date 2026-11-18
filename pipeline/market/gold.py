"""MARKET pipeline (stocks-market): market data in, cleaned, and the light market summaries. Runs after the NSE evening
file (job stocks-market-close, then the research pipeline), after the 08:45 IST pre-open snapshot Mon-Fri (job
stocks-market-preopen), at 11:15 IST Tue-Sat (job stocks-market-morning: US prices, FRED, the F&O list) and on
Saturday at 08:00 (job stocks-market-weekend: fundamentals, the Nifty 200 list).

GOLD: the tables the dashboard, the morning brief and the paper trader read.

  stocks.gold.unusual_volume                 volume spikes vs each stock's 20-day average
  stocks.gold.market_breadth                 advances/declines and % above 20-day average, per market/day
  stocks.gold.nifty_after_us_moves           each Indian session next to the US session before it
  stocks.gold.nifty_after_us_moves_summary   how India opens/closes after big US up/down days
  stocks.silver.adr_pairs                    Indian ADR / NSE pairs
  stocks.gold.adr_premium                    Indian ADRs in New York vs their NSE shares (+ USD/INR premium)
  stocks.gold.global_markets_daily           global instruments with 1/5/20-day changes
  stocks.gold.global_snapshot                latest reading per global instrument (FRED/Massive or the pre-open Yahoo snapshot)
  stocks.gold.india_vs_global                which global moves line up with India's next open
  stocks.gold.company_fundamentals           per company: profitability, debt, cash, growth, P/E and the long-term pool's quality tests
  stocks.gold.longterm_candidates            long-term pool: Nifty 200 companies ranked, this week's 10 picks with target weights and reasons
  stocks.gold.intraday_universe              Nifty 200 stocks the trader may trade intraday (liquidity guardrails), flagged on news/moves
  stocks.gold.swing_signals                  results/news breakouts the swing pool tests in shadow mode

Tables of the research and trading pipelines are never defined here; the three pipelines never run at
the same time (Free Edition allows one active pipeline).

Market proxies (no index data in these sources): NIFTYBEES (Nifty 50 ETF on NSE) and SPY (S&P 500 ETF);
change them with pipeline configuration keys stocks.india_proxy / stocks.us_proxy.
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
#   WITH pairs AS (SELECT * FROM stocks.silver.adr_pairs   -- ('INFY','INFY',1.0), ('HDB','HDFCBANK',3.0), ...
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
    name="stocks.silver.adr_pairs",
    comment="Reference: Indian ADRs and their NSE shares (adr, nse_symbol, shares_per_adr), from ADR_PAIRS.",
    table_properties={"quality": "silver"},
)
def adr_pairs():
    # A pipeline table rather than an inline DataFrame, so adr_premium reads only Delta sources and can
    # refresh incrementally. Ratios are hand-maintained in ADR_PAIRS above.
    return spark.createDataFrame(ADR_PAIRS, "adr STRING, nse_symbol STRING, shares_per_adr DOUBLE")  # noqa: F821

@dp.materialized_view(
    name="stocks.gold.adr_premium",
    comment="Indian ADRs vs NSE shares: implied INR/USD per pair and premium vs the day's median pair.",
    table_properties={"quality": "gold"},
)
def adr_premium():
    pairs = spark.read.table("stocks.silver.adr_pairs")  # noqa: F821
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

# Latest reading per instrument: the newest of FRED/Massive (global_markets_daily) and the 08:45 IST Yahoo snapshot
# (silver.premarket_quotes); on the same date FRED/Massive wins. Yahoo rows carry Yahoo's own changes, so a reading
# never mixes two sources (Brent: FRED has spot, Yahoo the front-month future). source is 'Yahoo live' for a session
# still trading at 08:45 IST (Asia, futures, currencies) and 'Yahoo' for a finished one.
#
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.global_snapshot AS
#   WITH r AS (
#     SELECT *, 0 AS pref FROM stocks.gold.global_markets_daily
#     UNION ALL
#     SELECT instrument, name, region, asset_class, unit,
#            CASE WHEN reading = 'live' THEN 'Yahoo live' ELSE 'Yahoo' END AS source, obs_date, value,
#            change_abs, change_pct, change_5d_pct, change_20d_pct, 1 AS pref
#     FROM stocks.silver.premarket_quotes
#   )
#   SELECT * EXCEPT (rn, pref) FROM (
#     SELECT *, row_number() OVER (PARTITION BY instrument ORDER BY obs_date DESC, pref) AS rn FROM r
#   ) WHERE rn = 1;
SNAPSHOT_COLUMNS = ["instrument", "name", "region", "asset_class", "unit", "source", "obs_date", "value",
                    "change_abs", "change_pct", "change_5d_pct", "change_20d_pct"]

@dp.materialized_view(
    name="stocks.gold.global_snapshot",
    comment="Latest reading per global instrument (FRED/Massive or the 08:45 IST Yahoo snapshot, whichever is newer; "
            "FRED/Massive on a tie), with its changes and as-of date. source 'Yahoo live' = that market was still "
            "trading at 08:45 IST.",
    table_properties={"quality": "gold"},
)
def global_snapshot():
    official = (
        spark.read.table("stocks.gold.global_markets_daily")  # noqa: F821
        .select(*SNAPSHOT_COLUMNS)
        .withColumn("_pref", F.lit(0))
    )
    yahoo = (
        spark.read.table("stocks.silver.premarket_quotes")  # noqa: F821
        .withColumn("source", F.when(F.col("reading") == "live", F.lit("Yahoo live")).otherwise(F.lit("Yahoo")))
        .select(*SNAPSHOT_COLUMNS)
        .withColumn("_pref", F.lit(1))
    )
    latest = Window.partitionBy("instrument").orderBy(F.col("obs_date").desc(), F.col("_pref"))
    return (
        official.unionByName(yahoo)
        .withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn", "_pref")
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

# ---------------------------------------------------------------------------
# Company fundamentals: the long-term pool's quality tests (system plan, phase 4)
# ---------------------------------------------------------------------------
# One row per company from the latest annual figures (fiscal years end 31 March), the annual
# figures 3 years earlier, the last 4 annual cash flows, the last quarters and the latest NSE
# close. Banks and lenders (no EBIT reported) are judged on return on assets instead of return
# on capital; their debt and cash-flow tests are left null (deposits make them meaningless).
# Not available from this source: promoter pledges and holding, bad-loan ratios, P/E history.
# P/E is compared with the other companies here instead (pe_percentile, 0 = cheapest).
#
# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.company_fundamentals AS
#   WITH annual AS (SELECT symbol, period_end, FIRST(CASE WHEN metric = 'EBIT' THEN value END, true) AS EBIT, ...
#                   FROM stocks.silver.fundamentals
#                   WHERE period_type = '12M' GROUP BY symbol, period_end), ...
#   SELECT symbol, fiscal_year_end, revenue_cr, net_income_cr, roce_pct, roe_pct, roa_pct, debt_to_equity,
#          ocf_positive_years, revenue_cagr_3y_pct, profit_cagr_3y_pct, eps_ttm, close, pe, pe_percentile,
#          profit_down_2q, pass_capital, pass_balance, pass_cash, pass_growth, pass_price, tests_passed,
#          tests_available, quality_score FROM ...;
ANNUAL_METRICS = ["TotalRevenue", "NetIncome", "EBIT", "TotalDebt", "StockholdersEquity", "OperatingCashFlow",
                  "InvestedCapital", "DilutedEPS", "TotalAssets"]

QUARTERLY_METRICS = ["TotalRevenue", "NetIncome", "DilutedEPS"]

def _cagr(now, then, years):
    return F.when((now > 0) & (then > 0) & (years > 0),
                  (F.pow(now / then, 1 / years) - 1) * 100)

def _metric_columns(metrics):
    """One column per metric from long rows (metric, value): conditional aggregation instead of
    GroupedData.pivot, which Lakeflow Declarative Pipelines doesn't support."""
    return [F.first(F.when(F.col("metric") == m, F.col("value")), ignorenulls=True).alias(m) for m in metrics]

@dp.materialized_view(
    name="stocks.gold.company_fundamentals",
    comment="Per NSE company: latest fiscal-year profitability (ROCE, ROE, ROA), debt to equity, years of "
            "positive operating cash flow, 3-year revenue and profit growth, trailing EPS, P/E and its "
            "percentile, two-quarter profit decline, and the long-term pool's pass/fail quality tests.",
    table_properties={"quality": "gold"},
)
def company_fundamentals():
    f = spark.read.table("stocks.silver.fundamentals")  # noqa: F821
    by_end = Window.partitionBy("symbol").orderBy(F.col("period_end").desc())

    annual = (f.filter("period_type = '12M'").groupBy("symbol", "period_end")
              .agg(*_metric_columns(ANNUAL_METRICS))
              .withColumn("rn", F.row_number().over(by_end)))
    latest = annual.filter("rn = 1").drop("rn").withColumnRenamed("period_end", "fiscal_year_end")
    base = annual.filter("rn = 4").select(
        "symbol", F.col("period_end").alias("base_end"),
        F.col("TotalRevenue").alias("revenue_base"), F.col("NetIncome").alias("net_income_base"))
    ocf = annual.filter("rn <= 4").groupBy("symbol").agg(
        F.sum(F.when(F.col("OperatingCashFlow") > 0, 1).otherwise(0)).alias("ocf_positive_years"),
        F.count(F.col("OperatingCashFlow")).alias("ocf_years"))

    quarterly = (f.filter("period_type = '3M'").groupBy("symbol", "period_end")
                 .agg(*_metric_columns(QUARTERLY_METRICS))
                 .withColumn("rn", F.row_number().over(by_end)))
    q = quarterly.filter("rn <= 4").groupBy("symbol").agg(
        F.sum("DilutedEPS").alias("eps_4q"), F.count("DilutedEPS").alias("eps_quarters"),
        F.min("period_end").alias("q_first"), F.max("period_end").alias("q_last"),
        F.max(F.when(F.col("rn") == 1, F.col("NetIncome"))).alias("ni_q1"),
        F.max(F.when(F.col("rn") == 2, F.col("NetIncome"))).alias("ni_q2"),
        F.max(F.when(F.col("rn") == 3, F.col("NetIncome"))).alias("ni_q3"),
        F.max(F.when(F.col("rn") == 3, F.col("period_end"))).alias("q3_end"))

    price = (spark.read.table("stocks.silver.nse_eq_daily").filter("series = 'EQ'")  # noqa: F821
             .withColumn("rn", F.row_number().over(
                 Window.partitionBy("symbol").orderBy(F.col("trade_date").desc())))
             .filter("rn = 1").select("symbol", "close", F.col("trade_date").alias("price_date")))

    years = F.months_between("fiscal_year_end", "base_end") / 12
    eps_ttm = F.when((F.col("eps_quarters") == 4) & (F.months_between("q_last", "q_first") <= 10),
                     F.col("eps_4q")).otherwise(F.col("DilutedEPS"))
    df = (latest.join(base, "symbol", "left").join(ocf, "symbol", "left").join(q, "symbol", "left")
          .join(price, "symbol", "left")
          .withColumn("is_financial", F.col("EBIT").isNull())
          .withColumn("revenue_cr", F.round(F.col("TotalRevenue") / 1e7, 1))
          .withColumn("net_income_cr", F.round(F.col("NetIncome") / 1e7, 1))
          .withColumn("roce_pct", F.round(F.col("EBIT") / F.col("InvestedCapital") * 100, 1))
          .withColumn("roe_pct", F.round(F.col("NetIncome") / F.col("StockholdersEquity") * 100, 1))
          .withColumn("roa_pct", F.round(F.col("NetIncome") / F.col("TotalAssets") * 100, 2))
          .withColumn("debt_to_equity", F.round(F.col("TotalDebt") / F.col("StockholdersEquity"), 2))
          .withColumn("revenue_cagr_3y_pct", F.round(_cagr(F.col("TotalRevenue"), F.col("revenue_base"), years), 1))
          .withColumn("profit_cagr_3y_pct", F.round(_cagr(F.col("NetIncome"), F.col("net_income_base"), years), 1))
          .withColumn("eps_ttm", F.round(eps_ttm, 2))
          .withColumn("pe", F.round(F.when(F.col("eps_ttm") > 0, F.col("close") / F.col("eps_ttm")), 1))
          .withColumn("profit_down_2q", (F.col("ni_q1") < F.col("ni_q2")) & (F.col("ni_q2") < F.col("ni_q3"))
                      & (F.months_between("q_last", "q3_end") <= 7)))
    ranked = df.filter("pe IS NOT NULL").withColumn(
        "pe_percentile", F.round(F.percent_rank().over(Window.orderBy("pe")) * 100, 0)).select("symbol", "pe_percentile")
    df = df.join(ranked, "symbol", "left")

    fin = F.col("is_financial")
    df = (df
          .withColumn("pass_capital", F.when(fin, F.col("roa_pct") >= 1.2).otherwise(F.col("roce_pct") >= 15))
          .withColumn("pass_balance", F.when(~fin, F.col("debt_to_equity") < 0.5))
          .withColumn("pass_cash", F.when(~fin & (F.col("ocf_years") >= 3),
                                          F.col("ocf_positive_years") >= F.col("ocf_years") - 1))
          .withColumn("pass_growth", (F.col("revenue_cagr_3y_pct") > 10) & (F.col("profit_cagr_3y_pct") > 10))
          .withColumn("pass_price", F.col("pe_percentile") <= 50))
    tests = ["pass_capital", "pass_balance", "pass_cash", "pass_growth", "pass_price"]
    passed = sum(F.when(F.col(t), 1).otherwise(0) for t in tests)
    available = sum(F.when(F.col(t).isNotNull(), 1).otherwise(0) for t in tests)
    return (df.withColumn("tests_passed", passed).withColumn("tests_available", available)
            .withColumn("quality_score", F.round(F.col("tests_passed") / F.col("tests_available"), 2))
            .select("symbol", "fiscal_year_end", "is_financial", "revenue_cr", "net_income_cr", "roce_pct",
                    "roe_pct", "roa_pct", "debt_to_equity", "ocf_positive_years", "ocf_years",
                    "revenue_cagr_3y_pct", "profit_cagr_3y_pct", "eps_ttm", "close", "price_date", "pe",
                    "pe_percentile", "profit_down_2q", *tests, "tests_passed", "tests_available", "quality_score"))

# ---------------------------------------------------------------------------
# Long-term pool selection (system plan, phase 4)
# ---------------------------------------------------------------------------
# Who qualifies: Nifty 200 members (all companies if the list hasn't arrived yet) with at least 3
# quality tests available, a quality score of 0.6 or more, a positive P/E, and no profit fall two
# quarters in a row.
# Ranking: score = 100 x quality_score + growth bonus (3-year profit growth, capped at 30%, / 3)
#                  - price penalty (P/E percentile / 5). Higher is better.
# Diversification: at most 2 picks per industry (2 x 9% stays under the 25% industry cap).
# Picks: the top 10 after the industry cap; 90% of the pool invested in equal parts (max 15% each),
# 10% kept as cash. Each row carries its reasons in plain words.
#
# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.longterm_candidates AS
#   WITH eligible AS (SELECT f.*, m.company, m.industry, <score> AS score FROM stocks.gold.company_fundamentals f
#                     LEFT JOIN stocks.silver.index_members m USING (symbol) WHERE <eligibility>),
#        capped AS (SELECT *, row_number() OVER (PARTITION BY coalesce(industry, symbol) ORDER BY score DESC) AS in_industry
#                   FROM eligible)
#   SELECT *, row_number() OVER (ORDER BY in_industry > 2, score DESC) AS rank, ... FROM capped;
PICKS = 10

PER_INDUSTRY = 2

INVESTED_PCT = 90.0

@dp.materialized_view(
    name="stocks.gold.longterm_candidates",
    comment="Long-term pool candidates: eligible Nifty 200 companies ranked by quality, growth and price; "
            "is_pick marks this week's 10 picks (max 2 per industry) with target weights and plain-words reasons.",
    table_properties={"quality": "gold"},
)
def longterm_candidates():
    f = spark.read.table("stocks.gold.company_fundamentals")  # noqa: F821
    m = spark.read.table("stocks.silver.index_members")  # noqa: F821
    members = m.agg(F.count(F.lit(1)).alias("member_count"))
    df = (f.join(m.select("symbol", "company", "industry"), "symbol", "left").crossJoin(members)
          .filter((F.col("member_count") == 0) | F.col("company").isNotNull())
          .filter((F.col("tests_available") >= 3) & (F.col("quality_score") >= 0.6) & (F.col("pe") > 0)
                  & ~F.coalesce(F.col("profit_down_2q"), F.lit(False))))
    score = (F.col("quality_score") * 100
             + F.least(F.greatest(F.coalesce("profit_cagr_3y_pct", F.lit(0)), F.lit(0)), F.lit(30)) / 3
             - F.coalesce("pe_percentile", F.lit(50)) / 5)
    df = df.withColumn("score", F.round(score, 1))
    in_ind = Window.partitionBy(F.coalesce("industry", "symbol")).orderBy(F.col("score").desc(), "symbol")
    df = df.withColumn("in_industry", F.row_number().over(in_ind))
    overall = Window.orderBy((F.col("in_industry") > PER_INDUSTRY).asc(), F.col("score").desc(), "symbol")
    df = df.withColumn("rank", F.row_number().over(overall))
    df = df.withColumn("is_pick", (F.col("rank") <= PICKS) & (F.col("in_industry") <= PER_INDUSTRY))
    n_picks = df.agg(F.sum(F.col("is_pick").cast("int")).alias("n_picks"))
    df = df.crossJoin(n_picks).withColumn(
        "target_weight_pct", F.when(F.col("is_pick"), F.round(F.least(F.lit(INVESTED_PCT) / F.col("n_picks"), F.lit(15.0)), 1)))

    def fmt(c, d=1):
        return F.format_number(F.col(c), d)

    reason = F.concat_ws(" ",
        F.when(F.col("is_financial"), F.concat(F.lit("Return on assets "), fmt("roa_pct", 2), F.lit("% (a bank or lender)."))
         ).otherwise(F.concat(F.lit("Return on capital "), fmt("roce_pct"), F.lit("%."))),
        F.when(~F.col("is_financial") & F.col("debt_to_equity").isNotNull(),
               F.concat(F.lit("Debt "), fmt("debt_to_equity", 2), F.lit("x equity."))),
        F.when(F.col("profit_cagr_3y_pct").isNotNull(),
               F.concat(F.lit("Profit grew "), fmt("profit_cagr_3y_pct"), F.lit("% a year over 3 years."))),
        F.concat(F.lit("P/E "), fmt("pe"), F.lit(", cheaper than "), F.format_number(F.lit(100) - F.col("pe_percentile"), 0),
                 F.lit("% of the large companies checked.")),
        F.when(F.col("ocf_years") > 0, F.concat(F.lit("Operating cash flow positive in "), F.col("ocf_positive_years").cast("string"),
                                                F.lit(" of "), F.col("ocf_years").cast("string"), F.lit(" years."))),
    )
    return df.withColumn("reasons", reason).select(
        "rank", "is_pick", "symbol", "company", "industry", "score", "quality_score", "tests_passed", "tests_available",
        "roce_pct", "roa_pct", "debt_to_equity", "revenue_cagr_3y_pct", "profit_cagr_3y_pct", "pe", "pe_percentile",
        "close", "price_date", "target_weight_pct", "reasons", "is_financial", "fiscal_year_end")

# ---------------------------------------------------------------------------
# News radar: which Nifty 200 stocks the paper trader may trade intraday beyond its 36-stock watchlist,
# and the results/news breakouts its swing pool tests in shadow mode.
# ---------------------------------------------------------------------------
# Guardrails (so dummy results stay realistic): Nifty 200 member, main-board EQ series (not the trade-to-trade
# BE/BZ series, where same-day trading isn't allowed), close >= Rs 50, and an average of Rs 50 crore or more
# traded a day over 20 days (close x volume, so no unit assumptions about the bhavcopy's turnover column).
#
# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.intraday_universe AS
#   WITH b AS (SELECT ..., avg(close * volume) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) / 1e7
#                      AS avg_turnover_cr, volume / avg(volume) OVER (... ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
#                      AS volume_ratio
#              FROM stocks.silver.daily_bars JOIN stocks.silver.index_members USING (symbol) WHERE series = 'EQ')
#   SELECT ..., close >= 50 AND avg_turnover_cr >= 50 AS eligible,
#          eligible AND (news_count > 0 OR abs(return_pct) >= 3 OR volume_ratio >= 2) AS flagged
#   FROM b LEFT JOIN (recent non-routine NSE filings per symbol) n USING (symbol)
#   WHERE trade_date = (SELECT max(trade_date) FROM b);
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.swing_signals AS
#   SELECT ... FROM b JOIN stocks.silver.news n ON n.symbol = b.symbol
#                  AND n.news_date BETWEEN date_sub(b.trade_date, 2) AND b.trade_date
#   WHERE eligible AND close > prior 20-day high AND volume_ratio >= 2 AND return_pct >= 2 AND close > 50-day average;
UNIVERSE_MIN_PRICE = float(spark.conf.get("stocks.universe_min_price", "50"))  # noqa: F821

UNIVERSE_MIN_TURNOVER_CR = float(spark.conf.get("stocks.universe_min_turnover_cr", "50"))  # noqa: F821

NEWS_CATEGORIES = ["results", "order", "mna", "management", "rating", "regulatory", "pledge", "insider",
                   "dividend_split_bonus", "board_meeting"]

SWING_LOOKBACK_DAYS = 45

def _universe_bars():
    members = spark.read.table("stocks.silver.index_members").select("symbol", "company", "industry")  # noqa: F821
    by_sym = Window.partitionBy("symbol").orderBy("trade_date")
    return (
        bars().filter(F.col("market") == "IN").join(members, "symbol")
        .withColumn("avg_turnover_cr", F.avg(F.col("close") * F.col("volume")).over(by_sym.rowsBetween(-19, 0)) / 1e7)
        .withColumn("avg_volume_20d", F.avg("volume").over(by_sym.rowsBetween(-20, -1)))
        .withColumn("volume_ratio", F.col("volume") / F.col("avg_volume_20d"))
        .withColumn("high_20d_prior", F.max("high").over(by_sym.rowsBetween(-20, -1)))
        .withColumn("low_5d", F.min("low").over(by_sym.rowsBetween(-4, 0)))
        .withColumn("ma50", F.avg("close").over(by_sym.rowsBetween(-49, 0)))
        .withColumn("eligible", (F.col("close") >= UNIVERSE_MIN_PRICE)
                    & (F.col("avg_turnover_cr") >= UNIVERSE_MIN_TURNOVER_CR))
    )

def _company_news():
    return (spark.read.table("stocks.silver.news")  # noqa: F821
            .filter(F.col("symbol").isNotNull() & F.col("category").isin(NEWS_CATEGORIES))
            .select("symbol", "category", "title", "news_ts", "news_date"))

@dp.materialized_view(
    name="stocks.gold.intraday_universe",
    comment="Nifty 200 stocks on the latest NSE day with the paper trader's guardrails (EQ series, close >= Rs 50, "
            "Rs 50 crore+ traded a day): eligible to trade intraday, and flagged when they have a fresh NSE filing, "
            "moved 3%+ or traded 2x their usual volume. The trader adds flagged names to its focus list.",
    table_properties={"quality": "gold"},
)
def intraday_universe():
    b = _universe_bars()
    latest = b.agg(F.max("trade_date").alias("trade_date"))
    today = b.join(latest, "trade_date")
    news = _company_news().join(latest.withColumnRenamed("trade_date", "latest_date"),
                                F.col("news_date") >= F.date_sub("latest_date", 2))
    newest = Window.partitionBy("symbol").orderBy(F.col("news_ts").desc())
    per_symbol = (news.withColumn("n", F.count("*").over(Window.partitionBy("symbol")))
                  .withColumn("rn", F.row_number().over(newest)).filter("rn = 1")
                  .select("symbol", F.col("n").alias("news_count"), F.col("category").alias("latest_news_category"),
                          F.col("title").alias("latest_news_title"), F.col("news_ts").alias("latest_news_ts")))
    df = today.join(per_symbol, "symbol", "left").withColumn("news_count", F.coalesce("news_count", F.lit(0)))
    has_news = F.col("news_count") > 0
    big_move = F.abs(F.col("return_pct")) >= 3
    busy = F.col("volume_ratio") >= 2
    reasons = F.concat_ws(", ",
                          F.when(has_news, F.concat(F.lit("NSE filing: "), F.col("latest_news_category"))),
                          F.when(big_move, F.concat(F.lit("moved "), F.format_number("return_pct", 1), F.lit("%"))),
                          F.when(busy, F.concat(F.format_number("volume_ratio", 1), F.lit("x usual volume"))))
    return df.select(
        "trade_date", "symbol", "company", "industry", "close", F.round("return_pct", 2).alias("return_pct"),
        F.round("volume_ratio", 2).alias("volume_ratio"), F.round("avg_turnover_cr", 1).alias("avg_turnover_cr"),
        "eligible", (F.col("eligible") & (has_news | big_move | busy)).alias("flagged"),
        reasons.alias("reasons"), "news_count", "latest_news_category", "latest_news_title", "latest_news_ts",
    )

@dp.materialized_view(
    name="stocks.gold.swing_signals",
    comment="Swing ideas tested in shadow mode by the paper trader: eligible Nifty 200 stocks that, within 2 days of "
            "an NSE filing (results, orders, ...), closed above their 20-day high on 2x volume, up 2%+ and above "
            f"their 50-day average. Last {SWING_LOOKBACK_DAYS} days. Suggested stop: the 5-day low, at most 8% below.",
    table_properties={"quality": "gold"},
)
def swing_signals():
    b = _universe_bars().filter(F.col("trade_date") >= F.date_sub(F.current_date(), SWING_LOOKBACK_DAYS))
    hits = b.filter(F.col("eligible") & (F.col("close") > F.col("high_20d_prior")) & (F.col("volume_ratio") >= 2)
                    & (F.col("return_pct") >= 2) & (F.col("close") > F.col("ma50")))
    news = _company_news().withColumnRenamed("symbol", "news_symbol")
    joined = hits.join(news, (news.news_symbol == hits.symbol)
                       & (news.news_date >= F.date_sub(hits.trade_date, 2)) & (news.news_date <= hits.trade_date))
    newest = Window.partitionBy("symbol", "trade_date").orderBy(F.col("news_ts").desc())
    return (joined.withColumn("rn", F.row_number().over(newest)).filter("rn = 1")
            .select("trade_date", "symbol", "company", "industry", "close",
                    F.round("return_pct", 2).alias("return_pct"), F.round("volume_ratio", 2).alias("volume_ratio"),
                    F.round(F.greatest("low_5d", F.col("close") * 0.92), 2).alias("stop"),
                    F.round("avg_turnover_cr", 1).alias("avg_turnover_cr"),
                    F.col("category").alias("news_category"), F.col("title").alias("news_title")))
