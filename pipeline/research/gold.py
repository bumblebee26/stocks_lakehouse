"""RESEARCH pipeline (stocks-research): the heavy daily research on top of the market tables (pandas per stock). Runs
once per NSE day, straight after the market pipeline in job stocks-market-close.

GOLD: the tables the dashboard, the morning brief and the paper trader read.

  stocks.gold.technical_indicators           daily indicators, levels and the scanner's six filters per Nifty 200 stock
  stocks.gold.technical_screen               today's technical scanner: strict picks, near-misses, levels
  stocks.gold.signal_performance             how each scanner signal did 5/10/20 days later vs NIFTYBEES
  stocks.gold.data_freshness                 the latest date each data source has
  stocks.gold.momentum_ranks                 Nifty 200 momentum ranks for the momentum shadow pools (swing and long term)

Reads market tables (stocks.silver.nse_eq_daily, index_members, news, nse_delivery, fno_symbols,
gold.company_fundamentals) as plain tables: the market pipeline has always run just before.
"""

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

    dp.materialized_view = dp.table

from pyspark.sql import Window
from pyspark.sql import functions as F

NEWS_CATEGORIES = ["results", "order", "mna", "management", "rating", "regulatory", "pledge", "insider",
                   "dividend_split_bonus", "board_meeting"]

def _company_news():
    return (spark.read.table("stocks.silver.news")  # noqa: F821
            .filter(F.col("symbol").isNotNull() & F.col("category").isin(NEWS_CATEGORIES))
            .select("symbol", "category", "title", "news_ts", "news_date"))

# ---------------------------------------------------------------------------
# Technical scanner (Nifty 200): indicators, the daily screen and how each signal has done since
# ---------------------------------------------------------------------------
# The indicator maths is plain pandas, run per stock (applyInPandas), between the two markers below; the tests
# load exactly that block. Definitions (standard ones):
#   prices      adjusted for splits/bonuses/rights from NSE's own previous close: on an ex-date NSE quotes the
#               previous close adjusted (PrvsClsgPric), so factor = prev_close / yesterday's close; earlier
#               prices are multiplied by it (volumes divided) when it is more than 3% off 1. Dividends aren't adjusted.
#   SMA 20/50/200, RSI(14) and ATR(14) Wilder, MACD(12,26,9), ADX(14) Wilder (rising = above its value 3 days
#   earlier), MFI(14), volume vs the previous 20 days' average, 20-day high (previous 20 days).
#   swing points: a high (low) that is the highest (lowest) of the 5 days on either side; usable 5 days later.
#   resistance  nearest swing high above the close (last 250 days); support nearest swing low below it (120 days)
#   setup / stance (first that applies):
#     extended     RSI > 70, MFI > 80 or more than 8% above the 20-day average -> wait/avoid (no chasing)
#     breakout     close above the previous 20-day high on 1.5x volume -> buy-on-dip (retest of the level)
#     at resistance close within 3% below resistance -> breakout-only (trigger = resistance)
#     pullback     low touched the 20-day average and the close is back above it -> buy-on-dip
#     otherwise    trend -> wait
#   levels      entry zone from the setup; reference entry = top of the zone; stop = 0.1 ATR under the nearest swing low below
#               the entry zone, or entry - 1.5 ATR when there is none within 3 ATR ("ATR stop"); T1 = next resistance above the entry,
#               else entry + 2R ("measured"); T2 = entry + 3R (at least T1 + 1R); R:R = (T1 - entry) / (entry - stop).
#   filters     above all three averages; RSI 50-65; MACD above its signal; ADX > 20 and rising; volume >= 1.5x;
#               R:R >= 2. strict = all six and not extended; near-miss = above all three averages and 4+ of 6.
# --- technicals (pure pandas) ---
import numpy as _np  # noqa: E402

import pandas as _pd  # noqa: E402

TECH_FILTERS = ["above_mas", "rsi_ok", "macd_ok", "adx_ok", "volume_ok", "rr_ok"]

TECH_FILTER_WORDS = {"above_mas": "not above all of its 20/50/200-day averages", "rsi_ok": "RSI outside 50-65",
                     "macd_ok": "MACD below its signal line", "adx_ok": "ADX not above 20 and rising",
                     "volume_ok": "volume under 1.5x its 20-day average", "rr_ok": "reward under 2x the risk"}

PIVOT = 5

def _wilder(s, n):
    return s.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()

def adjust_prices(df):
    """Split/bonus-adjusted OHLC and volume from NSE's previous close (see above)."""
    df = df.sort_values("trade_date").reset_index(drop=True)
    # above 3% only: a day missing from our data also makes NSE's previous close differ, by an ordinary move
    factor = (df["prev_close"] / df["close"].shift(1)).where(lambda f: (f - 1).abs() > 0.03, 1.0).fillna(1.0)
    # each day's prices are scaled by the product of every later factor
    later = factor[::-1].cumprod()[::-1].shift(-1).fillna(1.0)
    for c in ("open", "high", "low", "close"):
        df[c] = df[c] * later
    df["volume"] = df["volume"] / later
    df["adjusted"] = later != 1.0
    return df

def add_indicators(df):
    c, h, low, v = df["close"], df["high"], df["low"], df["volume"]
    for n in (20, 50, 200):
        df[f"sma{n}"] = c.rolling(n, min_periods=n).mean()
    d = c.diff()
    rs = _wilder(d.clip(lower=0), 14) / _wilder(-d.clip(upper=0), 14)
    df["rsi14"] = 100 - 100 / (1 + rs)
    ema12, ema26 = c.ewm(span=12, adjust=False).mean(), c.ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    tr = _pd.concat([h - low, (h - c.shift()).abs(), (low - c.shift()).abs()], axis=1).max(axis=1)
    df["atr14"] = _wilder(tr, 14)
    up, down = h.diff(), -low.diff()
    plus_dm = _np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = _np.where((down > up) & (down > 0), down, 0.0)
    atr = _wilder(tr, 14)
    plus_di = 100 * _wilder(_pd.Series(plus_dm, index=df.index), 14) / atr
    minus_di = 100 * _wilder(_pd.Series(minus_dm, index=df.index), 14) / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    df["adx14"] = _wilder(dx, 14)
    tp = (h + low + c) / 3
    flow = tp * v
    pos = flow.where(tp > tp.shift(), 0.0).rolling(14).sum()
    neg = flow.where(tp < tp.shift(), 0.0).rolling(14).sum()
    df["mfi14"] = 100 - 100 / (1 + pos / neg.replace(0, _np.nan))
    df["vol_avg20"] = v.shift(1).rolling(20, min_periods=20).mean()
    df["vol_ratio"] = v / df["vol_avg20"]
    df["high20_prev"] = h.shift(1).rolling(20, min_periods=20).max()
    win = 2 * PIVOT + 1
    df["swing_high"] = h.where(h == h.rolling(win, center=True).max())
    df["swing_low"] = low.where(low == low.rolling(win, center=True).min())
    return df

def add_levels(df):
    """Setup, stance and levels for every day, using only swing points confirmed by that day."""
    rows = []
    sh = df["swing_high"].to_numpy()
    sl = df["swing_low"].to_numpy()
    for i in range(len(df)):
        r = df.iloc[i]
        out = dict.fromkeys(("setup", "stance", "entry_low", "entry_high", "trigger", "stop", "stop_basis",
                             "target1", "target1_basis", "target2", "rr", "resistance", "support"))
        if _np.isnan(r["atr14"]) or _np.isnan(r["sma20"]):
            rows.append(out)
            continue
        close, atr = r["close"], r["atr14"]
        known = max(0, i - PIVOT + 1)            # pivots at index <= i - PIVOT are confirmed
        highs = [x for x in sh[max(0, i - 250):known] if not _np.isnan(x)]
        lows = [x for x in sl[max(0, i - 120):known] if not _np.isnan(x)]
        res = min((x for x in highs if x > close), default=None)
        sup = max((x for x in lows if x < close), default=None)
        out["resistance"], out["support"] = res, sup
        extended = (r["rsi14"] > 70) or (r["mfi14"] > 80) or (close > r["sma20"] * 1.08)
        broke = (not _np.isnan(r["high20_prev"])) and close > r["high20_prev"] and r["vol_ratio"] >= 1.5
        if extended:
            setup, stance, lo, hi, trig = "extended", "wait/avoid", None, None, None
        elif broke:
            lvl = r["high20_prev"]
            setup, stance, lo, hi, trig = "20-day breakout", "buy-on-dip", lvl, lvl + 0.5 * atr, lvl
        elif res is not None and close >= res * 0.97:
            setup, stance, lo, hi, trig = "at resistance", "breakout-only", res * 1.001, res * 1.001 + 0.5 * atr, res * 1.001
        elif r["low"] <= r["sma20"] * 1.01 and close > r["sma20"]:
            setup, stance, lo, hi, trig = "pullback to 20-day average", "buy-on-dip", r["sma20"], close, None
        else:
            setup, stance, lo, hi, trig = "trend", "wait", None, None, None
        out.update(setup=setup, stance=stance, entry_low=lo, entry_high=hi, trigger=trig)
        if hi is not None:
            entry = hi
            # the stop sits under a swing low below the whole entry zone (not one inside it)
            base = max((x for x in lows if x < lo), default=None)
            if base is not None and entry - (base - 0.1 * atr) <= 3 * atr:
                sup = base
                stop, basis = sup - 0.1 * atr, "swing low"
            else:
                stop, basis = entry - 1.5 * atr, "ATR stop"
            stop = min(stop, lo - 0.1 * atr)     # always under the bottom of the entry zone
            risk = entry - stop
            t1 = min((x for x in highs if x > entry * 1.005), default=None)
            t1_basis = "resistance" if t1 is not None else "measured (2R)"
            t1 = t1 if t1 is not None else entry + 2 * risk
            out.update(stop=stop, stop_basis=basis, target1=t1, target1_basis=t1_basis, target2=max(entry + 3 * risk, t1 + risk),
                       rr=(t1 - entry) / risk if risk > 0 else None)
        rows.append(out)
    return _pd.concat([df.reset_index(drop=True), _pd.DataFrame(rows)], axis=1)

def add_filters(df):
    df["above_mas"] = (df["close"] > df["sma20"]) & (df["close"] > df["sma50"]) & (df["close"] > df["sma200"])
    df["rsi_ok"] = df["rsi14"].between(50, 65)
    df["macd_ok"] = df["macd"] > df["macd_signal"]
    df["macd_cross_up"] = df["macd_ok"] & ~(df["macd"].shift(1) > df["macd_signal"].shift(1))
    df["adx_ok"] = (df["adx14"] > 20) & (df["adx14"] > df["adx14"].shift(3))
    df["volume_ok"] = df["vol_ratio"] >= 1.5
    df["rr_ok"] = df["rr"].astype(float) >= 2
    df["filters_passed"] = df[TECH_FILTERS].fillna(False).astype(int).sum(axis=1)
    df["strict"] = (df["filters_passed"] == len(TECH_FILTERS)) & (df["setup"] != "extended")
    df["near_miss"] = ~df["strict"] & df["above_mas"].fillna(False) & (df["filters_passed"] >= 4)
    df["misses"] = df[TECH_FILTERS].fillna(False).apply(
        lambda row: "; ".join(TECH_FILTER_WORDS[k] for k in TECH_FILTERS if not row[k]), axis=1)
    df["score"] = (df["filters_passed"] * 10 + df["adx14"].clip(upper=50).fillna(0) / 5
                   + df["vol_ratio"].clip(upper=4).fillna(0) * 2)
    return df

def technicals(df):
    """One stock's daily bars -> indicators, levels and filters for every day."""
    if len(df) < 30:
        return df.iloc[0:0]
    return add_filters(add_levels(add_indicators(adjust_prices(df))))

# --- end technicals ---

TECH_COLUMNS = (
    "symbol STRING, trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, "
    "adjusted BOOLEAN, sma20 DOUBLE, sma50 DOUBLE, sma200 DOUBLE, rsi14 DOUBLE, macd DOUBLE, macd_signal DOUBLE, "
    "atr14 DOUBLE, adx14 DOUBLE, mfi14 DOUBLE, vol_avg20 DOUBLE, vol_ratio DOUBLE, high20_prev DOUBLE, "
    "setup STRING, stance STRING, entry_low DOUBLE, entry_high DOUBLE, trigger DOUBLE, stop DOUBLE, "
    "stop_basis STRING, target1 DOUBLE, target1_basis STRING, target2 DOUBLE, rr DOUBLE, resistance DOUBLE, "
    "support DOUBLE, above_mas BOOLEAN, rsi_ok BOOLEAN, macd_ok BOOLEAN, macd_cross_up BOOLEAN, adx_ok BOOLEAN, "
    "volume_ok BOOLEAN, rr_ok BOOLEAN, filters_passed INT, strict BOOLEAN, near_miss BOOLEAN, misses STRING, "
    "score DOUBLE"
)

TECH_DAYS = 420   # enough for the 200-day average plus ~6 months of signal history

TECH_INDEX_PROXY = "NIFTYBEES"   # Nifty 50 regime, via the ETF that tracks it

def _technicals_frame(pdf):
    out = technicals(pdf)
    cols = [c.split(" ")[0] for c in TECH_COLUMNS.split(", ")]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]

# SQL equivalent: not practical in SQL (Wilder smoothing and EMAs are recursive): computed per stock in pandas.
@dp.materialized_view(
    name="stocks.gold.technical_indicators",
    comment="Daily indicators per Nifty 200 stock (and NIFTYBEES for the Nifty 50 regime), split/bonus-adjusted: "
            "20/50/200-day averages, RSI(14), MACD(12,26,9), ADX(14), ATR(14), MFI(14), volume vs 20-day average, "
            "swing-point support/resistance, setup, stance, indicative levels and the scanner's six filters.",
    table_properties={"quality": "gold"},
)
def technical_indicators():
    members = spark.read.table("stocks.silver.index_members").select("symbol")  # noqa: F821
    wanted = members.unionByName(spark.createDataFrame([(TECH_INDEX_PROXY,)], "symbol STRING"))  # noqa: F821
    bars_in = (spark.read.table("stocks.silver.nse_eq_daily")  # noqa: F821
               .filter((F.col("series") == "EQ") & (F.col("trade_date") >= F.date_sub(F.current_date(), int(TECH_DAYS * 1.5))))
               .join(wanted, "symbol")
               .select("symbol", "trade_date", "open", "high", "low", "close", "prev_close", "volume"))
    return bars_in.groupBy("symbol").applyInPandas(_technicals_frame, schema=TECH_COLUMNS)

# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.technical_screen AS
#   SELECT t.*, CASE WHEN strict THEN 'strict' WHEN near_miss THEN 'near-miss' ELSE 'none' END AS list_type,
#          d.deliv_per, f.symbol IS NOT NULL AS fno, m.company, m.industry, m.as_of AS nifty200_as_of,
#          f.as_of AS fno_as_of, <NIFTYBEES row as regime_* columns>
#   FROM stocks.gold.technical_indicators t JOIN stocks.silver.index_members m USING (symbol)
#   LEFT JOIN stocks.silver.nse_delivery d ON d.symbol = t.symbol AND d.trade_date = t.trade_date AND d.series = 'EQ'
#   LEFT JOIN stocks.silver.fno_symbols f USING (symbol)
#   WHERE t.trade_date = (SELECT max(trade_date) FROM stocks.gold.technical_indicators);
@dp.materialized_view(
    name="stocks.gold.technical_screen",
    comment="Today's technical scanner over the Nifty 200 (latest completed NSE session): every stock with its "
            "indicators, setup, stance, indicative levels, filters passed and missed, list_type (strict / "
            "near-miss / none), rank, delivery %, F&O status, and the Nifty 50 regime (via NIFTYBEES). "
            "Indicative levels only; never a guaranteed return.",
    table_properties={"quality": "gold"},
)
def technical_screen():
    t = spark.read.table("stocks.gold.technical_indicators")  # noqa: F821
    latest = t.agg(F.max("trade_date").alias("trade_date"))
    today = t.join(latest, "trade_date")
    members = spark.read.table("stocks.silver.index_members").select(  # noqa: F821
        "symbol", "company", "industry", F.col("as_of").alias("nifty200_as_of"))
    deliv = (spark.read.table("stocks.silver.nse_delivery").filter(F.col("series") == "EQ")  # noqa: F821
             .select("symbol", "trade_date", "deliv_per"))
    fno = spark.read.table("stocks.silver.fno_symbols").select(  # noqa: F821
        "symbol", F.lit(True).alias("fno"), F.col("as_of").alias("fno_as_of"))
    regime = (today.filter(F.col("symbol") == TECH_INDEX_PROXY)
              .select(F.col("close").alias("regime_close"), F.col("rsi14").alias("regime_rsi"),
                      F.col("sma20").alias("regime_sma20"), F.col("sma50").alias("regime_sma50"),
                      F.col("sma200").alias("regime_sma200"), F.col("adx14").alias("regime_adx")))
    rank_w = Window.partitionBy("list_type").orderBy(F.col("score").desc())
    return (today.filter(F.col("symbol") != TECH_INDEX_PROXY)
            .join(members, "symbol")
            .join(deliv, ["symbol", "trade_date"], "left")
            .join(fno, "symbol", "left")
            .crossJoin(regime)
            .withColumn("fno", F.coalesce("fno", F.lit(False)))
            .withColumn("list_type", F.when(F.col("strict"), "strict").when(F.col("near_miss"), "near-miss")
                        .otherwise("none"))
            .withColumn("rank", F.row_number().over(rank_w)))

SIGNALS = {
    "strict": "strict (all six filters)", "near_miss": "near-miss (4-5 filters, above all averages)",
    "above_mas": "above 20/50/200-day averages", "macd_cross_up": "MACD crossed above its signal",
    "breakout": "20-day breakout on 1.5x volume", "pullback": "pullback to the 20-day average",
}

# SQL equivalent (abridged):
#   forward return over h days = close(t+h) / close(t) - 1, minus NIFTYBEES over the same days;
#   SELECT signal, horizon_days, count(*) AS signals, avg(fwd_ret_pct), avg(excess_pct),
#          avg(CASE WHEN excess_pct > 0 THEN 1 ELSE 0 END) * 100 AS beat_niftybees_pct
#   FROM <one row per stock-day where the signal fired and the horizon has passed> GROUP BY 1, 2;
@dp.materialized_view(
    name="stocks.gold.signal_performance",
    comment="How each scanner signal did afterwards: average return 5, 10 and 20 trading days later, the excess "
            "over NIFTYBEES and how often it beat NIFTYBEES, over the last ~6 months. Past results only.",
    table_properties={"quality": "gold"},
)
def signal_performance():
    t = spark.read.table("stocks.gold.technical_indicators")  # noqa: F821
    by_sym = Window.partitionBy("symbol").orderBy("trade_date")
    for h in (5, 10, 20):
        t = t.withColumn(f"fwd{h}", F.lead("close", h).over(by_sym) / F.col("close") - 1)
    bench = t.filter(F.col("symbol") == TECH_INDEX_PROXY).select(
        "trade_date", *[F.col(f"fwd{h}").alias(f"bench{h}") for h in (5, 10, 20)])
    t = (t.filter(F.col("symbol") != TECH_INDEX_PROXY).join(bench, "trade_date")
         .withColumn("breakout", F.col("setup") == "20-day breakout")
         .withColumn("pullback", F.col("setup") == "pullback to the 20-day average"))
    parts = []
    for sig, label in SIGNALS.items():
        for h in (5, 10, 20):
            parts.append(
                t.filter(F.col(sig) & F.col(f"fwd{h}").isNotNull() & F.col(f"bench{h}").isNotNull())
                .agg(F.count("*").alias("signals"),
                     F.round(F.avg(F.col(f"fwd{h}")) * 100, 2).alias("avg_return_pct"),
                     F.round(F.avg(F.col(f"fwd{h}") - F.col(f"bench{h}")) * 100, 2).alias("avg_excess_pct"),
                     F.round(F.avg(F.when(F.col(f"fwd{h}") > F.col(f"bench{h}"), 1).otherwise(0)) * 100, 1)
                     .alias("beat_niftybees_pct"))
                .withColumn("signal", F.lit(sig)).withColumn("signal_label", F.lit(label))
                .withColumn("horizon_days", F.lit(h)))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.select("signal", "signal_label", "horizon_days", "signals", "avg_return_pct", "avg_excess_pct",
                      "beat_niftybees_pct")

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.data_freshness AS
#   SELECT 'nse_prices' AS source, max(trade_date) AS latest_date FROM stocks.silver.nse_eq_daily UNION ALL ...;
@dp.materialized_view(
    name="stocks.gold.data_freshness",
    comment="The latest date each data source has. Readers compare it with the date it should have (from the "
            "market's own calendar) and leave a missing source out instead of using an older day.",
    table_properties={"quality": "gold"},
)
def data_freshness():
    srcs = [
        ("nse_prices", "stocks.silver.nse_eq_daily", "trade_date"),
        ("nse_delivery", "stocks.silver.nse_delivery", "trade_date"),
        ("us_prices", "stocks.silver.us_daily", "trade_date"),
        ("fred_global", "stocks.silver.fred_daily", "obs_date"),
        ("company_news", "stocks.silver.news", "news_date"),
        ("fno_list", "stocks.silver.fno_symbols", "as_of"),
        ("nifty200_list", "stocks.silver.index_members", "as_of"),
        ("technical_scanner", "stocks.gold.technical_indicators", "trade_date"),
    ]
    parts = [spark.read.table(tbl).agg(F.max(col).cast("date").alias("latest_date"))  # noqa: F821
             .withColumn("source", F.lit(name)) for name, tbl, col in srcs]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.select("source", "latest_date").withColumn("checked_at", F.current_timestamp())

# ---------------------------------------------------------------------------
# Momentum (Nifty 200): the ranks behind the momentum shadow pools (swing and long term, paper only)
# ---------------------------------------------------------------------------
# From gold.technical_indicators (split/bonus-adjusted closes), on the latest NSE day:
#   returns over 1, 3, 6 and 12 months (21, 63, 126, 252 sessions), yearly volatility (stdev of daily returns x
#   sqrt(252)), distance from the 52-week high, ATR(14), RSI(14), uptrend (close > 50-day > 200-day average).
#   long term  lt_score = average of the z-scores (across the Nifty 200) of 6- and 12-month return / volatility,
#              the method behind NSE's Momentum 30 index; lt_rank among stocks with a year of history whose
#              fundamentals aren't known to be bad (a loss, or debt above 1.5x equity outside banks).
#   swing      swing_score = 40 x percentile of the 1-month return + 30 x percentile of the 3-month return
#              + 15 within 5% of the 52-week high (8 within 10%) + 15 for a fresh positive filing (results, order,
#              merger/acquisition, dividend/bonus/buyback, last 5 days) + 10 for a strict technical-scanner pick
#              - 25 for a fresh regulatory or pledge filing. swing_eligible = uptrend, 1-month return above 0 and
#              RSI under 80 (momentum is allowed to be stretched; only blow-offs are left out).
MOM_POSITIVE_NEWS = ["results", "order", "mna", "dividend_split_bonus"]

MOM_NEGATIVE_NEWS = ["regulatory", "pledge"]

# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.momentum_ranks AS
#   WITH r AS (SELECT *, close / lag(close, 21) OVER w - 1 AS ret_1m, ..., stddev(close / lag(close) OVER w - 1)
#              OVER (w ROWS 251 PRECEDING) * sqrt(252) AS vol_1y, max(high) OVER (w ROWS 251 PRECEDING) AS high_52w
#              FROM stocks.gold.technical_indicators WINDOW w AS (PARTITION BY symbol ORDER BY trade_date))
#   SELECT ..., (z(ret_6m / vol_1y) + z(ret_12m / vol_1y)) / 2 AS lt_score, rank() ... AS lt_rank, ... swing_rank
#   FROM r WHERE trade_date = (SELECT max(trade_date) FROM r) ...;
@dp.materialized_view(
    name="stocks.gold.momentum_ranks",
    comment="Nifty 200 momentum on the latest NSE day, for the momentum shadow pools (paper only): 1/3/6/12-month "
            "returns, yearly volatility, distance from the 52-week high, ATR, RSI, uptrend, fresh positive and "
            "negative filings, scanner status, basic quality; lt_score/lt_rank (volatility-adjusted 6- and 12-month "
            "momentum, the Momentum 30 method) and swing_score/swing_rank (1- and 3-month strength, near the high, "
            "news). Past strength only; never a guaranteed return.",
    table_properties={"quality": "gold"},
)
def momentum_ranks():
    t = (spark.read.table("stocks.gold.technical_indicators")  # noqa: F821
         .filter(F.col("symbol") != TECH_INDEX_PROXY))
    w = Window.partitionBy("symbol").orderBy("trade_date")
    year = w.rowsBetween(-251, 0)
    r = (t.withColumn("n", F.row_number().over(w))
         .withColumn("daily_ret", F.col("close") / F.lag("close").over(w) - 1)
         .withColumn("vol_1y_pct", F.stddev("daily_ret").over(year) * F.sqrt(F.lit(252.0)) * 100)
         .withColumn("high_52w", F.max("high").over(year)))
    for name, n in (("ret_1m_pct", 21), ("ret_3m_pct", 63), ("ret_6m_pct", 126), ("ret_12m_pct", 252)):
        r = r.withColumn(name, (F.col("close") / F.lag("close", n).over(w) - 1) * 100)
    latest = r.agg(F.max("trade_date").alias("trade_date"))
    d = r.join(latest, "trade_date")

    news = _company_news().filter(F.col("news_date") >= F.date_sub(F.current_date(), 7))
    newest = Window.partitionBy("symbol").orderBy(F.col("news_ts").desc())
    pos = (news.filter(F.col("category").isin(MOM_POSITIVE_NEWS))
           .withColumn("rn", F.row_number().over(newest))
           .groupBy("symbol").agg(F.count("*").alias("news_pos_5d"),
                                  F.max(F.when(F.col("rn") == 1, F.concat_ws(": ", "category", "title"))).alias("latest_good_news")))
    neg = (news.filter(F.col("category").isin(MOM_NEGATIVE_NEWS)).groupBy("symbol")
           .agg(F.count("*").alias("news_neg_5d")))
    fund = (spark.read.table("stocks.gold.company_fundamentals")  # noqa: F821
            .select("symbol", F.when((F.col("net_income_cr") <= 0)
                                     | (~F.col("is_financial") & (F.col("debt_to_equity") > 1.5)), False)
                    .when(F.col("net_income_cr").isNotNull(), True).alias("quality_ok")))
    members = spark.read.table("stocks.silver.index_members").select("symbol", "company", "industry")  # noqa: F821

    d = (d.join(members, "symbol").join(pos, "symbol", "left").join(neg, "symbol", "left").join(fund, "symbol", "left")
         .withColumn("news_pos_5d", F.coalesce("news_pos_5d", F.lit(0)))
         .withColumn("news_neg_5d", F.coalesce("news_neg_5d", F.lit(0)))
         .withColumn("pct_of_52w_high", F.col("close") / F.col("high_52w") * 100)
         .withColumn("uptrend", (F.col("close") > F.col("sma50")) & (F.col("sma50") > F.col("sma200")))
         .withColumn("lt_eligible", (F.col("n") >= 253) & F.col("vol_1y_pct").isNotNull()
                     & (F.col("vol_1y_pct") > 0) & (F.coalesce("quality_ok", F.lit(True))))
         .withColumn("m6", F.col("ret_6m_pct") / F.col("vol_1y_pct"))
         .withColumn("m12", F.col("ret_12m_pct") / F.col("vol_1y_pct")))
    allw = Window.partitionBy(F.lit(1))
    elig = Window.partitionBy("lt_eligible")
    d = (d.withColumn("z6", (F.col("m6") - F.avg("m6").over(elig)) / F.stddev("m6").over(elig))
         .withColumn("z12", (F.col("m12") - F.avg("m12").over(elig)) / F.stddev("m12").over(elig))
         .withColumn("lt_score", F.when(F.col("lt_eligible"), (F.col("z6") + F.col("z12")) / 2))
         .withColumn("p1", F.percent_rank().over(allw.orderBy("ret_1m_pct")))
         .withColumn("p3", F.percent_rank().over(allw.orderBy("ret_3m_pct")))
         .withColumn("swing_eligible", F.coalesce(F.col("uptrend") & (F.col("ret_1m_pct") > 0)
                                                  & (F.col("rsi14") < 80), F.lit(False)))
         .withColumn("swing_score",
                     40 * F.col("p1") + 30 * F.col("p3")
                     + F.when(F.col("pct_of_52w_high") >= 95, 15).when(F.col("pct_of_52w_high") >= 90, 8).otherwise(0)
                     + F.when(F.col("news_pos_5d") > 0, 15).otherwise(0)
                     + F.when(F.col("strict"), 10).otherwise(0)
                     - F.when(F.col("news_neg_5d") > 0, 25).otherwise(0)))
    lt_rank = Window.partitionBy("lt_eligible").orderBy(F.col("lt_score").desc_nulls_last())
    sw_rank = Window.partitionBy("swing_eligible").orderBy(F.col("swing_score").desc())
    return (d.withColumn("lt_rank", F.when(F.col("lt_eligible"), F.row_number().over(lt_rank)))
            .withColumn("swing_rank", F.when(F.col("swing_eligible"), F.row_number().over(sw_rank)))
            .select("trade_date", "symbol", "company", "industry", F.round("close", 2).alias("close"),
                    *[F.round(c, 2).alias(c) for c in ("ret_1m_pct", "ret_3m_pct", "ret_6m_pct", "ret_12m_pct",
                                                        "vol_1y_pct", "pct_of_52w_high")],
                    F.round("atr14", 2).alias("atr14"), F.round("rsi14", 1).alias("rsi14"), "uptrend",
                    F.col("strict").alias("scanner_strict"), "news_pos_5d", "news_neg_5d", "latest_good_news",
                    "quality_ok", "lt_eligible", F.round("lt_score", 3).alias("lt_score"), "lt_rank",
                    "swing_eligible", F.round("swing_score", 1).alias("swing_score"), "swing_rank"))
