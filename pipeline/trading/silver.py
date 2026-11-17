"""TRADING pipeline (stocks-trading): the paper-trader's journal and portfolio exports and everything built on them.
Runs when an export lands (job stocks-trading: 15:45 and 23:30 IST).

SILVER: cleaned, typed, de-duplicated tables. Materialized views: they recompute incrementally where
possible, and make de-duplication simple (a re-fetched day replaces the old copy: latest file wins).
Data-quality expectations drop bad rows and report counts in the pipeline UI.

  stocks.silver.trade_events                 paper-trader journal events (dummy money), one row per event_id
  stocks.silver.portfolio_pools              paper-trader pools per day (capital, cash, equity)
  stocks.silver.portfolio_holdings           paper-trader holdings per day
  stocks.silver.shadow_pools                 paper-trader shadow pools per day (rules tested on paper; never in portfolio totals)
  stocks.silver.shadow_holdings              what each shadow pool holds per day

Reads market tables (NIFTYBEES closes in stocks.silver.nse_eq_daily, the Nifty 200 list) as plain
tables; they are at most a day old, which is fine for the portfolio and shadow comparisons.
"""

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

    dp.materialized_view = dp.table

from pyspark.sql import Window
from pyspark.sql import functions as F

# The paper-trading ledger was reset on 17 Nov 2026: every pool restarted with its starting cash on 18 Nov. Money
# tables (portfolio, shadow pools, daily P&L, results per setup/hour, decisions, custom trades) start here. Learning
# tables (trades, proposal_outcomes, track_record, rating_review, shadow rules, model and ORB tests) keep all history.
LEDGER_START = spark.conf.get("stocks.ledger_start", "2026-11-18")  # noqa: F821


def since_ledger_start(df, col="trade_date"):
    return df.filter(F.col(col) >= F.to_date(F.lit(LEDGER_START)))


def latest_file_wins(df, keys):
    """Keep one row per key: the one from the most recently modified file."""
    w = Window.partitionBy(*keys).orderBy(F.col("_file_modified_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

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
# Paper-trader portfolio (dummy money)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.portfolio_pools AS
#   SELECT to_date(snapshot_date) AS snapshot_date, pool, capital, cash, holdings_value, equity
#   FROM stocks.bronze.portfolio_raw WHERE record_type = 'pool'   -- newest file per day wins
def _latest_portfolio_file():
    raw = since_ledger_start(spark.read.table("stocks.bronze.portfolio_raw").withColumn(  # noqa: F821
        "snapshot_date", F.to_date("snapshot_date")), "snapshot_date")
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

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.silver.shadow_holdings AS
#   SELECT to_date(snapshot_date) AS snapshot_date, pool, symbol, sector, qty, avg_cost, last_price, value, pnl,
#          to_date(opened) AS opened
#   FROM stocks.bronze.portfolio_raw WHERE record_type = 'shadow_holding';   -- newest file per day wins
@dp.materialized_view(
    name="stocks.silver.shadow_holdings",
    comment="What each paper-trader SHADOW pool holds per day (swing_shadow, swing_tech_shadow, long_term_shadow): "
            "quantity, average cost, last price, value and P&L (dummy Rs). Never part of the portfolio.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({"has_keys": "snapshot_date IS NOT NULL AND pool IS NOT NULL AND symbol IS NOT NULL"})
def shadow_holdings():
    return (_latest_portfolio_file().filter("record_type = 'shadow_holding'")
            .select("snapshot_date", "pool", "symbol", "sector", "qty", "avg_cost", "last_price", "value", "pnl",
                    F.to_date("opened").alias("opened")))
