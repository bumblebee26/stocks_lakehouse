"""TRADING pipeline (stocks-trading): the paper-trader's journal and portfolio exports and everything built on them.
Runs when an export lands (job stocks-trading: 15:45 and 23:30 IST).

BRONZE: raw files from the landing volume, ingested incrementally with Auto Loader.

Each new file in /Volumes/stocks/bronze/landing/<source>/date=YYYY-MM-DD/ is read
exactly once. Columns are kept as delivered, plus lineage:
  _source_file, _file_modified_at, _ingested_at, trade_date (parsed from the path)

Schema changes from a source never break the stream: unexpected columns go into
_rescued_data instead (schemaEvolutionMode = rescue).

  stocks.bronze.trades_raw                   paper-trader journal exports
  stocks.bronze.portfolio_raw                paper-trader portfolio snapshots
  stocks.bronze.pool_trades_raw              paper-trader pool buys and sells (swing, long-term, shadows)
Reads market tables (NIFTYBEES closes in stocks.silver.nse_eq_daily, the Nifty 200 list) as plain
tables; they are at most a day old, which is fine for the portfolio and shadow comparisons.
"""

try:  # Lakeflow Declarative Pipelines (current API)
    from pyspark import pipelines as dp
except ImportError:  # older runtimes: Delta Live Tables API, same decorators
    import dlt as dp

    dp.materialized_view = dp.table

from pyspark.sql import functions as F

LANDING = spark.conf.get("stocks.landing_path", "/Volumes/stocks/bronze/landing")  # noqa: F821

def with_lineage(df):
    """Add lineage columns from Auto Loader's hidden _metadata column."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "trade_date",
            F.to_date(F.regexp_extract(F.col("_metadata.file_path"), r"date=(\d{4}-\d{2}-\d{2})", 1)),
        )
    )

# ---------------------------------------------------------------------------
# Paper trades (bumblebee26/paper-trader): the day's journal as JSON lines
# ---------------------------------------------------------------------------
# Pushed once a day at 15:45 IST by the stocks-trader-guardian Lambda to
#   /Volumes/stocks/bronze/landing/trades/date=YYYY-MM-DD/events.jsonl
# One line per event (proposed, filled, exited, rejected, ...), a unique event_id on each,
# and a final ledger_snapshot line (balance and realized P&L at export time).
# A re-export overwrites the same file. Auto Loader skips a file it has seen unless allowOverwrites is on (it
# wasn't until 26 Nov, so the 23:30 export and evening ratings never arrived); silver keeps the newest copy of
# each event_id.
#
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.trades_raw
#   COMMENT 'Raw paper-trader journal events (dummy money), one row per event per file'
#   TBLPROPERTIES ('quality' = 'bronze')
#   AS SELECT *, _metadata.file_path AS _source_file, ... (same lineage columns as above)
#   FROM STREAM read_files(
#     '/Volumes/stocks/bronze/landing/trades/',
#     format            => 'json',
#     pathGlobFilter    => '*.jsonl',
#     schema            => '<TRADES_SCHEMA below>',
#     rescuedDataColumn => '_rescued_data',
#     partitionColumns  => ''
#   );
TRADES_SCHEMA = (
    "event_id STRING, proposal_id STRING, timestamp_ist STRING, event STRING, symbol STRING, "
    "setup STRING, side STRING, entry DOUBLE, stop DOUBLE, target DOUBLE, qty DOUBLE, "
    "risk_rs DOUBLE, rr DOUBLE, reasons STRING, user_reply STRING, fill_price DOUBLE, "
    "exit_price DOUBLE, exit_reason STRING, pnl_rs DOUBLE, fees_rs DOUBLE, note STRING, "
    "balance DOUBLE, starting_capital DOUBLE, realized_today DOUBLE"
)

@dp.table(
    name="stocks.bronze.trades_raw",
    comment="Raw paper-trader journal events (dummy money), one row per event per file.",
    table_properties={"quality": "bronze"},
)
def trades_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .option("cloudFiles.allowOverwrites", "true")   # re-exports overwrite a day's file: read it again
        .schema(TRADES_SCHEMA)
        .load(f"{LANDING}/trades/")
    )

# ---------------------------------------------------------------------------
# Paper-trader portfolio: pools and holdings, pushed daily at 15:45 IST with the journal
# ---------------------------------------------------------------------------
# record_type 'pool': one row per pool (intraday, swing, long_term) with capital, cash, equity.
# record_type 'holding': one row per holding with quantity, average cost, last price, value, P&L.
#
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.portfolio_raw AS
#   SELECT *, <lineage columns> FROM STREAM read_files('/Volumes/stocks/bronze/landing/portfolio/',
#     format => 'json', pathGlobFilter => '*.jsonl', schema => '<PORTFOLIO_SCHEMA below>');
PORTFOLIO_SCHEMA = (
    "record_type STRING, snapshot_date STRING, pool STRING, capital DOUBLE, cash DOUBLE, holdings_value DOUBLE, "
    "equity DOUBLE, symbol STRING, sector STRING, qty DOUBLE, avg_cost DOUBLE, last_price DOUBLE, value DOUBLE, "
    "pnl DOUBLE, opened STRING"
)

@dp.table(
    name="stocks.bronze.portfolio_raw",
    comment="Raw paper-trader portfolio snapshots (dummy money): pools and holdings, one row per record per daily file.",
    table_properties={"quality": "bronze"},
)
def portfolio_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .option("cloudFiles.allowOverwrites", "true")   # re-exports overwrite a day's file: read it again
        .schema(PORTFOLIO_SCHEMA)
        .load(f"{LANDING}/portfolio/")
    )


# ---------------------------------------------------------------------------
# Paper-trader pool trades: every buy and sell in the delivery pools (real swing and long-term, and the shadow
# pools), pushed with the journal (15:45 and 23:30 IST; the last 4 days each time, so a day can arrive again).
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.pool_trades_raw AS
#   SELECT *, <lineage columns> FROM STREAM read_files('/Volumes/stocks/bronze/landing/pool_trades/',
#     format => 'json', pathGlobFilter => '*.jsonl', schema => '<POOL_TRADES_SCHEMA below>');
POOL_TRADES_SCHEMA = (
    "trade_id STRING, pool STRING, kind STRING, timestamp_ist STRING, event STRING, symbol STRING, qty DOUBLE, "
    "price DOUBLE, fees_rs DOUBLE, value_rs DOUBLE, pnl_rs DOUBLE, cash_after DOUBLE, reason STRING, note STRING"
)


@dp.table(
    name="stocks.bronze.pool_trades_raw",
    comment="Raw paper-trader pool trades (dummy money): buys and sells in the swing and long-term pools, real and "
            "shadow, one row per trade per daily file.",
    table_properties={"quality": "bronze"},
)
def pool_trades_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .option("cloudFiles.allowOverwrites", "true")   # re-exports overwrite a day's file: read it again
        .schema(POOL_TRADES_SCHEMA)
        .load(f"{LANDING}/pool_trades/")
    )
