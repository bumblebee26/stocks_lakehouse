"""BRONZE: raw files from the landing volume, ingested incrementally with Auto Loader.

Each new file in /Volumes/stocks/bronze/landing/<source>/date=YYYY-MM-DD/ is read
exactly once. Columns are kept as delivered (NSE as strings), plus lineage:
  _source_file, _file_modified_at, _ingested_at, trade_date (parsed from the path)

Schema changes from a source never break the stream: unexpected columns go into
_rescued_data instead (schemaEvolutionMode = rescue).
"""

try:  # Lakeflow Declarative Pipelines (current API)
    from pyspark import pipelines as dp
except ImportError:  # older runtimes: Delta Live Tables API, same decorators
    import dlt as dp

    dp.materialized_view = dp.table

from pyspark.sql import functions as F

LANDING = spark.conf.get("stocks.landing_path", "/Volumes/stocks/bronze/landing")  # noqa: F821
NEWS = spark.conf.get("stocks.news_path", "/Volumes/stocks/bronze/news")  # noqa: F821


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
# NSE equities bhavcopy (UDiFF CSV; the Lambda unzips it before pushing)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.nse_bhavcopy_raw
#   COMMENT 'Raw NSE UDiFF equities bhavcopy, one row per instrument per day'
#   TBLPROPERTIES ('quality' = 'bronze')
#   AS SELECT
#     *,
#     _metadata.file_path              AS _source_file,
#     _metadata.file_modification_time AS _file_modified_at,
#     current_timestamp()              AS _ingested_at,
#     to_date(regexp_extract(_metadata.file_path, 'date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS trade_date
#   FROM STREAM read_files(
#     '/Volumes/stocks/bronze/landing/nse/',
#     format               => 'csv',
#     header               => true,
#     pathGlobFilter       => '*.csv',
#     inferColumnTypes     => false,
#     schemaEvolutionMode  => 'rescue',
#     partitionColumns     => ''          -- don't add a 'date' column from the folder name
#   );
@dp.table(
    name="stocks.bronze.nse_bhavcopy_raw",
    comment="Raw NSE UDiFF equities bhavcopy, one row per instrument per day.",
    table_properties={"quality": "bronze"},
)
def nse_bhavcopy_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")  # ignore any .zip copies
        .option("cloudFiles.inferColumnTypes", "false")  # keep strings in bronze
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.partitionColumns", "")  # trade_date comes from with_lineage()
        .load(f"{LANDING}/nse/")
    )


# ---------------------------------------------------------------------------
# Massive grouped daily (one JSON document per US trading day)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.massive_grouped_raw
#   COMMENT 'Raw Massive grouped-daily responses, one row per file (results array = all US tickers)'
#   TBLPROPERTIES ('quality' = 'bronze')
#   AS SELECT
#     *,
#     _metadata.file_path              AS _source_file,
#     _metadata.file_modification_time AS _file_modified_at,
#     current_timestamp()              AS _ingested_at,
#     to_date(regexp_extract(_metadata.file_path, 'date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS trade_date
#   FROM STREAM read_files(
#     '/Volumes/stocks/bronze/landing/massive/',
#     format              => 'json',
#     multiLine           => true,
#     pathGlobFilter      => '*.json',
#     schemaHints         => 'results STRING',   -- keep the array as raw JSON text (see below)
#     schemaEvolutionMode => 'rescue',
#     partitionColumns    => ''
#   );
#
# Why results is a STRING: each bar has fields "T" (ticker) and "t" (timestamp). Delta column
# names are case-insensitive, so a parsed struct with both fails with DELTA_DUPLICATE_COLUMNS_FOUND.
# Bronze stores the JSON as delivered; silver renames "t" before parsing.
MASSIVE_SCHEMA_HINTS = "results STRING"


@dp.table(
    name="stocks.bronze.massive_grouped_raw",
    comment="Raw Massive grouped-daily responses, one row per file (results = JSON text, all US tickers).",
    table_properties={"quality": "bronze"},
)
def massive_grouped_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("pathGlobFilter", "*.json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", MASSIVE_SCHEMA_HINTS)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.partitionColumns", "")
        .load(f"{LANDING}/massive/")
    )


# ---------------------------------------------------------------------------
# FRED global markets (newline-delimited JSON, one observation per line)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.fred_observations_raw
#   COMMENT 'Raw FRED observations: global indices, FX, VIX, oil, rates. One row per series per date per file'
#   TBLPROPERTIES ('quality' = 'bronze')
#   AS SELECT
#     *,
#     _metadata.file_path              AS _source_file,
#     _metadata.file_modification_time AS _file_modified_at,
#     current_timestamp()              AS _ingested_at,
#     to_date(regexp_extract(_metadata.file_path, 'date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS trade_date
#   FROM STREAM read_files(
#     '/Volumes/stocks/bronze/landing/fred/',
#     format            => 'json',
#     pathGlobFilter    => '*.json',
#     schema            => 'series_id STRING, date STRING, value DOUBLE, name STRING,
#                           region STRING, asset_class STRING, unit STRING',
#     rescuedDataColumn => '_rescued_data',
#     partitionColumns  => ''
#   );
#
# The schema is given explicitly (we write these files ourselves), so the table works even
# before the first FRED file arrives; inference would fail on an empty folder.
FRED_SCHEMA = (
    "series_id STRING, date STRING, value DOUBLE, name STRING, "
    "region STRING, asset_class STRING, unit STRING"
)


@dp.table(
    name="stocks.bronze.fred_observations_raw",
    comment="Raw FRED observations: global indices, FX, VIX, oil, rates. One row per series per date per file.",
    table_properties={"quality": "bronze"},
)
def fred_observations_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.json")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(FRED_SCHEMA)
        .load(f"{LANDING}/fred/")
    )


# ---------------------------------------------------------------------------
# Paper trades (bumblebee26/paper-trader): the day's journal as JSON lines
# ---------------------------------------------------------------------------
# Pushed once a day at 15:45 IST by the stocks-trader-guardian Lambda to
#   /Volumes/stocks/bronze/landing/trades/date=YYYY-MM-DD/events.jsonl
# One line per event (proposed, filled, exited, rejected, ...), a unique event_id on each,
# and a final ledger_snapshot line (balance and realized P&L at export time).
# A re-export overwrites the same file; silver keeps the newest copy of each event_id.
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
        .schema(TRADES_SCHEMA)
        .load(f"{LANDING}/trades/")
    )


# ---------------------------------------------------------------------------
# Company news: NSE RSS feeds, fetched every 30 minutes by the stocks-fetch-news Lambda
# ---------------------------------------------------------------------------
# Lands in its own volume (not landing/), so news doesn't trigger a pipeline refresh every
# 30 minutes; it is picked up at the normal refreshes. Each run writes snap_HHMM.jsonl; the
# same item appears in many snapshots, and silver keeps one row per id.
# latest.jsonl (a copy of the newest snapshot, read by the paper trader) is skipped here.
#
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.news_raw
#   COMMENT 'Raw NSE company news items, one row per item per snapshot'
#   AS SELECT *, <lineage columns> FROM STREAM read_files(
#     '/Volumes/stocks/bronze/news/', format => 'json', pathGlobFilter => 'snap_*.jsonl',
#     schema => '<NEWS_SCHEMA below>', rescuedDataColumn => '_rescued_data', partitionColumns => '');
NEWS_SCHEMA = (
    "id STRING, feed STRING, symbol STRING, company STRING, category STRING, title STRING, "
    "description STRING, link STRING, published_ist STRING, published_raw STRING, fetched_at_ist STRING"
)


@dp.table(
    name="stocks.bronze.news_raw",
    comment="Raw NSE company news items (announcements, results, board meetings, corporate actions), "
            "one row per item per 30-minute snapshot.",
    table_properties={"quality": "bronze"},
)
def news_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "snap_*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(NEWS_SCHEMA)
        .load(f"{NEWS}/")
    )


# ---------------------------------------------------------------------------
# Company fundamentals: weekly, from paper-trader's "Company fundamentals" workflow
# ---------------------------------------------------------------------------
# One JSON line per company x metric x period: annual (12M) revenue, net income, EBIT, debt,
# equity, operating cash flow, invested capital, EPS, total assets; quarterly (3M) revenue,
# net income and EPS. Source: Yahoo Finance fundamentals (INR).
#
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.fundamentals_raw AS
#   SELECT *, <lineage columns> FROM STREAM read_files('/Volumes/stocks/bronze/landing/fundamentals/',
#     format => 'json', pathGlobFilter => '*.jsonl', schema => '<FUNDAMENTALS_SCHEMA below>');
FUNDAMENTALS_SCHEMA = (
    "symbol STRING, source_symbol STRING, metric STRING, period_type STRING, period_end STRING, "
    "value DOUBLE, currency STRING, source STRING, fetched_at_ist STRING"
)


@dp.table(
    name="stocks.bronze.fundamentals_raw",
    comment="Raw company financials (annual and quarterly), one row per company x metric x period per weekly file.",
    table_properties={"quality": "bronze"},
)
def fundamentals_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(FUNDAMENTALS_SCHEMA)
        .load(f"{LANDING}/fundamentals/")
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
        .schema(PORTFOLIO_SCHEMA)
        .load(f"{LANDING}/portfolio/")
    )


# ---------------------------------------------------------------------------
# Index members: NSE's Nifty 200 list with each company's industry (weekly, with the fundamentals)
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.index_members_raw AS
#   SELECT *, <lineage columns> FROM STREAM read_files('/Volumes/stocks/bronze/landing/index_members/',
#     format => 'json', pathGlobFilter => '*.jsonl', schema => '<INDEX_MEMBERS_SCHEMA below>');
INDEX_MEMBERS_SCHEMA = "symbol STRING, company STRING, industry STRING, isin STRING, index_name STRING, as_of STRING"


@dp.table(
    name="stocks.bronze.index_members_raw",
    comment="Raw NSE index constituent lists (Nifty 200), one row per company per weekly file.",
    table_properties={"quality": "bronze"},
)
def index_members_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(INDEX_MEMBERS_SCHEMA)
        .load(f"{LANDING}/index_members/")
    )
