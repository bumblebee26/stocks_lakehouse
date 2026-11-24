"""MARKET pipeline (stocks-market): market data in, cleaned, and the light market summaries. Runs after the NSE evening
file (job stocks-market-close, then the research pipeline), after the 08:30 IST pre-open snapshot Mon-Fri (job
stocks-market-preopen), at 11:15 IST Tue-Sat (job stocks-market-morning: US prices, FRED, the F&O list) and on
Saturday at 08:00 (job stocks-market-weekend: fundamentals, the Nifty 200 list).

BRONZE: raw files from the landing volume, ingested incrementally with Auto Loader.

Each new file in /Volumes/stocks/bronze/landing/<source>/date=YYYY-MM-DD/ is read
exactly once. Columns are kept as delivered, plus lineage:
  _source_file, _file_modified_at, _ingested_at, trade_date (parsed from the path)

Schema changes from a source never break the stream: unexpected columns go into
_rescued_data instead (schemaEvolutionMode = rescue).

  stocks.bronze.nse_bhavcopy_raw             NSE equities bhavcopy
  stocks.bronze.massive_grouped_raw          Massive grouped daily (all US stocks)
  stocks.bronze.fred_observations_raw        FRED series
  stocks.bronze.news_raw                     company news snapshots (news volume)
  stocks.bronze.fundamentals_raw             company financials (Saturday)
  stocks.bronze.index_members_raw            Nifty 200 lists
  stocks.bronze.nse_delivery_raw             NSE delivery file
  stocks.bronze.nse_indices_raw              NSE index closes (India VIX included)
  stocks.bronze.nse_fo_raw                   NSE F&O stock list
  stocks.bronze.nse_holidays_raw             NSE trading holidays
  stocks.bronze.premarket_raw                pre-open world-markets snapshot (Yahoo, 08:30 IST Mon-Fri)
Tables of the research and trading pipelines are never defined here; the three pipelines never run at
the same time (Free Edition allows one active pipeline).
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

# ---------------------------------------------------------------------------
# NSE delivery figures (from sec_bhavdata_full, cleaned by the NSE Lambda) and the F&O stock list
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.nse_delivery_raw AS
#   SELECT *, <lineage columns> FROM STREAM read_files('/Volumes/stocks/bronze/landing/nse_delivery/',
#     format => 'json', pathGlobFilter => '*.jsonl', schema => 'symbol STRING, series STRING, deliv_qty DOUBLE,
#     deliv_per DOUBLE');
#   (stocks.bronze.nse_fo_raw: the same from landing/nse_fo/, schema 'symbol STRING, underlying STRING, as_of STRING')
DELIVERY_SCHEMA = "symbol STRING, series STRING, deliv_qty DOUBLE, deliv_per DOUBLE"

FNO_SCHEMA = "symbol STRING, underlying STRING, as_of STRING"

@dp.table(
    name="stocks.bronze.nse_delivery_raw",
    comment="Raw NSE delivery figures per stock per day (delivered quantity and % of traded quantity).",
    table_properties={"quality": "bronze"},
)
def nse_delivery_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(DELIVERY_SCHEMA)
        .load(f"{LANDING}/nse_delivery/")
    )

# NSE index closes (ind_close_all, cleaned by the NSE Lambda), India VIX included.
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.nse_indices_raw AS
#   SELECT *, <lineage columns> FROM STREAM read_files('/Volumes/stocks/bronze/landing/nse_indices/',
#     format => 'json', pathGlobFilter => '*.jsonl', schema => '<INDICES_SCHEMA below>');
INDICES_SCHEMA = "index_name STRING, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, change_pct DOUBLE"

@dp.table(
    name="stocks.bronze.nse_indices_raw",
    comment="Raw NSE index closes per day (every NSE index, India VIX included).",
    table_properties={"quality": "bronze"},
)
def nse_indices_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(INDICES_SCHEMA)
        .load(f"{LANDING}/nse_indices/")
    )

# NSE trading holidays (from 24 Nov 2026), the whole list re-landed by the 08:30 brief each weekday.
# SQL equivalent: as above from landing/nse_holidays/, schema 'holiday_date STRING, description STRING, as_of STRING'.
HOLIDAYS_SCHEMA = "holiday_date STRING, description STRING, as_of STRING"

@dp.table(
    name="stocks.bronze.nse_holidays_raw",
    comment="Raw NSE trading-holiday lists (cash market), one row per holiday per landed file.",
    table_properties={"quality": "bronze"},
)
def nse_holidays_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(HOLIDAYS_SCHEMA)
        .load(f"{LANDING}/nse_holidays/")
    )

@dp.table(
    name="stocks.bronze.nse_fo_raw",
    comment="Raw NSE F&O stock lists, one row per stock per weekly file.",
    table_properties={"quality": "bronze"},
)
def nse_fo_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(FNO_SCHEMA)
        .load(f"{LANDING}/nse_fo/")
    )

# ---------------------------------------------------------------------------
# Pre-open snapshot: world markets from Yahoo, written by the 08:30 IST brief (fetchers/snapshot.py)
# ---------------------------------------------------------------------------
# One JSON line per instrument: its latest daily candle and Yahoo's own 1/5/20-session changes. Ids FRED also
# carries keep FRED's id (SP500, DEXINUS...), so gold.global_snapshot can show whichever reading is newer.
#
# SQL equivalent:
#   CREATE OR REFRESH STREAMING TABLE stocks.bronze.premarket_raw AS
#   SELECT *, <lineage columns> FROM STREAM read_files('/Volumes/stocks/bronze/landing/premarket/',
#     format => 'json', pathGlobFilter => '*.jsonl', schema => '<PREMARKET_SCHEMA below>');
PREMARKET_SCHEMA = (
    "instrument STRING, name STRING, region STRING, asset_class STRING, unit STRING, yahoo_symbol STRING, "
    "obs_date STRING, value DOUBLE, change_abs DOUBLE, change_pct DOUBLE, change_5d_pct DOUBLE, "
    "change_20d_pct DOUBLE, reading STRING, fetched_at_ist STRING"
)

@dp.table(
    name="stocks.bronze.premarket_raw",
    comment="Raw pre-open world-markets snapshots from Yahoo (08:30 IST Mon-Fri), one row per instrument per file.",
    table_properties={"quality": "bronze"},
)
def premarket_raw():
    return with_lineage(
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.partitionColumns", "")
        .schema(PREMARKET_SCHEMA)
        .load(f"{LANDING}/premarket/")
    )
