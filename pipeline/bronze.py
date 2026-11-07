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
#     schemaEvolutionMode  => 'rescue'
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
#     schemaHints         => 'results ARRAY<STRUCT<T: STRING, o: DOUBLE, h: DOUBLE, l: DOUBLE, c: DOUBLE,
#                              v: DOUBLE, vw: DOUBLE, n: BIGINT, t: BIGINT, otc: BOOLEAN>>',
#     schemaEvolutionMode => 'rescue'
#   );
MASSIVE_SCHEMA_HINTS = (
    "results ARRAY<STRUCT<T: STRING, o: DOUBLE, h: DOUBLE, l: DOUBLE, c: DOUBLE, "
    "v: DOUBLE, vw: DOUBLE, n: BIGINT, t: BIGINT, otc: BOOLEAN>>"
)


@dp.table(
    name="stocks.bronze.massive_grouped_raw",
    comment="Raw Massive grouped-daily responses, one row per file (results array = all US tickers).",
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
        .load(f"{LANDING}/massive/")
    )
