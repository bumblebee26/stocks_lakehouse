# ---------------------------------------------------------------------------
# Catalog, medallion schemas, and the landing volume
# ---------------------------------------------------------------------------
# Free Edition uses "Default Storage", which the API can't create catalogs in.
# So the catalog is created ONCE in the UI (Catalog -> + -> Create catalog,
# name "stocks", Default Storage) and then imported:
#   terraform import databricks_catalog.stocks stocks
#
# Schemas and the volume are created by Terraform normally. If you already
# made them with SQL, import those too:
#   terraform import "databricks_schema.layer[\"bronze\"]" stocks.bronze
#   terraform import "databricks_schema.layer[\"silver\"]" stocks.silver
#   terraform import "databricks_schema.layer[\"gold\"]"   stocks.gold
#   terraform import databricks_volume.landing stocks.bronze.landing

resource "databricks_catalog" "stocks" {
  name    = var.catalog_name
  comment = "Multi-market stock lakehouse. Managed by Terraform."

  # The UI assigns Default Storage; don't let Terraform try to change or replace it.
  lifecycle {
    ignore_changes  = [storage_root, comment]
    prevent_destroy = true
  }
}

resource "databricks_schema" "layer" {
  for_each = toset(var.schemas)

  catalog_name = databricks_catalog.stocks.name
  name         = each.value
  comment      = "${each.value} layer. Managed by Terraform."
}

resource "databricks_volume" "landing" {
  catalog_name = databricks_catalog.stocks.name
  schema_name  = databricks_schema.layer["bronze"].name
  name         = var.landing_volume_name
  volume_type  = "MANAGED"
  comment      = "Raw files pushed by the AWS Lambda fetchers. Managed by Terraform."
}

# Company news (NSE RSS every 30 minutes). Its own volume, outside the job's file-arrival
# trigger: news lands too often to refresh the pipeline each time on Free Edition.
resource "databricks_volume" "news" {
  catalog_name = databricks_catalog.stocks.name
  schema_name  = databricks_schema.layer["bronze"].name
  name         = "news"
  volume_type  = "MANAGED"
  comment      = "NSE company news JSON lines, pushed every 30 minutes by the stocks-fetch-news Lambda. Managed by Terraform."
}

# Make sure each source folder exists in the landing volume, so Auto Loader has
# a path to watch even before a source's first file arrives (e.g. FRED until its
# API key is set). The pipeline only reads *.csv / *.json, so these are ignored.
resource "databricks_file" "landing_readme" {
  for_each = {
    nse     = "NSE equities bhavcopy (UDiFF CSV, unzipped by the Lambda). One folder per trading date."
    massive = "Massive grouped daily (all US stocks, raw JSON). One folder per US trading date."
    fred    = "FRED global markets (newline-delimited JSON). One folder per fetch date; each file covers a date range."
    trades  = "Paper-trader journal (dummy money, JSON lines), pushed daily at 15:45 IST by stocks-trader-guardian (bumblebee26/paper-trader)."
  }

  path           = "/Volumes/${databricks_volume.landing.catalog_name}/${databricks_volume.landing.schema_name}/${databricks_volume.landing.name}/${each.key}/_README.txt"
  content_base64 = base64encode("${each.value}\nWritten by AWS Lambda. Managed by Terraform.\n")
}
