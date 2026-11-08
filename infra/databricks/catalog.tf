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
