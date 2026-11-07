# ---------------------------------------------------------------------------
# Catalog and medallion schemas
# ---------------------------------------------------------------------------
# Managed tables use the workspace's default storage. If your workspace rejects
# a catalog without a storage location, create it once in the UI (Catalog -> +
# -> Create catalog, default storage) and import it:
#   terraform import databricks_catalog.stocks stocks
resource "databricks_catalog" "stocks" {
  provider = databricks.workspace

  name    = var.catalog_name
  comment = "Multi-market stock lakehouse. Managed by Terraform."
}

resource "databricks_schema" "layer" {
  for_each = toset(var.schemas)

  provider     = databricks.workspace
  catalog_name = databricks_catalog.stocks.name
  name         = each.value
  comment      = "${each.value} layer. Managed by Terraform."
}

# ---------------------------------------------------------------------------
# Grants. databricks_grants is authoritative: each block below is the
# complete list of grants on that object, so anything added by hand is removed.
# ---------------------------------------------------------------------------
locals {
  admins_group   = databricks_group.admins.display_name
  analysts_group = databricks_group.analysts.display_name
  pipeline_sp    = databricks_service_principal.pipeline.application_id
}

resource "databricks_grants" "landing_credential" {
  provider           = databricks.workspace
  storage_credential = databricks_storage_credential.landing.id

  grant {
    principal  = local.admins_group
    privileges = ["ALL_PRIVILEGES"]
  }

  depends_on = [databricks_mws_permission_assignment.admins]
}

# Only admins and the pipeline can read raw files; analysts never can.
resource "databricks_grants" "landing_location" {
  provider          = databricks.workspace
  external_location = databricks_external_location.landing.id

  grant {
    principal  = local.admins_group
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = local.pipeline_sp
    privileges = ["READ_FILES"]
  }

  depends_on = [
    databricks_mws_permission_assignment.admins,
    databricks_mws_permission_assignment.pipeline_sp,
  ]
}

resource "databricks_grants" "catalog" {
  provider = databricks.workspace
  catalog  = databricks_catalog.stocks.name

  grant {
    principal  = local.admins_group
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = local.pipeline_sp
    privileges = ["USE_CATALOG"]
  }

  grant {
    principal  = local.analysts_group
    privileges = ["USE_CATALOG"]
  }

  depends_on = [
    databricks_mws_permission_assignment.admins,
    databricks_mws_permission_assignment.analysts,
    databricks_mws_permission_assignment.pipeline_sp,
  ]
}

# Pipeline writes every layer; analysts read gold only.
resource "databricks_grants" "schema" {
  for_each = databricks_schema.layer

  provider = databricks.workspace
  schema   = each.value.id

  grant {
    principal = local.pipeline_sp
    privileges = [
      "USE_SCHEMA",
      "CREATE_TABLE",
      "CREATE_MATERIALIZED_VIEW",
      "SELECT",
      "MODIFY",
    ]
  }

  dynamic "grant" {
    for_each = each.key == "gold" ? [1] : []

    content {
      principal  = local.analysts_group
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }

  depends_on = [
    databricks_mws_permission_assignment.analysts,
    databricks_mws_permission_assignment.pipeline_sp,
  ]
}
