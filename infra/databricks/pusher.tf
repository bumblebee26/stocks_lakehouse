# ---------------------------------------------------------------------------
# Identity the AWS Lambdas use to push files into the landing volume.
#
# pusher_identity = "user" (default)
#   A dedicated personal access token for YOUR user, created by Terraform.
#   On Free Edition a service principal's Files API calls were rejected (403,
#   missing entitlement) even with workspace and SQL access granted, so this is
#   the working option. The token has your permissions, so it is short-lived,
#   used for nothing else, and readable only by the fetcher and sync Lambdas.
#
# pusher_identity = "service_principal"
#   Least privilege: an SP limited by Unity Catalog grants to the one volume.
#   Kept for workspaces where SP entitlements work (e.g. paid Databricks).
#
# Either way the token value lands in AWS SSM (SecureString) and also in
# Terraform state (private, encrypted, HTTPS-only bucket).
# ---------------------------------------------------------------------------

locals {
  use_sp        = var.pusher_identity == "service_principal"
  token_comment = "AWS Lambda fetchers -> ${var.catalog_name}.bronze.${var.landing_volume_name}. Managed by Terraform."
}

# ----- Option A: dedicated user token (default) ------------------------------
resource "databricks_token" "pusher" {
  count = local.use_sp ? 0 : 1

  comment          = local.token_comment
  lifetime_seconds = var.pusher_token_lifetime_days * 86400
}

# ----- Option B: service principal -------------------------------------------
resource "databricks_service_principal" "pusher" {
  count = local.use_sp ? 1 : 0

  display_name = "${var.name_prefix}-lambda-pusher"

  # Files API requires one of these entitlements; UC grants below still limit
  # the SP to the landing volume.
  workspace_access      = true
  databricks_sql_access = true
}

# Authoritative for token permissions, so the "users" group is kept too.
resource "databricks_permissions" "token_usage" {
  count = local.use_sp ? 1 : 0

  authorization = "tokens"

  access_control {
    group_name       = "users"
    permission_level = "CAN_USE"
  }

  access_control {
    service_principal_name = databricks_service_principal.pusher[0].application_id
    permission_level       = "CAN_USE"
  }
}

resource "databricks_obo_token" "pusher" {
  count = local.use_sp ? 1 : 0

  application_id   = databricks_service_principal.pusher[0].application_id
  comment          = local.token_comment
  lifetime_seconds = var.pusher_token_lifetime_days * 86400

  depends_on = [databricks_permissions.token_usage]
}

resource "databricks_grants" "catalog" {
  count   = local.use_sp ? 1 : 0
  catalog = databricks_catalog.stocks.name

  grant {
    principal  = databricks_service_principal.pusher[0].application_id
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "bronze" {
  count  = local.use_sp ? 1 : 0
  schema = databricks_schema.layer["bronze"].id

  grant {
    principal  = databricks_service_principal.pusher[0].application_id
    privileges = ["USE_SCHEMA"]
  }
}

resource "databricks_grants" "landing_volume" {
  count  = local.use_sp ? 1 : 0
  volume = databricks_volume.landing.id

  grant {
    principal  = databricks_service_principal.pusher[0].application_id
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}

# ----- Hand the token and URL to the Lambdas via SSM -------------------------
resource "aws_ssm_parameter" "pusher_token" {
  name        = "/${var.name_prefix}/databricks-pusher-token"
  description = "Databricks token for the Lambda fetchers (${var.pusher_identity})."
  type        = "SecureString"
  value       = local.use_sp ? databricks_obo_token.pusher[0].token_value : databricks_token.pusher[0].token_value
}

resource "aws_ssm_parameter" "workspace_url" {
  name        = "/${var.name_prefix}/databricks-workspace-url"
  description = "Databricks workspace URL for the Lambda fetchers."
  type        = "String"
  value       = trimsuffix(var.databricks_workspace_url, "/")
}
