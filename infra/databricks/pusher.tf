# ---------------------------------------------------------------------------
# Identity the AWS Lambdas use to push files into the landing volume.
#
# A workspace service principal (not you) owns the token, so the Lambda can
# only do what this SP is granted: write to one volume, nothing else.
# Its token is stored straight into AWS SSM as a SecureString.
#
# Note: the token value is also kept in Terraform state. The state bucket is
# private, encrypted and HTTPS-only, which is acceptable for this project.
# ---------------------------------------------------------------------------

resource "databricks_service_principal" "pusher" {
  display_name = "${var.name_prefix}-lambda-pusher"
}

# Allow the SP to hold a token. This resource is authoritative for token
# permissions, so the "users" group is kept too (admins always have access).
resource "databricks_permissions" "token_usage" {
  authorization = "tokens"

  access_control {
    group_name       = "users"
    permission_level = "CAN_USE"
  }

  access_control {
    service_principal_name = databricks_service_principal.pusher.application_id
    permission_level       = "CAN_USE"
  }
}

resource "databricks_obo_token" "pusher" {
  application_id   = databricks_service_principal.pusher.application_id
  comment          = "AWS Lambda fetchers -> ${var.catalog_name}.bronze.${var.landing_volume_name}. Managed by Terraform."
  lifetime_seconds = var.pusher_token_lifetime_days * 86400

  depends_on = [databricks_permissions.token_usage]
}

resource "aws_ssm_parameter" "pusher_token" {
  name        = "/${var.name_prefix}/databricks-pusher-token"
  description = "Databricks token for the Lambda fetchers (service principal ${databricks_service_principal.pusher.display_name})."
  type        = "SecureString"
  value       = databricks_obo_token.pusher.token_value
}

resource "aws_ssm_parameter" "workspace_url" {
  name        = "/${var.name_prefix}/databricks-workspace-url"
  description = "Databricks workspace URL for the Lambda fetchers."
  type        = "String"
  value       = trimsuffix(var.databricks_workspace_url, "/")
}

# ---------------------------------------------------------------------------
# Grants: the SP may write files to the landing volume and nothing else.
# databricks_grants is authoritative per object; as creator you stay owner.
# ---------------------------------------------------------------------------
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.stocks.name

  grant {
    principal  = databricks_service_principal.pusher.application_id
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "bronze" {
  schema = databricks_schema.layer["bronze"].id

  grant {
    principal  = databricks_service_principal.pusher.application_id
    privileges = ["USE_SCHEMA"]
  }
}

resource "databricks_grants" "landing_volume" {
  volume = databricks_volume.landing.id

  grant {
    principal  = databricks_service_principal.pusher.application_id
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}
