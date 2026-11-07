# ---------------------------------------------------------------------------
# Groups (account level)
# ---------------------------------------------------------------------------
resource "databricks_group" "admins" {
  provider     = databricks.account
  display_name = "${var.name_prefix}-admins"
}

resource "databricks_group" "analysts" {
  provider     = databricks.account
  display_name = "${var.name_prefix}-analysts"
}

# ---------------------------------------------------------------------------
# People
# ---------------------------------------------------------------------------
# Admins already exist (you signed up), so look them up instead of creating them.
data "databricks_user" "admins" {
  for_each = toset(var.admin_user_emails)

  provider  = databricks.account
  user_name = each.value
}

resource "databricks_group_member" "admins" {
  for_each = data.databricks_user.admins

  provider  = databricks.account
  group_id  = databricks_group.admins.id
  member_id = each.value.id
}

# Analysts are new users; Databricks emails them an invite.
resource "databricks_user" "analysts" {
  for_each = toset(var.analyst_user_emails)

  provider  = databricks.account
  user_name = each.value
}

resource "databricks_group_member" "analysts" {
  for_each = databricks_user.analysts

  provider  = databricks.account
  group_id  = databricks_group.analysts.id
  member_id = each.value.id
}

# ---------------------------------------------------------------------------
# Pipeline service principal: runs the Lakeflow pipeline and job.
# No secret is created; it never logs in, it is only a "run as" identity.
# ---------------------------------------------------------------------------
resource "databricks_service_principal" "pipeline" {
  provider     = databricks.account
  display_name = "${var.name_prefix}-pipeline-sp"
}

# ---------------------------------------------------------------------------
# Give the groups and the SP access to the workspace
# ---------------------------------------------------------------------------
resource "databricks_mws_permission_assignment" "admins" {
  provider     = databricks.account
  workspace_id = var.databricks_workspace_id
  principal_id = databricks_group.admins.id
  permissions  = ["ADMIN"]
}

resource "databricks_mws_permission_assignment" "analysts" {
  provider     = databricks.account
  workspace_id = var.databricks_workspace_id
  principal_id = databricks_group.analysts.id
  permissions  = ["USER"]
}

resource "databricks_mws_permission_assignment" "pipeline_sp" {
  provider     = databricks.account
  workspace_id = var.databricks_workspace_id
  principal_id = databricks_service_principal.pipeline.id
  permissions  = ["USER"]
}
