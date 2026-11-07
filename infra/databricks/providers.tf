# Both Databricks providers authenticate as the bootstrap service principal via
# OAuth machine-to-machine. Export before running terraform:
#   export DATABRICKS_CLIENT_ID=<terraform-sp application id>
#   export DATABRICKS_CLIENT_SECRET=<terraform-sp OAuth secret>
# See README "Databricks bootstrap" for how to create that service principal.

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "stocks-lakehouse"
      ManagedBy = "terraform"
      Stack     = "databricks"
    }
  }
}

# Account level: users, groups, service principals, workspace assignment.
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}

# Workspace level: Unity Catalog objects and grants.
provider "databricks" {
  alias = "workspace"
  host  = var.databricks_workspace_url
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
}
