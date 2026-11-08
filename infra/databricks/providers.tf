# Databricks Free Edition has no account console or account-level APIs, so this
# stack uses only a workspace-level provider.
#
# Terraform authenticates with a bootstrap personal access token you create once
# in the UI (Settings -> Developer -> Access tokens). Export it before running:
#   set DATABRICKS_TOKEN=dapi...
# The workspace URL comes from terraform.tfvars.

provider "databricks" {
  host = var.databricks_workspace_url
}

# Used only to store the Lambda's Databricks token in SSM Parameter Store.
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
