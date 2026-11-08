provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "stocks-lakehouse"
      ManagedBy = "terraform"
      Stack     = "aws"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  # Created by the databricks stack; referenced here by name.
  databricks_url_param   = "/${var.name_prefix}/databricks-workspace-url"
  databricks_token_param = "/${var.name_prefix}/databricks-pusher-token"
  databricks_param_arns = [
    for name in [local.databricks_url_param, local.databricks_token_param] :
    "arn:${local.partition}:ssm:${var.aws_region}:${local.account_id}:parameter${name}"
  ]
}
