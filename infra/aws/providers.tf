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
}
