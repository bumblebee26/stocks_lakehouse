# One-time stack that creates the S3 bucket holding Terraform state for the
# other stacks. It keeps its own (tiny) state locally on purpose.

terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "stocks-lakehouse"
      ManagedBy = "terraform"
      Stack     = "bootstrap"
    }
  }
}

variable "aws_region" {
  description = "AWS region for the state bucket."
  type        = string
  default     = "ap-southeast-2"
}

variable "name_prefix" {
  description = "Short prefix used in every resource name."
  type        = string
  default     = "stocks"
}

data "aws_caller_identity" "current" {}

module "state_bucket" {
  source = "../modules/secure_bucket"

  # Account ID suffix keeps the name globally unique without you picking one.
  bucket_name                        = "${var.name_prefix}-lakehouse-tfstate-${data.aws_caller_identity.current.account_id}"
  noncurrent_version_expiration_days = 90
}

output "state_bucket_name" {
  description = "Put this in infra/aws/backend.hcl and infra/databricks/backend.hcl."
  value       = module.state_bucket.id
}
