terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.70.0, < 2.0.0"
    }
  }

  # Partial config: values come from backend.hcl (see backend.hcl.example).
  backend "s3" {}
}
