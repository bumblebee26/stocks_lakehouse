variable "aws_region" {
  description = "AWS region (same as the aws stack)."
  type        = string
  default     = "ap-southeast-2"
}

variable "name_prefix" {
  description = "Short prefix used in resource names."
  type        = string
  default     = "stocks"
}

variable "databricks_workspace_url" {
  description = "Workspace URL without the ?o= part, e.g. https://dbc-1234abcd-5678.cloud.databricks.com"
  type        = string
}

variable "catalog_name" {
  description = "Unity Catalog catalog for the project."
  type        = string
  default     = "stocks"
}

variable "schemas" {
  description = "Medallion schemas inside the catalog."
  type        = list(string)
  default     = ["bronze", "silver", "gold"]
}

variable "landing_volume_name" {
  description = "Managed volume in the bronze schema that the Lambdas push raw files into."
  type        = string
  default     = "landing"
}

variable "pusher_token_lifetime_days" {
  description = "Lifetime of the Lambda's Databricks token. Re-apply before it expires to rotate it."
  type        = number
  default     = 180
}
