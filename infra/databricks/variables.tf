variable "aws_region" {
  description = "AWS region (same as the aws stack)."
  type        = string
  default     = "ap-south-1"
}

variable "name_prefix" {
  description = "Short prefix used in every resource name."
  type        = string
  default     = "stocks"
}

variable "databricks_account_id" {
  description = "Databricks account ID (account console, top-right user menu)."
  type        = string
}

variable "databricks_workspace_url" {
  description = "Workspace URL, e.g. https://dbc-1234abcd-5678.cloud.databricks.com"
  type        = string
}

variable "databricks_workspace_id" {
  description = "Numeric workspace ID (the number after 'o=' in the workspace URL, or in the account console)."
  type        = number
}

variable "admin_user_emails" {
  description = "Existing Databricks users (already in the account) to add to the project admins group."
  type        = list(string)
}

variable "analyst_user_emails" {
  description = "Users to create and add to the read-only analysts group. Leave empty for now if it's just you."
  type        = list(string)
  default     = []
}

variable "landing_bucket_name" {
  description = "Output 'landing_bucket_name' from the aws stack."
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
