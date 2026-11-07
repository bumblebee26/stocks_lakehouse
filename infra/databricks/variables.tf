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

variable "pusher_identity" {
  description = "Identity behind the Lambda's Databricks token: \"user\" (dedicated personal token; works on Free Edition) or \"service_principal\" (least privilege)."
  type        = string
  default     = "user"

  validation {
    condition     = contains(["user", "service_principal"], var.pusher_identity)
    error_message = "pusher_identity must be \"user\" or \"service_principal\"."
  }
}

# ----- Pipeline and job -------------------------------------------------------
variable "pipeline_development_mode" {
  description = "Lakeflow development mode (faster iteration, no automatic retries). Turn off once stable."
  type        = bool
  default     = true
}

variable "job_trigger" {
  description = "How the refresh job starts: \"file_arrival\" (when new files land in the volume) or \"schedule\" (06:00 IST Tue-Sat)."
  type        = string
  default     = "file_arrival"

  validation {
    condition     = contains(["file_arrival", "schedule"], var.job_trigger)
    error_message = "job_trigger must be \"file_arrival\" or \"schedule\"."
  }
}

variable "india_proxy_symbol" {
  description = "NSE symbol used as the Indian market proxy in gold (Nifty 50 ETF)."
  type        = string
  default     = "NIFTYBEES"
}

variable "us_proxy_symbol" {
  description = "US ticker used as the US market proxy in gold (S&P 500 ETF)."
  type        = string
  default     = "SPY"
}

variable "big_move_pct" {
  description = "A US day counts as a big move at or beyond this % change."
  type        = number
  default     = 1.0
}

variable "volume_spike_ratio" {
  description = "Unusual volume = at least this many times the prior 20-day average."
  type        = number
  default     = 3.0
}

variable "min_avg_volume" {
  description = "Ignore stocks whose 20-day average volume is below this (illiquid names)."
  type        = number
  default     = 50000
}
