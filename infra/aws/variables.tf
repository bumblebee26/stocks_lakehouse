variable "aws_region" {
  description = <<-EOT
    AWS region for all project resources. On the AWS free plan (projects
    experience) only us-east-1, us-west-2 and the project's own region are
    allowed; this project's region is Sydney.
  EOT
  type        = string
  default     = "ap-southeast-2"
}

variable "name_prefix" {
  description = "Short prefix used in every resource name (IAM roles, buckets, parameters)."
  type        = string
  default     = "stocks"
}

variable "fetchers" {
  description = "One Lambda execution role per fetcher. Each may only write to its own landing prefixes."
  type = map(object({
    landing_prefixes  = list(string)
    needs_massive_key = bool
    needs_fred_key    = bool
  }))
  default = {
    nse    = { landing_prefixes = ["nse"], needs_massive_key = false, needs_fred_key = false }
    us     = { landing_prefixes = ["massive"], needs_massive_key = true, needs_fred_key = false }
    global = { landing_prefixes = ["fred"], needs_massive_key = false, needs_fred_key = true }
  }
}

variable "landing_noncurrent_version_days" {
  description = "Days to keep overwritten or deleted versions of landing files."
  type        = number
  default     = 30
}

variable "lambda_python_runtime" {
  description = "Lambda runtime for the fetchers."
  type        = string
  default     = "python3.13"
}

variable "log_retention_days" {
  description = "Days to keep fetcher logs in CloudWatch."
  type        = number
  default     = 14
}

variable "schedules_enabled" {
  description = "Turn the daily schedules on. Leave false until a manual test run succeeds."
  type        = bool
  default     = false
}

variable "databricks_push_enabled" {
  description = "Also push each fetched file to the Databricks landing volume (needs the databricks stack applied)."
  type        = bool
  default     = true
}

variable "databricks_volume_path" {
  description = "Unity Catalog volume the fetchers push into (output landing_volume_path of the databricks stack)."
  type        = string
  default     = "/Volumes/stocks/bronze/landing"
}

# ----- Pre-market brief -------------------------------------------------------
variable "premarket_enabled" {
  description = "Send the pre-market brief to Telegram at 08:45 IST Mon-Fri. Leave false until a dry run looks right."
  type        = bool
  default     = false
}

variable "us_proxy_symbol" {
  description = "US ticker used as the US market proxy (S&P 500 ETF). Keep in sync with the databricks stack."
  type        = string
  default     = "SPY"
}

variable "india_proxy_symbol" {
  description = "NSE symbol used as the Indian market proxy (Nifty 50 ETF). Keep in sync with the databricks stack."
  type        = string
  default     = "NIFTYBEES"
}

variable "big_move_pct" {
  description = "A US day counts as a big move at or beyond this % change. Keep in sync with the databricks stack."
  type        = number
  default     = 1.0
}

variable "fred_enabled" {
  description = "Run the daily FRED (global markets) fetch. Turn on after the FRED API key is stored in SSM."
  type        = bool
  default     = false
}
