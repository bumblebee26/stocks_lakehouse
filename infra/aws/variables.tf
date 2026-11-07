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
  }))
  default = {
    nse = { landing_prefixes = ["nse"], needs_massive_key = false }
    us  = { landing_prefixes = ["massive", "stooq"], needs_massive_key = true }
  }
}

variable "landing_noncurrent_version_days" {
  description = "Days to keep overwritten or deleted versions of landing files."
  type        = number
  default     = 30
}
