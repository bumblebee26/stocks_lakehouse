variable "aws_region" {
  description = "AWS region for all project resources. Mumbai keeps you close to NSE."
  type        = string
  default     = "ap-south-1"
}

variable "name_prefix" {
  description = "Short prefix used in every resource name (IAM roles, groups, buckets)."
  type        = string
  default     = "stocks"
}

variable "admin_user_name" {
  description = "IAM user for the human project admin."
  type        = string
  default     = "stocks-admin"
}

variable "create_console_login" {
  description = "Create a console password for the admin user. It must be changed at first sign-in."
  type        = bool
  default     = true
}

variable "manage_account_password_policy" {
  description = "Set an account-wide IAM password policy. Turn off if your account already has one you want to keep."
  type        = bool
  default     = true
}

variable "admin_role_session_hours" {
  description = "How long an assumed admin-role session lasts."
  type        = number
  default     = 4
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
