output "landing_bucket_name" {
  description = "Landing bucket for raw files from the fetchers."
  value       = module.landing_bucket.id
}

output "landing_bucket_arn" {
  value = module.landing_bucket.arn
}

output "fetcher_role_arns" {
  description = "Execution role per Lambda fetcher."
  value       = { for k, r in aws_iam_role.fetcher : k => r.arn }
}

output "massive_api_key_parameter" {
  value = aws_ssm_parameter.massive_api_key.name
}

output "fred_api_key_parameter" {
  value = aws_ssm_parameter.fred_api_key.name
}

output "fetcher_function_names" {
  description = "Invoke these manually to test or backfill."
  value       = { for k, f in aws_lambda_function.fetcher : k => f.function_name }
}

output "schedules_enabled" {
  value = var.schedules_enabled
}
