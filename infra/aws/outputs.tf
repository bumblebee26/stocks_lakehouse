output "landing_bucket_name" {
  description = "Landing bucket. Pass this to the databricks stack as landing_bucket_name."
  value       = module.landing_bucket.id
}

output "landing_bucket_arn" {
  value = module.landing_bucket.arn
}

output "admin_user_name" {
  value = aws_iam_user.admin.name
}

output "admin_role_arn" {
  description = "Use as role_arn in your AWS CLI profile."
  value       = aws_iam_role.admin.arn
}

output "admin_console_signin_url" {
  value = "https://${local.account_id}.signin.aws.amazon.com/console"
}

output "admin_switch_role_url" {
  description = "Open after signing in with MFA to switch into the admin role."
  value       = "https://signin.aws.amazon.com/switchrole?account=${local.account_id}&roleName=${aws_iam_role.admin.name}&displayName=stocks-admin"
}

output "admin_initial_password" {
  description = "One-time password; you must change it at first sign-in. Read with: terraform output -raw admin_initial_password"
  value       = var.create_console_login ? aws_iam_user_login_profile.admin[0].password : null
  sensitive   = true
}

output "fetcher_role_arns" {
  description = "Execution role per Lambda fetcher."
  value       = { for k, r in aws_iam_role.fetcher : k => r.arn }
}

output "massive_api_key_parameter" {
  value = aws_ssm_parameter.massive_api_key.name
}
