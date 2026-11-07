output "pipeline_sp_application_id" {
  description = "Set the Lakeflow pipeline and job 'Run as' to this service principal."
  value       = databricks_service_principal.pipeline.application_id
}

output "admins_group" {
  value = databricks_group.admins.display_name
}

output "analysts_group" {
  value = databricks_group.analysts.display_name
}

output "landing_external_location" {
  value = databricks_external_location.landing.name
}

output "landing_url" {
  description = "Path for Auto Loader / read_files in the pipeline."
  value       = databricks_external_location.landing.url
}

output "uc_landing_role_arn" {
  value = aws_iam_role.uc_landing.arn
}

output "catalog" {
  value = databricks_catalog.stocks.name
}
