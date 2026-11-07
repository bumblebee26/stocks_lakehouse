output "catalog" {
  value = databricks_catalog.stocks.name
}

output "landing_volume_path" {
  description = "Where the Lambdas push files; Auto Loader reads from here."
  value       = "/Volumes/${databricks_volume.landing.catalog_name}/${databricks_volume.landing.schema_name}/${databricks_volume.landing.name}"
}

output "pusher_sp_application_id" {
  value = databricks_service_principal.pusher.application_id
}

output "pusher_token_parameter" {
  value = aws_ssm_parameter.pusher_token.name
}

output "workspace_url_parameter" {
  value = aws_ssm_parameter.workspace_url.name
}

output "pusher_token_expiry_days" {
  value = var.pusher_token_lifetime_days
}
