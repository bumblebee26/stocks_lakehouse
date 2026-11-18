output "catalog" {
  value = databricks_catalog.stocks.name
}

output "landing_volume_path" {
  description = "Where the Lambdas push files; Auto Loader reads from here."
  value       = "/Volumes/${databricks_volume.landing.catalog_name}/${databricks_volume.landing.schema_name}/${databricks_volume.landing.name}"
}

output "pusher_identity" {
  value = var.pusher_identity
}

output "pusher_sp_application_id" {
  value = local.use_sp ? databricks_service_principal.pusher[0].application_id : null
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

output "pipeline_ids" {
  value = { for k, p in databricks_pipeline.stocks : k => p.id }
}

output "pipeline_urls" {
  value = { for k, p in databricks_pipeline.stocks : k => "${trimsuffix(var.databricks_workspace_url, "/")}/pipelines/${p.id}" }
}

output "job_ids" {
  value = {
    market_close   = databricks_job.market_close.id
    market_morning = databricks_job.market_morning.id
    market_weekend = databricks_job.market_weekend.id
    trading        = databricks_job.trading.id
  }
}

output "job_trigger" {
  value = var.job_trigger
}
