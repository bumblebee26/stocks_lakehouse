# Massive (US market data) API key, stored as an SSM SecureString.
# Standard-tier parameters are free, unlike Secrets Manager.
#
# Terraform only creates a placeholder; set the real key yourself so it never
# lands in Terraform state:
#   aws ssm put-parameter --name /stocks/massive-api-key \
#     --type SecureString --overwrite --value 'YOUR_KEY'
resource "aws_ssm_parameter" "massive_api_key" {
  name        = "/${var.name_prefix}/massive-api-key"
  description = "Massive API key for the US fetcher. Real value set outside Terraform."
  type        = "SecureString"
  value       = "SET_ME_OUTSIDE_TERRAFORM"

  lifecycle {
    ignore_changes = [value]
  }
}
