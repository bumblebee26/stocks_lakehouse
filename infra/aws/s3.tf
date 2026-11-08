# Raw files from the fetchers land here:
#   s3://<bucket>/landing/{nse,massive,stooq}/date=YYYY-MM-DD/...
module "landing_bucket" {
  source = "../modules/secure_bucket"

  bucket_name                        = "${var.name_prefix}-lakehouse-landing-${local.account_id}"
  noncurrent_version_expiration_days = var.landing_noncurrent_version_days
}
