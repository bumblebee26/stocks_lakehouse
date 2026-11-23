# ---------------------------------------------------------------------------
# Mobile app: "NSE Paper Trading Desk" on the phone (added 22 Nov 2026).
#
#   app/ (index.html, manifest, icon)  ->  s3://<app bucket>/app/
#   Lambda app-export, a few minutes after each Databricks job: runs the dashboard's own SQL
#   (config/queries.json, rendered by the databricks stack) -> app/data/desk.json
#   Served by one of two front doors, chosen by var.app_use_cloudfront:
#     false (now): a Lambda function URL (fetchers/app_web.py) reads app/ from the bucket. Used while AWS has
#       CloudFront blocked on the account (23 Nov 2026: "Your account must be verified").
#     true: CloudFront (HTTPS, origin access control); a viewer-request function serves /<secret path>/... from app/.
#   Either way only /<secret path>/<app file> is served, anything else is 404, and the bucket stays private.
#
# The link is the front door's address plus a random 32-character path, stored in SSM as a SecureString
# (/<prefix>/desk-app-url: AWS console -> Systems Manager -> Parameter Store -> Show decrypted value). The path is
# marked sensitive, so it never appears in the public plan or Actions logs. To revoke the link, replace
# terraform_data.app_path (terraform apply -replace=terraform_data.app_path): a new path is made.
# ---------------------------------------------------------------------------

locals {
  app_function_name = "${var.name_prefix}-app-export"
  app_bucket_name   = "${var.name_prefix}-desk-app-${local.account_id}"
  app_path          = sensitive(replace(terraform_data.app_path.output, "-", ""))
  app_files = {
    "index.html"           = "text/html; charset=utf-8"
    "manifest.webmanifest" = "application/manifest+json"
    "icon.svg"             = "image/svg+xml"
  }
  # Minutes after each job (IST): pre-open, morning, trading export, market close, late export, Saturday.
  app_export_schedules = {
    preopen   = "cron(10 9 ? * MON-FRI *)"
    morning   = "cron(40 11 ? * TUE-SAT *)"
    afternoon = "cron(15 16 ? * MON-FRI *)"
    close     = "cron(30 20 ? * MON-FRI *)"
    late      = "cron(55 23 ? * MON-FRI *)"
    weekend   = "cron(45 8 ? * SAT *)"
  }
}

# A random path, made once (uuid() is random; ignore_changes keeps the first one).
resource "terraform_data" "app_path" {
  input = uuid()

  lifecycle {
    ignore_changes = [input]
  }
}

data "aws_iam_policy_document" "app_bucket_cloudfront" {
  count = var.app_use_cloudfront ? 1 : 0

  statement {
    sid       = "CloudFrontReadsTheAppPathOnly"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["arn:${local.partition}:s3:::${local.app_bucket_name}/app/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.app[0].arn]
    }
  }
}

module "app_bucket" {
  source = "../modules/secure_bucket"

  bucket_name                        = local.app_bucket_name
  noncurrent_version_expiration_days = 7
  force_destroy                      = true
  extra_policy_json                  = var.app_use_cloudfront ? data.aws_iam_policy_document.app_bucket_cloudfront[0].json : null
}

resource "aws_s3_object" "app_files" {
  for_each = local.app_files

  bucket        = module.app_bucket.id
  key           = "app/${each.key}"
  source        = "${path.module}/../../app/${each.key}"
  etag          = filemd5("${path.module}/../../app/${each.key}")
  content_type  = each.value
  cache_control = "max-age=300"
}

resource "aws_cloudfront_origin_access_control" "app" {
  count = var.app_use_cloudfront ? 1 : 0

  name                              = "${var.name_prefix}-desk-app"
  description                       = "CloudFront reads the desk app bucket (private)."
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# Serves /<secret path>/... from app/ in the bucket; everything else is 404. Runs at the edge (CloudFront
# Functions: 2 million invocations a month are free).
resource "aws_cloudfront_function" "app_path" {
  count = var.app_use_cloudfront ? 1 : 0

  name    = "${var.name_prefix}-desk-app-path"
  runtime = "cloudfront-js-2.0"
  comment = "Secret path -> app/ (desk app)"
  publish = true
  code    = replace(file("${path.module}/app_path.js"), "__APP_PATH__", local.app_path)
}

resource "aws_cloudfront_distribution" "app" {
  count = var.app_use_cloudfront ? 1 : 0

  enabled             = true
  comment             = "NSE Paper Trading Desk mobile app"
  price_class         = "PriceClass_200" # includes India
  is_ipv6_enabled     = true
  default_root_object = "index.html"

  origin {
    domain_name              = "${local.app_bucket_name}.s3.${var.aws_region}.amazonaws.com"
    origin_id                = "app"
    origin_access_control_id = aws_cloudfront_origin_access_control.app[0].id
  }

  default_cache_behavior {
    target_origin_id           = "app"
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD"]
    cached_methods             = ["GET", "HEAD"]
    compress                   = true
    cache_policy_id            = "83da9c7e-98b4-4e11-a168-04f0df8e2c65" # managed: UseOriginCacheControlHeaders
    response_headers_policy_id = "67f7725c-6f97-4210-82d7-5512b31e9d03" # managed: SecurityHeadersPolicy

    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.app_path[0].arn
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

resource "aws_iam_role" "app_export" {
  name               = "${var.name_prefix}-app-export-role"
  description        = "App export: read the dashboard queries, run them on Databricks SQL, write the app's data file."
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy_attachment" "app_export_logs" {
  role       = aws_iam_role.app_export.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "app_export" {
  statement {
    sid     = "ReadDatabricksSettings"
    effect  = "Allow"
    actions = ["ssm:GetParameter"]
    resources = concat(local.databricks_param_arns, [
      "arn:${local.partition}:ssm:${var.aws_region}:${local.account_id}:parameter${local.databricks_warehouse_param}",
    ])
  }

  statement {
    sid       = "ReadQueries"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${module.app_bucket.arn}/config/queries.json"]
  }

  statement {
    sid       = "ReadWriteAppData"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${module.app_bucket.arn}/app/data/*"]
  }
}

resource "aws_iam_role_policy" "app_export" {
  name   = "app-export"
  role   = aws_iam_role.app_export.id
  policy = data.aws_iam_policy_document.app_export.json
}

resource "aws_cloudwatch_log_group" "app_export" {
  name              = "/aws/lambda/${local.app_function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "app_export" {
  function_name = local.app_function_name
  description   = "Mobile app: the dashboard's SQL -> one JSON file under the app's secret path."
  role          = aws_iam_role.app_export.arn

  runtime       = var.lambda_python_runtime
  architectures = ["arm64"]
  handler       = "app_export.handler"
  timeout       = 600 # a cold serverless warehouse plus ~17 queries
  memory_size   = 256

  filename         = data.archive_file.fetchers.output_path
  source_code_hash = data.archive_file.fetchers.output_base64sha256

  environment {
    variables = {
      DATABRICKS_URL_PARAM       = local.databricks_url_param
      DATABRICKS_TOKEN_PARAM     = local.databricks_token_param
      DATABRICKS_WAREHOUSE_PARAM = local.databricks_warehouse_param
      APP_BUCKET                 = module.app_bucket.id
      APP_PREFIX                 = "app"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.app_export,
    aws_iam_role_policy_attachment.app_export_logs,
    aws_iam_role_policy.app_export,
  ]
}

resource "aws_iam_role_policy" "scheduler_invoke_app_export" {
  name = "invoke-app-export"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InvokeAppExportOnly"
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.app_export.arn
    }]
  })
}

resource "aws_scheduler_schedule" "app_export" {
  for_each = local.app_export_schedules

  name        = "${local.app_function_name}-${each.key}"
  description = "Mobile app data export after the ${each.key} Databricks run (IST)."
  state       = "ENABLED"

  schedule_expression          = each.value
  schedule_expression_timezone = "Asia/Kolkata"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.app_export.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts       = 1
      maximum_event_age_in_seconds = 1800
    }
  }
}

locals {
  app_base_url = var.app_use_cloudfront ? "https://${aws_cloudfront_distribution.app[0].domain_name}" : trimsuffix(aws_lambda_function_url.app_web[0].function_url, "/")
}

resource "aws_ssm_parameter" "app_url" {
  name        = "/${var.name_prefix}/desk-app-url"
  description = "The desk mobile app's link (address + secret path). Anyone with it can open the app."
  type        = "SecureString"
  value       = "${local.app_base_url}/${local.app_path}/"
}

# ---------------------------------------------------------------------------
# Lambda function URL front door (var.app_use_cloudfront = false). Free tier: 1 million requests a month.
# ---------------------------------------------------------------------------

locals {
  app_web_function_name = "${var.name_prefix}-app-web"
}

resource "aws_iam_role" "app_web" {
  count = var.app_use_cloudfront ? 0 : 1

  name               = "${var.name_prefix}-app-web-role"
  description        = "Desk app web server: read the app's files from its bucket."
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy_attachment" "app_web_logs" {
  count = var.app_use_cloudfront ? 0 : 1

  role       = aws_iam_role.app_web[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "app_web" {
  count = var.app_use_cloudfront ? 0 : 1

  name = "app-web"
  role = aws_iam_role.app_web[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "ReadAppFilesOnly"
      Effect   = "Allow"
      Action   = "s3:GetObject"
      Resource = "${module.app_bucket.arn}/app/*"
    }]
  })
}

resource "aws_cloudwatch_log_group" "app_web" {
  count = var.app_use_cloudfront ? 0 : 1

  name              = "/aws/lambda/${local.app_web_function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "app_web" {
  count = var.app_use_cloudfront ? 0 : 1

  function_name = local.app_web_function_name
  description   = "Desk mobile app: serves /<secret path>/<app file> from the private bucket; 404 otherwise."
  role          = aws_iam_role.app_web[0].arn

  runtime       = var.lambda_python_runtime
  architectures = ["arm64"]
  handler       = "app_web.handler"
  timeout       = 10
  memory_size   = 128

  filename         = data.archive_file.fetchers.output_path
  source_code_hash = data.archive_file.fetchers.output_base64sha256

  environment {
    variables = {
      APP_BUCKET = module.app_bucket.id
      APP_PREFIX = "app"
      APP_PATH   = local.app_path # sensitive: hidden in the plan and Actions logs
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.app_web,
    aws_iam_role_policy_attachment.app_web_logs,
    aws_iam_role_policy.app_web,
  ]
}

resource "aws_lambda_function_url" "app_web" {
  count = var.app_use_cloudfront ? 0 : 1

  function_name      = aws_lambda_function.app_web[0].function_name
  authorization_type = "NONE" # a phone can't sign requests; the secret path is the key
}

# A public (NONE) function URL needs both statements (AWS, Oct 2025); InvokeFunction is limited to calls that
# come through the URL.
resource "aws_lambda_permission" "app_web_url" {
  count = var.app_use_cloudfront ? 0 : 1

  statement_id           = "FunctionURLAllowPublicAccess"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.app_web[0].function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "app_web_invoke" {
  count = var.app_use_cloudfront ? 0 : 1

  statement_id             = "FunctionURLInvokeAllowPublicAccess"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.app_web[0].function_name
  principal                = "*"
  invoked_via_function_url = true
}

# One-off export right after a deploy that changes the app's data (24 Nov: new charts), so the phone doesn't wait for
# the next scheduled run. Change the time to run it again.
resource "aws_scheduler_schedule" "app_export_once" {
  name        = "${local.app_function_name}-once"
  description = "One-off app data export after the 24 Nov app update."
  state       = "ENABLED"

  schedule_expression          = "at(2026-11-24T04:20:00)"
  schedule_expression_timezone = "Asia/Kolkata"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.app_export.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})
  }
}
