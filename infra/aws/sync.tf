# One-off / repair function: copies files from S3 landing/ into the Databricks
# landing volume. Separate role because, unlike the fetchers, it must READ S3.
# Invoke manually; it has no schedule.

locals {
  sync_function_name = "${var.name_prefix}-sync-volume"
}

resource "aws_iam_role" "sync" {
  name               = "${var.name_prefix}-sync-volume-role"
  description        = "Copy S3 landing files to the Databricks volume: read-only S3."
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy_attachment" "sync_logs" {
  role       = aws_iam_role.sync.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "sync" {
  statement {
    sid       = "ListLandingOnly"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [module.landing_bucket.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["landing/*"]
    }
  }

  statement {
    sid       = "ReadLandingObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${module.landing_bucket.arn}/landing/*"]
  }

  statement {
    sid       = "ReadDatabricksPushConfig"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = local.databricks_param_arns
  }

  statement {
    sid       = "InvokeSelfForContinuation"
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = ["arn:${local.partition}:lambda:${var.aws_region}:${local.account_id}:function:${local.sync_function_name}"]
  }
}

resource "aws_iam_role_policy" "sync" {
  name   = "sync-to-volume"
  role   = aws_iam_role.sync.id
  policy = data.aws_iam_policy_document.sync.json
}

resource "aws_cloudwatch_log_group" "sync" {
  name              = "/aws/lambda/${local.sync_function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "sync" {
  function_name = local.sync_function_name
  description   = "Copy S3 landing/ files into the Databricks landing volume (skips existing)."
  role          = aws_iam_role.sync.arn

  runtime       = var.lambda_python_runtime
  architectures = ["arm64"]
  handler       = "sync.handler"
  timeout       = 900
  memory_size   = 256

  filename         = data.archive_file.fetchers.output_path
  source_code_hash = data.archive_file.fetchers.output_base64sha256

  environment {
    variables = {
      LANDING_BUCKET         = module.landing_bucket.id
      DATABRICKS_URL_PARAM   = local.databricks_url_param
      DATABRICKS_TOKEN_PARAM = local.databricks_token_param
      LANDING_VOLUME_PATH    = var.databricks_volume_path
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.sync,
    aws_iam_role_policy_attachment.sync_logs,
    aws_iam_role_policy.sync,
  ]
}

output "sync_function_name" {
  value = aws_lambda_function.sync.function_name
}
