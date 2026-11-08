# Pre-market brief: queries the gold tables via Databricks SQL and sends a
# Telegram message at 08:45 IST, Monday-Friday.
#
# Telegram settings are created as placeholders; set the real values yourself
# so they never land in Terraform state:
#   aws ssm put-parameter --name /stocks/telegram-bot-token --type SecureString --overwrite --value <token>
#   aws ssm put-parameter --name /stocks/telegram-chat-id   --type SecureString --overwrite --value <chat id>

locals {
  premarket_function_name    = "${var.name_prefix}-premarket-brief"
  databricks_warehouse_param = "/${var.name_prefix}/databricks-warehouse-id"
}

resource "aws_ssm_parameter" "telegram_bot_token" {
  name        = "/${var.name_prefix}/telegram-bot-token"
  description = "Telegram bot token for the pre-market brief. Real value set outside Terraform."
  type        = "SecureString"
  value       = "SET_ME_OUTSIDE_TERRAFORM"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "telegram_chat_id" {
  name        = "/${var.name_prefix}/telegram-chat-id"
  description = "Telegram chat ID that receives the pre-market brief. Real value set outside Terraform."
  type        = "SecureString"
  value       = "SET_ME_OUTSIDE_TERRAFORM"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_iam_role" "premarket" {
  name               = "${var.name_prefix}-premarket-role"
  description        = "Pre-market brief: read Databricks + Telegram settings from SSM. No S3 access."
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy_attachment" "premarket_logs" {
  role       = aws_iam_role.premarket.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "premarket" {
  statement {
    sid     = "ReadSettings"
    effect  = "Allow"
    actions = ["ssm:GetParameter"]
    resources = concat(local.databricks_param_arns, [
      "arn:${local.partition}:ssm:${var.aws_region}:${local.account_id}:parameter${local.databricks_warehouse_param}",
      aws_ssm_parameter.telegram_bot_token.arn,
      aws_ssm_parameter.telegram_chat_id.arn,
    ])
  }
}

resource "aws_iam_role_policy" "premarket" {
  name   = "read-settings"
  role   = aws_iam_role.premarket.id
  policy = data.aws_iam_policy_document.premarket.json
}

resource "aws_cloudwatch_log_group" "premarket" {
  name              = "/aws/lambda/${local.premarket_function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "premarket" {
  function_name = local.premarket_function_name
  description   = "Pre-market brief from the gold tables -> Telegram."
  role          = aws_iam_role.premarket.arn

  runtime       = var.lambda_python_runtime
  architectures = ["arm64"]
  handler       = "premarket.handler"
  timeout       = 300 # a cold serverless SQL warehouse can take a minute or two
  memory_size   = 256

  filename         = data.archive_file.fetchers.output_path
  source_code_hash = data.archive_file.fetchers.output_base64sha256

  environment {
    variables = {
      DATABRICKS_URL_PARAM       = local.databricks_url_param
      DATABRICKS_TOKEN_PARAM     = local.databricks_token_param
      DATABRICKS_WAREHOUSE_PARAM = local.databricks_warehouse_param
      TELEGRAM_TOKEN_PARAM       = aws_ssm_parameter.telegram_bot_token.name
      TELEGRAM_CHAT_ID_PARAM     = aws_ssm_parameter.telegram_chat_id.name
      US_PROXY                   = var.us_proxy_symbol
      INDIA_PROXY                = var.india_proxy_symbol
      BIG_MOVE_PCT               = tostring(var.big_move_pct)
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.premarket,
    aws_iam_role_policy_attachment.premarket_logs,
    aws_iam_role_policy.premarket,
  ]
}

resource "aws_iam_role_policy" "scheduler_invoke_premarket" {
  name = "invoke-premarket"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "InvokePremarketOnly"
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.premarket.arn
    }]
  })
}

resource "aws_scheduler_schedule" "premarket" {
  name        = local.premarket_function_name
  description = "Pre-market brief to Telegram, 08:45 IST Mon-Fri."
  state       = var.premarket_enabled ? "ENABLED" : "DISABLED"

  schedule_expression          = "cron(45 8 ? * MON-FRI *)"
  schedule_expression_timezone = "Asia/Kolkata"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.premarket.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts       = 1
      maximum_event_age_in_seconds = 1800
    }
  }
}

output "premarket_function_name" {
  value = aws_lambda_function.premarket.function_name
}
