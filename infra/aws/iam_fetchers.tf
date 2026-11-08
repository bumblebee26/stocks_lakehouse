# One execution role per Lambda fetcher. Each can only ADD files under its
# own landing prefixes: no reads, no deletes, no listing, no other prefixes.

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    sid     = "LambdaServiceInThisAccount"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    # Guards against the confused-deputy problem.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "fetcher" {
  for_each = var.fetchers

  name               = "${var.name_prefix}-fetch-${each.key}-role"
  description        = "Lambda fetcher '${each.key}': write-only to its landing prefixes."
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

# CloudWatch Logs only.
resource "aws_iam_role_policy_attachment" "fetcher_logs" {
  for_each = var.fetchers

  role       = aws_iam_role.fetcher[each.key].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "fetcher" {
  for_each = var.fetchers

  statement {
    sid     = "WriteOwnLandingPrefixesOnly"
    effect  = "Allow"
    actions = ["s3:PutObject"]
    resources = [
      for prefix in each.value.landing_prefixes :
      "${module.landing_bucket.arn}/landing/${prefix}/*"
    ]
  }

  statement {
    sid       = "ReadDatabricksPushConfig"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = local.databricks_param_arns
  }

  dynamic "statement" {
    for_each = each.value.needs_massive_key ? [1] : []

    content {
      sid       = "ReadMassiveApiKey"
      effect    = "Allow"
      actions   = ["ssm:GetParameter"]
      resources = [aws_ssm_parameter.massive_api_key.arn]
    }
  }
}

resource "aws_iam_role_policy" "fetcher" {
  for_each = var.fetchers

  name   = "landing-write"
  role   = aws_iam_role.fetcher[each.key].id
  policy = data.aws_iam_policy_document.fetcher[each.key].json
}

# Long backfills continue themselves by invoking the same function again
# (see fetchers/common.py run_dates). Each role may invoke only its own function(s).
# ARNs are built from names to avoid a dependency cycle with aws_lambda_function.
data "aws_iam_policy_document" "fetcher_self_invoke" {
  for_each = var.fetchers

  statement {
    sid     = "InvokeOwnFunctionForBackfillContinuation"
    effect  = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      for fn, cfg in local.functions :
      "arn:${local.partition}:lambda:${var.aws_region}:${local.account_id}:function:${var.name_prefix}-fetch-${fn}"
      if cfg.role_key == each.key
    ]
  }
}

resource "aws_iam_role_policy" "fetcher_self_invoke" {
  for_each = { for k, v in var.fetchers : k => v if length([for fn, cfg in local.functions : fn if cfg.role_key == k]) > 0 }

  name   = "self-invoke-for-backfill"
  role   = aws_iam_role.fetcher[each.key].id
  policy = data.aws_iam_policy_document.fetcher_self_invoke[each.key].json
}
