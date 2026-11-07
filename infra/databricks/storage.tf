# Read-only link from Unity Catalog to s3://<landing bucket>/landing/
#
# Order matters and avoids a dependency cycle:
#   1. Storage credential is created pointing at the role ARN (built from its name).
#   2. Databricks returns an external ID for that credential.
#   3. The IAM role is created trusting Databricks with that external ID.
#   4. The external location is created and validated against the role.

locals {
  uc_role_name   = "${var.name_prefix}-uc-landing-read-role"
  uc_role_arn    = "arn:${local.partition}:iam::${local.account_id}:role/${local.uc_role_name}"
  landing_bucket = "arn:${local.partition}:s3:::${var.landing_bucket_name}"
}

resource "databricks_storage_credential" "landing" {
  provider = databricks.workspace

  name      = "${var.name_prefix}-landing-cred"
  comment   = "Read-only access to the landing bucket. Managed by Terraform."
  read_only = true

  aws_iam_role {
    role_arn = local.uc_role_arn
  }
}

# Trust policy for the Unity Catalog master role plus the self-assume Databricks requires.
data "databricks_aws_unity_catalog_assume_role_policy" "landing" {
  provider = databricks.workspace

  aws_account_id = local.account_id
  role_name      = local.uc_role_name
  external_id    = databricks_storage_credential.landing.aws_iam_role[0].external_id
}

data "aws_iam_policy_document" "uc_landing_read" {
  statement {
    sid    = "ListLandingBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [local.landing_bucket]
  }

  statement {
    sid       = "ReadLandingObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${local.landing_bucket}/landing/*"]
  }

  statement {
    sid       = "SelfAssumeRequiredByUnityCatalog"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [local.uc_role_arn]
  }
}

resource "aws_iam_role" "uc_landing" {
  name               = local.uc_role_name
  description        = "Unity Catalog read-only access to the stocks landing bucket."
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.landing.json
}

resource "aws_iam_role_policy" "uc_landing_read" {
  name   = "landing-read"
  role   = aws_iam_role.uc_landing.id
  policy = data.aws_iam_policy_document.uc_landing_read.json
}

resource "databricks_external_location" "landing" {
  provider = databricks.workspace

  name            = "${var.name_prefix}-landing"
  url             = "s3://${var.landing_bucket_name}/landing/"
  credential_name = databricks_storage_credential.landing.id
  read_only       = true
  comment         = "Raw files from the fetchers. Managed by Terraform."

  # IAM can take a few seconds to propagate. If the first apply fails
  # validation here, simply run terraform apply again.
  depends_on = [
    aws_iam_role.uc_landing,
    aws_iam_role_policy.uc_landing_read,
  ]
}
