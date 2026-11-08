# ---------------------------------------------------------------------------
# GitHub Actions identities for CI/CD (.github/workflows/terraform.yml).
#
# The AWS free plan denies IAM identity providers (iam:*Provider*), so GitHub
# OIDC federation isn't possible. Instead CI uses two IAM users whose access
# keys are created OUTSIDE Terraform (so they never land in state) and stored
# as GitHub secrets:
#
#   stocks-gha-plan   read-only; runs `terraform plan` on pull requests
#                     (plan runs with -lock=false, so it needs no write access)
#   stocks-gha-apply  manages the project; runs `terraform apply` on merge to main
#                     (keys live in the GitHub "production" environment only)
#
# Rotate the keys periodically: create a new key, update the GitHub secret,
# delete the old key.
# ---------------------------------------------------------------------------

resource "aws_iam_user" "gha_plan" {
  name          = "${var.name_prefix}-gha-plan"
  force_destroy = true # allow destroy even after access keys are added
}

resource "aws_iam_user_policy_attachment" "gha_plan_readonly" {
  user       = aws_iam_user.gha_plan.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_iam_user" "gha_apply" {
  name          = "${var.name_prefix}-gha-apply"
  force_destroy = true
}

# Everything except IAM/Organizations/Account (S3, Lambda, SSM, Scheduler, Logs...).
resource "aws_iam_user_policy_attachment" "gha_apply_power_user" {
  user       = aws_iam_user.gha_apply.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/PowerUserAccess"
}

# IAM limited to this project's roles and CI users (name prefix), plus read access.
data "aws_iam_policy_document" "gha_apply_iam" {
  statement {
    sid     = "ManageProjectRolesAndCiUsers"
    effect  = "Allow"
    actions = ["iam:*"]
    resources = [
      "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*",
      "arn:${local.partition}:iam::${local.account_id}:user/${var.name_prefix}-gha-*",
    ]
  }

  statement {
    sid       = "ReadIam"
    effect    = "Allow"
    actions   = ["iam:Get*", "iam:List*"]
    resources = ["*"]
  }
}

resource "aws_iam_user_policy" "gha_apply_iam" {
  name   = "manage-project-iam"
  user   = aws_iam_user.gha_apply.name
  policy = data.aws_iam_policy_document.gha_apply_iam.json
}

output "github_ci_users" {
  description = "Create one access key per user (see README CI/CD) and store them as GitHub secrets."
  value = {
    plan  = aws_iam_user.gha_plan.name
    apply = aws_iam_user.gha_apply.name
  }
}
