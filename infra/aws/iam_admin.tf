# Human access model:
#   stocks-admin (IAM user)  -- member of -->  stocks-admins (group)
#     The group can ONLY manage its own password/MFA/access keys and assume
#     stocks-admin-role. The user has no other permissions on its own.
#   stocks-admin-role
#     Holds the real permissions and can only be assumed with MFA.
# So a leaked password or access key alone cannot change anything.

# ---------------------------------------------------------------------------
# Account password policy (account-wide)
# ---------------------------------------------------------------------------
resource "aws_iam_account_password_policy" "this" {
  count = var.manage_account_password_policy ? 1 : 0

  minimum_password_length        = 14
  require_lowercase_characters   = true
  require_uppercase_characters   = true
  require_numbers                = true
  require_symbols                = true
  allow_users_to_change_password = true
  password_reuse_prevention      = 5
}

# ---------------------------------------------------------------------------
# Admin role (assume with MFA only)
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "admin_role_trust" {
  statement {
    sid     = "AllowAccountPrincipalsWithMfa"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    # Trusting the account delegates the decision to IAM policies; only the
    # stocks-admins group policy below grants sts:AssumeRole on this role.
    principals {
      type        = "AWS"
      identifiers = ["arn:${local.partition}:iam::${local.account_id}:root"]
    }

    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }
}

resource "aws_iam_role" "admin" {
  name                 = "${var.name_prefix}-admin-role"
  description          = "Project admin role for stocks-lakehouse. MFA required."
  assume_role_policy   = data.aws_iam_policy_document.admin_role_trust.json
  max_session_duration = var.admin_role_session_hours * 3600
}

# Broad for now so Terraform can manage everything in this project.
# Tighten later (e.g. a permissions boundary) once the stack settles.
resource "aws_iam_role_policy_attachment" "admin_power_user" {
  role       = aws_iam_role.admin.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/PowerUserAccess"
}

resource "aws_iam_role_policy_attachment" "admin_iam" {
  role       = aws_iam_role.admin.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/IAMFullAccess"
}

# ---------------------------------------------------------------------------
# Admin group: self-service credentials + assume the admin role
# ---------------------------------------------------------------------------
resource "aws_iam_group" "admins" {
  name = "${var.name_prefix}-admins"
}

data "aws_iam_policy_document" "admins_self_service" {
  # Note: $${aws:username} is escaped so Terraform leaves it for IAM to resolve.

  statement {
    sid    = "ViewAccountInfo"
    effect = "Allow"
    actions = [
      "iam:GetAccountPasswordPolicy",
      "iam:ListVirtualMFADevices",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "ManageOwnPassword"
    effect = "Allow"
    actions = [
      "iam:ChangePassword",
      "iam:GetUser",
      "iam:GetLoginProfile",
    ]
    resources = ["arn:${local.partition}:iam::${local.account_id}:user/$${aws:username}"]
  }

  statement {
    sid    = "ManageOwnAccessKeys"
    effect = "Allow"
    actions = [
      "iam:CreateAccessKey",
      "iam:DeleteAccessKey",
      "iam:ListAccessKeys",
      "iam:UpdateAccessKey",
      "iam:GetAccessKeyLastUsed",
    ]
    resources = ["arn:${local.partition}:iam::${local.account_id}:user/$${aws:username}"]
  }

  statement {
    sid    = "ManageOwnVirtualMfaDevice"
    effect = "Allow"
    actions = [
      "iam:CreateVirtualMFADevice",
      "iam:DeleteVirtualMFADevice",
    ]
    resources = ["arn:${local.partition}:iam::${local.account_id}:mfa/*"]
  }

  statement {
    sid    = "ManageOwnUserMfa"
    effect = "Allow"
    actions = [
      "iam:EnableMFADevice",
      "iam:DeactivateMFADevice",
      "iam:ListMFADevices",
      "iam:ResyncMFADevice",
    ]
    resources = ["arn:${local.partition}:iam::${local.account_id}:user/$${aws:username}"]
  }

  statement {
    sid       = "AssumeAdminRoleWithMfa"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [aws_iam_role.admin.arn]

    condition {
      test     = "Bool"
      variable = "aws:MultiFactorAuthPresent"
      values   = ["true"]
    }
  }
}

resource "aws_iam_group_policy" "admins_self_service" {
  name   = "self-service-and-assume-admin-role"
  group  = aws_iam_group.admins.name
  policy = data.aws_iam_policy_document.admins_self_service.json
}

# ---------------------------------------------------------------------------
# Admin user
# ---------------------------------------------------------------------------
resource "aws_iam_user" "admin" {
  name = var.admin_user_name

  # Lets terraform destroy remove the user even after you add MFA/access keys.
  force_destroy = true
}

resource "aws_iam_user_group_membership" "admin" {
  user   = aws_iam_user.admin.name
  groups = [aws_iam_group.admins.name]
}

# Generated password, shown once via `terraform output -raw admin_initial_password`.
# You must change it at first sign-in, so the value in state goes stale immediately.
resource "aws_iam_user_login_profile" "admin" {
  count = var.create_console_login ? 1 : 0

  user                    = aws_iam_user.admin.name
  password_length         = 20
  password_reset_required = true

  lifecycle {
    ignore_changes = [password_length, password_reset_required]
  }

  depends_on = [aws_iam_account_password_policy.this]
}

# Access keys are intentionally NOT created here, so they never land in state.
# Create them yourself in the console after you set up MFA (see README).
