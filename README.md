# stocks-lakehouse

Multi-market stock lakehouse on AWS + Databricks, built on a medallion architecture.

- **Sources:** NSE bhavcopy (India), Massive grouped daily (US), Stooq (global indices)
- **Flow:** EventBridge → Lambda fetchers → S3 raw archive **and** a Databricks Unity Catalog Volume → Auto Loader → bronze → silver `daily_bars` → gold cross-market insights

## Runs on the AWS free plan

This AWS account uses AWS's **projects** experience on the free plan. AWS applies its own guardrails (service control and resource control policies), which you can't edit. The design works within them:

| Free-plan rule | How this repo handles it |
|---|---|
| Only `us-east-1`, `us-west-2` and the project region | Everything runs in **`ap-southeast-2` (Sydney)**, the project region |
| IAM groups, login profiles and aliases are denied | No IAM users. You sign in with `aws login` (temporary credentials, no access keys) |
| Outside principals can't access project resources | Databricks never reads S3. The Lambda **pushes** each file into a Databricks Volume |
| Only allow-listed services | Uses only S3, Lambda, EventBridge, SSM and IAM roles |

Databricks runs on **Databricks Free Edition**, separately from this AWS account.

## Layout

```
infra/
  modules/secure_bucket/   Private, encrypted, versioned, HTTPS-only S3 bucket
  bootstrap/               Terraform state bucket (run once, local state)
  aws/                     Landing bucket, fetcher roles, API key parameter
  databricks/              PAUSED: being redesigned for Free Edition. Do not apply yet.
fetchers/                  (next) Lambda handlers
pipeline/                  (next) bronze / silver / gold SQL
```

## Who can do what (AWS)

| Identity | Can | Cannot |
|---|---|---|
| You, through `AccountFullAccessRole` (`aws login`) | Manage the project | — |
| `stocks-fetch-nse-role` (Lambda) | `PutObject` to `landing/nse/*` | Read, delete, list, other prefixes |
| `stocks-fetch-us-role` (Lambda) | `PutObject` to `landing/massive/*` and `landing/stooq/*`; read the Massive key | Same as above |

---

## 0. Prerequisites (Windows Command Prompt)

- Terraform **1.10+** (`terraform version`)
- AWS CLI **v2.32+** (`aws --version`)

Sign in once per working session. A browser tab opens for approval:

```
aws login --profile stocks --region ap-southeast-2
set AWS_PROFILE=stocks
aws sts get-caller-identity
```

The account should be **538498973473**.

Terraform may not understand `aws login` profiles directly, so hand it the temporary credentials as environment variables (Command Prompt, one line):

```
for /f "delims=" %i in ('aws configure export-credentials --profile stocks --format windows-cmd') do %i
```

In PowerShell, use `--format powershell` and pipe the output to `Invoke-Expression` instead.

The credentials expire after a few hours. If Terraform says `ExpiredToken`, run `aws login` and the `for /f` line again.

## 1. State bucket (once)

```
cd infra\bootstrap
terraform init
terraform apply
terraform output -raw state_bucket_name
```

Keep `infra\bootstrap\terraform.tfstate` somewhere safe. It's tiny and only tracks the state bucket.

## 2. AWS stack

```
cd ..\aws
copy backend.hcl.example backend.hcl
```

Put the state bucket name in `backend.hcl`, then:

```
terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

### Set the Massive API key (kept out of Terraform state)

```
aws ssm put-parameter --name /stocks/massive-api-key --type SecureString --overwrite --value YOUR_MASSIVE_KEY --region ap-southeast-2
```

## 3. Fetchers (Lambda + schedules)

Created by the AWS stack from the code in `fetchers/`:

| Function | Source | Schedule (IST) | Lands at |
|---|---|---|---|
| `stocks-fetch-nse` | NSE equities bhavcopy (UDiFF zip) | 19:30 Mon–Fri | `landing/nse/date=YYYY-MM-DD/` |
| `stocks-fetch-massive` | Massive grouped daily (all US stocks) | 04:30 Tue–Sat | `landing/massive/date=YYYY-MM-DD/` |

Schedules are created **disabled** (`schedules_enabled = false`) until a manual run works.

Test or backfill by invoking manually (Command Prompt):

```
aws lambda invoke --function-name stocks-fetch-nse --cli-binary-format raw-in-base64-out --payload "{\"date\":\"2026-11-06\"}" out.json && type out.json
aws lambda invoke --function-name stocks-fetch-massive --cli-binary-format raw-in-base64-out --payload "{\"start\":\"2026-10-01\",\"end\":\"2026-10-31\"}" out.json && type out.json
```

- `{}` fetches the latest day, `{"date": ...}` one day, `{"start": ..., "end": ...}` every weekday in a range.
- Massive backfills wait 13 seconds between days (free-plan rate limit), so keep ranges to about 60 days per call.
- Holidays come back as `no_file` / `no_data`, not errors.

Unit tests (no AWS needed): `python -m unittest discover -s tests -v`

## 4. Databricks

Paused. The `infra/databricks` stack still describes the old design, where Databricks read S3 directly. The free plan blocks that, so it's being rewritten for Databricks Free Edition with a Volume. Don't apply it.

## Notes

- The free plan has a fixed number of days and credits; check AWS Settings → Projects. When it ends you'll need to upgrade or move the data elsewhere.
- Upgrading and activating **advanced features** removes the free-plan restrictions, but activation can't be undone and removes the spend limit.
