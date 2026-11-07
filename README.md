# stocks-lakehouse

Multi-market stock lakehouse on AWS + Databricks, built on a medallion architecture.

- **Sources:** NSE bhavcopy (India), Massive grouped daily (US), Stooq (global indices)
- **Flow:** Lambda fetchers → S3 `landing/` → Lakeflow pipeline with Auto Loader → bronze → silver `daily_bars` → gold cross-market insights

This first cut sets up **identity, access control and storage** only. The fetchers and pipeline come next.

## Layout

```
infra/
  modules/secure_bucket/   Private, encrypted, versioned, HTTPS-only S3 bucket
  bootstrap/               Terraform state bucket (run once, local state)
  aws/                     Landing bucket, admin user + MFA role, fetcher roles, API key parameter
  databricks/              Groups, pipeline service principal, S3 link, catalog, schemas, grants
fetchers/                  (next) Lambda handlers
pipeline/                  (next) bronze / silver / gold SQL
```

## Who can do what

| Identity | Where | Can | Cannot |
|---|---|---|---|
| `stocks-admin` user | AWS | Manage its own password/MFA/keys, assume `stocks-admin-role` **with MFA** | Anything else on its own |
| `stocks-admin-role` | AWS | Manage the project (PowerUser + IAM) | Be assumed without MFA |
| `stocks-fetch-nse-role` | AWS Lambda | `PutObject` to `landing/nse/*` | Read, delete, list, other prefixes |
| `stocks-fetch-us-role` | AWS Lambda | `PutObject` to `landing/massive/*`, `landing/stooq/*`; read the Massive key | Same as above |
| `stocks-uc-landing-read-role` | AWS ← Databricks | Read `landing/*` | Write anything |
| `stocks-admins` group | Databricks | Workspace admin, all privileges on catalog and landing | — |
| `stocks-pipeline-sp` | Databricks | Read landing files, write bronze/silver/gold | Log in |
| `stocks-analysts` group | Databricks | Read gold | See bronze/silver, read raw files |

---

## 0. Prerequisites

- Terraform **1.10+** (`terraform version`)
- AWS CLI **v2** (`aws --version`)
- A Databricks account on AWS with a workspace (serverless workspace is fine)
- **Temporary** admin credentials for the very first AWS apply. Create a throwaway IAM user with `AdministratorAccess` in the console. You'll delete it in step 2. Don't use root access keys.

```bash
aws configure --profile bootstrap        # paste the throwaway user's keys, region ap-south-1
export AWS_PROFILE=bootstrap
```

## 1. State bucket (once)

```bash
cd infra/bootstrap
terraform init
terraform apply
terraform output -raw state_bucket_name
```

Keep `infra/bootstrap/terraform.tfstate` somewhere safe. It's tiny and only tracks the state bucket.

## 2. AWS stack

```bash
cd ../aws
cp backend.hcl.example backend.hcl            # put the state bucket name in it
cp terraform.tfvars.example terraform.tfvars  # optional, all values have defaults
terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

### Switch from the throwaway user to `stocks-admin`

1. Get the one-time password: `terraform output -raw admin_initial_password`
2. Sign in at `terraform output -raw admin_console_signin_url` as `stocks-admin` and set a new password.
3. **IAM → Users → stocks-admin → Security credentials → Assign MFA device.** Then sign out and back in with MFA.
4. Open `terraform output -raw admin_switch_role_url` to switch into `stocks-admin-role` in the console.
5. Same page: **Create access key** (CLI use) for `stocks-admin`.
6. Configure the CLI. In `~/.aws/config`:

   ```ini
   [profile stocks-user]
   region = ap-south-1

   [profile stocks]
   role_arn       = arn:aws:iam::<account-id>:role/stocks-admin-role
   source_profile = stocks-user
   mfa_serial     = arn:aws:iam::<account-id>:mfa/<your-mfa-device-name>
   region         = ap-south-1
   ```

   Then `aws configure --profile stocks-user` and paste the new access key.

7. Terraform can't prompt for an MFA code, so have the AWS CLI do it and hand Terraform the temporary credentials:

   ```bash
   unset AWS_PROFILE
   eval "$(aws configure export-credentials --profile stocks --format env)"   # asks for MFA code
   aws sts get-caller-identity                                                # should show stocks-admin-role
   ```

   Run that `eval` line at the start of each working session.

8. **Delete the throwaway bootstrap user** in the IAM console.

### Set the Massive API key (kept out of Terraform state)

```bash
aws ssm put-parameter --name /stocks/massive-api-key \
  --type SecureString --overwrite --value 'YOUR_MASSIVE_KEY'
```

## 3. Databricks bootstrap (manual, once)

Terraform needs an identity in Databricks before it can create anything. In the **account console** (`https://accounts.cloud.databricks.com`):

1. **User management → Service principals → Add** `terraform-sp`.
2. Open it → **Roles** → enable **Account admin**.
3. **Credentials & secrets → Generate secret** (OAuth). Copy the client ID and secret.
4. **Workspaces → your workspace → Permissions → Add** `terraform-sp` as **Admin**.
5. In the workspace **SQL editor**, as yourself, let it create Unity Catalog objects:

   ```sql
   GRANT CREATE CATALOG, CREATE STORAGE CREDENTIAL, CREATE EXTERNAL LOCATION
     ON METASTORE TO `<terraform-sp application id>`;
   ```

Then export the credentials in the shell you run Terraform from. Don't put them in tfvars:

```bash
export DATABRICKS_CLIENT_ID=<terraform-sp application id>
export DATABRICKS_CLIENT_SECRET=<oauth secret>
```

## 4. Databricks stack

This stack also creates one AWS role (the Unity Catalog read role), so it needs both sets of credentials:

```bash
cd ../databricks
eval "$(aws configure export-credentials --profile stocks --format env)"
cp backend.hcl.example backend.hcl
cp terraform.tfvars.example terraform.tfvars   # fill in account ID, workspace URL/ID, bucket, your email
terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

If the external location fails validation on the first apply, IAM was still propagating. Run `terraform apply` again.

## 5. Check it works

**AWS: fetcher roles really are write-only and isolated**

```bash
aws iam simulate-principal-policy \
  --policy-source-arn $(terraform -chdir=infra/aws output -json fetcher_role_arns | jq -r .nse) \
  --action-names s3:PutObject s3:GetObject \
  --resource-arns arn:aws:s3:::<landing-bucket>/landing/nse/x arn:aws:s3:::<landing-bucket>/landing/massive/x
```

Expect `allowed` only for `PutObject` on `landing/nse/x`.

**Databricks: S3 link works** (upload any small CSV to `s3://<landing-bucket>/landing/test/` first):

```sql
SELECT * FROM read_files('s3://<landing-bucket>/landing/test/', format => 'csv');
```

**Databricks: analysts are limited.** As an analyst, `SHOW SCHEMAS IN stocks` should show only `gold` as usable, and queries on `stocks.bronze.*` should fail.

## Notes and known gaps

- `stocks-admin-role` has **PowerUserAccess + IAMFullAccess** so Terraform can manage the project. Tighten with a permissions boundary once things settle.
- The **account password policy** is account-wide. Set `manage_account_password_policy = false` if you already have one.
- **Catalog creation:** if the workspace rejects a catalog without a storage location, create `stocks` once in the UI with default storage, then `terraform import databricks_catalog.stocks stocks`.
- `databricks_grants` is **authoritative**: grants added by hand in the UI on these objects get removed on the next apply. Add them in Terraform instead.
- Analysts need **Can use** on a SQL warehouse to query gold. Add that once the warehouse is managed here.
