# Setup from scratch

Every step needed to build this project, in a working order. Commands are for **Windows Command Prompt**
(the shell this was built with). PowerShell differences are noted where they matter.

This is the order we *should* have followed. The order we actually followed hit a few walls first, and each
one is recorded in [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

**Time needed:** about 2 hours of hands-on work, plus about 2 hours of unattended backfill.

---

## Part A: Prerequisites

| Tool | Version | Check |
|---|---|---|
| Git | any | `git --version` |
| Terraform | 1.10 or newer | `terraform version` |
| AWS CLI | v2.32 or newer (`aws login` support) | `aws --version` |
| Python | 3.13 (only to run tests locally) | `python --version` |

Accounts you'll need, all free:

| Account | Used for | Sign up |
|---|---|---|
| AWS | Lambdas, S3, schedules | https://aws.amazon.com |
| Databricks Free Edition | Lakehouse, pipeline, dashboard | https://www.databricks.com/learn/free-edition |
| Massive (formerly Polygon.io), Basic plan | US stock data | https://massive.com |
| FRED | Global markets data | https://fredaccount.stlouisfed.org/apikeys |
| Telegram bot (from @BotFather) | Pre-market brief | Telegram app |
| GitHub | Code and CI/CD | https://github.com |

---

## Part B: AWS foundation

### B1. Understand which kind of AWS account you have

New AWS accounts may use the **projects** experience on the **free plan**. You can tell from the account
menu (top-right of the console): it shows "Free plan status", "Switch project", "Projects", "Team".

Open **https://settings.aws.com** → **Projects** → your project, and note:

- **Account ID**, for example `538498973473`.
- **Region**, for example `ap-southeast-2` (Sydney). This project runs **only** in that region, because
  the free plan allows only `us-east-1`, `us-west-2` and the project's own region.

If your region isn't `ap-southeast-2`, change `aws_region` in the Terraform stacks to match.

### B2. Sign in from the command line, with no access keys

```
aws login --profile stocks --region ap-southeast-2
set AWS_PROFILE=stocks
aws sts get-caller-identity
```

A browser tab opens; approve it. The last command should show your account ID and an
`assumed-role/AccountFullAccessRole/...` ARN.

If `aws login` says *"Profile 'default' is already configured with Access Key credentials"*, that's why we
use `--profile stocks`: it keeps the new sign-in separate from any old keys.

Check that the services this project uses are allowed. Each should succeed, even if the output is empty:

```
aws s3 ls --region ap-southeast-2
aws lambda list-functions --region ap-southeast-2
aws ssm describe-parameters --region ap-southeast-2
```

`set AWS_PROFILE=stocks` only lasts while the window is open. Repeat it in every new Command Prompt
(PowerShell: `$env:AWS_PROFILE="stocks"`).

### B3. Give Terraform your sign-in

Terraform may not understand `aws login` profiles, so export the temporary credentials into the window:

```
for /f "delims=" %i in ('aws configure export-credentials --profile stocks --format windows-cmd') do %i
```

They expire after a few hours. When anything says *expired*, clear them and sign in again:

```
set AWS_ACCESS_KEY_ID=
set AWS_SECRET_ACCESS_KEY=
set AWS_SESSION_TOKEN=
set AWS_CREDENTIAL_EXPIRATION=
aws login --profile stocks --region ap-southeast-2
```

(Typing `set NAME=` with nothing after `=` deletes the variable.)

### B4. Get the code

```
cd "C:\D Drive\Claude Code"
git clone https://github.com/bumblebee26/stocks_lakehouse.git
cd stocks_lakehouse
git checkout dev
```

### B5. Create the Terraform state bucket (once)

```
cd infra\bootstrap
terraform init
terraform apply
terraform output -raw state_bucket_name
```

Expect **7 resources added** and a name like `stocks-lakehouse-tfstate-<account-id>`. Keep
`infra\bootstrap\terraform.tfstate` safe; it only tracks this one bucket.

### B6. Create the AWS stack

```
cd ..\aws
copy backend.hcl.example backend.hcl
notepad backend.hcl
```

Set the bucket from B5 and save:

```
bucket       = "stocks-lakehouse-tfstate-<account-id>"
key          = "aws/terraform.tfstate"
region       = "ap-southeast-2"
encrypt      = true
use_lockfile = true
```

Then:

```
terraform init -backend-config=backend.hcl
terraform apply
```

This creates the landing bucket, all Lambdas and schedules, IAM roles, SSM placeholders and the two CI users.
The shared settings in `project.auto.tfvars` decide which schedules are on.

Committed `.terraform.lock.hcl` files pin provider versions. If you change providers, run
`terraform init -upgrade` and commit the updated lock file.

---

## Part C: Databricks

### C1. Create the workspace

Sign up for **Databricks Free Edition**. When the workspace opens, copy its URL up to `.com`, for example
`https://dbc-8cf15fdd-260d.cloud.databricks.com`, **without** the `?o=...` part.

### C2. Create a bootstrap token for Terraform

1. Click your **user icon** (top-right) → **Settings**.
2. **Developer** → **Access tokens** → **Manage** → **Generate new token**.
3. Comment `terraform`, lifetime **90 days**, **Generate**, and copy the token (`dapi...`). It's shown only once.

Set it in the window, **with no quotes and no spaces around `=`**:

```
set DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXX
echo %DATABRICKS_TOKEN:~0,8%
```

The echo should print `dapi` plus 4 more characters.

### C3. Create the catalog in the UI (once)

Free Edition stores data in **Default Storage**, and its API can't create catalogs there. So:

1. **Catalog** (left sidebar) → **+** → **Create a catalog**.
2. Name: `stocks`. Storage: leave **Default Storage**. **Create**.

### C4. Create the Databricks stack

```
cd ..\databricks
copy ..\aws\backend.hcl backend.hcl
notepad backend.hcl
```

Change only the key line to `key = "databricks/terraform.tfstate"` and save. Then:

```
copy terraform.tfvars.example terraform.tfvars
notepad terraform.tfvars
```

Set `databricks_workspace_url = "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"` (no `?o=`) and save.

```
for /f "delims=" %i in ('aws configure export-credentials --profile stocks --format windows-cmd') do %i
terraform init -backend-config=backend.hcl
terraform import databricks_catalog.stocks stocks
terraform apply
```

Check the plan: it must **not** destroy or replace `databricks_catalog.stocks`. This creates the
`bronze`/`silver`/`gold` schemas, the `landing` volume and its source folders, the Lambda's Databricks token
(written to SSM), the pipeline, the refresh job, the dashboard, and the warehouse-ID parameter.

The outputs include `pipeline_url` and `dashboard_url`.

---

## Part D: Data

### D1. Store the API keys

These go straight into SSM, so they never appear in Terraform state or git:

```
aws ssm put-parameter --name /stocks/massive-api-key --type SecureString --overwrite --value YOUR_MASSIVE_KEY --region ap-southeast-2
aws ssm put-parameter --name /stocks/fred-api-key --type SecureString --overwrite --value YOUR_FRED_KEY --region ap-southeast-2
```

To check a key without printing it, this shows the last 4 characters:

```
powershell -c "$k = aws ssm get-parameter --name /stocks/massive-api-key --with-decryption --region ap-southeast-2 --query Parameter.Value --output text; $k.Substring($k.Length-4)"
```

### D2. Test each fetcher for one day

Use a recent trading day (here Friday 6 Nov 2026):

```
aws lambda invoke --function-name stocks-fetch-nse --region ap-southeast-2 --cli-binary-format raw-in-base64-out --payload "{\"date\":\"2026-11-06\"}" nse_test.json
type nse_test.json

aws lambda invoke --function-name stocks-fetch-massive --region ap-southeast-2 --cli-binary-format raw-in-base64-out --payload "{\"date\":\"2026-11-06\"}" massive_test.json
type massive_test.json

aws lambda invoke --function-name stocks-fetch-fred --region ap-southeast-2 --cli-binary-format raw-in-base64-out --payload "{}" fred_test.json
type fred_test.json
```

Each should show `"status": "written"`, with no `FunctionError`. Then check both copies:

```
aws s3 ls s3://stocks-lakehouse-landing-<account-id>/landing/ --recursive --human-readable
```

In Databricks: **Catalog → stocks → bronze → Volumes → landing → Refresh**. You should see `nse/`,
`massive/` and `fred/` with `date=...` folders.

### D3. Backfill history

Long ranges run in the background (`--invocation-type Event`) and **continue themselves** when they near
Lambda's 15-minute limit, so one command covers years. You can close the terminal.

```
aws lambda invoke --function-name stocks-fetch-nse --region ap-southeast-2 --invocation-type Event --cli-binary-format raw-in-base64-out --payload "{\"start\":\"2024-07-08\",\"end\":\"2026-11-05\"}" nse_bf.json
aws lambda invoke --function-name stocks-fetch-massive --region ap-southeast-2 --invocation-type Event --cli-binary-format raw-in-base64-out --payload "{\"start\":\"2024-11-11\",\"end\":\"2026-11-05\"}" massive_bf.json
aws lambda invoke --function-name stocks-fetch-fred --region ap-southeast-2 --invocation-type Event --cli-binary-format raw-in-base64-out --payload "{\"start\":\"2024-07-01\",\"end\":\"2026-11-07\"}" fred_bf.json
```

| Source | Start date | Why | Duration |
|---|---|---|---|
| NSE | 2024-07-08 | First day of the UDiFF format | ~30–40 min |
| Massive | ~2 years ago | Free plan keeps a rolling 2 years | ~2 hours (13 s per day, rate limit) |
| FRED | any | One request per series covers the whole range | ~1 min |

Run **only one Massive backfill at a time**, or you'll hit its rate limit. Check progress:

```
aws s3 ls s3://stocks-lakehouse-landing-<account-id>/landing/nse/ --recursive | find /c ".zip"
aws s3 ls s3://stocks-lakehouse-landing-<account-id>/landing/massive/ --recursive | find /c ".json"
```

Expect about **575** NSE files and about **500** Massive files (holidays have no file). If a backfill stops
partway, list the folders, find the last `date=` that landed, and restart from the next day.

If any files reached S3 but not the volume, copy them across. This skips files already there:

```
aws lambda invoke --function-name stocks-sync-volume --region ap-southeast-2 --invocation-type Event --cli-binary-format raw-in-base64-out --payload "{}" sync.json
```

### D4. Run the pipeline the first time

In Databricks: **Jobs & Pipelines** → pipeline **stocks-medallion** → dropdown next to **Start** →
**Full refresh all**. Watch bronze, silver and gold turn green. After this, the **stocks-refresh** job runs
it automatically whenever new files land.

Check it in the **SQL Editor**:

```sql
SELECT market, count(*) AS bars, count(DISTINCT symbol) AS symbols,
       min(trade_date) AS first_day, max(trade_date) AS last_day
FROM stocks.silver.daily_bars
GROUP BY market;

SELECT * FROM stocks.gold.nifty_after_us_moves_summary ORDER BY us_move;
SELECT * FROM stocks.gold.global_snapshot ORDER BY asset_class, region;
```

### D5. Turn on the daily schedules

Edit `infra\aws\project.auto.tfvars`:

```
schedules_enabled = true  # NSE + Massive daily fetches
fred_enabled      = true  # FRED global markets
```

Apply. Once CI/CD is set up (Part F), make this change through a pull request instead.

---

## Part E: Dashboard and pre-market brief

### E1. Dashboard

Open the `dashboard_url` from C4's outputs, or **Dashboards → Stocks lakehouse: India x US**. It's defined in
`infra/databricks/dashboard.tf`; edits made in the UI are overwritten on the next apply.

### E2. Telegram settings

Bot token: **@BotFather → /mybots → your bot → API Token**. Chat ID: send your bot a message, then open
`https://api.telegram.org/bot<TOKEN>/getUpdates` and find `"chat":{"id":...}`.

```
aws ssm put-parameter --name /stocks/telegram-bot-token --type SecureString --overwrite --value YOUR_BOT_TOKEN --region ap-southeast-2
aws ssm put-parameter --name /stocks/telegram-chat-id --type SecureString --overwrite --value YOUR_CHAT_ID --region ap-southeast-2
```

Don't paste these commands, with the real values in them, into chats or tickets.

### E3. Dry run, real send, then schedule

```
aws lambda invoke --function-name stocks-premarket-brief --region ap-southeast-2 --cli-binary-format raw-in-base64-out --payload "{\"dry_run\":true}" brief.json
type brief.json
```

The first call can take 30–90 seconds while the SQL warehouse starts. If the message looks right, send it
for real with payload `"{}"`, check Telegram, then set `premarket_enabled = true` in
`infra\aws\project.auto.tfvars` and deploy it.

---

## Part F: GitHub and CI/CD

### F1. Branches

- `main`: starts as a single empty commit and only changes through pull requests.
- `dev`: where all work is committed.

Protect `main`: **Settings → Rules → Rulesets → New branch ruleset**.
- Enforcement: **Active**. Target: **Include default branch**.
- Tick **Restrict deletions**, **Block force pushes**, and **Require a pull request before merging**.
- Set required approvals to **0** (you can't approve your own PR).
- Optionally tick **Require status checks to pass**.

On a free GitHub account, rulesets apply to public repositories only.

### F2. Access keys for the CI users

The AWS stack already created `stocks-gha-plan` (read-only) and `stocks-gha-apply`. The free plan blocks
GitHub OIDC, so CI uses access keys. Create them outside Terraform:

```
aws iam create-access-key --user-name stocks-gha-plan
aws iam create-access-key --user-name stocks-gha-apply
```

### F3. GitHub settings

**Settings → Secrets and variables → Actions:**

| Tab | Name | Value |
|---|---|---|
| Variables | `TF_STATE_BUCKET` | `stocks-lakehouse-tfstate-<account-id>` |
| Variables | `DATABRICKS_WORKSPACE_URL` | workspace URL without `?o=` |
| Secrets | `AWS_PLAN_ACCESS_KEY_ID`, `AWS_PLAN_SECRET_ACCESS_KEY` | `stocks-gha-plan` key |
| Secrets | `DATABRICKS_TOKEN` | a new Databricks token, comment `github-ci`, 90 days (made as in C2) |

**Settings → Environments → New environment** `production` → **Add environment secret**:
`AWS_APPLY_ACCESS_KEY_ID`, `AWS_APPLY_SECRET_ACCESS_KEY` from the `stocks-gha-apply` key.

### F4. First pull request

Open a PR from `dev` into `main`. Two comments appear with the `terraform plan` for `aws` and `databricks`.
If everything was applied locally, expect **No changes**, or only re-uploads of the same code. Merge it; the
**Terraform** workflow applies `aws`, then `databricks`.

### F5. Normalize line endings on your laptop (once)

`.gitattributes` makes every file use LF. Re-checkout so your Windows copy matches CI:

```
cd "C:\D Drive\Claude Code\stocks_lakehouse"
git pull
git rm --cached -r -q .
git reset --hard
```

This rewrites tracked files only. Git-ignored files (`terraform.tfvars`, `backend.hcl`, `.terraform/`) are
untouched.

From now on, follow [OPERATIONS.md](OPERATIONS.md): change on `dev` → PR → merge.

---

## Part G: Clean-up

- [ ] Remove old access keys from your laptop's `default` AWS profile (`%USERPROFILE%\.aws\credentials`) and
      deactivate them in IAM, if they're no longer used.
- [ ] Delete the old `.csv.zip` copies in `/Volumes/stocks/bronze/landing/nse/` (from before the Lambda
      unzipped files). The pipeline ignores them, so this is optional.
- [ ] Commit updated `.terraform.lock.hcl` files after any `terraform init -upgrade`.
- [ ] Add calendar reminders for token expiry (see [OPERATIONS.md](OPERATIONS.md#rotating-secrets)).
