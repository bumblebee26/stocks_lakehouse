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
  databricks/              Free Edition: catalog, schemas, landing volume, Lambda pusher identity
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
- **Long ranges continue themselves:** when a run nears Lambda's time limit it re-invokes itself with the remaining dates, so one call can cover years. Start long backfills with `--invocation-type Event` so your terminal doesn't wait.
- Massive waits 13 seconds between days (free-plan rate limit). Run only **one** Massive backfill at a time.
- Available history: NSE UDiFF from **8 Jul 2024**; Massive free plan keeps a rolling **2 years**.
- Holidays come back as `no_file` / `no_data`, not errors.

Unit tests (no AWS needed): `python -m unittest discover -s tests -v`

### Push to Databricks

After each S3 write, the fetchers also upload the same file to `/Volumes/stocks/bronze/landing/<source>/date=YYYY-MM-DD/`
using the Files API and the `stocks-lambda-pusher` token from SSM (`databricks_push_enabled = true` by default).

`stocks-sync-volume` copies files already in S3 into the volume (skipping ones already there). Use it once after the
backfill, and any time to repair gaps:

```
aws lambda invoke --function-name stocks-sync-volume --region ap-southeast-2 --invocation-type Event --cli-binary-format raw-in-base64-out --payload "{}" sync.json
```

## 4. Databricks (Free Edition)

Free Edition has no account-level APIs, so `infra/databricks` manages only workspace objects:
the `stocks` catalog, `bronze`/`silver`/`gold` schemas, the `stocks.bronze.landing` volume, a
`stocks-lambda-pusher` service principal that may only write to that volume, and its token
(stored in AWS SSM as `/stocks/databricks-pusher-token`).

One manual step: create a **bootstrap personal access token** for Terraform in the workspace
(your user icon -> **Settings** -> **Developer** -> **Access tokens** -> **Generate new token**).

```
cd infra\databricks
copy backend.hcl.example backend.hcl
copy terraform.tfvars.example terraform.tfvars
```

Fill both in (state bucket, workspace URL without `?o=`), then:

```
set DATABRICKS_TOKEN=dapi...your bootstrap token...
for /f "delims=" %i in ('aws configure export-credentials --profile stocks --format windows-cmd') do %i
terraform init -backend-config=backend.hcl
terraform apply
```

If you already created the catalog/schemas/volume with SQL, import them first (commands at the top of `catalog.tf`).

## 5. Lakeflow pipeline (Python, SQL equivalents in comments)

`pipeline/` holds one file per layer. Each table's Python definition has the equivalent
Lakeflow SQL in a comment above it, for reference.

| Layer | Table | What it is |
|---|---|---|
| Bronze | `stocks.bronze.nse_bhavcopy_raw` | NSE UDiFF CSV rows as delivered (strings) + lineage columns |
| Bronze | `stocks.bronze.massive_grouped_raw` | One row per Massive JSON file (`results` = all US tickers) |
| Silver | `stocks.silver.nse_eq_daily` | NSE securities, typed, de-duplicated, quality-checked |
| Silver | `stocks.silver.us_daily` | US stocks exploded from `results`, OTC excluded, quality-checked |
| Silver | `stocks.silver.daily_bars` | **Both markets in one schema**: market, symbol, trade_date, OHLC, prev_close, volume, currency |
| Silver | `stocks.silver.trading_days` | Trading calendar per market, derived from the data |
| Gold | `stocks.gold.unusual_volume` | Volume >= 3x the prior 20-day average |
| Gold | `stocks.gold.market_breadth` | Advances/declines, median return, % above 20-day average |
| Gold | `stocks.gold.nifty_after_us_moves` | Each Indian session (NIFTYBEES) next to the preceding US session (SPY) |
| Gold | `stocks.gold.nifty_after_us_moves_summary` | How India opens/closes after big US up/down days |
| Gold | `stocks.gold.adr_premium` | INFY, WIT, HDB, IBN, RDY ADRs vs NSE shares (no FX feed needed) |

NSE files are zips in S3; the Lambda unzips them on the way into the volume because Spark can't read zip.

`infra/databricks/pipeline.tf` uploads the code, creates the serverless pipeline `stocks-medallion`, and a
`stocks-refresh` job that runs it **when new files land** in the volume (`job_trigger = "file_arrival"`), or at
06:00 IST Tue–Sat (`job_trigger = "schedule"`). Failures email you. Thresholds and proxy symbols are variables.

## Notes

- The free plan has a fixed number of days and credits; check AWS Settings → Projects. When it ends you'll need to upgrade or move the data elsewhere.
- Upgrading and activating **advanced features** removes the free-plan restrictions, but activation can't be undone and removes the spend limit.
