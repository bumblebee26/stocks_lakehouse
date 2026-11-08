# Troubleshooting

Every problem we hit while building this project, what caused it, and the fix. Search this page for the
error text you see.

## AWS account and sign-in

### Almost everything is denied, in every region (`explicit deny in a service control policy`)

**Cause:** The account uses AWS's **projects** experience on the **free plan**. AWS-managed SCPs and RCPs
apply, and you can't edit or detach them. Read-only calls fail outside the allowed regions, and some IAM
actions fail everywhere. It looks like a "quarantine" policy, but it isn't one, and changing SCPs from a
management account is the wrong fix.

**Fix:** Work within the rules (see [ARCHITECTURE.md](ARCHITECTURE.md#1-the-aws-account-is-on-the-free-plan-projects-experience)):
- Use the project region (AWS Settings → Projects → Region), here `ap-southeast-2`.
- Don't create IAM users with passwords, groups or identity providers.
- Sign in with `aws login`.

The alternative is to upgrade and activate advanced features, which can't be undone.

### `Profile 'default' is already configured with Access Key credentials` (from `aws login`)

**Cause:** `aws login` won't overwrite a profile that holds access keys.
**Fix:** Use a separate profile: `aws login --profile stocks --region ap-southeast-2`, then
`set AWS_PROFILE=stocks`.

### `Credentials were refreshed, but the refreshed credentials are still expired`

**Cause:** Old temporary credentials from `aws configure export-credentials` (the `for /f` line) are still set
as environment variables, and they take priority over the profile.
**Fix:** Clear them and sign in again:
```
set AWS_ACCESS_KEY_ID=
set AWS_SECRET_ACCESS_KEY=
set AWS_SESSION_TOKEN=
set AWS_CREDENTIAL_EXPIRATION=
aws login --profile stocks --region ap-southeast-2
```
`set NAME=` with nothing after `=` deletes the variable; there's no value to look up. Only use the `for /f`
line right before Terraform commands. Plain `aws` commands don't need it.

### `Method invocation failed ... does not contain a method named 'Substring'`

**Cause:** Piping CLI output into `powershell -c "$input.Substring(...)"`; `$input` is a stream, not a string.
**Fix:** Capture it into a variable first:
```
powershell -c "$k = aws ssm get-parameter --name /stocks/massive-api-key --with-decryption --region ap-southeast-2 --query Parameter.Value --output text; $k.Substring($k.Length-4)"
```

## Terraform

### Lambda still runs old code after `terraform apply`

**Symptom:** `aws lambda get-function-configuration` shows old environment variables, for example no
`PUSH_TO_DATABRICKS`.
**Cause:** `terraform apply` ran in the wrong folder (`infra\databricks` instead of `infra\aws`), or before
`git pull`.
**Fix:** `git pull`, `cd infra\aws`, `terraform apply`. Each stack only manages its own folder's resources.
With CI/CD this can't happen: merges apply both stacks.

### Plan shows code changes when nothing changed (`source_code_hash`, `md5`)

**Cause:** Windows checkouts used CRLF line endings and different file modes, so Lambda zips and pipeline
files hashed differently on a laptop than in CI (Linux). Local and CI applies would keep flipping them.
**Fix (in the repo):** `.gitattributes` forces LF, and `archive_file` uses `output_file_mode = "0644"`. On an
existing Windows clone, re-checkout once:
```
git rm --cached -r -q .
git reset --hard
```

### `terraform fmt` check fails in CI

**Cause:** Alignment. `terraform fmt` lines up `=` signs and trailing `#` comments across consecutive lines.
For example, changing `false` to `true` leaves an extra space that only existed to align with a 5-letter value.
**Fix:** `terraform fmt -recursive infra` locally, or apply the diff shown in the CI log, and push.

## Databricks

### `cannot create catalog: Metastore storage root URL does not exist. Default Storage is enabled ...`

**Cause:** Free Edition uses Default Storage, and the API can't create catalogs there.
**Fix:** Create the catalog once in the UI (**Catalog → + → Create a catalog**, name `stocks`, Default
Storage), then `terraform import databricks_catalog.stocks stocks`, then `terraform apply`. The resource
ignores `storage_root` and has `prevent_destroy`.

### `Invalid access token` (Terraform plan/apply)

**Cause:** The `DATABRICKS_TOKEN` variable holds the wrong value. Usually the placeholder text, quotes
(`set DATABRICKS_TOKEN="dapi..."` stores the quotes), a space after `=`, or the window was closed.
**Fix:** Check `echo %DATABRICKS_TOKEN:~0,8%`; it should print `dapi` plus 4 characters with no `"`. Set it
again without quotes, or generate a new token (Settings → Developer → Access tokens). In CI, update the
`DATABRICKS_TOKEN` secret.

### `HTTP 403 ... This API is disabled for users without the databricks-sql-access or workspace-access or workspace-consume entitlements`

**Cause:** The Lambda called the Files API with a **service principal's** token. On Free Edition this stayed
403 even after granting `workspace_access` and `databricks_sql_access`.
**Fix (in the repo):** `pusher_identity = "user"` (default). Terraform creates a dedicated, expiring personal
token and stores it in SSM.

### Pipeline: `Found duplicate column(s): results.element.t` / `UNABLE_TO_INFER_TABLE_SCHEMA` (massive_grouped_raw)

**Cause:** Massive bars have both `T` (ticker) and `t` (timestamp). Delta column names are case-insensitive, so
they count as duplicates.
**Fix (in the repo):** Bronze reads `results` as a string (`schemaHints = "results STRING"`). Silver renames
`"t":` to `"ts":` before `from_json`. After changing a bronze table's schema, run **Full refresh all**.

### NSE files are in the volume but the bronze table is empty

**Cause:** Spark can read gzip but not **zip**. The original NSE files are `.csv.zip`.
**Fix (in the repo):** The Lambda unzips NSE files when pushing to the volume (S3 keeps the zip). Old `.zip`
copies in the volume are ignored (`pathGlobFilter = *.csv`); re-sync to get `.csv` versions.

### A bronze table fails because its folder is empty

**Cause:** Auto Loader infers schemas from files, so an empty folder (FRED before its key is set) can't be
inferred.
**Fix (in the repo):** FRED bronze has an explicit schema, and Terraform writes a `_README.txt` into every
source folder so the path always exists.

### The refresh job doesn't start when files land

**Cause:** The trigger waits until no new files have arrived for 5 minutes (so a backfill causes one run, not
hundreds), and runs at most every 15 minutes. File-arrival triggers may also be unavailable on some plans.
**Fix:** Wait, or click **Run now**. If the trigger never fires, set `job_trigger = "schedule"` (06:00 IST
Tue–Sat) and deploy.

## Data sources

### Massive returns nothing for older dates

**Cause:** The free Basic plan keeps a rolling **2 years** of history.
**Fix:** Start backfills about 2 years back. Ten years needs a paid plan.

### NSE: no file before July 2024

**Cause:** NSE switched to the **UDiFF** bhavcopy on 8 Jul 2024. Older files use different URLs and columns.
**Fix:** History starts 8 Jul 2024. Older years would need a second fetcher and silver mapping.

### NSE returns an HTML page instead of a zip

**Cause:** NSE blocks requests that don't look like a browser.
**Fix (in the repo):** The fetcher sends browser headers and checks the response starts with `PK` (zip). If it
starts failing again, NSE has changed its protection; check the Lambda log.

### Stooq returns a page asking for an API key

**Cause:** Stooq has required an API key since March 2026 and added a bot challenge in June 2026; keys expire by
IP address or usage.
**Fix:** Not used. Global data comes from **FRED** plus US-listed country ETFs.

### A backfill stopped partway

**Cause:** Usually a failed push to Databricks (for example an expired token). The run fails on the first
error, so later dates aren't fetched.
**Fix:** List the source's folders in S3, find the last `date=` that landed, restart from the next day, then
run the sync Lambda to fill the volume. Run one Massive backfill at a time.

## Pre-market brief

### Message not received

Check in order:
1. Schedule on? `aws scheduler list-schedules --region ap-southeast-2 --query "Schedules[].[Name,State]" --output table`
2. Lambda log: `aws logs tail /aws/lambda/stocks-premarket-brief --since 2h --region ap-southeast-2`
3. Telegram settings: a wrong chat ID gives `HTTP 400 ... chat not found`. Make sure you've sent your bot at
   least one message.
4. A cold SQL warehouse can take a minute or two to start; the Lambda waits up to 4 minutes.

### `TABLE_OR_VIEW_NOT_FOUND` for a gold table (e.g. `stocks.gold.global_snapshot`)

**Cause:** New tables only exist after the pipeline has run with the new code. Merging deploys the code; the
tables appear on the next pipeline run (triggered by the next file, or **Run now**). It also happens if that run
failed.
**Fix:** Run the `stocks-refresh` job (or the pipeline) and check every table is green. The brief no longer fails
outright: a missing table only drops its section, and the message says "⚠️ Unavailable today: ...".

### Message says "⚠️ Latest US data is from ..."

The pipeline is behind. Check the Massive fetcher log and the `stocks-refresh` job runs.

## Paper trading tables

### The Paper trading page shows `TABLE_OR_VIEW_NOT_FOUND`
**Cause:** The pipeline hasn't run since the trading tables were deployed.
**Fix:** Run the `stocks-refresh` job once (Jobs & Pipelines → stocks-refresh → Run now). The tables stay
empty until the first 15:45 export from paper-trader.

### No trades for a day that had trading
**Cause:** The 15:45 export didn't run or failed (e.g. expired Databricks pusher token).
**Fix:** In paper-trader, check `aws logs tail /aws/lambda/stocks-trader-guardian --since 1d --region ap-southeast-2`,
then re-send the day: see paper-trader's OPERATIONS.md ("Re-send a day to the lakehouse"). Re-sends replace the
day's file; silver keeps one copy per `event_id`, so nothing is double-counted.

### `trading_daily.reconciliation` says `MISMATCH`
**Cause:** The P&L rebuilt from the day's events differs from the trader ledger's own figure by Rs 1 or more:
an event is missing from the export, or the ledger was edited or reset during the day.
**Fix:** Compare `reconciliation_diff_rs` with the trader's S3 copy (`exports/date=<day>/events.jsonl`) and
`state/ledger.json` history (the bucket is versioned). Re-export the day if the journal has more events than
the volume file.

## GitHub / CI

### `git push` rejected (`fetch first` / non-fast-forward)

**Cause:** Someone (you, from your laptop) pushed to `dev` in the meantime, for example lock files or a
`.gitignore` change.
**Fix:** `git pull --rebase origin dev`, then push.

### CI plan fails with `AccessDenied` or `InvalidClientTokenId`

**Cause:** Wrong or deleted CI access key in GitHub secrets.
**Fix:** Create a new key (`aws iam create-access-key --user-name stocks-gha-plan`, or `stocks-gha-apply` for
the `production` environment), update the secrets, and delete the old key.

### Why not GitHub OIDC?

The free plan denies `iam:*Provider*`, so the OIDC identity provider can't be created. After upgrading the AWS
account, switching CI to OIDC and deleting the two CI users is a worthwhile improvement.
