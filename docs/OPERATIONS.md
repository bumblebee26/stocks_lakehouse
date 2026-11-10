# Operations

Day-to-day running of the lakehouse once [SETUP.md](SETUP.md) is done.

## What runs on its own

| Time (IST) | Job | Writes |
|---|---|---|
| 04:30 and 07:30 Tue–Sat | `stocks-fetch-massive` (07:30 = retry; each run fetches any of the last 3 weekdays not yet in the volume) | `landing/massive/date=<US date>/grouped_daily_YYYYMMDD.json` |
| 06:00 Tue–Sat | `stocks-fetch-fred` | `landing/fred/date=<today>/fred_<start>_<end>.json` (last 21 days) |
| 5 min after files stop arriving | Databricks job `stocks-refresh` | Refreshes all bronze/silver/gold tables, then `sql/dashboard_summary.sql` rebuilds `gold.dashboard_summary_facts` and `gold.dashboard_summary` (the dashboard's "In plain words" text) |
| every 30 min, 08:00–19:30 Mon–Fri | `stocks-fetch-news` | news volume `date=<today>/snap_HHMM.jsonl` + `latest.jsonl` (S3 copy under `landing/news/`); read by the pipeline at its next refresh |
| 08:45 Mon–Fri | `stocks-premarket-brief` | Telegram message |
| 15:45 Mon–Fri | paper-trader's `stocks-trader-guardian` (other repo) | `landing/trades/date=<today>/events.jsonl` and `landing/portfolio/date=<today>/portfolio.jsonl` (volume only; the S3 copies are in the trader's own bucket) |
| 19:30 Mon–Fri | `stocks-fetch-nse` | `landing/nse/date=<today>/bhavcopy_cm_YYYYMMDD.csv.zip` (+ `.csv` in the volume) |
| 07:15 Sat | paper-trader's *Company fundamentals* workflow (other repo, GitHub Actions) | `landing/fundamentals/date=<today>/fundamentals.jsonl` and `landing/index_members/date=<today>/nifty200.jsonl` (volume only) |

Market holidays are handled: the fetchers record `no_file` / `no_data` instead of failing.

## Making a change

```
1. Commit to dev and push            → CI runs tests, pipeline wiring, terraform fmt + validate
2. Open a pull request dev → main    → terraform plan for aws and databricks, posted as PR comments
3. Read the plan comments            → only the changes you expect?
4. Merge                             → terraform apply: aws, then databricks
```

| To change | Edit |
|---|---|
| Turn schedules or the brief on/off | `infra/aws/project.auto.tfvars` |
| Fetch logic | `fetchers/*.py` (+ tests in `tests/`) |
| Tables and insights | `pipeline/bronze.py`, `silver.py`, `gold.py` |
| Thresholds (big move %, volume spike, proxies) | variables in `infra/databricks/variables.tf` |
| Dashboard | `infra/databricks/dashboard.tf` |
| Dashboard "In plain words" summaries | `sql/dashboard_summary.sql` (checked with `EXPLAIN` in the plan job) |
| Long-term pool scoring and picks | `company_fundamentals` and `longterm_candidates` in `pipeline/gold.py` |
| Global instruments | `SERIES` in `fetchers/fred.py`, `COUNTRY_ETFS` in `pipeline/silver.py` |

Before pushing Terraform changes, run `terraform fmt -recursive infra` (CI fails on formatting). Run the tests
with `python -m unittest discover -s tests`.

**Local `terraform apply`** is only for first-time setup and emergencies. If you do it, run it from the right
folder (`infra\aws` or `infra\databricks`), and the next CI plan should then show no changes.

## Backfills and re-runs

All fetchers accept the same payloads:

| Payload | Meaning |
|---|---|
| `{}` | Normal scheduled run (latest day, or FRED's last 21 days) |
| `{"date": "2026-11-06"}` | One day (NSE, Massive) |
| `{"start": "2026-01-01", "end": "2026-03-31"}` | A range. NSE/Massive: every weekday. FRED: the whole range in one file. |

```
aws lambda invoke --function-name stocks-fetch-nse --region ap-southeast-2 --invocation-type Event --cli-binary-format raw-in-base64-out --payload "{\"start\":\"2026-01-01\",\"end\":\"2026-03-31\"}" out.json
```

- Re-fetching a day overwrites the same file; silver keeps the newest copy.
- Long ranges continue themselves in new invocations. Run **one Massive backfill at a time** (rate limit).
- Massive's free plan only serves the last 2 years.
- After a backfill the refresh job runs on its own. To rebuild everything from scratch instead, use
  **Full refresh all** on the pipeline.

### Copy S3 files into Databricks (repair)

If the Databricks push failed for some files (for example while a token was expired), copy what's missing:

```
aws lambda invoke --function-name stocks-sync-volume --region ap-southeast-2 --invocation-type Event --cli-binary-format raw-in-base64-out --payload "{}" sync.json
```

Options: `{"prefix": "landing/nse/"}` for one source, `{"overwrite": true}` to re-copy everything.

## Monitoring

| Check | How |
|---|---|
| Lambda logs | `aws logs tail /aws/lambda/stocks-fetch-nse --since 1h --region ap-southeast-2` (or `-massive`, `-fred`, `-news`, `stocks-premarket-brief`, `stocks-sync-volume`) |
| Schedules on? | `aws scheduler list-schedules --region ap-southeast-2 --query "Schedules[].[Name,State]" --output table` |
| Files arriving? | `aws s3 ls s3://stocks-lakehouse-landing-<account-id>/landing/nse/` |
| Pipeline runs | Databricks → **Jobs & Pipelines → stocks-refresh** (two tasks: `refresh_medallion`, then `dashboard_summary`; failures email you) |
| Data freshness | `SELECT market, max(trade_date) FROM stocks.silver.trading_days GROUP BY market;` |
| Global freshness | `SELECT instrument, obs_date FROM stocks.gold.global_snapshot ORDER BY obs_date;` |
| CI/CD | GitHub → **Actions** tab |

The pre-market brief warns ("⚠️ Latest US data is from ...") when its data is more than 4 days old.

## Rotating secrets

| Secret | Lives in | Expires | How to rotate |
|---|---|---|---|
| Databricks token for Terraform (laptop) | Your shell (`DATABRICKS_TOKEN`) | 90 days | Generate a new one (Settings → Developer → Access tokens); revoke the old |
| Databricks token for CI | GitHub secret `DATABRICKS_TOKEN` | 90 days | Generate new → update the secret → revoke the old |
| Databricks token for the Lambdas | SSM `/stocks/databricks-pusher-token` (created by Terraform) | 180 days | In `infra\databricks` (local apply): `terraform apply -replace="databricks_token.pusher[0]"`. Terraform writes the new token to SSM; the Lambdas pick it up within 5 minutes |
| CI AWS keys | GitHub secrets / `production` environment | Never (rotate yearly) | `aws iam create-access-key --user-name ...` → update secrets → `aws iam delete-access-key ...` |
| Massive, FRED, Telegram | SSM | Never | `aws ssm put-parameter ... --overwrite` |

If the CI Databricks token expires, plans fail with *Invalid access token*. If the Lambda token expires,
files still land in S3; after rotating it, run the sync Lambda to fill the volume.

## AWS free plan

The free plan has a fixed number of days and credits (AWS Settings → Projects). Before it ends, either upgrade
to a paid plan or export the data. Upgrading and activating **advanced features** removes the free-plan
guardrails, but it can't be undone and it removes the spend limit.

Expected cost on a paid plan is a few cents a month: a handful of short Lambda runs a day, tens of MB in S3,
and free-tier SSM parameters.

## Security checks

`.github/workflows/security.yml` runs on every pull request, on main and every Monday 03:47 IST; it uses no
secrets. Findings are in the repository's **Security → Code scanning** tab and in each job's log.

| Check | Looks for | Fails the PR on |
|---|---|---|
| CodeQL (Python, workflows) | Injection, request forgery, unsafe deserialisation, secrets in logs | High severity (`.github/scripts/sarif_summary.py`) |
| zizmor | Attacks on GitHub Actions: script injection, over-broad tokens, risky triggers, unpinned actions | High severity |
| Checkov | Risky Terraform settings: public buckets, wide IAM, missing encryption | Report only; reviewed exceptions with reasons in `.checkov.yaml` |
| pip-audit (paper-trader) | Known vulnerabilities in `requirements.txt` | Any |
| Dependabot | Weekly grouped update PRs for actions (and pip) | — |

Every action is pinned to a commit hash with its version in a comment, so a hijacked tag can't swap in
other code; Dependabot keeps the hashes current. Turn on in **Settings → Code security**: secret scanning
with push protection, Dependabot alerts and security updates.

