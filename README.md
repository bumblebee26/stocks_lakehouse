# stocks-lakehouse

A multi-market stock lakehouse on **AWS + Databricks**, built on a **medallion architecture**
(bronze → silver → gold), fully deployed with **Terraform** and **GitHub Actions**.

It collects Indian, US and global market data every day, turns it into cross-market insights, shows them on
a dashboard, and sends a **pre-market brief to Telegram** before the Indian market opens.

![Base diagram: sources → AWS ingest → Databricks lakehouse → dashboard and Telegram](docs/diagrams/01_overview.png)

**Diagrams with explanations:** [docs/ARCHITECTURE_DIAGRAMS.md](docs/ARCHITECTURE_DIAGRAMS.md)
covers the base view above, the full [architecture](docs/ARCHITECTURE_DIAGRAMS.md#2-architecture),
[data lineage](docs/ARCHITECTURE_DIAGRAMS.md#3-data-lineage) (every pipeline table) and the
[CI/CD flow](docs/ARCHITECTURE_DIAGRAMS.md#4-cicd-flow), and the
[platform shared with paper-trader](docs/ARCHITECTURE_DIAGRAMS.md#5-platform-with-paper-trader).

## What you get

| Output | Where | When |
|---|---|---|
| Daily raw data (NSE, US, global) | S3 `landing/` + Databricks volume | Every trading day |
| Bronze, silver and gold tables | Unity Catalog `stocks.*` | Refreshed when new files land |
| Dashboard "Stocks lakehouse: India x US" (pages Overview, Portfolio, Intraday trading, Company quality, Details; each with an "In plain words" summary written by Databricks AI) | Databricks workspace | Always current; summaries rebuilt after every refresh |
| Pre-market brief | Your Telegram | 08:45 IST, Mon–Fri |
| Paper-trading analysis ([paper-trader](https://github.com/bumblebee26/paper-trader)) | `stocks.gold.trad*` tables + dashboard page "Intraday trading" (KPI tiles: net P&L, win rate, profit factor, expectancy, win/loss ratio; equity curve, daily P&L, drawdown, by setup and hour) | After the 15:45 IST export |
| Dummy portfolio and holdings | `stocks.gold.portfolio_daily`, `stocks.gold.holdings_latest` + dashboard page "Portfolio" (value, return vs NIFTYBEES, fall from peak, allocation by pool and sector, holdings) | After the 15:45 IST export |
| Your own trades from Telegram ([paper-trader](https://github.com/bumblebee26/paper-trader)) | `stocks.gold.custom_trades` (intraday and pool trades you placed, marked custom) | After each export |
| News radar and swing shadow ([paper-trader](https://github.com/bumblebee26/paper-trader)) | `stocks.gold.intraday_universe` (Nifty 200 names the trader may trade, flagged on news/moves), `stocks.gold.swing_signals` (news breakouts), `stocks.gold.shadow_pools_daily` (shadow pools vs NIFTYBEES) | After each NSE day / daily |
| Company quality and long-term picks | `stocks.gold.company_fundamentals`, `stocks.gold.longterm_candidates` (this week's 10 picks from the Nifty 200, max 2 per industry, with target weights and reasons) + dashboard page "Company quality" | Saturdays |

Gold insights include: how India opens after big US moves, which global markets line up with India's open,
market breadth, unusual volume, Indian ADR premium vs USD/INR, and a global markets snapshot.

## Daily timeline (IST)

| Time | What happens |
|---|---|
| 04:30 Tue–Sat (07:30 retry) | Massive: previous US session (all US stocks), catching up missed days |
| 06:00 Tue–Sat | FRED: global indices, FX, VIX, oil, rates (rolling 21-day window) |
| after each file | Databricks job refreshes the pipeline (file-arrival trigger), then rebuilds the dashboard summaries |
| every minute 09:00–15:59, every 30 min otherwise (08:00–19:30) Mon–Fri | NSE company news (announcements, results, board meetings, corporate actions) |
| 08:45 Mon–Fri | Pre-market brief to Telegram |
| 15:45 Mon–Fri | paper-trader pushes the day's trade journal to `landing/trades/` and a portfolio snapshot to `landing/portfolio/` |
| 19:30 Mon–Fri | NSE: today's equities bhavcopy |
| 23:30 Mon–Fri | paper-trader re-sends the last 4 days' journals, so evening 👍/👎 ratings reach `gold.track_record` |
| 07:15 Sat | paper-trader pushes company financials to `landing/fundamentals/` and NSE's Nifty 200 list to `landing/index_members/` |

## Repository layout

```
fetchers/          Lambda code: nse, massive, fred, news, sync, premarket (+ shared helpers). Python stdlib + boto3 only.
pipeline/          Lakeflow pipeline in Python (bronze.py, silver.py, gold.py), SQL equivalents in comments
sql/               dashboard_summary.sql: "In plain words" page summaries, run by the refresh job after the pipeline
infra/
  bootstrap/       S3 bucket for Terraform state (run once, locally)
  modules/         secure_bucket: private, encrypted, versioned, HTTPS-only S3
  aws/             Landing bucket, Lambdas, schedules, IAM, SSM parameters, CI users
  databricks/      Catalog, schemas, volume, token, pipeline, job, dashboard
tests/             Unit tests (no cloud access needed)
scripts/           check_pipeline.py (pipeline wiring check, used by CI), check_sql.py (dashboard and summary SQL
                   checked with EXPLAIN in the plan job), render_diagrams .sh/.cmd (re-render docs/diagrams)
.github/           CI (tests, fmt, validate), security checks, CD (plan on PR, apply on merge)
docs/              Full documentation (below); docs/diagrams/ holds the diagram sources and images
```

## Documentation

| Doc | Read it when |
|---|---|
| [docs/PLATFORM.md](docs/PLATFORM.md) | You want the whole platform with paper-trader: diagram, daily timeline, contracts, security, costs |
| [docs/SYSTEM_PLAN.md](docs/SYSTEM_PLAN.md) | You want the complete system plan: pools, phases, rules, backtests, risks, decisions |
| [docs/ARCHITECTURE_DIAGRAMS.md](docs/ARCHITECTURE_DIAGRAMS.md) | You want the picture: base, architecture, lineage, CI/CD and platform diagrams, each explained |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | You want to understand how it fits together and why it's built this way |
| [docs/SETUP.md](docs/SETUP.md) | You're building it from scratch: every step, in order, with Windows commands |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Day to day: making changes, backfills, monitoring, rotating secrets |
| [docs/DATA_MODEL.md](docs/DATA_MODEL.md) | You're querying the tables: every table, its grain and columns |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Something failed: every error we hit while building this, and the fix |

## How changes ship

```
edit on dev  ──push──▶  CI: tests, pipeline wiring, terraform fmt + validate
              ──PR to main──▶  terraform plan (aws, databricks) posted as PR comments; dashboard SQL checked;
                               security checks (CodeQL, zizmor, Checkov)
              ──merge──▶  terraform apply (aws, then databricks)
```

`main` only changes through pull requests. Nobody runs `terraform apply` locally except for first-time setup
or emergencies.

## Data sources and limits

| Source | Coverage | History available | Cost |
|---|---|---|---|
| NSE UDiFF bhavcopy | All NSE cash-market securities | From 8 Jul 2024 (format change) | Free |
| Massive grouped daily | All US stocks and ETFs | Rolling 2 years on the free Basic plan | Free (API key) |
| NSE RSS feeds | Company announcements, financial results, board meetings, corporate actions | What the feeds carry at each 30-minute fetch | Free |
| Yahoo Finance fundamentals (fetched by paper-trader) | Annual and quarterly financials for NSE's Nifty 200 list (plus the trader's watchlist) | 4 fiscal years and recent quarters | Free |
| FRED | 14 series: S&P 500, Dow, Nasdaq, Nikkei 225, VIX, USD/INR, USD/JPY, EUR/USD, GBP/USD, USD/CNY, dollar index, Brent, WTI, US 10Y | Decades | Free (API key) |

Country ETFs (Japan, Germany, UK, Eurozone, Europe, China, Hong Kong, Korea, Taiwan, Brazil, EM, India)
come from the Massive data, giving daily equity coverage for markets FRED doesn't carry.

*Historical tendencies only. Not investment advice.*
