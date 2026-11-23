# Pipelines and schedule

## Sources
| Source | What | When (IST) |
|---|---|---|
| NSE bhavcopy + delivery + index file | Every NSE stock's day, delivery %, every index incl. India VIX | 19:30 Mon–Fri |
| NSE company news | Announcements, results, board meetings, corporate actions | Every minute 09:00–15:59, every 30 min otherwise |
| Yahoo (pre-open snapshot) | 21 world markets: US indices and futures, Asia live, Europe, rupee, crude, gold, yields, VIX | 08:30 Mon–Fri |
| Massive | Every US stock's previous session | 11:00 Tue–Sat |
| FRED | VIX, FX, oil, rates, world indices | 06:00 Tue–Sat |
| paper-trader | Journal, portfolio snapshot, company financials | 15:45 and 23:30 Mon–Fri; Sat 07:15 |

Every file lands in S3 (kept forever) and in the Databricks landing volume.

## Pipelines (Databricks Free Edition: one runs at a time)
| Pipeline | Tables | What it builds |
|---|---|---|
| stocks-market | 37 | Prices, delivery, indices, global markets, breadth, unusual volume, the pre-open snapshot |
| stocks-research | 5 | Technical scanner, momentum ranks, news radar, swing signals, data freshness |
| stocks-trading | 23 | Trades, daily P&L, proposals and outcomes, track record, shadow tests, portfolio |

## Jobs
| Job | Starts | Runs |
|---|---|---|
| stocks-market-preopen | When the 08:30 snapshot lands (Mon–Fri) | market → dashboard summaries (Global markets current by ~08:45) |
| stocks-market-morning | 11:15 Tue–Sat | market → summaries (US history) |
| stocks-market-close | When the NSE file lands (~19:30 Mon–Fri) | market → research → summaries |
| stocks-market-weekend | Sat 08:00 | market → research → summaries (financials, Nifty 200 list) |
| stocks-trading | When the trader's journal lands (15:45, 23:30) | trading → summaries |
