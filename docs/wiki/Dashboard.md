# Dashboard: NSE Paper Trading Desk

Six pages, the same order on each: tiles → main chart → breakdowns → detail tables.

| Page | What it answers |
|---|---|
| Desk | The whole dummy portfolio vs NIFTYBEES; header chips for auto mode, India VIX and risk; today's decisions; shadow leaderboard |
| Intraday | Real vs every shadow on the same trades (exit rules, early exits, models, opening-range, retest, 09:15 buy, gap fade); auto-bar ladder and sizing; paper fills vs the real minute; the selector; the ~200-stock backtest |
| Swing | Real (NIFTYBEES) vs the swing shadow pools, with a Real / Shadow toggle: P&L in Rs, trades placed, the best shadow (which one, value, P&L) vs NIFTYBEES, and every buy and sell of the last 30 days |
| Long term | Real (cash) vs the quality and momentum shadow pools, with a Real / Shadow toggle: P&L in Rs, trades placed, the best shadow vs NIFTYBEES, and every buy and sell of the last 30 days |
| Your calls | Your 👍/👎 against real outcomes: what following or ignoring them cost or saved |
| Global markets | Pre-open read: US close, Asia live, rupee, crude, yields, VIX, breadth; which moves line up with India's open |

Money tables count from the 18 Nov 2026 reset; history tables keep everything.

## On your phone
The same numbers as a mobile app (`app/`): Desk (with the day's story and each pool's value and P&L), Intraday, Swing and Long term (each with a P&L chart that follows the Real / Shadows / Both switch), Markets (with five years of USD/INR) and Custom
tabs (plus Custom, from 24 Nov: Accenture's five-year chart), refreshed a few minutes after each Databricks job (about 09:10, 11:40, 16:15, 20:30, 23:55, Sat 08:45 IST).
The link is the app's address (a Lambda function URL for now; CloudFront once AWS enables it on the account) plus a secret path, stored in AWS Systems Manager → Parameter Store →
`/stocks/desk-app-url` (Show decrypted value). Open it on the phone and use "Add to Home Screen".
