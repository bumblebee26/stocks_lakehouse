# FAQ

**Why is a source "left out" of the brief?** The brief shows a figure only if it is from the last session; a
missing or late source is named, never replaced by an older day.

**Why are scanner levels null?** Levels are computed only for stocks with a trade to plan (strict picks and
"breakout-only" near-misses). "Wait" and "wait/avoid" rows show which filter they missed.

**When does today's data arrive?** NSE's end-of-day file at 19:30; tables refresh by about 20:00–20:15.
Global markets refresh by ~09:00 from the pre-open snapshot.

**Why only one pipeline at a time?** Databricks Free Edition allows one active pipeline; jobs are chained so they
never overlap.

**Can I run it myself?** Yes: see [Setup](../SETUP.md). You need AWS, a Databricks Free Edition workspace, a
Telegram bot and GitHub Actions.
