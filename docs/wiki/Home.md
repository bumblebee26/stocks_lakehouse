# stocks_lakehouse wiki

The **research desk** behind [paper-trader](https://github.com/bumblebee26/paper-trader): AWS Lambdas fetch market
data on schedules, three Databricks pipelines turn it into bronze → silver → gold tables, and a dashboard, an 08:45
Telegram brief and a technical scanner read from them. The paper trader reads the gold tables before every session
and sends its journal back after the close.

## Pages
- [Pipelines and schedule](Pipelines-and-schedule.md): sources, jobs and when each table refreshes
- [Dashboard](Dashboard.md): the six pages of the "NSE Paper Trading Desk"
- [FAQ](FAQ.md)

## Deeper docs
[Architecture](../ARCHITECTURE.md) · [Diagrams](../ARCHITECTURE_DIAGRAMS.md) · [Data model](../DATA_MODEL.md) ·
[Platform](../PLATFORM.md) · [Setup](../SETUP.md) · [Operations](../OPERATIONS.md) ·
[Troubleshooting](../TROUBLESHOOTING.md)

Licensed under Apache 2.0 · © 2026 Viren Baria · dummy money only, not investment advice.
