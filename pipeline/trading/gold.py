"""TRADING pipeline (stocks-trading): the paper-trader's journal and portfolio exports and everything built on them.
Runs when an export lands (job stocks-trading: 15:45 and 23:30 IST).

GOLD: the tables the dashboard, the morning brief and the paper trader read.

  stocks.gold.trades                         one row per trade: entry, exit, net P&L, R multiple
  stocks.gold.trading_daily                  per day: activity, P&L, balance, reconciliation vs the ledger
  stocks.gold.trading_by_setup               results per setup and side
  stocks.gold.trading_by_hour                results per entry hour (IST)
  stocks.gold.trading_decisions              your YES/NO choices: taken trades vs what skipped ones would have made
  stocks.gold.proposal_outcomes              every proposal: scores, mood, mode, decision, outcome, the user's 👍/👎
  stocks.gold.track_record                   how proposals turned out by setup, hour, mood, suggestion band, ...
  stocks.gold.rating_review                  your 👍/👎 vs what really happened (never trains the trader)
  stocks.gold.portfolio_daily                whole dummy portfolio per day: value, P&L, return, fall from peak, NIFTYBEES alongside
  stocks.gold.holdings_latest                current holdings with P&L, weight and days held
  stocks.gold.shadow_pools_daily             shadow pools (swing, long-term) per day vs NIFTYBEES since each one started
  stocks.gold.custom_trades                  your own trades placed from Telegram (intraday, swing, long-term)
  stocks.gold.shadow_rules_trades            your real trades under other exit rules and the shadow models
  stocks.gold.shadow_rules_summary           totals of the above per rule
  stocks.gold.orb_test                       the opening-range test on paper
  stocks.gold.model_backtest                 the shadow models' daily walk-forward backtest
Reads market tables (NIFTYBEES closes in stocks.silver.nse_eq_daily, the Nifty 200 list) as plain
tables; they are at most a day old, which is fine for the portfolio and shadow comparisons.
"""

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

    dp.materialized_view = dp.table

from pyspark.sql import Window
from pyspark.sql import functions as F

# ===========================================================================
# Paper trading (dummy money). Source: stocks.silver.trade_events
# ===========================================================================
# Money flow in the trader's ledger, which these tables reproduce:
#   fill:  balance -= brokerage                         (fees_rs on the 'filled' event)
#   exit:  balance += gross P&L - brokerage             (pnl_rs on 'exited'/'squared_off' is already net
#                                                         of the exit brokerage; fees_rs shows it)
#   net P&L of a trade = exit pnl_rs - entry fees_rs
# Slippage is already inside the fill and exit prices.


def _trade_events_df():
    return spark.read.table("stocks.silver.trade_events")  # noqa: F821

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.trades AS
#   WITH fills AS (
#     SELECT trade_date, proposal_id, symbol, setup, side, qty, risk_rs, entry AS planned_entry, stop, target,
#            fill_price AS entry_price, event_ts AS entry_time, fees_rs AS entry_fees_rs
#     FROM stocks.silver.trade_events WHERE event = 'filled'),
#   exits AS (
#     SELECT trade_date, proposal_id, exit_price, exit_reason, event_ts AS exit_time,
#            pnl_rs AS exit_pnl_rs, fees_rs AS exit_fees_rs
#     FROM stocks.silver.trade_events WHERE event IN ('exited', 'squared_off'))
#   SELECT f.*, e.exit_price, e.exit_reason, e.exit_time,
#          CASE WHEN e.proposal_id IS NULL THEN 'open' ELSE 'closed' END            AS status,
#          e.exit_pnl_rs - f.entry_fees_rs                                          AS net_pnl_rs,
#          f.entry_fees_rs + coalesce(e.exit_fees_rs, 0)                            AS fees_rs,
#          (e.exit_pnl_rs - f.entry_fees_rs) / f.risk_rs                            AS r_multiple,
#          (unix_timestamp(e.exit_time) - unix_timestamp(f.entry_time)) / 60        AS holding_minutes,
#          hour(f.entry_time)                                                       AS entry_hour,
#          e.exit_pnl_rs - f.entry_fees_rs > 0                                      AS is_win
#   FROM fills f LEFT JOIN exits e USING (trade_date, proposal_id);
@dp.materialized_view(
    name="stocks.gold.trades",
    comment="Paper trades (dummy money), one row per trade: entry, exit, fees, net P&L, R multiple.",
    table_properties={"quality": "gold"},
)
def trades():
    ev = _trade_events_df()
    fills = ev.filter("event = 'filled'").select(
        "trade_date", "proposal_id", "symbol", "setup", "side", "qty", "risk_rs",
        F.col("entry").alias("planned_entry"), "stop", "target",
        F.col("fill_price").alias("entry_price"),
        F.col("event_ts").alias("entry_time"),
        F.col("fees_rs").alias("entry_fees_rs"),
    )
    exits = ev.filter("event IN ('exited', 'squared_off')").select(
        "trade_date", "proposal_id", "exit_price", "exit_reason",
        F.col("event_ts").alias("exit_time"),
        F.col("pnl_rs").alias("exit_pnl_rs"),
        F.col("fees_rs").alias("exit_fees_rs"),
    )
    net = F.col("exit_pnl_rs") - F.col("entry_fees_rs")
    return (
        fills.join(exits, ["trade_date", "proposal_id"], "left")
        .withColumn("status", F.when(F.col("exit_time").isNull(), "open").otherwise("closed"))
        .withColumn("net_pnl_rs", net)
        .withColumn("fees_rs", F.col("entry_fees_rs") + F.coalesce(F.col("exit_fees_rs"), F.lit(0.0)))
        .withColumn("r_multiple", net / F.col("risk_rs"))
        .withColumn("holding_minutes",
                    (F.unix_timestamp("exit_time") - F.unix_timestamp("entry_time")) / 60)
        .withColumn("entry_hour", F.hour("entry_time"))
        .withColumn("is_win", net > 0)
        .drop("exit_pnl_rs")
    )

# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.trading_daily AS
#   WITH ev AS (
#     SELECT trade_date,
#            count_if(event = 'proposed')                AS proposals,
#            count_if(event = 'blocked_by_risk_gate')    AS blocked_by_risk_gate,
#            count_if(event = 'rejected')                AS rejected,
#            count(DISTINCT CASE WHEN event = 'expired' THEN proposal_id END) AS expired,
#            count_if(event = 'skipped_after_approval')  AS skipped_after_approval,
#            count_if(event = 'filled')                  AS trades_opened,
#            sum(CASE WHEN event IN ('exited','squared_off') THEN pnl_rs END)
#              - coalesce(sum(CASE WHEN event = 'filled' THEN fees_rs END), 0) AS realized_from_events_rs,
#            sum(CASE WHEN event = 'passed_outcome' THEN pnl_rs END)           AS passed_hypothetical_pnl_rs
#     FROM stocks.silver.trade_events GROUP BY trade_date),
#   t AS (SELECT trade_date, count(*) AS trades_closed, count_if(is_win) AS wins, sum(net_pnl_rs) AS net_pnl_rs,
#                sum(fees_rs) AS fees_rs FROM stocks.gold.trades WHERE status = 'closed' GROUP BY trade_date),
#   s AS (SELECT trade_date, balance, starting_capital, realized_today
#         FROM stocks.silver.trade_events WHERE event = 'ledger_snapshot')
#   SELECT ev.*, t.trades_closed, t.wins, 100 * t.wins / t.trades_closed AS win_rate_pct, t.net_pnl_rs, t.fees_rs,
#          s.balance AS end_balance_rs, s.balance - s.starting_capital AS total_pnl_since_start_rs,
#          s.realized_today AS ledger_realized_rs,
#          round(coalesce(ev.realized_from_events_rs, 0) - s.realized_today, 2) AS reconciliation_diff_rs,
#          CASE WHEN s.realized_today IS NULL THEN 'no snapshot'
#               WHEN abs(coalesce(ev.realized_from_events_rs, 0) - s.realized_today) < 1 THEN 'ok'
#               ELSE 'MISMATCH' END AS reconciliation
#   FROM ev LEFT JOIN t USING (trade_date) LEFT JOIN s USING (trade_date);
@dp.materialized_view(
    name="stocks.gold.trading_daily",
    comment="Paper trading per day: proposals and decisions, P&L, end balance, and a reconciliation of "
            "the P&L rebuilt from events against the trader's S3 ledger snapshot.",
    table_properties={"quality": "gold"},
)
def trading_daily():
    ev = _trade_events_df()

    def count_if(cond):
        return F.sum(F.when(F.expr(cond), 1).otherwise(0))

    activity = ev.groupBy("trade_date").agg(
        count_if("event = 'proposed'").alias("proposals"),
        count_if("event = 'blocked_by_risk_gate'").alias("blocked_by_risk_gate"),
        count_if("event = 'rejected'").alias("rejected"),
        F.countDistinct(F.when(F.col("event") == "expired", F.col("proposal_id"))).alias("expired"),
        count_if("event = 'skipped_after_approval'").alias("skipped_after_approval"),
        count_if("event = 'filled'").alias("trades_opened"),
        (F.coalesce(F.sum(F.when(F.col("event").isin("exited", "squared_off"), F.col("pnl_rs"))), F.lit(0.0))
         - F.coalesce(F.sum(F.when(F.col("event") == "filled", F.col("fees_rs"))), F.lit(0.0))
         ).alias("realized_from_events_rs"),
        F.sum(F.when(F.col("event") == "passed_outcome", F.col("pnl_rs"))).alias("passed_hypothetical_pnl_rs"),
    )
    closed = (
        spark.read.table("stocks.gold.trades")  # noqa: F821
        .filter("status = 'closed'")
        .groupBy("trade_date")
        .agg(
            F.count("*").alias("trades_closed"),
            F.sum(F.col("is_win").cast("int")).alias("wins"),
            F.sum("net_pnl_rs").alias("net_pnl_rs"),
            F.sum("fees_rs").alias("fees_rs"),
        )
    )
    snap = ev.filter("event = 'ledger_snapshot'").select(
        "trade_date",
        F.col("balance").alias("end_balance_rs"),
        "starting_capital",
        F.col("realized_today").alias("ledger_realized_rs"),
    )
    diff = F.col("realized_from_events_rs") - F.col("ledger_realized_rs")
    return (
        activity.join(closed, "trade_date", "left")
        .join(snap, "trade_date", "left")
        .withColumn("win_rate_pct", 100 * F.col("wins") / F.col("trades_closed"))
        .withColumn("total_pnl_since_start_rs", F.col("end_balance_rs") - F.col("starting_capital"))
        .withColumn("reconciliation_diff_rs", F.round(diff, 2))
        .withColumn(
            "reconciliation",
            F.when(F.col("ledger_realized_rs").isNull(), "no snapshot")
            .when(F.abs(diff) < 1, "ok")
            .otherwise("MISMATCH"),
        )
        .drop("starting_capital")
    )

def _results(group_cols):
    t = spark.read.table("stocks.gold.trades").filter("status = 'closed'")  # noqa: F821
    return (
        t.groupBy(*group_cols)
        .agg(
            F.count("*").alias("trades"),
            F.sum(F.col("is_win").cast("int")).alias("wins"),
            F.sum("net_pnl_rs").alias("net_pnl_rs"),
            F.avg("net_pnl_rs").alias("avg_pnl_per_trade_rs"),
            F.avg("r_multiple").alias("avg_r_multiple"),
            F.max("net_pnl_rs").alias("best_trade_rs"),
            F.min("net_pnl_rs").alias("worst_trade_rs"),
            F.sum("fees_rs").alias("fees_rs"),
        )
        .withColumn("win_rate_pct", 100 * F.col("wins") / F.col("trades"))
    )

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.trading_by_setup AS
#   SELECT setup, side, count(*) AS trades, count_if(is_win) AS wins, sum(net_pnl_rs) AS net_pnl_rs,
#          avg(net_pnl_rs) AS avg_pnl_per_trade_rs, avg(r_multiple) AS avg_r_multiple,
#          max(net_pnl_rs) AS best_trade_rs, min(net_pnl_rs) AS worst_trade_rs, sum(fees_rs) AS fees_rs,
#          100 * count_if(is_win) / count(*) AS win_rate_pct
#   FROM stocks.gold.trades WHERE status = 'closed' GROUP BY setup, side;
@dp.materialized_view(
    name="stocks.gold.trading_by_setup",
    comment="Paper trading results per setup and side (closed trades): win rate, P&L, average R.",
    table_properties={"quality": "gold"},
)
def trading_by_setup():
    return _results(["setup", "side"])

# SQL equivalent: as trading_by_setup, grouped by entry_hour (IST hour of the fill).
@dp.materialized_view(
    name="stocks.gold.trading_by_hour",
    comment="Paper trading results per entry hour (IST), closed trades.",
    table_properties={"quality": "gold"},
)
def trading_by_hour():
    return _results(["entry_hour"])

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.trading_decisions AS
#   SELECT trade_date,
#          count_if(event = 'filled')                                   AS taken,
#          count_if(event = 'passed_outcome')                           AS passed,
#          count_if(event = 'passed_outcome' AND user_reply = 'rejected') AS passed_by_no,
#          count_if(event = 'passed_outcome' AND user_reply = 'expired')  AS passed_by_no_reply,
#          sum(CASE WHEN event = 'passed_outcome' THEN pnl_rs END)      AS passed_would_have_made_rs,
#          count_if(event = 'passed_outcome' AND pnl_rs > 0)            AS passed_would_have_won
#   FROM stocks.silver.trade_events GROUP BY trade_date;
#   (+ taken_net_pnl_rs from stocks.gold.trades, and saved_by_skipping_rs = -passed_would_have_made_rs)
@dp.materialized_view(
    name="stocks.gold.trading_decisions",
    comment="Your YES/NO decisions per day: P&L of taken trades vs what the skipped proposals would have "
            "made. saved_by_skipping_rs > 0 means skipping helped.",
    table_properties={"quality": "gold"},
)
def trading_decisions():
    ev = _trade_events_df()
    passed = F.col("event") == "passed_outcome"
    decisions = ev.groupBy("trade_date").agg(
        F.sum(F.when(F.col("event") == "filled", 1).otherwise(0)).alias("taken"),
        F.sum(F.when(passed, 1).otherwise(0)).alias("passed"),
        F.sum(F.when(passed & (F.col("user_reply") == "rejected"), 1).otherwise(0)).alias("passed_by_no"),
        F.sum(F.when(passed & (F.col("user_reply") == "expired"), 1).otherwise(0)).alias("passed_by_no_reply"),
        F.sum(F.when(passed, F.col("pnl_rs"))).alias("passed_would_have_made_rs"),
        F.sum(F.when(passed & (F.col("pnl_rs") > 0), 1).otherwise(0)).alias("passed_would_have_won"),
    )
    taken_pnl = (
        spark.read.table("stocks.gold.trades")  # noqa: F821
        .filter("status = 'closed'")
        .groupBy("trade_date")
        .agg(F.sum("net_pnl_rs").alias("taken_net_pnl_rs"))
    )
    return (
        decisions.join(taken_pnl, "trade_date", "left")
        .withColumn("saved_by_skipping_rs", -F.coalesce(F.col("passed_would_have_made_rs"), F.lit(0.0)))
    )

# ---------------------------------------------------------------------------
# The feedback loop: every proposal with its scores, decision, outcome and the user's rating, and the
# track record paper-trader reads each session (trader/track.py) so decisions learn from results.
# ---------------------------------------------------------------------------
# A proposal's outcome is its real net P&L when it was traded, or what it would have made when it was
# skipped (passed_outcome), so the model's judgement is measured on every proposal, not only the taken ones.
#
# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.proposal_outcomes AS
#   SELECT p.trade_date, p.proposal_id, p.event_ts AS proposed_at, date_format(p.event_ts, 'HH') AS hour,
#          p.symbol, p.setup, p.side, p.risk_rs,
#          CAST(regexp_extract(p.note, 'suggested \\w+ (\\d+)%', 1) AS INT) AS suggested_yes_pct, ...,
#          CASE WHEN filled THEN 'taken' WHEN auto_no THEN 'skipped_auto' WHEN rejected THEN 'skipped_by_you'
#               WHEN expired THEN 'expired' WHEN not_filled THEN 'not_filled' END AS decision,
#          coalesce(t.net_pnl_rs, po.pnl_rs) AS outcome_rs, fb.user_reply AS feedback
#   FROM proposed p LEFT JOIN decisions ... LEFT JOIN stocks.gold.trades t ... LEFT JOIN passed_outcome po ...
#   LEFT JOIN latest user_feedback fb ...;
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.track_record AS
#   SELECT 'setup' AS dimension, setup AS value, count(*) AS trades, sum(won) AS wins, ... FROM outcomes
#   GROUP BY setup UNION ALL ... (hour, mood, yes_band, reviewer, mode, decision, source, sector, side,
#   scanner, all);
# The user's 👍/👎 is kept in proposal_outcomes for their own review (gold.rating_review, the dashboard's "Your
# ratings" page) but is never a track_record dimension: ratings don't train the trader (from 17 Nov).
TRACK_DAYS = 90

TRACK_DIMENSIONS = [("all", None), ("setup", "setup"), ("hour", "hour"), ("mood", "mood"), ("yes_band", "yes_band"),
                    ("reviewer", "reviewer_verdict"), ("mode", "mode"), ("decision", "decision"),
                    ("source", "source"), ("sector", "sector"), ("side", "side"),
                    ("scanner", "scanner")]

# Sector of each watchlist stock, as paper-trader names them (trader/sectors.py), for proposals made before the
# trader wrote "sector ..." into its note (16 Nov). Newer proposals carry their own sector; others are "Other".
WATCHLIST_SECTOR = {
    "ADANIPORTS": "Infra",
    "ASIANPAINT": "Consumer",
    "AUBANK": "Banks",
    "AXISBANK": "Banks",
    "BAJAJ-AUTO": "Auto",
    "BAJAJFINSV": "Financials",
    "BAJFINANCE": "Financials",
    "BANDHANBNK": "Banks",
    "BANKBARODA": "Banks",
    "BHARTIARTL": "Telecom",
    "BPCL": "Energy",
    "BRITANNIA": "FMCG",
    "CANBK": "Banks",
    "CHOLAFIN": "Financials",
    "CIPLA": "Pharma",
    "COALINDIA": "Energy",
    "COFORGE": "IT",
    "DIVISLAB": "Pharma",
    "DRREDDY": "Pharma",
    "EICHERMOT": "Auto",
    "FEDERALBNK": "Banks",
    "HCLTECH": "IT",
    "HDFCBANK": "Banks",
    "HDFCLIFE": "Financials",
    "HEROMOTOCO": "Auto",
    "HINDALCO": "Metals",
    "HINDUNILVR": "FMCG",
    "ICICIBANK": "Banks",
    "IDFCFIRSTB": "Banks",
    "INDUSINDBK": "Banks",
    "INFY": "IT",
    "ITC": "FMCG",
    "JSWSTEEL": "Metals",
    "KOTAKBANK": "Banks",
    "LT": "Infra",
    "LTIM": "IT",
    "LUPIN": "Pharma",
    "M&M": "Auto",
    "MARUTI": "Auto",
    "MPHASIS": "IT",
    "NESTLEIND": "FMCG",
    "NTPC": "Energy",
    "ONGC": "Energy",
    "PERSISTENT": "IT",
    "PNB": "Banks",
    "POWERGRID": "Energy",
    "RELIANCE": "Energy",
    "SBILIFE": "Financials",
    "SBIN": "Banks",
    "SHRIRAMFIN": "Financials",
    "SUNPHARMA": "Pharma",
    "TATASTEEL": "Metals",
    "TCS": "IT",
    "TECHM": "IT",
    "TITAN": "Consumer",
    "TRENT": "Consumer",
    "ULTRACEMCO": "Infra",
    "VEDL": "Metals",
    "WIPRO": "IT",
}

def _note_value(pattern, cast=None):
    raw = F.regexp_extract(F.col("note"), pattern, 1)
    col = F.when(raw == "", None).otherwise(F.trim(raw))
    return col.cast(cast) if cast else col

@dp.materialized_view(
    name="stocks.gold.proposal_outcomes",
    comment="One row per paper-trader proposal: its scores (suggestion, live checks, agent, reviewer), market mood, "
            "mode (auto/human), decision (taken, skipped_by_you, skipped_auto, expired, not_filled), outcome in "
            "rupees (real if taken, what it would have made if skipped) and the user's 👍/👎 feedback.",
    table_properties={"quality": "gold"},
)
def proposal_outcomes():
    ev = _trade_events_df()
    proposed = (
        ev.filter("event = 'proposed'")
        .select("trade_date", "proposal_id", F.col("event_ts").alias("proposed_at"), "symbol", "setup", "side",
                "risk_rs", "note")
        .withColumn("hour", F.date_format("proposed_at", "HH"))
        .withColumn("suggested_yes_pct", _note_value(r"suggested \w+ (\d+)%", "int"))
        .withColumn("checks_pct", _note_value(r"checks (\d+)%", "int"))
        .withColumn("agent_pct", _note_value(r"agent ([\d.]+)%", "double"))
        .withColumn("reviewer_verdict", _note_value(r"reviewer (YES|NO|UNSURE) "))
        .withColumn("reviewer_pct", _note_value(r"reviewer \w+ (\d+)%", "int"))
        .withColumn("mood", _note_value(r"mood ([a-z ]+?) \|"))
        .withColumn("mode", F.coalesce(_note_value(r"mode (\w+)"), F.lit("human")))
        .withColumn("source", F.coalesce(_note_value(r"source (\w+)"), F.lit("watchlist")))
        .withColumn("scanner", _note_value(r"scanner (strict|near|none)"))   # from 17 Nov; older rows: null
        .withColumn("sector", F.coalesce(_note_value(r"sector ([A-Za-z&/ -]+?) \|"),
                                         F.element_at(F.create_map(*[F.lit(x) for kv in WATCHLIST_SECTOR.items()
                                                                     for x in kv]), F.col("symbol")),
                                         F.lit("Other")))
        .withColumn("yes_band", F.concat((F.floor(F.col("suggested_yes_pct") / 10) * 10).cast("int").cast("string"),
                                         F.lit("-"),
                                         (F.floor(F.col("suggested_yes_pct") / 10) * 10 + 9).cast("int").cast("string"),
                                         F.lit("%")))
        .drop("note")
    )
    flag = lambda cond: F.max(F.when(cond, 1).otherwise(0))  # noqa: E731
    decided = ev.filter(F.col("proposal_id").isNotNull()).groupBy("proposal_id").agg(
        flag(F.col("event") == "filled").alias("filled"),
        flag((F.col("event") == "rejected") & (F.col("user_reply") == "AUTO-NO")).alias("auto_no"),
        flag(F.col("event") == "rejected").alias("rejected"),
        flag(F.col("event") == "expired").alias("expired"),
        flag(F.col("event") == "skipped_after_approval").alias("not_filled"),
        F.max(F.when(F.col("event") == "passed_outcome", F.col("pnl_rs"))).alias("would_have_made_rs"),
    )
    taken = (spark.read.table("stocks.gold.trades")  # noqa: F821
             .filter("status = 'closed'").select("proposal_id", F.col("net_pnl_rs").alias("taken_net_pnl_rs")))
    fb_window = Window.partitionBy("proposal_id").orderBy(F.col("event_ts").desc())
    feedback = (ev.filter("event = 'user_feedback'")
                .withColumn("rn", F.row_number().over(fb_window)).filter("rn = 1")
                .select("proposal_id",
                        # 'none' = the user took their 👍/👎 reaction away: not rated
                        F.when(F.col("user_reply") == "none", None).otherwise(F.col("user_reply")).alias("feedback")))
    decision = (F.when(F.col("filled") == 1, "taken").when(F.col("auto_no") == 1, "skipped_auto")
                .when(F.col("rejected") == 1, "skipped_by_you").when(F.col("expired") == 1, "expired")
                .when(F.col("not_filled") == 1, "not_filled").otherwise("undecided"))
    outcome = F.when(F.col("filled") == 1, F.col("taken_net_pnl_rs")).otherwise(F.col("would_have_made_rs"))
    return (
        proposed.join(decided, "proposal_id", "left").join(taken, "proposal_id", "left")
        .join(feedback, "proposal_id", "left")
        .withColumn("decision", decision)
        .withColumn("outcome_rs", outcome)
        .withColumn("won", F.when(F.col("outcome_rs").isNotNull(), (F.col("outcome_rs") > 0).cast("int")))
        .withColumn("outcome_r", F.col("outcome_rs") / F.col("risk_rs"))
        .drop("filled", "auto_no", "rejected", "expired", "not_filled", "would_have_made_rs", "taken_net_pnl_rs")
    )

@dp.materialized_view(
    name="stocks.gold.track_record",
    comment=f"How proposals turned out over the last {TRACK_DAYS} days, by dimension (setup, hour, mood, yes_band, "
            "reviewer, mode, decision, source, sector, side (BUY/SELL), scanner, all; never the user's ratings): proposals with an outcome, wins, win rate, average and total "
            "outcome in rupees. paper-trader shows it in proposals and gives it to Claude each session.",
    table_properties={"quality": "gold"},
)
def track_record():
    po = (spark.read.table("stocks.gold.proposal_outcomes")  # noqa: F821
          .filter(F.col("outcome_rs").isNotNull())
          .filter(F.col("trade_date") >= F.date_sub(F.current_date(), TRACK_DAYS)))
    # Your own (custom) trades appear only under dimension 'source', so they don't change how the trader's
    # own setups, hours, moods and suggestion bands are judged.
    own = po.filter(F.col("source") != "custom")
    parts = []
    for dimension, column in TRACK_DIMENSIONS:
        value = F.lit("all") if column is None else F.col(column).cast("string")
        parts.append(
            (po if dimension == "source" else own).withColumn("value", value).filter(F.col("value").isNotNull())
            .groupBy(F.lit(dimension).alias("dimension"), "value")
            .agg(F.count("*").alias("trades"), F.sum("won").alias("wins"),
                 F.round(F.avg("outcome_rs"), 2).alias("avg_outcome_rs"),
                 F.round(F.sum("outcome_rs"), 2).alias("total_outcome_rs"),
                 F.round(F.avg("outcome_r"), 2).alias("avg_outcome_r"))
        )
    out = parts[0]
    for part in parts[1:]:
        out = out.unionByName(part)
    return out.withColumn("win_rate_pct", F.round(F.col("wins") / F.col("trades") * 100, 1))

# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.rating_review AS
#   SELECT coalesce(feedback, 'not rated') AS rating, decision_group, count(*) AS proposals, sum(won) AS wins,
#          avg(won) * 100 AS win_rate_pct, sum(outcome_rs) AS total_outcome_rs, avg(outcome_rs) AS avg_outcome_rs,
#          avg(outcome_r) AS avg_outcome_r,
#          sum(CASE WHEN feedback = 'like' AND won = 1 OR feedback = 'dislike' AND won = 0 THEN 1 END) AS right_calls
#   FROM stocks.gold.proposal_outcomes WHERE outcome_rs IS NOT NULL
#   GROUP BY 1, 2 (decision_group: taken / skipped / all) ...;
@dp.materialized_view(
    name="stocks.gold.rating_review",
    comment="Your 👍/👎 against what really happened, for your own learning (ratings never train the trader): per "
            "rating (like, dislike, not rated) and decision group (taken, skipped, all): proposals with an outcome, "
            "wins, win rate, total and average result in rupees and R, and right_calls (a 👍 that made money or a 👎 "
            "that lost) with right_pct. Outcome = the real net P&L if taken, what it would have made if skipped.",
    table_properties={"quality": "gold"},
)
def rating_review():
    po = (spark.read.table("stocks.gold.proposal_outcomes")  # noqa: F821
          .filter(F.col("outcome_rs").isNotNull() & (F.col("source") != "custom"))
          .withColumn("rating", F.coalesce(F.col("feedback"), F.lit("not rated")))
          .withColumn("decision_group", F.when(F.col("decision") == "taken", "taken").otherwise("skipped"))
          .withColumn("right", F.when(((F.col("rating") == "like") & (F.col("won") == 1))
                                      | ((F.col("rating") == "dislike") & (F.col("won") == 0)), 1)
                      .when(F.col("rating").isin("like", "dislike"), 0)))
    both = po.unionByName(po.withColumn("decision_group", F.lit("all")))
    return (both.groupBy("rating", "decision_group")
            .agg(F.count("*").alias("proposals"), F.sum("won").alias("wins"),
                 F.round(F.sum("outcome_rs"), 2).alias("total_outcome_rs"),
                 F.round(F.avg("outcome_rs"), 2).alias("avg_outcome_rs"),
                 F.round(F.avg("outcome_r"), 2).alias("avg_outcome_r"),
                 F.sum("right").alias("right_calls"))
            .withColumn("win_rate_pct", F.round(F.col("wins") / F.col("proposals") * 100, 1))
            .withColumn("right_pct", F.round(F.col("right_calls") / F.col("proposals") * 100, 1)))

# ---------------------------------------------------------------------------
# Paper-trader portfolio (dummy money): the dashboard's Portfolio page
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.portfolio_daily AS
#   SELECT snapshot_date, sum(equity) AS total_value_rs, sum(capital) AS capital_rs, ...,
#          100 * total / first(total) AS portfolio_index, 100 * nifty / first(nifty) AS nifty_index,
#          100 * (total / max(total) OVER (ORDER BY snapshot_date) - 1) AS drawdown_pct
#   FROM stocks.silver.portfolio_pools GROUP BY snapshot_date ...;
@dp.materialized_view(
    name="stocks.gold.portfolio_daily",
    comment="The whole dummy portfolio per day: value by pool and in total, P&L and return on capital, fall from "
            "the peak, and NIFTYBEES on the same days (both indexed to 100 on the first day).",
    table_properties={"quality": "gold"},
)
def portfolio_daily():
    pools = spark.read.table("stocks.silver.portfolio_pools")  # noqa: F821
    by_pool = F.sum(F.when(F.col("pool") == "intraday", F.col("equity")))
    day = (pools.groupBy("snapshot_date").agg(
        F.round(F.sum("equity"), 2).alias("total_value_rs"),
        F.round(F.sum("capital"), 2).alias("capital_rs"),
        F.round(F.sum("cash"), 2).alias("cash_rs"),
        F.round(by_pool, 2).alias("intraday_rs"),
        F.round(F.sum(F.when(F.col("pool") == "swing", F.col("equity"))), 2).alias("swing_rs"),
        F.round(F.sum(F.when(F.col("pool") == "long_term", F.col("equity"))), 2).alias("long_term_rs")))
    nifty = (spark.read.table("stocks.silver.nse_eq_daily")  # noqa: F821
             .filter("symbol = 'NIFTYBEES' AND series = 'EQ'").select(F.col("trade_date"), F.col("close")))
    last_nifty = (day.join(nifty, nifty.trade_date <= day.snapshot_date, "left")
                  .groupBy("snapshot_date").agg(F.max_by("close", "trade_date").alias("niftybees_close")))
    order = Window.orderBy("snapshot_date")
    whole = Window.orderBy("snapshot_date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    upto = Window.orderBy("snapshot_date").rowsBetween(Window.unboundedPreceding, 0)
    return (day.join(last_nifty, "snapshot_date", "left")
            .withColumn("pnl_rs", F.round(F.col("total_value_rs") - F.col("capital_rs"), 2))
            .withColumn("return_pct", F.round((F.col("total_value_rs") / F.col("capital_rs") - 1) * 100, 2))
            .withColumn("peak_rs", F.max("total_value_rs").over(upto))
            .withColumn("drawdown_pct", F.round((F.col("total_value_rs") / F.col("peak_rs") - 1) * 100, 2))
            .withColumn("portfolio_index", F.round(F.col("total_value_rs") / F.first("total_value_rs").over(whole) * 100, 2))
            .withColumn("nifty_index", F.round(F.col("niftybees_close") / F.first("niftybees_close", ignorenulls=True).over(whole) * 100, 2))
            .withColumn("day_change_rs", F.round(F.col("total_value_rs") - F.lag("total_value_rs").over(order), 2)))

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.holdings_latest AS
#   SELECT h.*, 100 * pnl / (value - pnl) AS pnl_pct, 100 * value / total AS weight_pct,
#          datediff(snapshot_date, opened) AS days_held
#   FROM stocks.silver.portfolio_holdings h WHERE snapshot_date = (SELECT max(snapshot_date) ...);
@dp.materialized_view(
    name="stocks.gold.holdings_latest",
    comment="Current holdings across pools (latest snapshot): quantity, average cost, last price, value, P&L in Rs "
            "and %, weight in the whole portfolio, days held.",
    table_properties={"quality": "gold"},
)
def holdings_latest():
    h = spark.read.table("stocks.silver.portfolio_holdings")  # noqa: F821
    pools = spark.read.table("stocks.silver.portfolio_pools")  # noqa: F821
    last = h.agg(F.max("snapshot_date").alias("snapshot_date"))
    total = (pools.join(last, "snapshot_date").agg(F.sum("equity").alias("portfolio_value")))
    return (h.join(last, "snapshot_date").crossJoin(total)
            .withColumn("cost", F.col("value") - F.col("pnl"))
            .withColumn("pnl_pct", F.round(F.when(F.col("cost") > 0, F.col("pnl") / F.col("cost") * 100), 2))
            .withColumn("weight_pct", F.round(F.col("value") / F.col("portfolio_value") * 100, 2))
            .withColumn("days_held", F.datediff("snapshot_date", "opened"))
            .select("snapshot_date", "pool", "symbol", "sector", "qty", "avg_cost", "last_price", "value", "pnl",
                    "pnl_pct", "weight_pct", "opened", "days_held"))

# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.shadow_pools_daily AS
#   SELECT snapshot_date, pool, equity, (equity / capital - 1) * 100 AS return_pct,
#          (niftybees_close / first(niftybees_close) OVER (PARTITION BY pool ORDER BY snapshot_date) - 1) * 100
#            AS nifty_return_pct, return_pct - nifty_return_pct AS vs_nifty_pct
#   FROM stocks.silver.shadow_pools LEFT JOIN (NIFTYBEES close on or before each day);
@dp.materialized_view(
    name="stocks.gold.shadow_pools_daily",
    comment="Shadow pools per day (swing_shadow: news breakouts; long_term_shadow: weekly quality picks): equity, "
            "return since the start, NIFTYBEES over the same days, and the difference. A shadow pool earns real "
            "money only if it beats NIFTYBEES at its review.",
    table_properties={"quality": "gold"},
)
def shadow_pools_daily():
    sp = spark.read.table("stocks.silver.shadow_pools")  # noqa: F821
    nifty = (spark.read.table("stocks.silver.nse_eq_daily")  # noqa: F821
             .filter("symbol = 'NIFTYBEES' AND series = 'EQ'").select("trade_date", "close"))
    days = sp.select("snapshot_date").distinct()
    last_nifty = (days.join(nifty, nifty.trade_date <= days.snapshot_date, "left")
                  .groupBy("snapshot_date").agg(F.max_by("close", "trade_date").alias("niftybees_close")))
    first = Window.partitionBy("pool").orderBy("snapshot_date").rowsBetween(Window.unboundedPreceding, 0)
    return (sp.join(last_nifty, "snapshot_date", "left")
            .withColumn("return_pct", F.round((F.col("equity") / F.col("capital") - 1) * 100, 2))
            .withColumn("nifty_return_pct", F.round(
                (F.col("niftybees_close") / F.first("niftybees_close", ignorenulls=True).over(first) - 1) * 100, 2))
            .withColumn("vs_nifty_pct", F.round(F.col("return_pct") - F.col("nifty_return_pct"), 2))
            .select("snapshot_date", "pool", "capital", "cash", "holdings_value", "equity", "return_pct",
                    "niftybees_close", "nifty_return_pct", "vs_nifty_pct"))

# ---------------------------------------------------------------------------
# Your own trades from Telegram (/buy, /short, /swing, /longterm in paper-trader): marked custom
# ---------------------------------------------------------------------------
# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.custom_trades AS
#   SELECT trade_date, 'intraday' AS pool, symbol, side, qty, entry_price AS price, exit_price, net_pnl_rs AS pnl_rs,
#          fees_rs, status, entry_time AS event_ts FROM stocks.gold.trades WHERE setup = 'Custom'
#   UNION ALL
#   SELECT trade_date, regexp_extract(note, 'pool (\\w+)', 1) AS pool, symbol, side, qty, fill_price AS price,
#          NULL AS exit_price, pnl_rs, fees_rs, CASE side WHEN 'BUY' THEN 'bought' ELSE 'sold' END AS status, event_ts
#   FROM stocks.silver.trade_events WHERE event = 'pool_trade' AND note LIKE '%source custom%';
@dp.materialized_view(
    name="stocks.gold.custom_trades",
    comment="Your own trades placed from Telegram (dummy money): intraday ones with their net P&L and status, and "
            "swing / long-term buys and sells (pnl_rs on sells, after costs). NIFTYBEES sells that paid for a "
            "swing buy are included. The trader's own trades are in gold.trades.",
    table_properties={"quality": "gold"},
)
def custom_trades():
    intraday = (spark.read.table("stocks.gold.trades")  # noqa: F821
                .filter(F.col("setup") == "Custom")
                .select("trade_date", F.lit("intraday").alias("pool"), "symbol", "side", "qty",
                        F.col("entry_price").alias("price"), "exit_price", F.col("net_pnl_rs").alias("pnl_rs"),
                        "fees_rs", "status", F.col("entry_time").alias("event_ts")))
    pool = (_trade_events_df()
            .filter((F.col("event") == "pool_trade") & F.col("note").contains("source custom"))
            .select("trade_date", F.regexp_extract("note", r"pool (\w+)", 1).alias("pool"), "symbol", "side", "qty",
                    F.col("fill_price").alias("price"), F.lit(None).cast("double").alias("exit_price"), "pnl_rs",
                    "fees_rs", F.when(F.col("side") == "BUY", "bought").otherwise("sold").alias("status"), "event_ts"))
    return intraday.unionByName(pool)

# ---------------------------------------------------------------------------
# Shadow results for the dashboard: what other rules and the shadow models would have done (paper only)
# ---------------------------------------------------------------------------
SHADOW_RULE_WORDS = {
    "current": "current rules", "breakeven": "stop to entry at +1R", "half_and_run": "sell half at +1R",
    "trailing": "trailing stop", "time_stop": "60-minute time stop", "no_stop": "no stop",
    "wide_1_5": "stop 1.5x wider, same Rs risk", "wide_2": "stop 2x wider, same Rs risk",
    "close_stop": "stop on a 5-min close", "filter": "model: trade filter", "vol_stop": "model: volatility stop",
    "early_cut": "model: early cut", "let_run": "model: let winners run", "combined": "models combined",
    "orb5_2r": "5-min range, 2R target", "orb5_eod": "5-min range, held to 15:10",
    "orb15_2r": "15-min range, 2R target", "orb15_eod": "15-min range, held to 15:10",
}

def _rule_label():
    m = F.create_map(*[F.lit(x) for kv in SHADOW_RULE_WORDS.items() for x in kv])
    return F.coalesce(m[F.col("rule")], F.col("rule"))

def _note_pairs(df, sep):
    """Explode a 'k=v<sep>k=v' note into (rule, value) rows; empty values are dropped."""
    return (df.withColumn("kv", F.explode(F.split("note", sep)))
            .withColumn("rule", F.split("kv", "=").getItem(0))
            .withColumn("value", F.expr("try_cast(split(kv, '=')[1] AS DOUBLE)"))
            .filter(F.col("value").isNotNull() & (F.col("rule") != "")))

# SQL equivalent (abridged):
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.shadow_rules_trades AS
#   SELECT trade_date, proposal_id, symbol, setup, side, 'exit rules' AS test, rule, value AS would_have_made_rs,
#          pnl_rs AS actual_rs FROM trade_events LATERAL VIEW explode(split(note, ';')) ... WHERE event = 'exit_variants'
#   UNION ALL ... event = 'model_shadow' (test 'models'; p_win and filter_takes kept as columns);
@dp.materialized_view(
    name="stocks.gold.shadow_rules_trades",
    comment="Your real intraday trades replayed under other rules (paper only): test 'exit rules' (stop variants, "
            "trailing, breakeven, half and run, time stop; from the day report) and test 'models' (the shadow "
            "models' decisions, from the 16:10 model job). One row per trade x rule: what it would have made (Rs) "
            "next to what it really made.",
    table_properties={"quality": "gold"},
)
def shadow_rules_trades():
    ev = spark.read.table("stocks.silver.trade_events")  # noqa: F821
    base = ["trade_date", "proposal_id", "symbol", "setup", "side", F.col("pnl_rs").alias("actual_rs")]
    exits_ = (_note_pairs(ev.filter("event = 'exit_variants'"), ";")
              .select(*base, F.lit("exit rules").alias("test"), "rule", F.col("value").alias("would_have_made_rs")))
    m = ev.filter("event = 'model_shadow'")
    models = (_note_pairs(m, ";").filter(~F.col("rule").isin("p_win", "filter_takes"))
              .select(*base, F.lit("models").alias("test"), "rule", F.col("value").alias("would_have_made_rs")))
    return (exits_.unionByName(models).withColumn("rule_label", _rule_label())
            .withColumn("vs_actual_rs", F.round(F.col("would_have_made_rs") - F.col("actual_rs"), 2)))

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.shadow_rules_summary AS
#   SELECT test, rule, rule_label, count(*) AS trades, sum(would_have_made_rs), sum(actual_rs), ...
#   FROM stocks.gold.shadow_rules_trades GROUP BY test, rule, rule_label;
@dp.materialized_view(
    name="stocks.gold.shadow_rules_summary",
    comment="Totals of gold.shadow_rules_trades per test and rule: trades, what the rule would have made (Rs), what "
            "really happened (Rs), the difference, and how often the rule beat the real result.",
    table_properties={"quality": "gold"},
)
def shadow_rules_summary():
    t = spark.read.table("stocks.gold.shadow_rules_trades")  # noqa: F821
    return (t.groupBy("test", "rule", "rule_label")
            .agg(F.count("*").alias("trades"), F.min("trade_date").alias("first_day"), F.max("trade_date").alias("last_day"),
                 F.round(F.sum("would_have_made_rs"), 0).alias("would_have_made_rs"),
                 F.round(F.sum("actual_rs"), 0).alias("actual_rs"),
                 F.round(F.sum("vs_actual_rs"), 0).alias("vs_actual_rs"),
                 F.round(F.avg(F.when(F.col("vs_actual_rs") > 0, 100).otherwise(0)), 0).alias("beat_actual_pct")))

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.orb_test AS
#   SELECT trade_date, symbol, rule, value AS would_have_made_rs
#   FROM trade_events LATERAL VIEW explode(split(note, ';')) ... WHERE event = 'orb_shadow';
@dp.materialized_view(
    name="stocks.gold.orb_test",
    comment="The opening-range test on paper (top stocks in play each day): 5- vs 15-minute ranges, with a 2R "
            "target or held to 15:10. One row per stock x day x variant, in rupees at the real trade size.",
    table_properties={"quality": "gold"},
)
def orb_test():
    ev = spark.read.table("stocks.silver.trade_events").filter("event = 'orb_shadow'")  # noqa: F821
    return (_note_pairs(ev, ";").select("trade_date", "symbol", "rule", F.col("value").alias("would_have_made_rs"))
            .withColumn("rule_label", _rule_label()))

# SQL equivalent:
#   CREATE OR REFRESH MATERIALIZED VIEW stocks.gold.model_backtest AS
#   SELECT trade_date AS run_date, setup AS rule, <note fields as columns> FROM trade_events
#   WHERE event = 'model_backtest';
@dp.materialized_view(
    name="stocks.gold.model_backtest",
    comment="The shadow models' daily walk-forward backtest (every scanner setup on every tradable stock, each day "
            "judged only by models trained on earlier days), one row per run day x rule: signals, trades, total and "
            "average result in R (1R ~ Rs 400), win %, share of days up, worst day; is_latest marks the newest run.",
    table_properties={"quality": "gold"},
)
def model_backtest():
    ev = spark.read.table("stocks.silver.trade_events").filter("event = 'model_backtest'")  # noqa: F821
    wide = (ev.withColumn("m", F.expr("str_to_map(note, ';', '=')"))
            .select(F.col("trade_date").alias("run_date"), F.col("setup").alias("rule"),
                    *[F.expr(f"try_cast(m['{k}'] AS DOUBLE)").alias(k) for k in
                      ("signals", "traded", "total_r", "avg_r", "win_pct", "days", "days_up_pct", "worst_day_r")])
            .withColumn("rule_label", _rule_label()))
    latest = wide.agg(F.max("run_date").alias("run_date")).withColumn("is_latest", F.lit(True))
    base = wide.filter("rule = 'current'").select("run_date", F.col("total_r").alias("current_total_r"))
    return (wide.join(latest, "run_date", "left").join(base, "run_date", "left")
            .withColumn("is_latest", F.coalesce("is_latest", F.lit(False)))
            .withColumn("vs_current_r", F.round(F.col("total_r") - F.col("current_total_r"), 1))
            .drop("current_total_r"))
