"""Pre-market brief for the Indian session, sent to Telegram at 08:45 IST Mon-Fri.

Reads the gold/silver tables through Databricks SQL and summarises:
  - the previous US session (index ETFs, breadth)
  - global markets: Asia/Europe/EM equities, VIX, oil, US 10-year, USD/INR
  - what the Nifty proxy has historically done after a US day like that
  - Indian ADRs overnight
  - India's unusual-volume names from the last Indian session

Event: {} for a normal run, {"dry_run": true} to return the message without sending.
"""

import datetime as dt
import os
from zoneinfo import ZoneInfo

import databricks_sql
import telegram
from common import log
from telegram import esc

IST = ZoneInfo("Asia/Kolkata")
US_PROXY = os.environ.get("US_PROXY", "SPY")
INDIA_PROXY = os.environ.get("INDIA_PROXY", "NIFTYBEES")
BIG_MOVE_PCT = float(os.environ.get("BIG_MOVE_PCT", "1.0"))
UNUSUAL_TOP_N = int(os.environ.get("UNUSUAL_TOP_N", "10"))
STALE_AFTER_DAYS = 4

US_INDEX_ETFS = [(US_PROXY, "S&amp;P 500"), ("QQQ", "Nasdaq 100"), ("DIA", "Dow"), ("IWM", "Russell 2000")]
ADRS = ["INFY", "WIT", "HDB", "IBN", "RDY"]
# (instrument, label, kind) for the Global section; kind decides how the change is shown.
GLOBAL_EQUITIES = [("NIKKEI225", "Nikkei 225"), ("VGK", "Europe"), ("FXI", "China"),
                   ("EWY", "Korea"), ("EEM", "EM")]
GLOBAL_RISK = [("VIXCLS", "VIX", "level"), ("DCOILBRENTEU", "Brent", "usd"),
               ("DGS10", "US 10Y", "rate"), ("DTWEXBGS", "Dollar index", "level"),
               ("DEXINUS", "USD/INR", "fx")]
BUCKET_PHRASE = {"US up big": "big US up day", "US down big": "big US down day", "US flat": "flat US day"}


def _in_list(symbols):
    return ", ".join(f"'{s}'" for s in symbols)  # symbols are constants from this file


QUERIES = {
    "us_bars": f"""
        SELECT b.trade_date, b.symbol, b.close, (b.close / b.prev_close - 1) * 100 AS return_pct
        FROM stocks.silver.daily_bars b
        WHERE b.market = 'US'
          AND b.trade_date = (SELECT max(trade_date) FROM stocks.silver.trading_days WHERE market = 'US')
          AND b.symbol IN ({_in_list([s for s, _ in US_INDEX_ETFS] + ADRS)})
    """,
    "us_breadth": """
        SELECT trade_date, advances, declines, pct_above_ma20, median_return_pct
        FROM stocks.gold.market_breadth WHERE market = 'US'
        ORDER BY trade_date DESC LIMIT 1
    """,
    "global": f"""
        SELECT instrument, obs_date, value, change_pct, change_abs
        FROM stocks.gold.global_snapshot
        WHERE instrument IN ({_in_list([i for i, _ in GLOBAL_EQUITIES] + [i for i, _, _ in GLOBAL_RISK])})
    """,
    "summary": "SELECT * FROM stocks.gold.nifty_after_us_moves_summary",
    "study_start": "SELECT min(india_date) AS first_day FROM stocks.gold.nifty_after_us_moves",
    "unusual_in": f"""
        SELECT symbol, trade_date, close, return_pct, volume_ratio
        FROM stocks.gold.unusual_volume
        WHERE market = 'IN'
          AND trade_date = (SELECT max(trade_date) FROM stocks.silver.trading_days WHERE market = 'IN')
        ORDER BY volume_ratio DESC LIMIT {UNUSUAL_TOP_N}
    """,
}


def pct(value, digits=2):
    return "n/a" if value is None else f"{value:+.{digits}f}%"


def nice_date(iso):
    return dt.date.fromisoformat(iso).strftime("%a %d %b") if iso else "n/a"


def us_move_bucket(return_pct):
    if return_pct is None:
        return None
    if return_pct >= BIG_MOVE_PCT:
        return "US up big"
    if return_pct <= -BIG_MOVE_PCT:
        return "US down big"
    return "US flat"


def global_lines(rows, today):
    """One line for equities, one for risk gauges; as-of dates shown when older than 4 days."""
    by_id = {r["instrument"]: r for r in rows}

    def stale(r):
        days = (today - dt.date.fromisoformat(r["obs_date"])).days
        return f" (as of {nice_date(r['obs_date'])})" if days > STALE_AFTER_DAYS else ""

    eq = [f"{label} {pct(by_id[i]['change_pct'])}{stale(by_id[i])}" for i, label in GLOBAL_EQUITIES if i in by_id]
    risk = []
    for i, label, kind in GLOBAL_RISK:
        r = by_id.get(i)
        if not r:
            continue
        if kind == "rate":
            change = "" if r["change_abs"] is None else f" ({r['change_abs'] * 100:+.0f} bp)"
            risk.append(f"{label} {r['value']:.2f}%{change}{stale(r)}")
        elif kind == "usd":
            risk.append(f"{label} ${r['value']:.1f} ({pct(r['change_pct'], 1)}){stale(r)}")
        elif kind == "fx":
            risk.append(f"{label} {r['value']:.2f} (as of {nice_date(r['obs_date'])})")
        else:
            change = "" if r["change_abs"] is None else f" ({r['change_abs']:+.1f})"
            risk.append(f"{label} {r['value']:.1f}{change}{stale(r)}")

    lines = []
    if eq:
        lines.append(" · ".join(eq))
    if risk:
        lines.append(" · ".join(risk))
    return lines


def build_message(data, today):
    bars = {r["symbol"]: r for r in data["us_bars"]}
    us_date = data["us_bars"][0]["trade_date"] if data["us_bars"] else None
    lines = [f"<b>Pre-market brief · {today:%a %d %b %Y}</b>", ""]

    if us_date and (today - dt.date.fromisoformat(us_date)).days > STALE_AFTER_DAYS:
        lines += [f"⚠️ Latest US data is from {nice_date(us_date)}; the pipeline may be behind.", ""]

    # US session
    lines.append(f"<b>US session ({nice_date(us_date)})</b>")
    index_parts = [f"{name} ({sym}) {pct(bars[sym]['return_pct'])}" for sym, name in US_INDEX_ETFS if sym in bars]
    lines.append(" · ".join(index_parts) if index_parts else "No US index data.")
    if data["us_breadth"]:
        b = data["us_breadth"][0]
        lines.append(f"Breadth: {int(b['advances']):,} up / {int(b['declines']):,} down · "
                     f"{b['pct_above_ma20']:.0f}% above 20-day avg")
    lines.append("")

    # Global markets
    glob = global_lines(data.get("global", []), today)
    if glob:
        lines += ["<b>Global</b>", *glob, ""]

    # What usually follows
    proxy_return = bars.get(US_PROXY, {}).get("return_pct")
    bucket = us_move_bucket(proxy_return)
    stats = next((r for r in data["summary"] if r["us_move"] == bucket), None)
    lines.append("<b>What usually follows</b>")
    if stats and stats["sessions"]:
        since = nice_date(data["study_start"][0]["first_day"]) if data["study_start"] else "start"
        lines.append(
            f"After a <i>{BUCKET_PHRASE[bucket]}</i> ({int(stats['sessions'])} sessions since {since}): "
            f"{esc(INDIA_PROXY)} opened {pct(stats['avg_india_gap_pct'])} on average, "
            f"closed up on {stats['pct_india_up_days']:.0f}% of days; "
            f"the gap followed the US direction {stats['pct_gap_same_direction_as_us']:.0f}% of the time."
        )
    else:
        lines.append("Not enough history for this kind of US day yet.")
    lines.append("")

    # ADRs overnight
    adr_parts = [f"{sym} {pct(bars[sym]['return_pct'])}" for sym in ADRS if sym in bars]
    if adr_parts:
        lines += ["<b>Indian ADRs overnight</b>", " · ".join(adr_parts), ""]

    # India unusual volume
    unusual = data["unusual_in"]
    in_date = unusual[0]["trade_date"] if unusual else None
    lines.append(f"<b>India unusual volume ({nice_date(in_date)})</b>" if in_date else "<b>India unusual volume</b>")
    if unusual:
        for i, r in enumerate(unusual, 1):
            lines.append(f"{i}. {esc(r['symbol'])} · {r['volume_ratio']:.1f}× avg volume · {pct(r['return_pct'], 1)}")
    else:
        lines.append("None above the threshold.")

    lines += ["", "<i>Sources: NSE bhavcopy, Massive, FRED. Historical tendencies, not advice.</i>"]
    return "\n".join(lines)


def handler(event, context):
    event = event or {}
    data = {name: databricks_sql.query(sql) for name, sql in QUERIES.items()}
    message = build_message(data, dt.datetime.now(IST).date())
    if event.get("dry_run"):
        log.info("Dry run; message not sent:\n%s", message)
        return {"sent": False, "message": message}
    telegram.send(message)
    return {"sent": True, "chars": len(message)}
