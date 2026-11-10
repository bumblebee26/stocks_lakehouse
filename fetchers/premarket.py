"""Pre-market brief for the Indian session, sent to Telegram at 08:45 IST Mon-Fri.

Reads the gold/silver tables through Databricks SQL and builds one message:

  Key takeaways      4 one-line bullets: US, world, risk, India
  US session         index ETFs (table) + breadth
  World markets      Asia, Europe, Americas, EM: 1-day and 5-day change (table)
  Rates, FX, oil     VIX, US 10Y, Brent, WTI, dollar, USD/INR, USD/JPY, EUR/USD (table)
  India setup        what usually follows, markets most linked to India's open,
                     ADRs overnight (table), unusual volume (table)

Telegram has no table markup, so tables are <pre> blocks (monospaced) kept narrow
enough for a phone screen.

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

US_INDEX_ETFS = [(US_PROXY, "S&P 500"), ("QQQ", "Nasdaq 100"), ("DIA", "Dow"), ("IWM", "Russell 2000")]
ADRS = [("INFY", "INFY"), ("WIT", "WIPRO"), ("HDB", "HDFCBANK"), ("IBN", "ICICIBANK"), ("RDY", "DRREDDY")]

# World markets table: (instrument, label). Japan is the Nikkei index; the rest are US-listed
# country ETFs (USD), which trade during US hours.
WORLD = [
    ("NIKKEI225", "Japan"),
    ("EWY", "Korea"),
    ("FXI", "China"),
    ("EWH", "Hong Kong"),
    ("EWT", "Taiwan"),
    ("VGK", "Europe"),
    ("EWG", "Germany"),
    ("EWU", "UK"),
    ("EWZ", "Brazil"),
    ("EEM", "Emerging mkts"),
    ("INDA", "India (INDA)"),
]

# Rates, FX & commodities table: (instrument, label, how to show the change)
MACRO = [
    ("VIXCLS", "VIX", "pts"),
    ("DGS10", "US 10Y", "bp"),
    ("DCOILBRENTEU", "Brent", "pct"),
    ("DCOILWTICO", "WTI", "pct"),
    ("DTWEXBGS", "Dollar idx", "pct"),
    ("DEXINUS", "USD/INR", "pct"),
    ("DEXJPUS", "USD/JPY", "pct"),
    ("DEXUSEU", "EUR/USD", "pct"),
]

BUCKET_PHRASE = {"US up big": "big US up day", "US down big": "big US down day", "US flat": "flat US day"}


def _in_list(symbols):
    return ", ".join(f"'{s}'" for s in symbols)  # symbols are constants from this file


QUERIES = {
    "us_bars": f"""
        SELECT b.trade_date, b.symbol, b.close, (b.close / b.prev_close - 1) * 100 AS return_pct
        FROM stocks.silver.daily_bars b
        WHERE b.market = 'US'
          AND b.trade_date = (SELECT max(trade_date) FROM stocks.silver.trading_days WHERE market = 'US')
          AND b.symbol IN ({_in_list([s for s, _ in US_INDEX_ETFS] + [a for a, _ in ADRS])})
    """,
    "us_breadth": """
        SELECT trade_date, advances, declines, pct_above_ma20, median_return_pct
        FROM stocks.gold.market_breadth WHERE market = 'US'
        ORDER BY trade_date DESC LIMIT 1
    """,
    "global": f"""
        SELECT instrument, obs_date, value, change_pct, change_5d_pct, change_abs
        FROM stocks.gold.global_snapshot
        WHERE instrument IN ({_in_list([i for i, _ in WORLD] + [i for i, _, _ in MACRO])})
    """,
    "linked": """
        SELECT instrument, corr_with_india_gap, sessions
        FROM stocks.gold.india_vs_global
        WHERE asset_class IN ('equity_index', 'equity_etf') AND instrument NOT IN ('INDA', 'SP500', 'DJIA', 'NASDAQCOM')
        ORDER BY abs(corr_with_india_gap) DESC
        LIMIT 3
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

# Friendly names for the "unavailable" warning when a query fails.
SECTION_NAMES = {
    "us_bars": "US session and ADRs",
    "us_breadth": "US breadth",
    "global": "world markets",
    "linked": "markets linked to India",
    "summary": "what usually follows",
    "study_start": "what usually follows",
    "unusual_in": "India unusual volume",
}


def fetch_all(queries):
    """Run every query; a failed one becomes [] and is reported, so one missing
    table (e.g. a new gold table before its first pipeline run) can't stop the brief."""
    data, failed = {}, []
    for name, sql in queries.items():
        try:
            data[name] = databricks_sql.query(sql)
        except Exception as err:  # noqa: BLE001 - any failure: skip the section, keep the brief
            log.error("Query %s failed: %s", name, err)
            data[name] = []
            failed.append(name)
    return data, failed


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def pct(value, digits=2):
    return "n/a" if value is None else f"{value:+.{digits}f}%"


def num(value, digits=2, sign=True):
    """Number for a table cell: '+1.42', '-0.40', '—' for missing."""
    if value is None:
        return "—"
    return f"{value:+.{digits}f}" if sign else f"{value:,.{digits}f}"


def nice_date(iso, fmt="%a %d %b"):
    return dt.date.fromisoformat(iso).strftime(fmt) if iso else "n/a"


def table(headers, rows, align):
    """Monospaced table in a Telegram <pre> block. align: 'l' or 'r' per column.
    headers=None gives a plain two-column list without a header row."""
    cells = ([headers] if headers else []) + rows
    widths = [max(len(str(r[i])) for r in cells) for i in range(len(align))]

    def fmt(row):
        parts = [str(c).ljust(w) if a == "l" else str(c).rjust(w) for c, w, a in zip(row, widths, align)]
        return "  ".join(parts).rstrip()

    lines = [fmt(r) for r in rows]
    if headers:
        rule = "-" * max(len(fmt(r)) for r in cells)
        lines = [fmt(headers), rule] + lines
    return "<pre>" + esc("\n".join(lines)) + "</pre>"


def us_move_bucket(return_pct):
    if return_pct is None:
        return None
    if return_pct >= BIG_MOVE_PCT:
        return "US up big"
    if return_pct <= -BIG_MOVE_PCT:
        return "US down big"
    return "US flat"


def vix_regime(level):
    if level is None:
        return ""
    if level < 15:
        return "calm"
    if level < 20:
        return "normal"
    if level < 30:
        return "elevated"
    return "high"


def macro_change(row, kind):
    if kind == "bp":
        return "—" if row["change_abs"] is None else f"{row['change_abs'] * 100:+.0f}bp"
    if kind == "pts":
        return num(row["change_abs"], 1)
    return "—" if row["change_pct"] is None else f"{row['change_pct']:+.1f}%"


def macro_value(row, kind):
    value = row["value"]
    if kind == "bp":
        return f"{value:.2f}%"
    if abs(value) < 10:  # e.g. EUR/USD 1.0712: two decimals would hide the move
        return f"{value:.4f}"
    return f"{value:,.2f}"


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
def takeaways(data, bars, glob, stats, bucket):
    out = []
    spy, qqq = bars.get(US_PROXY), bars.get("QQQ")
    if spy:
        nas = f", Nasdaq {pct(qqq['return_pct'])}" if qqq else ""
        out.append(f"<b>US:</b> S&amp;P {pct(spy['return_pct'])}{nas}, a {BUCKET_PHRASE[bucket]}.")

    world = [(label, glob[i]["change_pct"]) for i, label in WORLD
             if i in glob and glob[i]["change_pct"] is not None and i != "INDA"]
    if world:
        low = min(world, key=lambda x: x[1])
        high = max(world, key=lambda x: x[1])
        out.append(f"<b>World:</b> weakest {esc(low[0])} {pct(low[1])}, strongest {esc(high[0])} {pct(high[1])}.")

    risk = []
    if "VIXCLS" in glob:
        v = glob["VIXCLS"]
        change = "" if v["change_abs"] is None else f" ({v['change_abs']:+.1f})"
        risk.append(f"VIX {v['value']:.1f}{change}, {vix_regime(v['value'])}")
    if "DCOILBRENTEU" in glob:
        risk.append(f"Brent ${glob['DCOILBRENTEU']['value']:.0f}")
    if "DEXINUS" in glob:
        risk.append(f"USD/INR {glob['DEXINUS']['value']:.2f}")
    if risk:
        out.append("<b>Risk:</b> " + " · ".join(risk) + ".")

    if stats and stats["sessions"]:
        out.append(f"<b>India:</b> after similar US days, {esc(INDIA_PROXY)} opened "
                   f"{pct(stats['avg_india_gap_pct'])} on average.")
    return out


def build_message(data, today, failed=()):
    bars = {r["symbol"]: r for r in data["us_bars"]}
    glob = {r["instrument"]: r for r in data.get("global", [])}
    us_date = data["us_bars"][0]["trade_date"] if data["us_bars"] else None
    proxy_return = bars.get(US_PROXY, {}).get("return_pct")
    bucket = us_move_bucket(proxy_return)
    stats = next((r for r in data["summary"] if r["us_move"] == bucket), None)

    lines = [f"📊 <b>Pre-market brief</b> · {today:%a %d %b %Y}"]
    if us_date:
        lines.append(f"<i>Markets as of the {nice_date(us_date)} close</i>")
    lines.append("")

    if failed:
        missing = sorted({SECTION_NAMES.get(n, n) for n in failed})
        lines += [f"⚠️ Unavailable today: {esc(', '.join(missing))} (check the Databricks pipeline).", ""]
    expected = today - dt.timedelta(days=1)
    while expected.weekday() >= 5:
        expected -= dt.timedelta(days=1)
    if us_date and dt.date.fromisoformat(us_date) < expected:
        lines += [f"⚠️ US data for {expected:%a %d %b} hasn't arrived (or US markets were closed); "
                  f"the US figures below are from {nice_date(us_date)}.", ""]

    # Key takeaways
    bullets = takeaways(data, bars, glob, stats, bucket) if bucket else []
    if bullets:
        lines += ["🔎 <b>Key takeaways</b>", *[f"• {b}" for b in bullets], ""]

    # US session
    us_rows = [[label, num(bars[s]["return_pct"])] for s, label in US_INDEX_ETFS if s in bars]
    lines.append(f"🇺🇸 <b>US session</b> · {nice_date(us_date)}" if us_date else "🇺🇸 <b>US session</b>")
    if us_rows:
        lines.append(table(["Index (ETF)", "1D %"], us_rows, "lr"))
    else:
        lines.append("No US index data.")
    if data["us_breadth"]:
        b = data["us_breadth"][0]
        lines.append(f"Breadth: {int(b['advances']):,} up / {int(b['declines']):,} down · "
                     f"{b['pct_above_ma20']:.0f}% above 20-day avg")
    lines.append("")

    # World markets
    stale_marks = False
    world_rows = []
    for i, label in WORLD:
        r = glob.get(i)
        if not r:
            continue
        stale = us_date is not None and r["obs_date"] < us_date
        stale_marks |= stale
        world_rows.append([label + ("*" if stale else ""), num(r["change_pct"]), num(r.get("change_5d_pct"))])
    if world_rows:
        lines += ["🌏 <b>World markets</b>", table(["Market", "1D %", "5D %"], world_rows, "lrr")]
        note = "Japan: Nikkei 225. Others: US-listed country ETFs."
        if stale_marks:
            note += " * = earlier session."
        lines += [f"<i>{note}</i>", ""]

    # Rates, FX & commodities
    macro_rows = [[label, macro_value(glob[i], kind), macro_change(glob[i], kind), nice_date(glob[i]["obs_date"], "%d %b")]
                  for i, label, kind in MACRO if i in glob]
    if macro_rows:
        lines += ["💱 <b>Rates, FX &amp; oil</b>", table(["", "Last", "Chg", "As of"], macro_rows, "lrrr"), ""]

    # India setup
    lines.append(f"🇮🇳 <b>India setup</b> ({esc(INDIA_PROXY)})")
    if stats and stats["sessions"]:
        since = nice_date(data["study_start"][0]["first_day"], "%b %Y") if data["study_start"] else "start"
        lines.append(f"After a <i>{BUCKET_PHRASE[bucket]}</i> ({int(stats['sessions'])} sessions since {since}):")
        lines.append(table(None, [
            ["Avg opening gap", pct(stats["avg_india_gap_pct"])],
            ["Closed higher", f"{stats['pct_india_up_days']:.0f}% of days"],
            ["Gap same way as US", f"{stats['pct_gap_same_direction_as_us']:.0f}% of days"],
        ], "lr"))
    else:
        lines.append("Not enough history for this kind of US day yet.")

    linked = [r for r in data.get("linked", []) if r["instrument"] in glob]
    if linked:
        labels = dict(WORLD)
        parts = [f"{esc(labels.get(r['instrument'], r['instrument']))} {pct(glob[r['instrument']]['change_pct'], 1)} "
                 f"(r={r['corr_with_india_gap']:.2f})" for r in linked]
        lines.append("Most linked to India's open: " + " · ".join(parts))
    lines.append("")

    # ADRs
    adr_rows = [[adr, nse, num(bars[adr]["return_pct"])] for adr, nse in ADRS if adr in bars]
    if adr_rows:
        lines += ["🏦 <b>Indian ADRs</b> (New York close)", table(["ADR", "NSE", "1D %"], adr_rows, "llr"), ""]

    # Unusual volume
    unusual = data["unusual_in"]
    in_date = unusual[0]["trade_date"] if unusual else None
    lines.append(f"🔥 <b>Unusual volume</b> · NSE {nice_date(in_date)}" if in_date else "🔥 <b>Unusual volume</b> · NSE")
    if unusual:
        rows = [[str(n), r["symbol"], f"{r['volume_ratio']:.1f}×", num(r["return_pct"], 1)]
                for n, r in enumerate(unusual, 1)]
        lines.append(table(["#", "Symbol", "Volume", "1D %"], rows, "llrr"))
    else:
        lines.append("None above the threshold.")

    lines += ["", "<i>Sources: NSE, Massive, FRED. Historical tendencies, not advice.</i>"]
    return "\n".join(lines)


def handler(event, context):
    event = event or {}
    data, failed = fetch_all(QUERIES)
    message = build_message(data, dt.datetime.now(IST).date(), failed)
    if event.get("dry_run"):
        log.info("Dry run; message not sent:\n%s", message)
        return {"sent": False, "failed_queries": failed, "message": message}
    telegram.send(message)
    return {"sent": True, "failed_queries": failed, "chars": len(message)}
