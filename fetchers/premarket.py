"""Pre-market brief for the Indian session, sent to Telegram at 08:30 IST Mon-Fri, as two messages.

1) The brief, India first:
  Data check        each source's date against the date it should have (from the market's own calendar: the
                    dates of Yahoo's Nifty 50 and S&P 500 daily candles). A source that is missing a day that
                    wasn't a holiday is LEFT OUT and named; an older day is never used in its place. Holidays
                    are stated.
  Key takeaways     India VIX and the risk per trade it implies (1%; 0.75% at 20+; 0.5% at 30+), the Nifty 50
                    regime (NIFTYBEES vs its 20/50/200-day averages), the scanner's count, and global markets
                    in a supporting role: S&P 500, Nasdaq, Brent, USD/INR, live from Yahoo.
  Global shock      only when the S&P 500 moved 2% or more: one line, with what usually followed in India.
  Indian ADRs       New York close of INFY, WIT, HDB, IBN, RDY: evidence for those five stocks only.
  Unusual volume    NSE's last session (only when that session's file has arrived).
2) The technical scanner (Nifty 200): up to 10 strict picks (all six filters; never forced), the closest
   near-misses with the filter each missed, the Nifty 50 regime, and the lists' dates and sources. Each pick has
   indicative levels, NSE and Groww links, and a "Swing buy" button the paper trader confirms before acting.

Before either message, it saves the pre-open snapshot of world markets to the lakehouse (snapshot.py), which starts
the stocks-market-preopen job so the dashboard's Global markets page is current before NSE opens.

Telegram has no table markup, so tables are <pre> blocks kept narrow enough for a phone.
Event: {} for a normal run, {"dry_run": true} to return the messages without sending (and without saving).
"""

import datetime as dt
import os
import re
from zoneinfo import ZoneInfo

import databricks_sql
import snapshot
import telegram
import yahoo
from common import log
from telegram import esc

IST = ZoneInfo("Asia/Kolkata")
INDIA_PROXY = os.environ.get("INDIA_PROXY", "NIFTYBEES")
UNUSUAL_TOP_N = int(os.environ.get("UNUSUAL_TOP_N", "8"))
SHOCK_PCT = float(os.environ.get("SHOCK_PCT", "2.0"))
STRICT_MAX = 10
NEAR_MAX = 5

ADRS = [("INFY", "INFY"), ("WIT", "WIPRO"), ("HDB", "HDFCBANK"), ("IBN", "ICICIBANK"), ("RDY", "DRREDDY")]
CALENDARS = {"NSE": "^NSEI", "US": "^GSPC"}
LIVE = [("^GSPC", "S&P 500"), ("^IXIC", "Nasdaq"), ("BZ=F", "Brent"), ("INR=X", "USD/INR"),
        ("^INDIAVIX", "India VIX")] + [(a, a) for a, _ in ADRS]

# Lakehouse sources the brief uses: (name in gold.data_freshness, label, calendar it follows)
FRESHNESS = [("nse_prices", "NSE prices", "NSE"), ("nse_delivery", "NSE delivery %", "NSE"),
             ("technical_scanner", "Scanner", "NSE")]

QUERIES = {
    "freshness": "SELECT source, CAST(latest_date AS STRING) AS latest_date FROM stocks.gold.data_freshness",
    # India VIX from NSE's own index file (19:30 the evening before); Yahoo's ^INDIAVIX is only the backup
    "india_vix": """
        SELECT CAST(trade_date AS STRING) AS trade_date, close, change_pct
        FROM stocks.silver.nse_index_daily WHERE lower(index_name) = 'india vix'
        ORDER BY trade_date DESC LIMIT 1
    """,
    "summary": "SELECT * FROM stocks.gold.nifty_after_us_moves_summary",
    "unusual_in": f"""
        SELECT symbol, CAST(trade_date AS STRING) AS trade_date, close, return_pct, volume_ratio
        FROM stocks.gold.unusual_volume
        WHERE market = 'IN'
          AND trade_date = (SELECT max(trade_date) FROM stocks.silver.trading_days WHERE market = 'IN')
        ORDER BY volume_ratio DESC LIMIT {UNUSUAL_TOP_N}
    """,
    "screen": """
        SELECT symbol, company, CAST(trade_date AS STRING) AS trade_date, list_type, rank, close, setup, stance,
               rsi14, macd, macd_signal, macd_cross_up, adx14, adx_ok, above_mas, close > sma20 AS above20,
               close > sma50 AS above50, close > sma200 AS above200, vol_ratio, deliv_per, fno, entry_low,
               entry_high, trigger, stop, stop_basis, target1, target1_basis, target2, rr, misses, filters_passed,
               CAST(nifty200_as_of AS STRING) AS nifty200_as_of, CAST(fno_as_of AS STRING) AS fno_as_of,
               regime_close, regime_rsi, regime_sma20, regime_sma50, regime_sma200, regime_adx
        FROM stocks.gold.technical_screen
        WHERE list_type IN ('strict', 'near-miss')
        ORDER BY list_type DESC, rank
    """,
}

SECTION_NAMES = {"freshness": "data check", "summary": "what usually follows a shock",
                 "unusual_in": "India unusual volume", "screen": "technical scanner"}


def fetch_all(queries):
    """Run every query; a failed one becomes [] and is reported, so one missing table can't stop the brief."""
    data, failed = {}, []
    for name, sql in queries.items():
        try:
            data[name] = databricks_sql.query(sql)
        except Exception as err:  # noqa: BLE001 - any failure: skip the section, keep the brief
            log.error("Query %s failed: %s", name, err)
            data[name] = []
            failed.append(name)
    return data, failed


def fetch_live(today):
    """Yahoo: each market's last session date (the calendar) and the live figures. Missing ones are left out."""
    sessions, live = {}, {}
    for mkt, sym in CALENDARS.items():
        try:
            last, _ = yahoo.last_session(sym, today)
            sessions[mkt] = last["date"] if last else None
        except Exception as err:  # noqa: BLE001 - fall back to the previous weekday
            log.warning("calendar %s unavailable: %s", sym, err)
            sessions[mkt] = None
    for sym, _ in LIVE:
        try:
            ch = yahoo.change(sym, today)
        except Exception as err:  # noqa: BLE001 - that figure is left out
            log.warning("quote %s unavailable: %s", sym, err)
            ch = None
        if ch:
            live[sym] = ch
    return sessions, live


# ---------------------------------------------------------------------------
# Calendar and freshness
# ---------------------------------------------------------------------------
def previous_weekday(d):
    d -= dt.timedelta(days=1)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d


def expected_dates(today, sessions):
    """{market: (expected session date, [weekday holidays between it and today], from_calendar)}."""
    out = {}
    for mkt in CALENDARS:
        iso = sessions.get(mkt)
        if iso:
            exp = dt.date.fromisoformat(iso)
            holidays, d = [], previous_weekday(today)
            while d > exp:
                holidays.append(d)
                d = previous_weekday(d)
            out[mkt] = (exp, holidays, True)
        else:
            out[mkt] = (previous_weekday(today), [], False)
    return out


def freshness(data, expected):
    """{source: {'label', 'latest', 'expected', 'fresh'}} for the lakehouse sources the brief uses."""
    latest = {r["source"]: r["latest_date"] for r in data.get("freshness", [])}
    out = {}
    for src, label, mkt in FRESHNESS:
        exp = expected[mkt][0]
        got = latest.get(src)
        out[src] = {"label": label, "latest": got, "expected": exp,
                    "fresh": bool(got) and dt.date.fromisoformat(got[:10]) >= exp}
    return out


def with_nse_vix(live, data):
    """India VIX from NSE's index file (gold standard) unless Yahoo has a newer day; Yahoo only as the backup."""
    rows = data.get("india_vix") or []
    r = rows[0] if rows else None
    if not r or r.get("close") is None or not r.get("trade_date"):
        return live
    yahoo_row = live.get("^INDIAVIX")
    if yahoo_row and yahoo_row["date"] > r["trade_date"][:10]:
        return live
    change = r.get("change_pct")
    return {**live, "^INDIAVIX": {"date": r["trade_date"][:10], "close": float(r["close"]),
                                  "change_pct": float(change) if change is not None else 0.0, "source": "NSE"}}


def fresh_live(live, sym, expected_day):
    """A live Yahoo figure only counts if it is from the session it should be from."""
    r = live.get(sym)
    return r if r and dt.date.fromisoformat(r["date"]) >= expected_day else None


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def pct(value, digits=2):
    return "n/a" if value is None else f"{value:+.{digits}f}%"


def num(value, digits=2, sign=True):
    if value is None:
        return "—"
    return f"{value:+.{digits}f}" if sign else f"{value:,.{digits}f}"


def nice_date(d, fmt="%a %d %b"):
    if not d:
        return "n/a"
    d = dt.date.fromisoformat(d[:10]) if isinstance(d, str) else d
    return d.strftime(fmt)


def table(headers, rows, align):
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


def vix_risk(level):
    """India VIX -> (regime, risk per trade %)."""
    if level >= 30:
        return "high", 0.5
    if level >= 20:
        return "elevated", 0.75
    if level >= 15:
        return "normal", 1.0
    return "calm", 1.0


def rs(x):
    return "—" if x is None else f"{x:,.2f}" if x < 1000 else f"{x:,.1f}"


def groww_slug(company):
    """Groww names stock pages after the company: 'Dr. Reddy's Laboratories Ltd.' -> 'dr-reddys-laboratories-ltd'."""
    s = (company or "").lower().replace("&", " ").replace("'", "").replace("’", "").replace(".", " ")
    s = re.sub(r"\blimited\b", "ltd", s)
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def links(symbol, company):
    nse = f'<a href="https://www.nseindia.com/get-quotes/equity?symbol={esc(symbol).replace("&amp;", "%26")}">NSE</a>'
    slug = groww_slug(company)
    return nse + (f' · <a href="https://groww.in/stocks/{slug}">Groww</a>' if slug else "")


# ---------------------------------------------------------------------------
# Message 1: the brief
# ---------------------------------------------------------------------------
def data_check(expected, fresh, live, failed):
    lines = []
    for mkt, name in (("NSE", "NSE"), ("US", "US")):
        exp, hols, from_cal = expected[mkt]
        if hols:
            lines.append(f"🗓 {name} closed {', '.join(f'{d:%a %d %b}' for d in reversed(hols))} (holiday); "
                         f"last session {exp:%a %d %b}.")
        if not from_cal:
            lines.append(f"⚠️ {name} calendar unavailable; assuming {exp:%a %d %b} was a session.")
    ok = [f"{f['label']} {nice_date(f['latest'])}" for f in fresh.values() if f["fresh"]]
    if ok:
        lines.append("✅ " + " · ".join(ok) + f" · live quotes {dt.datetime.now(IST):%H:%M} IST")
    for f in fresh.values():
        if not f["fresh"] and "freshness" not in failed:
            have = f"latest {nice_date(f['latest'])}" if f["latest"] else "none yet"
            lines.append(f"⚠️ {f['label']} for {f['expected']:%a %d %b} hasn't arrived ({have}): left out, "
                         "no older day used.")
    us_exp = expected["US"][0]
    missing = [label for sym, label in LIVE[:5] if not fresh_live(live, sym, us_exp if sym != "^INDIAVIX"
                                                                    else expected["NSE"][0])]
    if missing:
        lines.append(f"⚠️ Not available this morning: {esc(', '.join(missing))}: left out.")
    if failed:
        names = sorted({SECTION_NAMES.get(n, n) for n in failed})
        lines.append(f"⚠️ Unavailable today: {esc(', '.join(names))} (check the Databricks pipeline).")
    return lines


def regime_words(r):
    if not r or r.get("regime_close") is None:
        return None
    c = r["regime_close"]
    above = [n for n, k in (("20", "regime_sma20"), ("50", "regime_sma50"), ("200", "regime_sma200"))
             if r.get(k) is not None and c > r[k]]
    if len(above) == 3:
        where = "above its 20/50/200-day averages (uptrend)"
    elif not above:
        where = "below its 20/50/200-day averages (downtrend): fewer longs"
    else:
        where = f"above only its {'/'.join(above)}-day average{'s' if len(above) > 1 else ''} (mixed)"
    rsi = f", RSI {r['regime_rsi']:.0f}" if r.get("regime_rsi") is not None else ""
    adx = f", ADX {r['regime_adx']:.0f}" if r.get("regime_adx") is not None else ""
    return f"Nifty 50 ({esc(INDIA_PROXY)} {rs(c)}) {where}{rsi}{adx}."


def build_brief(data, live, expected, fresh, today, failed=()):
    us_exp, nse_exp = expected["US"][0], expected["NSE"][0]
    lines = [f"📊 <b>Pre-market brief</b> · {today:%a %d %b %Y}",
             f"<i>NSE last session {nse_exp:%a %d %b} · US last session {us_exp:%a %d %b}</i>", ""]
    lines += data_check(expected, fresh, live, failed) + [""]

    screen = data.get("screen", []) if fresh["technical_scanner"]["fresh"] else []
    strict = [r for r in screen if r["list_type"] == "strict"]
    near = [r for r in screen if r["list_type"] == "near-miss"]

    bullets = []
    vix = fresh_live(live, "^INDIAVIX", nse_exp)
    if vix:
        regime, risk = vix_risk(vix["close"])
        bullets.append(f"<b>Risk:</b> India VIX {vix['close']:.1f} ({vix['change_pct']:+.1f}%), {regime}: "
                       f"{risk:g}% of the pool at risk per trade.")
    reg = regime_words(screen[0]) if screen else None
    if reg:
        bullets.append(f"<b>India:</b> {reg}")
    if fresh["technical_scanner"]["fresh"] and "screen" not in failed:
        bullets.append(f"<b>Scanner:</b> {len(strict)} strict pick{'s' if len(strict) != 1 else ''}, "
                       f"{len(near)} near-miss{'es' if len(near) != 1 else ''} (next message).")
    glob = []
    for sym, label in (("^GSPC", "S&amp;P"), ("^IXIC", "Nasdaq")):
        r = fresh_live(live, sym, us_exp)
        if r:
            glob.append(f"{label} {pct(r['change_pct'])}")
    for sym, label, fmt in (("BZ=F", "Brent", "${:.0f}"), ("INR=X", "USD/INR", "{:.2f}")):
        r = fresh_live(live, sym, us_exp)
        if r:
            glob.append(f"{label} {fmt.format(r['close'])} ({r['change_pct']:+.1f}%)")
    if glob:
        bullets.append(f"<b>Global</b> (supporting only, {us_exp:%a}): " + " · ".join(glob) + ".")
    if bullets:
        lines += ["🔎 <b>Key takeaways</b>", *[f"• {b}" for b in bullets], ""]

    spx = fresh_live(live, "^GSPC", us_exp)
    if spx and abs(spx["change_pct"]) >= SHOCK_PCT:
        side = "down" if spx["change_pct"] < 0 else "up"
        stats = next((r for r in data.get("summary", []) if r["us_move"] == f"US {side} big"), None)
        hist = (f" After big US {side} days, {esc(INDIA_PROXY)} opened {pct(stats['avg_india_gap_pct'])} on "
                f"average ({int(stats['sessions'])} sessions)." if stats and stats.get("sessions") else "")
        lines += [f"🌐 <b>Global shock:</b> S&amp;P 500 {pct(spx['change_pct'])} on {us_exp:%a}. Expect a "
                  f"{'weak' if side == 'down' else 'strong'} open; let the first 15 minutes settle.{hist}", ""]

    adr_rows = [[adr, nse, num(r["change_pct"])] for adr, nse in ADRS if (r := fresh_live(live, adr, us_exp))]
    if adr_rows:
        lines += [f"🏦 <b>Indian ADRs</b> (New York, {us_exp:%a}) · evidence for these stocks only",
                  table(["ADR", "NSE", "1D %"], adr_rows, "llr"), ""]

    if fresh["nse_prices"]["fresh"]:
        unusual = [r for r in data.get("unusual_in", []) if r["trade_date"][:10] >= nse_exp.isoformat()]
        lines.append(f"🔥 <b>Unusual volume</b> · NSE {nse_exp:%a %d %b}")
        if unusual:
            rows = [[str(n), r["symbol"], f"{r['volume_ratio']:.1f}×", num(r["return_pct"], 1)]
                    for n, r in enumerate(unusual, 1)]
            lines.append(table(["#", "Symbol", "Volume", "1D %"], rows, "llrr"))
        else:
            lines.append("None above the threshold.")
        lines.append("")

    lines.append("<i>Sources: NSE (bhavcopy, India VIX), Yahoo Finance (live quotes, market calendars). "
                 "Tendencies and indicative figures, not advice.</i>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Message 2: the technical scanner
# ---------------------------------------------------------------------------
def _pick_lines(n, r):
    dma = "/".join(k for k, f in (("20", "above20"), ("50", "above50"), ("200", "above200")) if r.get(f)) or "none"
    macd = f"MACD {r['macd']:.2f}/{r['macd_signal']:.2f}" + (" crossed ↑" if r.get("macd_cross_up") else "")
    deliv = f" · Deliv {r['deliv_per']:.0f}%" if r.get("deliv_per") is not None else ""
    out = [f"<b>{n}. {esc(r['symbol'])}</b> Rs {rs(r['close'])} · {esc(r['setup'])} · <b>{esc(r['stance'])}</b>",
           f"RSI {r['rsi14']:.0f} · {macd} · ADX {r['adx14']:.0f}{' rising' if r.get('adx_ok') else ''} · above DMA {dma} · Vol {r['vol_ratio']:.1f}×"
           f"{deliv} · {'F&amp;O' if r.get('fno') else 'not F&amp;O'}"]
    if r.get("entry_high") is not None and r.get("stop") is not None:
        trig = f" · trigger {rs(r['trigger'])}" if r.get("trigger") is not None else ""
        out.append(f"Entry {rs(r['entry_low'])}–{rs(r['entry_high'])}{trig} · stop {rs(r['stop'])} "
                   f"({esc(r['stop_basis'])}) · T1 {rs(r['target1'])} · T2 {rs(r['target2'])} · R:R {r['rr']:.1f}")
    out.append(links(r["symbol"], r.get("company")))
    return out


def build_scanner(data, expected, fresh, failed=()):
    """(message, buttons). Always says something, including when the list is empty or the data is missing."""
    nse_exp = expected["NSE"][0]
    head = f"🧭 <b>Technical scanner</b> · Nifty 200 · NSE close {nse_exp:%a %d %b}"
    if "screen" in failed:
        return f"{head}\n⚠️ Scanner unavailable today (check the Databricks pipeline).", []
    f = fresh["technical_scanner"]
    if not f["fresh"]:
        have = nice_date(f["latest"]) if f["latest"] else "none yet"
        return (f"{head}\n⚠️ The scan for {nse_exp:%a %d %b} hasn't run (latest {have}). Left out today; an "
                f"older day's scan isn't used."), []
    screen = data.get("screen", [])
    strict = [r for r in screen if r["list_type"] == "strict"][:STRICT_MAX]
    near = [r for r in screen if r["list_type"] == "near-miss"][:NEAR_MAX]
    lines = [head]
    reg = regime_words(screen[0]) if screen else None
    if reg:
        lines.append(f"<i>{reg}</i>")
    lines.append("")
    if strict:
        lines.append("<b>Strict picks</b> (all six: above 20/50/200 DMA, RSI 50-65, MACD above signal, "
                     "ADX &gt;20 rising, volume ≥1.5×, R:R ≥2)")
        for n, r in enumerate(strict, 1):
            lines += _pick_lines(n, r) + [""]
    else:
        lines += ["No stock passed all six filters today; none forced.", ""]
    if near:
        lines.append("<b>Near-misses</b> (watch, don't act)")
        for r in near:
            lines.append(f"• {esc(r['symbol'])} Rs {rs(r['close'])}, {esc(r['stance'])}: missed "
                         f"{esc(r['misses'] or 'not extended check')}")
        lines.append("")
    asof = screen[0] if screen else {}
    lines.append(f"<i>Levels are indicative (swing points and ATR), never a guaranteed return. No chasing: RSI "
                 f"&gt;70, MFI &gt;80 or &gt;8% above the 20 DMA is wait/avoid. Lists: Nifty 200 as of "
                 f"{nice_date(asof.get('nifty200_as_of'), '%d %b %Y')} (niftyindices.com), F&amp;O as of "
                 f"{nice_date(asof.get('fno_as_of'), '%d %b %Y')} (NSE fo_mktlots). Prices: NSE bhavcopy, "
                 f"split/bonus-adjusted; delivery: NSE sec_bhavdata_full.</i>")
    buttons = []
    for r in strict:
        if r.get("stop") is None or r.get("target2") is None:
            continue
        data_ = f"sw:{r['symbol']}:{r['stop']:.2f}:{r['target2']:.2f}"
        if len(data_.encode()) <= 64:
            buttons.append([(f"Swing buy {r['symbol']} (asks first)", data_)])
    return "\n".join(lines), buttons


def chunks(text, limit=telegram.MAX_LENGTH - 96):
    """Split a long message at blank lines (between picks) so no part is cut mid-tag."""
    parts, cur = [], ""
    for block in text.split("\n\n"):
        add = (cur + "\n\n" + block) if cur else block
        if len(add) > limit and cur:
            parts.append(cur)
            cur = block
        else:
            cur = add
    parts.append(cur)
    return parts


def save_snapshot(now, dry_run):
    """The pre-open snapshot for the lakehouse (snapshot.py), first so the market run can start at once.
    It never stops the brief: a failure is logged and reported."""
    try:
        if dry_run:
            return {"rows": len(snapshot.collect(now)), "key": None}
        return snapshot.save(now)
    except Exception as err:  # noqa: BLE001
        log.warning("pre-open snapshot failed: %s", err)
        return {"rows": 0, "key": None, "error": str(err)[:200]}


def handler(event, context):
    event = event or {}
    now = dt.datetime.now(IST)
    today = now.date()
    snap = save_snapshot(now, bool(event.get("dry_run")))
    data, failed = fetch_all(QUERIES)
    sessions, live = fetch_live(today)
    live = with_nse_vix(live, data)
    expected = expected_dates(today, sessions)
    fresh = freshness(data, expected)
    brief = build_brief(data, live, expected, fresh, today, failed)
    scan, buttons = build_scanner(data, expected, fresh, failed)
    if event.get("dry_run"):
        log.info("Dry run; messages not sent:\n%s\n\n%s", brief, scan)
        return {"sent": False, "failed_queries": failed, "message": brief, "scanner": scan, "buttons": buttons,
                "snapshot": snap}
    telegram.send(brief)
    parts = chunks(scan)
    for i, part in enumerate(parts):
        last = i == len(parts) - 1
        telegram.send(part if len(parts) == 1 else f"{part}\n<i>({i + 1}/{len(parts)})</i>",
                      buttons=buttons if last else None)
    return {"sent": True, "failed_queries": failed, "chars": len(brief) + len(scan),
            "scanner_parts": len(parts), "swing_buttons": len(buttons), "snapshot": snap}
