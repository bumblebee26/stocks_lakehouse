"""Mobile app export: the dashboard's numbers as one JSON file for the phone app (from 22 Nov 2026).

Runs a few minutes after each Databricks job (EventBridge Scheduler). Reads the dashboard's own SQL, rendered by
the databricks stack into s3://<app bucket>/config/queries.json ({dataset: {sql, fields}}), runs each query on the
SQL warehouse and writes s3://<app bucket>/<secret path>/data/desk.json: {updated_at, datasets: {name: {fields,
rows}}, failed}. A query that fails keeps its previous rows (and is listed in failed), so one slow table never
blanks the app. The app (app/index.html) is served by CloudFront from the same secret path.

Event: {} normally; {"dry_run": true} returns the payload without writing it.

From 24 Nov it also adds Yahoo's full daily history (weekly before the last five years) for the app's charts that have no lakehouse table:
USD/INR, Nifty 50, Sensex, gold and silver (Markets tab) and Accenture, NYSE: ACN (Desk tab). A failed fetch keeps the
previous series. It also reads two small files from paper-trader's state bucket (read-only): the latest opening read
(`morning`, Intraday tab) and which delivery pools are in auto mode (`pool_auto`, Swing and Long tabs).

From 24 Nov the Markets tab also shows Mumbai petrol, diesel and CNG prices (dataset fuel_mumbai), read on every
export from one source, CarDekho's Mumbai fuel page (all three on one page; prices revise at 6 a.m.). A new price is
accepted only within 15% of the last good one; otherwise, or if the page fails, the last good price stays with its
date (the starting values are from 15-16 Nov 2026).
"""

import datetime as dt
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

import boto3

import databricks_sql
import nse_holidays
from common import http_get, log

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
QUERIES_KEY = "config/queries.json"
WORKERS = 4
# app dataset -> (Yahoo symbol, label); five years of daily closes as rows {date, close}
YAHOO_SERIES = {"usdinr_5y": ("INR=X", "USD/INR"), "acn_5y": ("ACN", "Accenture (NYSE: ACN, USD)"),
                # Markets slide (24 Nov): the switchable chart and the India / gold & silver tiles. Gold and silver
                # are international prices in USD per troy ounce; the app turns them into Rs per 10 g / per kg
                # with USD/INR (before Indian duty and GST).
                "nifty_5y": ("^NSEI", "Nifty 50"), "sensex_5y": ("^BSESN", "Sensex"),
                "gold_5y": ("GC=F", "Gold (USD/oz)"), "silver_5y": ("SI=F", "Silver (USD/oz)")}
MORNING_DAYS = 6   # how far back to look for the trader's latest morning (plan / opening read)
MORNING_FIELDS = ["date", "read", "top", "plan", "focus", "gaps_up", "gaps_down", "preopen_count"]
FUEL_URL = "https://www.cardekho.com/fuel-price-in-mumbai-city"
FUEL_SOURCE = "CarDekho"
FUEL_FIELDS = ["item", "rate", "unit", "as_of", "source"]
FUEL_UNITS = {"Petrol": "Rs / litre", "Diesel": "Rs / litre", "CNG": "Rs / kg"}
FUEL_START = {"Petrol": (111.21, "2026-11-16"), "Diesel": (97.83, "2026-11-16"), "CNG": (91.50, "2026-11-15")}
FUEL_BAND = 0.15
# Gold and silver in India (24 Nov): IBJA (India Bullion and Jewellers Association), the daily reference rates
# Indian jewellers and banks quote. Gold per 10 g (999 = 24K, 916 = 22K), silver 999 per kg, excluding 3% GST and
# making charges. The page has two tables (AM and PM) of date, 999, 995, 916, 750, 585, silver 999, platinum 999.
BULLION_URL = "https://ibjarates.com/"
BULLION_SOURCE = "IBJA"
BULLION_FIELDS = ["item", "rate", "unit", "prev", "change_pct", "as_of", "session", "source"]
BULLION_ITEMS = (("Gold 24K (999)", 0, "Rs / 10 g"), ("Gold 22K (916)", 2, "Rs / 10 g"), ("Silver (999)", 5, "Rs / kg"))
BULLION_ROW = re.compile(r"(\d{2})/(\d{2})/(\d{4})" + r"\s+([\d,]{4,9})" * 7)
KEEP_IF_EMPTY = ("sector_board", "sector_stocks")   # holidays and late data: keep the last trading day's


def _s3():
    return boto3.client("s3")


def load_previous(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001 - first run, or unreadable: start empty
        return {}


def run_all(queries, previous, query=None):
    """{name: {fields, rows}} and the names that failed (those keep the previous rows)."""
    query = query or databricks_sql.query
    old = (previous or {}).get("datasets") or {}

    def one(item):
        name, spec = item
        try:
            return name, {"fields": spec.get("fields") or [], "rows": query(spec["sql"])}, None
        except Exception as err:  # noqa: BLE001 - keep the app showing the last good numbers
            log.error("App dataset %s failed: %s", name, err)
            return name, old.get(name) or {"fields": spec.get("fields") or [], "rows": []}, name

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(one, sorted(queries.items())))
    datasets = {name: data for name, data, _ in results}
    failed = [name for _, _, name in results if name]
    return datasets, failed


def thin_old(rows, years=5):
    """Full history for the app's "All" range, kept light: daily for the last `years` years, one close per week
    (the week's last) before that."""
    if not rows:
        return rows
    last = dt.date.fromisoformat(rows[-1]["date"])
    cut = last.replace(year=last.year - years).isoformat()
    old, recent = [r for r in rows if r["date"] < cut], [r for r in rows if r["date"] >= cut]
    weekly = {}
    for r in old:
        weekly[dt.date.fromisoformat(r["date"]).isocalendar()[:2]] = r
    return [weekly[k] for k in sorted(weekly)] + recent


def yahoo_series(previous, fetch=None):
    """{dataset: {fields, rows, label}} for YAHOO_SERIES and the names that failed (those keep the previous rows)."""
    if fetch is None:
        import yahoo

        fetch = yahoo.daily
    old = (previous or {}).get("datasets") or {}
    out, failed = {}, []
    for name, (sym, label) in YAHOO_SERIES.items():
        try:
            bars, _ = fetch(sym, "all")
            rows = thin_old([{"date": b["date"], "close": round(float(b["close"]), 4)} for b in bars if b.get("close")])
            if not rows:
                raise ValueError("no closes")
            out[name] = {"fields": ["date", "close"], "rows": rows, "label": label}
        except Exception as err:  # noqa: BLE001 - keep the last good series
            log.error("Yahoo series %s failed: %s", name, err)
            out[name] = old.get(name) or {"fields": ["date", "close"], "rows": [], "label": label}
            failed.append(name)
    return out, failed


def parse_fuel(html):
    """{fuel: [prices]} from the page text: every rupee amount just after the fuel's name (e.g. "CNG ... ₹91.5"),
    in page order. Menus and car prices can match too, so fuel_rates takes the first one near the last good price."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&#8377;|&#x20[bB]9;", "₹", text)
    text = re.sub(r"\s+", " ", text)
    out = {}
    for fuel in FUEL_UNITS:
        pattern = fuel + r"\b[^₹]{0,160}?(?:₹|Rs\.?)\s*([0-9]{2,3}(?:\.[0-9]{1,2})?)"
        out[fuel] = [float(v) for v in re.findall(pattern, text, flags=re.I)]
    return out


def fuel_rates(previous, fetch=None, today=None):
    """The fuel_mumbai dataset and whether anything failed. Each price keeps its last good value and date unless the
    page gives one within FUEL_BAND of it."""
    today = today or dt.datetime.now(IST).date().isoformat()
    prev_rows = (((previous or {}).get("datasets") or {}).get("fuel_mumbai") or {}).get("rows") or []
    old = {r["item"]: r for r in prev_rows if r.get("item") in FUEL_UNITS}
    try:
        html = (fetch or (lambda: http_get(FUEL_URL, headers={"User-Agent": "Mozilla/5.0"}, retries=2)))()
        got = parse_fuel(html.decode("utf-8", "replace") if isinstance(html, bytes) else html)
    except Exception as err:  # noqa: BLE001 - keep the last good prices
        log.error("Fuel prices failed: %s", err)
        got = {}
    rows, bad = [], False
    for fuel, unit in FUEL_UNITS.items():
        last = old.get(fuel) or {"rate": f"{FUEL_START[fuel][0]:.2f}", "as_of": FUEL_START[fuel][1],
                                 "source": FUEL_SOURCE}
        near = [v for v in got.get(fuel, []) if abs(v / float(last["rate"]) - 1) <= FUEL_BAND]
        if near:
            last = {"rate": f"{near[0]:.2f}", "as_of": today, "source": FUEL_SOURCE}
        else:
            bad = True
            log.error("No %s price within 15%% of %s on the page (found %s); kept the last one", fuel,
                      last["rate"], got.get(fuel))
        rows.append({"item": fuel, "rate": last["rate"], "unit": unit, "as_of": last["as_of"],
                     "source": last["source"]})
    return {"fields": FUEL_FIELDS, "rows": rows}, bad


def parse_bullion(html):
    """{'AM'|'PM': [(iso date, [7 numbers]), newest first]} from IBJA's page (tables in page order: AM, then PM)."""
    text = re.sub(r"<[^>]+>", " ", html or "")
    text = re.sub(r"&nbsp;|\s+", " ", text)
    tables, seen = {"AM": [], "PM": []}, set()
    for m in BULLION_ROW.finditer(text):
        day = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        vals = [float(v.replace(",", "")) for v in m.groups()[3:]]
        tables["PM" if day in seen else "AM"].append((day, vals))
        seen.add(day)
    return tables


def bullion_rates(previous, fetch=None):
    """The bullion_in dataset (IBJA's latest closing rates, PM if published, else AM, with the change from the day
    before) and whether it failed (then the last good rows stay)."""
    old = (((previous or {}).get("datasets") or {}).get("bullion_in")) or {"fields": BULLION_FIELDS, "rows": []}
    try:
        html = (fetch or (lambda: http_get(BULLION_URL, headers={"User-Agent": "Mozilla/5.0"}, retries=2)))()
        tables = parse_bullion(html.decode("utf-8", "replace") if isinstance(html, bytes) else html)
    except Exception as err:  # noqa: BLE001 - keep the last good rates
        log.error("IBJA rates failed: %s", err)
        return old, True
    session = "PM" if tables["PM"] else "AM"
    t = tables[session]
    if not t or not 20_000 < t[0][1][0] < 1_000_000:   # sanity: 24K gold per 10 g in rupees
        log.error("IBJA page had no usable rates (%s rows)", len(t))
        return old, True
    (day, now), before = t[0], (t[1][1] if len(t) > 1 else None)
    rows = [{"item": name, "rate": now[i], "unit": unit, "prev": before[i] if before else None,
             "change_pct": round((now[i] / before[i] - 1) * 100, 2) if before and before[i] else None,
             "as_of": day, "session": session, "source": BULLION_SOURCE} for name, i, unit in BULLION_ITEMS]
    return {"fields": BULLION_FIELDS, "rows": rows}, False


def market_status(previous, fetch=None, today=None):
    """Datasets nse_holidays (NSE's list; the last good one if NSE can't be read) and market_status (one row:
    holiday name or empty, card title and message, next open). The app shows its holiday card only when
    holiday is set (from 24 Nov 2026)."""
    today = today or dt.datetime.now(IST).date()
    rows = nse_holidays.fetch(fetch)
    failed = rows is None
    if failed:
        rows = ((((previous or {}).get("datasets") or {}).get("nse_holidays")) or {}).get("rows") or []
    st = nse_holidays.status(today, rows)
    return ({"fields": ["holiday_date", "description"], "rows": rows},
            {"fields": ["date", "holiday", "title", "message", "next_open"],
             "rows": [{"date": today.isoformat(), "holiday": st["holiday"] or "", "title": st["title"],
                       "message": st["message"], "next_open": st["next_open"]}]}, failed)


def _gaps(pre, up=True, n=5):
    """Top n pre-open gaps up (or down) from preopen/<day>.json's {symbol: {gap_pct, buy_sell}}."""
    rows = [{"symbol": sym, "gap_pct": r.get("gap_pct"), "buy_sell": r.get("buy_sell")}
            for sym, r in (pre or {}).items() if isinstance(r, dict) and r.get("gap_pct") is not None]
    rows = [r for r in rows if (r["gap_pct"] > 0 if up else r["gap_pct"] < 0)]
    return sorted(rows, key=lambda r: -r["gap_pct"] if up else r["gap_pct"])[:n]


def trader_files(s3, bucket, today=None):
    """From paper-trader's state bucket (read-only), the latest trading morning as dataset `morning`: the day plan
    (plans/<day>.json: India VIX, risk per trade, bias, focus list), NSE's pre-open gaps (preopen/<day>.json) and the
    09:25 opening read with the stocks in play (opening/<day>.json); plus which delivery pools are in auto mode
    (state/pool_auto.json) as `pool_auto`."""
    today = today or dt.datetime.now(IST).date()
    morning = {"fields": MORNING_FIELDS, "rows": []}
    if bucket:
        for back in range(MORNING_DAYS):
            day = (today - dt.timedelta(days=back)).isoformat()
            plan = load_previous(s3, bucket, f"plans/{day}.json")
            opening = load_previous(s3, bucket, f"opening/{day}.json")
            if not plan and not opening:
                continue
            pre = (load_previous(s3, bucket, f"preopen/{day}.json") or {}).get("stocks") or {}
            p = (plan or {}).get("plan") or {}
            top = [{"symbol": r.get("symbol"), "score": r.get("score"), "rvol": r.get("rvol"),
                    "gap_pct": r.get("gap_pct"), "first10_pct": r.get("first10_pct"), "news": bool(r.get("news")),
                    "preopen_gap": (r.get("preopen") or {}).get("gap_pct"),
                    "buy_sell": (r.get("preopen") or {}).get("buy_sell")}
                   for r in ((opening or {}).get("stocks") or [])[:5]]
            morning["rows"] = [{
                "date": day, "read": (opening or {}).get("read") or {}, "top": top,
                "plan": {"vix": p.get("india_vix", p.get("vix")), "risk_pct": p.get("risk_per_trade_pct"),
                         "bias": p.get("bias"), "regime": p.get("nifty_regime_words")} if plan else {},
                "focus": (plan or {}).get("focus") or [],
                "gaps_up": _gaps(pre, True), "gaps_down": _gaps(pre, False), "preopen_count": len(pre)}]
            break
    auto = (load_previous(s3, bucket, "state/pool_auto.json") if bucket else {}) or {}
    pools = {"swing": True, "long_term": False, **{k: bool(v) for k, v in auto.items() if k in ("swing", "long_term")}}
    return morning, {"fields": ["pool", "on"], "rows": [{"pool": k, "on": v} for k, v in pools.items()]}


def _pct(v):
    return "" if v is None else f"{v:+.1f}%"


def morning_lines(row):
    """The morning as flat rows (page, section, item, detail) for the dashboard: page 'desk' = the market (India VIX
    and bias, pre-open gaps, the opening read); page 'intraday' = the trader's plan (risk per trade, focus list,
    stocks in play)."""
    if not row:
        return []
    out, p, r = [], row.get("plan") or {}, row.get("read") or {}

    def add(page, sec, item, detail=""):
        out.append({"date": row["date"], "page": page, "section": sec, "item": item, "detail": detail})

    def gap(g):
        return _pct(g["gap_pct"]) + (f", buy/sell {g['buy_sell']:g}x" if g.get("buy_sell") else "")
    if p.get("vix") is not None or p.get("bias"):
        add("desk", "1 Market", f"India VIX {p['vix']:.1f}" if p.get("vix") is not None else "India VIX –",
            "; ".join(x for x in [f"bias: {p['bias']}" if p.get("bias") else "", p.get("regime") or ""] if x))
    for g in row.get("gaps_up") or []:
        add("desk", "2 Pre-open gaps up", g["symbol"], gap(g))
    for g in row.get("gaps_down") or []:
        add("desk", "3 Pre-open gaps down", g["symbol"], gap(g))
    if r.get("nifty_gap_pct") is not None:
        add("desk", "4 Opening read", f"Nifty opened {_pct(r['nifty_gap_pct'])}, {_pct(r.get('nifty_first10_pct'))} in 10 min ({r.get('nifty_gap')})",
            "; ".join(x for x in [f"{r['up_from_open']} stocks up from their open" if r.get("up_from_open") else "",
                                  f"opening range {round(r['median_or_vs_day_range'] * 100)}% of a usual day's"
                                  if r.get("median_or_vs_day_range") is not None else "",
                                  f"India VIX {_pct(r.get('india_vix_change_pct'))}" if r.get("india_vix_change_pct") is not None else ""] if x))
    if p.get("risk_pct") is not None:
        add("intraday", "1 Plan", f"Risk {p['risk_pct']}% per trade", f"set from India VIX {p['vix']:.1f}" if p.get("vix") is not None else "")
    if row.get("focus"):
        add("intraday", "2 Focus", f"{len(row['focus'])} stocks", ", ".join(row["focus"]))
    for i, t in enumerate(row.get("top") or [], 1):
        bits = [f"score {t.get('score')}", f"{t.get('rvol')}x usual volume", f"gap {_pct(t.get('gap_pct'))}"]
        bits += ["news"] if t.get("news") else []
        bits += [f"pre-open {_pct(t['preopen_gap'])}" + (f", buy/sell {t['buy_sell']:g}x" if t.get("buy_sell") else "")] \
            if t.get("preopen_gap") is not None else []
        add("intraday", "3 In play at 09:25", f"{i}. {t.get('symbol')}", " · ".join(bits))
    return out


COUNCIL_METHODS = (("real", "Real decisions"), ("reviewer", "Live reviewer"),
                   ("council", "Council (reviewer + tape + base rate)"), ("control", "Control (reviewer x3)"))


def council_card(s3, bucket):
    """The council test's scorecard (paper-trader council/scorecard.json, written after the close) as rows."""
    card = (load_previous(s3, bucket, "council/scorecard.json") if bucket else None) or {}
    rows = [{"method": label, "n": (card.get(k) or {}).get("n"), "rupees": (card.get(k) or {}).get("rupees"),
             "hit_pct": (card.get(k) or {}).get("hit_pct"), "brier": (card.get(k) or {}).get("brier")}
            for k, label in COUNCIL_METHODS if card.get(k)]
    d = card.get("disagreements") or {}
    return {"fields": ["method", "n", "rupees", "hit_pct", "brier", "as_of", "disagree_n", "council_rs", "reviewer_rs"],
            "rows": [{**r, "as_of": card.get("as_of"), "disagree_n": d.get("n"), "council_rs": d.get("council_rupees"),
                      "reviewer_rs": d.get("reviewer_rupees")} for r in rows]}


def push_volume(name, rows):
    """Copy rows as <name> (JSON lines, overwritten) to the Databricks trader_morning volume for the dashboard. Not
    the landing volume: that one starts a pipeline run on every new file. Never fails the export."""
    vol = os.environ.get("MORNING_VOLUME_PATH")
    if not vol or not rows:
        return False
    try:
        import databricks_volume
        databricks_volume.upload(name, "\n".join(json.dumps(x) for x in rows).encode(), volume=vol)
        return True
    except Exception as err:  # noqa: BLE001
        log.error("Copy of %s to Databricks failed: %s", name, err)
        return False


def push_morning(row):
    """The morning for the dashboard (latest.jsonl in the trader_morning volume)."""
    return bool(row) and push_volume("latest.jsonl", morning_lines(row))


def handler(event, context):
    event = event or {}
    bucket, prefix = os.environ["APP_BUCKET"], os.environ["APP_PREFIX"]
    s3 = _s3()
    queries = json.loads(s3.get_object(Bucket=bucket, Key=QUERIES_KEY)["Body"].read())
    data_key = f"{prefix}/data/desk.json"
    previous = load_previous(s3, bucket, data_key)
    datasets, failed = run_all(queries, previous)
    for name in KEEP_IF_EMPTY:   # e.g. a holiday before the day's tables rebuild: show the last trading day
        old = ((previous or {}).get("datasets") or {}).get(name)
        if name in datasets and not datasets[name]["rows"] and old and old.get("rows"):
            datasets[name] = old
    series, series_failed = yahoo_series(previous)
    datasets.update(series)
    datasets["nse_holidays"], datasets["market_status"], hol_failed = market_status(previous)
    failed += ["nse_holidays"] if hol_failed else []
    datasets["morning"], datasets["pool_auto"] = trader_files(s3, os.environ.get("TRADER_BUCKET"))
    push_morning((datasets["morning"]["rows"] or [None])[0])
    datasets["council"] = council_card(s3, os.environ.get("TRADER_BUCKET"))
    push_volume("council.jsonl", datasets["council"]["rows"])
    datasets["bullion_in"], bullion_failed = bullion_rates(previous)
    failed += ["bullion_in"] if bullion_failed else []
    datasets["fuel_mumbai"], fuel_failed = fuel_rates(previous)
    failed += ["fuel_mumbai"] if fuel_failed else []
    failed += series_failed
    payload = {"updated_at": dt.datetime.now(IST).strftime("%Y-%m-%d %H:%M"), "datasets": datasets,
               "failed": failed}
    body = json.dumps(payload, separators=(",", ":"), default=str).encode()
    if event.get("dry_run"):
        return {"written": False, "bytes": len(body), "failed": failed, "payload": payload}
    s3.put_object(Bucket=bucket, Key=data_key, Body=body, ContentType="application/json",
                  CacheControl="max-age=60")
    log.info("App data written: %d datasets, %d bytes, failed %s", len(datasets), len(body), failed)
    return {"written": True, "bytes": len(body), "failed": failed}
