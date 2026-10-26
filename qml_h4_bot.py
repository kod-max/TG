#!/usr/bin/env python3
"""QML H4 Telegram signal bot. Signal-only; no broker execution."""
from __future__ import annotations
import os, time, json, math, sqlite3, logging, hashlib, html, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Any
import requests

# ============================== CONFIG ==============================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TD_API_KEY_1 = os.getenv("TWELVE_DATA_API_KEY", "")
# Exactly one credential is used; no key rotation or quota pooling.

SYMBOLS = ["XAU/USD", "EUR/USD"]
PATTERN_INTERVAL = "4h"
HISTORY_BARS = 300
SCAN_INTERVAL_SECONDS = 14400        # scan after each H4 close
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "900"))  # 15 min default
SETUP_MAX_AGE_H4_BARS = 18
PIVOT_LEFT = 3
PIVOT_RIGHT = 3
MIN_HEAD_SWEEP_ATR = 0.08
SL_ATR_BUFFER = 0.15
MIN_RR_TP1 = 1.0
MIN_RR_TP2 = 1.5
MIN_RR_TP3 = 2.0
MIN_TARGET_SEPARATION_ATR = 0.25  # merge clustered H4 swing levels, never invent distances
ATR_PERIOD = 14
MAX_API_RETRIES = 2
REQUEST_TIMEOUT_SECONDS = 15
MAX_QUOTE_AGE_SECONDS = int(os.getenv("MAX_QUOTE_AGE_SECONDS", "1200"))
PER_KEY_BUDGET_PER_MINUTE = int(os.getenv("TWELVE_DATA_CREDITS_PER_MINUTE", "6"))
DAILY_API_CREDIT_LIMIT = int(os.getenv("TWELVE_DATA_DAILY_CREDIT_LIMIT", "0"))  # 0 = account limit not configured
DAILY_RESERVE_FRACTION = 0.20
DB_PATH = os.getenv("DB_PATH", "qml_signals.sqlite3")
LOG_PATH = os.getenv("LOG_PATH", "qml_bot.log")
ADMIN_TELEGRAM_IDS = []  # e.g. [123456789]
HTTP_HOST = "0.0.0.0"
HTTP_PORT = int(os.getenv("PORT", "10000"))
HEALTH_STATE = {"bot_loop": "starting", "last_scan_at": None, "last_monitor_at": None, "last_error": None}
HEALTH_LOCK = threading.Lock()

# Do not use multiple accounts/keys to circumvent provider limits. Assign each
# configured credential only as permitted by its account's subscription terms.
API_KEYS = [TD_API_KEY_1]
BASE_URL = "https://api.twelvedata.com"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

logging.basicConfig(level=logging.INFO, handlers=[RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=4), logging.StreamHandler()], format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("qml")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str) -> datetime:
    # Twelve Data timeseries commonly returns YYYY-MM-DD HH:MM:SS without timezone.
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def round_price(symbol: str, value: float) -> float:
    decimals = 2 if symbol == "XAU/USD" else 5
    quantum = Decimal("1").scaleb(-decimals)
    return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))


@dataclass
class Candle:
    dt: datetime
    open: float
    high: float
    low: float
    close: float


def init_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""CREATE TABLE IF NOT EXISTS signals(
        id TEXT PRIMARY KEY, symbol TEXT NOT NULL, direction TEXT NOT NULL,
        status TEXT NOT NULL, qml REAL NOT NULL, entry REAL NOT NULL, sl REAL NOT NULL,
        tp1 REAL NOT NULL, tp2 REAL NOT NULL, tp3 REAL NOT NULL,
        created_at TEXT NOT NULL, last_bar_at TEXT NOT NULL, expires_at TEXT NOT NULL,
        payload TEXT NOT NULL, last_price REAL, last_price_at TEXT,
        entry_at TEXT, tp1_at TEXT, tp2_at TEXT, tp3_at TEXT, sl_at TEXT,
        notified_events TEXT NOT NULL DEFAULT '[]')""")
    con.execute("""CREATE TABLE IF NOT EXISTS diagnostics(
        key TEXT PRIMARY KEY, value INTEGER NOT NULL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS scan_watermarks(
        symbol TEXT PRIMARY KEY, last_closed_bar TEXT NOT NULL)""")
    # Backward-compatible migrations for databases created by v0.1.
    cols = {r[1] for r in con.execute("PRAGMA table_info(signals)").fetchall()}
    for name in ("model_at", "confirmed_at", "sent_at", "invalidated_at"):
        if name not in cols:
            con.execute(f"ALTER TABLE signals ADD COLUMN {name} TEXT")
    con.commit()
    return con


def diag(con: sqlite3.Connection, key: str, amount: int = 1) -> None:
    con.execute("INSERT INTO diagnostics(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=value+excluded.value", (key, amount))
    con.commit()


class RateBudget:
    """Conservative in-process pacing. Actual quota remains account-specific."""
    def __init__(self):
        self.calls: dict[str, list[float]] = {}
    def wait(self, key: str):
        now = time.time()
        arr = [t for t in self.calls.get(key, []) if now-t < 60]
        if len(arr) >= PER_KEY_BUDGET_PER_MINUTE:
            time.sleep(max(0.5, 60-(now-arr[0])))
            now = time.time(); arr = [t for t in arr if now-t < 60]
        arr.append(now); self.calls[key] = arr
        if DAILY_API_CREDIT_LIMIT > 0:
            day = utc_now().date().isoformat()
            key_hash = hashlib.sha256(key.encode()).hexdigest()[:16]
            con = sqlite3.connect(DB_PATH, timeout=10)
            try:
                con.execute("CREATE TABLE IF NOT EXISTS api_usage(day TEXT NOT NULL,key_hash TEXT NOT NULL,credits INTEGER NOT NULL,PRIMARY KEY(day,key_hash))")
                used_row = con.execute("SELECT credits FROM api_usage WHERE day=? AND key_hash=?",(day,key_hash)).fetchone()
                used = used_row[0] if used_row else 0
                cap = int(DAILY_API_CREDIT_LIMIT * (1.0-DAILY_RESERVE_FRACTION))
                if used >= cap:
                    raise RuntimeError("Configured Twelve Data daily credit budget reached")
                con.execute("INSERT INTO api_usage(day,key_hash,credits) VALUES(?,?,1) ON CONFLICT(day,key_hash) DO UPDATE SET credits=credits+1",(day,key_hash))
                con.commit()
            finally:
                con.close()

BUDGET = RateBudget()


def usable_keys() -> list[str]:
    return [k for k in API_KEYS if k and not k.startswith("PASTE_")]


def td_get(endpoint: str, params: dict[str, Any], key: str) -> dict[str, Any]:
    query = dict(params); query["apikey"] = key
    for attempt in range(MAX_API_RETRIES + 1):
        try:
            BUDGET.wait(key)  # count each HTTP attempt, including retries
            r = requests.get(f"{BASE_URL}/{endpoint}", params=query, timeout=REQUEST_TIMEOUT_SECONDS)
            if r.status_code == 429:
                log.warning("Twelve Data rate limit (key redacted), cooldown")
                time.sleep(min(60, 15 * (attempt + 1)))
                if attempt >= MAX_API_RETRIES: raise RuntimeError("Twelve Data 429: retry budget exhausted")
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "error" or "code" in data and "message" in data:
                msg = str(data.get("message", "API error"))
                log.warning("Twelve Data returned API error: %s", msg[:180])
                raise RuntimeError(msg[:180])
            return data
        except (requests.RequestException, ValueError) as e:
            if attempt >= MAX_API_RETRIES: raise RuntimeError(f"Twelve Data request failed: {type(e).__name__}") from e
            time.sleep(2 ** attempt)
    raise RuntimeError("unreachable")


def fetch_candles(symbol: str, interval: str, outputsize: int, key: str) -> list[Candle]:
    data = td_get("time_series", {"symbol": symbol, "interval": interval, "outputsize": outputsize, "timezone": "UTC", "format": "JSON"}, key)
    values = data.get("values")
    if not isinstance(values, list) or not values:
        raise RuntimeError(f"No candle data for {symbol} {interval}")
    candles = [Candle(parse_dt(x["datetime"]), float(x["open"]), float(x["high"]), float(x["low"]), float(x["close"])) for x in values]
    candles.sort(key=lambda c: c.dt)
    # Exclude any currently forming candle. Conservative close-time handling.
    duration = timedelta(hours=4) if interval == "4h" else timedelta(days=1)
    now = utc_now()
    return [c for c in candles if c.dt + duration <= now]


def fetch_price(symbol: str, key: str) -> tuple[float, datetime]:
    # /quote carries last_quote_at; unlike /price it lets us reject stale quotes.
    data = td_get("quote", {"symbol": symbol, "interval": "1min", "timezone": "UTC", "format": "JSON"}, key)
    price = float(data["close"])
    raw_stamp = data.get("last_quote_at") or data.get("timestamp")
    if raw_stamp is None:
        raise RuntimeError(f"Quote timestamp missing for {symbol}")
    stamp = datetime.fromtimestamp(int(raw_stamp), tz=timezone.utc)
    age = (utc_now() - stamp).total_seconds()
    if age < -120 or age > MAX_QUOTE_AGE_SECONDS:
        raise RuntimeError(f"Stale quote for {symbol}; age_seconds={int(age)}")
    return price, stamp


def atr(candles: list[Candle], period: int = ATR_PERIOD) -> float:
    if len(candles) < period + 1: return 0.0
    trs=[]
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i-1]
        trs.append(max(c.high-c.low, abs(c.high-p.close), abs(c.low-p.close)))
    return sum(trs[-period:])/period

@dataclass
class Pivot:
    index: int
    kind: str  # H or L
    price: float
    dt: datetime


def confirmed_pivots(candles: list[Candle], left: int, right: int) -> list[Pivot]:
    out=[]
    # Only pivots with all right-side confirmation bars are returned.
    for i in range(left, len(candles)-right):
        window=candles[i-left:i+right+1]; cur=candles[i]
        is_hi = cur.high >= max(x.high for j,x in enumerate(window) if j != left)
        is_lo = cur.low <= min(x.low for j,x in enumerate(window) if j != left)
        if is_hi: out.append(Pivot(i, "H", cur.high, cur.dt))
        if is_lo: out.append(Pivot(i, "L", cur.low, cur.dt))
    # Normalize to alternating pivots; keep the more extreme point for repeated kind.
    norm=[]
    for p in sorted(out, key=lambda x:(x.index, x.kind)):
        if norm and p.index == norm[-1].index and p.kind != norm[-1].kind:
            continue
        if norm and norm[-1].kind == p.kind:
            prev=norm[-1]
            if (p.kind == "H" and p.price > prev.price) or (p.kind == "L" and p.price < prev.price): norm[-1]=p
        else: norm.append(p)
    return norm


def find_qml(candles: list[Candle], symbol: str) -> list[dict[str, Any]]:
    """Detect QML using only confirmed pivots and a later closed H4 BOS candle.

    Bearish: prior swing high A -> swing low B -> confirmed higher-high head C,
    then an H4 close below B. Bullish is the exact mirror. The QML entry is A.
    """
    pivots = confirmed_pivots(candles, PIVOT_LEFT, PIVOT_RIGHT)
    vol = atr(candles)
    if vol <= 0 or len(pivots) < 3:
        return []
    results: list[dict[str, Any]] = []
    bar_delta = timedelta(hours=4)
    for j in range(2, len(pivots)):
        a, b, c = pivots[j-2:j+1]
        if not (a.index < b.index < c.index):
            continue
        if c.index + PIVOT_RIGHT >= len(candles):
            continue
        c_confirm_index = c.index + PIVOT_RIGHT
        # Do not let the BOS occur before the head pivot itself is confirmed.
        after = candles[c_confirm_index+1:]
        if [a.kind, b.kind, c.kind] == ["H", "L", "H"]:
            if c.price < a.price + MIN_HEAD_SWEEP_ATR * vol:
                continue
            bos = next((x for x in after if x.close < b.price), None)
            if bos:
                results.append({"direction":"SELL", "qml":a.price, "head":c.price,
                    "broken_swing":b.price, "model_dt":a.dt,
                    "confirmed_dt":bos.dt + bar_delta, "bos_dt":bos.dt,
                    "pivot_indices":[a.index,b.index,c.index]})
        elif [a.kind, b.kind, c.kind] == ["L", "H", "L"]:
            if c.price > a.price - MIN_HEAD_SWEEP_ATR * vol:
                continue
            bos = next((x for x in after if x.close > b.price), None)
            if bos:
                results.append({"direction":"BUY", "qml":a.price, "head":c.price,
                    "broken_swing":b.price, "model_dt":a.dt,
                    "confirmed_dt":bos.dt + bar_delta, "bos_dt":bos.dt,
                    "pivot_indices":[a.index,b.index,c.index]})
    # One model per direction/QML/head/BOS close; retain no future-bar assumptions.
    seen=set(); unique=[]
    for x in results:
        key=(x["direction"], round(x["qml"],8), round(x["head"],8), x["bos_dt"].isoformat())
        if key not in seen:
            seen.add(key); unique.append(x)
    return unique


def structural_targets(candles: list[Candle], direction: str, entry: float, sl: float, broken_swing: float, head: float, symbol: str) -> Optional[tuple[float,float,float]]:
    vol=atr(candles)
    risk=abs(entry-sl)
    if risk <= 0: return None
    # Select past confirmed swing levels in trade direction; do not fabricate targets.
    piv=confirmed_pivots(candles, PIVOT_LEFT, PIVOT_RIGHT)
    if direction == "SELL":
        candidates=sorted({p.price for p in piv if p.kind=="L" and p.price < entry}, reverse=True)
        candidates=[x for x in candidates if x < entry and x <= broken_swing]
        # First target is broken structure if below entry; next are lower structural lows.
        candidates=sorted(set([broken_swing]+candidates), reverse=True)
        candidates=[x for x in candidates if x < entry]
    else:
        candidates=sorted({p.price for p in piv if p.kind=="H" and p.price > entry})
        candidates=sorted(set([broken_swing]+candidates))
        candidates=[x for x in candidates if x > entry]
    # Require targets beyond increasing RR floors; if no three genuine levels, reject.
    chosen=[]
    min_gap = max((0.25 if symbol=="XAU/USD" else 0.00001), vol * MIN_TARGET_SEPARATION_ATR)
    for x in candidates:
        if direction=="SELL" and chosen and x >= chosen[-1]: continue
        if direction=="BUY" and chosen and x <= chosen[-1]: continue
        if chosen and abs(x-chosen[-1]) < min_gap: continue
        rr=abs(entry-x)/risk
        floor=[MIN_RR_TP1,MIN_RR_TP2,MIN_RR_TP3][min(len(chosen),2)]
        if rr >= floor:
            chosen.append(x)
            if len(chosen)==3: break
    if len(chosen)<3: return None
    return tuple(round_price(symbol,x) for x in chosen)  # type: ignore



def target_basis(candles: list[Candle], direction: str, targets: tuple[float,float,float], broken_swing: float) -> list[str]:
    """Attach the H4 structural origin of each target for audit/state records."""
    pivots=confirmed_pivots(candles,PIVOT_LEFT,PIVOT_RIGHT)
    expected="L" if direction=="SELL" else "H"
    out=[]
    for i,price in enumerate(targets,1):
        if abs(price-broken_swing) <= (0.01 if direction=="SELL" else 0.00001):
            out.append("BOS: broken swing level")
            continue
        matches=[p for p in pivots if p.kind==expected and abs(p.price-price) <= (0.01 if direction=="SELL" else 0.00001)]
        if matches:
            p=matches[-1]
            out.append(f"Confirmed H4 swing {expected} at {p.dt.strftime('%Y-%m-%d %H:%M')} UTC")
        else:
            out.append("Confirmed H4 structural level")
    return out


def format_signal(s: dict[str, Any]) -> str:
    rr = lambda tp: abs(tp-s["entry"]) / abs(s["entry"]-s["sl"])
    instrument = "ЗОЛОТО" if s["symbol"] == "XAU/USD" else "EUR/USD"
    direction = s["direction"]
    status = "Ожидается вход"
    basis = s.get("target_basis", ["Структура H4"]*3)
    short_basis = ["уровень пробоя" if str(x).startswith("BOS:") else "экстремум H4" for x in basis]
    return ("<b>━━━━━━━━━━━━━━━━━━</b>\n"
      f"<b>QML H4 | {instrument}</b>\n"
      f"<b>НАПРАВЛЕНИЕ: {direction}</b>\n"
      f"Статус: {status}\n"
      "<b>━━━━━━━━━━━━━━━━━━</b>\n\n"
      f"<b>ВХОД:</b> <code>{s['entry']:.{2 if s['symbol']=='XAU/USD' else 5}f}</code>\n"
      f"<b>СТОП:</b> <code>{s['sl']:.{2 if s['symbol']=='XAU/USD' else 5}f}</code>\n\n"
      "<b>ЦЕЛИ:</b>\n"
      f"TP1 — <code>{s['tp1']:.{2 if s['symbol']=='XAU/USD' else 5}f}</code> | RR 1:{rr(s['tp1']):.2f} | {short_basis[0]}\n"
      f"TP2 — <code>{s['tp2']:.{2 if s['symbol']=='XAU/USD' else 5}f}</code> | RR 1:{rr(s['tp2']):.2f} | {short_basis[1]}\n"
      f"TP3 — <code>{s['tp3']:.{2 if s['symbol']=='XAU/USD' else 5}f}</code> | RR 1:{rr(s['tp3']):.2f} | {short_basis[2]}\n\n"
      "<b>━━━━━━━━━━━━━━━━━━</b>\n"
      "Основание: QML H4\n"
      f"ID: <code>{html.escape(s['id'])}</code>\n\n"
      "<i>Сигнал не является исполненным ордером.</i>\n"
      "<b>━━━━━━━━━━━━━━━━━━</b>")


def telegram_send(text: str) -> bool:
    if not BOT_TOKEN or BOT_TOKEN.startswith("PASTE_") or not CHAT_ID or CHAT_ID.startswith("PASTE_"):
        log.info("Telegram not configured; message not sent")
        return False
    r=requests.post(f"{TELEGRAM_URL}/sendMessage", data={"chat_id":CHAT_ID,"text":text,"parse_mode":"HTML","disable_web_page_preview":True}, timeout=REQUEST_TIMEOUT_SECONDS)
    if not r.ok: raise RuntimeError(f"Telegram send failed HTTP {r.status_code}")
    body=r.json()
    if not body.get("ok"): raise RuntimeError("Telegram API rejected message")
    return True


def insert_signal(con: sqlite3.Connection, symbol: str, setup: dict[str,Any],
                  candles: list[Candle], spot_price: float,
                  min_confirmed_after: Optional[datetime] = None) -> Optional[str]:
    direction=setup["direction"]; qml=setup["qml"]; head=setup["head"]; broken=setup["broken_swing"]
    confirmed_dt=setup["confirmed_dt"]
    last=candles[-1]
    # A model must be newly confirmed relative to the persistent scan watermark.
    if min_confirmed_after is not None and confirmed_dt <= min_confirmed_after:
        diag(con,"reject_not_new"); return None
    if utc_now() > confirmed_dt + timedelta(hours=4*SETUP_MAX_AGE_H4_BARS):
        diag(con,"reject_old_model"); return None
    vol=atr(candles)
    sl = head + SL_ATR_BUFFER*vol if direction=="SELL" else head - SL_ATR_BUFFER*vol
    entry=qml
    # QML limit entry is a RETEST: bearish price must still be below QML; bullish above.
    if direction=="SELL" and not (spot_price < entry):
        diag(con,"reject_entry_already_passed"); return None
    if direction=="BUY" and not (spot_price > entry):
        diag(con,"reject_entry_already_passed"); return None
    # If any post-confirmation H4 bar already touched the entry, do not recreate it as pending.
    post=[c for c in candles if c.dt >= setup["bos_dt"]]
    if direction=="SELL" and any(c.high >= entry for c in post):
        diag(con,"reject_entry_touched_historically"); return None
    if direction=="BUY" and any(c.low <= entry for c in post):
        diag(con,"reject_entry_touched_historically"); return None
    targets=structural_targets(candles,direction,entry,sl,broken,head,symbol)
    if not targets: diag(con,"reject_no_three_structural_targets"); return None
    tp1,tp2,tp3=targets
    if direction=="SELL" and not (entry>tp1>tp2>tp3 and entry<sl): return None
    if direction=="BUY" and not (sl<entry<tp1<tp2<tp3): return None
    # If the first target was already traded after BOS, this cannot be a fresh alert.
    if direction=="SELL" and (spot_price <= tp1 or any(c.low <= tp1 for c in post)):
        diag(con,"reject_tp1_pre_signal"); return None
    if direction=="BUY" and (spot_price >= tp1 or any(c.high >= tp1 for c in post)):
        diag(con,"reject_tp1_pre_signal"); return None
    raw=f"{symbol}|{direction}|{setup['bos_dt'].isoformat()}|{qml:.8f}|{head:.8f}"
    sid=hashlib.sha256(raw.encode()).hexdigest()[:12].upper()
    if con.execute("SELECT 1 FROM signals WHERE id=?",(sid,)).fetchone():
        diag(con,"duplicate"); return None
    sent_at=utc_now(); expires=confirmed_dt+timedelta(hours=4*SETUP_MAX_AGE_H4_BARS)
    payload={"symbol":symbol,"direction":direction,"qml":round_price(symbol,qml),"entry":round_price(symbol,entry),
        "sl":round_price(symbol,sl),"tp1":tp1,"tp2":tp2,"tp3":tp3,
        "head":round_price(symbol,head),"broken_swing":round_price(symbol,broken),
        "target_basis":target_basis(candles,direction,targets,broken),
        "model_at":setup["model_dt"].isoformat(),"confirmed_at":confirmed_dt.isoformat(),
        "created_at":sent_at.isoformat(),"id":sid}
    con.execute("""INSERT INTO signals
      (id,symbol,direction,status,qml,entry,sl,tp1,tp2,tp3,created_at,last_bar_at,expires_at,payload,
       last_price,last_price_at,model_at,confirmed_at,sent_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (sid,symbol,direction,"ENTRY PENDING",payload["qml"],payload["entry"],payload["sl"],tp1,tp2,tp3,
       sent_at.isoformat(),last.dt.isoformat(),expires.isoformat(),json.dumps(payload),
       round_price(symbol,spot_price),sent_at.isoformat(),setup["model_dt"].isoformat(),confirmed_dt.isoformat(),None))
    con.commit(); diag(con,"signals_created")
    try:
        if telegram_send(format_signal(payload)):
            con.execute("UPDATE signals SET sent_at=? WHERE id=?",(utc_now().isoformat(),sid)); con.commit()
        else:
            diag(con,"telegram_not_configured")
    except Exception as e:
        log.exception("Signal saved but Telegram send failed: %s", type(e).__name__)
    return sid


def event_message(row: sqlite3.Row, event: str, price: float) -> str:
    symbol = "ЗОЛОТО" if row["symbol"] == "XAU/USD" else "EUR/USD"
    labels = {
        "ENTRY REACHED": "ВХОД ДОСТИГНУТ", "STOP LOSS HIT": "СТОП ДОСТИГНУТ",
        "TP1 HIT": "TP1 ДОСТИГНУТ", "TP2 HIT": "TP2 ДОСТИГНУТ",
        "TP3 HIT": "TP3 ДОСТИГНУТ", "SETUP EXPIRED": "СРОК СИГНАЛА ИСТЁК",
        "SETUP INVALIDATED": "СЕТАП ОТМЕНЁН"
    }
    label = labels.get(event, event)
    direction = row["direction"]
    decimals = 2 if row["symbol"] == "XAU/USD" else 5
    next_target = "\nСледующая цель: TP2" if event == "TP1 HIT" else ("\nСледующая цель: TP3" if event == "TP2 HIT" else "")
    return (f"<b>{symbol} | {label}</b>\n\n"
            f"Цена: <code>{price:.{decimals}f}</code>\n"
            f"Направление: {direction}{next_target}\n"
            f"ID: <code>{html.escape(row['id'])}</code>")


def process_price(con: sqlite3.Connection, row: sqlite3.Row, price: float, stamp: datetime) -> None:
    status=row["status"]; direction=row["direction"]; entry=row["entry"]; sl=row["sl"]
    tp=[row["tp1"],row["tp2"],row["tp3"]]
    sym=row["symbol"]; price=round_price(sym,price); prev=row["last_price"]
    notified=json.loads(row["notified_events"] or "[]")
    def emit(event: str, new_status: str) -> bool:
        nonlocal status
        if event in notified:
            status=new_status
            return True
        try:
            delivered=telegram_send(event_message(row,event,price))
        except Exception as e:
            log.warning("Telegram event delivery failed: %s",type(e).__name__)
            return False
        if not delivered:
            return False
        notified.append(event); status=new_status
        con.execute("UPDATE signals SET status=?,notified_events=? WHERE id=?",(status,json.dumps(notified),row["id"]))
        con.commit()
        return True
    # Ignore a quote older than our last recorded quote; don't rewind state.
    if row["last_price_at"]:
        try:
            if stamp <= parse_dt(row["last_price_at"]):
                return
        except Exception:
            pass
    if status == "ENTRY PENDING" and utc_now() > parse_dt(row["expires_at"]):
        if not emit("SETUP EXPIRED","EXPIRED"):
            return
    elif status == "ENTRY PENDING":
        # Pending order is invalidated if stop/invalidation extreme is crossed, or
        # price reaches TP1 before the QML retest (missed setup, never a late entry).
        invalid = (price >= sl or price <= tp[0]) if direction=="SELL" else (price <= sl or price >= tp[0])
        if invalid:
            if not emit("SETUP INVALIDATED","INVALIDATED"):
                return
        else:
            if prev is not None:
                # Sell limit retest is approached upward; buy limit retest downward.
                touched = (prev < entry <= price) if direction=="SELL" else (prev > entry >= price)
            else:
                touched = price >= entry if direction=="SELL" else price <= entry
            if touched and not emit("ENTRY REACHED","ACTIVE"):
                return
    elif status in ("ACTIVE","TP1 HIT","TP2 HIT"):
        # Check a stop first. A gap across stop and targets is ambiguous; don't invent order.
        stop_hit = price >= sl if direction=="SELL" else price <= sl
        favorable = price < (prev if prev is not None else price) if direction=="SELL" else price > (prev if prev is not None else price)
        if stop_hit:
            if not emit("STOP LOSS HIT","STOP LOSS HIT"):
                return
        elif favorable:
            # A monotonic sampled move may cross multiple sequential targets; send each once.
            reached = [i for i,t in enumerate(tp) if (price <= t if direction=="SELL" else price >= t)]
            for i in reached:
                event=f"TP{i+1} HIT"
                if event not in notified and status in ("ACTIVE","TP1 HIT","TP2 HIT"):
                    next_status = "TP1 HIT" if i==0 else ("TP2 HIT" if i==1 else "TP3 HIT")
                    if not emit(event,next_status):
                        return
            if status=="TP3 HIT":
                pass
    con.execute("""UPDATE signals SET status=?,last_price=?,last_price_at=?,notified_events=?,
      entry_at=CASE WHEN ? IN ('ACTIVE','TP1 HIT','TP2 HIT','TP3 HIT') AND entry_at IS NULL THEN ? ELSE entry_at END,
      tp1_at=CASE WHEN ?='TP1 HIT' AND tp1_at IS NULL THEN ? ELSE tp1_at END,
      tp2_at=CASE WHEN ?='TP2 HIT' AND tp2_at IS NULL THEN ? ELSE tp2_at END,
      tp3_at=CASE WHEN ?='TP3 HIT' AND tp3_at IS NULL THEN ? ELSE tp3_at END,
      sl_at=CASE WHEN ?='STOP LOSS HIT' AND sl_at IS NULL THEN ? ELSE sl_at END,
      invalidated_at=CASE WHEN ?='INVALIDATED' AND invalidated_at IS NULL THEN ? ELSE invalidated_at END WHERE id=?""",
      (status,price,stamp.isoformat(),json.dumps(notified),status,stamp.isoformat(),
       status,stamp.isoformat(),status,stamp.isoformat(),status,stamp.isoformat(),
       status,stamp.isoformat(),status,stamp.isoformat(),row["id"]))
    con.commit()


def command_reply(chat_id: str, text: str):
    if not BOT_TOKEN: return
    requests.post(f"{TELEGRAM_URL}/sendMessage", data={"chat_id":chat_id,"text":text,"parse_mode":"HTML"}, timeout=10)


def handle_updates(con: sqlite3.Connection, offset: int) -> int:
    if not BOT_TOKEN: return offset
    try:
        r=requests.get(f"{TELEGRAM_URL}/getUpdates",params={"offset":offset,"timeout":1,"allowed_updates":json.dumps(["message"])},timeout=5)
        if not r.ok: return offset
        for u in r.json().get("result",[]):
            offset=max(offset,u["update_id"]+1); m=u.get("message",{}); chat=m.get("chat",{}); user=m.get("from",{}); command=m.get("text","").strip()
            cid=str(chat.get("id","")); uid=user.get("id")
            if cid != str(CHAT_ID): continue
            if command.startswith(("/start","/help")):
                command_reply(cid,"<b>QML H4 | Сигналы</b>\n/start, /help — помощь\n/status — состояние бота\n/signals — актуальные сетапы\n/history — последние сигналы")
            elif command.startswith(("/status","/signals","/history")):
                if ADMIN_TELEGRAM_IDS and uid not in ADMIN_TELEGRAM_IDS:
                    command_reply(cid,"Нет доступа к этой команде."); continue
                if command.startswith("/status"):
                    count=con.execute("SELECT COUNT(*) FROM signals WHERE status IN ('ENTRY PENDING','ACTIVE','TP1 HIT','TP2 HIT')").fetchone()[0]
                    command_reply(cid,f"<b>Состояние QML H4</b>\nИнструменты: ЗОЛОТО, EUR/USD\nТаймфрейм: H4\nАктуальных сетапов: {count}")
                elif command.startswith("/signals"):
                    rows=con.execute("SELECT id,symbol,direction,status,entry,sl,tp1,tp2,tp3 FROM signals WHERE status NOT IN ('TP3 HIT','STOP LOSS HIT','INVALIDATED','EXPIRED','CANCELLED') ORDER BY created_at DESC LIMIT 20").fetchall()
                    status_ru={"ENTRY PENDING":"Ожидается вход","ACTIVE":"Вход достигнут","TP1 HIT":"TP1 достигнут","TP2 HIT":"TP2 достигнут"}
                    lines=[]
                    for x in rows:
                        instrument="ЗОЛОТО" if x["symbol"]=="XAU/USD" else "EUR/USD"
                        lines.append(f"<b>{instrument} {x['direction']}</b> — {status_ru.get(x['status'],x['status'])}\nВход <code>{x['entry']}</code> | Стоп <code>{x['sl']}</code>\nTP1 <code>{x['tp1']}</code> · TP2 <code>{x['tp2']}</code> · TP3 <code>{x['tp3']}</code>\nID <code>{html.escape(x['id'])}</code>")
                    command_reply(cid,"<b>Актуальные сетапы</b>\n\n"+("\n\n".join(lines) if lines else "Сейчас актуальных сетапов нет."))
                else:
                    rows=con.execute("SELECT id,symbol,direction,status,created_at FROM signals ORDER BY created_at DESC LIMIT 20").fetchall()
                    lines=[]
                    for x in rows:
                        instrument="ЗОЛОТО" if x["symbol"]=="XAU/USD" else "EUR/USD"
                        lines.append(f"{x['created_at'][:16]} | {instrument} {x['direction']} | {x['status']} | <code>{html.escape(x['id'])}</code>")
                    command_reply(cid,"<b>История сигналов</b>\n"+("\n".join(lines) if lines else "История пока пуста."))
    except Exception as e: log.warning("Telegram polling failed: %s", type(e).__name__)
    return offset


# Explicit mapping only. Keep every symbol on the account whose entitlement was verified.
# Defaults to key 1 for every symbol; change indices only after verifying provider terms.
SYMBOL_KEY_INDEX = {"XAU/USD": 0, "EUR/USD": 0}

def choose_key(symbol: str, index: int = 0) -> str:
    keys = API_KEYS
    if not keys or not any(k and not k.startswith("PASTE_") for k in keys):
        raise RuntimeError("No Twelve Data API key configured")
    idx = SYMBOL_KEY_INDEX.get(symbol, 0)
    if idx < 0 or idx >= len(keys) or not keys[idx] or keys[idx].startswith("PASTE_"):
        raise RuntimeError(f"No configured API credential assigned to {symbol}")
    return keys[idx]


def bot_loop():
    con=init_db()
    with HEALTH_LOCK:
        HEALTH_STATE["bot_loop"] = "running"
    if not usable_keys(): log.error("No Twelve Data key configured. Set TWELVE_DATA_API_KEY.")
    offset=0; last_monitor=0.0; last_scan=0.0
    while True:
        try:
            offset=handle_updates(con,offset)
            now=time.time()
            if usable_keys():
                if now-last_scan >= SCAN_INTERVAL_SECONDS:
                    for i,symbol in enumerate(SYMBOLS):
                        try:
                            key=choose_key(symbol,i)
                            candles=fetch_candles(symbol,PATTERN_INTERVAL,HISTORY_BARS,key)
                            if len(candles) < 80:
                                log.warning("Insufficient closed H4 bars for %s",symbol); continue
                            latest_bar=candles[-1].dt
                            wm=con.execute("SELECT last_closed_bar FROM scan_watermarks WHERE symbol=?",(symbol,)).fetchone()
                            if wm is None:
                                # First launch: establish baseline only. Never emit a backlog of old models.
                                con.execute("INSERT INTO scan_watermarks(symbol,last_closed_bar) VALUES(?,?)",(symbol,latest_bar.isoformat()))
                                con.commit(); diag(con,"startup_watermark_initialized"); continue
                            watermark=parse_dt(wm["last_closed_bar"])
                            if latest_bar <= watermark:
                                continue
                            setups=find_qml(candles,symbol)
                            diag(con,"models_checked",len(setups))
                            # Fresh quote required to determine whether limit entry remains ahead.
                            spot,spot_stamp=fetch_price(symbol,key)
                            for setup in setups:
                                insert_signal(con,symbol,setup,candles,spot,watermark)
                            con.execute("UPDATE scan_watermarks SET last_closed_bar=? WHERE symbol=?",(latest_bar.isoformat(),symbol))
                            con.commit()
                        except Exception as e:
                            log.warning("Scan failed for %s: %s",symbol,type(e).__name__)
                    last_scan=now
                    with HEALTH_LOCK:
                        HEALTH_STATE["last_scan_at"] = utc_now().isoformat()
                if now-last_monitor >= MONITOR_INTERVAL_SECONDS:
                    rows=con.execute("SELECT * FROM signals WHERE status IN ('ENTRY PENDING','ACTIVE','TP1 HIT','TP2 HIT')").fetchall()
                    quote_cache: dict[str, tuple[float,datetime]] = {}
                    for row in rows:
                        try:
                            symbol=row["symbol"]
                            if symbol not in quote_cache:
                                key=choose_key(symbol,SYMBOLS.index(symbol))
                                quote_cache[symbol]=fetch_price(symbol,key)
                            price,stamp=quote_cache[symbol]
                            # Retry a saved-but-undelivered initial signal only while its limit
                            # retest is still genuinely pending and the setup has not expired.
                            if row["sent_at"] is None:
                                if utc_now() > parse_dt(row["expires_at"]):
                                    con.execute("UPDATE signals SET status='EXPIRED' WHERE id=?",(row["id"]))
                                    con.commit(); diag(con,"expired_unsent"); continue
                                still_pending = (row["tp1"] < price < row["entry"]) if row["direction"]=="SELL" else (row["entry"] < price < row["tp1"])
                                if not still_pending:
                                    con.execute("UPDATE signals SET status='INVALIDATED',invalidated_at=? WHERE id=?",(utc_now().isoformat(),row["id"]))
                                    con.commit(); diag(con,"unsent_signal_no_longer_pending"); continue
                                try:
                                    if not telegram_send(format_signal(json.loads(row["payload"]))):
                                        continue
                                    con.execute("UPDATE signals SET sent_at=? WHERE id=?",(utc_now().isoformat(),row["id"]))
                                    con.commit()
                                except Exception as e:
                                    log.warning("Retry of initial signal failed: %s",type(e).__name__)
                                    continue
                            process_price(con,row,price,stamp)
                        except Exception as e:
                            log.warning("Monitor failed for %s: %s",row["symbol"],type(e).__name__)
                    last_monitor=now
                    with HEALTH_LOCK:
                        HEALTH_STATE["last_monitor_at"] = utc_now().isoformat()
            time.sleep(30)
        except KeyboardInterrupt:
            log.info("Stopping QML bot"); break
        except Exception as e:
            with HEALTH_LOCK:
                HEALTH_STATE["last_error"] = type(e).__name__
            log.exception("Main loop error: %s",type(e).__name__); time.sleep(10)
    con.close()



class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health", "/healthz"):
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Not found\n")
            return
        with HEALTH_LOCK:
            state = dict(HEALTH_STATE)
        body = json.dumps({"service": "qml-h4-signal-bot", "status": "ok", **state}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        # Keep health checks out of the trading logs.
        return


def main():
    worker = threading.Thread(target=bot_loop, name="qml-trading-loop", daemon=True)
    worker.start()
    server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), HealthHandler)
    server.daemon_threads = True
    log.info("HTTP health server listening on %s:%s", HTTP_HOST, HTTP_PORT)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        log.info("Stopping QML HTTP service")
    finally:
        server.shutdown()
        server.server_close()
if __name__ == '__main__': main()
