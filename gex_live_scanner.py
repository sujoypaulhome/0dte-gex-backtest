"""
GEX Wall Rejection 0DTE Live Scanner
=====================================
Runs each trading morning (9:30-10:30 ET). Detects momentum exhaustion via
ATR spike on 5-min candles, then alerts with exact credit spread trades to
place manually in TradeStation.

Flow:
  1. Barchart IV rank pre-scan → find high-IV tickers to add to scan list
  2. Polygon GEX wall detection → call wall / put wall per ticker
  3. Fetch 5-min bars + compute ATR(14)
  4. Poll every 60s for ATR spikes in 9:35-10:30 ET window
  5. On spike → send trade alert via ntfy.sh + console + sound
"""

import os
import sys
import re
import csv
import time
import json
import logging
import urllib.parse
import winsound
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests as req
from dotenv import load_dotenv

# ============================================================================
# Config
# ============================================================================

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

POLYGON_KEY = os.environ.get("POLYGON_API_KEY", "")
BARCHART_USER = os.environ.get("BARCHART_USER", "")
BARCHART_PASS = os.environ.get("BARCHART_PASS", "")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "skp-gamma-alerts")

if not POLYGON_KEY:
    sys.exit("ERROR: Set POLYGON_API_KEY in .env")

# File logging — all print output goes to scanner.log + console
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
_log = logging.getLogger("gex_scanner")

# Redirect print → logger so all output goes to both console and file
_orig_print = print
def print(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    _log.info(msg)

POLYGON_BASE = "https://api.polygon.io"
ET = ZoneInfo("America/New_York")

# Core tickers (always scanned, profitable in backtest)
CORE_TICKERS = ["QQQ", "IWM", "TSLA", "NVDA", "AMZN"]

# 0DTE schedules
DAILY_0DTE = {"SPY", "QQQ", "IWM"}
MWF_0DTE = {"TSLA", "NVDA", "AMZN", "META", "AAPL", "MSFT", "AVGO", "GOOGL", "IBIT"}

# Spread widths
SPREAD_WIDTHS = {
    "QQQ": 2.0, "IWM": 1.0,
    "TSLA": 5.0, "NVDA": 5.0, "AMZN": 5.0,
}
DEFAULT_SPREAD_WIDTH = 5.0

# ATR settings
ATR_PERIODS = 14
ATR_SPIKE_MULTIPLIER = 2.0
SCAN_START = (9, 35)
SCAN_END = (10, 30)
POLL_INTERVAL_SEC = 60

# Credit estimate fallback
CREDIT_PER_DOLLAR = 0.30
MULTIPLIER = 100

# Max dynamic tickers to add from IV scan
MAX_DYNAMIC_TICKERS = 5
MIN_OPTION_VOLUME = 1000


# ============================================================================
# Polygon HTTP helpers (with 429 retry)
# ============================================================================

def _poly_get(url: str, params: Dict = None) -> Dict:
    if params is None:
        params = {}
    params["apiKey"] = POLYGON_KEY
    last = None
    for attempt in range(5):
        r = req.get(url, params=params, timeout=30)
        last = r
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                pass
        if r.status_code == 429:
            wait = 15 * (attempt + 1)
            print(f"    Rate limited, waiting {wait}s...")
            time.sleep(wait)
        else:
            time.sleep(1)
    raise RuntimeError(f"Polygon GET failed {last.status_code}: {last.text[:200]}")


def _poly_next(next_url: str) -> Dict:
    if "apiKey=" not in next_url:
        sep = "&" if "?" in next_url else "?"
        next_url = f"{next_url}{sep}apiKey={POLYGON_KEY}"
    last = None
    for attempt in range(5):
        r = req.get(next_url, timeout=30)
        last = r
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                pass
        if r.status_code == 429:
            wait = 15 * (attempt + 1)
            print(f"    Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        time.sleep(1)
    raise RuntimeError(f"Polygon NEXT failed {last.status_code}: {last.text[:200]}")


# ============================================================================
# Notifications
# ============================================================================

def ntfy_send(title: str, body: str, priority: str = "default", tags: str = ""):
    """Send push notification via ntfy.sh."""
    if not NTFY_TOPIC:
        return
    try:
        headers = {"Title": title, "Priority": priority}
        if tags:
            headers["Tags"] = tags
        req.post(f"https://ntfy.sh/{NTFY_TOPIC}", headers=headers,
                 data=body.encode("utf-8"), timeout=10)
    except Exception as e:
        print(f"  ntfy send failed: {e}")


def beep():
    """Play alert sound on Windows."""
    try:
        winsound.Beep(1000, 500)
    except Exception:
        pass


# ============================================================================
# Phase 1: Barchart IV Rank Pre-Scan
# ============================================================================

def barchart_fetch_iv_rank() -> List[Dict]:
    """
    Login to Barchart, fetch top 50 tickers by IV rank (1yr).
    Returns list of dicts: [{symbol, name, iv_rank, iv_pctl, volume}, ...]
    """
    if not BARCHART_USER or not BARCHART_PASS:
        print("  Barchart credentials not set, skipping IV scan")
        return []

    session = req.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    # 1) GET login page for CSRF token
    try:
        r = session.get("https://www.barchart.com/login", timeout=15)
    except Exception as e:
        print(f"  Barchart login page failed: {e}")
        return []

    match = re.search(r'name="_token"\s+value="([^"]+)"', r.text)
    if not match:
        print("  Could not find CSRF token on Barchart login page")
        return []
    csrf = match.group(1)

    # 2) POST login
    r = session.post("https://www.barchart.com/login", data={
        "_token": csrf, "email": BARCHART_USER,
        "password": BARCHART_PASS, "remember": "on",
    }, headers={
        "Referer": "https://www.barchart.com/login",
        "Origin": "https://www.barchart.com",
        "Content-Type": "application/x-www-form-urlencoded",
    }, allow_redirects=True, timeout=15)

    if "login" in r.url.lower() and "incorrect" in r.text.lower():
        print("  Barchart login failed: bad credentials")
        return []

    # 3) Refresh XSRF token
    session.get("https://www.barchart.com/options/iv-rank-percentile/high",
                timeout=15)

    xsrf = session.cookies.get("XSRF-TOKEN")
    if not xsrf:
        print("  No XSRF token from Barchart")
        return []

    session.headers.update({
        "X-XSRF-TOKEN": urllib.parse.unquote(xsrf),
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.barchart.com/options/iv-rank-percentile/high",
    })

    # 4) Call internal API
    r = session.get(
        "https://www.barchart.com/proxies/core-api/v1/options/get",
        params={
            "list": "ivRankHigh",
            "fields": ("symbol,symbolName,optionsImpliedVolatilityRank1y,"
                       "optionsImpliedVolatilityPercentile1y,"
                       "optionsTotalVolume,tradeTime"),
            "orderBy": "optionsImpliedVolatilityRank1y",
            "orderDir": "desc",
            "meta": "field.shortName,field.type",
            "hasOptions": "true",
            "limit": "50",
        },
        timeout=15,
    )

    if r.status_code != 200:
        print(f"  Barchart API returned {r.status_code}")
        return []

    data = r.json()
    rows = data.get("data", [])

    results = []
    for row in rows:
        raw = row.get("raw", {})
        results.append({
            "symbol": raw.get("symbol", ""),
            "name": raw.get("symbolName", ""),
            "iv_rank": float(raw.get("optionsImpliedVolatilityRank1y", 0) or 0),
            "iv_pctl": float(raw.get("optionsImpliedVolatilityPercentile1y", 0) or 0),
            "volume": int(raw.get("optionsTotalVolume", 0) or 0),
        })

    return results


def has_0dte_today(symbol: str) -> bool:
    """Check if symbol has 0DTE expiration today."""
    dow = date.today().weekday()
    if symbol in DAILY_0DTE:
        return dow < 5
    if symbol in MWF_0DTE:
        return dow in (0, 2, 4)
    return dow == 4  # Friday only


def build_scan_list(iv_data: List[Dict]) -> Tuple[List[str], Dict[str, float]]:
    """
    Build today's scan list: core tickers + top dynamic from IV rank.
    Returns (scan_list, iv_rank_map).
    """
    core = [t for t in CORE_TICKERS if has_0dte_today(t)]
    core_set = set(core)
    iv_rank_map = {}

    # Map IV rank for core tickers
    for row in iv_data:
        if row["symbol"] in core_set:
            iv_rank_map[row["symbol"]] = row["iv_rank"]

    # Find dynamic additions
    dynamic = []
    for row in iv_data:
        sym = row["symbol"]
        if sym in core_set:
            continue
        if not has_0dte_today(sym):
            continue
        if row["volume"] < MIN_OPTION_VOLUME:
            continue
        dynamic.append(sym)
        iv_rank_map[sym] = row["iv_rank"]
        if len(dynamic) >= MAX_DYNAMIC_TICKERS:
            break

    scan_list = core + dynamic
    return scan_list, iv_rank_map


def send_iv_rank_notification(scan_list: List[str], iv_data: List[Dict]):
    """Send top 20 IV ranked tickers to ntfy."""
    lines = ["Today's top IV Rank tickers:", ""]
    lines.append(f"{'#':<3} {'Sym':<7} {'IV Rank':>8} {'IV Pctl':>8}")
    lines.append("-" * 30)
    scan_set = set(scan_list)
    for i, row in enumerate(iv_data[:20], 1):
        marker = " <--" if row["symbol"] in scan_set else ""
        lines.append(
            f"{i:<3} {row['symbol']:<7} {row['iv_rank']:>7.1f}% "
            f"{row['iv_pctl']:>7.1f}%{marker}"
        )
    lines.append("")
    lines.append(f"Scanning: {', '.join(scan_list)}")
    ntfy_send("GEX Scanner - IV Rank Top 20", "\n".join(lines),
              tags="bar_chart")


# ============================================================================
# Phase 2: GEX Wall Detection (Polygon)
# ============================================================================

def get_underlying_price(symbol: str) -> Optional[float]:
    """Fetch latest underlying price from Polygon."""
    try:
        js = _poly_get(f"{POLYGON_BASE}/v3/trades/{symbol}",
                       {"limit": 1, "sort": "timestamp", "order": "desc"})
        res = js.get("results") or []
        if res:
            px = res[0].get("price") or res[0].get("p")
            if px is not None and np.isfinite(float(px)):
                return float(px)
    except Exception:
        pass
    try:
        js = _poly_get(
            f"{POLYGON_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}")
        tkr = js.get("ticker") or {}
        p = ((tkr.get("lastTrade") or {}).get("p")
             or (tkr.get("day") or {}).get("c")
             or (tkr.get("prevDay") or {}).get("c"))
        if p is not None and np.isfinite(float(p)):
            return float(p)
    except Exception:
        pass
    try:
        js = _poly_get(f"{POLYGON_BASE}/v2/aggs/ticker/{symbol}/prev")
        res = js.get("results") or []
        if res and res[0].get("c") is not None:
            return float(res[0]["c"])
    except Exception:
        pass
    return None


def nearest_expiration(symbol: str) -> Optional[str]:
    """Find nearest 0DTE (or closest) expiration."""
    today = date.today()
    params = {
        "underlying_ticker": symbol,
        "expired": "false",
        "order": "asc",
        "sort": "expiration_date",
        "limit": 1000,
    }
    data = _poly_get(f"{POLYGON_BASE}/v3/reference/options/contracts", params)
    exps = set()
    while True:
        for it in data.get("results", []) or []:
            exp = it.get("expiration_date")
            if exp:
                exps.add(exp)
        nxt = data.get("next_url")
        if not nxt:
            break
        data = _poly_next(nxt)

    if not exps:
        return None

    # Prefer 0DTE (today)
    today_str = today.isoformat()
    if today_str in exps:
        return today_str

    # Nearest within 0-7 DTE
    candidates = []
    for e in sorted(exps):
        dte = (date.fromisoformat(e) - today).days
        if 0 <= dte <= 7:
            candidates.append((dte, e))
    if candidates:
        candidates.sort()
        return candidates[0][1]
    return None


def fetch_gex_walls(symbol: str) -> Optional[Dict]:
    """
    Fetch GEX walls: call_wall, next_call_wall, put_wall, next_put_wall, spot.
    Returns dict or None on failure.
    """
    exp = nearest_expiration(symbol)
    if not exp:
        print(f"    [{symbol}] No expiration found")
        return None

    # Fetch option chain snapshot
    params = {
        "expiration_date": exp,
        "order": "asc",
        "sort": "strike_price",
        "limit": 250,
    }
    data = _poly_get(f"{POLYGON_BASE}/v3/snapshot/options/{symbol}", params)
    contracts = []

    def parse_batch(js):
        for res in js.get("results", []) or []:
            details = res.get("details", {}) or {}
            typ = (details.get("contract_type")
                   or res.get("contract_type") or "").lower()
            strike = details.get("strike_price") or res.get("strike_price")
            oi = res.get("open_interest")
            if typ in ("call", "put") and strike is not None:
                contracts.append({
                    "strike": float(strike),
                    "type": typ,
                    "oi": float(oi) if oi is not None else 0.0,
                })

    parse_batch(data)
    while data.get("next_url"):
        data = _poly_next(data["next_url"])
        parse_batch(data)

    if not contracts:
        print(f"    [{symbol}] No option contracts found")
        return None

    df = pd.DataFrame(contracts)
    calls = df[df["type"] == "call"].sort_values("oi", ascending=False)
    puts = df[df["type"] == "put"].sort_values("oi", ascending=False)

    spot = get_underlying_price(symbol)
    if spot is None:
        print(f"    [{symbol}] Could not get underlying price")
        return None

    result = {"spot": spot, "expiry": exp}

    if not calls.empty:
        result["call_wall"] = float(calls.iloc[0]["strike"])
        result["next_call_wall"] = float(calls.iloc[1]["strike"]) if len(calls) > 1 else result["call_wall"]
    else:
        result["call_wall"] = spot * 1.02
        result["next_call_wall"] = spot * 1.04

    if not puts.empty:
        result["put_wall"] = float(puts.iloc[0]["strike"])
        result["next_put_wall"] = float(puts.iloc[1]["strike"]) if len(puts) > 1 else result["put_wall"]
    else:
        result["put_wall"] = spot * 0.98
        result["next_put_wall"] = spot * 0.96

    print(f"    [{symbol}] CW={result['call_wall']:.0f} "
          f"(next={result['next_call_wall']:.0f}) | "
          f"PW={result['put_wall']:.0f} "
          f"(next={result['next_put_wall']:.0f}) | "
          f"Spot={spot:.2f} | Exp={exp}")
    return result


# ============================================================================
# Phase 3: 5-Min Bars + ATR
# ============================================================================

def fetch_5min_bars(symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch 5-min bars from Polygon, converted to ET."""
    all_results = []
    js = _poly_get(
        f"{POLYGON_BASE}/v2/aggs/ticker/{symbol}/range/5/minute/{start_date}/{end_date}",
        {"adjusted": "true", "sort": "asc", "limit": 50000},
    )
    all_results.extend(js.get("results") or [])
    while js.get("next_url"):
        js = _poly_next(js["next_url"])
        all_results.extend(js.get("results") or [])

    if not all_results:
        return pd.DataFrame()

    df = pd.DataFrame(all_results)
    df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    df["t"] = df["t"].dt.tz_convert("America/New_York")
    df = df.rename(columns={"o": "open", "h": "high", "l": "low",
                             "c": "close", "v": "volume"})
    df = df.set_index("t")
    df = df[["open", "high", "low", "close", "volume"]].copy()
    return df


def compute_atr(bars: pd.DataFrame, periods: int = ATR_PERIODS) -> pd.Series:
    """ATR(14) on 5-min bars using True Range."""
    high = bars["high"]
    low = bars["low"]
    prev_close = bars["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=periods, min_periods=periods).mean()


# ============================================================================
# Phase 4: Spike Detection
# ============================================================================

def check_for_spike(
    symbol: str,
    bars: pd.DataFrame,
    atr: pd.Series,
    today: date,
) -> Optional[Dict]:
    """
    Check for ATR spike in today's 9:35-10:30 window.
    Returns spike info dict or None.
    """
    day_bars = bars[bars.index.date == today]
    if day_bars.empty:
        return None

    ref = day_bars.index[0]
    scan_start = ref.replace(hour=SCAN_START[0], minute=SCAN_START[1],
                             second=0, microsecond=0)
    scan_end = ref.replace(hour=SCAN_END[0], minute=SCAN_END[1],
                           second=0, microsecond=0)
    window = day_bars[(day_bars.index >= scan_start) & (day_bars.index < scan_end)]

    for ts, bar in window.iterrows():
        current_atr = atr.get(ts)
        if current_atr is None or np.isnan(current_atr) or current_atr <= 0:
            continue

        candle_range = float(bar["high"] - bar["low"])
        if candle_range > ATR_SPIKE_MULTIPLIER * current_atr:
            candle_close = float(bar["close"])
            candle_open = float(bar["open"])

            if candle_close > candle_open:
                direction = "CALL_WALL_PUSH"
            elif candle_close < candle_open:
                direction = "PUT_WALL_PUSH"
            else:
                if float(bar["high"]) - candle_close <= candle_close - float(bar["low"]):
                    direction = "CALL_WALL_PUSH"
                else:
                    direction = "PUT_WALL_PUSH"

            return {
                "symbol": symbol,
                "signal_type": direction,
                "signal_time": str(ts),
                "candle_range": candle_range,
                "atr": float(current_atr),
                "spike_ratio": round(candle_range / current_atr, 2),
                "candle_close": candle_close,
            }

    return None


# ============================================================================
# Phase 5: Option Price Lookup + Alert
# ============================================================================

def build_occ_ticker(symbol: str, expiry: date, option_type: str,
                     strike: float) -> str:
    """Build OCC-format option ticker. option_type: 'C' or 'P'."""
    exp_str = expiry.strftime("%y%m%d")
    strike_int = int(strike * 1000)
    return f"O:{symbol}{exp_str}{option_type}{strike_int:08d}"


def fetch_option_price(symbol: str, expiry: date, option_type: str,
                       strike: float, signal_time: str) -> Optional[float]:
    """Fetch option price near signal_time from 5-min bars."""
    occ = build_occ_ticker(symbol, expiry, option_type, strike)
    date_str = expiry.isoformat()
    try:
        js = _poly_get(
            f"{POLYGON_BASE}/v2/aggs/ticker/{occ}/range/5/minute/{date_str}/{date_str}",
            {"adjusted": "true", "sort": "asc", "limit": 5000},
        )
    except RuntimeError:
        return None

    results = js.get("results") or []
    if not results:
        return None

    try:
        sig_epoch_ms = int(pd.Timestamp(signal_time).timestamp() * 1000)
    except Exception:
        return None

    best = None
    for bar in results:
        if bar.get("t", 0) <= sig_epoch_ms:
            best = bar
        else:
            break

    if best and best.get("c") is not None:
        return float(best["c"])
    return None


def get_day_open(bars: pd.DataFrame, today: date) -> Optional[float]:
    """Get today's opening price from 5-min bars."""
    day_bars = bars[bars.index.date == today]
    if day_bars.empty:
        return None
    return float(day_bars.iloc[0]["open"])


def send_trade_alert(
    spike: Dict,
    walls: Dict,
    iv_rank: float,
    day_open: float,
    credit_per_share: float,
    used_real: bool,
):
    """Send trade alert via console + ntfy + sound."""
    symbol = spike["symbol"]
    expiry = date.fromisoformat(walls["expiry"])
    expiry_str = expiry.strftime("%m/%d")
    width = SPREAD_WIDTHS.get(symbol, DEFAULT_SPREAD_WIDTH)

    if spike["signal_type"] == "CALL_WALL_PUSH":
        spread_name = "BEAR CALL SPREAD"
        short_type = "Call"
        short_strike = walls["call_wall"]
        long_strike = short_strike + width
    else:
        spread_name = "BULL PUT SPREAD"
        short_type = "Put"
        short_strike = walls["put_wall"]
        long_strike = short_strike - width

    credit_total = credit_per_share * MULTIPLIER
    max_loss = (width * MULTIPLIER) - credit_total
    sig_hhmm = spike["signal_time"].split(" ")[-1][:5] if " " in spike["signal_time"] else spike["signal_time"][:5]
    price_tag = "REAL" if used_real else "EST"
    direction = "Bullish" if spike["signal_type"] == "CALL_WALL_PUSH" else "Bearish"

    # Console alert
    print(f"\n{'='*56}")
    print(f"  ** SIGNAL: {symbol} {spread_name} **")
    print(f"{'='*56}")
    print(f"  Time:     {sig_hhmm} ET")
    print(f"  Spot:     ${walls['spot']:.2f}")
    print(f"  Open:     ${day_open:.2f}")
    print(f"  IV Rank:  {iv_rank:.0f}%")
    print(f"")
    print(f"  GEX WALLS:")
    cw_tag = " (target)" if spike["signal_type"] == "CALL_WALL_PUSH" else ""
    pw_tag = " (target)" if spike["signal_type"] == "PUT_WALL_PUSH" else ""
    print(f"    Call Wall: ${walls['call_wall']:.0f}{cw_tag}")
    print(f"    Next Call: ${walls['next_call_wall']:.0f}")
    print(f"    Put Wall:  ${walls['put_wall']:.0f}{pw_tag}")
    print(f"    Next Put:  ${walls['next_put_wall']:.0f}")
    print(f"")
    print(f"  TRADE:")
    print(f"    Sell 1x {symbol} {expiry_str} ${short_strike:.0f} {short_type}")
    print(f"    Buy  1x {symbol} {expiry_str} ${long_strike:.0f} {short_type}")
    print(f"    Width: ${width:.2f} | Credit: ~${credit_per_share:.2f} "
          f"(${credit_total:.0f}/contract) [{price_tag}]")
    print(f"    Max Loss: ${max_loss:.0f}/contract")
    print(f"")
    print(f"  TRIGGER:")
    print(f"    ATR: ${spike['atr']:.2f} | Range: ${spike['candle_range']:.2f} "
          f"({spike['spike_ratio']:.1f}x) | {direction} spike")
    print(f"{'='*56}\n")

    # ntfy push
    body = (
        f"TRADE TO PLACE:\n"
        f"  Sell 1x {symbol} {expiry_str} ${short_strike:.0f} {short_type}\n"
        f"  Buy  1x {symbol} {expiry_str} ${long_strike:.0f} {short_type}\n"
        f"  Expiry: 0DTE | Width: ${width:.2f}\n"
        f"  Credit: ~${credit_per_share:.2f}/share (${credit_total:.0f}/contract) [{price_tag}]\n"
        f"  Max Loss: ${max_loss:.0f}/contract\n"
        f"\n"
        f"PRICE:\n"
        f"  Open:    ${day_open:.2f}\n"
        f"  Current: ${walls['spot']:.2f}\n"
        f"\n"
        f"GEX WALLS:\n"
        f"  Call Wall: ${walls['call_wall']:.0f}{cw_tag}\n"
        f"  Next Call: ${walls['next_call_wall']:.0f}\n"
        f"  Put Wall:  ${walls['put_wall']:.0f}{pw_tag}\n"
        f"  Next Put:  ${walls['next_put_wall']:.0f}\n"
        f"\n"
        f"IV Rank: {iv_rank:.0f}%\n"
        f"\n"
        f"TRIGGER:\n"
        f"  {sig_hhmm} ET | {direction} spike\n"
        f"  ATR: ${spike['atr']:.2f} | Range: ${spike['candle_range']:.2f} ({spike['spike_ratio']:.1f}x)"
    )
    ntfy_send(f"{symbol} {spread_name}", body, priority="high",
              tags="rotating_light")

    # Sound
    beep()


# ============================================================================
# Phase 6: Signal Logging
# ============================================================================

def log_signal(spike: Dict, walls: Dict, iv_rank: float,
               credit_per_share: float, day_open: float):
    """Append signal to signals_log.csv."""
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "signals_log.csv")
    width = SPREAD_WIDTHS.get(spike["symbol"], DEFAULT_SPREAD_WIDTH)

    if spike["signal_type"] == "CALL_WALL_PUSH":
        short_strike = walls["call_wall"]
        long_strike = short_strike + width
    else:
        short_strike = walls["put_wall"]
        long_strike = short_strike - width

    row = {
        "date": date.today().isoformat(),
        "time": spike["signal_time"],
        "symbol": spike["symbol"],
        "signal_type": spike["signal_type"],
        "wall_strike": short_strike,
        "long_strike": long_strike,
        "spread_width": width,
        "estimated_credit": round(credit_per_share * MULTIPLIER, 2),
        "spot": walls["spot"],
        "day_open": day_open,
        "iv_rank": round(iv_rank, 1),
        "atr": round(spike["atr"], 4),
        "candle_range": round(spike["candle_range"], 4),
        "spike_ratio": spike["spike_ratio"],
        "call_wall": walls["call_wall"],
        "put_wall": walls["put_wall"],
    }

    write_header = not os.path.exists(log_path)
    with open(log_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ============================================================================
# Main
# ============================================================================

def main():
    print("=" * 60)
    print("  GEX Wall Rejection 0DTE Live Scanner")
    print(f"  {datetime.now(tz=ET).strftime('%Y-%m-%d %H:%M:%S ET')}")
    print("=" * 60)

    today = date.today()

    # ---- Phase 1: Barchart IV rank ----
    print("\n[Phase 1] Fetching IV rank from Barchart...")
    iv_data = barchart_fetch_iv_rank()
    if iv_data:
        print(f"  Got {len(iv_data)} tickers from Barchart")
    else:
        print("  No IV data (weekend or login issue). Using core tickers only.")

    scan_list, iv_rank_map = build_scan_list(iv_data)
    print(f"\n  Scan list: {', '.join(scan_list)}")

    if iv_data:
        send_iv_rank_notification(scan_list, iv_data)
        print("  Sent IV rank notification")

    # ---- Phase 2: GEX walls ----
    print(f"\n[Phase 2] Fetching GEX walls...")
    walls_map: Dict[str, Dict] = {}
    for symbol in scan_list:
        try:
            walls = fetch_gex_walls(symbol)
            if walls:
                walls_map[symbol] = walls
        except Exception as e:
            print(f"    [{symbol}] Error: {e}")

    if not walls_map:
        print("  No GEX walls found for any ticker. Exiting.")
        return

    # ---- Phase 3: Fetch 5-min bars + ATR ----
    print(f"\n[Phase 3] Fetching 5-min bars...")
    bars_map: Dict[str, pd.DataFrame] = {}
    start_date = today - timedelta(days=5)

    for symbol in walls_map:
        try:
            bars = fetch_5min_bars(symbol, start_date, today)
            if not bars.empty:
                bars_map[symbol] = bars
                print(f"    [{symbol}] {len(bars)} bars")
            else:
                print(f"    [{symbol}] No 5-min bars")
        except Exception as e:
            print(f"    [{symbol}] Error: {e}")

    # ---- Phase 4: Polling loop ----
    print(f"\n[Phase 4] Entering scan loop ({SCAN_START[0]}:{SCAN_START[1]:02d}"
          f"-{SCAN_END[0]}:{SCAN_END[1]:02d} ET, polling every {POLL_INTERVAL_SEC}s)")

    fired = set()  # symbols that already triggered
    signals = []   # collected signals

    while True:
        now = datetime.now(tz=ET)
        now_hm = (now.hour, now.minute)

        if now_hm > (SCAN_END[0], SCAN_END[1]):
            print(f"\n  Scan window closed at {now.strftime('%H:%M ET')}")
            break

        if now_hm < (SCAN_START[0], SCAN_START[1]):
            wait = 60
            next_check = f"{SCAN_START[0]}:{SCAN_START[1]:02d}"
            print(f"  Waiting for scan window ({next_check} ET)... "
                  f"[{now.strftime('%H:%M:%S')}]")
            time.sleep(wait)
            continue

        active = [s for s in bars_map if s not in fired]
        if not active:
            print(f"  All tickers fired. Done.")
            break

        print(f"  [{now.strftime('%H:%M:%S')}] Polling {len(active)} tickers: "
              f"{', '.join(active)}")

        for symbol in active:
            try:
                # Refresh today's bars
                new_bars = fetch_5min_bars(symbol, today, today)
                if not new_bars.empty:
                    existing = bars_map[symbol]
                    combined = pd.concat([existing, new_bars])
                    bars_map[symbol] = combined[~combined.index.duplicated(keep="last")]

                # Compute ATR
                atr = compute_atr(bars_map[symbol])

                # Check for spike
                spike = check_for_spike(symbol, bars_map[symbol], atr, today)
                if spike:
                    print(f"    *** SPIKE: {symbol} {spike['signal_type']} "
                          f"({spike['spike_ratio']:.1f}x ATR) ***")
                    fired.add(symbol)

                    # Fetch real option prices
                    walls = walls_map[symbol]
                    expiry = date.fromisoformat(walls["expiry"])
                    width = SPREAD_WIDTHS.get(symbol, DEFAULT_SPREAD_WIDTH)

                    if spike["signal_type"] == "CALL_WALL_PUSH":
                        opt_type = "C"
                        short_strike = walls["call_wall"]
                        long_strike = short_strike + width
                    else:
                        opt_type = "P"
                        short_strike = walls["put_wall"]
                        long_strike = short_strike - width

                    short_px = fetch_option_price(
                        symbol, expiry, opt_type, short_strike,
                        spike["signal_time"])
                    time.sleep(0.25)
                    long_px = fetch_option_price(
                        symbol, expiry, opt_type, long_strike,
                        spike["signal_time"])

                    if (short_px is not None and long_px is not None
                            and short_px > long_px):
                        credit_per_share = short_px - long_px
                        used_real = True
                    else:
                        credit_per_share = CREDIT_PER_DOLLAR * width
                        used_real = False

                    iv_rank = iv_rank_map.get(symbol, 0.0)
                    day_open = get_day_open(bars_map[symbol], today) or walls["spot"]

                    send_trade_alert(spike, walls, iv_rank, day_open,
                                     credit_per_share, used_real)
                    log_signal(spike, walls, iv_rank, credit_per_share, day_open)
                    signals.append(spike)

            except Exception as e:
                print(f"    [{symbol}] Poll error: {e}")

        # Wait for next poll
        remaining = [s for s in bars_map if s not in fired]
        if not remaining:
            print(f"\n  All tickers fired. Done.")
            break

        time.sleep(POLL_INTERVAL_SEC)

    # ---- Phase 6: Summary ----
    print(f"\n{'='*60}")
    print(f"  END OF SCAN SUMMARY")
    print(f"{'='*60}")
    print(f"  Tickers scanned:  {len(bars_map)}")
    print(f"  Signals fired:    {len(signals)}")
    print(f"  No signal:        {len(bars_map) - len(signals)}")
    if signals:
        print(f"\n  Signals:")
        for s in signals:
            print(f"    {s['symbol']} | {s['signal_type']} | "
                  f"{s['spike_ratio']:.1f}x ATR | {s['signal_time']}")
    print()


if __name__ == "__main__":
    main()
