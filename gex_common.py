"""
GEX Common — Shared utilities for the live scanner and backtest.
================================================================
Polygon API helpers, bar fetching, ATR, option pricing, GEX walls,
and 0DTE schedule constants.

Usage:
    import gex_common
    gex_common.init("YOUR_POLYGON_API_KEY")
    # then call gex_common.fetch_5min_bars(...) etc.
"""

import time
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests


# ============================================================================
# Module-level state (set once via init())
# ============================================================================

_API_KEY: str = ""
_BASE: str = "https://api.polygon.io"


def init(api_key: str, base_url: str = "https://api.polygon.io"):
    """Initialize the module with a Polygon API key."""
    global _API_KEY, _BASE
    _API_KEY = api_key
    _BASE = base_url


# ============================================================================
# 0DTE Schedule Constants
# ============================================================================

DAILY_0DTE = {"SPY", "QQQ", "IWM"}
MWF_0DTE = {"TSLA", "NVDA", "AMZN", "META", "AAPL", "MSFT", "AVGO", "GOOGL", "IBIT"}


def has_0dte(symbol: str, trade_date: date = None) -> bool:
    """Check whether symbol has a 0DTE expiration on the given date (default: today)."""
    if trade_date is None:
        trade_date = date.today()
    dow = trade_date.weekday()
    if symbol in DAILY_0DTE:
        return dow < 5
    if symbol in MWF_0DTE:
        return dow in (0, 2, 4)
    return dow == 4  # Friday only


# ============================================================================
# Polygon HTTP Helpers (with 429 retry)
# ============================================================================

def poly_get(url: str, params: Dict = None) -> Dict:
    """GET with Polygon API key injection and 429 retry (5 attempts)."""
    if params is None:
        params = {}
    params["apiKey"] = _API_KEY
    last = None
    for attempt in range(5):
        r = requests.get(url, params=params, timeout=30)
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


def poly_next(next_url: str) -> Dict:
    """Follow Polygon pagination next_url with retry."""
    if "apiKey=" not in next_url:
        sep = "&" if "?" in next_url else "?"
        next_url = f"{next_url}{sep}apiKey={_API_KEY}"
    last = None
    for attempt in range(5):
        r = requests.get(next_url, timeout=30)
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
# Price & Expiration
# ============================================================================

def get_underlying_price(symbol: str) -> Optional[float]:
    """Best-effort latest stock/ETF price (tries trades → snapshot → prev close)."""
    try:
        js = poly_get(f"{_BASE}/v3/trades/{symbol}",
                      {"limit": 1, "sort": "timestamp", "order": "desc"})
        res = js.get("results") or []
        if res:
            px = res[0].get("price") or res[0].get("p")
            if px is not None and np.isfinite(float(px)):
                return float(px)
    except Exception:
        pass
    try:
        js = poly_get(
            f"{_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}")
        tkr = js.get("ticker") or {}
        p = ((tkr.get("lastTrade") or {}).get("p")
             or (tkr.get("day") or {}).get("c")
             or (tkr.get("prevDay") or {}).get("c"))
        if p is not None and np.isfinite(float(p)):
            return float(p)
    except Exception:
        pass
    try:
        js = poly_get(f"{_BASE}/v2/aggs/ticker/{symbol}/prev")
        res = js.get("results") or []
        if res and res[0].get("c") is not None:
            return float(res[0]["c"])
    except Exception:
        pass
    return None


def nearest_expiration(symbol: str, min_dte: int = 0, max_dte: int = 7) -> Optional[str]:
    """Find the nearest option expiration date within [min_dte, max_dte]."""
    today = date.today()
    params = {
        "underlying_ticker": symbol,
        "expired": "false",
        "order": "asc",
        "sort": "expiration_date",
        "limit": 1000,
    }
    data = poly_get(f"{_BASE}/v3/reference/options/contracts", params)
    exps = set()
    while True:
        for it in data.get("results", []) or []:
            exp = it.get("expiration_date")
            if exp:
                exps.add(exp)
        nxt = data.get("next_url")
        if not nxt:
            break
        data = poly_next(nxt)

    if not exps:
        return None

    candidates = []
    for e in sorted(exps):
        dte = (date.fromisoformat(e) - today).days
        if min_dte <= dte <= max_dte:
            candidates.append((dte, e))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    # fallback: nearest overall
    all_sorted = sorted(
        [(abs((date.fromisoformat(e) - today).days), e) for e in exps],
        key=lambda x: x[0],
    )
    return all_sorted[0][1] if all_sorted else None


# ============================================================================
# Option Helpers
# ============================================================================

def build_occ_ticker(symbol: str, expiry: date, option_type: str,
                     strike: float) -> str:
    """Build OCC-format option ticker. option_type: 'C' or 'P'."""
    exp_str = expiry.strftime("%y%m%d")
    strike_int = int(strike * 1000)
    return f"O:{symbol}{exp_str}{option_type}{strike_int:08d}"


def fetch_option_price(symbol: str, expiry: date, option_type: str,
                       strike: float, signal_time: str) -> Optional[float]:
    """Fetch option price near signal_time from 5-min aggregate bars."""
    occ = build_occ_ticker(symbol, expiry, option_type, strike)
    date_str = expiry.isoformat()
    try:
        js = poly_get(
            f"{_BASE}/v2/aggs/ticker/{occ}/range/5/minute/{date_str}/{date_str}",
            {"adjusted": "true", "sort": "asc", "limit": 5000},
        )
    except RuntimeError:
        return None

    results = js.get("results") or []
    if not results:
        return None

    try:
        sig_dt = pd.Timestamp(signal_time)
        sig_epoch_ms = int(sig_dt.timestamp() * 1000)
    except Exception:
        return None

    best = None
    for bar in results:
        t = bar.get("t", 0)
        if t <= sig_epoch_ms:
            best = bar
        else:
            break

    if best is not None and best.get("c") is not None:
        return float(best["c"])
    return None


# ============================================================================
# Bar Fetching
# ============================================================================

def fetch_5min_bars(symbol: str, start_date: date,
                    end_date: date) -> pd.DataFrame:
    """Fetch 5-min bars with pagination, converted to America/New_York tz."""
    all_results = []
    js = poly_get(
        f"{_BASE}/v2/aggs/ticker/{symbol}/range/5/minute/{start_date}/{end_date}",
        {"adjusted": "true", "sort": "asc", "limit": 50000},
    )
    all_results.extend(js.get("results") or [])
    while js.get("next_url"):
        js = poly_next(js["next_url"])
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


# ============================================================================
# ATR
# ============================================================================

def compute_atr(bars: pd.DataFrame, periods: int = 14) -> pd.Series:
    """ATR using True Range on OHLC bars."""
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
# GEX Wall Detection
# ============================================================================

def fetch_gex_walls(symbol: str) -> Optional[Dict]:
    """
    Fetch GEX walls from Polygon option chain snapshot (highest OI strikes).
    Returns dict: {spot, expiry, call_wall, next_call_wall, put_wall, next_put_wall}
    or None on failure.
    """
    exp = nearest_expiration(symbol)
    if not exp:
        print(f"    [{symbol}] No expiration found")
        return None

    params = {
        "expiration_date": exp,
        "order": "asc",
        "sort": "strike_price",
        "limit": 250,
    }
    data = poly_get(f"{_BASE}/v3/snapshot/options/{symbol}", params)
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
        data = poly_next(data["next_url"])
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
