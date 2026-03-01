"""
Momentum-Triggered GEX Wall 0DTE Credit Spread Backtest
========================================================
Detect momentum exhaustion via ATR spike on 5-min candles during the first
hour of trading (9:30-10:30 ET).  When a candle's range exceeds 2x the
rolling ATR(14), the move is likely overextended — sell a credit spread at
the nearest GEX wall in the direction of the move, betting on mean reversion.

Strategy:
  - Rapid UP move   → find nearest CALL_WALL above → sell bear call spread
  - Rapid DOWN move → find nearest PUT_WALL below  → sell bull put spread
  - No momentum spike → no trade

Self-contained: no imports from existing scripts.
"""

import os
import sys
import json
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import mplfinance as mpf
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# ============================================================================
# Section 0: Config
# ============================================================================

API_KEY = os.environ.get("POLYGON_API_KEY", "")
if not API_KEY:
    sys.exit("ERROR: Set POLYGON_API_KEY environment variable. Get a free key at https://polygon.io")
BASE = "https://api.polygon.io"

SNAPSHOT_PAGE_LIMIT = 250

CALL_SIGN = +1.0
PUT_SIGN = -1.0
MULTIPLIERS = {"QQQ": 100, "IWM": 100, "TSLA": 100,
               "NVDA": 100, "AMZN": 100, "META": 100, "GOOGL": 100}


@dataclass
class BacktestConfig:
    symbols: List[str] = field(default_factory=lambda: [
        "QQQ", "IWM", "TSLA", "NVDA", "AMZN", "META", "GOOGL",
    ])
    lookback_days: int = 10
    scan_start_hour: int = 9                 # 9:35 ET (skip open candle)
    scan_start_minute: int = 35
    scan_end_hour: int = 10                  # 10:30 ET
    scan_end_minute: int = 30
    atr_candle_minutes: int = 5              # 5-min candles for ATR
    atr_lookback_periods: int = 14           # 14-period ATR (standard)
    atr_lookback_days: int = 2               # Use 2 trading days of 5-min bars for ATR baseline
    atr_spike_multiplier: float = 2.0        # Trigger when candle range > 2x ATR
    spread_widths: Dict[str, float] = field(
        default_factory=lambda: {
            "QQQ": 2.0, "IWM": 1.0,
            "TSLA": 5.0, "NVDA": 5.0, "AMZN": 5.0,
            "META": 5.0, "GOOGL": 5.0,
        }
    )
    credit_per_dollar: float = 0.30          # fallback credit per $1 width when real prices unavailable
    snap_increment: float = 5.0              # snap walls to $5 strikes
    multiplier: int = 100


# ============================================================================
# Section 1: HTTP Helpers
# ============================================================================

def _get(url: str, params: Dict = None) -> Dict:
    if params is None:
        params = {}
    params["apiKey"] = API_KEY
    last = None
    for attempt in range(3):
        r = requests.get(url, params=params, timeout=30)
        last = r
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                pass
        time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(f"Polygon GET failed {last.status_code}: {last.text[:200]}")


def _get_next(next_url: str) -> Dict:
    if "apiKey=" not in next_url:
        sep = "&" if "?" in next_url else "?"
        next_url = f"{next_url}{sep}apiKey={API_KEY}"
    last = None
    for attempt in range(3):
        r = requests.get(next_url, timeout=30)
        last = r
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                pass
        time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(f"Polygon NEXT failed {last.status_code}: {last.text[:200]}")


# ============================================================================
# Section 2: Data Fetching
# ============================================================================

def get_underlying_price(symbol: str) -> Optional[float]:
    """Best-effort latest stock/ETF price."""
    # v3 trades (latest)
    try:
        js = _get(f"{BASE}/v3/trades/{symbol}",
                  {"limit": 1, "sort": "timestamp", "order": "desc"})
        res = js.get("results") or []
        if res:
            px = res[0].get("price") or res[0].get("p")
            if px is not None and np.isfinite(float(px)):
                return float(px)
    except Exception:
        pass
    # v2 snapshot
    try:
        js = _get(f"{BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}")
        tkr = js.get("ticker") or {}
        p = ((tkr.get("lastTrade") or {}).get("p")
             or (tkr.get("day") or {}).get("c")
             or (tkr.get("prevDay") or {}).get("c"))
        if p is not None and np.isfinite(float(p)):
            return float(p)
    except Exception:
        pass
    # prev close
    try:
        js = _get(f"{BASE}/v2/aggs/ticker/{symbol}/prev")
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
    data = _get(f"{BASE}/v3/reference/options/contracts", params)

    exps = set()
    while True:
        for it in data.get("results", []) or []:
            exp = it.get("expiration_date")
            if exp:
                exps.add(exp)
        nxt = data.get("next_url")
        if not nxt:
            break
        data = _get_next(nxt)

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


def fetch_chain_snapshot(symbol: str, expiry: str) -> Tuple[List[Dict], float]:
    """Fetch option snapshots for symbol & expiry. Returns (contracts, spot)."""
    params = {
        "expiration_date": expiry,
        "order": "asc",
        "sort": "strike_price",
        "limit": SNAPSHOT_PAGE_LIMIT,
    }
    url = f"{BASE}/v3/snapshot/options/{symbol}"
    data = _get(url, params)

    results: List[Dict] = []

    def parse_batch(js: Dict):
        for res in js.get("results", []) or []:
            details = res.get("details", {}) or {}
            typ = (details.get("contract_type")
                   or res.get("contract_type") or "").lower()
            strike = details.get("strike_price") or res.get("strike_price")
            oi = res.get("open_interest")
            greeks = res.get("greeks") or {}
            gamma = greeks.get("gamma")
            if typ in ("call", "put") and strike is not None:
                results.append({
                    "strike": float(strike),
                    "type": typ,
                    "oi": float(oi) if oi is not None else 0.0,
                    "gamma": float(gamma) if gamma is not None else float("nan"),
                })

    parse_batch(data)
    while data.get("next_url"):
        data = _get_next(data["next_url"])
        parse_batch(data)

    S = get_underlying_price(symbol)
    if S is None:
        raise RuntimeError(f"[{symbol}] Could not resolve underlying price.")
    return results, S


def get_current_gex_levels(symbol: str) -> Dict:
    """
    Get today's CALL_WALL, PUT_WALL, and spot price from live snapshot.
    Returns dict: {call_wall, put_wall, spot}
    """
    exp = nearest_expiration(symbol, min_dte=0, max_dte=7)
    if not exp:
        raise RuntimeError(f"[{symbol}] No expiration found.")
    contracts, S = fetch_chain_snapshot(symbol, exp)
    if not contracts:
        raise RuntimeError(f"[{symbol}] Snapshot returned no contracts.")

    df = pd.DataFrame(contracts)
    df["oi"] = pd.to_numeric(df["oi"], errors="coerce").fillna(0.0)

    calls = df[df["type"] == "call"]
    puts = df[df["type"] == "put"]

    call_wall = float(calls.sort_values("oi", ascending=False).iloc[0]["strike"]) if not calls.empty else S * 1.02
    put_wall = float(puts.sort_values("oi", ascending=False).iloc[0]["strike"]) if not puts.empty else S * 0.98

    print(f"  [{symbol}] Live GEX levels — CALL_WALL: {call_wall:.2f}, "
          f"PUT_WALL: {put_wall:.2f}, Spot: {S:.2f}, Expiry: {exp}")
    return {"call_wall": call_wall, "put_wall": put_wall, "spot": S}


def fetch_daily_bars(symbol: str, calendar_days: int = 120) -> pd.DataFrame:
    """Fetch daily OHLCV bars. Returns DataFrame with date index."""
    end = date.today()
    start = end - timedelta(days=calendar_days)
    js = _get(
        f"{BASE}/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
        {"adjusted": "true", "sort": "asc", "limit": 5000},
    )
    arr = js.get("results") or []
    if not arr:
        return pd.DataFrame()

    df = pd.DataFrame(arr)
    df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
    df = df.rename(columns={"o": "open", "h": "high", "l": "low",
                             "c": "close", "v": "volume"})
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df = df.set_index("date")
    return df


def fetch_5min_bars(symbol: str, start_date: date,
                    end_date: date) -> pd.DataFrame:
    """
    Fetch 5-min bars over a date range, converted to America/New_York tz.
    Uses pagination to get all bars.
    """
    all_results = []
    js = _get(
        f"{BASE}/v2/aggs/ticker/{symbol}/range/5/minute/{start_date}/{end_date}",
        {"adjusted": "true", "sort": "asc", "limit": 50000},
    )
    all_results.extend(js.get("results") or [])

    while js.get("next_url"):
        js = _get_next(js["next_url"])
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


def fetch_intraday_bars(symbol: str, start_date: date,
                        end_date: date) -> pd.DataFrame:
    """
    Fetch 1-min bars over a date range, converted to America/New_York tz.
    Uses pagination to get all bars.
    """
    all_results = []
    js = _get(
        f"{BASE}/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}",
        {"adjusted": "true", "sort": "asc", "limit": 50000},
    )
    all_results.extend(js.get("results") or [])

    # Paginate if there's a next_url
    while js.get("next_url"):
        js = _get_next(js["next_url"])
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


# --- 0DTE Expiry Helpers ---

# Tickers with daily 0DTE expirations (Mon-Fri)
DAILY_0DTE = {"SPY", "QQQ", "IWM"}
# Tickers with Mon/Wed/Fri 0DTE only
MWF_0DTE = {"TSLA", "NVDA", "AMZN", "META", "AAPL", "MSFT", "AVGO", "GOOGL", "IBIT"}


def has_0dte_expiry(symbol: str, trade_date: date) -> bool:
    """Check whether this symbol has a 0DTE expiration on the given date."""
    dow = trade_date.weekday()  # 0=Mon, 4=Fri
    if symbol in DAILY_0DTE:
        return dow < 5  # Mon-Fri
    if symbol in MWF_0DTE:
        return dow in (0, 2, 4)  # Mon, Wed, Fri
    return dow == 4  # Friday-only fallback


def build_option_ticker(symbol: str, expiry: date, option_type: str, strike: float) -> str:
    """
    Build OCC-format option ticker for Polygon.
    Example: O:SPY260227C00695000
    option_type: "C" or "P"
    """
    exp_str = expiry.strftime("%y%m%d")
    strike_int = int(strike * 1000)
    return f"O:{symbol}{exp_str}{option_type}{strike_int:08d}"


def fetch_option_price_at_time(
    symbol: str,
    expiry: date,
    option_type: str,
    strike: float,
    signal_time: str,
) -> Optional[float]:
    """
    Fetch real option price near signal_time using 5-min aggregate bars.
    Returns the close of the 5-min candle whose timestamp <= signal_time,
    or None if no data is available.
    """
    occ = build_option_ticker(symbol, expiry, option_type, strike)
    date_str = expiry.isoformat()

    try:
        js = _get(
            f"{BASE}/v2/aggs/ticker/{occ}/range/5/minute/{date_str}/{date_str}",
            {"adjusted": "true", "sort": "asc", "limit": 5000},
        )
    except RuntimeError:
        return None

    results = js.get("results") or []
    if not results:
        return None

    # Parse signal_time to epoch ms for comparison
    # signal_time format: "2026-02-27 09:45:00-05:00" (tz-aware string)
    try:
        sig_dt = pd.Timestamp(signal_time)
        sig_epoch_ms = int(sig_dt.timestamp() * 1000)
    except Exception:
        return None

    # Find the latest candle with timestamp <= signal_time
    best = None
    for bar in results:
        t = bar.get("t", 0)
        if t <= sig_epoch_ms:
            best = bar
        else:
            break  # bars are sorted ascending

    if best is not None and best.get("c") is not None:
        return float(best["c"])
    return None


# ============================================================================
# Section 3: Historical Wall Estimation
# ============================================================================

def snap_to_increment(value: float, increment: float) -> float:
    """Snap a value to the nearest increment (e.g. $5)."""
    return round(round(value / increment) * increment, 2)


def compute_historical_walls(
    symbol: str,
    current_walls: Dict,
    daily_bars: pd.DataFrame,
    cfg: BacktestConfig,
) -> pd.DataFrame:
    """
    For each historical day, estimate CALL_WALL and PUT_WALL by preserving
    the current wall's *percentage offset* from spot, applied to that day's
    open, then snapped to the nearest $5 strike.

    The percentage offsets are clamped so walls stay within a realistic range
    (0.5%-3% from open), reflecting typical GEX clustering behavior.

    Returns DataFrame: [date, open, close, call_wall, put_wall]
    """
    current_spot = current_walls["spot"]
    current_call_wall = current_walls["call_wall"]
    current_put_wall = current_walls["put_wall"]
    inc = cfg.snap_increment

    # Compute current wall offsets as dollar amounts, then convert to
    # number of $5 increments. This preserves the structural relationship
    # (e.g., CW is ~2 increments above, PW is ~1 increment below).
    cw_offset_inc = max(1, round((current_call_wall - current_spot) / inc))
    pw_offset_inc = max(1, round((current_spot - current_put_wall) / inc))

    # Cap at 3 increments ($15) — walls beyond that are unreachable
    # in a 15-minute window and represent distant gamma, not a "wall"
    cw_offset_inc = min(cw_offset_inc, 3)
    pw_offset_inc = min(pw_offset_inc, 3)

    rows = []
    for dt, bar in daily_bars.iterrows():
        day_open = bar["open"]

        # Place walls N increments above/below the snapped open
        open_snapped = snap_to_increment(day_open, inc)
        cw = open_snapped + cw_offset_inc * inc
        pw = open_snapped - pw_offset_inc * inc

        # Ensure CALL_WALL > open, PUT_WALL < open
        if cw <= day_open:
            cw = snap_to_increment(day_open + inc, inc)
        if pw >= day_open:
            pw = snap_to_increment(day_open - inc, inc)

        rows.append({
            "date": dt,
            "open": day_open,
            "close": bar["close"],
            "call_wall": cw,
            "put_wall": pw,
        })

    return pd.DataFrame(rows)


# ============================================================================
# Section 4: Signal Detection
# ============================================================================

@dataclass
class WallSignal:
    trade_date: date
    symbol: str
    signal_type: str          # "CALL_WALL_PUSH" or "PUT_WALL_PUSH"
    wall_strike: float
    open_price: float
    signal_price: float       # candle range that triggered the spike
    signal_time: str          # timestamp of the signal bar


def compute_5min_atr(bars_5min: pd.DataFrame, periods: int = 14) -> pd.Series:
    """
    Standard ATR on 5-min bars using True Range.
    TR = max(H-L, |H-prevC|, |L-prevC|)
    ATR = rolling mean of TR over `periods` bars.
    Returns Series aligned with bars_5min index.
    """
    high = bars_5min["high"]
    low = bars_5min["low"]
    prev_close = bars_5min["close"].shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(window=periods, min_periods=periods).mean()
    return atr


def detect_momentum_signal(
    symbol: str,
    trade_date: date,
    call_wall: float,
    put_wall: float,
    open_price: float,
    bars_5min: pd.DataFrame,
    cfg: BacktestConfig,
) -> Optional[WallSignal]:
    """
    Detect momentum exhaustion via ATR spike on 5-min candles.

    1. Compute ATR(14) on rolling 5-min bars (includes prior days for baseline).
    2. Scan the current day's 9:30-10:30 ET window for any candle where
       candle_range > atr_spike_multiplier * ATR(14).
    3. If found:
         Bullish spike (close > open) → target CALL_WALL → sell bear call spread
         Bearish spike (close < open) → target PUT_WALL → sell bull put spread
    4. First spike wins (one trade per day max).
    """
    if bars_5min.empty:
        return None

    # Compute ATR on all available 5-min bars (includes prior days)
    atr = compute_5min_atr(bars_5min, periods=cfg.atr_lookback_periods)

    # Filter to current day's scan window
    day_bars = bars_5min[bars_5min.index.date == trade_date]
    if day_bars.empty:
        return None

    ref_ts = day_bars.index[0]
    scan_start = ref_ts.replace(
        hour=cfg.scan_start_hour, minute=cfg.scan_start_minute, second=0,
        microsecond=0,
    )
    scan_end = ref_ts.replace(
        hour=cfg.scan_end_hour, minute=cfg.scan_end_minute, second=0,
        microsecond=0,
    )
    window = day_bars[(day_bars.index >= scan_start) & (day_bars.index < scan_end)]
    if window.empty:
        return None

    # Scan for momentum spike
    for ts, bar in window.iterrows():
        current_atr = atr.get(ts)
        if current_atr is None or np.isnan(current_atr) or current_atr <= 0:
            continue

        candle_range = float(bar["high"] - bar["low"])
        if candle_range > cfg.atr_spike_multiplier * current_atr:
            # Determine direction
            candle_close = float(bar["close"])
            candle_open = float(bar["open"])

            if candle_close > candle_open:
                # Bullish spike → target CALL_WALL
                direction = "CALL_WALL_PUSH"
                wall = call_wall
            elif candle_close < candle_open:
                # Bearish spike → target PUT_WALL
                direction = "PUT_WALL_PUSH"
                wall = put_wall
            else:
                # Doji — use high vs low to determine direction
                if float(bar["high"]) - candle_close <= candle_close - float(bar["low"]):
                    direction = "CALL_WALL_PUSH"
                    wall = call_wall
                else:
                    direction = "PUT_WALL_PUSH"
                    wall = put_wall

            return WallSignal(
                trade_date=trade_date,
                symbol=symbol,
                signal_type=direction,
                wall_strike=wall,
                open_price=open_price,
                signal_price=candle_range,
                signal_time=str(ts),
            )

    return None


# ============================================================================
# Section 5: Trade Simulation
# ============================================================================

@dataclass
class TradeResult:
    trade_date: date
    symbol: str
    signal_type: str          # "BEAR_CALL_SPREAD" or "BULL_PUT_SPREAD"
    signal_time: str          # when the momentum spike triggered
    wall_strike: float
    long_strike: float
    spread_width: float
    credit_received: float    # per contract, in dollars
    closing_price: float
    pnl_per_contract: float
    is_winner: bool
    short_option_price: float = 0.0   # real price of short leg at entry
    long_option_price: float = 0.0    # real price of long leg at entry
    used_real_prices: bool = False     # True if real option data was fetched


def simulate_credit_spread(
    signal: WallSignal,
    closing_price: float,
    cfg: BacktestConfig,
    expiry: Optional[date] = None,
) -> TradeResult:
    """
    Simulate a 0DTE credit spread based on the wall signal.

    Attempts to fetch real option prices from Polygon for both legs.
    Falls back to flat credit_per_dollar if real prices are unavailable.

    BEAR_CALL_SPREAD (call wall push):
        Short call at wall, long call at wall + width
        Max profit if close <= wall → keep full credit
        Max loss if close >= wall + width

    BULL_PUT_SPREAD (put wall push):
        Short put at wall, long put at wall - width
        Max profit if close >= wall → keep full credit
        Max loss if close <= wall - width
    """
    width = cfg.spread_widths.get(signal.symbol, 1.0)

    if signal.signal_type == "CALL_WALL_PUSH":
        short_strike = signal.wall_strike
        long_strike = short_strike + width
        spread_type = "BEAR_CALL_SPREAD"
        option_type = "C"
    else:  # PUT_WALL_PUSH
        short_strike = signal.wall_strike
        long_strike = short_strike - width
        spread_type = "BULL_PUT_SPREAD"
        option_type = "P"

    # Try to fetch real option prices
    short_price = None
    long_price = None
    used_real = False

    if expiry is not None:
        short_price = fetch_option_price_at_time(
            signal.symbol, expiry, option_type, short_strike, signal.signal_time,
        )
        time.sleep(0.25)  # rate-limit: 2 calls per trade
        long_price = fetch_option_price_at_time(
            signal.symbol, expiry, option_type, long_strike, signal.signal_time,
        )

    if short_price is not None and long_price is not None and short_price > long_price:
        credit_per_share = short_price - long_price
        credit = credit_per_share * cfg.multiplier
        used_real = True
    else:
        # Fallback to flat credit estimate
        credit_per_share = cfg.credit_per_dollar * width
        credit = credit_per_share * cfg.multiplier
        short_price = short_price or 0.0
        long_price = long_price or 0.0

    # P&L calculation
    if spread_type == "BEAR_CALL_SPREAD":
        if closing_price <= short_strike:
            pnl = credit
        elif closing_price >= long_strike:
            pnl = credit - (width * cfg.multiplier)
        else:
            intrinsic = (closing_price - short_strike) * cfg.multiplier
            pnl = credit - intrinsic
    else:  # BULL_PUT_SPREAD
        if closing_price >= short_strike:
            pnl = credit
        elif closing_price <= long_strike:
            pnl = credit - (width * cfg.multiplier)
        else:
            intrinsic = (short_strike - closing_price) * cfg.multiplier
            pnl = credit - intrinsic

    return TradeResult(
        trade_date=signal.trade_date,
        symbol=signal.symbol,
        signal_type=spread_type,
        signal_time=signal.signal_time,
        wall_strike=short_strike,
        long_strike=long_strike,
        spread_width=width,
        credit_received=round(credit, 2),
        closing_price=closing_price,
        pnl_per_contract=round(pnl, 2),
        is_winner=pnl > 0,
        short_option_price=round(short_price, 4) if short_price else 0.0,
        long_option_price=round(long_price, 4) if long_price else 0.0,
        used_real_prices=used_real,
    )


# ============================================================================
# Section 6: Backtest Engine
# ============================================================================

def run_backtest(cfg: BacktestConfig) -> Tuple[pd.DataFrame, Dict]:
    """
    Run the full backtest across all configured symbols.
    Returns (trade_log DataFrame, summary dict).
    """
    all_trades: List[TradeResult] = []
    total_trading_days = 0
    no_signal_days = 0

    for symbol in cfg.symbols:
        print(f"\n{'='*60}")
        print(f"  Processing {symbol}")
        print(f"{'='*60}")

        # Step 1: Get current GEX levels
        print(f"\n  Step 1: Fetching current GEX levels...")
        try:
            current_walls = get_current_gex_levels(symbol)
        except RuntimeError as e:
            print(f"  ERROR: {e}")
            continue

        # Step 2: Fetch daily bars (120 calendar days to cover 60 trading days)
        print(f"  Step 2: Fetching daily bars...")
        daily_bars = fetch_daily_bars(symbol, calendar_days=120)
        if daily_bars.empty:
            print(f"  WARNING: No daily bars for {symbol}")
            continue

        # Keep only the last N trading days
        daily_bars = daily_bars.tail(cfg.lookback_days)
        print(f"    Got {len(daily_bars)} trading days "
              f"({daily_bars.index[0]} to {daily_bars.index[-1]})")

        # Step 3: Fetch 5-min bars in bulk (includes extra days for ATR baseline)
        print(f"  Step 3: Fetching 5-min bars...")
        intraday_start = daily_bars.index[0] - timedelta(days=cfg.atr_lookback_days + 3)
        intraday_end = daily_bars.index[-1] + timedelta(days=1)
        bars_5min = fetch_5min_bars(symbol, intraday_start, intraday_end)
        if bars_5min.empty:
            print(f"  WARNING: No 5-min bars for {symbol}")
            continue
        print(f"    Got {len(bars_5min)} five-minute bars")

        # Step 4: Compute historical wall estimates
        print(f"  Step 4: Computing historical wall estimates...")
        hist_walls = compute_historical_walls(symbol, current_walls, daily_bars, cfg)
        print(f"    Sample walls (first 3 days):")
        for _, row in hist_walls.head(3).iterrows():
            print(f"      {row['date']}: open={row['open']:.2f}, "
                  f"CW={row['call_wall']:.0f}, PW={row['put_wall']:.0f}")

        # Step 5: Detect signals and simulate trades for each day
        print(f"  Step 5: Scanning for momentum spikes...")
        symbol_trade_count = 0
        symbol_no_signal = 0
        symbol_trade_list: List[TradeResult] = []

        skipped_no_0dte = 0
        for _, day_row in hist_walls.iterrows():
            trade_date = day_row["date"]

            # Skip days without 0DTE for this symbol
            if not has_0dte_expiry(symbol, trade_date):
                skipped_no_0dte += 1
                continue

            total_trading_days += 1

            signal = detect_momentum_signal(
                symbol=symbol,
                trade_date=trade_date,
                call_wall=day_row["call_wall"],
                put_wall=day_row["put_wall"],
                open_price=day_row["open"],
                bars_5min=bars_5min,
                cfg=cfg,
            )

            if signal is None:
                symbol_no_signal += 1
                no_signal_days += 1
                continue

            # Simulate the credit spread using real option prices
            trade = simulate_credit_spread(
                signal, day_row["close"], cfg, expiry=trade_date,
            )
            all_trades.append(trade)
            symbol_trade_list.append(trade)
            symbol_trade_count += 1

            win_loss = "WIN" if trade.is_winner else "LOSS"
            price_tag = "REAL" if trade.used_real_prices else "EST"
            sig_hhmm = trade.signal_time.split(" ")[-1][:5] if " " in trade.signal_time else trade.signal_time[:5]
            print(f"    {trade_date} | {sig_hhmm} | {trade.signal_type:20s} | "
                  f"wall={trade.wall_strike:.0f} | "
                  f"close={trade.closing_price:.2f} | "
                  f"cr=${trade.credit_received:>6.2f} [{price_tag}] | "
                  f"P&L=${trade.pnl_per_contract:+.2f} | {win_loss}")

        print(f"\n  {symbol} summary: {symbol_trade_count} trades, "
              f"{symbol_no_signal} no-signal days, "
              f"{skipped_no_0dte} days skipped (no 0DTE)")

        # Step 6: Plot per-ticker backtest chart
        print(f"  Step 6: Generating chart...")
        chart_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "backtest_results")
        plot_backtest_chart(symbol, bars_5min, hist_walls,
                            symbol_trade_list, chart_dir)

    # Build trade log DataFrame
    if all_trades:
        trade_log = pd.DataFrame([asdict(t) for t in all_trades])
    else:
        trade_log = pd.DataFrame(columns=[
            "trade_date", "symbol", "signal_type", "signal_time",
            "wall_strike", "long_strike", "spread_width",
            "credit_received", "closing_price", "pnl_per_contract",
            "is_winner", "short_option_price", "long_option_price",
            "used_real_prices",
        ])

    # Build summary
    summary = build_summary(trade_log, total_trading_days, no_signal_days)
    return trade_log, summary


def build_summary(trade_log: pd.DataFrame, total_days: int,
                  no_signal_days: int) -> Dict:
    """Compute summary statistics from the trade log."""
    if trade_log.empty:
        return {
            "total_trading_days": total_days,
            "no_signal_days": no_signal_days,
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_pnl": 0.0,
            "profit_factor": 0.0,
            "max_win": 0.0,
            "max_loss": 0.0,
        }

    total_trades = len(trade_log)
    wins = int(trade_log["is_winner"].sum())
    losses = total_trades - wins
    win_rate = wins / total_trades if total_trades > 0 else 0.0
    total_pnl = float(trade_log["pnl_per_contract"].sum())
    avg_pnl = float(trade_log["pnl_per_contract"].mean())

    gross_profit = float(trade_log.loc[trade_log["pnl_per_contract"] > 0,
                                       "pnl_per_contract"].sum())
    gross_loss = abs(float(trade_log.loc[trade_log["pnl_per_contract"] <= 0,
                                         "pnl_per_contract"].sum()))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    max_win = float(trade_log["pnl_per_contract"].max())
    max_loss = float(trade_log["pnl_per_contract"].min())

    # Per-symbol breakdown
    by_symbol = {}
    for sym in trade_log["symbol"].unique():
        sym_df = trade_log[trade_log["symbol"] == sym]
        sym_wins = int(sym_df["is_winner"].sum())
        sym_total = len(sym_df)
        real_count = int(sym_df["used_real_prices"].sum()) if "used_real_prices" in sym_df.columns else 0
        avg_credit = round(float(sym_df["credit_received"].mean()), 2) if sym_total > 0 else 0.0
        by_symbol[sym] = {
            "trades": sym_total,
            "wins": sym_wins,
            "losses": sym_total - sym_wins,
            "win_rate": round(sym_wins / sym_total, 4) if sym_total > 0 else 0.0,
            "total_pnl": round(float(sym_df["pnl_per_contract"].sum()), 2),
            "avg_credit": avg_credit,
            "real_prices": real_count,
        }

    return {
        "total_trading_days": total_days,
        "no_signal_days": no_signal_days,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 4),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(avg_pnl, 2),
        "profit_factor": round(profit_factor, 4),
        "max_win": round(max_win, 2),
        "max_loss": round(max_loss, 2),
        "by_symbol": by_symbol,
    }


# ============================================================================
# Section 7: Output
# ============================================================================

def plot_backtest_chart(
    symbol: str,
    bars_5min: pd.DataFrame,
    hist_walls: pd.DataFrame,
    symbol_trades: List,
    output_dir: str,
):
    """
    Per-ticker backtest chart matching gex_plot_w_indicators.py style.
    5-min candles with daily GEX walls and trade entry markers.
    """
    if bars_5min.empty or hist_walls.empty:
        return

    # Filter 5-min bars to only the backtest dates
    backtest_dates = set(hist_walls["date"].tolist())
    mask = bars_5min.index.map(lambda t: t.date() in backtest_dates)
    ohlc = bars_5min[mask].copy()
    if ohlc.empty:
        return

    # Rename columns for mplfinance
    ohlc = ohlc.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    })

    # Build wall overlay series aligned with 5-min bars (stepped by day)
    call_wall_series = pd.Series(np.nan, index=ohlc.index)
    put_wall_series = pd.Series(np.nan, index=ohlc.index)
    for _, row in hist_walls.iterrows():
        day = row["date"]
        day_mask = ohlc.index.date == day
        call_wall_series[day_mask] = row["call_wall"]
        put_wall_series[day_mask] = row["put_wall"]

    # Build addplot overlays
    apds = []
    apds.append(mpf.make_addplot(
        call_wall_series, type="line", color="red",
        linestyle="--", width=1.5, alpha=0.9,
    ))
    apds.append(mpf.make_addplot(
        put_wall_series, type="line", color="green",
        linestyle="--", width=1.5, alpha=0.9,
    ))

    # Trade entry markers at the wall strike level
    # Bull put spread (PUT_WALL_PUSH) → ^  up-arrow at put wall (betting price stays above)
    # Bear call spread (CALL_WALL_PUSH) → v down-arrow at call wall (betting price stays below)
    # Color: lime = win, red = loss
    bull_win = pd.Series(np.nan, index=ohlc.index)   # ^ lime
    bull_loss = pd.Series(np.nan, index=ohlc.index)  # ^ red
    bear_win = pd.Series(np.nan, index=ohlc.index)   # v lime
    bear_loss = pd.Series(np.nan, index=ohlc.index)  # v red
    for t in symbol_trades:
        try:
            sig_ts = pd.Timestamp(t.signal_time)
            idx = ohlc.index.get_indexer([sig_ts], method="nearest")[0]
            if 0 <= idx < len(ohlc):
                is_bull = t.signal_type == "BULL_PUT_SPREAD"
                if is_bull and t.is_winner:
                    bull_win.iloc[idx] = t.wall_strike
                elif is_bull and not t.is_winner:
                    bull_loss.iloc[idx] = t.wall_strike
                elif not is_bull and t.is_winner:
                    bear_win.iloc[idx] = t.wall_strike
                else:
                    bear_loss.iloc[idx] = t.wall_strike
        except Exception:
            continue

    if bull_win.notna().any():
        apds.append(mpf.make_addplot(
            bull_win, type="scatter", marker="^",
            markersize=60, color="lime",
        ))
    if bull_loss.notna().any():
        apds.append(mpf.make_addplot(
            bull_loss, type="scatter", marker="^",
            markersize=60, color="red",
        ))
    if bear_win.notna().any():
        apds.append(mpf.make_addplot(
            bear_win, type="scatter", marker="v",
            markersize=60, color="lime",
        ))
    if bear_loss.notna().any():
        apds.append(mpf.make_addplot(
            bear_loss, type="scatter", marker="v",
            markersize=60, color="red",
        ))

    # Style: exact match to gex_plot_w_indicators.py
    mc = mpf.make_marketcolors(up="green", down="red", inherit=True)
    style = mpf.make_mpf_style(
        base_mpf_style="yahoo",
        marketcolors=mc,
        gridcolor="#2a2e35",
        gridstyle="--",
        rc={
            "figure.facecolor": "#0b0b0b",
            "axes.facecolor": "#111214",
            "axes.labelcolor": "white",
            "axes.titlecolor": "white",
            "xtick.color": "white",
            "ytick.color": "white",
            "text.color": "white",
        },
    )

    # Session separators (vertical lines at day opens)
    days = ohlc.index.normalize().unique()
    daily_vlines = []
    for d in days:
        first = ohlc.index[ohlc.index.normalize() == d].min()
        if pd.notna(first):
            daily_vlines.append(first)

    # Y-limits with padding — expand to include wall levels
    pr_hi = float(ohlc["High"].max())
    pr_lo = float(ohlc["Low"].min())
    cw_max = call_wall_series.max()
    pw_min = put_wall_series.min()
    if not np.isnan(cw_max):
        pr_hi = max(pr_hi, cw_max)
    if not np.isnan(pw_min):
        pr_lo = min(pr_lo, pw_min)
    pad = max(0.01, 0.08 * (pr_hi - pr_lo))
    ylim = (pr_lo - pad, pr_hi + pad)

    fig, axlist = mpf.plot(
        ohlc,
        type="candle",
        style=style,
        volume=True,
        panel_ratios=(3, 1),
        title=f"{symbol} — GEX Wall Rejection Backtest",
        ylabel="Price",
        ylabel_lower="Volume",
        vlines=dict(vlines=daily_vlines, colors="gray",
                    linestyle="--", alpha=0.5),
        addplot=apds if apds else None,
        figscale=1.5,
        warn_too_much_data=len(ohlc) + 1,
        ylim=ylim,
        datetime_format="%b %d %H:%M",
        xrotation=0,
        returnfig=True,
    )

    price_ax = axlist[0]
    vol_ax = axlist[2] if len(axlist) > 2 else axlist[-1]

    # Move price y-axis ticks to left (gamma labels on right)
    price_ax.yaxis.set_label_position("left")
    price_ax.yaxis.tick_left()

    # Enforce white labels/ticks
    price_ax.set_title(price_ax.get_title(), color="white", fontweight="bold")
    price_ax.set_ylabel("Price", color="white")
    vol_ax.set_ylabel("Volume", color="white")
    for ax in (price_ax, vol_ax):
        ax.tick_params(axis="x", colors="white")
        ax.tick_params(axis="y", colors="white")

    # Panel backgrounds
    price_ax.set_facecolor("#111214")
    vol_ax.set_facecolor("#0f1012")

    # Win/loss count for legend
    n_wins = sum(1 for t in symbol_trades if t.is_winner)
    n_losses = len(symbol_trades) - n_wins

    # Legend
    legend_elems = [
        Line2D([0], [0], color="red", linestyle="--", lw=1.5,
               label="Call Wall"),
        Line2D([0], [0], color="green", linestyle="--", lw=1.5,
               label="Put Wall"),
        Line2D([0], [0], marker="^", color="lime", linestyle="None",
               markersize=6, label=f"Bull Put Win"),
        Line2D([0], [0], marker="v", color="lime", linestyle="None",
               markersize=6, label=f"Bear Call Win"),
        Line2D([0], [0], marker="^", color="red", linestyle="None",
               markersize=6, label=f"Bull Put Loss"),
        Line2D([0], [0], marker="v", color="red", linestyle="None",
               markersize=6, label=f"Bear Call Loss"),
    ]
    price_ax.legend(handles=legend_elems, loc="upper left",
                    frameon=False, fontsize=8, labelcolor="white",
                    ncol=2)

    fig.tight_layout()

    # Save to file
    os.makedirs(output_dir, exist_ok=True)
    chart_path = os.path.join(output_dir, f"{symbol}_backtest.png")
    fig.savefig(chart_path, dpi=150, facecolor="#0b0b0b")
    plt.close(fig)
    print(f"  Saved chart: {chart_path}")

def print_results(trade_log: pd.DataFrame, summary: Dict):
    """Pretty-print trade log and summary stats to console."""
    print("\n")
    print("=" * 80)
    print("  MOMENTUM-TRIGGERED GEX WALL 0DTE CREDIT SPREAD — BACKTEST RESULTS")
    print("=" * 80)

    if trade_log.empty:
        print("\n  No trades generated.")
        return

    # Trade log table
    print("\n  TRADE LOG")
    print("  " + "-" * 100)
    print(f"  {'Date':<12} {'Time':<6} {'Sym':<6} {'Type':<22} {'Wall':>7} "
          f"{'Close':>8} {'Short':>7} {'Long':>7} {'Credit':>8} "
          f"{'P&L':>9} {'Src':<4} {'Result':<6}")
    print("  " + "-" * 100)

    for _, row in trade_log.iterrows():
        result = "WIN" if row["is_winner"] else "LOSS"
        td = row["trade_date"]
        if isinstance(td, date):
            td = td.isoformat()
        sig_t = str(row.get("signal_time", ""))
        sig_hhmm = sig_t.split(" ")[-1][:5] if " " in sig_t else sig_t[:5]
        price_tag = "REAL" if row.get("used_real_prices") else "EST"
        short_px = row.get("short_option_price", 0.0) or 0.0
        long_px = row.get("long_option_price", 0.0) or 0.0
        print(f"  {td:<12} {sig_hhmm:<6} {row['symbol']:<6} {row['signal_type']:<22} "
              f"{row['wall_strike']:>7.0f} {row['closing_price']:>8.2f} "
              f"${short_px:>5.2f} ${long_px:>5.2f} "
              f"${row['credit_received']:>6.2f} ${row['pnl_per_contract']:>+8.2f} "
              f"{price_tag:<4} {result:<6}")

    print("  " + "-" * 100)

    # Real vs estimated pricing summary
    if "used_real_prices" in trade_log.columns:
        n_real = int(trade_log["used_real_prices"].sum())
        n_est = len(trade_log) - n_real
        print(f"\n  Pricing: {n_real} trades with real option prices, "
              f"{n_est} trades with estimated (fallback) pricing")

    # Summary stats
    print(f"\n  SUMMARY")
    print(f"  " + "-" * 40)
    print(f"  Total trading days:   {summary['total_trading_days']}")
    print(f"  No-signal days:       {summary['no_signal_days']}")
    print(f"  Total trades:         {summary['total_trades']}")
    print(f"  Wins / Losses:        {summary['wins']} / {summary['losses']}")
    print(f"  Win rate:             {summary['win_rate']:.1%}")
    print(f"  Total P&L:            ${summary['total_pnl']:+,.2f}")
    print(f"  Avg P&L per trade:    ${summary['avg_pnl']:+,.2f}")
    print(f"  Profit factor:        {summary['profit_factor']:.2f}")
    print(f"  Max win:              ${summary['max_win']:+,.2f}")
    print(f"  Max loss:             ${summary['max_loss']:+,.2f}")

    # Per-symbol
    if "by_symbol" in summary:
        print(f"\n  PER-SYMBOL BREAKDOWN")
        print(f"  " + "-" * 40)
        for sym, stats in summary["by_symbol"].items():
            print(f"  {sym}: {stats['trades']} trades | "
                  f"{stats['wins']}W/{stats['losses']}L | "
                  f"WR={stats['win_rate']:.1%} | "
                  f"P&L=${stats['total_pnl']:+,.2f} | "
                  f"avg cr=${stats.get('avg_credit', 0):,.2f} | "
                  f"real={stats.get('real_prices', 0)}")

    print()


def save_results(trade_log: pd.DataFrame, summary: Dict,
                 output_dir: str = None):
    """Save trade_log.csv and summary.json to output_dir."""
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "backtest_results")
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, "trade_log.csv")
    json_path = os.path.join(output_dir, "summary.json")

    trade_log.to_csv(csv_path, index=False)
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"  Saved trade log:  {csv_path}")
    print(f"  Saved summary:    {json_path}")


# ============================================================================
# Main
# ============================================================================

def main():
    cfg = BacktestConfig()

    print("Momentum-Triggered GEX Wall 0DTE Credit Spread Backtest")
    print(f"Symbols: {cfg.symbols}")
    print(f"Lookback: {cfg.lookback_days} trading days")
    print(f"Scan window: {cfg.scan_start_hour}:{cfg.scan_start_minute:02d}"
          f"-{cfg.scan_end_hour}:{cfg.scan_end_minute:02d} ET")
    print(f"ATR: {cfg.atr_lookback_periods}-period on {cfg.atr_candle_minutes}-min candles, "
          f"spike > {cfg.atr_spike_multiplier}x")
    print(f"Spread widths: {cfg.spread_widths}")
    print(f"Fallback credit per $1 width: ${cfg.credit_per_dollar:.2f}/share (used when real prices unavailable)")

    trade_log, summary = run_backtest(cfg)
    print_results(trade_log, summary)
    save_results(trade_log, summary)


if __name__ == "__main__":
    main()
