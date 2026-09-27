"""
Indicator maths — PURE functions over DataFrames. No network, no I/O, no clock.

Daily:    MACD(12,26,9), RSI(14) Wilder, ATR(14) Wilder, SMA(20), avg volume(20)
          — daily RSI/MACD are context only now, shown but never gating; see
          signals.evaluate_daily_gate.
Intraday: opening range, session VWAP (anchored 09:30), overnight high/low,
          time-of-day volume baseline, RSI(14) on 5-minute bars and again on
          bars resampled to 15-minute (intraday_rsi) — this is the real
          momentum gate, checked in signals.evaluate_trigger.

All exponential and Wilder smoothing uses `adjust=False`, matching what charting
platforms display. See spec §6.1 and §6.2.

Bar-labelling convention (verified empirically, asserted in tests): yfinance 5m
bars are LEFT-edge labelled — the bar stamped 09:30 covers 09:30-09:35. So the
opening range "09:30, 09:35, 09:40 bars" spans 09:30-09:45.

Extended-hours caveat (spec §1.3 audit): yfinance returns extended-hours bars
with valid OHLC but volume identically zero, and no bars at all between 20:00
and 03:59 ET. Overnight levels are therefore computed over the covered window
only — see `overnight_levels`, which reports the window it actually used.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from agent.daytrading.calendar import ET

# Regular trading hours, by bar label. 15:55 is the last 5m bar of the session.
RTH_START = "09:30"
RTH_LAST_BAR = "15:55"


# ── daily ────────────────────────────────────────────────────────────────────

def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """Returns (macd_line, macd_signal, macd_hist)."""
    macd_line = ema(close, fast) - ema(close, slow)
    macd_signal = ema(macd_line, signal)
    return macd_line, macd_signal, macd_line - macd_signal


def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    """Wilder's RSI.

    `min_periods=n` matches the existing agent/signals/technicals.py behaviour:
    the first n-1 values are NaN rather than a misleading warm-up number.

    Zero-loss handling matters for the DayTrading gate: with no down moves the
    ratio is undefined, but the correct RSI is 100 (and 50 when the series is
    flat), not NaN. Returning NaN would make an unbroken advance — exactly the
    momentum this strategy looks for — read as "indicator unavailable" and
    silently drop the name into a degraded state.
    """
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, min_periods=n, adjust=False).mean()

    rs = up / dn.replace(0, np.nan)
    out = 100 - 100 / (1 + rs)

    warm = up.notna() & dn.notna()
    out = out.mask(warm & (dn == 0) & (up > 0), 100.0)   # only gains
    out = out.mask(warm & (dn == 0) & (up == 0), 50.0)   # perfectly flat
    return out


def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    """Wilder's ATR."""
    pc = close.shift()
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()


def sma(close: pd.Series, n: int = 20) -> pd.Series:
    return close.rolling(n).mean()


def trend_ma(close: pd.Series, ma_type: str = "SMA", period: int = 20) -> pd.Series:
    """The moving average the daily trend gate compares close against.

    Configurable because the choice is a judgement call, not a fact: a shorter
    or exponential average turns faster but sits closer to price, which in a
    rising market makes the gate STRICTER, not looser (measured: SMA20 passes
    45% of ticker-days, EMA9 34%).
    """
    kind = str(ma_type).strip().upper()
    if kind == "EMA":
        return ema(close, period)
    if kind == "SMA":
        return sma(close, period)
    raise ValueError(f"trend_ma_type must be 'SMA' or 'EMA'; got {ma_type!r}")


@dataclass(frozen=True)
class DailyIndicators:
    """Snapshot of the daily indicators as of the last completed session."""
    as_of: date
    close: float             # adjusted close (indicator basis)
    rsi_14: Optional[float]
    macd_line: Optional[float]
    macd_signal: Optional[float]
    macd_hist: Optional[float]
    trend_ma: Optional[float]            # the gate's moving average
    trend_ma_label: str                  # e.g. "SMA20" / "EMA9", for display
    atr_14: Optional[float]
    avg_volume_20: Optional[float]
    trend_ma_distance_pct: Optional[float]  # (close - trend_ma) / trend_ma


def _last_float(s: pd.Series) -> Optional[float]:
    if s is None or len(s) == 0:
        return None
    v = s.iloc[-1]
    return float(v) if pd.notna(v) else None


def daily_indicators(
    daily_adj: pd.DataFrame,
    trend_ma_type: str = "SMA",
    trend_ma_period: int = 20,
) -> Optional[DailyIndicators]:
    """Compute daily indicators from an ADJUSTED OHLCV frame (spec §1.2 rule).

    The trend moving average is configurable (type and period); everything else
    is fixed. Reference levels (prev_close etc.) must come from the RAW frame —
    see `prev_close_raw`.
    """
    if daily_adj is None or daily_adj.empty:
        return None
    df = daily_adj.dropna(subset=["Close"])
    if df.empty:
        return None

    close = df["Close"].astype(float)
    line, sig, hist = macd(close)
    ma = trend_ma(close, trend_ma_type, trend_ma_period)

    c = float(close.iloc[-1])
    ma_val = _last_float(ma)
    ma_label = f"{str(trend_ma_type).strip().upper()}{int(trend_ma_period)}"

    idx = df.index[-1]
    as_of = idx.date() if isinstance(idx, (pd.Timestamp, datetime)) else idx

    return DailyIndicators(
        as_of=as_of,
        close=c,
        rsi_14=_last_float(rsi(close, 14)),
        macd_line=_last_float(line),
        macd_signal=_last_float(sig),
        macd_hist=_last_float(hist),
        trend_ma=ma_val,
        trend_ma_label=ma_label,
        atr_14=_last_float(atr(df["High"].astype(float), df["Low"].astype(float), close, 14)),
        avg_volume_20=_last_float(df["Volume"].astype(float).rolling(20).mean())
        if "Volume" in df.columns else None,
        trend_ma_distance_pct=((c - ma_val) / ma_val) if (ma_val and ma_val > 0) else None,
    )


def prev_close_raw(daily_raw: pd.DataFrame) -> Optional[float]:
    """Previous session's close from the RAW (unadjusted) frame.

    Reference level: compared against a live quote, so it must be the actual
    print, never a back-adjusted value (spec §1.2).
    """
    if daily_raw is None or daily_raw.empty:
        return None
    c = daily_raw["Close"].astype(float).dropna()
    return float(c.iloc[-1]) if len(c) else None


# ── intraday ─────────────────────────────────────────────────────────────────

def to_et(bars: pd.DataFrame) -> pd.DataFrame:
    """Normalise a bar frame's index to tz-aware ET. Idempotent."""
    if bars is None or bars.empty:
        return bars
    out = bars.copy()
    idx = out.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.DatetimeIndex(idx)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    out.index = idx.tz_convert(ET)
    return out


def session_bars(bars_et: pd.DataFrame, day: date) -> pd.DataFrame:
    """Regular-session bars (09:30-15:55) for one day. Index must already be ET."""
    if bars_et is None or bars_et.empty:
        return bars_et if bars_et is not None else pd.DataFrame()
    same_day = bars_et[bars_et.index.normalize() == pd.Timestamp(day, tz=ET)]
    if same_day.empty:
        return same_day
    return same_day.between_time(RTH_START, RTH_LAST_BAR)


def intraday_rsi(bars: pd.DataFrame, resample: Optional[str] = None, n: int = 14) -> pd.Series:
    """Wilder RSI(n) on `bars`' Close, optionally resampled first (e.g.
    "15min" for a 15-minute reading built from 5-minute bars).

    Takes the FULL multi-day intraday series (TickerData.intraday, not the
    single-session `rth` slice) — a same-day-only series has at most a
    handful of bars before mid-morning, nowhere near enough for a stable
    14-period Wilder reading. Using the rolling multi-day history means a
    valid RSI exists from the very first tradeable bar of the day, the same
    way any charting platform's intraday RSI works across session
    boundaries rather than resetting to zero at each open.

    The resampled series is left-labelled (a bin labelled 09:30 covers
    09:30-09:45, matching the 5-minute convention this module already
    uses) — callers reading a still-forming bin must use
    `last_completed_value` below, not a raw index lookup, or they risk
    reading a bin before its data has actually finished arriving.
    """
    if bars is None or bars.empty:
        return pd.Series(dtype=float)
    close = bars["Close"]
    if resample:
        agg = bars.resample(resample, label="left", closed="left").agg(
            {"Close": "last"}
        ).dropna(subset=["Close"])
        close = agg["Close"]
    return rsi(close, n)


def last_completed_value(
    series: pd.Series, as_of: datetime, bar_duration: timedelta
) -> Optional[float]:
    """The most recent entry in a left-labelled `series` whose bin has fully
    completed by `as_of` — mirrors signals.completed_bars' exact rule
    (label + duration <= as_of) so a still-forming bin (e.g. a 15-minute bin
    only 5 or 10 minutes into its window) is never read early."""
    if series is None or series.empty:
        return None
    complete = series[series.index + bar_duration <= as_of]
    if complete.empty:
        return None
    val = complete.iloc[-1]
    return float(val) if pd.notna(val) else None


@dataclass(frozen=True)
class OpeningRange:
    or_high: float
    or_low: float
    or_height: float
    or_avg_volume: float
    bar_count: int


def opening_range(
    rth: pd.DataFrame, or_start: str = "09:30", or_end: str = "09:40"
) -> Optional[OpeningRange]:
    """Opening range over the bars stamped or_start..or_end inclusive.

    With left-edge labelling, the default 09:30-09:40 means three bars covering
    09:30-09:45.
    """
    if rth is None or rth.empty:
        return None
    orb = rth.between_time(or_start, or_end)
    if orb.empty:
        return None
    hi = float(orb["High"].max())
    lo = float(orb["Low"].min())
    return OpeningRange(
        or_high=hi,
        or_low=lo,
        or_height=hi - lo,
        or_avg_volume=float(orb["Volume"].mean()),
        bar_count=len(orb),
    )


def session_vwap(rth: pd.DataFrame) -> pd.Series:
    """Session VWAP anchored at 09:30 — NOT anchored to the pre-market open.

    5-minute typical price tracks 1-minute VWAP to ~0.04bp, so 5m bars suffice.
    """
    if rth is None or rth.empty:
        return pd.Series(dtype=float)
    tp = (rth["High"].astype(float) + rth["Low"].astype(float) + rth["Close"].astype(float)) / 3
    vol = rth["Volume"].astype(float)
    cum_vol = vol.cumsum()
    return (tp * vol).cumsum() / cum_vol.replace(0, np.nan)


@dataclass(frozen=True)
class OvernightLevels:
    """Overnight extremes plus the window actually covered by the data.

    yfinance provides no bars between 20:00 and 03:59 ET, so the nominal
    16:00->09:29:59 window is only partially covered. `covered_window` is the
    human-readable description of what these numbers are really based on, and
    the UI displays it alongside the level (decision recorded in spec §1.3
    follow-up: use the partial window, label it).
    """
    overnight_high: Optional[float]
    overnight_low: Optional[float]
    premarket_last: Optional[float]
    covered_window: str
    bar_count: int


def overnight_levels(
    bars_et: pd.DataFrame, prev_session: date, day: date
) -> OvernightLevels:
    """Overnight high/low from prev_session 16:00 to `day` 09:29:59 ET.

    Note: volume is deliberately not aggregated here. yfinance reports zero
    volume on every extended-hours bar, so any pre-market volume figure would be
    a constant 0 — it is omitted rather than displayed as a misleading number.
    """
    empty = OvernightLevels(None, None, None, "no extended-hours bars", 0)
    if bars_et is None or bars_et.empty:
        return empty

    start = pd.Timestamp(datetime.combine(prev_session, time(16, 0)), tz=ET)
    end = pd.Timestamp(datetime.combine(day, time(9, 29, 59)), tz=ET)
    on = bars_et[(bars_et.index >= start) & (bars_et.index <= end)]
    if on.empty:
        return empty

    first, last = on.index.min(), on.index.max()
    return OvernightLevels(
        overnight_high=float(on["High"].max()),
        overnight_low=float(on["Low"].min()),
        premarket_last=float(on["Close"].iloc[-1]),
        covered_window=f"{first:%Y-%m-%d %H:%M}-{last:%H:%M} ET",
        bar_count=len(on),
    )


def time_of_day_volume_baseline(
    bars_et: pd.DataFrame, day: date, lookback_sessions: int = 20
) -> pd.Series:
    """Mean regular-session volume per 5m slot over the prior N sessions.

    Volume at 09:50 is structurally higher than at 14:00 in every stock every
    day, so a flat volume threshold passes everything in the morning. This is
    the baseline that makes relative volume meaningful.
    """
    if bars_et is None or bars_et.empty:
        return pd.Series(dtype=float)

    tod = bars_et.between_time(RTH_START, RTH_LAST_BAR)
    if tod.empty:
        return pd.Series(dtype=float)

    prior = tod[tod.index.normalize() < pd.Timestamp(day, tz=ET)]
    if prior.empty:
        return pd.Series(dtype=float)

    sessions = sorted(set(prior.index.normalize()))[-lookback_sessions:]
    prior = prior[prior.index.normalize().isin(sessions)]

    slot = prior.index.strftime("%H:%M")
    return prior.groupby(slot)["Volume"].mean()


def relative_volume(bar_volume: float, slot: str, baseline: pd.Series) -> Optional[float]:
    """Bar volume / its time-of-day baseline. Context only, never a gate."""
    if baseline is None or len(baseline) == 0 or slot not in baseline.index:
        return None
    base = float(baseline.loc[slot])
    if base <= 0:
        return None
    return float(bar_volume) / base
