from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd


def compute_technicals(price_df: pd.DataFrame) -> Dict[str, float]:
    # yfinance can return a NaN close for the current (incomplete) session —
    # drop those rows so spot and every indicator use the last valid close.
    close = price_df["Close"].astype(float).dropna().copy()
    if close.empty:
        # Spot 0 routes callers into their existing "spot unavailable" handling
        return {"spot": 0.0, "ma20": 0.0, "ma50": 0.0, "rsi14": 50.0, "hv20": 0.25}

    ma20 = close.rolling(20).mean().iloc[-1]
    ma50 = close.rolling(50).mean().iloc[-1]

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    # Wilder's smoothing (alpha=1/14) matches RSI values shown on most platforms
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    ret = close.pct_change().dropna()
    hv20 = ret.rolling(20).std().iloc[-1] * np.sqrt(252) if len(ret) >= 20 else np.nan

    return {
        "spot": float(close.iloc[-1]),
        "ma20": float(ma20) if pd.notna(ma20) else float(close.iloc[-1]),
        "ma50": float(ma50) if pd.notna(ma50) else float(close.iloc[-1]),
        "rsi14": float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50.0,
        "hv20": float(hv20) if pd.notna(hv20) else 0.25,
    }


def classify_regime(spot: float, ma20: float, ma50: float, rsi14: float) -> Tuple[str, str]:
    """A real, varying Bullish/Neutral/Bearish label from the same inputs
    agent/scoring/score.py's trend sub-score already uses — that sub-score's
    "reason" was a hardcoded string that never actually changed with the
    computed value, so there was no reusable label anywhere. This is that
    label, for the per-ticker context panel.

    Bullish: above both MAs and not overbought. Bearish: below both MAs, or
    overbought while below the long MA (stretched with no support underneath).
    Everything else — mixed MA signals, or overbought while still trending up
    — is Neutral: informative context, not a directional call.
    """
    above20 = spot > ma20
    above50 = spot > ma50
    overbought = rsi14 > 75
    oversold = rsi14 < 30

    if above20 and above50 and not overbought:
        return "Bullish", f"close above both MA20 (${ma20:,.2f}) and MA50 (${ma50:,.2f})"
    if not above20 and not above50:
        reason = f"close below both MA20 (${ma20:,.2f}) and MA50 (${ma50:,.2f})"
        if oversold:
            reason += f"; RSI {rsi14:.0f} oversold"
        return "Bearish", reason
    if overbought and not above50:
        return "Bearish", f"RSI {rsi14:.0f} overbought while below MA50 (${ma50:,.2f})"
    if overbought:
        return "Neutral", f"RSI {rsi14:.0f} overbought — trend intact but stretched"
    return "Neutral", "mixed signal — above one MA, below the other"
