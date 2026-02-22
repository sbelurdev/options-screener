from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def compute_technicals(price_df: pd.DataFrame) -> Dict[str, float]:
    close = price_df["Close"].astype(float).copy()

    ma20 = close.rolling(20).mean().iloc[-1]
    ma50 = close.rolling(50).mean().iloc[-1]

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
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
