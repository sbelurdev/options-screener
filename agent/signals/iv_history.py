"""
ATM IV snapshot persistence and true IV Rank.

Each pipeline run records one ATM IV observation per ticker (from the
expiration nearest 30 DTE — the IV30 convention) into a small CSV shared
across profiles. Once enough daily observations accumulate, a true IV Rank
can be computed:

    IV Rank = (current IV − 1yr low IV) / (1yr high IV − 1yr low IV) × 100

Until min_observations distinct days exist for a ticker, callers fall back
to the HV-rank proxy (compute_ivr_proxy).
"""

from __future__ import annotations

import math
import threading
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

IV_HISTORY_COLUMNS = ["date", "ticker", "atm_iv", "dte_used", "spot"]

# Tickers are processed concurrently but share one history file
_HISTORY_LOCK = threading.Lock()


def _load_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=IV_HISTORY_COLUMNS)
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=IV_HISTORY_COLUMNS)
    for col in IV_HISTORY_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[IV_HISTORY_COLUMNS]


def estimate_atm_iv(calls_df: Optional[pd.DataFrame], puts_df: Optional[pd.DataFrame], spot: float) -> Optional[float]:
    """Mean IV of the strike nearest spot, averaged across both sides of the chain."""
    if spot is None or not math.isfinite(float(spot)) or spot <= 0:
        return None
    ivs = []
    for df in (puts_df, calls_df):
        if df is None or getattr(df, "empty", True):
            continue
        if "strike" not in df.columns or "impliedVolatility" not in df.columns:
            continue
        strikes = pd.to_numeric(df["strike"], errors="coerce")
        iv_col = pd.to_numeric(df["impliedVolatility"], errors="coerce")
        diffs = (strikes - float(spot)).abs()
        diffs = diffs[strikes.notna() & iv_col.notna() & (iv_col > 0)]
        if diffs.empty:
            continue
        ivs.append(float(iv_col.loc[diffs.idxmin()]))
    if not ivs:
        return None
    return sum(ivs) / len(ivs)


def record_iv_snapshot(
    path: Path,
    ticker: str,
    run_date: date,
    atm_iv: float,
    dte_used: int,
    spot: float,
    logger,
) -> None:
    """Upsert today's ATM IV observation — one row per (date, ticker), last run of the day wins."""
    with _HISTORY_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        df = _load_history(path)
        df = df[~((df["date"] == run_date.isoformat()) & (df["ticker"] == ticker))]
        row = pd.DataFrame(
            [
                {
                    "date": run_date.isoformat(),
                    "ticker": ticker,
                    "atm_iv": round(float(atm_iv), 6),
                    "dte_used": int(dte_used),
                    "spot": round(float(spot), 4),
                }
            ]
        )
        df = pd.concat([df, row], ignore_index=True)
        df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
        df.to_csv(path, index=False)
    logger.info("%s: recorded ATM IV %.1f%% (dte=%d) to %s", ticker, atm_iv * 100, dte_used, path)


def compute_true_iv_rank(
    path: Path,
    ticker: str,
    current_iv: float,
    min_observations: int = 20,
    lookback_days: int = 365,
) -> Tuple[Optional[float], str]:
    """
    True IV Rank over recorded snapshots within lookback_days.
    Returns (rank 0–100, source note), or (None, reason) when history is still
    too short — callers should then fall back to the HV-rank proxy.
    """
    with _HISTORY_LOCK:
        df = _load_history(path)
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    hist = df[(df["ticker"] == ticker) & (df["date"] >= cutoff)]
    obs = hist["atm_iv"].astype(float).dropna()
    if len(obs) < min_observations:
        return None, f"IV history {len(obs)}/{min_observations} days — using HV-rank proxy"

    iv_low = float(obs.min())
    iv_high = float(obs.max())
    if iv_high <= iv_low or iv_high < 1e-6:
        return None, "IV history range too flat — using HV-rank proxy"

    rank = max(0.0, min(100.0, (float(current_iv) - iv_low) / (iv_high - iv_low) * 100.0))
    span_days = len(obs)
    return round(rank, 1), f"true IV rank ({span_days} obs; IV={current_iv * 100:.0f}%, range {iv_low * 100:.0f}–{iv_high * 100:.0f}%)"
