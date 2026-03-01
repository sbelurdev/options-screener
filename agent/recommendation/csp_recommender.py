"""
Cash-Secured Put (CSP) Recommendation Engine.

Analyses monthly-bucket PUT candidates per ticker and produces actionable
trade recommendations. All criteria are configurable under csp_recommendation
in config.yaml.

Recommendation logic — a CSP is recommended only when ALL of the following hold:
  1. IV Rank (IVR) > ivr_min (default 30%)  — premium is elevated enough to sell
  2. No earnings announcement within earnings_buffer_days (default 7) of expiration
  3. Strike at or below a key support level (52-week low, 20-day swing low, or ≥5% OTM)
  4. Delta of the strike is between delta_min and delta_max (default 0.10–0.25 abs)

Criteria 1 and 3 are optional (use_support_filter, ivr_min can be set to 0 to skip).
If IVR data is not derivable, the trade is flagged Borderline rather than rejected.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


DEFAULT_REC_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "max_recommendations": 10,
    "ivr_min": 30.0,
    "earnings_buffer_days": 7,
    "delta_min": 0.10,
    "delta_max": 0.25,
    "use_support_filter": True,
    "support_pct_buffer": 0.02,
}


# ── IVR proxy ──────────────────────────────────────────────────────────────────

def compute_ivr_proxy(
    price_df: pd.DataFrame,
    current_iv: Optional[float],
) -> Tuple[Optional[float], str]:
    """
    IV Rank proxy using a rolling 20-day annualised HV series.

    Formula:  IVR = (current_val - hv_low) / (hv_high - hv_low) * 100

    current_val = option IV when available, else latest 20-day HV.
    Uses whatever price history is provided (typically 6 months).

    Returns (ivr 0–100 or None, human-readable source note).
    """
    if price_df is None or price_df.empty or len(price_df) < 25:
        return None, "insufficient price history for IVR proxy"

    close = price_df["Close"].astype(float)
    ret = close.pct_change().dropna()
    hv_series = ret.rolling(20).std().dropna() * math.sqrt(252)

    if len(hv_series) < 5:
        return None, "insufficient HV data for IVR proxy"

    hv_low = float(hv_series.min())
    hv_high = float(hv_series.max())

    if hv_high <= hv_low or hv_high < 1e-6:
        return None, "HV range too flat for IVR proxy"

    if current_iv is not None and current_iv > 0:
        current_val = current_iv
        source = "proxy: option IV vs period HV range"
    else:
        current_val = float(hv_series.iloc[-1])
        source = "proxy: current HV vs period HV range (option IV unavailable)"

    ivr = max(0.0, min(100.0, (current_val - hv_low) / (hv_high - hv_low) * 100.0))
    return round(ivr, 1), source


# ── Support levels ─────────────────────────────────────────────────────────────

def get_support_levels(price_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """Return 52-week low and 20-day swing low from available price history."""
    if price_df is None or price_df.empty:
        return {"low_52w": None, "swing_low_20d": None}

    low_col = (
        price_df["Low"].astype(float)
        if "Low" in price_df.columns
        else price_df["Close"].astype(float)
    )
    low_52w = float(low_col.min())
    swing_low_20d = (
        float(low_col.rolling(20).min().iloc[-1]) if len(low_col) >= 20 else None
    )
    return {"low_52w": low_52w, "swing_low_20d": swing_low_20d}


def _near_round_number(strike: float) -> bool:
    """True if strike is within 1% of the nearest $5 increment."""
    nearest = round(strike / 5) * 5
    return abs(strike - nearest) / max(strike, 1e-6) < 0.01


# ── Per-ticker recommendation ──────────────────────────────────────────────────

def recommend_csp_for_ticker(
    ticker: str,
    candidates: List[Dict[str, Any]],
    price_df: pd.DataFrame,
    technicals: Dict[str, float],
    earnings_date: Optional[date],
    rec_config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Produce one CSP recommendation for a single ticker.

    Works from the already-screened monthly PUT candidates produced by the main
    pipeline, then applies the tighter recommendation criteria on top.
    """
    today = date.today()
    spot = float(technicals.get("spot", 0))

    ivr_min = float(rec_config.get("ivr_min", 30.0))
    earnings_buffer = int(rec_config.get("earnings_buffer_days", 7))
    delta_min = float(rec_config.get("delta_min", 0.10))
    delta_max = float(rec_config.get("delta_max", 0.25))
    use_support = bool(rec_config.get("use_support_filter", True))
    support_buffer = float(rec_config.get("support_pct_buffer", 0.02))

    base: Dict[str, Any] = {
        "ticker": ticker,
        "recommend": "No",
        "reason": "",
        "spot": spot if spot > 0 else None,
        "strike": None,
        "expiration": None,
        "premium": None,
        "delta": None,
        "ivr": None,
        "ivr_source": None,
        "max_profit": None,
        "breakeven": None,
        "cash_required": None,
        "near_support": False,
        "near_round_number": False,
        "dte": None,
        "annualized_yield": None,
    }

    # Monthly PUT candidates only
    monthly_puts = [
        c for c in candidates
        if c.get("strategy") == "PUT" and c.get("bucket") == "monthly"
    ]
    if not monthly_puts:
        base["reason"] = "No monthly PUT candidates survived initial screening"
        return base

    # ── IVR proxy ──────────────────────────────────────────────────────────────
    current_iv = next(
        (float(c["implied_volatility"]) for c in monthly_puts if c.get("implied_volatility")),
        None,
    )
    ivr_value, ivr_source = compute_ivr_proxy(price_df, current_iv)

    # ── Earnings proximity ─────────────────────────────────────────────────────
    earnings_too_close = False
    if earnings_date is not None:
        days_to_earnings = (earnings_date - today).days
        earnings_too_close = 0 <= days_to_earnings <= earnings_buffer

    # ── Support levels ─────────────────────────────────────────────────────────
    support = get_support_levels(price_df)

    def _at_or_below_support(strike: float) -> bool:
        if support["low_52w"] and strike <= support["low_52w"] * (1 + support_buffer):
            return True
        if support["swing_low_20d"] and strike <= support["swing_low_20d"] * (1 + support_buffer):
            return True
        # Proxy: at least 5% OTM counts as a reasonable distance below current price
        if spot > 0 and (spot - strike) / spot >= 0.05:
            return True
        return False

    def _delta_ok(c: Dict) -> bool:
        d = c.get("delta")
        if d is None:
            return False
        return delta_min <= abs(float(d)) <= delta_max

    # Filter by delta + support
    qualified = [
        c for c in monthly_puts
        if _delta_ok(c) and (not use_support or _at_or_below_support(float(c.get("strike", 0))))
    ]

    # Relax support if nothing qualifies
    support_relaxed = False
    if not qualified:
        qualified = [c for c in monthly_puts if _delta_ok(c)]
        support_relaxed = bool(qualified)

    if not qualified:
        reasons: List[str] = []
        if ivr_value is not None and ivr_value < ivr_min:
            reasons.append(f"IVR {ivr_value:.0f}% below {ivr_min:.0f}% threshold")
        if earnings_too_close:
            reasons.append("earnings too close to expiration")
        reasons.append(f"no strike with |delta| {delta_min:.2f}–{delta_max:.2f}")
        base["reason"] = "; ".join(reasons)
        base["ivr"] = ivr_value
        base["ivr_source"] = ivr_source
        return base

    # Best = max annualized yield among qualified candidates
    best = max(qualified, key=lambda x: float(x.get("annualized_yield") or 0))
    strike = float(best["strike"])
    premium = float(best.get("mid", 0))
    delta_val = best.get("delta")
    dte_val = best.get("dte")
    near_round = _near_round_number(strike)
    near_support_flag = not support_relaxed

    # ── Verdict ────────────────────────────────────────────────────────────────
    hard_fails: List[str] = []
    soft_fails: List[str] = []

    if earnings_too_close:
        hard_fails.append(f"earnings within {earnings_buffer} days of expiration")
    if ivr_value is not None and ivr_value < ivr_min:
        hard_fails.append(f"IVR {ivr_value:.0f}% below {ivr_min:.0f}% threshold")
    if ivr_value is None:
        soft_fails.append(f"IVR unavailable ({ivr_source})")
    if support_relaxed:
        soft_fails.append("strike above support levels — use caution")
    if near_round:
        soft_fails.append("strike near round number (may act as support)")

    if hard_fails:
        verdict = "No"
        reason = "; ".join(hard_fails)
    elif soft_fails:
        verdict = "Borderline"
        reason = "; ".join(soft_fails)
    else:
        ivr_str = f"IVR {ivr_value:.0f}%" if ivr_value is not None else "IVR n/a"
        reason = f"{ivr_str}; delta {abs(float(delta_val or 0)):.2f}; strike at/below support"
        verdict = "Yes"

    return {
        **base,
        "recommend": verdict,
        "reason": reason,
        "strike": strike,
        "expiration": best.get("expiration"),
        "premium": round(premium, 2),
        "delta": round(float(delta_val), 3) if delta_val is not None else None,
        "ivr": ivr_value,
        "ivr_source": ivr_source,
        "max_profit": round(premium * 100, 2),
        "breakeven": round(strike - premium, 2),
        "cash_required": round(strike * 100, 2),
        "near_support": near_support_flag,
        "near_round_number": near_round,
        "dte": dte_val,
        "annualized_yield": best.get("annualized_yield"),
    }


# ── Batch entry point ──────────────────────────────────────────────────────────

def build_csp_recommendations(
    ticker_results: Dict[str, Dict[str, Any]],
    csp_tickers: List[str],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Run recommendation engine for all CSP tickers.
    Returns a list sorted by verdict (Yes → Borderline → No) then by yield desc.
    Capped at max_recommendations.
    """
    rec_config = config.get("csp_recommendation", {})
    if not rec_config.get("enabled", True):
        return []

    max_recs = int(rec_config.get("max_recommendations", 10))

    results = []
    for ticker in csp_tickers:
        tr = ticker_results.get(ticker, {})
        rec = recommend_csp_for_ticker(
            ticker=ticker,
            candidates=tr.get("candidates", []),
            price_df=tr.get("price_df", pd.DataFrame()),
            technicals=tr.get("technicals", {}),
            earnings_date=tr.get("earnings_date"),
            rec_config=rec_config,
        )
        results.append(rec)

    order = {"Yes": 0, "Borderline": 1, "No": 2}
    results.sort(
        key=lambda r: (order.get(r["recommend"], 3), -(float(r.get("annualized_yield") or 0)))
    )
    return results[:max_recs]
