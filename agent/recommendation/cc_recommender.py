"""
Covered Call (CC) Recommendation Engine.

Analyses monthly and short-term CALL candidates per ticker and produces two
actionable trade recommendations per ticker:
  - Short-Term: closest expiration within short_term_dte_max (default 16 DTE)
  - Monthly:    closest expiration in the 30–45 DTE window

Recommendation logic — a covered call is recommended only when ALL hold:
  1. IV Rank (IVR) > ivr_min (default 30%) — premium is elevated enough to sell
  2. No earnings announcement within earnings_buffer_days of expiration
  3. Delta of the target strike is between delta_min and delta_max (0.10–0.25)

A strike below min_acceptable_sale_price (optional per-ticker config) is a soft
warning (Borderline), not a hard reject — the user may still want the income.
A strike near a resistance level is flagged as a positive signal (stock may stall
there, reducing assignment risk).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from agent.recommendation.csp_recommender import compute_ivr_proxy


DEFAULT_CC_REC_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "max_recommendations": 20,
    "ivr_min": 30.0,
    "earnings_buffer_days": 7,
    "delta_min": 0.10,
    "delta_max": 0.25,
    "short_term_dte_max": 16,
    "use_resistance_filter": True,
    "resistance_pct_buffer": 0.02,
    "min_acceptable_sale_prices": {},  # ticker -> float, optional per-ticker floor
}


# ── Resistance levels ───────────────────────────────────────────────────────

def get_resistance_levels(price_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """Return period high and 20-day swing high from available price history."""
    if price_df is None or price_df.empty:
        return {"high_52w": None, "swing_high_20d": None}

    high_col = (
        price_df["High"].astype(float)
        if "High" in price_df.columns
        else price_df["Close"].astype(float)
    )
    high_52w = float(high_col.max())
    swing_high_20d = float(high_col.rolling(20).max().iloc[-1]) if len(high_col) >= 20 else None
    return {"high_52w": high_52w, "swing_high_20d": swing_high_20d}


def _near_round_number(strike: float) -> bool:
    """True if strike is within 1% of the nearest $5 increment."""
    nearest = round(strike / 5) * 5
    return abs(strike - nearest) / max(strike, 1e-6) < 0.01


def _near_resistance(strike: float, resistance: Dict[str, Optional[float]], buffer: float) -> bool:
    """True if strike is within buffer% of a resistance level (stock may stall there)."""
    for level in (resistance.get("high_52w"), resistance.get("swing_high_20d")):
        if level is not None and level * (1 - buffer) <= strike <= level * (1 + buffer):
            return True
    return False


# ── Per-bucket recommendation ───────────────────────────────────────────────

def _recommend_for_bucket(
    ticker: str,
    term_label: str,
    call_candidates: List[Dict[str, Any]],
    resistance: Dict[str, Optional[float]],
    technicals: Dict[str, float],
    earnings_date: Optional[date],
    min_acceptable_price: Optional[float],
    price_df: pd.DataFrame,
    rec_config: Dict[str, Any],
) -> Dict[str, Any]:
    spot = float(technicals.get("spot", 0))
    ivr_min = float(rec_config.get("ivr_min", 30.0))
    earnings_buffer = int(rec_config.get("earnings_buffer_days", 7))
    delta_min = float(rec_config.get("delta_min", 0.10))
    delta_max = float(rec_config.get("delta_max", 0.25))
    resistance_buffer = float(rec_config.get("resistance_pct_buffer", 0.02))

    base: Dict[str, Any] = {
        "ticker": ticker,
        "term": term_label,
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
        "downside_breakeven": None,
        "near_resistance": False,
        "near_round_number": False,
        "below_min_price": False,
        "dte": None,
        "annualized_yield": None,
        "min_acceptable_price": min_acceptable_price,
    }

    if not call_candidates:
        base["reason"] = f"No {term_label} CALL candidates survived initial screening"
        return base

    def _delta_ok(c: Dict) -> bool:
        d = c.get("delta")
        return d is not None and delta_min <= abs(float(d)) <= delta_max

    def _earnings_ok(c: Dict) -> bool:
        if earnings_date is None:
            return True
        c_exp_str = c.get("expiration")
        if not c_exp_str:
            return True
        try:
            c_exp = date.fromisoformat(str(c_exp_str))
        except ValueError:
            return True
        days_before = (c_exp - earnings_date).days
        return not (earnings_date <= c_exp and days_before <= earnings_buffer)

    qualified = [c for c in call_candidates if _delta_ok(c) and _earnings_ok(c)]
    earnings_blocked = any(not _earnings_ok(c) for c in call_candidates if _delta_ok(c))

    if not qualified:
        # Use any available IV from this bucket's pool for the reason message
        pool_iv = next(
            (float(c["implied_volatility"]) for c in call_candidates if c.get("implied_volatility")),
            None,
        )
        ivr_value, ivr_source = compute_ivr_proxy(price_df, pool_iv)
        reasons: List[str] = []
        if ivr_value is not None and ivr_value < ivr_min:
            reasons.append(f"IVR {ivr_value:.0f}% below {ivr_min:.0f}% threshold")
        if earnings_blocked:
            reasons.append("earnings too close to expiration")
        reasons.append(f"no strike with |delta| {delta_min:.2f}–{delta_max:.2f}")
        base["ivr"] = ivr_value
        base["ivr_source"] = ivr_source
        base["reason"] = "; ".join(reasons)
        return base

    best = max(qualified, key=lambda x: float(x.get("score") or 0))

    # Compute IVR from the recommended contract's own IV — keeps IVR consistent
    # with the implied_volatility shown in the detail table for that contract.
    best_iv = float(best["implied_volatility"]) if best.get("implied_volatility") else None
    ivr_value, ivr_source = compute_ivr_proxy(price_df, best_iv)

    strike = float(best["strike"])
    premium = float(best.get("mid", 0))
    delta_val = best.get("delta")
    dte_val = best.get("dte")

    near_round = _near_round_number(strike)
    near_res = _near_resistance(strike, resistance, resistance_buffer)
    below_min = min_acceptable_price is not None and strike < min_acceptable_price

    # ── Verdict ─────────────────────────────────────────────────────────────
    hard_fails: List[str] = []
    soft_fails: List[str] = []

    if ivr_value is not None and ivr_value < ivr_min:
        hard_fails.append(f"IVR {ivr_value:.0f}% below {ivr_min:.0f}% threshold")
    if ivr_value is None:
        soft_fails.append(f"IVR unavailable ({ivr_source})")
    elif ivr_value >= 99.9:
        soft_fails.append("IVR proxy at ceiling (100%) — likely overstated vs. 1-yr HV range")
    elif ivr_value == 0.0:
        soft_fails.append("IVR proxy at floor (0%) — current vol may be understated")
    if below_min:
        soft_fails.append(
            f"strike ${strike:.2f} below min acceptable ${min_acceptable_price:.2f} — assignment risk"
        )

    if hard_fails:
        verdict = "No"
        reason = "; ".join(hard_fails)
    elif soft_fails:
        verdict = "Borderline"
        reason = "; ".join(soft_fails)
    else:
        ivr_str = f"IVR {ivr_value:.0f}%" if ivr_value is not None else "IVR n/a"
        reason = f"{ivr_str}; delta {abs(float(delta_val or 0)):.2f}"
        if near_res:
            reason += "; strike near resistance (favourable)"
        verdict = "Yes"

    max_profit = round((strike - spot + premium) * 100, 2) if spot > 0 else None
    downside_breakeven = round(spot - premium, 2) if spot > 0 else None

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
        "max_profit": max_profit,
        "downside_breakeven": downside_breakeven,
        "near_resistance": near_res,
        "near_round_number": near_round,
        "below_min_price": below_min,
        "dte": dte_val,
        "annualized_yield": best.get("annualized_yield"),
    }


# ── Per-ticker entry point ──────────────────────────────────────────────────

def recommend_cc_for_ticker(
    ticker: str,
    candidates: List[Dict[str, Any]],
    price_df: pd.DataFrame,
    technicals: Dict[str, float],
    earnings_date: Optional[date],
    min_acceptable_price: Optional[float],
    rec_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Produce up to two CC recommendations (Short-Term + Monthly) for one ticker.
    Returns a list of dicts (one per term).
    """
    spot = float(technicals.get("spot", 0))
    no_data_row = lambda term: {
        "ticker": ticker, "term": term, "recommend": "No",
        "reason": "Spot price unavailable",
        "spot": None, "strike": None, "expiration": None, "premium": None,
        "delta": None, "ivr": None, "ivr_source": None,
        "max_profit": None, "downside_breakeven": None,
        "near_resistance": False, "near_round_number": False, "below_min_price": False,
        "dte": None, "annualized_yield": None, "min_acceptable_price": min_acceptable_price,
    }
    if spot <= 0:
        return [no_data_row("Short-Term"), no_data_row("Monthly")]

    short_term_dte_max = int(rec_config.get("short_term_dte_max", 16))

    all_calls = [c for c in candidates if c.get("strategy") == "CALL"]

    short_term_calls = [
        c for c in all_calls
        if c.get("bucket") in ("current_week", "next_week")
        and (c.get("dte") or 99) <= short_term_dte_max
    ]
    monthly_calls = [c for c in all_calls if c.get("bucket") == "monthly"]

    resistance = get_resistance_levels(price_df)

    results = []
    for term_label, pool in [("Short-Term", short_term_calls), ("Monthly", monthly_calls)]:
        results.append(
            _recommend_for_bucket(
                ticker=ticker,
                term_label=term_label,
                call_candidates=pool,
                resistance=resistance,
                technicals=technicals,
                earnings_date=earnings_date,
                min_acceptable_price=min_acceptable_price,
                price_df=price_df,
                rec_config=rec_config,
            )
        )
    return results


# ── Batch entry point ───────────────────────────────────────────────────────

def build_cc_recommendations(
    ticker_results: Dict[str, Dict[str, Any]],
    cc_tickers: List[str],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Run CC recommendation engine for all covered-call tickers.
    Returns list sorted by verdict (Yes → Borderline → No) then yield desc.
    Capped at max_recommendations.
    """
    rec_config = config.get("cc_recommendation", {})
    if not rec_config.get("enabled", True):
        return []

    max_recs = int(rec_config.get("max_recommendations", 20))
    min_prices: Dict[str, Any] = rec_config.get("min_acceptable_sale_prices") or {}

    results: List[Dict[str, Any]] = []
    for ticker in cc_tickers:
        tr = ticker_results.get(ticker, {})
        raw_min = min_prices.get(ticker) or min_prices.get(ticker.upper())
        min_price = float(raw_min) if raw_min is not None else None
        recs = recommend_cc_for_ticker(
            ticker=ticker,
            candidates=tr.get("candidates", []),
            price_df=tr.get("price_df", pd.DataFrame()),
            technicals=tr.get("technicals", {}),
            earnings_date=tr.get("earnings_date"),
            min_acceptable_price=min_price,
            rec_config=rec_config,
        )
        results.extend(recs)

    order = {"Yes": 0, "Borderline": 1, "No": 2}
    results.sort(
        key=lambda r: (order.get(r["recommend"], 3), -(float(r.get("annualized_yield") or 0)))
    )
    return results[:max_recs]
