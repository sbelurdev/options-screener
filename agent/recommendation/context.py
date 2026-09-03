"""
Per-ticker context: the information that's a property of the ticker itself
(spot, earnings date, trend regime, IV rank, support/resistance levels, and a
suggested strike derived from them) rather than of any one candidate row.
Computed once per ticker per run and persisted alongside the CC/CSP recs so
the presentation layer can show it once, in a panel, instead of repeating it
as a "Why" string on every row.

Strategy-independent by design — support and resistance levels, regime, IV
rank, and earnings apply the same way whether the ticker is being screened
for a covered call or a cash-secured put, so a ticker present in both
strategies' watchlists gets one shared context entry, not two. Whether a
given reading is FAVORABLE differs by strategy, though — that's what
strategy_fit() is for, applied at render time once the user has picked a
strategy, not baked into the stored context itself.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from agent.recommendation.cc_recommender import get_resistance_levels
from agent.recommendation.csp_recommender import get_support_levels
from agent.signals.technicals import classify_regime


def build_ticker_context(
    ticker_result: Dict[str, Any],
    resistance_buffer: float = 0.02,
    support_buffer: float = 0.02,
) -> Dict[str, Any]:
    """`ticker_result` is one entry of pipeline.py's `ticker_results_map` —
    already carries `price_df`, `technicals`, `earnings_date`, `iv_rank`, and
    `atm_iv` from `_process_ticker`, so this does no new fetching.

    `resistance_buffer`/`support_buffer` should match the recommender's own
    `resistance_pct_buffer`/`support_pct_buffer` config, so the suggested
    strike shown here is the same number that actually drives a row's
    near_resistance/near_support flag, not a second, disconnected guess.
    """
    price_df = ticker_result.get("price_df")
    technicals = ticker_result.get("technicals") or {}
    spot = float(technicals.get("spot") or 0.0)
    ma20 = float(technicals.get("ma20") or spot)
    ma50 = float(technicals.get("ma50") or spot)
    rsi14 = float(technicals.get("rsi14") or 50.0)
    hv20 = technicals.get("hv20")
    regime, regime_reason = classify_regime(spot, ma20, ma50, rsi14)

    earnings_date: Optional[date] = ticker_result.get("earnings_date")

    ivr_value, ivr_source = ticker_result.get("iv_rank") or (None, None)
    atm_iv = ticker_result.get("atm_iv")
    vrp = (
        round(atm_iv / hv20, 3)
        if atm_iv is not None and hv20 is not None and hv20 > 0
        else None
    )

    support = get_support_levels(price_df)
    resistance = get_resistance_levels(price_df)
    # Nearer-term (20d) level preferred — more responsive to current
    # conditions than the 52-week extreme; falls back when unavailable
    # (fewer than 20 sessions of history).
    resistance_level = resistance.get("swing_high_20d") or resistance.get("high_52w")
    support_level = support.get("swing_low_20d") or support.get("low_52w")
    suggested_call_strike = (
        round(resistance_level * (1 + resistance_buffer), 2) if resistance_level else None
    )
    suggested_put_strike = (
        round(support_level * (1 - support_buffer), 2) if support_level else None
    )

    return {
        "spot": spot,
        "ma20": ma20,
        "ma50": ma50,
        "rsi14": rsi14,
        "regime": regime,
        "regime_reason": regime_reason,
        "earnings_date": earnings_date.isoformat() if earnings_date else None,
        "ivr": ivr_value,
        "ivr_source": ivr_source,
        "vrp": vrp,
        "support": support,
        "resistance": resistance,
        "suggested_call_strike": suggested_call_strike,
        "suggested_put_strike": suggested_put_strike,
    }


def build_context_store(
    ticker_results_map: Dict[str, Dict[str, Any]],
    resistance_buffer: float = 0.02,
    support_buffer: float = 0.02,
) -> Dict[str, Any]:
    """One entry per ticker that actually produced a price history."""
    return {
        ticker: build_ticker_context(result, resistance_buffer, support_buffer)
        for ticker, result in ticker_results_map.items()
        if result.get("price_df") is not None
    }


def strategy_fit(context: Dict[str, Any], option_right: str) -> Dict[str, Dict[str, str]]:
    """Per-metric favorability for `context`, judged for `option_right`
    ("CALL" = covered call, "PUT" = cash-secured put). Each metric gets
    {"read": "favorable"/"mixed"/"unfavorable"/"unknown", "detail": one
    phrase stating the fact and what it implies for this strategy} — the
    detail is what the context panel's hover tooltip shows. A reading aid,
    not a hard rule.

    Regime and RSI read OPPOSITELY by strategy (a covered call is a
    neutral-to-mildly-bearish income strategy on shares you hold; a
    cash-secured put is a bullish-to-neutral strategy you'd be assigned
    into) — see the module docstring. IV Rank does not flip: richer
    premium is equally attractive selling either one.
    """
    regime = context.get("regime")
    regime_reason = context.get("regime_reason") or ""
    rsi = context.get("rsi14")
    ivr = context.get("ivr")
    ivr_source = context.get("ivr_source")

    is_put = option_right == "PUT"
    label = "cash-secured put" if is_put else "covered call"

    if is_put:
        regime_read = {"Bullish": "favorable", "Neutral": "mixed",
                       "Bearish": "unfavorable"}.get(regime, "unknown")
        regime_implication = {
            "favorable": "less risk of being assigned into a downtrend",
            "mixed": "no strong directional support either way",
            "unfavorable": "assignment would mean buying into a decline",
        }.get(regime_read, "regime unavailable")
    else:
        regime_read = {"Neutral": "favorable", "Bullish": "mixed",
                       "Bearish": "unfavorable"}.get(regime, "unknown")
        regime_implication = {
            "favorable": "a stalling stock is less likely to get your shares called away",
            "mixed": "a breakout risks early call-away and capped upside",
            "unfavorable": "the shares you'd be holding are losing value",
        }.get(regime_read, "regime unavailable")
    regime_detail = (f"{regime_reason} — {regime_implication}." if regime_reason
                     else f"{label}: {regime_implication}.")

    if rsi is None:
        rsi_read = "unknown"
        rsi_detail = "RSI unavailable this run."
    elif is_put:
        if rsi < 30:
            rsi_read = "unfavorable"
            rsi_detail = f"RSI {rsi:.0f} is oversold — the stock may still be falling, raising assignment risk."
        elif 40 <= rsi <= 70:
            rsi_read = "favorable"
            rsi_detail = f"RSI {rsi:.0f} is in a healthy neutral band — not extended in either direction."
        else:
            rsi_read = "mixed"
            rsi_detail = f"RSI {rsi:.0f} is outside the healthy 40-70 band — worth a closer look."
    else:
        if rsi > 75:
            rsi_read = "favorable"
            rsi_detail = f"RSI {rsi:.0f} is overbought — a pullback or stall is more likely, protecting your shares from being called away."
        elif rsi < 30:
            rsi_read = "unfavorable"
            rsi_detail = f"RSI {rsi:.0f} is oversold — a weak stock undercuts the point of holding it for income."
        else:
            rsi_read = "mixed"
            rsi_detail = f"RSI {rsi:.0f} is in a neutral band — no strong signal either way."

    if ivr is None:
        ivr_read = "unknown"
        ivr_detail = f"IV Rank unavailable this run ({ivr_source or 'insufficient history'})."
    elif ivr >= 50:
        ivr_read = "favorable"
        ivr_detail = f"IV Rank {ivr:.0f}% is elevated — premium is rich relative to its own history, a better price to sell at."
    else:
        ivr_read = "mixed"
        ivr_detail = f"IV Rank {ivr:.0f}% is low — premium is cheap relative to its own history."

    return {
        "regime": {"read": regime_read, "detail": regime_detail},
        "rsi": {"read": rsi_read, "detail": rsi_detail},
        "ivr": {"read": ivr_read, "detail": ivr_detail},
    }
