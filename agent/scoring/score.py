from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

# Component weights are normalized by their sum, so they need not add to 1.
DEFAULT_WEIGHTS: Dict[str, float] = {
    "income": 0.35,     # risk-adjusted annualized yield (EV of premium kept)
    "delta": 0.20,      # proximity to the target short delta
    "trend": 0.15,      # MA/RSI alignment for the strategy
    "liquidity": 0.10,  # spread + open interest + volume
    "vrp": 0.10,        # IV / HV20 — premium richness vs realised vol
    "theta": 0.10,      # annualized decay yield per dollar of collateral
}
DEFAULT_DELTA_TARGET = 0.20
DEFAULT_INCOME_YIELD_CAP = 1.5  # yields at/above this score 1.0 on the absolute axis


def _clamp_0_1(value: float) -> float:
    return max(0.0, min(1.0, value))


def get_scoring_params(config: Dict[str, Any]) -> Tuple[Dict[str, float], float, float]:
    """Resolve (normalized weights, delta_target, income_yield_cap) from config."""
    sc = config.get("scoring") or {}
    weights = dict(DEFAULT_WEIGHTS)
    for key, val in (sc.get("weights") or {}).items():
        if key in weights:
            weights[key] = max(float(val), 0.0)
    total = sum(weights.values())
    if total <= 0:
        weights = dict(DEFAULT_WEIGHTS)
        total = sum(weights.values())
    weights = {k: v / total for k, v in weights.items()}
    delta_target = abs(float(sc.get("delta_target", DEFAULT_DELTA_TARGET)))
    income_cap = max(float(sc.get("income_yield_cap", DEFAULT_INCOME_YIELD_CAP)), 0.01)
    return weights, delta_target, income_cap


def _log_scaled(value: float, cap: float) -> float:
    """log1p scaling with a configurable saturation ceiling."""
    return _clamp_0_1(math.log1p(min(max(value, 0.0), cap)) / math.log1p(cap))


def ev_yield(row: Dict[str, Any]) -> float:
    """Risk-adjusted income: |delta| approximates P(assignment), so yield × (1 − |delta|)
    is the premium weighted by the probability of keeping it. A missing delta
    (None or the 0.0 fallback) leaves the yield unadjusted."""
    ann_yield = float(row.get("annualized_yield") or 0.0)
    delta_raw = row.get("delta")
    assignment_prob = min(abs(float(delta_raw)), 1.0) if delta_raw else 0.0
    return ann_yield * (1.0 - assignment_prob)


def score_candidate(
    row: Dict[str, Any],
    technicals: Dict[str, float],
    config: Dict[str, Any],
    income_percentile: Optional[float] = None,
) -> Tuple[float, str]:
    """
    Composite score in [0, 1]. When income_percentile is given (rank of this
    row's EV yield within its ticker/strategy pool), the income component blends
    the absolute log-scaled yield 50/50 with that percentile — this keeps
    differentiation on leveraged ETFs whose yields all exceed the absolute cap.
    """
    strategy = row["strategy"]
    weights, delta_target, income_cap = get_scoring_params(config)

    ann_yield = float(row.get("annualized_yield") or 0.0)
    ev = ev_yield(row)
    abs_income = _log_scaled(ev, income_cap)
    if income_percentile is None:
        income_score = abs_income
    else:
        income_score = 0.5 * abs_income + 0.5 * _clamp_0_1(income_percentile)

    delta = row.get("delta")
    if delta is None:
        delta_score = 0.45
        delta_reason = "delta fallback"
    else:
        target = -delta_target if strategy == "PUT" else delta_target
        dist = abs(float(delta) - target)
        delta_score = _clamp_0_1(1.0 - (dist / 0.25))
        delta_reason = f"delta {float(delta):.2f}"

    spot = float(row.get("spot") or technicals["spot"])
    ma20 = float(technicals["ma20"])
    ma50 = float(technicals["ma50"])
    rsi = float(technicals["rsi14"])

    if strategy == "PUT":
        trend = 0.55
        if spot > ma20:
            trend += 0.20
        if spot > ma50:
            trend += 0.20
        if rsi > 75:
            trend -= 0.20
        trend_reason = "bullish/neutral alignment"
    else:
        trend = 0.55
        if spot > ma20:
            trend += 0.15
        else:
            trend -= 0.15  # Below short-term MA — bearish for held shares
        if spot > ma50:
            trend += 0.15
        else:
            trend -= 0.15  # Below medium-term MA — bearish for held shares
        if rsi > 75:
            trend -= 0.20  # Overbought — elevated call-away risk
        trend_reason = "bullish/neutral alignment"
    trend_score = _clamp_0_1(trend)

    spread = float(row.get("spread_pct") or 1.0)
    oi = float(row.get("open_interest") or 0.0)
    vol = float(row.get("volume") or 0.0)
    max_spread_cfg = config.get("max_spread_pct")
    if max_spread_cfg is None:
        spread_component = 0.5
    else:
        spread_component = _clamp_0_1(1.0 - spread / max(float(max_spread_cfg), 1e-6))
    oi_component = _clamp_0_1(oi / 2000.0)
    vol_component = _clamp_0_1(vol / 500.0)
    liquidity_score = 0.5 * spread_component + 0.25 * oi_component + 0.25 * vol_component

    # VRP: IV/HV20 of 0.8 or less scores 0 (premium cheap vs realised vol),
    # 1.6 or more scores 1 (premium rich). Missing data is neutral.
    vrp = row.get("vrp")
    vrp_score = 0.5 if vrp is None else _clamp_0_1((float(vrp) - 0.8) / 0.8)

    # Theta: annualized decay yield per dollar of collateral, same log scale
    # as income. Missing theta (e.g. yfinance chains) is neutral.
    theta_yield = row.get("theta_yield")
    theta_score = 0.5 if theta_yield is None else _log_scaled(float(theta_yield), income_cap)

    score = (
        weights["income"] * income_score
        + weights["delta"] * delta_score
        + weights["trend"] * trend_score
        + weights["liquidity"] * liquidity_score
        + weights["vrp"] * vrp_score
        + weights["theta"] * theta_score
    )

    if bool(row.get("earnings_before_expiry")):
        score *= 1.0 - float(config["earnings_risk_penalty"])

    why = (
        f"income={ann_yield:.2%} (risk-adj {ev:.2%}), {delta_reason}, {trend_reason}, "
        f"spread={spread:.2%}, OI={int(oi)}, vol={int(vol)}"
    )
    if vrp is not None:
        why += f", IV/HV={float(vrp):.2f}"
    if theta_yield is not None:
        why += f", theta-yield={float(theta_yield):.2%}"
    if bool(row.get("earnings_before_expiry")):
        why += ", earnings-risk penalty applied"

    return score, why


def score_candidates(
    rows: List[Dict[str, Any]],
    technicals: Dict[str, float],
    config: Dict[str, Any],
) -> None:
    """
    Score a pool of same-strategy candidates in place (sets score /
    why_ranked_high). The pool should span all expirations for one ticker and
    strategy so the income percentile compares like with like.
    """
    if not rows:
        return
    n = len(rows)
    if n == 1:
        percentiles = {id(rows[0]): None}
    else:
        order = sorted(range(n), key=lambda i: ev_yield(rows[i]))
        percentiles = {id(rows[order[rank]]): rank / (n - 1) for rank in range(n)}
    for row in rows:
        score, why = score_candidate(row, technicals, config, income_percentile=percentiles[id(row)])
        row["score"] = round(score, 4)
        row["why_ranked_high"] = why
