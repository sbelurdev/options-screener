from __future__ import annotations

import math
from typing import Any, Dict, Tuple


def _clamp_0_1(value: float) -> float:
    return max(0.0, min(1.0, value))


def score_candidate(row: Dict[str, Any], technicals: Dict[str, float], config: Dict[str, Any]) -> Tuple[float, str]:
    strategy = row["strategy"]

    ann_yield = float(row.get("annualized_yield") or 0.0)
    # Logarithmic scale keeps differentiation at high yields (e.g. leveraged ETFs)
    # log1p(1.0) ≈ 0.693, so a 100% yield scores ~1.0; a 35% yield scores ~0.74
    income_score = _clamp_0_1(math.log1p(ann_yield) / math.log1p(1.0))

    delta = row.get("delta")
    if delta is None:
        delta_score = 0.45
        delta_reason = "delta fallback"
    else:
        target = -0.20 if strategy == "PUT" else 0.20
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

    score = 0.40 * income_score + 0.25 * delta_score + 0.20 * trend_score + 0.15 * liquidity_score

    if bool(row.get("earnings_before_expiry")):
        score *= 1.0 - float(config["earnings_risk_penalty"])

    why = (
        f"income={ann_yield:.2%}, {delta_reason}, {trend_reason}, "
        f"spread={spread:.2%}, OI={int(oi)}, vol={int(vol)}"
    )
    if bool(row.get("earnings_before_expiry")):
        why += ", earnings-risk penalty applied"

    return score, why
