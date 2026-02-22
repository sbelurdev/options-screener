from __future__ import annotations

from typing import Any, Dict, Tuple


def _clamp_0_1(value: float) -> float:
    return max(0.0, min(1.0, value))


def score_candidate(row: Dict[str, Any], technicals: Dict[str, float], config: Dict[str, Any]) -> Tuple[float, str]:
    strategy = row["strategy"]

    ann_yield = float(row.get("annualized_yield") or 0.0)
    income_score = _clamp_0_1(ann_yield / 0.35)

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
        if spot < ma20:
            trend += 0.20
        if rsi >= 60:
            trend += 0.15
        trend_reason = "income-harvest trend fit"
    trend_score = _clamp_0_1(trend)

    spread = float(row.get("spread_pct") or 1.0)
    oi = float(row.get("open_interest") or 0.0)
    vol = float(row.get("volume") or 0.0)
    spread_component = _clamp_0_1(1.0 - spread / max(float(config["max_spread_pct"]), 1e-6))
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
