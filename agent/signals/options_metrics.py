from __future__ import annotations

import math
from datetime import date
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
from scipy.stats import norm

from agent.utils.dates import is_third_friday


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        out = float(value)
        if math.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def spread_pct(bid: float, ask: float) -> Optional[float]:
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    if ask < bid:
        return None
    return (ask - bid) / mid


def black_scholes_delta(
    strategy: str,
    spot: float,
    strike: float,
    dte: int,
    iv: float,
    risk_free_rate: float,
) -> Optional[float]:
    if spot <= 0 or strike <= 0 or dte <= 0 or iv is None or iv <= 0:
        return None
    # Convert calendar days to approximate trading days, then annualise on 252-day basis
    t = (dte * 252 / 365) / 252.0  # == dte / 365.0, keeps calendar & vol bases consistent
    try:
        d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
    except (ValueError, ZeroDivisionError):
        return None

    if strategy == "CALL":
        return norm.cdf(d1)
    return norm.cdf(d1) - 1.0


def annualized_yield(strategy: str, credit: float, strike: float, spot: float, dte: int) -> Optional[float]:
    if dte <= 0:
        return None
    # Premium quote is per-share; convert to per-contract to match collateral basis.
    premium_per_contract = credit * 100.0
    denom = strike * 100.0 if strategy == "PUT" else spot * 100.0
    if denom <= 0:
        return None
    return (premium_per_contract / denom) * (365.0 / dte)


def breakeven(strategy: str, strike: float, spot: float, credit: float) -> float:
    if strategy == "PUT":
        return strike - credit
    return spot - credit


def get_dte(expiration: date, today: date) -> int:
    return max((expiration - today).days, 0)


def select_expiration_buckets(expirations: List[date], today: date, config: Dict[str, Any], logger) -> Dict[str, Dict[str, Any]]:
    future = sorted([d for d in expirations if d > today])  # exclude same-day (0-DTE)

    def _pick_in_range(min_d: int, max_d: int) -> Optional[date]:
        candidates = [d for d in future if min_d <= (d - today).days <= max_d]
        return min(candidates) if candidates else None

    buckets: Dict[str, Dict[str, Any]] = {
        "current_week": {"label": "Current Week", "expiration": None},
        "next_week": {"label": "Next Week", "expiration": None},
        "monthly": {"label": "Monthly", "expiration": None},
    }

    current = _pick_in_range(0, int(config["dte_current_week_max_days"]))
    if current is None and future:
        current = future[0]
        buckets["current_week"]["label"] = "Current Week (fallback)"
    buckets["current_week"]["expiration"] = current

    next_week = _pick_in_range(int(config["dte_next_week_min_days"]), int(config["dte_next_week_max_days"]))
    if next_week is None and future:
        if current is not None:
            later = [d for d in future if d > current]
            if later:
                next_week = later[0]
        elif future:
            next_week = future[0]
        if next_week is not None:
            buckets["next_week"]["label"] = "Next Week (fallback)"
    # Prevent next_week duplicating current_week
    if next_week is not None and next_week == current:
        next_week = None
    buckets["next_week"]["expiration"] = next_week

    min_dte = int(config["monthly_target_dte_min"])
    max_dte = int(config["monthly_target_dte_max"])
    # Only consider third-Friday dates that also fall within the configured DTE range
    monthly_candidates = [
        d for d in future
        if is_third_friday(d) and min_dte <= (d - today).days <= max_dte
    ]
    if monthly_candidates:
        monthly = monthly_candidates[0]
    else:
        proxy = [d for d in future if min_dte <= (d - today).days <= max_dte]
        if proxy:
            target_mid = (min_dte + max_dte) / 2.0
            monthly = min(proxy, key=lambda x: abs((x - today).days - target_mid))
        else:
            monthly = future[-1] if future else None
        if monthly is not None:
            buckets["monthly"]["label"] = "Monthly (proxy)"
    # Prevent monthly duplicating current_week or next_week
    if monthly is not None and monthly in (current, next_week):
        monthly = None
    buckets["monthly"]["expiration"] = monthly

    for b_name, b in buckets.items():
        exp = b.get("expiration")
        if exp is not None:
            logger.info("Bucket selected %s -> %s (%s)", b_name, exp.isoformat(), b.get("label"))
        else:
            logger.warning("Bucket selected %s -> none", b_name)

    return buckets


def _passes_delta_or_otm(strategy: str, delta_value: Optional[float], otm_pct: Optional[float], config: Dict[str, Any]) -> bool:
    if delta_value is not None:
        if strategy == "PUT":
            return float(config["delta_put_min"]) <= delta_value <= float(config["delta_put_max"])
        return float(config["delta_call_min"]) <= delta_value <= float(config["delta_call_max"])

    if otm_pct is None:
        return False

    if strategy == "PUT":
        return float(config["put_otm_pct_min"]) <= otm_pct <= float(config["put_otm_pct_max"])
    return float(config["call_otm_pct_min"]) <= otm_pct <= float(config["call_otm_pct_max"])


def build_option_records(
    ticker: str,
    strategy: str,
    options_df: pd.DataFrame,
    expiration: date,
    bucket_name: str,
    bucket_label: str,
    spot: float,
    technicals: Dict[str, float],
    earnings_date: Optional[date],
    config: Dict[str, Any],
    logger,
    decision_logger: Optional[Callable[[dict], None]] = None,
) -> List[Dict[str, Any]]:
    today = date.today()
    dte = get_dte(expiration, today)
    if dte <= 0:
        return []

    if options_df is None or options_df.empty:
        logger.warning("%s %s %s: empty chain", ticker, strategy, expiration.isoformat())
        return []

    rows: List[Dict[str, Any]] = []
    missing_delta_count = 0
    min_oi = safe_float(config.get("min_open_interest"))
    min_vol = safe_float(config.get("min_volume"))
    max_sp = safe_float(config.get("max_spread_pct"))
    risk_free = safe_float(config.get("risk_free_rate"))

    req_cols = [
        "strike",
        "bid",
        "ask",
        "lastPrice",
        "volume",
        "openInterest",
        "impliedVolatility",
        "contractSymbol",
    ]
    for c in req_cols:
        if c not in options_df.columns:
            options_df[c] = np.nan
            logger.info("%s %s %s: missing field '%s', using fallback", ticker, strategy, expiration, c)

    for _, r in options_df.iterrows():
        strike = safe_float(r.get("strike"))
        bid = safe_float(r.get("bid"), 0.0) or 0.0
        ask = safe_float(r.get("ask"), 0.0) or 0.0
        volume = int(safe_float(r.get("volume"), 0) or 0)
        oi = int(safe_float(r.get("openInterest"), 0) or 0)
        iv = safe_float(r.get("impliedVolatility"))
        contract_symbol = str(r.get("contractSymbol") or "")

        def _log_decision(filtered: bool, reason: str) -> None:
            if decision_logger is None:
                return
            decision_logger(
                {
                    "event": "screening_decision",
                    "expiration": expiration.isoformat(),
                    "option_type": strategy,
                    "contractSymbol": contract_symbol,
                    "strike": strike,
                    "bid": bid,
                    "ask": ask,
                    "lastPrice": safe_float(r.get("lastPrice")),
                    "volume": volume,
                    "openInterest": oi,
                    "impliedVolatility": iv,
                    "status": "filtered" if filtered else "kept",
                    "filtered": "yes" if filtered else "no",
                    "filter_reason": reason if filtered else "",
                }
            )

        if strike is None or strike <= 0:
            _log_decision(True, "invalid_strike")
            continue
        if bid <= 0 or ask <= 0:
            _log_decision(True, "invalid_bid_ask")
            continue

        sp = spread_pct(bid, ask)
        if sp is None:
            _log_decision(True, "invalid_spread")
            continue
        if min_oi is not None and oi < int(min_oi):
            _log_decision(True, f"open_interest_below_min:{oi}<{int(min_oi)}")
            continue
        if min_vol is not None and volume < int(min_vol):
            _log_decision(True, f"volume_below_min:{volume}<{int(min_vol)}")
            continue
        if max_sp is not None and sp > float(max_sp):
            _log_decision(True, f"spread_above_max:{sp:.6f}>{float(max_sp):.6f}")
            continue

        mid = (bid + ask) / 2.0
        ann_yield = annualized_yield(strategy, mid, strike, spot, dte)
        if ann_yield is None or ann_yield < float(config["min_annualized_yield"]):
            _log_decision(
                True,
                f"annualized_yield_below_min:{0.0 if ann_yield is None else ann_yield:.6f}<"
                f"{float(config['min_annualized_yield']):.6f}",
            )
            continue

        if strategy == "PUT":
            otm_pct = (spot - strike) / spot if spot > 0 else None
        else:
            otm_pct = (strike - spot) / spot if spot > 0 else None

        if otm_pct is not None and otm_pct < 0:
            _log_decision(True, f"not_otm:{otm_pct:.6f}")
            continue

        delta_raw = safe_float(r.get("delta"))
        delta_source = "provided"
        if delta_raw is None:
            bs_delta = None
            if risk_free is not None and iv is not None:
                bs_delta = black_scholes_delta(
                    strategy=strategy,
                    spot=spot,
                    strike=strike,
                    dte=dte,
                    iv=iv,
                    risk_free_rate=float(risk_free),
                )
            if bs_delta is not None:
                delta_raw = bs_delta
                delta_source = "black_scholes"
            else:
                delta_source = "otm_fallback"

        if not _passes_delta_or_otm(strategy, delta_raw, otm_pct, config):
            _log_decision(
                True,
                "delta_or_otm_out_of_range"
                if delta_raw is not None
                else "otm_fallback_out_of_range",
            )
            continue

        # Delta unavailable after all attempts — default to 0 and count for warning
        if delta_raw is None:
            delta_raw = 0.0
            missing_delta_count += 1

        earnings_before_expiry = earnings_date is not None and earnings_date <= expiration and earnings_date >= today

        record = {
            "run_date": today.isoformat(),
            "ticker": ticker,
            "bucket": bucket_name,
            "bucket_label": bucket_label,
            "expiration": expiration.isoformat(),
            "strategy": strategy,
            "contract_symbol": contract_symbol,
            "spot": round(spot, 4),
            "strike": round(strike, 4),
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "mid": round(mid, 4),
            "spread_pct": round(sp, 6),
            "volume": volume,
            "open_interest": oi,
            "implied_volatility": round(iv, 6) if iv is not None else None,
            "delta": round(delta_raw, 6),
            "delta_source": delta_source,
            "dte": dte,
            "annualized_yield": ann_yield,
            "breakeven": round(breakeven(strategy, strike, spot, mid), 4),
            "otm_pct": round(otm_pct, 6) if otm_pct is not None else None,
            "earnings_date": earnings_date.isoformat() if earnings_date else "",
            "earnings_before_expiry": earnings_before_expiry,
            "ma20": technicals["ma20"],
            "ma50": technicals["ma50"],
            "rsi14": technicals["rsi14"],
            "hv20": technicals["hv20"],
        }
        rows.append(record)
        _log_decision(False, "")

    if missing_delta_count > 0:
        logger.warning(
            "%s %s %s: delta missing for %d/%d candidate(s) — defaulted to 0. "
            "Set risk_free_rate in config.yaml to enable Black-Scholes delta calculation.",
            ticker, strategy, expiration.isoformat(), missing_delta_count, len(rows),
        )

    return rows
