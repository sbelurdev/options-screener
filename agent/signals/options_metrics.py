from __future__ import annotations

import math
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm


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


def get_term_for_dte(dte: int) -> Tuple[str, str]:
    """
    Map a DTE value to a (bucket_name, bucket_label) term tuple.
      0–14  DTE → Short-Term
      15–28 DTE → Medium-Term
      29+   DTE → Long-Term
    """
    if dte <= 14:
        return "short_term", "Short-Term"
    elif dte <= 28:
        return "medium_term", "Medium-Term"
    else:
        return "long_term", "Long-Term"


def select_expiration_dates(
    expirations: List[date],
    today: date,
    max_dte: int = 45,
) -> List[date]:
    """
    Select expiration dates to fetch option chains for:
      - DTE <= 14:           ALL available expirations (captures daily / weekly options)
      - 14 < DTE <= max_dte: Friday expirations only   (standard weekly / monthly)

    Same-day expirations and anything beyond max_dte are excluded.
    Returns a sorted, deduplicated list.
    """
    result: List[date] = []
    for d in expirations:
        dte = (d - today).days
        if dte <= 0:
            continue          # exclude same-day or past
        if dte > max_dte:
            continue          # hard cap
        if dte <= 14:
            result.append(d)  # all expirations within the short-term window
        elif d.weekday() == 4:
            result.append(d)  # Fridays only beyond 14 DTE
    return sorted(set(result))


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

        # Max profit at expiry (per contract = 100 shares):
        #   PUT  → keep full premium if stock stays above strike
        #   CALL → premium + (strike − spot) upside if assigned at strike
        if strategy == "PUT":
            max_profit_val = round(mid * 100, 2)
        else:
            max_profit_val = round((strike - spot + mid) * 100, 2) if spot > 0 else None

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
            "max_profit": max_profit_val,
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
