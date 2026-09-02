"""
Option chain filtering and contract selection — pure given a fetched chain.

Filters calls by DTE, delta band, spread as % of mid, and open interest; ranks by
distance from target delta then by tightest spread.

When nothing qualifies it reports WHICH filter eliminated the candidates and the
closest near-miss — "no contract found" with no explanation is a dead end for the
user. See spec §7.1.

Greeks source (decision recorded against spec §5.4): Public's greeks endpoint is
preferred; when it is unavailable the fallback reuses this repo's existing
Black-Scholes in agent/signals/options_metrics.py, extended here for theta,
rather than adding py_vollib. At 3-5 DTE the risk-free rate barely moves delta,
so it is hardcoded (RISK_FREE_RATE) rather than adding a rates dependency.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from scipy.stats import norm

from agent.signals.options_metrics import black_scholes_delta, safe_float, spread_pct

# 3-5 DTE: delta is essentially insensitive to r, so a constant beats a dependency.
RISK_FREE_RATE = 0.04

# Filter order is also the reporting order: the first filter to eliminate a
# contract is the one named in the rejection, which keeps the explanation stable.
FILTER_ORDER = ["dte", "delta", "spread", "open_interest"]


@dataclass
class ContractCandidate:
    symbol: str
    expiration: date
    dte: int
    strike: float
    bid: float
    ask: float
    mid: float
    spread_pct_of_mid: Optional[float]
    delta: Optional[float]
    theta: Optional[float]
    implied_volatility: Optional[float]
    open_interest: Optional[int]
    option_volume: Optional[int]
    underlying_price: float
    quote_time: Optional[datetime] = None
    underlying_quote_time: Optional[datetime] = None
    delta_source: str = "unknown"

    # Populated during filtering
    rejected_by: Optional[str] = None
    reject_detail: str = ""

    @property
    def delta_distance(self) -> float:
        return abs((self.delta if self.delta is not None else 0.0) - 0.65)


@dataclass
class SelectionResult:
    """Either a chosen contract, or a precise account of why there wasn't one."""
    contract: Optional[ContractCandidate] = None
    considered: int = 0
    eliminated_by: Dict[str, int] = field(default_factory=dict)
    near_miss: Optional[ContractCandidate] = None
    near_miss_reason: str = ""
    degraded_reason: Optional[str] = None
    # Why the chosen contract won, and what it beat.
    qualified: int = 0
    selection_reason: str = ""
    runner_up: Optional[ContractCandidate] = None

    @property
    def found(self) -> bool:
        return self.contract is not None

    @property
    def degraded(self) -> bool:
        return self.degraded_reason is not None

    def explain(self) -> str:
        if self.found:
            return "contract selected"
        if self.degraded:
            return f"degraded: {self.degraded_reason}"
        if not self.considered:
            return "no call contracts in the chain"
        parts = [f"{k}={v}" for k, v in self.eliminated_by.items() if v]
        msg = f"{self.considered} contracts considered; eliminated by " + ", ".join(parts)
        if self.near_miss is not None:
            msg += f". Closest: {self.near_miss_reason}"
        return msg


def black_scholes_theta(
    spot: float, strike: float, dte: int, iv: float, risk_free_rate: float = RISK_FREE_RATE
) -> Optional[float]:
    """Per-day theta for a call. Fallback only, when the provider supplies none.

    Extends the repo's existing Black-Scholes rather than adding a new greeks
    dependency (see module docstring).
    """
    if spot <= 0 or strike <= 0 or dte <= 0 or not iv or iv <= 0:
        return None
    t = dte / 365.0
    try:
        d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * iv * iv) * t) / (iv * math.sqrt(t))
        d2 = d1 - iv * math.sqrt(t)
        annual = (
            -(spot * norm.pdf(d1) * iv) / (2 * math.sqrt(t))
            - risk_free_rate * strike * math.exp(-risk_free_rate * t) * norm.cdf(d2)
        )
        return annual / 365.0
    except (ValueError, ZeroDivisionError):
        return None


def build_candidates(
    chain: pd.DataFrame,
    expiration: date,
    as_of: date,
    underlying_price: float,
    *,
    quote_time: Optional[datetime] = None,
    underlying_quote_time: Optional[datetime] = None,
) -> List[ContractCandidate]:
    """Normalise a provider call chain into candidates, filling greeks if absent."""
    if chain is None or chain.empty:
        return []

    dte = (expiration - as_of).days
    out: List[ContractCandidate] = []

    for _, row in chain.iterrows():
        bid = safe_float(row.get("bid"), 0.0) or 0.0
        ask = safe_float(row.get("ask"), 0.0) or 0.0
        strike = safe_float(row.get("strike"))
        if strike is None or strike <= 0:
            continue
        mid = (bid + ask) / 2.0
        iv = safe_float(row.get("impliedVolatility"))

        delta = safe_float(row.get("delta"))
        delta_source = "provider"
        if delta is None:
            delta = black_scholes_delta("CALL", underlying_price, strike, dte, iv or 0.0,
                                        RISK_FREE_RATE)
            delta_source = "black-scholes" if delta is not None else "unavailable"

        theta = safe_float(row.get("theta"))
        if theta is None and iv:
            theta = black_scholes_theta(underlying_price, strike, dte, iv)

        oi = safe_float(row.get("openInterest"))
        vol = safe_float(row.get("volume"))

        out.append(ContractCandidate(
            symbol=str(row.get("contractSymbol") or ""),
            expiration=expiration,
            dte=dte,
            strike=float(strike),
            bid=bid,
            ask=ask,
            mid=mid,
            spread_pct_of_mid=spread_pct(bid, ask),
            delta=delta,
            theta=theta,
            implied_volatility=iv,
            open_interest=int(oi) if oi is not None else None,
            option_volume=int(vol) if vol is not None else None,
            underlying_price=underlying_price,
            quote_time=quote_time,
            underlying_quote_time=underlying_quote_time,
            delta_source=delta_source,
        ))
    return out


def select_contract(
    candidates: List[ContractCandidate],
    *,
    dte_min: int,
    dte_max: int,
    delta_min: float,
    delta_max: float,
    delta_target: float,
    max_spread_pct_of_mid: float,
    min_open_interest: int,
) -> SelectionResult:
    """Apply filters in order, rank survivors, and explain any empty result."""
    if not candidates:
        return SelectionResult(considered=0)

    eliminated: Dict[str, int] = {k: 0 for k in FILTER_ORDER}
    survivors: List[ContractCandidate] = []

    for c in candidates:
        if not (dte_min <= c.dte <= dte_max):
            c.rejected_by, c.reject_detail = "dte", f"DTE {c.dte} outside [{dte_min}, {dte_max}]"
        elif c.delta is None:
            c.rejected_by, c.reject_detail = "delta", "delta unavailable"
        elif not (delta_min <= c.delta <= delta_max):
            c.rejected_by, c.reject_detail = (
                "delta", f"delta {c.delta:.3f} outside [{delta_min:.2f}, {delta_max:.2f}]")
        elif c.spread_pct_of_mid is None:
            c.rejected_by, c.reject_detail = "spread", "no two-sided market"
        elif c.spread_pct_of_mid > max_spread_pct_of_mid:
            c.rejected_by, c.reject_detail = (
                "spread", f"spread {c.spread_pct_of_mid:.2%} > {max_spread_pct_of_mid:.2%}")
        elif (c.open_interest or 0) < min_open_interest:
            c.rejected_by, c.reject_detail = (
                "open_interest", f"OI {c.open_interest or 0} < {min_open_interest}")
        else:
            survivors.append(c)
            continue
        eliminated[c.rejected_by] += 1

    if survivors:
        survivors.sort(key=lambda c: (abs(c.delta - delta_target), c.spread_pct_of_mid or 9.9))
        best = survivors[0]
        runner_up = survivors[1] if len(survivors) > 1 else None

        # State the ranking rule and the winning margin, so the choice is
        # inspectable rather than "trust the sort".
        bits = [
            f"delta {best.delta:.3f} (closest to target {delta_target:.2f}, "
            f"off by {abs(best.delta - delta_target):.3f})",
            f"spread {best.spread_pct_of_mid:.2%} of mid"
            if best.spread_pct_of_mid is not None else "spread n/a",
            f"OI {best.open_interest or 0:,}",
            f"{best.dte} DTE",
        ]
        if runner_up is not None:
            bits.append(
                f"chosen over {len(survivors) - 1} other qualifying contract"
                f"{'s' if len(survivors) > 2 else ''} "
                f"(next: {runner_up.strike:.2f} delta {runner_up.delta:.3f})"
            )
        else:
            bits.append("only qualifying contract")

        return SelectionResult(contract=best, considered=len(candidates),
                               eliminated_by=eliminated, qualified=len(survivors),
                               selection_reason="; ".join(bits), runner_up=runner_up)

    # Nothing qualified — surface the closest near-miss so the user can judge
    # whether a threshold is slightly too tight rather than hitting a dead end.
    in_dte = [c for c in candidates if dte_min <= c.dte <= dte_max and c.delta is not None]
    pool = in_dte or [c for c in candidates if c.delta is not None]
    near = min(pool, key=lambda c: abs(c.delta - delta_target), default=None)
    reason = ""
    if near is not None:
        reason = (
            f"{near.strike:.2f} exp {near.expiration} — delta {near.delta:.3f}, "
            f"spread {near.spread_pct_of_mid:.2%} " if near.spread_pct_of_mid is not None
            else f"{near.strike:.2f} exp {near.expiration} — delta {near.delta:.3f}, spread n/a "
        )
        reason += f"OI {near.open_interest or 0}; blocked by {near.rejected_by} ({near.reject_detail})"

    return SelectionResult(contract=None, considered=len(candidates),
                           eliminated_by=eliminated, near_miss=near, near_miss_reason=reason)
