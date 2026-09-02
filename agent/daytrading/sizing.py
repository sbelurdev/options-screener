"""
Position sizing and exit levels — PURE functions. No network, no I/O, no clock.

Every intermediate value is returned, not just the final contract count, so the
UI can show the whole derivation.

The stop is on the UNDERLYING price, never on the option price: option quotes gap
and spreads widen, so an option-price stop triggers on noise. See spec §7.2.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from agent.daytrading.calendar import hard_close_time, require_et

CONTRACT_MULTIPLIER = 100


@dataclass(frozen=True)
class SizingResult:
    """Full derivation, not just the answer."""
    entry_price: float
    stop_level: float
    stop_distance: float
    stop_basis: str              # which of or_high / vwap set the stop
    account_size: float
    risk_pct_per_trade: float
    risk_dollars: float
    delta: float
    risk_per_contract: float
    contracts: int
    # Set when a quantity cannot responsibly be suggested (e.g. account_size 0).
    blocked_reason: Optional[str] = None

    @property
    def sizeable(self) -> bool:
        return self.blocked_reason is None


def compute_size(
    entry_price: float,
    or_high: float,
    vwap_at_entry: float,
    delta: Optional[float],
    account_size: float,
    risk_pct_per_trade: float,
) -> SizingResult:
    """Contracts to trade, with every intermediate value exposed.

    stop_level is max(or_high, vwap) — whichever is hit first coming down.
    """
    stop_level = max(or_high, vwap_at_entry)
    stop_basis = "or_high" if or_high >= vwap_at_entry else "vwap"
    stop_distance = entry_price - stop_level
    risk_dollars = account_size * risk_pct_per_trade
    d = delta if delta is not None else 0.0
    risk_per_contract = d * stop_distance * CONTRACT_MULTIPLIER

    blocked: Optional[str] = None
    contracts = 0

    if account_size <= 0:
        blocked = "enter an account size to size the position"
    elif delta is None:
        blocked = "contract delta unavailable"
    elif stop_distance <= 0:
        # Price is at or below the stop: there is no risk unit to divide by, and
        # any quantity would be meaningless.
        blocked = (
            f"entry {entry_price:.2f} is not above the stop {stop_level:.2f} "
            f"({stop_basis}) — no valid risk distance"
        )
    elif risk_per_contract <= 0:
        blocked = "risk per contract is zero or negative"
    else:
        contracts = int(math.floor(risk_dollars / risk_per_contract))
        if contracts < 1:
            blocked = (
                f"risk budget ${risk_dollars:,.2f} is below the cost of one "
                f"contract's risk (${risk_per_contract:,.2f})"
            )

    return SizingResult(
        entry_price=entry_price,
        stop_level=stop_level,
        stop_distance=stop_distance,
        stop_basis=stop_basis,
        account_size=account_size,
        risk_pct_per_trade=risk_pct_per_trade,
        risk_dollars=risk_dollars,
        delta=d,
        risk_per_contract=risk_per_contract,
        contracts=contracts,
        blocked_reason=blocked,
    )


@dataclass(frozen=True)
class ExitLevels:
    stop_level: float
    stop_basis: str
    target_1: float           # measured move: entry + or_height
    target_1r: float          # 1R: entry + stop_distance
    time_stop: datetime       # tz-aware ET
    hard_close: Optional[datetime]  # tz-aware ET
    is_half_day: bool


def compute_exits(
    entry_price: float,
    entry_time: datetime,
    or_high: float,
    or_height: float,
    vwap_at_entry: float,
    *,
    time_stop_minutes: int = 45,
    configured_hard_close: str = "15:30",
) -> ExitLevels:
    """Exit levels for the signal card. `entry_time` must be tz-aware ET."""
    entry_time = require_et(entry_time, "entry_time")

    stop_level = max(or_high, vwap_at_entry)
    stop_basis = "or_high" if or_high >= vwap_at_entry else "vwap"
    stop_distance = entry_price - stop_level

    day = entry_time.date()
    hc = hard_close_time(day, configured_hard_close)
    # On a half day the hard close moves to early_close - 30m and theta arrives
    # faster; hard_close_time() already applies that, this just reports it.
    from agent.daytrading.calendar import session_info  # local: avoids cycle at import
    half = session_info(day).is_half_day

    return ExitLevels(
        stop_level=stop_level,
        stop_basis=stop_basis,
        target_1=entry_price + or_height,
        target_1r=entry_price + stop_distance,
        time_stop=entry_time + timedelta(minutes=time_stop_minutes),
        hard_close=hc,
        is_half_day=half,
    )
