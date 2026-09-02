"""
Gate and trigger evaluation — PURE functions. No network, no I/O, no clock.

Stage A  daily name gate   (prior completed session)
Stage B  setup gate        (~09:29 ET)
Stage C  trigger           (each COMPLETED 5m bar close, 09:45 -> cutoff)

Every result carries which condition failed and its actual value, not just a
boolean — the UI shows the user why a name did not qualify.

Two invariants this module exists to protect (spec §6.3):
  * Bars are evaluated only once COMPLETE. A bar stamped 09:45 covers
    09:45-09:50 and is not decidable until 09:50. Mid-bar evaluation produces
    signals that later vanish, which destroys trust in the tool faster than
    anything else.
  * At most one fire per ticker per day. The firing bar is frozen and returned
    unchanged on every subsequent evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from agent.daytrading.calendar import ET, at_et, require_et
from agent.daytrading.indicators import (
    DailyIndicators,
    OpeningRange,
    OvernightLevels,
    relative_volume,
)

BAR_DURATION = timedelta(minutes=5)


@dataclass(frozen=True)
class Condition:
    """One evaluated condition, carrying the actual value that decided it.

    `threshold` is the single numeric value `actual` was compared against, when
    there is one (e.g. the EMA9 level for a trend check, the overnight high for
    a breakout check). It exists so a failure can be shown as both sides of the
    comparison — "356.61 < overnight_high 516.66" — without the reader having to
    cross-reference another panel to see why. Left None for conditions with no
    single threshold (a two-sided band like RSI, or a non-numeric check like the
    earnings-date test).
    """
    name: str
    passed: bool
    actual: Optional[float]
    expected: str          # human-readable, e.g. "50.0 <= rsi <= 75.0"
    detail: str = ""
    threshold: Optional[float] = None

    def describe(self) -> str:
        act = "n/a" if self.actual is None else f"{self.actual:,.4g}"
        return f"{self.name}: {act} (need {self.expected})"


@dataclass
class GateResult:
    passed: bool
    conditions: List[Condition] = field(default_factory=list)
    # Populated when the gate could not be evaluated at all (missing/degraded
    # data). Distinct from passed=False, which means "evaluated, did not
    # qualify" — the UI must never render degraded as no-signal.
    degraded_reason: Optional[str] = None

    @property
    def degraded(self) -> bool:
        return self.degraded_reason is not None

    def failures(self) -> List[Condition]:
        return [c for c in self.conditions if not c.passed]

    def first_failure(self) -> Optional[Condition]:
        f = self.failures()
        return f[0] if f else None


# ── Stage A: daily name gate ─────────────────────────────────────────────────

def evaluate_daily_gate(
    ind: Optional[DailyIndicators],
    *,
    rsi_min: float,
    rsi_max: float,
    require_close_above_trend_ma: bool = True,
    earnings_date: Optional[date] = None,
    option_expiry: Optional[date] = None,
    block_on_earnings_in_window: bool = True,
) -> GateResult:
    """Stage A, evaluated on the prior completed session.

    MACD is deliberately NOT a gate condition. It is still computed and shown as
    context in the readiness panel, but it never blocks a name — so it is also
    absent from the required-data check below, otherwise a missing MACD would
    degrade a ticker over an indicator that decides nothing.
    """
    if ind is None:
        return GateResult(False, [], degraded_reason="no daily indicators available")

    missing = [
        n for n, v in (("rsi_14", ind.rsi_14), ("trend_ma", ind.trend_ma)) if v is None
    ]
    if missing:
        return GateResult(
            False, [], degraded_reason=f"daily indicators incomplete: {', '.join(missing)}"
        )

    conds: List[Condition] = [
        Condition(
            "rsi_14", rsi_min <= ind.rsi_14 <= rsi_max, ind.rsi_14,
            f"{rsi_min:g} <= rsi <= {rsi_max:g}",
        )
    ]

    if require_close_above_trend_ma:
        label = ind.trend_ma_label or "trend MA"
        conds.append(Condition(
            f"close_above_{label.lower()}", ind.close > ind.trend_ma,
            ind.close,
            f"close > {label}",
            detail=f"close={ind.close:.2f} {label}={ind.trend_ma:.2f}",
            threshold=ind.trend_ma,
        ))

    if block_on_earnings_in_window:
        # Blocking condition: earnings landing inside the option's life. Holding
        # a 3-5 DTE call through a print is a materially different bet — IV crush
        # can take the position out even when the direction is right.
        in_window = (
            earnings_date is not None
            and option_expiry is not None
            and ind.as_of <= earnings_date <= option_expiry
        )
        conds.append(Condition(
            "no_earnings_in_window", not in_window, None,
            "no earnings on/before option expiry",
            detail=(f"earnings {earnings_date}" if earnings_date else "no earnings date known"),
        ))

    return GateResult(all(c.passed for c in conds), conds)


# ── Stage B: setup gate (~09:29 ET) ──────────────────────────────────────────

@dataclass
class SetupResult:
    gate: GateResult
    today_open: Optional[float] = None
    prev_close: Optional[float] = None
    gap_pct: Optional[float] = None
    overnight: Optional[OvernightLevels] = None

    @property
    def passed(self) -> bool:
        return self.gate.passed

    @property
    def degraded(self) -> bool:
        return self.gate.degraded


def evaluate_setup_gate(
    today_open: Optional[float],
    prev_close: Optional[float],
    overnight: Optional[OvernightLevels] = None,
) -> SetupResult:
    """Stage B. Both prices must be RAW prints (spec §1.2), not adjusted.

    No ceiling on gap size — a large gap is not disqualifying here.
    """
    if today_open is None or prev_close is None or prev_close <= 0:
        return SetupResult(
            GateResult(False, [], degraded_reason="today_open or prev_close unavailable"),
            today_open, prev_close, None, overnight,
        )

    gap_pct = (today_open - prev_close) / prev_close
    cond = Condition(
        "gap_up", today_open > prev_close, today_open, "today_open > prev_close",
        detail=f"open={today_open:.2f} prev_close={prev_close:.2f}",
        threshold=prev_close,
    )
    return SetupResult(
        GateResult(cond.passed, [cond]), today_open, prev_close, gap_pct, overnight,
    )


# ── Stage C: trigger ─────────────────────────────────────────────────────────

@dataclass
class TriggerFire:
    """A frozen firing. Once produced it is never recomputed for that day."""
    ticker: str
    bar_time: datetime           # tz-aware ET, the bar's label (left edge)
    bar_close: float
    bar_volume: float
    or_high: float
    overnight_high: Optional[float]
    vwap_at_bar: float
    or_avg_volume: float
    relative_volume: Optional[float]
    conditions: List[Condition]


@dataclass
class TriggerResult:
    fired: bool
    fire: Optional[TriggerFire] = None
    # Conditions from the most recent COMPLETED bar evaluated, for the live
    # monitor panel — shows how close a name currently is.
    last_evaluated_bar: Optional[datetime] = None
    last_conditions: List[Condition] = field(default_factory=list)
    bars_evaluated: int = 0
    degraded_reason: Optional[str] = None
    cutoff_passed: bool = False

    @property
    def degraded(self) -> bool:
        return self.degraded_reason is not None


def completed_bars(
    rth: pd.DataFrame, now_et: datetime, bar_duration: timedelta = BAR_DURATION
) -> pd.DataFrame:
    """Bars whose close has actually happened as of now_et.

    A bar labelled T covers [T, T+5m); it is complete only once now >= T+5m.
    This is the single guard that prevents mid-bar signals.
    """
    now_et = require_et(now_et, "now_et")
    if rth is None or rth.empty:
        return rth if rth is not None else pd.DataFrame()
    return rth[rth.index + bar_duration <= now_et]


def evaluate_trigger(
    ticker: str,
    rth: pd.DataFrame,
    now_et: datetime,
    *,
    opening_range: Optional[OpeningRange],
    overnight: Optional[OvernightLevels],
    vwap: pd.Series,
    day: date,
    trigger_start: str = "09:45",
    trigger_cutoff: str = "11:00",
    volume_baseline: Optional[pd.Series] = None,
    already_fired: Optional[TriggerFire] = None,
) -> TriggerResult:
    """Stage C. Scans completed bars in [trigger_start, cutoff] and fires once.

    All four conditions must hold on the SAME bar:
        close > or_high, close > overnight_high, close > vwap, volume > or_avg_vol
    """
    now_et = require_et(now_et, "now_et")

    # A prior fire is frozen — return it unchanged, never re-evaluate.
    if already_fired is not None:
        return TriggerResult(True, already_fired, already_fired.bar_time,
                             already_fired.conditions, 0)

    if opening_range is None:
        return TriggerResult(False, degraded_reason="opening range unavailable")
    if rth is None or rth.empty:
        return TriggerResult(False, degraded_reason="no intraday bars")
    if vwap is None or vwap.empty:
        return TriggerResult(False, degraded_reason="VWAP unavailable")

    start_ts = at_et(day, trigger_start)
    cutoff_ts = at_et(day, trigger_cutoff)
    cutoff_passed = now_et > cutoff_ts

    done = completed_bars(rth, now_et)
    window = done[(done.index >= start_ts) & (done.index <= cutoff_ts)]

    on_high = overnight.overnight_high if overnight else None

    last_conditions: List[Condition] = []
    last_bar_time: Optional[datetime] = None
    evaluated = 0

    for ts, bar in window.iterrows():
        evaluated += 1
        close = float(bar["Close"])
        volume = float(bar["Volume"])
        vwap_here = float(vwap.loc[ts]) if ts in vwap.index else None

        conds = [
            Condition("close_above_or_high", close > opening_range.or_high, close,
                      f"> or_high {opening_range.or_high:.2f}",
                      threshold=opening_range.or_high),
            Condition(
                "close_above_overnight_high",
                (on_high is None) or (close > on_high), close,
                f"> overnight_high {on_high:.2f}" if on_high is not None
                else "no overnight level (not blocking)",
                detail="" if on_high is not None else "overnight bars unavailable",
                threshold=on_high,
            ),
            Condition("close_above_vwap",
                      vwap_here is not None and close > vwap_here, close,
                      f"> vwap {vwap_here:.2f}" if vwap_here is not None else "> vwap (unavailable)",
                      threshold=vwap_here),
            Condition("volume_above_or_avg", volume > opening_range.or_avg_volume, volume,
                      f"> or_avg_volume {opening_range.or_avg_volume:,.0f}",
                      threshold=opening_range.or_avg_volume),
        ]
        last_conditions = conds
        last_bar_time = ts

        if all(c.passed for c in conds):
            slot = ts.strftime("%H:%M")
            fire = TriggerFire(
                ticker=ticker,
                bar_time=ts.to_pydatetime() if isinstance(ts, pd.Timestamp) else ts,
                bar_close=close,
                bar_volume=volume,
                or_high=opening_range.or_high,
                overnight_high=on_high,
                vwap_at_bar=vwap_here if vwap_here is not None else float("nan"),
                or_avg_volume=opening_range.or_avg_volume,
                relative_volume=relative_volume(volume, slot, volume_baseline)
                if volume_baseline is not None else None,
                conditions=conds,
            )
            return TriggerResult(True, fire, last_bar_time, conds, evaluated,
                                 cutoff_passed=cutoff_passed)

    return TriggerResult(False, None, last_bar_time, last_conditions, evaluated,
                         cutoff_passed=cutoff_passed)
