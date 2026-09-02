"""
The DayTrading tab UI (Streamlit), registered alongside the existing
Calls / Puts / Performance tabs in app.py.

Five panels: watchlist config, pre-market readiness, live trigger monitor,
signal card, data health. Visual conventions match
agent/reporting/dashboard_table.py rather than introducing a second style.

Degraded inputs render as DEGRADED, never as no-signal — silence that looks like
"no setup today" when it actually means "the data did not load" is the most
dangerous failure this tool can have. See spec §8.

This tab is decision support. It never places an order.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from html import escape
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import streamlit as st

from agent.daytrading import calendar as cal
from agent.daytrading import contracts as ct
from agent.daytrading import indicators as ind
from agent.daytrading import signals as sig
from agent.daytrading import sizing as sz
from agent.daytrading.config import (
    MARKET_CONTEXT_TICKERS,
    DayTradingConfig,
    load_config,
    save_config,
)
from agent.daytrading.data import DayTradingData, TickerData, looks_halted_locally
from agent.daytrading.notify import _crit_cell as crit_cell
from agent.daytrading.notify import last_email_sent
from agent.daytrading.scheduler import get_runtime, run_live_cycle
from agent.utils.config_io import load_merged_config
from agent.utils.logging import setup_logging


DISCLAIMER = (
    "Decision support only - this tool never places an order, and nothing here is "
    "an instruction to trade. Educational use; options carry assignment, gap, "
    "event and total-loss risk."
)

_CSS = """
<style>
.dt-wrap{border:1px solid rgba(148,163,184,0.16);border-radius:12px;overflow-x:auto;
 background:rgba(15,23,42,0.35);margin-bottom:6px;}
.dt{border-collapse:separate;border-spacing:0;width:100%;font-size:12.5px;
 font-family:ui-monospace,'Segoe UI Mono',Consolas,monospace;}
.dt th{padding:8px 10px;text-align:left;white-space:nowrap;font-weight:600;font-size:10.5px;
 letter-spacing:0.08em;text-transform:uppercase;color:#94a3b8;background:rgba(30,41,59,0.85);
 border-bottom:1px solid rgba(148,163,184,0.25);}
.dt th.num,.dt td.num{text-align:right;font-variant-numeric:tabular-nums;}
.dt td{padding:5px 10px;white-space:nowrap;vertical-align:top;
 border-bottom:1px solid rgba(148,163,184,0.07);}
.dt tr.ready td{background:rgba(34,197,94,0.10);}
.dt tr.ready td:first-child{box-shadow:inset 3px 0 0 #22c55e;}
.dt tr.degraded td{background:rgba(251,191,36,0.10);}
.dt tr.degraded td:first-child{box-shadow:inset 3px 0 0 #fbbf24;}
.dt tr.excluded td{opacity:0.45;font-style:italic;}
.dt tbody tr:hover td{background:rgba(148,163,184,0.13);}
.chip{display:inline-block;padding:1px 8px;border-radius:999px;font-size:10px;
 font-weight:700;letter-spacing:0.06em;}
.chip-ready{background:rgba(34,197,94,0.18);color:#4ade80;border:1px solid rgba(34,197,94,0.45);}
.chip-no{background:rgba(148,163,184,0.14);color:#94a3b8;border:1px solid rgba(148,163,184,0.35);}
.chip-deg{background:rgba(251,191,36,0.16);color:#fbbf24;border:1px solid rgba(251,191,36,0.45);}
.chip-exc{background:rgba(148,163,184,0.10);color:#64748b;border:1px solid rgba(148,163,184,0.25);}
.chip-fire{background:rgba(239,68,68,0.16);color:#f87171;border:1px solid rgba(239,68,68,0.45);}
.dt td.why{white-space:normal;min-width:200px;max-width:420px;font-size:11px;
 line-height:1.35;color:#94a3b8;}
.dt-note{color:#64748b;font-size:11.5px;margin:2px 2px 10px;}
.dt-banner{padding:9px 12px;border-radius:8px;margin:6px 0 12px;font-size:12.5px;}
.dt-warn{background:rgba(251,191,36,0.10);border:1px solid rgba(251,191,36,0.35);color:#fbbf24;}
.dt-halt{background:rgba(239,68,68,0.10);border:1px solid rgba(239,68,68,0.40);color:#f87171;}
.dt-ok{background:rgba(34,197,94,0.08);border:1px solid rgba(34,197,94,0.30);color:#4ade80;}
.dt-card{border:1px solid rgba(148,163,184,0.20);border-radius:12px;padding:14px 16px;
 background:rgba(15,23,42,0.5);margin-bottom:10px;}
.dt-card h4{margin:0 0 8px;color:#fff;font-size:15px;}
.dt-kv{font-family:ui-monospace,Consolas,monospace;font-size:12px;color:#cbd5e1;}
.dt-kv b{color:#fff;}
</style>
"""


# ── evaluation orchestration ─────────────────────────────────────────────────

@dataclass
class TickerEvaluation:
    """Everything the panels need for one ticker on one day."""
    ticker: str
    excluded: bool = False
    exclusion_reason: str = ""
    degraded_reasons: List[str] = field(default_factory=list)

    daily: Optional[ind.DailyIndicators] = None
    daily_gate: Optional[sig.GateResult] = None
    setup: Optional[sig.SetupResult] = None

    opening_range: Optional[ind.OpeningRange] = None
    overnight: Optional[ind.OvernightLevels] = None
    vwap: Optional[pd.Series] = None
    rth: Optional[pd.DataFrame] = None
    volume_baseline: Optional[pd.Series] = None

    trigger: Optional[sig.TriggerResult] = None
    halted: bool = False
    earnings_date: Optional[date] = None

    # True when the session has not opened yet (overnight, weekend, holiday, or
    # pre-market). Stage A is still evaluated — it runs on the prior completed
    # session — but Stages B and C legitimately have no data, which is NOT the
    # same thing as a degraded feed.
    session_pending: bool = False
    pending_reason: str = ""

    @property
    def degraded(self) -> bool:
        if self.degraded_reasons:
            return True
        gates = [self.daily_gate]
        if not self.session_pending:
            gates += [self.setup.gate if self.setup else None, self.trigger]
        for r in gates:
            if r is not None and getattr(r, "degraded", False):
                return True
        return False

    @property
    def ready(self) -> bool:
        """Passed both pre-trigger gates and is not degraded or halted."""
        return (
            not self.excluded and not self.degraded and not self.halted
            and not self.session_pending
            and self.daily_gate is not None and self.daily_gate.passed
            and self.setup is not None and self.setup.passed
        )

    @property
    def gate_ok(self) -> bool:
        """Stage A only — meaningful before the session opens."""
        return self.daily_gate is not None and self.daily_gate.passed

    def status_chip(self) -> str:
        if self.excluded:
            return f"<span class='chip chip-exc'>EXCLUDED</span>"
        if self.halted:
            return "<span class='chip chip-fire'>HALTED</span>"
        if self.degraded:
            return "<span class='chip chip-deg'>DEGRADED</span>"
        if self.session_pending:
            # Before the open only the daily gate has an answer; say which.
            return ("<span class='chip chip-ready'>GATE OK</span>" if self.gate_ok
                    else "<span class='chip chip-no'>GATE FAIL</span>")
        if self.ready:
            return "<span class='chip chip-ready'>READY</span>"
        return "<span class='chip chip-no'>NOT READY</span>"

    def rationale(self) -> List[tuple]:
        """(stage, plain-language reason) for every stage that was evaluated.

        Mirrors the CC/CSP screener's `reason` convention: short factual
        clauses joined with '; ', each carrying the value that decided it.
        """
        out: List[tuple] = []
        d = self.daily

        if self.daily_gate is not None and d is not None and not self.daily_gate.degraded:
            bits = []
            if d.rsi_14 is not None:
                bits.append(f"RSI {d.rsi_14:.1f}")
            if d.macd_hist is not None:
                # Context only — MACD is not a gate, so label it as such rather
                # than letting it read like a condition that was checked.
                bits.append(
                    f"MACD {'above' if d.macd_hist > 0 else 'below'} signal "
                    f"(hist {d.macd_hist:+.3f}, context only)")
            if d.trend_ma_distance_pct is not None:
                bits.append(
                    f"close {abs(d.trend_ma_distance_pct) * 100:.1f}% "
                    f"{'above' if d.trend_ma_distance_pct > 0 else 'below'} "
                    f"{d.trend_ma_label}")
            verdict = "trend intact" if self.daily_gate.passed else "gate failed"
            if not self.daily_gate.passed:
                bits += [c.describe() for c in self.daily_gate.failures()]
            out.append((f"Daily gate ({verdict}, on {d.as_of})", "; ".join(bits)))

        if self.setup is not None and not self.setup.degraded:
            s = self.setup
            if s.gap_pct is not None:
                direction = "gapped up" if s.gap_pct > 0 else "gapped down"
                bits = [f"{direction} {abs(s.gap_pct) * 100:.2f}% "
                        f"(open {s.today_open:,.2f} vs prev close {s.prev_close:,.2f})"]
                if s.overnight and s.overnight.overnight_high is not None:
                    bits.append(f"overnight high {s.overnight.overnight_high:,.2f} "
                                f"[{s.overnight.covered_window}]")
                out.append((f"Setup ({'pass' if s.passed else 'fail'})", "; ".join(bits)))

        if self.trigger is not None and self.trigger.fired:
            f = self.trigger.fire
            bits = [
                f"broke the opening range at {f.bar_time:%H:%M} ET "
                f"(close {f.bar_close:,.2f} > OR high {f.or_high:,.2f})",
            ]
            if f.overnight_high is not None:
                bits.append(f"cleared overnight high {f.overnight_high:,.2f}")
            bits.append(f"held above VWAP {f.vwap_at_bar:,.2f}")
            vol_x = (f.bar_volume / f.or_avg_volume) if f.or_avg_volume else None
            bits.append(
                f"volume {f.bar_volume:,.0f}"
                + (f" = {vol_x:.1f}x the OR average" if vol_x else ""))
            if f.relative_volume is not None:
                bits.append(f"{f.relative_volume:.1f}x its own time-of-day norm")
            out.append(("Trigger (all four conditions on one completed bar)",
                        "; ".join(bits)))
        return out

    def blocking_reason(self) -> str:
        if self.excluded:
            return self.exclusion_reason
        if self.halted:
            return "trading halt"
        if self.degraded:
            bits = list(self.degraded_reasons)
            for r in (self.daily_gate, self.setup.gate if self.setup else None, self.trigger):
                if r is not None and getattr(r, "degraded_reason", None):
                    bits.append(r.degraded_reason)
            return "; ".join(bits)

        fails: List[str] = []
        if self.daily_gate and not self.daily_gate.passed:
            fails += [c.describe() for c in self.daily_gate.failures()]
        if self.session_pending:
            # Stage B/C have not run; say so rather than leaving the cell blank.
            return "; ".join(fails) if fails else self.pending_reason
        if self.setup and not self.setup.passed:
            fails += [c.describe() for c in self.setup.gate.failures()]
        return "; ".join(fails)


def poll_phase(cfg: DayTradingConfig, day: date, now: datetime) -> str:
    """Which polling phase `now` falls in. Pure — takes the clock as an argument.

    closed    not a trading day, or outside [poll_start, poll_end] -> no requests
    premarket before trigger_start            -> overnight_high still moving
    trigger   [trigger_start, trigger_cutoff] -> a signal can fire
    manage    after the cutoff                -> entries closed; only an open
                                                 position is worth following
    """
    now = cal.require_et(now, "now")
    if not cal.is_trading_day(day):
        # Checked before any clock-time comparison: a weekend or holiday is
        # closed regardless of what the wall clock says, so a tab left open
        # over a weekend polls nothing and no email can fire.
        return "closed"
    if now < cal.at_et(day, cfg.poll_start) or now > cal.at_et(day, cfg.poll_end):
        return "closed"
    if now < cal.at_et(day, cfg.trigger_start):
        return "premarket"
    if now <= cal.at_et(day, cfg.trigger_cutoff):
        return "trigger"
    return "manage"


def poll_set(
    phase: str,
    watchlist: List[str],
    still_live: List[str],
    fired_tickers: List[str],
    context: List[str],
) -> List[str]:
    """Tickers worth a request in this phase.

    The narrowing is what makes an all-day 5-minute cadence affordable: after
    the cutoff nothing can fire, so only a name with an open position earns a
    request; and a name that failed the daily or setup gate cannot recover
    intraday, so it earns none once the session is under way.
    """
    if phase == "closed":
        return []
    if phase == "premarket":
        # overnight_high runs to 09:29 and the opening range has not formed, so
        # every candidate still needs its bars.
        return sorted(set(watchlist) | set(context))
    if phase == "trigger":
        return sorted((set(still_live) | set(context)) & set(watchlist) | set(context))
    return sorted(set(fired_tickers) | set(context))  # manage


@dataclass(frozen=True)
class PollRow:
    """Whether one ticker is still being polled, and why."""
    ticker: str
    polling: bool
    reason: str
    category: str  # context | pending | ready | degraded | position | stopped


def poll_status(
    evals: Dict[str, "TickerEvaluation"],
    phase: str,
    context: List[str],
    fired: Dict[str, Any],
    day: date,
    cfg: DayTradingConfig,
) -> List[PollRow]:
    """Per-ticker polling decision with a stated reason.

    Two reasons a name stops for the day rather than pausing: the daily and
    setup gates both run on data fixed before the open, so a failure there
    cannot reverse intraday; and a fired signal is frozen. Anything else that
    stops is phase-driven and will resume.
    """
    rows: List[PollRow] = []
    for t in sorted(evals):
        ev = evals[t]
        fire = fired.get(f"{day.isoformat()}:{t}")

        if ev.excluded:
            rows.append(PollRow(t, False, f"excluded: {ev.exclusion_reason}", "stopped"))
            continue
        if phase == "closed":
            rows.append(PollRow(
                t, False,
                f"outside the {cfg.poll_start}-{cfg.poll_end} ET polling window",
                "stopped"))
            continue
        if fire is not None:
            # Frozen: it fired once and will not fire again today. Still polled
            # while a position is live so its exits can be tracked.
            if phase == "manage":
                rows.append(PollRow(
                    t, True,
                    f"open position from {fire.bar_time:%H:%M} - tracking exits", "position"))
            else:
                rows.append(PollRow(
                    t, True, f"signal fired {fire.bar_time:%H:%M} - frozen, tracking exits",
                    "position"))
            continue
        if t in context:
            rows.append(PollRow(t, True, "index context - always polled", "context"))
            continue
        if ev.degraded:
            rows.append(PollRow(
                t, True, f"degraded, may recover: {ev.blocking_reason()[:70]}", "degraded"))
            continue
        if ev.session_pending:
            rows.append(PollRow(t, True, ev.pending_reason or "session not open yet", "pending"))
            continue
        if ev.daily_gate is not None and not ev.daily_gate.passed:
            f = ev.daily_gate.first_failure()
            rows.append(PollRow(
                t, False,
                f"daily gate failed ({f.describe() if f else 'n/a'}) - fixed before the open, "
                f"cannot recover today", "stopped"))
            continue
        if ev.setup is not None and not ev.setup.passed:
            f = ev.setup.gate.first_failure()
            rows.append(PollRow(
                t, False,
                f"setup gate failed ({f.describe() if f else 'n/a'}) - the open is set, "
                f"cannot recover today", "stopped"))
            continue
        if phase == "manage":
            rows.append(PollRow(
                t, False,
                f"entries closed at {cfg.trigger_cutoff} and no position held", "stopped"))
            continue
        rows.append(PollRow(t, True, "ready - watching for a trigger", "ready"))
    return rows


def _find_condition(conditions: List[sig.Condition], name: str) -> Optional[sig.Condition]:
    """Exact match first, then prefix — the trend-MA condition's name is
    dynamic (close_above_ema9, close_above_sma20, ...), so callers that only
    know the family ask by prefix."""
    for c in conditions:
        if c.name == name:
            return c
    for c in conditions:
        if c.name.startswith(name):
            return c
    return None


@dataclass(frozen=True)
class CriteriaRow:
    """One ticker's gate + trigger conditions, for the per-ticker email table.

    Each field is a Condition when that check actually ran, or None when it
    was never reached — distinct states, since None must never be shown as a
    failure (see spec §8.5's degraded-vs-no-signal distinction, applied here
    to individual criteria rather than the whole ticker).
    """
    ticker: str
    monitored: bool
    reason: str
    trend_ma_label: str
    rsi_band: str
    rsi: Optional[sig.Condition] = None
    trend_ma: Optional[sig.Condition] = None
    earnings: Optional[sig.Condition] = None
    gap: Optional[sig.Condition] = None
    or_high: Optional[sig.Condition] = None
    overnight_high: Optional[sig.Condition] = None
    vwap: Optional[sig.Condition] = None
    volume: Optional[sig.Condition] = None


def build_criteria_rows(
    evals: Dict[str, "TickerEvaluation"],
    poll_rows: List[PollRow],
    cfg: DayTradingConfig,
) -> List[CriteriaRow]:
    """One CriteriaRow per ticker, reusing the exact Condition objects the
    gates/trigger already produced — this table shows no number that was not
    already used to make a real decision.
    """
    by_ticker = {r.ticker: r for r in poll_rows}
    label = f"{cfg.trend_ma_type.upper()}{cfg.trend_ma_period}"
    rsi_band = f"{cfg.rsi_min:g}-{cfg.rsi_max:g}"
    rows: List[CriteriaRow] = []

    for t in sorted(evals):
        ev = evals[t]
        pr = by_ticker.get(t)
        monitored = pr.polling if pr else False
        reason = pr.reason if pr else ""

        daily_conds = ev.daily_gate.conditions if (ev.daily_gate and not ev.daily_gate.degraded) else []
        setup_conds = (ev.setup.gate.conditions
                       if (ev.setup and not ev.setup.degraded) else [])

        # Trigger conditions: a fired ticker's are frozen on the firing bar;
        # an actively-watched ticker's are from the most recently evaluated
        # bar (live, can still change); anything else never reached Stage C.
        trig_conds: List[sig.Condition] = []
        if ev.trigger is not None:
            if ev.trigger.fired and ev.trigger.fire is not None:
                trig_conds = ev.trigger.fire.conditions
            elif not ev.trigger.degraded:
                trig_conds = ev.trigger.last_conditions

        rows.append(CriteriaRow(
            ticker=t,
            monitored=monitored,
            reason=reason,
            trend_ma_label=label,
            rsi_band=rsi_band,
            rsi=_find_condition(daily_conds, "rsi_14"),
            trend_ma=_find_condition(daily_conds, "close_above_"),
            earnings=_find_condition(daily_conds, "no_earnings_in_window"),
            gap=_find_condition(setup_conds, "gap_up"),
            or_high=_find_condition(trig_conds, "close_above_or_high"),
            overnight_high=_find_condition(trig_conds, "close_above_overnight_high"),
            vwap=_find_condition(trig_conds, "close_above_vwap"),
            volume=_find_condition(trig_conds, "volume_above_or_avg"),
        ))
    return rows


def evaluate_ticker(
    ticker: str,
    td: Optional[TickerData],
    cfg: DayTradingConfig,
    day: date,
    now: datetime,
    *,
    today_bars: Optional[pd.DataFrame] = None,
    earnings_date: Optional[date] = None,
    option_expiry: Optional[date] = None,
    halted: bool = False,
    already_fired: Optional[sig.TriggerFire] = None,
    static: Optional[Any] = None,
) -> TickerEvaluation:
    """Run the full pipeline for one ticker. Pure apart from the data handed in."""
    # Validate at the entry point: a naive `now` would otherwise reach a pandas
    # comparison against a tz-aware index and surface as an opaque TypeError
    # instead of the intended timezone error.
    now = cal.require_et(now, "now")
    ev = TickerEvaluation(ticker=ticker, earnings_date=earnings_date, halted=halted)

    if cfg.is_excluded(ticker):
        ev.excluded = True
        ev.exclusion_reason = cfg.exclusion_reason(ticker)
        return ev

    if td is None or td.degraded:
        ev.degraded_reasons = list(td.degraded_reasons) if td else ["no data fetched"]
        if td is None:
            return ev

    # Stage A
    # Per-day constants come precomputed when a StaticContext is supplied;
    # recomputing them every rerun cost ~220ms/ticker for values that cannot move.
    ev.daily = (static.daily if static is not None else
                ind.daily_indicators(td.daily_adj, cfg.trend_ma_type, cfg.trend_ma_period))
    ev.daily_gate = sig.evaluate_daily_gate(
        ev.daily,
        rsi_min=cfg.rsi_min, rsi_max=cfg.rsi_max,
        require_close_above_trend_ma=cfg.require_close_above_trend_ma,
        earnings_date=earnings_date, option_expiry=option_expiry,
        block_on_earnings_in_window=cfg.block_on_earnings_in_window,
    )

    # Intraday frames: today's live bars when available, else the warm-up frame.
    bars = td.intraday
    if today_bars is not None and not today_bars.empty:
        bars = pd.concat([td.intraday, today_bars])
        bars = bars[~bars.index.duplicated(keep="last")].sort_index()

    prev = cal.previous_session(day)
    ev.rth = ind.session_bars(bars, day)
    ev.overnight = ind.overnight_levels(bars, prev, day) if prev else None
    ev.volume_baseline = (static.volume_baseline if static is not None
                          else ind.time_of_day_volume_baseline(bars, day))

    # Before the open there is legitimately no intraday session data. Stop here
    # rather than running Stage B against nothing, which would report a missing
    # today_open as a degraded feed when the market is simply shut.
    if not cal.session_has_started(day, now):
        ev.session_pending = True
        info = cal.session_info(day)
        if not info.is_trading_day:
            nxt = cal.next_session(day)
            ev.pending_reason = (
                f"{day.isoformat()} is not a trading day"
                + (f" - next session {nxt.isoformat()}" if nxt else "")
            )
        else:
            ev.pending_reason = f"session opens {info.market_open:%H:%M} ET"
        return ev

    # Stage B — reference levels come from the RAW frame (spec §1.2)
    prev_close = (static.prev_close if static is not None
                  else ind.prev_close_raw(td.daily_raw))
    today_open = float(ev.rth["Open"].iloc[0]) if ev.rth is not None and not ev.rth.empty else None
    ev.setup = sig.evaluate_setup_gate(today_open, prev_close, ev.overnight)

    # Stage C
    if ev.rth is not None and not ev.rth.empty:
        ev.opening_range = ind.opening_range(ev.rth, cfg.or_start, cfg.or_end)
        ev.vwap = ind.session_vwap(ev.rth)
        if halted or looks_halted_locally(ev.rth, now):
            ev.halted = True

        if ev.daily_gate.passed and ev.setup.passed and not ev.halted:
            ev.trigger = sig.evaluate_trigger(
                ticker, ev.rth, now,
                opening_range=ev.opening_range, overnight=ev.overnight,
                vwap=ev.vwap, day=day,
                trigger_start=cfg.trigger_start, trigger_cutoff=cfg.trigger_cutoff,
                volume_baseline=ev.volume_baseline, already_fired=already_fired,
            )
    return ev


# ── small render helpers ─────────────────────────────────────────────────────

def _f(v: Optional[float], fmt: str = ",.2f", dash: str = "-") -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return dash
    return format(v, fmt)


def _pct(v: Optional[float], places: int = 2) -> str:
    return "-" if v is None or pd.isna(v) else f"{v * 100:.{places}f}%"


def _table(headers: List[str], rows: List[Dict[str, Any]], numeric: set) -> str:
    out = [_CSS, "<div class='dt-wrap'><table class='dt'><thead><tr>"]
    for h in headers:
        out.append(f"<th{' class=\"num\"' if h in numeric else ''}>{escape(h)}</th>")
    out.append("</tr></thead><tbody>")
    for r in rows:
        out.append(f"<tr class='{r.get('_cls', '')}'>")
        for h in headers:
            cell = r.get(h, "")
            cls = "num" if h in numeric else ("why" if h in ("Why", "Reason", "Blocking") else "")
            out.append(f"<td{f' class=\"{cls}\"' if cls else ''}>{cell}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)


# ── panel 8.1: watchlist configuration ───────────────────────────────────────

def _render_watchlist(cfg: DayTradingConfig, profile: str) -> None:
    st.markdown("#### Watchlist")
    c1, c2 = st.columns([3, 2])

    with c1:
        new = st.text_input("Add ticker", key="dt_add_ticker", placeholder="e.g. NVDA").strip().upper()
        add, add_excl = st.columns(2)
        if add.button("Add to watchlist", use_container_width=True, disabled=not new):
            if new in [t.upper() for t in cfg.watchlist]:
                st.warning(f"{new} is already on the watchlist.")
            else:
                cfg.watchlist.append(new)
                save_config(profile, cfg)
                st.success(f"Added {new}.")
                st.rerun()
        # Spec §3: prompt once, with a one-click path to exclude instead.
        if add_excl.button("Add as exclusion", use_container_width=True, disabled=not new,
                           help="Do you hold this, or work for this company?"):
            cfg.exclusions[new] = "held / employer stock"
            if new not in [t.upper() for t in cfg.watchlist]:
                cfg.watchlist.append(new)
            save_config(profile, cfg)
            st.info(f"{new} added as an exclusion — it will never be evaluated.")
            st.rerun()
        if new:
            st.caption(
                "Do you hold this, or work for this company? Insider-trading policies "
                "commonly ban short-term trading and derivatives in employer stock, and "
                "day-trading calls on a name you already hold can create wash sales and "
                "straddle-rule complications. Use **Add as exclusion** if either applies."
            )

    with c2:
        removable = list(cfg.watchlist)
        drop = st.selectbox("Remove ticker", ["-"] + removable, key="dt_remove")
        if st.button("Remove", use_container_width=True, disabled=(drop == "-")):
            cfg.watchlist = [t for t in cfg.watchlist if t != drop]
            cfg.exclusions.pop(drop, None)
            save_config(profile, cfg)
            st.rerun()

    if cfg.exclusions:
        st.caption("Excluded — never evaluated, never produces a signal")
        for tkr, reason in list(cfg.exclusions.items()):
            e1, e2, e3 = st.columns([1, 3, 1])
            e1.markdown(f"<span class='chip chip-exc'>{escape(tkr)}</span>", unsafe_allow_html=True)
            e2.markdown(f"<span style='opacity:0.6'>{escape(reason)}</span>", unsafe_allow_html=True)
            if e3.button("Un-exclude", key=f"dt_unexc_{tkr}"):
                cfg.exclusions.pop(tkr, None)
                save_config(profile, cfg)
                st.rerun()

    st.markdown(_CSS, unsafe_allow_html=True)


# ── panel 8.2: pre-market readiness ──────────────────────────────────────────

def _render_premarket(evals: Dict[str, TickerEvaluation], cfg: DayTradingConfig,
                      day: Optional[date] = None, now: Optional[datetime] = None) -> None:
    st.markdown("#### Pre-market readiness")

    if day is not None and now is not None and not cal.session_has_started(day, now):
        info = cal.session_info(day)
        target = day if info.is_trading_day else (cal.next_session(day) or day)
        prev = cal.previous_session(day)
        st.markdown(
            f"<div class='dt-banner dt-warn'>Market closed. Showing the daily gate for the "
            f"<b>next session ({target:%a %d %b})</b>, computed on the last completed session"
            + (f" ({prev:%a %d %b})" if prev else "")
            + ". Gap, opening range and trigger are unavailable until the open - "
            "that is expected, not degraded data.</div>",
            unsafe_allow_html=True,
        )

    ctx = [t for t in MARKET_CONTEXT_TICKERS if t in evals]
    if ctx:
        cols = st.columns(len(ctx))
        for col, t in zip(cols, ctx):
            ev = evals[t]
            last = float(ev.rth["Close"].iloc[-1]) if ev.rth is not None and not ev.rth.empty else None
            vw = float(ev.vwap.iloc[-1]) if ev.vwap is not None and len(ev.vwap) else None
            pc = ev.setup.prev_close if ev.setup else None
            col.metric(
                f"{t} (index context)",
                f"${_f(last)}",
                f"{_pct((last - pc) / pc) if (last and pc) else '-'} vs prev close",
            )
            col.caption(f"prev close ${_f(pc)} · VWAP ${_f(vw)}")
        st.caption(
            "Index context is displayed whether or not `require_market_gate` is on — "
            f"currently **{'a hard gate' if cfg.require_market_gate else 'non-blocking'}**. "
            "These names run 0.6-0.9 correlated to the index intraday."
        )

    headers = ["Ticker", "Status", "RSI", "MACD hist*", "Trend MA dist", "Gap", "Overnight high",
               "Earnings", "Blocking"]
    numeric = {"RSI", "MACD hist*", "Trend MA dist", "Gap", "Overnight high"}
    rows: List[Dict[str, Any]] = []

    for t, ev in evals.items():
        cls = ("excluded" if ev.excluded else
               "degraded" if ev.degraded else
               "ready" if ev.ready else "")
        d = ev.daily
        on_txt = "-"
        if ev.overnight and ev.overnight.overnight_high is not None:
            on_txt = (f"{ev.overnight.overnight_high:,.2f}"
                      f"<div class='dt-note' style='margin:0'>{escape(ev.overnight.covered_window)}</div>")
        rows.append({
            "Ticker": f"<b>{escape(t)}</b>",
            "Status": ev.status_chip(),
            "RSI": _f(d.rsi_14 if d else None, ".1f"),
            "MACD hist*": _f(d.macd_hist if d else None, "+.3f"),
            "Trend MA dist": (f"{_pct(d.trend_ma_distance_pct, 1)} "
                              f"<span style='opacity:.6'>{escape(d.trend_ma_label)}</span>"
                              if d else "-"),
            "Gap": _pct(ev.setup.gap_pct if ev.setup else None, 2),
            "Overnight high": on_txt,
            "Earnings": ev.earnings_date.isoformat() if ev.earnings_date else "-",
            "Blocking": escape(ev.blocking_reason()),
            "_cls": cls,
        })

    st.html(_table(headers, rows, numeric))
    st.caption(
        "Overnight high is computed over the window actually covered by the feed "
        "(shown under the value). yfinance provides no bars between 20:00 and 03:59 ET, "
        "and reports zero volume on all extended-hours bars — so no pre-market volume "
        "figure is shown rather than a misleading zero."
    )


# ── panel 8.3: live trigger monitor ──────────────────────────────────────────

def _render_monitor(evals: Dict[str, TickerEvaluation], cfg: DayTradingConfig,
                    day: date, now: datetime) -> None:
    st.markdown("#### Live trigger monitor")

    if not cal.session_has_started(day, now):
        info = cal.session_info(day)
        if info.is_trading_day:
            msg = (f"Session opens {info.market_open:%H:%M} ET. The trigger window is "
                   f"{cfg.trigger_start}-{cfg.trigger_cutoff} ET.")
        else:
            nxt = cal.next_session(day)
            msg = (f"{day:%a %d %b %Y} is not a trading day."
                   + (f" Next session: {nxt:%a %d %b %Y}." if nxt else ""))
        st.markdown(f"<div class='dt-banner dt-warn'>{escape(msg)} No intraday data "
                    f"exists yet - this is not a data problem.</div>",
                    unsafe_allow_html=True)
        return

    cutoff = cal.at_et(day, cfg.trigger_cutoff)
    past = now > cutoff
    remaining = cutoff - now

    if past:
        st.markdown(
            f"<div class='dt-banner dt-warn'>Trigger window closed at "
            f"{cfg.trigger_cutoff} ET — no further entries today.</div>",
            unsafe_allow_html=True,
        )
    else:
        mins = int(remaining.total_seconds() // 60)
        st.markdown(
            f"<div class='dt-banner dt-ok'>Trigger window {cfg.trigger_start}-"
            f"{cfg.trigger_cutoff} ET · <b>{mins // 60}h {mins % 60}m</b> to cutoff</div>",
            unsafe_allow_html=True,
        )

    candidates = {t: e for t, e in evals.items() if e.ready or (e.trigger and e.trigger.fired)}
    if not candidates:
        st.caption("No tickers passed the pre-trigger gates today.")
        return

    headers = ["Ticker", "Trigger", "OR high", "OR low", "Last", "VWAP",
               "Last vol", "OR avg vol", "RelVol", "Failing"]
    numeric = {"OR high", "OR low", "Last", "VWAP", "Last vol", "OR avg vol", "RelVol"}
    rows: List[Dict[str, Any]] = []

    for t, ev in candidates.items():
        tr = ev.trigger
        last_close = last_vol = None
        relvol = None
        if ev.rth is not None and not ev.rth.empty:
            done = sig.completed_bars(ev.rth, now)
            if not done.empty:
                last_close = float(done["Close"].iloc[-1])
                last_vol = float(done["Volume"].iloc[-1])
                relvol = ind.relative_volume(
                    last_vol, done.index[-1].strftime("%H:%M"), ev.volume_baseline)

        if tr and tr.fired:
            chip = "<span class='chip chip-fire'>FIRED</span>"
            failing = f"fired {tr.fire.bar_time:%H:%M} ET"
        elif tr and tr.degraded:
            chip = "<span class='chip chip-deg'>DEGRADED</span>"
            failing = escape(tr.degraded_reason or "")
        else:
            chip = "<span class='chip chip-no'>WATCHING</span>"
            failing = escape("; ".join(
                c.describe() for c in (tr.last_conditions if tr else []) if not c.passed
            ) or "waiting for a completed bar")

        vw = float(ev.vwap.iloc[-1]) if ev.vwap is not None and len(ev.vwap) else None
        rows.append({
            "Ticker": f"<b>{escape(t)}</b>",
            "Trigger": chip,
            "OR high": _f(ev.opening_range.or_high if ev.opening_range else None),
            "OR low": _f(ev.opening_range.or_low if ev.opening_range else None),
            "Last": _f(last_close),
            "VWAP": _f(vw),
            "Last vol": _f(last_vol, ",.0f"),
            "OR avg vol": _f(ev.opening_range.or_avg_volume if ev.opening_range else None, ",.0f"),
            "RelVol": f"{relvol:.2f}x" if relvol is not None else "-",
            "Failing": failing,
            "_cls": "degraded" if (tr and tr.degraded) else "",
        })

    st.html(_table(headers, rows, numeric))
    st.caption(
        "Evaluated on completed 5-minute bars only — a bar stamped 09:45 covers "
        "09:45-09:50 and is not decided until 09:50. RelVol is context, not a gate."
    )


# ── panel 8.4: signal card ───────────────────────────────────────────────────

def _render_signal_card(ev: TickerEvaluation, cfg: DayTradingConfig,
                        selection: Optional[ct.SelectionResult]) -> None:
    fire = ev.trigger.fire
    st.markdown(
        f"<div class='dt-banner dt-halt'><b>Recommendation only.</b> No order has been "
        f"placed and none will be. This tool cannot trade.</div>",
        unsafe_allow_html=True,
    )
    st.markdown(f"##### {ev.ticker} — triggered {fire.bar_time:%H:%M} ET")

    # Why this recommendation exists, stage by stage, in plain language.
    reasons = ev.rationale()
    if reasons:
        st.html(_table(
            ["Stage", "Reason"],
            [{"Stage": f"<b>{escape(stage)}</b>", "Reason": escape(text)}
             for stage, text in reasons],
            set(),
        ))

    trig_rows = [
        {"Condition": escape(c.name), "Value on firing bar": _f(c.actual),
         "Required": escape(c.expected)}
        for c in fire.conditions
    ]
    st.html(_table(["Condition", "Value on firing bar", "Required"], trig_rows,
                   {"Value on firing bar"}))

    if selection is None or not selection.found:
        why = selection.explain() if selection else "contract lookup not run"
        st.markdown(
            f"<div class='dt-banner dt-warn'>No contract selected. {escape(why)}</div>",
            unsafe_allow_html=True,
        )
        # The exit levels sit on the UNDERLYING, so they are still valid and
        # still worth showing without a contract — only sizing needs a delta.
        _render_exits(ev, cfg, fire, entry=fire.bar_close, size=None)
        return

    c = selection.contract
    st.html(_table(
        ["Strike", "Expiry", "DTE", "Bid", "Ask", "Mid", "Spread", "Delta", "Theta",
         "IV", "OI", "Volume", "Underlying"],
        [{
            "Strike": _f(c.strike), "Expiry": c.expiration.isoformat(), "DTE": str(c.dte),
            "Bid": _f(c.bid), "Ask": _f(c.ask), "Mid": _f(c.mid),
            "Spread": _pct(c.spread_pct_of_mid), "Delta": _f(c.delta, ".3f"),
            "Theta": _f(c.theta, ".3f"), "IV": _pct(c.implied_volatility, 1),
            "OI": _f(c.open_interest, ",.0f"), "Volume": _f(c.option_volume, ",.0f"),
            "Underlying": _f(c.underlying_price),
        }],
        {"Strike", "DTE", "Bid", "Ask", "Mid", "Spread", "Delta", "Theta", "IV", "OI",
         "Volume", "Underlying"},
    ))
    if selection.selection_reason:
        st.markdown(
            f"<div class='dt-note'><b>Why this contract:</b> "
            f"{escape(selection.selection_reason)}. Ranked by distance from the "
            f"{cfg.delta_target:.2f} delta target, then tightest spread; "
            f"{selection.qualified} of {selection.considered} contracts passed the "
            f"DTE / delta / spread / open-interest filters.</div>",
            unsafe_allow_html=True,
        )
    if c.delta_source != "provider":
        st.caption(f"Greeks source: {c.delta_source} (provider greeks unavailable).")

    entry = c.underlying_price
    vwap_at = float(ev.vwap.loc[fire.bar_time]) if ev.vwap is not None and fire.bar_time in ev.vwap.index else fire.vwap_at_bar
    size = sz.compute_size(entry, fire.or_high, vwap_at, c.delta,
                           cfg.account_size, cfg.risk_pct_per_trade)
    exits = sz.compute_exits(entry, fire.bar_time, fire.or_high,
                             (ev.opening_range.or_height if ev.opening_range else 0.0),
                             vwap_at, time_stop_minutes=cfg.time_stop_minutes,
                             configured_hard_close=cfg.hard_close)

    st.markdown("**Sizing** — every step shown, not just the answer")
    st.markdown(
        f"<div class='dt-card dt-kv'>"
        f"stop = max(or_high {size.stop_level if size.stop_basis=='or_high' else fire.or_high:,.2f}, "
        f"vwap {vwap_at:,.2f}) = <b>{size.stop_level:,.2f}</b> ({size.stop_basis})<br>"
        f"stop_distance = {entry:,.2f} − {size.stop_level:,.2f} = <b>{size.stop_distance:,.2f}</b><br>"
        f"risk_dollars = {size.account_size:,.2f} × {size.risk_pct_per_trade:.3%} "
        f"= <b>{size.risk_dollars:,.2f}</b><br>"
        f"risk_per_contract = delta {size.delta:.3f} × {size.stop_distance:,.2f} × 100 "
        f"= <b>{size.risk_per_contract:,.2f}</b><br>"
        f"contracts = floor({size.risk_dollars:,.2f} / {size.risk_per_contract:,.2f}) = "
        f"<b>{size.contracts}</b>"
        f"</div>", unsafe_allow_html=True)
    if not size.sizeable:
        st.markdown(
            f"<div class='dt-banner dt-warn'>No quantity suggested — {escape(size.blocked_reason or '')}.</div>",
            unsafe_allow_html=True)

    _render_exits(ev, cfg, fire, entry=entry, size=size)


def _render_exits(ev: TickerEvaluation, cfg: DayTradingConfig, fire, *,
                  entry: float, size: Optional[sz.SizingResult]) -> None:
    """Exit levels. Rendered with or without a contract — they sit on the
    UNDERLYING, so they remain valid when no option was selected."""
    vwap_at = (float(ev.vwap.loc[fire.bar_time])
               if ev.vwap is not None and fire.bar_time in ev.vwap.index
               else fire.vwap_at_bar)
    or_height = ev.opening_range.or_height if ev.opening_range else 0.0
    exits = sz.compute_exits(entry, fire.bar_time, fire.or_high, or_height, vwap_at,
                             time_stop_minutes=cfg.time_stop_minutes,
                             configured_hard_close=cfg.hard_close)

    st.html(_table(
        ["Level", "Value", "Basis"],
        [
            {"Level": "Stop", "Value": _f(exits.stop_level),
             "Basis": f"underlying price stop ({exits.stop_basis}) — never on the option price"},
            {"Level": "Target 1", "Value": _f(exits.target_1), "Basis": "entry + OR height (measured move)"},
            {"Level": "1R", "Value": _f(exits.target_1r), "Basis": "entry + stop distance"},
            {"Level": "Time stop", "Value": exits.time_stop.strftime("%H:%M ET"),
             "Basis": f"entry + {cfg.time_stop_minutes} min"},
            {"Level": "Hard close", "Value": exits.hard_close.strftime("%H:%M ET") if exits.hard_close else "-",
             "Basis": "half day — early close − 30 min" if exits.is_half_day else "configured hard close"},
        ],
        {"Value"},
    ))

    stop_dist = entry - exits.stop_level
    rr = ((exits.target_1 - entry) / stop_dist) if stop_dist > 0 else None
    st.markdown(
        f"<div class='dt-note'><b>Why these exits:</b> the stop sits at the "
        f"{exits.stop_basis} ({exits.stop_level:,.2f}) because that is the first level "
        f"the move fails at coming down, and it is placed on the <b>underlying</b> — "
        f"option quotes gap and spreads widen, so an option-price stop triggers on noise. "
        f"Target 1 projects the opening range height ({or_height:,.2f}) from entry"
        + (f", giving {rr:.1f}R against a {stop_dist:,.2f} risk" if rr else "")
        + f". The {cfg.time_stop_minutes}-minute time stop exists because a breakout that "
        f"has not worked by then usually is not going to, and theta is running the whole time."
        f"</div>",
        unsafe_allow_html=True,
    )


# ── polling panel ────────────────────────────────────────────────────────────

_POLL_CHIP = {
    "context": ("chip-ready", "POLLING"),
    "pending": ("chip-ready", "POLLING"),
    "ready": ("chip-ready", "POLLING"),
    "degraded": ("chip-deg", "POLLING"),
    "position": ("chip-fire", "POLLING"),
    "stopped": ("chip-no", "STOPPED"),
}


def _render_polling(rows: List[PollRow], phase: str, cfg: DayTradingConfig,
                    day: date, now: datetime) -> None:
    st.markdown("#### Polling")
    on = [r for r in rows if r.polling]
    off = [r for r in rows if not r.polling]

    nxt = ""
    if phase != "closed":
        mins = 5 - (now.minute % 5)
        nxt = f" &middot; next poll in ~{mins} min"
    st.markdown(
        f"<div class='dt-banner dt-ok'><b>{len(on)} polling &middot; {len(off)} stopped</b> "
        f"&mdash; phase <b>{escape(phase)}</b>, window {cfg.poll_start}-{cfg.poll_end} ET"
        f"{nxt}. Each polled ticker costs one request per 5-minute cycle.</div>",
        unsafe_allow_html=True,
    )

    body = []
    for r in rows:
        css, label = _POLL_CHIP[r.category]
        body.append({
            "Ticker": f"<b>{escape(r.ticker)}</b>",
            "Polling": f"<span class='chip {css}'>{label}</span>",
            "Reason": escape(r.reason),
            "_cls": "" if r.polling else "excluded",
        })
    st.html(_table(["Ticker", "Polling", "Reason"], body, set()))
    st.caption(
        "A ticker stops for the day once its daily or setup gate fails — both run on "
        "data fixed before the open, so the answer cannot change intraday. Degraded "
        "names keep polling because the failure may be transient."
    )


# ── panel 8.4: monitoring status (the criteria table also sent by email) ────

def _render_criteria(rows: List["CriteriaRow"]) -> None:
    st.markdown("#### Monitoring status")
    st.caption(
        "The same table sent in every DayTrading email - one row per ticker, one "
        "column per gate/trigger criterion, live as of this render. Cells read "
        "PASS/FAIL actual vs threshold, e.g. \"FAIL 730.59 < 731.40\", so a miss "
        "is self-explanatory without cross-referencing another panel."
    )
    if not rows:
        st.info("No tickers evaluated yet.")
        return

    rsi_h = f"RSI ({rows[0].rsi_band})"
    ma_h = rows[0].trend_ma_label
    headers = ["Ticker", "Monitored", rsi_h, ma_h, "Earnings", "Gap Up",
              ">OR High", ">Overnight High", ">VWAP", "Vol>OR Avg", "Reason"]
    body = []
    for r in rows:
        body.append({
            "Ticker": f"<b>{escape(r.ticker)}</b>",
            "Monitored": "Yes" if r.monitored else "No",
            rsi_h: escape(crit_cell(r.rsi)),
            ma_h: escape(crit_cell(r.trend_ma)),
            "Earnings": escape(crit_cell(r.earnings)),
            "Gap Up": escape(crit_cell(r.gap)),
            ">OR High": escape(crit_cell(r.or_high)),
            ">Overnight High": escape(crit_cell(r.overnight_high)),
            ">VWAP": escape(crit_cell(r.vwap)),
            "Vol>OR Avg": escape(crit_cell(r.volume)),
            "Reason": escape(r.reason),
            "_cls": "" if r.monitored else "excluded",
        })
    st.html(_table(headers, body, set()))


# ── panel 8.5: data health ───────────────────────────────────────────────────

def _render_health(dd: DayTradingData, evals: Dict[str, TickerEvaluation],
                   day: date, halts: List[str], halt_ts: Optional[datetime],
                   phase_info: Optional[tuple] = None) -> None:
    st.markdown("#### Data health")
    info = cal.session_info(day)

    if not info.is_trading_day:
        st.markdown(f"<div class='dt-banner dt-warn'>{day.isoformat()} is not an NYSE "
                    f"trading day — no evaluation runs.</div>", unsafe_allow_html=True)
    elif info.is_half_day:
        st.markdown(
            f"<div class='dt-banner dt-warn'><b>Half day.</b> Early close "
            f"{info.early_close_time:%H:%M} ET; hard close moves to "
            f"{cal.hard_close_time(day):%H:%M} ET and theta arrives faster.</div>",
            unsafe_allow_html=True)

    if dd.breaker.tripped:
        st.markdown(
            f"<div class='dt-banner dt-halt'><b>Circuit breaker tripped</b> after "
            f"{dd.breaker.consecutive_failures} consecutive failures "
            f"({escape(dd.breaker.last_error)}). Polling stopped — press Refresh to retry.</div>",
            unsafe_allow_html=True)
    if halts:
        st.markdown(f"<div class='dt-banner dt-halt'><b>Halted:</b> {escape(', '.join(halts))}</div>",
                    unsafe_allow_html=True)

    if phase_info:
        phase, polled, cfg_ = phase_info
        blurb = {
            "closed": f"outside the {cfg_.poll_start}-{cfg_.poll_end} ET polling window — no requests",
            "premarket": "pre-market — polling the full watchlist to keep overnight_high current",
            "trigger": "trigger window — polling only names that can still fire, plus index context",
            "manage": "entries closed — polling only open positions, plus index context",
        }[phase]
        st.markdown(
            f"<div class='dt-banner dt-ok'><b>Poll phase: {escape(phase)}</b> &mdash; "
            f"{escape(blurb)}. This render polled <b>{len(polled)}</b> ticker"
            f"{'s' if len(polled) != 1 else ''}"
            + (f" ({escape(', '.join(polled))})" if polled else "") + ".</div>",
            unsafe_allow_html=True,
        )

    wu = dd.last_warmup
    age = dd.cache_age()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Warm-up", wu.fetched_at.strftime("%H:%M:%S") if wu else "not run",
              f"{age.total_seconds() / 60:.1f} min ago" if age else None)
    c2.metric("Warm-up source", "cache" if (wu and wu.from_cache) else "network",
              f"{wu.elapsed_seconds:.1f}s" if wu else None)
    c3.metric("Consecutive failures", dd.breaker.consecutive_failures,
              "TRIPPED" if dd.breaker.tripped else "ok")
    c4.metric("Halt feed", halt_ts.strftime("%H:%M:%S") if halt_ts else "not fetched")

    rows = []
    for t, ev in evals.items():
        if ev.excluded:
            continue
        ext = 0
        if ev.overnight:
            ext = ev.overnight.bar_count
        rows.append({
            "Ticker": f"<b>{escape(t)}</b>",
            "Ext-hours bars": str(ext),
            "Overnight window": escape(ev.overnight.covered_window) if ev.overnight else "-",
            "RTH bars": str(len(ev.rth) if ev.rth is not None else 0),
            "State": ("<span class='chip chip-deg'>DEGRADED</span>" if ev.degraded
                      else "<span class='chip chip-ready'>OK</span>"),
            "Reason": escape("; ".join(ev.degraded_reasons)) if ev.degraded_reasons else "",
            "_cls": "degraded" if ev.degraded else "",
        })
    if rows:
        st.html(_table(["Ticker", "Ext-hours bars", "Overnight window", "RTH bars", "State", "Reason"],
                       rows, {"Ext-hours bars", "RTH bars"}))
    st.caption(
        "A degraded ticker is shown as DEGRADED, never as no-signal — 'no setup today' "
        "and 'the data did not load' must not look the same."
    )


# ── entry point ──────────────────────────────────────────────────────────────

def render_daytrading_tab(profile: str, logger=None) -> None:
    """Render the whole tab. Called from app.py inside its tab context."""
    st.markdown(_CSS, unsafe_allow_html=True)
    cfg = load_config(profile)

    problems = cfg.validate()
    if problems:
        st.error("Configuration problems: " + "; ".join(problems))

    live_now = cal.now_et()

    # ── simulation control ───────────────────────────────────────────────────
    # Replays one past session through the exact same evaluation path. This is
    # deliberately a single-day replay, not a backtest engine (spec §0) — no
    # multi-day sweep, no aggregate P&L.
    sim_default = cal.previous_session(live_now.date()) or live_now.date()
    if cal.session_has_started(live_now.date(), live_now):
        sim_default = live_now.date()

    sc = st.columns([1.2, 1, 1, 2])
    sim_on = sc[0].toggle(
        "Simulate a session", value=not cal.session_has_started(live_now.date(), live_now),
        help="Replay a past trading day through the same gates, triggers and sizing. "
             "Historical data is limited to the last ~60 days of 5-minute bars.",
    )
    sim_date = sc[1].date_input(
        "Simulation date", value=sim_default,
        min_value=live_now.date() - timedelta(days=58),
        max_value=live_now.date(), disabled=not sim_on, key="dt_sim_date",
    )
    sim_time = sc[2].time_input(
        "As of (ET)", value=time(10, 0), step=300,
        disabled=not sim_on, key="dt_sim_time",
        help="The simulated clock. 10:00 sits inside the 09:45-11:00 trigger window "
             "with three completed bars already evaluated. Move it to 11:00 to see "
             "the full window's outcome; before 09:45 nothing can fire, because the "
             "opening range needs the 09:30, 09:35 and 09:40 bars first.",
    )

    if sim_on:
        if not cal.is_trading_day(sim_date):
            nxt = cal.previous_session(sim_date)
            sc[3].warning(f"{sim_date:%a %d %b} was not a trading day"
                          + (f" — try {nxt:%a %d %b}." if nxt else "."))
        day = sim_date
        now = datetime.combine(sim_date, sim_time, tzinfo=cal.ET)
        state = cal.session_state(now)
    else:
        day = live_now.date()
        now = live_now
        state = cal.session_state(now)

    top = st.columns([1.4, 1, 1.4, 2])
    if sim_on:
        top[0].markdown(f"**SIMULATED {now:%a %d %b %H:%M} ET**")
    else:
        top[0].markdown(f"**{now:%H:%M:%S} ET**")
    top[1].markdown(f"session: **{state}**")
    refresh = top[2].button("Refresh data", use_container_width=True)
    top[3].caption(DISCLAIMER)

    if sim_on:
        st.markdown(
            f"<div class='dt-banner dt-warn'><b>Simulation.</b> Replaying "
            f"{day:%a %d %b %Y} as at {sim_time:%H:%M} ET. Daily indicators use only "
            f"sessions before that date and intraday bars are cut at the simulated "
            f"clock, so no future data leaks in. Option chains are fetched "
            f"<i>live</i> and will not match that day's quotes.</div>",
            unsafe_allow_html=True,
        )

    # Gate toggles surfaced rather than buried (spec §3).
    g1, g2 = st.columns(2)
    block_earn = g1.checkbox(
        "Block on earnings inside the option's life", value=cfg.block_on_earnings_in_window,
        help="Holding a 3-5 DTE call through an earnings print is a different bet — "
             "IV crush can take the position out even when the direction is right.")
    market_gate = g2.checkbox(
        "Require market (SPY/QQQ) gate", value=cfg.require_market_gate,
        help="Off by default: index context is shown but does not block a signal.")
    if block_earn != cfg.block_on_earnings_in_window or market_gate != cfg.require_market_gate:
        cfg.block_on_earnings_in_window = block_earn
        cfg.require_market_gate = market_gate
        save_config(profile, cfg)

    app_cfg_full = load_merged_config(profile)
    app_cfg_full['active_profile'] = profile
    # Logs to the same app.log the CC/CSP pipeline writes to (options_agent
    # logger, keyed by name so setup_logging just returns the existing handle)
    # rather than the silent _NullLogger fallback — otherwise nothing about a
    # DayTrading email send or failure is ever recorded anywhere to check later.
    _log = logger or setup_logging(app_cfg_full)

    # Live evaluation, polling and email alerts run in a background thread
    # (agent.daytrading.scheduler), independent of any browser tab — see that
    # module's docstring for why. This tab reads its most recent completed
    # cycle rather than running a second, duplicate one. Simulation mode is
    # the exception: it's a self-contained historical replay that must never
    # touch the live runtime's poll/email state, so it keeps its own inline
    # evaluation loop below and reuses only the runtime's DayTradingData
    # instance (to avoid a second warm-up fetch of the same day's data).
    runtime = get_runtime(profile, logger=_log)
    dd: DayTradingData = runtime.dd
    if refresh:
        dd.breaker.reset()
        if not sim_on:
            # Runs one cycle right here, synchronously, instead of waiting up
            # to 30s for the background thread's next tick — a click on
            # "Refresh data" should fetch now, not just show whatever the
            # last tick happened to leave. cycle_lock (blocking=False) means
            # if the background thread is mid-cycle at this exact moment, the
            # click skips rather than double-running a fetch concurrently.
            with st.spinner("Refreshing…"):
                ran = run_live_cycle(runtime, cfg, app_cfg_full, _log, blocking=False)
            if not ran:
                st.toast("A background cycle was already in progress — showing its result.")

    last_mail = last_email_sent(dd.cache_dir, profile)
    if last_mail:
        sent_at = datetime.fromisoformat(last_mail["sent_at"])
        age = live_now - sent_at
        age_str = (f"{age.days}d ago" if age.days >= 1
                   else f"{age.seconds // 3600}h ago" if age.seconds >= 3600
                   else f"{max(age.seconds // 60, 1)}m ago")
        st.caption(f"Last email: **{last_mail['kind']}** &middot; "
                  f"{sent_at:%a %d %b %H:%M} ET ({age_str})", unsafe_allow_html=True)
    else:
        st.caption("Last email: none sent yet")

    tickers = sorted(set(cfg.active_watchlist()) | set(MARKET_CONTEXT_TICKERS))

    if sim_on:
        if refresh:
            dd.clear_static()
        with st.spinner("Loading market data…"):
            # Warm-up is always keyed to the real date — the 60-day intraday
            # frame it caches is what the simulated session replays from.
            wu = dd.warm_up(tickers, day=live_now.date(), force=refresh)
            phase = poll_phase(cfg, day, now)
            halts, halt_ts = [], None  # the live halt feed describes *now*, not the replayed day

        evals: Dict[str, TickerEvaluation] = {}
        for t in sorted(set(cfg.watchlist) | set(MARKET_CONTEXT_TICKERS)):
            td = wu.data.get(t)
            if td is not None:
                td = td.as_of(day, now)  # cut off future data — see TickerData.as_of
            evals[t] = evaluate_ticker(
                t, td, cfg, day, now,
                today_bars=None, halted=False,
                already_fired=None,  # a replay recomputes from scratch every render
                static=None,
            )

        sim_fired: Dict[str, sig.TriggerFire] = {}  # isolated from the live runtime's fired dict
        poll_rows = poll_status(evals, phase, MARKET_CONTEXT_TICKERS, sim_fired, day, cfg)
        criteria_rows = build_criteria_rows(evals, poll_rows, cfg)
        to_poll: List[str] = []
    else:
        with runtime.lock:
            evals = runtime.evals
            poll_rows = runtime.poll_rows
            criteria_rows = runtime.criteria_rows
            phase = runtime.phase
            to_poll = runtime.to_poll
            halts = runtime.halts
            halt_ts = runtime.halt_ts
            last_cycle_at = runtime.last_cycle_at
            last_error = runtime.last_error

        if last_cycle_at is None:
            st.info("Waiting for the background scheduler's first cycle to "
                    "complete (up to 30s after the server started)…")
        else:
            age_s = (live_now - last_cycle_at).total_seconds()
            st.caption(f"Live as of {last_cycle_at:%H:%M:%S} ET ({age_s:.0f}s ago) "
                      "— refreshed automatically by the server every 30s.")
        if last_error:
            st.warning(f"Background scheduler's last cycle raised an error: {last_error}")
        if not evals:
            _render_watchlist(cfg, profile)
            return

    _render_watchlist(cfg, profile)
    st.divider()
    _render_polling(poll_rows, phase, cfg, day, now)
    st.divider()
    _render_criteria(criteria_rows)
    st.divider()
    _render_premarket(evals, cfg, day, now)
    st.divider()
    _render_monitor(evals, cfg, day, now)

    signalled = [e for e in evals.values() if e.trigger and e.trigger.fired]
    if signalled:
        st.divider()
        st.markdown("#### Signals")
        for ev in signalled:
            fire = ev.trigger.fire
            key = f"{day.isoformat()}:{ev.ticker}:{fire.bar_time:%H%M}"
            if sim_on:
                # Chain lookups are cached per (ticker, firing bar) so re-rendering
                # the page does not re-hit the options API for a signal already
                # resolved. Isolated from the live runtime's contract cache.
                chain_cache: Dict[str, ct.SelectionResult] = st.session_state.setdefault(
                    "dt_sim_contracts", {})
                if day < live_now.date():
                    # No provider serves historical option chains, so that day's
                    # strikes and greeks cannot be reconstructed. Say so plainly
                    # rather than letting the DTE filter report "no expiration in
                    # the window", which reads like a market condition.
                    chain_cache[key] = ct.SelectionResult(degraded_reason=(
                        f"contract selection is unavailable for past sessions - neither "
                        f"yfinance nor Public.com serves historical option chains, so the "
                        f"{cfg.dte_min}-{cfg.dte_max} DTE strikes that existed on "
                        f"{day:%d %b} cannot be rebuilt (they have since expired). "
                        f"The signal, levels and exits below come from historical bars "
                        f"and are accurate"
                    ))
                elif key not in chain_cache:
                    with st.spinner(f"Looking up {ev.ticker} contracts…"):
                        cands, err = dd.fetch_call_candidates(
                            ev.ticker, day, fire.bar_close, app_cfg_full,
                            dte_min=cfg.dte_min, dte_max=cfg.dte_max,
                            underlying_quote_time=fire.bar_time,
                        )
                        chain_cache[key] = (
                            ct.SelectionResult(degraded_reason=err) if err
                            else ct.select_contract(
                                cands,
                                dte_min=cfg.dte_min, dte_max=cfg.dte_max,
                                delta_min=cfg.delta_min, delta_max=cfg.delta_max,
                                delta_target=cfg.delta_target,
                                max_spread_pct_of_mid=cfg.max_spread_pct_of_mid,
                                min_open_interest=cfg.min_open_interest,
                            )
                        )
                sel = chain_cache[key]
            else:
                # Resolved by the background scheduler's cycle that produced this
                # signal; a brand-new fire not yet resolved shows as pending until
                # the next cycle picks it up.
                sel = runtime.contracts.get(key, ct.SelectionResult(
                    degraded_reason="contract lookup pending - the background "
                                    "scheduler resolves this on its next cycle"))
            _render_signal_card(ev, cfg, sel)

    st.divider()
    _render_health(dd, evals, day, halts, halt_ts, (phase, to_poll, cfg))
