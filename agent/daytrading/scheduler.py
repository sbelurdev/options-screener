"""
Server-side background scheduler for the DayTrading tab's live pipeline —
warm-up, polling, evaluation, contract resolution and email alerts — run
independent of any browser tab, mirroring the CC/CSP scheduler in app.py.

Before this module existed, all of that only happened when render_daytrading_tab
executed as part of a browser session's script rerun, and nothing drove that
rerun on a fixed cadence — the tab depended on a live, open, ticking browser
session even more than the old CC/CSP scheduler did (see app.py's
_run_scheduler_loop for that history).

One DayTradingRuntime per profile is now the single source of truth for the
live (non-simulation) state. The background thread owns it; the Streamlit tab
just reads it to render. Simulation mode is unaffected — it's a self-contained
historical replay that never touches this runtime's email/poll state, though
it reuses the runtime's DayTradingData instance to avoid a second warm-up
fetch of the same day's data.
"""

from __future__ import annotations

import threading
import time as _time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from agent.daytrading import calendar as cal
from agent.daytrading import contracts as ct
from agent.daytrading import sizing as sz
from agent.daytrading.config import MARKET_CONTEXT_TICKERS, load_config
from agent.daytrading.data import DayTradingData
from agent.daytrading.notify import (
    maybe_send_polling_stopped_email,
    maybe_send_signal_email,
    maybe_send_warmup_email,
    record_email_sent,
)
from agent.utils.config_io import load_merged_config
from agent.utils.logging import setup_logging

_CYCLE_SECONDS = 30


@dataclass
class DayTradingRuntime:
    """Live state the background thread owns and the tab (non-sim mode) reads.

    No st.session_state — this must be safe to touch from a thread with no
    Streamlit ScriptRunContext at all. `lock` guards the read/render side
    against reading a half-updated cycle; the writer (run_live_cycle) holds
    it only for the final in-place swap, not for the network calls before it.
    `cycle_lock` serializes the background thread's own tick against a
    manual "Refresh data" click running an out-of-band cycle (see
    run_live_cycle), so the two can never race on fired/mail_state/etc.
    """
    profile: str
    dd: DayTradingData
    fired: Dict[str, Any] = field(default_factory=dict)          # "{day}:{ticker}" -> TriggerFire
    active_poll: List[str] = field(default_factory=list)
    mail_state: Dict[str, Any] = field(default_factory=dict)
    emailed_keys: set = field(default_factory=set)
    contracts: Dict[str, Any] = field(default_factory=dict)      # "{day}:{ticker}:{HHMM}" -> SelectionResult
    evals: Dict[str, Any] = field(default_factory=dict)
    poll_rows: list = field(default_factory=list)
    criteria_rows: list = field(default_factory=list)
    phase: str = "closed"
    to_poll: List[str] = field(default_factory=list)
    halts: List[str] = field(default_factory=list)
    halt_ts: Optional[datetime] = None
    last_cycle_at: Optional[datetime] = None
    last_error: Optional[str] = None
    lock: threading.Lock = field(default_factory=threading.Lock)
    cycle_lock: threading.Lock = field(default_factory=threading.Lock)


_runtimes: Dict[str, DayTradingRuntime] = {}
_runtimes_lock = threading.Lock()


def get_runtime(profile: str, logger=None) -> DayTradingRuntime:
    """The shared runtime for `profile`, created on first access — safe to
    call both from the Streamlit tab (to read) and the background thread
    (to own), whichever gets there first."""
    with _runtimes_lock:
        rt = _runtimes.get(profile)
        if rt is None:
            rt = DayTradingRuntime(profile=profile, dd=DayTradingData(logger=logger))
            _runtimes[profile] = rt
        return rt


def run_live_cycle(rt: DayTradingRuntime, cfg, app_cfg_full: dict, log,
                   *, blocking: bool = True) -> bool:
    """One full evaluation + email pass for the current live moment.

    Mutates `rt` in place; no Streamlit calls, so this is callable from any
    thread — the background loop's own tick, or a "Refresh data" click
    running an out-of-band cycle synchronously so the click actually fetches
    fresh data instead of showing whatever the last background tick left (up
    to 30s stale). `cycle_lock` serializes the two so they never race on
    `fired`/`mail_state`/etc; `blocking=False` (used by a manual refresh) skips
    the cycle instead of waiting if the background thread already holds it,
    returning False, rather than blocking the interactive click on a fetch
    already in flight.

    Returns True if a cycle actually ran, False if `blocking=False` and the
    lock was already held.
    """
    acquired = rt.cycle_lock.acquire(blocking=blocking)
    if not acquired:
        return False
    try:
        return _run_live_cycle_locked(rt, cfg, app_cfg_full, log)
    finally:
        rt.cycle_lock.release()


def _run_live_cycle_locked(rt: DayTradingRuntime, cfg, app_cfg_full: dict, log) -> bool:
    from agent.daytrading.views import (
        build_criteria_rows, evaluate_ticker, poll_phase, poll_set, poll_status,
    )

    dd = rt.dd
    now = cal.now_et()
    day = now.date()
    tickers = sorted(set(cfg.active_watchlist()) | set(MARKET_CONTEXT_TICKERS))

    phase = poll_phase(cfg, day, now)
    if phase == "closed":
        # Nothing to evaluate or email outside [poll_start, poll_end] — a
        # trading-day calendar rollover at midnight ET must not be mistaken
        # for the session starting. Without this guard the thread ticks
        # right through midnight, sees a new day whose warm-up hasn't been
        # sent yet, and fires a warm-up email hours before the market opens
        # (caught in testing: a real 00:00 ET send for the next session).
        # last_cycle_at still advances so "is this thread alive" stays
        # honest; evals/poll_rows/criteria_rows are left as the prior
        # session's final read rather than a premature, misleading one.
        with rt.lock:
            rt.phase = phase
            rt.last_cycle_at = now
        return True

    wu = dd.warm_up(tickers, day=day)
    fired_tickers_today = [k.split(":")[1] for k in rt.fired if k.startswith(day.isoformat())]
    to_poll = poll_set(phase, tickers, rt.active_poll or tickers, fired_tickers_today,
                       MARKET_CONTEXT_TICKERS)
    live = dd.poll_all(to_poll) if to_poll else {}
    halts, halt_ts = dd.halted_symbols(tickers)

    evals: Dict[str, Any] = {}
    for t in sorted(set(cfg.watchlist) | set(MARKET_CONTEXT_TICKERS)):
        td = wu.data.get(t)
        static = None if td is None else dd.static_context(
            t, td, day, cfg.trend_ma_type, cfg.trend_ma_period)
        evals[t] = evaluate_ticker(
            t, td, cfg, day, now,
            today_bars=live.get(t),
            halted=t in halts,
            already_fired=rt.fired.get(f"{day.isoformat()}:{t}"),
            static=static,
        )
        tr = evals[t].trigger
        if tr and tr.fired:
            rt.fired.setdefault(f"{day.isoformat()}:{t}", tr.fire)

    active_poll = sorted(
        t for t, e in evals.items()
        if (e.ready or e.degraded or e.session_pending)
        and not (e.trigger and e.trigger.fired)
    )

    poll_rows = poll_status(evals, phase, MARKET_CONTEXT_TICKERS, rt.fired, day, cfg)
    criteria_rows = build_criteria_rows(evals, poll_rows, cfg)

    signalled = [e for e in evals.values() if e.trigger and e.trigger.fired]
    fired_today: List[tuple] = []
    for ev in signalled:
        fire = ev.trigger.fire
        key = f"{day.isoformat()}:{ev.ticker}:{fire.bar_time:%H%M}"
        if key not in rt.contracts:
            cands, err = dd.fetch_call_candidates(
                ev.ticker, day, fire.bar_close, app_cfg_full,
                dte_min=cfg.dte_min, dte_max=cfg.dte_max,
                underlying_quote_time=fire.bar_time,
            )
            rt.contracts[key] = (
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
        sel = rt.contracts[key]
        vwap_at = (float(ev.vwap.loc[fire.bar_time])
                   if ev.vwap is not None and fire.bar_time in ev.vwap.index
                   else fire.vwap_at_bar)
        entry = sel.contract.underlying_price if sel.found else fire.bar_close
        ev._email_size = (
            sz.compute_size(entry, fire.or_high, vwap_at, sel.contract.delta,
                            cfg.account_size, cfg.risk_pct_per_trade)
            if sel.found else None
        )
        ev._email_exits = sz.compute_exits(
            entry, fire.bar_time, fire.or_high,
            ev.opening_range.or_height if ev.opening_range else 0.0, vwap_at,
            time_stop_minutes=cfg.time_stop_minutes,
            configured_hard_close=cfg.hard_close,
        )
        fired_today.append((ev, sel))

    alert_to = cfg.alert_recipients(app_cfg_full.get("notify_email"))
    if maybe_send_warmup_email(app_cfg_full, evals, criteria_rows, fired_today, day,
                               rt.profile, rt.mail_state, log, dd.last_warmup,
                               recipients=alert_to, cache_dir=dd.cache_dir):
        record_email_sent(dd.cache_dir, rt.profile, "Warm-up", day)
    if maybe_send_polling_stopped_email(app_cfg_full, poll_rows, criteria_rows,
                                        fired_today, day, rt.profile, rt.mail_state, log,
                                        recipients=alert_to):
        record_email_sent(dd.cache_dir, rt.profile, "Polling-stopped", day)
    for ev, sel in fired_today:
        if maybe_send_signal_email(app_cfg_full, ev, cfg, sel, criteria_rows,
                                   fired_today, day, rt.profile, rt.emailed_keys, log,
                                   recipients=alert_to):
            record_email_sent(dd.cache_dir, rt.profile, f"Signal ({ev.ticker})", day)

    with rt.lock:
        rt.active_poll = active_poll
        rt.evals = evals
        rt.poll_rows = poll_rows
        rt.criteria_rows = criteria_rows
        rt.phase = phase
        rt.to_poll = to_poll
        rt.halts = halts
        rt.halt_ts = halt_ts
        rt.last_cycle_at = now
    return True


def _scheduler_loop(profile: str) -> None:
    cfg_full = load_merged_config(profile)
    log = setup_logging(cfg_full)
    rt = get_runtime(profile, logger=log)
    log.info(f"[daytrading-scheduler] started for profile '{profile}'")
    while True:
        try:
            cfg = load_config(profile)
            app_cfg_full = load_merged_config(profile)
            app_cfg_full["active_profile"] = profile
            run_live_cycle(rt, cfg, app_cfg_full, log)
            rt.last_error = None
        except Exception as exc:  # noqa: BLE001
            # A bug in one cycle must never kill the loop - the whole point of
            # this thread is to keep running without anyone watching it.
            rt.last_error = str(exc)
            log.exception(f"[daytrading-scheduler] cycle failed: {exc}")
        _time.sleep(_CYCLE_SECONDS)


def start_scheduler_thread(profile: str) -> threading.Thread:
    t = threading.Thread(target=_scheduler_loop, args=(profile,),
                        name="daytrading-scheduler", daemon=True)
    t.start()
    return t
