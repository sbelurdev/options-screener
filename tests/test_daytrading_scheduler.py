"""
Integration test for agent.daytrading.scheduler.run_live_cycle: exercises the
full wiring (config -> DayTradingData -> evaluate_ticker -> poll_status ->
build_criteria_rows -> email dedup) end-to-end with the network and email
boundaries mocked out, so it's safe to run without hitting real market data
APIs or sending real mail.

This is the regression test for the background-scheduler refactor that moved
the DayTrading tab's live warm-up/poll/email pipeline out of the Streamlit
tab (browser-dependent) into agent.daytrading.scheduler (a server-side
thread, independent of any browser tab) — see that module's docstring.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from agent.daytrading import config as dc
from agent.daytrading import data as dd_mod
from agent.daytrading import notify as nt
from agent.daytrading import scheduler as sched

ET = ZoneInfo("America/New_York")
# A fixed Friday, 10:00 ET — inside the default 08:45-15:45 poll window, so
# these tests don't depend on when they happen to run (a real run past
# midnight ET hit exactly this: poll_phase said "closed" for the un-mocked
# real clock, and run_live_cycle's closed-phase guard correctly no-opped).
_TRADING_DAY = datetime(2026, 8, 28, 10, 0, tzinfo=ET)


@pytest.fixture
def logger():
    class _L:
        def _noop(self, *a, **k):
            return None
        info = warning = error = exception = debug = _noop
    return _L()


@pytest.fixture
def app_cfg(tmp_path):
    return {
        "active_profile": "p",
        "notify_email": "me@example.com",
        "email": {"enabled": True, "smtp_host": "smtp.example.com", "smtp_port": 587,
                  "from_address": "bot@example.com",
                  "smtp_user_env_var": "SMTP_USER", "smtp_password_env_var": "SMTP_PASSWORD"},
    }


@pytest.fixture
def runtime(tmp_path, logger):
    dd = dd_mod.DayTradingData(cache_dir=str(tmp_path / "cache"), logger=logger)
    return sched.DayTradingRuntime(profile="p", dd=dd)


@pytest.fixture
def no_network(monkeypatch):
    """Every DayTradingData method run_live_cycle can call is patched to a
    no-op that touches no network — an empty warm-up (data={}) means every
    ticker evaluates with td=None, i.e. degraded, with nothing to fetch
    contracts for and no signal that could fire."""
    monkeypatch.setattr(sched.cal, "now_et", lambda: _TRADING_DAY)
    empty_warmup = dd_mod.WarmupResult(as_of=date(2026, 8, 28),
                                       fetched_at=None, data={})
    monkeypatch.setattr(dd_mod.DayTradingData, "warm_up", lambda self, *a, **k: empty_warmup)
    monkeypatch.setattr(dd_mod.DayTradingData, "poll_all", lambda self, *a, **k: {})
    monkeypatch.setattr(dd_mod.DayTradingData, "halted_symbols", lambda self, *a, **k: ([], None))
    monkeypatch.setattr(dd_mod.DayTradingData, "static_context", lambda self, *a, **k: None)
    monkeypatch.setattr(dd_mod.DayTradingData, "fetch_call_candidates",
                        lambda self, *a, **k: ([], "no network in this test"))


@pytest.fixture
def sent(monkeypatch):
    box = []
    monkeypatch.setattr(
        nt, "send_html_email",
        lambda cfg, s, h, t, log, rec=None: (box.append((s, h, t, rec)), True)[1])
    return box


def test_run_live_cycle_completes_and_populates_the_runtime(runtime, app_cfg, logger,
                                                             no_network, sent):
    """Wiring smoke test: every degraded ticker, no live data, no crash."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    sched.run_live_cycle(runtime, cfg, app_cfg, logger)

    assert runtime.last_cycle_at is not None
    assert runtime.last_error is None
    assert "AAPL" in runtime.evals
    assert runtime.evals["AAPL"].degraded_reasons  # td=None -> degraded, not silently no-signal
    assert runtime.criteria_rows
    assert runtime.poll_rows


def test_run_live_cycle_sends_exactly_one_warmup_email(runtime, app_cfg, logger,
                                                        no_network, sent):
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    sched.run_live_cycle(runtime, cfg, app_cfg, logger)
    warmups = [s for s, h, t, r in sent if "warm-up" in s.lower()]
    assert len(warmups) == 1


def test_second_cycle_same_day_does_not_resend_warmup(runtime, app_cfg, logger,
                                                       no_network, sent):
    """The in-memory mail_state dedup this thread's own loop relies on."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    sched.run_live_cycle(runtime, cfg, app_cfg, logger)
    sched.run_live_cycle(runtime, cfg, app_cfg, logger)
    warmups = [s for s, h, t, r in sent if "warm-up" in s.lower()]
    assert len(warmups) == 1


def test_a_fresh_runtime_does_not_resend_a_warmup_already_recorded_on_disk(
    tmp_path, app_cfg, logger, no_network, sent
):
    """Cross-process dedup: a second DayTradingRuntime (as a second AppTest/
    server process would create) must see the persisted record from the
    first and not resend — this is the exact bug found and fixed while
    testing the live app (see test_daytrading_notify.py's regression test
    for the underlying record_email_sent fix)."""
    dd = dd_mod.DayTradingData(cache_dir=str(tmp_path / "cache"), logger=logger)
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])

    rt1 = sched.DayTradingRuntime(profile="p", dd=dd)
    sched.run_live_cycle(rt1, cfg, app_cfg, logger)
    assert len([s for s, h, t, r in sent if "warm-up" in s.lower()]) == 1

    rt2 = sched.DayTradingRuntime(profile="p", dd=dd)  # fresh in-memory state, same disk cache_dir
    sched.run_live_cycle(rt2, cfg, app_cfg, logger)
    assert len([s for s, h, t, r in sent if "warm-up" in s.lower()]) == 1


def test_get_runtime_returns_the_same_instance_for_the_same_profile():
    a = sched.get_runtime("some-unique-test-profile")
    b = sched.get_runtime("some-unique-test-profile")
    assert a is b


def test_manual_refresh_runs_a_cycle_immediately(runtime, app_cfg, logger, no_network, sent):
    """A 'Refresh data' click calls run_live_cycle directly (blocking=False) -
    it must actually populate the runtime, not just be a no-op wrapper."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    assert runtime.last_cycle_at is None
    ran = sched.run_live_cycle(runtime, cfg, app_cfg, logger, blocking=False)
    assert ran is True
    assert runtime.last_cycle_at is not None


def test_non_blocking_refresh_skips_rather_than_waits_when_a_cycle_is_in_flight(
    runtime, app_cfg, logger, no_network, sent
):
    """Simulates the background thread already holding cycle_lock (e.g. its
    own tick in progress) when a manual refresh click comes in: the click
    must skip immediately (blocking=False), never block the interactive
    script waiting for the background cycle to finish."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    runtime.cycle_lock.acquire()
    try:
        ran = sched.run_live_cycle(runtime, cfg, app_cfg, logger, blocking=False)
        assert ran is False
        assert runtime.last_cycle_at is None  # nothing ran, nothing to show yet
    finally:
        runtime.cycle_lock.release()


def test_cycle_at_midnight_et_does_not_warm_up_or_email_for_the_new_day(
    runtime, app_cfg, logger, no_network, sent, monkeypatch
):
    """Regression: caught on the live server — a calendar rollover at
    00:00 ET made a new trading day's warm-up look "unsent today", and the
    thread fired a real warm-up + polling-stopped email hours before that
    day's market open. poll_phase correctly says "closed" at midnight; the
    bug was run_live_cycle not checking that before doing a full pass."""
    midnight_et = datetime(2026, 9, 1, 0, 2, tzinfo=ET)
    monkeypatch.setattr(sched.cal, "now_et", lambda: midnight_et)  # overrides no_network's daytime freeze
    warmup_calls = []
    monkeypatch.setattr(dd_mod.DayTradingData, "warm_up",
                        lambda self, *a, **k: (warmup_calls.append(1), None)[1])

    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    ran = sched.run_live_cycle(runtime, cfg, app_cfg, logger, blocking=False)

    assert ran is True  # the cycle "ran" (thread stays alive) but did nothing
    assert warmup_calls == [], "warm_up must not be called before the poll window opens"
    assert sent == [], "no email before the trading day's poll window has even started"
    assert runtime.phase == "closed"
    assert runtime.last_cycle_at == midnight_et
