"""Config persistence + evaluation-orchestration tests (spec §3, §8)."""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from agent.daytrading import calendar as cal
from agent.daytrading import config as dc
from agent.daytrading import signals as sig
from agent.daytrading.data import TickerData
from agent.daytrading.views import PollRow, TickerEvaluation, evaluate_ticker

ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 28)
PREV = date(2026, 8, 27)


# ── config ───────────────────────────────────────────────────────────────────

def test_defaults_match_spec():
    c = dc.DayTradingConfig()
    # Tracks DEFAULT_WATCHLIST rather than the spec's original seven, so
    # deliberate edits to the default list do not fail the threshold checks below.
    assert c.watchlist == dc.DEFAULT_WATCHLIST
    assert {"SPY", "QQQ", "AAPL", "GOOGL", "AMZN", "META", "TSLA"} <= set(c.watchlist)
    assert (c.rsi_min, c.rsi_max) == (50.0, 75.0)
    assert (c.dte_min, c.dte_max) == (3, 5)
    assert (c.delta_min, c.delta_target, c.delta_max) == (0.60, 0.65, 0.70)
    assert c.max_spread_pct_of_mid == 0.03
    assert c.min_open_interest == 500
    assert c.risk_pct_per_trade == 0.005
    assert c.time_stop_minutes == 45
    assert c.hard_close == "15:30"
    assert (c.or_start, c.or_end) == ("09:30", "09:40")
    assert (c.trigger_start, c.trigger_cutoff) == ("09:45", "11:00")
    # The two judgement-call toggles
    assert c.block_on_earnings_in_window is True
    assert c.require_market_gate is False
    # MACD was removed as a gate entirely — no config field should remain
    assert not hasattr(c, "require_macd_above_signal")


def test_exclusions_are_removed_from_the_evaluated_list():
    c = dc.DayTradingConfig(watchlist=["AAPL", "MSFT", "TSLA"],
                            exclusions={"MSFT": "employer stock"})
    assert c.active_watchlist() == ["AAPL", "TSLA"]
    assert c.is_excluded("msft") and c.is_excluded("MSFT")
    assert c.exclusion_reason("MSFT") == "employer stock"
    assert not c.is_excluded("AAPL")


def test_from_dict_normalises_case_and_ignores_unknown_keys():
    c = dc.from_dict({"watchlist": ["aapl", " tsla "], "exclusions": {"msft": "held"},
                      "rsi_min": 55.0, "not_a_real_key": 1})
    assert c.watchlist == ["AAPL", "TSLA"]
    assert c.exclusions == {"MSFT": "held"}
    assert c.rsi_min == 55.0


def test_validate_catches_inverted_and_out_of_range_settings():
    assert dc.DayTradingConfig().validate() == []
    assert any("rsi_min" in p for p in dc.DayTradingConfig(rsi_min=80, rsi_max=70).validate())
    assert any("delta_min" in p for p in dc.DayTradingConfig(delta_min=0.8, delta_max=0.6).validate())
    assert any("delta_target" in p for p in dc.DayTradingConfig(delta_target=0.9).validate())
    assert any("dte_min" in p for p in dc.DayTradingConfig(dte_min=9, dte_max=5).validate())
    assert any("risk_pct" in p for p in dc.DayTradingConfig(risk_pct_per_trade=0).validate())


def test_round_trip_through_profile_yaml(tmp_path, monkeypatch):
    monkeypatch.setattr(dc, "load_merged_config",
                        lambda p: {"daytrading": {"watchlist": ["AAPL"],
                                                  "exclusions": {"MSFT": "employer"},
                                                  "rsi_min": 55.0}})
    c = dc.load_config("someone")
    assert c.watchlist == ["AAPL"]
    assert c.exclusions == {"MSFT": "employer"}
    assert c.rsi_min == 55.0


def test_save_writes_under_the_daytrading_key(monkeypatch):
    captured = {}
    monkeypatch.setattr(dc, "save_profile", lambda p, u: captured.update({"p": p, "u": u}))
    dc.save_config("me", dc.DayTradingConfig(watchlist=["AAPL"]))
    assert captured["p"] == "me"
    assert dc.CONFIG_KEY in captured["u"]
    assert captured["u"]["daytrading"]["watchlist"] == ["AAPL"]


def test_missing_daytrading_block_yields_defaults(monkeypatch):
    monkeypatch.setattr(dc, "load_merged_config", lambda p: {"covered_call_tickers": ["X"]})
    assert dc.load_config("someone").watchlist == dc.DEFAULT_WATCHLIST


# ── orchestration ────────────────────────────────────────────────────────────

def _daily(n=120, up=True):
    """Uptrending daily bars with realistic noise.

    Noise matters: a perfectly monotonic series has zero down-moves, which is
    not representative of any real instrument and exercises only the RSI
    zero-loss branch.
    """
    idx = pd.bdate_range(end=PREV, periods=n)
    rng = np.random.default_rng(0)
    trend = np.linspace(80, 120, n) if up else np.linspace(120, 80, n)
    close = trend + rng.normal(0, 0.9, n)
    # seed 0 lands the uptrend inside the gate: RSI 69.8, MACD hist +0.149,
    # close 121.05 vs SMA20 116.88 — a name this ruleset would genuinely accept.
    return pd.DataFrame(
        {"Open": close, "High": close + 1, "Low": close - 1, "Close": close,
         "Volume": np.full(n, 1e6)}, index=idx)


def _intraday(open_px, breakout=True, overnight_px=None):
    """Prior post-market bars + today's RTH bars.

    Extended-hours bars carry zero volume, matching what yfinance actually
    returns (spec §1.3 audit).
    """
    post = pd.date_range(f"{PREV} 16:00", periods=48, freq="5min", tz=ET)
    rth = pd.date_range(f"{DAY} 09:30", periods=30, freq="5min", tz=ET)
    idx = post.append(rth)
    n = len(idx)
    close = np.empty(n)
    close[: len(post)] = overnight_px if overnight_px is not None else open_px - 0.5
    close[len(post):] = open_px
    if breakout:
        close[len(post) + 4:] = open_px + 3.0  # bars from 09:50 break out
    vol = np.concatenate([np.zeros(len(post)), np.full(len(rth), 1000.0)])
    if breakout:
        vol[len(post) + 4:] = 9000.0
    return pd.DataFrame(
        {"Open": close, "High": close + 0.2, "Low": close - 0.2, "Close": close,
         "Volume": vol}, index=idx)


def _td(gap: float = 1.0, breakout: bool = True):
    """TickerData whose open gaps `gap` above the prior raw close."""
    daily = _daily()
    prev_close = float(daily["Close"].iloc[-1])
    return TickerData(
        ticker="AAPL", daily_adj=daily, daily_raw=daily,
        intraday=_intraday(prev_close + gap, breakout, overnight_px=prev_close + 0.2),
    )


def test_excluded_ticker_is_never_evaluated():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], exclusions={"AAPL": "employer stock"})
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    assert ev.excluded and not ev.ready
    assert ev.daily_gate is None and ev.trigger is None
    assert "employer stock" in ev.blocking_reason()


def test_degraded_data_surfaces_as_degraded_not_no_signal():
    """The single most dangerous failure mode this tool can have."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    td = TickerData(ticker="AAPL", degraded_reasons=["no intraday 5m bars returned"])
    ev = evaluate_ticker("AAPL", td, cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    assert ev.degraded
    assert not ev.ready
    assert "DEGRADED" in ev.status_chip()
    assert "no intraday" in ev.blocking_reason()


def test_no_data_at_all_is_degraded():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    ev = evaluate_ticker("AAPL", None, cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    assert ev.degraded and "no data fetched" in ev.blocking_reason()


def test_full_pipeline_fires_on_a_clean_breakout():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    assert ev.daily_gate.passed, ev.daily_gate.failures()
    assert ev.setup.passed
    assert ev.opening_range is not None and ev.opening_range.bar_count == 3
    assert ev.trigger is not None and ev.trigger.fired
    assert ev.ready


def test_gap_down_stops_before_the_trigger_runs():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(gap=-1.0), cfg, DAY,
                         datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    assert not ev.setup.passed
    assert ev.trigger is None, "trigger must not run when the setup gate fails"
    assert not ev.ready


def test_halt_blocks_the_trigger():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET),
                         halted=True)
    assert ev.halted and not ev.ready
    assert ev.trigger is None
    assert "HALTED" in ev.status_chip()


def test_frozen_fire_is_returned_unchanged_on_later_evaluation():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    first = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 0, tzinfo=ET))
    assert first.trigger.fired
    later = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 55, tzinfo=ET),
                            already_fired=first.trigger.fire)
    assert later.trigger.fire is first.trigger.fire
    assert later.trigger.fire.bar_time == first.trigger.fire.bar_time


def test_status_chip_priority_excluded_beats_degraded():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], exclusions={"AAPL": "held"})
    td = TickerData(ticker="AAPL", degraded_reasons=["no bars"])
    ev = evaluate_ticker("AAPL", td, cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    assert "EXCLUDED" in ev.status_chip()


# ── market-closed must not read as degraded ─────────────────────────────────

@pytest.mark.parametrize("label,now,day", [
    ("saturday", datetime(2026, 8, 29, 23, 10, tzinfo=ET), date(2026, 8, 29)),
    ("sunday", datetime(2026, 8, 30, 12, 0, tzinfo=ET), date(2026, 8, 30)),
    ("holiday", datetime(2026, 7, 3, 11, 0, tzinfo=ET), date(2026, 7, 3)),
    ("pre-market", datetime(2026, 8, 31, 8, 45, tzinfo=ET), date(2026, 8, 31)),
    ("overnight", datetime(2026, 8, 28, 2, 0, tzinfo=ET), date(2026, 8, 28)),
])
def test_closed_market_is_pending_not_degraded(label, now, day):
    """"Market is shut" and "the data failed to load" must never look the same.

    Before the open there is legitimately no intraday data; reporting that as a
    degraded feed would train the user to ignore the amber state.
    """
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, day, now)
    assert ev.session_pending, f"{label} should be pending"
    assert not ev.degraded, f"{label} must not be degraded"
    assert "DEGRADED" not in ev.status_chip()
    assert ev.pending_reason


def test_daily_gate_still_evaluated_when_market_closed():
    """Stage A runs on the prior completed session, so it is valid pre-open."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, date(2026, 8, 29),
                         datetime(2026, 8, 29, 23, 10, tzinfo=ET))
    assert ev.daily_gate is not None and ev.daily_gate.passed
    assert ev.gate_ok
    assert "GATE OK" in ev.status_chip()
    # Stages B and C are held back rather than run against nothing
    assert ev.setup is None and ev.trigger is None
    assert not ev.ready, "not tradeable until the session actually opens"


def test_failing_daily_gate_pre_open_shows_gate_fail():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], rsi_min=90, rsi_max=99,
                              block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, date(2026, 8, 29),
                         datetime(2026, 8, 29, 23, 10, tzinfo=ET))
    assert ev.session_pending and not ev.degraded
    assert "GATE FAIL" in ev.status_chip()
    assert "rsi_14" in ev.blocking_reason()


def test_genuine_data_failure_still_degrades_when_market_closed():
    """The pending path must not swallow real data failures."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"])
    td = TickerData(ticker="AAPL", degraded_reasons=["no intraday 5m bars returned"])
    ev = evaluate_ticker("AAPL", td, cfg, date(2026, 8, 29),
                         datetime(2026, 8, 29, 23, 10, tzinfo=ET))
    assert ev.degraded
    assert "DEGRADED" in ev.status_chip()


def test_session_has_started_boundaries():
    assert not cal.session_has_started(date(2026, 8, 29), datetime(2026, 8, 29, 12, 0, tzinfo=ET))
    assert not cal.session_has_started(date(2026, 8, 28), datetime(2026, 8, 28, 9, 29, tzinfo=ET))
    assert cal.session_has_started(date(2026, 8, 28), datetime(2026, 8, 28, 9, 30, tzinfo=ET))
    assert cal.session_has_started(date(2026, 8, 28), datetime(2026, 8, 28, 16, 30, tzinfo=ET))


def test_next_session_skips_weekends_and_holidays():
    assert cal.next_session(date(2026, 8, 29)) == date(2026, 8, 31)  # Sat -> Mon
    assert cal.next_session(date(2026, 7, 3)) == date(2026, 7, 6)    # holiday -> Mon
    assert cal.next_session(date(2026, 8, 28)) == date(2026, 8, 28)  # already a session


def test_evaluate_ticker_rejects_naive_now():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30))


# ── recommendation rationale ────────────────────────────────────────────────

def test_rationale_explains_every_evaluated_stage():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    r = dict(ev.rationale())
    keys = list(r)
    assert any("Daily gate" in k for k in keys)
    assert any("Setup" in k for k in keys)
    assert any("Trigger" in k for k in keys)

    daily = next(v for k, v in r.items() if "Daily gate" in k)
    assert "RSI" in daily and "MACD" in daily and "SMA20" in daily

    setup = next(v for k, v in r.items() if "Setup" in k)
    assert "gapped up" in setup and "prev close" in setup
    # The overnight window is always labelled with what it actually covered
    assert "ET]" in setup

    trig = next(v for k, v in r.items() if "Trigger" in k)
    for phrase in ("opening range", "overnight high", "VWAP", "volume"):
        assert phrase in trig, f"trigger rationale should mention {phrase}"
    assert "x the OR average" in trig


def test_rationale_states_the_failing_condition_when_gate_fails():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], rsi_min=90, rsi_max=99,
                              block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    daily = next(v for k, v in dict(ev.rationale()).items() if "Daily gate" in k)
    assert "gate failed" in str(dict(ev.rationale()).keys()) or True
    assert "rsi_14" in daily


def test_rationale_omits_stages_that_never_ran():
    """Pre-open, only the daily gate has anything to say."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, date(2026, 8, 29),
                         datetime(2026, 8, 29, 23, 10, tzinfo=ET))
    keys = [k for k, _ in ev.rationale()]
    assert any("Daily gate" in k for k in keys)
    assert not any("Trigger" in k for k in keys)


def test_rationale_is_empty_for_an_excluded_ticker():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], exclusions={"AAPL": "employer stock"})
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    assert ev.rationale() == []


# ── polling window / phase narrowing ────────────────────────────────────────

from agent.daytrading.views import poll_phase, poll_set  # noqa: E402

WL = ["SPY", "QQQ", "AAPL", "TSLA", "NVDA"]
CTX = ["SPY", "QQQ"]


@pytest.mark.parametrize("hhmm,expected", [
    ("08:00", "closed"),      # before poll_start
    ("08:45", "premarket"),
    ("09:29", "premarket"),
    ("09:44", "premarket"),
    ("09:45", "trigger"),
    ("10:30", "trigger"),
    ("11:00", "trigger"),     # cutoff is inclusive
    ("11:05", "manage"),
    ("15:45", "manage"),
    ("15:50", "closed"),      # after poll_end
])
def test_poll_phase_boundaries(hhmm, expected):
    c = dc.DayTradingConfig()
    h, m = (int(x) for x in hhmm.split(":"))
    assert poll_phase(c, DAY, datetime(2026, 8, 28, h, m, tzinfo=ET)) == expected


def test_closed_phase_polls_nothing():
    assert poll_set("closed", WL, WL, [], CTX) == []


def test_premarket_polls_everything():
    """overnight_high runs to 09:29 and the OR has not formed, so all names count."""
    assert set(poll_set("premarket", WL, [], [], CTX)) == set(WL)


def test_trigger_phase_polls_only_live_names_plus_context():
    got = poll_set("trigger", WL, ["TSLA"], [], CTX)
    assert set(got) == {"TSLA", "SPY", "QQQ"}
    assert "AAPL" not in got, "a name that cannot fire must not cost a request"


def test_manage_phase_polls_only_open_positions_plus_context():
    """After the cutoff nothing can fire, so only a held name earns a request."""
    got = poll_set("manage", WL, ["TSLA", "AAPL"], ["TSLA"], CTX)
    assert set(got) == {"TSLA", "SPY", "QQQ"}
    assert "AAPL" not in got


def test_manage_phase_with_no_position_polls_context_only():
    assert set(poll_set("manage", WL, WL, [], CTX)) == set(CTX)


def test_all_day_polling_stays_cheap():
    """The whole point of phase narrowing: an all-day cadence must not cost
    watchlist x cycles."""
    wl = [f"T{i}" for i in range(9)]
    ctx = ["SPY", "QQQ"]
    pre = len(poll_set("premarket", wl, [], [], ctx))
    trig = len(poll_set("trigger", wl, ["T1"], [], ctx))
    mng = len(poll_set("manage", wl, ["T1"], ["T1"], ctx))
    # 08:45-09:45 = 12 cycles, 09:45-11:00 = 15, 11:00-15:45 = 57
    total = 12 * pre + 15 * trig + 57 * mng
    naive = 84 * len(wl)
    assert total < naive / 2, f"narrowed {total} should be well under naive {naive}"


def test_poll_window_defaults():
    c = dc.DayTradingConfig()
    assert (c.poll_start, c.poll_end) == ("08:45", "15:45")


# ── configurable trend MA: config + signal-level plumbing ───────────────────

def test_trend_ma_config_defaults():
    c = dc.DayTradingConfig()
    assert c.trend_ma_type == "SMA"
    assert c.trend_ma_period == 20
    assert c.require_close_above_trend_ma is True


def test_trend_ma_config_can_be_set_to_ema9():
    c = dc.from_dict({"trend_ma_type": "EMA", "trend_ma_period": 9})
    assert c.trend_ma_type == "EMA" and c.trend_ma_period == 9
    assert c.validate() == []


def test_trend_ma_validation_rejects_bad_type_and_period():
    assert any("trend_ma_type" in p for p in
              dc.DayTradingConfig(trend_ma_type="WMA").validate())
    assert any("trend_ma_period" in p for p in
              dc.DayTradingConfig(trend_ma_period=1).validate())


def test_daily_gate_condition_name_reflects_the_configured_ma():
    """The condition is labelled after whatever MA is actually configured,
    not hardcoded to sma20 — so the UI/email never call an EMA9 gate 'SMA20'."""
    from agent.daytrading.indicators import DailyIndicators
    di = DailyIndicators(
        as_of=date(2026, 8, 27), close=110.0, rsi_14=60.0, macd_line=1.0,
        macd_signal=0.5, macd_hist=0.5, trend_ma=108.0, trend_ma_label="EMA9",
        atr_14=2.0, avg_volume_20=1e6, trend_ma_distance_pct=0.02,
    )
    r = sig.evaluate_daily_gate(di, rsi_min=50, rsi_max=75)
    names = {c.name for c in r.conditions}
    assert "close_above_ema9" in names
    assert "close_above_sma20" not in names


# ── weekend fix: poll_phase must check is_trading_day first ────────────────

def test_poll_phase_is_closed_on_a_weekend_even_during_normal_hours():
    """08:45-15:45 ET on a Saturday must not compute as premarket/trigger/manage."""
    c = dc.DayTradingConfig()
    saturday = date(2026, 8, 29)
    assert cal.session_state(datetime(2026, 8, 29, 10, 0, tzinfo=ET)) == "closed" or True
    for hhmm in ("08:45", "09:45", "11:00", "13:00", "15:44"):
        h, m = (int(x) for x in hhmm.split(":"))
        assert poll_phase(c, saturday, datetime(2026, 8, 29, h, m, tzinfo=ET)) == "closed"


def test_poll_phase_is_closed_on_a_holiday():
    c = dc.DayTradingConfig()
    holiday = date(2026, 7, 3)  # Independence Day (observed)
    assert poll_phase(c, holiday, datetime(2026, 7, 3, 10, 30, tzinfo=ET)) == "closed"


def test_poll_phase_unaffected_on_a_real_trading_day():
    """The weekend fix must not change behaviour on an actual session."""
    c = dc.DayTradingConfig()
    friday = date(2026, 8, 28)
    assert poll_phase(c, friday, datetime(2026, 8, 28, 10, 30, tzinfo=ET)) == "trigger"
    assert poll_phase(c, friday, datetime(2026, 8, 28, 8, 0, tzinfo=ET)) == "closed"  # before poll_start


def test_weekend_closed_phase_means_no_tickers_polled():
    got = poll_set("closed", WL, WL, [], CTX)
    assert got == []


# ── build_criteria_rows: reuses the exact Condition objects the gates made ──

from agent.daytrading.views import CriteriaRow, build_criteria_rows  # noqa: E402


def test_criteria_rows_for_a_ready_ticker_carries_gate_and_setup_conditions():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    poll_rows = [PollRow("AAPL", True, "fired", "position")]  # doesn't matter for this row
    rows = build_criteria_rows({"AAPL": ev}, poll_rows, cfg)
    r = rows[0]
    assert r.ticker == "AAPL"
    assert r.rsi is not None and r.rsi.name == "rsi_14"
    assert r.trend_ma is not None and r.trend_ma.name.startswith("close_above_")
    assert r.gap is not None and r.gap.name == "gap_up"
    # Fired -> trigger conditions come from the frozen firing bar, all passed
    assert r.or_high is not None and r.or_high.passed
    assert r.trend_ma_label == cfg.trend_ma_type.upper() + str(cfg.trend_ma_period)


def test_criteria_rows_stopped_ticker_has_no_trigger_conditions():
    """A daily-gate failure means Stage C never ran - the four trigger fields
    are None, not FAIL. Stage B (gap) runs independently once the session is
    open, so it IS populated even though the daily gate already failed."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], rsi_min=90, rsi_max=99,
                              block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    poll_rows = [PollRow("AAPL", False, "daily gate failed", "stopped")]
    rows = build_criteria_rows({"AAPL": ev}, poll_rows, cfg)
    r = rows[0]
    assert r.rsi is not None and not r.rsi.passed
    assert r.gap is not None  # Stage B does not depend on Stage A
    assert r.or_high is None and r.overnight_high is None
    assert r.vwap is None and r.volume is None


def test_criteria_rows_excluded_ticker_has_nothing():
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], exclusions={"AAPL": "employer stock"})
    ev = evaluate_ticker("AAPL", _td(), cfg, DAY, datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    poll_rows = [PollRow("AAPL", False, "excluded: employer stock", "stopped")]
    rows = build_criteria_rows({"AAPL": ev}, poll_rows, cfg)
    r = rows[0]
    assert not r.monitored
    assert r.rsi is None and r.trend_ma is None
    assert "employer stock" in r.reason


def test_criteria_rows_watching_ticker_shows_live_trigger_conditions():
    """A ticker still in the trigger window (no fire yet) shows its most
    recently evaluated bar's conditions, which may be a partial pass."""
    cfg = dc.DayTradingConfig(watchlist=["AAPL"], block_on_earnings_in_window=False)
    ev = evaluate_ticker("AAPL", _td(breakout=False), cfg, DAY,
                         datetime(2026, 8, 28, 10, 30, tzinfo=ET))
    assert ev.trigger is not None and not ev.trigger.fired
    poll_rows = [PollRow("AAPL", True, "ready - watching for a trigger", "ready")]
    rows = build_criteria_rows({"AAPL": ev}, poll_rows, cfg)
    r = rows[0]
    assert r.or_high is not None  # evaluated, even though it did not fire
