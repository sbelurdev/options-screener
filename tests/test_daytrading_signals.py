"""Signal, contract-selection, and sizing tests for DayTrading (spec §9)."""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from agent.daytrading import contracts as ct
from agent.daytrading import indicators as ind
from agent.daytrading import signals as sig
from agent.daytrading import sizing as sz
from agent.daytrading.indicators import DailyIndicators, OpeningRange, OvernightLevels

ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 28)


def _di(**over):
    base = dict(
        as_of=date(2026, 8, 27), close=110.0, rsi_14=60.0, macd_line=1.0,
        macd_signal=0.5, macd_hist=0.5, trend_ma=100.0, trend_ma_label="SMA20",
        atr_14=2.0, avg_volume_20=1_000_000.0, trend_ma_distance_pct=0.10,
    )
    base.update(over)
    return DailyIndicators(**base)


def _bars(day, closes, volumes, start="09:30"):
    t0 = pd.Timestamp(f"{day} {start}", tz=ET)
    idx = pd.DatetimeIndex([t0 + timedelta(minutes=5 * i) for i in range(len(closes))])
    c = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {"Open": c, "High": c + 0.05, "Low": c - 0.05, "Close": c,
         "Volume": np.asarray(volumes, dtype=float)},
        index=idx,
    )


# ── Stage A: daily gate ──────────────────────────────────────────────────────

def test_daily_gate_passes_and_reports_all_conditions():
    r = sig.evaluate_daily_gate(_di())
    assert r.passed and not r.degraded
    assert {c.name for c in r.conditions} >= {"close_above_sma20"}
    # label is derived from trend_ma_label, e.g. SMA20 -> close_above_sma20
    assert "macd_above_signal" not in {c.name for c in r.conditions}, (
        "MACD was removed as a gate condition; it is context only"
    )
    assert "rsi_14" not in {c.name for c in r.conditions}, (
        "daily RSI was removed as a gate condition; momentum is now checked "
        "intraday in evaluate_trigger (rsi_momentum_5m_or_15m)"
    )


def test_daily_rsi_never_blocks_regardless_of_value():
    """Daily RSI is context-only now (see indicators.DailyIndicators.rsi_14
    still being computed and shown in the readiness panel) - it must never
    fail Stage A no matter how extreme."""
    for rsi in (0.0, 49.9, 80.1, 100.0):
        assert sig.evaluate_daily_gate(_di(rsi_14=rsi)).passed


def test_macd_below_signal_no_longer_blocks():
    """MACD was removed as a gate; a bearish MACD must not fail an otherwise good name."""
    r = sig.evaluate_daily_gate(_di(macd_line=0.1, macd_signal=0.9, macd_hist=-0.8))
    assert r.passed
    assert not any(c.name == "macd_above_signal" for c in r.conditions)


def test_missing_macd_does_not_degrade_the_gate():
    """MACD decides nothing, so its absence must not mark a ticker degraded."""
    r = sig.evaluate_daily_gate(_di(macd_line=None, macd_signal=None, macd_hist=None))
    assert not r.degraded
    assert r.passed


def test_missing_daily_rsi_does_not_degrade_the_gate():
    """Same reasoning as MACD - RSI decides nothing at Stage A now, so its
    absence must not mark a ticker degraded either."""
    r = sig.evaluate_daily_gate(_di(rsi_14=None))
    assert not r.degraded
    assert r.passed


def test_daily_gate_sma_failure():
    r = sig.evaluate_daily_gate(_di(close=90.0, trend_ma=100.0))
    assert not r.passed and any(c.name == "close_above_sma20" for c in r.failures())


def test_daily_gate_blocks_earnings_inside_option_life():
    r = sig.evaluate_daily_gate(
        _di(),
        earnings_date=date(2026, 8, 31), option_expiry=date(2026, 9, 2),
        block_on_earnings_in_window=True,
    )
    assert not r.passed
    assert any(c.name == "no_earnings_in_window" for c in r.failures())


def test_daily_gate_allows_earnings_after_expiry():
    r = sig.evaluate_daily_gate(
        _di(),
        earnings_date=date(2026, 9, 30), option_expiry=date(2026, 9, 2),
        block_on_earnings_in_window=True,
    )
    assert r.passed


def test_daily_gate_earnings_toggle_off_does_not_block():
    r = sig.evaluate_daily_gate(
        _di(),
        earnings_date=date(2026, 8, 31), option_expiry=date(2026, 9, 2),
        block_on_earnings_in_window=False,
    )
    assert r.passed


def test_daily_gate_missing_data_is_degraded_not_failed():
    """Degraded must be distinguishable from 'evaluated and did not qualify'."""
    r = sig.evaluate_daily_gate(None)
    assert r.degraded and not r.passed
    assert "no daily indicators" in r.degraded_reason

    r2 = sig.evaluate_daily_gate(_di(trend_ma=None))
    assert r2.degraded and "trend_ma" in r2.degraded_reason


# ── Stage B: setup gate ──────────────────────────────────────────────────────

def test_setup_gate_gap_up_passes_with_no_ceiling():
    r = sig.evaluate_setup_gate(today_open=150.0, prev_close=100.0)  # +50%
    assert r.passed
    assert r.gap_pct == pytest.approx(0.50)


def test_setup_gate_gap_down_fails():
    r = sig.evaluate_setup_gate(today_open=99.0, prev_close=100.0)
    assert not r.passed and r.gap_pct == pytest.approx(-0.01)


def test_setup_gate_flat_open_fails_strictly():
    assert not sig.evaluate_setup_gate(today_open=100.0, prev_close=100.0).passed


def test_setup_gate_missing_price_is_degraded():
    r = sig.evaluate_setup_gate(today_open=None, prev_close=100.0)
    assert r.degraded and not r.passed


# ── Stage C: trigger ─────────────────────────────────────────────────────────

def _passing_rsi(day, start="09:30"):
    """RSI(5m)/RSI(15m) series that read >60 by the time `day`'s bars begin -
    a monotonically rising close (no down-ticks) makes Wilder RSI 100 once
    warm (agent.daytrading.indicators.rsi's explicit zero-loss handling), so
    this is a clean, deterministic "momentum condition passes" fixture for
    tests whose actual focus is one of the other four conditions, not RSI
    itself (see test_trigger_momentum_condition_* for RSI's own behavior).
    Spans back well before `day` so the rolling 14-period warm-up (and the
    15-minute resample's own warm-up on top of that) has already completed
    by the first bar under test - a same-day-only series would still read
    NaN at 09:45, same as it would in production before this fixture existed.
    """
    t0 = pd.Timestamp(f"{day} {start}", tz=ET) - timedelta(minutes=5 * 40)
    n = 40 + 12  # 40 prior bars (warm-up) + enough to cover the test's own window
    idx = pd.DatetimeIndex([t0 + timedelta(minutes=5 * i) for i in range(n)])
    c = np.linspace(80.0, 120.0, n)  # strictly increasing -> RSI 100 once warm
    bars = pd.DataFrame(
        {"Open": c, "High": c + 0.05, "Low": c - 0.05, "Close": c, "Volume": np.full(n, 1000.0)},
        index=idx,
    )
    return ind.rsi(bars["Close"], 14), ind.intraday_rsi(bars, resample="15min")


def _trigger_setup(closes, volumes, *, or_high=100.0, or_avg_vol=1000.0, on_high=100.5):
    rth = _bars(DAY, closes, volumes, start="09:45")
    vwap = pd.Series(np.full(len(closes), 99.0), index=rth.index)
    orr = OpeningRange(or_high=or_high, or_low=98.0, or_height=2.0,
                       or_avg_volume=or_avg_vol, bar_count=3)
    on = OvernightLevels(on_high, 97.0, 100.0, "window", 100)
    return rth, vwap, orr, on


def test_trigger_fires_when_all_five_conditions_hold():
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    rsi5, rsi15 = _passing_rsi(DAY)
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                             rsi_5m=rsi5, rsi_15m=rsi15)
    assert r.fired
    assert r.fire.bar_close == pytest.approx(101.0)
    assert r.fire.bar_time.strftime("%H:%M") == "09:45"
    assert all(c.passed for c in r.fire.conditions)


@pytest.mark.parametrize("closes,volumes,failing", [
    ([99.5], [5000.0], "close_above_or_high"),       # below OR high
    ([100.2], [5000.0], "rsi_momentum_5m_or_15m"),   # above OR, but no RSI data supplied
    ([101.0], [500.0], "volume_above_or_avg"),        # volume too light
])
def test_trigger_does_not_fire_and_names_the_failing_condition(closes, volumes, failing):
    rth, vwap, orr, on = _trigger_setup(closes, volumes)
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY)
    assert not r.fired
    assert failing in {c.name for c in r.last_conditions if not c.passed}


def test_trigger_requires_close_above_vwap():
    rth = _bars(DAY, [101.0], [5000.0], start="09:45")
    vwap = pd.Series([102.0], index=rth.index)  # price below VWAP
    orr = OpeningRange(100.0, 98.0, 2.0, 1000.0, 3)
    on = OvernightLevels(100.5, 97.0, 100.0, "w", 10)
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY)
    assert not r.fired
    assert "close_above_vwap" in {c.name for c in r.last_conditions if not c.passed}


def test_mid_bar_evaluation_never_fires():
    """A bar labelled 09:45 covers 09:45-09:50 and must not decide before 09:50."""
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    rsi5, rsi15 = _passing_rsi(DAY)

    mid = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 47, tzinfo=ET),
                               opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                               rsi_5m=rsi5, rsi_15m=rsi15)
    assert not mid.fired and mid.bars_evaluated == 0

    at_close = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 50, tzinfo=ET),
                                    opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                                    rsi_5m=rsi5, rsi_15m=rsi15)
    assert at_close.fired, "the same bar must fire once complete"


def test_completed_bars_boundary_is_inclusive_at_bar_end():
    rth = _bars(DAY, [1.0, 2.0], [1.0, 1.0], start="09:45")
    assert len(sig.completed_bars(rth, datetime(2026, 8, 28, 9, 49, tzinfo=ET))) == 0
    assert len(sig.completed_bars(rth, datetime(2026, 8, 28, 9, 50, tzinfo=ET))) == 1
    assert len(sig.completed_bars(rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET))) == 2


def test_trigger_fires_at_most_once_per_ticker_per_day():
    rth, vwap, orr, on = _trigger_setup([101.0, 102.0, 103.0], [5000.0] * 3)
    rsi5, rsi15 = _passing_rsi(DAY)
    first = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 10, 5, tzinfo=ET),
                                 opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                                 rsi_5m=rsi5, rsi_15m=rsi15)
    assert first.fired and first.fire.bar_time.strftime("%H:%M") == "09:45"

    # Re-evaluating later with the earlier fire passed back returns it frozen
    again = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 10, 30, tzinfo=ET),
                                 opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                                 already_fired=first.fire)
    assert again.fired
    assert again.fire is first.fire
    assert again.fire.bar_time == first.fire.bar_time


def test_trigger_ignores_bars_before_start_and_after_cutoff():
    # 09:30 bar qualifies numerically but precedes trigger_start
    rth = _bars(DAY, [101.0], [5000.0], start="09:30")
    vwap = pd.Series([99.0], index=rth.index)
    orr = OpeningRange(100.0, 98.0, 2.0, 1000.0, 3)
    on = OvernightLevels(100.5, 97.0, 100.0, "w", 10)
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 10, 0, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY)
    assert not r.fired and r.bars_evaluated == 0

    # A qualifying bar after the 11:00 cutoff is likewise ignored
    late = _bars(DAY, [101.0], [5000.0], start="11:05")
    vwap_l = pd.Series([99.0], index=late.index)
    r2 = sig.evaluate_trigger("AAPL", late, datetime(2026, 8, 28, 11, 30, tzinfo=ET),
                              opening_range=orr, overnight=on, vwap=vwap_l, day=DAY)
    assert not r2.fired and r2.cutoff_passed


def test_trigger_degrades_rather_than_silently_not_firing():
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    now = datetime(2026, 8, 28, 9, 55, tzinfo=ET)

    no_or = sig.evaluate_trigger("AAPL", rth, now, opening_range=None, overnight=on,
                                 vwap=vwap, day=DAY)
    assert no_or.degraded and not no_or.fired

    no_bars = sig.evaluate_trigger("AAPL", pd.DataFrame(), now, opening_range=orr,
                                   overnight=on, vwap=vwap, day=DAY)
    assert no_bars.degraded


def test_trigger_without_overnight_level_does_not_block():
    """Overnight-high is no longer a trigger condition at all (it moved to an
    informational-only field computed in views.py), so a missing overnight
    level cannot block or even appear in the fired trigger's conditions."""
    rth, vwap, orr, _ = _trigger_setup([101.0], [5000.0])
    none_on = OvernightLevels(None, None, None, "no extended-hours bars", 0)
    rsi5, rsi15 = _passing_rsi(DAY)
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                             opening_range=orr, overnight=none_on, vwap=vwap, day=DAY,
                             rsi_5m=rsi5, rsi_15m=rsi15)
    assert r.fired
    assert all(c.name != "close_above_overnight_high" for c in r.fire.conditions)


def test_trigger_rejects_naive_now():
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    with pytest.raises(ValueError, match="timezone-aware"):
        sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY)


# ── Stage C: intraday RSI momentum condition ─────────────────────────────────

def _rsi_at(day, value_5m, value_15m, bar_label="09:45"):
    """RSI(5m)/RSI(15m) series carrying exactly the given reading at the one
    bar these tests evaluate — bypasses indicators.rsi's own math entirely
    (that's covered separately) so this isolates evaluate_trigger's own
    OR/threshold logic. The 15m series is left-labelled 15 minutes earlier
    than `bar_label`, matching the bin that completes exactly at that bar
    (e.g. the 09:30 bin covers [09:30, 09:45) and completes at 09:45)."""
    ts = pd.Timestamp(f"{day} {bar_label}", tz=ET)
    rsi5 = pd.Series([value_5m], index=[ts])
    rsi15 = pd.Series([value_15m], index=[ts - timedelta(minutes=15)])
    return rsi5, rsi15


def test_signal_does_not_fire_without_rsi_data():
    """No rsi_5m/rsi_15m supplied at all - the new condition must not pass
    vacuously; a signal needs real momentum confirmation, not an assumption."""
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY)
    assert not r.fired
    assert "rsi_momentum_5m_or_15m" in {c.name for c in r.last_conditions if not c.passed}


def test_signal_fires_on_rsi_5m_alone():
    """RSI(15m) below threshold, RSI(5m) above - OR means either is enough."""
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    rsi5, rsi15_low = _rsi_at(DAY, 75.0, 40.0)
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                             rsi_5m=rsi5, rsi_15m=rsi15_low)
    assert r.fired


def test_signal_fires_on_rsi_15m_alone():
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    rsi5_low, rsi15 = _rsi_at(DAY, 40.0, 75.0)
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                             rsi_5m=rsi5_low, rsi_15m=rsi15)
    assert r.fired


def test_signal_does_not_fire_when_both_rsi_below_threshold():
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    rsi5, rsi15 = _rsi_at(DAY, 55.0, 45.0)
    r = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                             rsi_5m=rsi5, rsi_15m=rsi15)
    assert not r.fired
    assert "rsi_momentum_5m_or_15m" in {c.name for c in r.last_conditions if not c.passed}


def test_rsi_threshold_is_configurable():
    """The default is 60, but the caller (DayTradingConfig.intraday_rsi_threshold)
    can move it - a reading of 65 must pass a threshold of 60 but fail 70."""
    rth, vwap, orr, on = _trigger_setup([101.0], [5000.0])
    rsi5, rsi15 = _rsi_at(DAY, 65.0, 65.0)
    lenient = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                                   opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                                   rsi_5m=rsi5, rsi_15m=rsi15, rsi_threshold=60.0)
    assert lenient.fired

    strict = sig.evaluate_trigger("AAPL", rth, datetime(2026, 8, 28, 9, 55, tzinfo=ET),
                                  opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                                  rsi_5m=rsi5, rsi_15m=rsi15, rsi_threshold=70.0)
    assert not strict.fired


def test_15m_rsi_reads_a_still_forming_bin_as_unavailable_not_stale():
    """The first 15-minute bin (09:30-09:45) is not complete until 09:45 - at
    09:45 itself (the first possible trigger bar), last_completed_value must
    find it (labelled 09:30, completes exactly at 09:45), not read ahead into
    a bin that hasn't finished accumulating."""
    from agent.daytrading import indicators as ind_mod
    t0 = pd.Timestamp(f"{DAY} 09:30", tz=ET)
    idx = pd.DatetimeIndex([t0 + timedelta(minutes=5 * i) for i in range(3)])  # 09:30,09:35,09:40
    series = pd.Series([70.0], index=[t0])  # one 15m bin, labelled 09:30
    # Exactly at 09:45 the 09:30 bin (covering [09:30, 09:45)) has just completed.
    assert ind_mod.last_completed_value(series, t0 + timedelta(minutes=15), timedelta(minutes=15)) == 70.0
    # One minute earlier it has not.
    assert ind_mod.last_completed_value(series, t0 + timedelta(minutes=14), timedelta(minutes=15)) is None


# ── Golden-file test (spec §9): recorded session -> OR, VWAP, trigger bar ────

def test_golden_session_reproduces_or_vwap_and_trigger_bar():
    """A committed synthetic session with a known, hand-computed outcome.

    Shape: opens inside the range, breaks out on the 10:00 bar with volume.
    """
    closes = [
        # 09:30  09:35  09:40  (opening range)
        100.0, 100.4, 100.2,
        # 09:45  09:50  09:55  (chop below OR high, light volume)
        100.1, 100.3, 100.35,
        # 10:00  <- breakout bar
        101.20,
        # 10:05  10:10
        101.4, 101.1,
    ]
    volumes = [2000.0, 1800.0, 1600.0, 900.0, 950.0, 800.0, 5200.0, 3000.0, 2500.0]
    bars = _bars(DAY, closes, volumes, start="09:30")

    orr = ind.opening_range(bars)
    assert orr.bar_count == 3
    assert orr.or_high == pytest.approx(100.45)   # max High = 100.4 + 0.05
    assert orr.or_low == pytest.approx(99.95)     # min Low  = 100.0 - 0.05
    assert orr.or_avg_volume == pytest.approx(1800.0)  # mean(2000,1800,1600)

    vwap = ind.session_vwap(bars)
    assert len(vwap) == len(bars)
    assert vwap.iloc[0] == pytest.approx(100.0)   # anchored at the 09:30 bar

    on = OvernightLevels(overnight_high=100.5, overnight_low=99.0,
                         premarket_last=100.1, covered_window="golden", bar_count=100)

    rsi5, rsi15 = _passing_rsi(DAY)
    r = sig.evaluate_trigger("GOLD", bars, datetime(2026, 8, 28, 10, 30, tzinfo=ET),
                             opening_range=orr, overnight=on, vwap=vwap, day=DAY,
                             rsi_5m=rsi5, rsi_15m=rsi15)

    assert r.fired, "the 10:00 bar should trigger"
    assert r.fire.bar_time.strftime("%H:%M") == "10:00"
    assert r.fire.bar_close == pytest.approx(101.20)
    assert r.fire.bar_volume == pytest.approx(5200.0)
    # And it clears every gate it was checked against
    assert r.fire.bar_close > orr.or_high
    assert r.fire.bar_close > on.overnight_high
    assert r.fire.bar_close > vwap.loc[r.fire.bar_time]
    assert r.fire.bar_volume > orr.or_avg_volume


# ── §7.1 contract selection ─────────────────────────────────────────────────

def _chain(rows):
    return pd.DataFrame(rows)


def test_contract_selection_ranks_by_delta_distance_then_spread():
    chain = _chain([
        {"contractSymbol": "A", "strike": 105.0, "bid": 1.00, "ask": 1.02,
         "delta": 0.62, "theta": -0.05, "impliedVolatility": 0.30,
         "openInterest": 1000, "volume": 50},
        {"contractSymbol": "B", "strike": 104.0, "bid": 1.00, "ask": 1.02,
         "delta": 0.65, "theta": -0.05, "impliedVolatility": 0.30,
         "openInterest": 1000, "volume": 50},
    ])
    cands = ct.build_candidates(chain, date(2026, 9, 2), DAY, 100.0)
    res = ct.select_contract(cands, dte_min=3, dte_max=5, delta_min=0.60, delta_max=0.70,
                             delta_target=0.65, max_spread_pct_of_mid=0.05,
                             min_open_interest=500)
    assert res.found
    assert res.contract.symbol == "B", "closest to target delta wins"


@pytest.mark.parametrize("bad,expect_filter", [
    ({"delta": 0.20}, "delta"),
    ({"openInterest": 10}, "open_interest"),
    ({"bid": 1.00, "ask": 1.40}, "spread"),
])
def test_no_qualifying_contract_reports_filter_and_near_miss(bad, expect_filter):
    row = {"contractSymbol": "X", "strike": 105.0, "bid": 1.00, "ask": 1.02,
           "delta": 0.65, "theta": -0.05, "impliedVolatility": 0.30,
           "openInterest": 1000, "volume": 50}
    row.update(bad)
    cands = ct.build_candidates(_chain([row]), date(2026, 9, 2), DAY, 100.0)
    res = ct.select_contract(cands, dte_min=3, dte_max=5, delta_min=0.60, delta_max=0.70,
                             delta_target=0.65, max_spread_pct_of_mid=0.05,
                             min_open_interest=500)
    assert not res.found
    assert res.eliminated_by[expect_filter] == 1
    assert res.near_miss is not None
    assert expect_filter in res.explain()
    assert res.explain() != "no contract found"


def test_dte_filter_eliminates_out_of_window_expiries():
    chain = _chain([{"contractSymbol": "A", "strike": 105.0, "bid": 1.0, "ask": 1.02,
                     "delta": 0.65, "impliedVolatility": 0.3, "openInterest": 1000,
                     "volume": 10}])
    cands = ct.build_candidates(chain, date(2026, 9, 25), DAY, 100.0)  # 28 DTE
    res = ct.select_contract(cands, dte_min=3, dte_max=5, delta_min=0.60, delta_max=0.70,
                             delta_target=0.65, max_spread_pct_of_mid=0.05,
                             min_open_interest=500)
    assert not res.found and res.eliminated_by["dte"] == 1


def test_delta_falls_back_to_black_scholes_when_provider_omits_it():
    chain = _chain([{"contractSymbol": "A", "strike": 100.0, "bid": 1.0, "ask": 1.02,
                     "impliedVolatility": 0.30, "openInterest": 1000, "volume": 10}])
    cands = ct.build_candidates(chain, date(2026, 9, 2), DAY, 100.0)
    assert cands[0].delta is not None
    assert cands[0].delta_source == "black-scholes"
    assert 0.4 < cands[0].delta < 0.7  # ATM call
    assert cands[0].theta is not None and cands[0].theta < 0


def test_empty_chain_is_reported_not_crashed():
    res = ct.select_contract([], dte_min=3, dte_max=5, delta_min=0.6, delta_max=0.7,
                             delta_target=0.65, max_spread_pct_of_mid=0.05,
                             min_open_interest=500)
    assert not res.found and res.considered == 0
    assert "no call contracts" in res.explain()


# ── §7.2 sizing and exits ───────────────────────────────────────────────────

def test_sizing_shows_every_intermediate_value():
    r = sz.compute_size(entry_price=101.0, or_high=100.0, vwap_at_entry=99.0,
                        delta=0.65, account_size=50_000.0, risk_pct_per_trade=0.005)
    assert r.stop_level == pytest.approx(100.0)
    assert r.stop_basis == "or_high"
    assert r.stop_distance == pytest.approx(1.0)
    assert r.risk_dollars == pytest.approx(250.0)
    assert r.risk_per_contract == pytest.approx(0.65 * 1.0 * 100)
    assert r.contracts == 3  # floor(250 / 65)
    assert r.sizeable


def test_sizing_stop_uses_vwap_when_it_is_higher():
    r = sz.compute_size(101.0, or_high=99.0, vwap_at_entry=100.5, delta=0.65,
                        account_size=50_000.0, risk_pct_per_trade=0.005)
    assert r.stop_level == pytest.approx(100.5)
    assert r.stop_basis == "vwap"


def test_sizing_with_zero_account_blocks_and_explains():
    r = sz.compute_size(101.0, 100.0, 99.0, 0.65, account_size=0.0, risk_pct_per_trade=0.005)
    assert not r.sizeable and r.contracts == 0
    assert "account size" in r.blocked_reason
    # The formula inputs are still exposed so the UI can show the derivation
    assert r.stop_distance == pytest.approx(1.0)
    assert r.risk_per_contract == pytest.approx(65.0)


def test_sizing_blocks_when_entry_not_above_stop():
    r = sz.compute_size(99.0, or_high=100.0, vwap_at_entry=99.5, delta=0.65,
                        account_size=50_000.0, risk_pct_per_trade=0.005)
    assert not r.sizeable and "not above the stop" in r.blocked_reason


def test_sizing_blocks_when_budget_below_one_contract():
    r = sz.compute_size(101.0, 100.0, 99.0, 0.65, account_size=1_000.0,
                        risk_pct_per_trade=0.005)  # $5 budget vs $65 risk
    assert not r.sizeable and "below the cost of one" in r.blocked_reason


def test_exit_levels_measured_move_and_1r():
    e = sz.compute_exits(entry_price=101.0, entry_time=datetime(2026, 8, 28, 10, 0, tzinfo=ET),
                         or_high=100.0, or_height=2.0, vwap_at_entry=99.0)
    assert e.stop_level == pytest.approx(100.0)
    assert e.target_1 == pytest.approx(103.0)     # entry + or_height
    assert e.target_1r == pytest.approx(102.0)    # entry + stop_distance
    assert e.time_stop.strftime("%H:%M") == "10:45"
    assert e.hard_close.strftime("%H:%M") == "15:30"
    assert not e.is_half_day


def test_exit_levels_on_half_day_move_hard_close_in():
    e = sz.compute_exits(101.0, datetime(2026, 11, 27, 10, 0, tzinfo=ET),
                         or_high=100.0, or_height=2.0, vwap_at_entry=99.0)
    assert e.is_half_day
    assert e.hard_close.strftime("%H:%M") == "12:30"


def test_exits_reject_naive_entry_time():
    with pytest.raises(ValueError, match="timezone-aware"):
        sz.compute_exits(101.0, datetime(2026, 8, 28, 10, 0), 100.0, 2.0, 99.0)


def test_selection_reason_explains_why_the_contract_won():
    rows = [{"contractSymbol": f"C{k}", "strike": 120.0 + k, "bid": 1.00, "ask": 1.02,
             "delta": 0.60 + k * 0.02, "theta": -0.05, "impliedVolatility": 0.30,
             "openInterest": 1000, "volume": 40} for k in range(5)]
    cands = ct.build_candidates(_chain(rows), date(2026, 9, 2), DAY, 122.0)
    res = ct.select_contract(cands, dte_min=3, dte_max=5, delta_min=0.60, delta_max=0.70,
                             delta_target=0.65, max_spread_pct_of_mid=0.05,
                             min_open_interest=500)
    assert res.found
    assert res.qualified == 5
    assert res.runner_up is not None
    r = res.selection_reason
    assert "closest to target 0.65" in r
    assert "spread" in r and "OI" in r and "DTE" in r
    assert "chosen over 4 other qualifying contracts" in r


def test_selection_reason_notes_a_sole_qualifying_contract():
    rows = [{"contractSymbol": "A", "strike": 105.0, "bid": 1.0, "ask": 1.02,
             "delta": 0.65, "impliedVolatility": 0.3, "openInterest": 1000, "volume": 5}]
    cands = ct.build_candidates(_chain(rows), date(2026, 9, 2), DAY, 100.0)
    res = ct.select_contract(cands, dte_min=3, dte_max=5, delta_min=0.60, delta_max=0.70,
                             delta_target=0.65, max_spread_pct_of_mid=0.05,
                             min_open_interest=500)
    assert res.qualified == 1 and res.runner_up is None
    assert "only qualifying contract" in res.selection_reason
