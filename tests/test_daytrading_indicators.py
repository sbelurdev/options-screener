"""Indicator + calendar tests for the DayTrading package (spec §9)."""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from agent.daytrading import calendar as cal
from agent.daytrading import indicators as ind

ET = ZoneInfo("America/New_York")


# ── fixtures ─────────────────────────────────────────────────────────────────

def _daily(n=120, start=100.0, step=0.5, seed=7):
    rng = np.random.default_rng(seed)
    close = start + np.arange(n) * step + rng.normal(0, 0.8, n)
    idx = pd.bdate_range(end=date(2026, 8, 28), periods=n)
    return pd.DataFrame(
        {
            "Open": close - 0.2,
            "High": close + 1.0,
            "Low": close - 1.0,
            "Close": close,
            "Volume": rng.integers(1_000_000, 5_000_000, n).astype(float),
        },
        index=idx,
    )


def _intraday_day(day: date, n_bars=78, base=100.0, vol=500_000.0, start="09:30"):
    """Regular-session 5m bars, left-edge labelled from 09:30."""
    t0 = pd.Timestamp(f"{day} {start}", tz=ET)
    idx = pd.DatetimeIndex([t0 + timedelta(minutes=5 * i) for i in range(n_bars)])
    close = base + np.linspace(0, 2.0, n_bars)
    return pd.DataFrame(
        {
            "Open": close - 0.05,
            "High": close + 0.10,
            "Low": close - 0.10,
            "Close": close,
            "Volume": np.full(n_bars, vol),
        },
        index=idx,
    )


# ── RSI: Wilder vs rolling mean (spec §9, explicit anti-regression) ──────────

def test_wilder_rsi_differs_materially_from_rolling_mean():
    """A future refactor must not silently swap Wilder for a rolling mean."""
    close = _daily(n=120)["Close"]

    wilder = ind.rsi(close, 14).iloc[-1]

    d = close.diff()
    up = d.clip(lower=0).rolling(14).mean()
    dn = (-d.clip(upper=0)).rolling(14).mean()
    rolling = (100 - 100 / (1 + up / dn)).iloc[-1]

    assert not np.isclose(wilder, rolling, atol=0.5), (
        f"Wilder ({wilder:.2f}) and rolling-mean ({rolling:.2f}) RSI must differ; "
        "if these converged, the implementation likely regressed to a rolling mean"
    )


def test_rsi_is_wilder_by_construction():
    """RSI must equal an independent Wilder computation, not a rolling one."""
    close = _daily(n=80)["Close"]
    got = ind.rsi(close, 14)

    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    expected = 100 - 100 / (1 + up / dn)

    pd.testing.assert_series_equal(got.dropna(), expected.dropna())


def test_rsi_with_no_down_moves_is_100_not_nan():
    """An unbroken advance is exactly the momentum this strategy hunts for.

    Returning NaN here would make the strongest possible name read as
    "indicator unavailable" and drop it into a degraded state.
    """
    idx = pd.bdate_range(end=date(2026, 8, 28), periods=40)
    close = pd.Series(np.linspace(100, 140, 40), index=idx)  # strictly increasing
    r = ind.rsi(close, 14)
    assert r.iloc[-1] == pytest.approx(100.0)
    assert not np.isnan(r.iloc[-1])


def test_rsi_on_a_flat_series_is_50():
    idx = pd.bdate_range(end=date(2026, 8, 28), periods=40)
    flat = pd.Series(np.full(40, 100.0), index=idx)
    assert ind.rsi(flat, 14).iloc[-1] == pytest.approx(50.0)


def test_rsi_with_no_up_moves_is_0():
    idx = pd.bdate_range(end=date(2026, 8, 28), periods=40)
    close = pd.Series(np.linspace(140, 100, 40), index=idx)  # strictly decreasing
    assert ind.rsi(close, 14).iloc[-1] == pytest.approx(0.0)


def test_rsi_bounds_and_warmup():
    close = _daily(n=60)["Close"]
    r = ind.rsi(close, 14)
    assert r.iloc[:13].isna().all(), "first n-1 values must be NaN, not a warm-up number"
    valid = r.dropna()
    assert ((valid >= 0) & (valid <= 100)).all()


# ── MACD / ATR / SMA hand-checked ────────────────────────────────────────────

def test_macd_matches_manual_ema_difference():
    close = _daily(n=200)["Close"]
    line, sig, hist = ind.macd(close)

    e12 = close.ewm(span=12, adjust=False).mean()
    e26 = close.ewm(span=26, adjust=False).mean()
    expected_line = e12 - e26
    pd.testing.assert_series_equal(line, expected_line)
    pd.testing.assert_series_equal(sig, expected_line.ewm(span=9, adjust=False).mean())
    pd.testing.assert_series_equal(hist, line - sig)


def test_atr_on_hand_checked_fixture():
    """Constant 2.0 true range -> ATR converges to exactly 2.0."""
    n = 60
    idx = pd.bdate_range(end=date(2026, 8, 28), periods=n)
    close = pd.Series(np.full(n, 100.0), index=idx)
    high = close + 1.0
    low = close - 1.0  # H-L = 2.0, and prev close == close, so TR == 2.0
    a = ind.atr(high, low, close, 14)
    assert a.iloc[-1] == pytest.approx(2.0, abs=1e-9)


def test_sma_matches_rolling_mean():
    close = _daily(n=50)["Close"]
    pd.testing.assert_series_equal(ind.sma(close, 20), close.rolling(20).mean())


def test_daily_indicators_snapshot():
    df = _daily(n=120)
    di = ind.daily_indicators(df)
    assert di is not None
    assert di.as_of == df.index[-1].date()
    assert di.close == pytest.approx(float(df["Close"].iloc[-1]))
    assert di.trend_ma_distance_pct == pytest.approx((di.close - di.trend_ma) / di.trend_ma)
    assert di.trend_ma_label == "SMA20"
    # Uptrending fixture: MACD line above signal, close above the trend MA
    assert di.macd_hist > 0
    assert di.close > di.trend_ma


def test_daily_indicators_handles_empty():
    assert ind.daily_indicators(pd.DataFrame()) is None
    assert ind.daily_indicators(None) is None


def test_prev_close_uses_last_raw_value():
    df = _daily(n=30)
    assert ind.prev_close_raw(df) == pytest.approx(float(df["Close"].iloc[-1]))


# ── bar labelling convention (spec §6.2 — must be asserted) ─────────────────

def test_bar_labelling_is_left_edge_so_opening_range_is_three_bars():
    """A bar stamped 09:30 covers 09:30-09:35, so 09:30..09:40 is 3 bars.

    If yfinance ever switched to right-edge labels, the opening range would
    silently shift five minutes and nothing else would look wrong.
    """
    day = date(2026, 8, 28)
    rth = _intraday_day(day)
    assert rth.index[0].strftime("%H:%M") == "09:30"
    assert rth.index[-1].strftime("%H:%M") == "15:55", "last RTH 5m bar is 15:55"
    assert len(rth) == 78, "09:30-15:55 inclusive is 78 five-minute bars"

    orb = rth.between_time("09:30", "09:40")
    assert len(orb) == 3
    assert [t.strftime("%H:%M") for t in orb.index] == ["09:30", "09:35", "09:40"]


# ── opening range / VWAP / overnight / baseline ─────────────────────────────

def test_opening_range_values():
    day = date(2026, 8, 28)
    rth = _intraday_day(day)
    orr = ind.opening_range(rth)
    orb = rth.between_time("09:30", "09:40")
    assert orr.bar_count == 3
    assert orr.or_high == pytest.approx(float(orb["High"].max()))
    assert orr.or_low == pytest.approx(float(orb["Low"].min()))
    assert orr.or_height == pytest.approx(orr.or_high - orr.or_low)
    assert orr.or_avg_volume == pytest.approx(float(orb["Volume"].mean()))


def test_opening_range_none_when_no_bars():
    assert ind.opening_range(pd.DataFrame()) is None
    assert ind.opening_range(None) is None


def test_session_vwap_is_anchored_at_open_and_volume_weighted():
    day = date(2026, 8, 28)
    rth = _intraday_day(day)
    v = ind.session_vwap(rth)
    assert len(v) == len(rth)
    # First VWAP value equals the first bar's typical price (anchor at 09:30)
    first_tp = (rth["High"].iloc[0] + rth["Low"].iloc[0] + rth["Close"].iloc[0]) / 3
    assert v.iloc[0] == pytest.approx(first_tp)
    # Constant volume -> VWAP equals the running mean of typical price
    tp = (rth["High"] + rth["Low"] + rth["Close"]) / 3
    assert v.iloc[-1] == pytest.approx(tp.mean())


def test_overnight_levels_reports_covered_window_and_omits_volume():
    """Overnight window is partial by nature; the label must state what it covers."""
    prev, day = date(2026, 8, 27), date(2026, 8, 28)
    # post-market prior day 16:00-19:55, then pre-market 04:00-09:25 today
    post = pd.DatetimeIndex([pd.Timestamp(f"{prev} 16:00", tz=ET) + timedelta(minutes=5 * i)
                             for i in range(48)])
    pre = pd.DatetimeIndex([pd.Timestamp(f"{day} 04:00", tz=ET) + timedelta(minutes=5 * i)
                            for i in range(66)])
    idx = post.append(pre)
    n = len(idx)
    df = pd.DataFrame(
        {"Open": np.full(n, 100.0), "High": np.full(n, 101.0),
         "Low": np.full(n, 99.0), "Close": np.full(n, 100.5),
         "Volume": np.zeros(n)},  # yfinance reports 0 volume in extended hours
        index=idx,
    )
    on = ind.overnight_levels(df, prev, day)
    assert on.overnight_high == pytest.approx(101.0)
    assert on.overnight_low == pytest.approx(99.0)
    assert on.premarket_last == pytest.approx(100.5)
    assert on.bar_count == 114
    assert "16:00" in on.covered_window and "09:25" in on.covered_window
    # No premarket volume field exists — it would be a constant 0 from this feed
    assert not hasattr(on, "premarket_volume")


def test_overnight_levels_empty_is_explicit_not_silent():
    on = ind.overnight_levels(pd.DataFrame(), date(2026, 8, 27), date(2026, 8, 28))
    assert on.overnight_high is None
    assert on.bar_count == 0
    assert "no extended-hours bars" in on.covered_window


def test_time_of_day_baseline_is_per_slot_and_excludes_today():
    day = date(2026, 8, 28)
    frames = []
    # 3 prior sessions at volume 100k, today at 999k
    for i, d in enumerate([date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27)]):
        frames.append(_intraday_day(d, n_bars=6, vol=100_000.0))
    frames.append(_intraday_day(day, n_bars=6, vol=999_000.0))
    bars = pd.concat(frames)

    base = ind.time_of_day_volume_baseline(bars, day)
    assert len(base) == 6
    assert base.loc["09:30"] == pytest.approx(100_000.0), "today must be excluded"
    assert ind.relative_volume(200_000.0, "09:30", base) == pytest.approx(2.0)
    assert ind.relative_volume(100_000.0, "99:99", base) is None


# ── NYSE 2026 calendar (spec §9 verified reference values) ──────────────────

def test_nyse_2026_has_251_sessions():
    assert len(cal._schedule(2026)) == 251


@pytest.mark.parametrize("d", [date(2026, 11, 27), date(2026, 12, 24)])
def test_nyse_2026_early_closes_are_1300_et(d):
    info = cal.session_info(d)
    assert info.is_trading_day
    assert info.is_half_day
    assert info.market_close.strftime("%H:%M") == "13:00"
    assert info.early_close_time.strftime("%H:%M") == "13:00"


def test_half_day_moves_hard_close_in_by_30_minutes():
    assert cal.hard_close_time(date(2026, 11, 27)).strftime("%H:%M") == "12:30"
    assert cal.hard_close_time(date(2026, 8, 28)).strftime("%H:%M") == "15:30"


def test_non_trading_day_has_no_session():
    info = cal.session_info(date(2026, 7, 3))  # Independence Day (observed)
    assert not info.is_trading_day
    assert info.market_open is None
    assert cal.hard_close_time(date(2026, 7, 3)) is None


@pytest.mark.parametrize("hhmm,expected", [
    ("03:00", "closed"), ("07:00", "premarket"), ("09:29", "premarket"),
    ("09:30", "regular"), ("15:59", "regular"), ("16:00", "afterhours"),
    ("19:59", "afterhours"), ("20:00", "closed"),
])
def test_session_state_boundaries(hhmm, expected):
    h, m = (int(x) for x in hhmm.split(":"))
    assert cal.session_state(datetime(2026, 8, 28, h, m, tzinfo=ET)) == expected


def test_session_state_on_weekend_is_closed():
    assert cal.session_state(datetime(2026, 8, 30, 12, 0, tzinfo=ET)) == "closed"


def test_half_day_afternoon_is_afterhours_not_regular():
    """13:30 on a 13:00-close half day must not read as 'regular'."""
    assert cal.session_state(datetime(2026, 11, 27, 13, 30, tzinfo=ET)) == "afterhours"


def test_previous_session_skips_weekend():
    assert cal.previous_session(date(2026, 8, 31)) == date(2026, 8, 28)


# ── timezone discipline (spec §9: assert no naive datetime is compared) ─────

def test_naive_datetime_is_rejected_not_coerced():
    with pytest.raises(ValueError, match="timezone-aware"):
        cal.session_state(datetime(2026, 8, 28, 10, 0))
    with pytest.raises(ValueError, match="timezone-aware"):
        cal.require_et(datetime(2026, 8, 28, 10, 0))


def test_require_et_normalises_other_zones():
    utc = datetime(2026, 8, 28, 14, 0, tzinfo=ZoneInfo("UTC"))
    got = cal.require_et(utc)
    assert got.tzinfo is not None
    assert got.strftime("%H:%M") == "10:00"  # 14:00 UTC == 10:00 EDT


def test_to_et_localises_naive_index_as_utc_then_converts():
    idx = pd.date_range("2026-08-28 13:30", periods=3, freq="5min")  # naive == UTC
    df = pd.DataFrame({"Close": [1.0, 2.0, 3.0]}, index=idx)
    out = ind.to_et(df)
    assert out.index.tz is not None
    assert out.index[0].strftime("%H:%M") == "09:30"


# ── configurable trend MA (SMA/EMA, any period) ─────────────────────────────

def test_trend_ma_sma_matches_rolling_mean():
    close = _daily(n=50)["Close"]
    pd.testing.assert_series_equal(ind.trend_ma(close, "SMA", 20), close.rolling(20).mean())


def test_trend_ma_ema_matches_ewm():
    close = _daily(n=50)["Close"]
    pd.testing.assert_series_equal(
        ind.trend_ma(close, "EMA", 9), close.ewm(span=9, adjust=False).mean())


def test_trend_ma_is_case_insensitive():
    close = _daily(n=30)["Close"]
    pd.testing.assert_series_equal(ind.trend_ma(close, "ema", 9), ind.trend_ma(close, "EMA", 9))


def test_trend_ma_rejects_unknown_type():
    with pytest.raises(ValueError, match="SMA' or 'EMA'"):
        ind.trend_ma(_daily(n=30)["Close"], "WMA", 20)


def test_daily_indicators_uses_the_configured_trend_ma():
    df = _daily(n=60)
    close = df["Close"].astype(float)
    di = ind.daily_indicators(df, trend_ma_type="EMA", trend_ma_period=9)
    expected = close.ewm(span=9, adjust=False).mean().iloc[-1]
    assert di.trend_ma == pytest.approx(float(expected))
    assert di.trend_ma_label == "EMA9"
    assert di.trend_ma_distance_pct == pytest.approx((di.close - di.trend_ma) / di.trend_ma)


def test_daily_indicators_ema9_and_sma20_can_disagree():
    """EMA9 sits closer to price than SMA20 - the two need not agree on trend."""
    df = _daily(n=60)
    sma20 = ind.daily_indicators(df, "SMA", 20)
    ema9 = ind.daily_indicators(df, "EMA", 9)
    assert sma20.trend_ma != pytest.approx(ema9.trend_ma)
