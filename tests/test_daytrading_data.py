"""Data-layer tests: circuit breaker, TTL cache, degraded detection (spec §9).

All network-free — yfinance is stubbed so these never hit Yahoo.
"""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from agent.daytrading import data as dt
from agent.daytrading.data import CircuitBreaker, DayTradingData, looks_halted_locally

ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 28)


def _frame(n, start="2026-08-28 09:30", freq="5min", vol=1000.0):
    idx = pd.date_range(start, periods=n, freq=freq, tz=ET)
    return pd.DataFrame(
        {"Open": np.full(n, 100.0), "High": np.full(n, 101.0), "Low": np.full(n, 99.0),
         "Close": np.full(n, 100.5), "Volume": np.full(n, vol)},
        index=idx,
    )


# ── circuit breaker ──────────────────────────────────────────────────────────

def test_circuit_breaker_trips_after_threshold_failures():
    cb = CircuitBreaker(threshold=3)
    for i in range(2):
        cb.record_failure("boom")
        assert not cb.tripped, f"must not trip on failure {i+1}"
    cb.record_failure("boom")
    assert cb.tripped
    assert cb.consecutive_failures == 3
    assert cb.last_error == "boom"


def test_circuit_breaker_success_resets_the_streak():
    cb = CircuitBreaker(threshold=3)
    cb.record_failure("a")
    cb.record_failure("b")
    cb.record_success()
    assert cb.consecutive_failures == 0
    cb.record_failure("c")
    assert not cb.tripped, "streak must restart after a success"


def test_circuit_breaker_only_reset_re_arms_it():
    cb = CircuitBreaker(threshold=1)
    cb.record_failure("x")
    assert cb.tripped
    cb.record_success()
    assert cb.tripped, "a success alone must not silently re-arm a tripped breaker"
    cb.reset()
    assert not cb.tripped


def test_tripped_breaker_stops_polling(tmp_path):
    d = DayTradingData(cache_dir=str(tmp_path))
    d.breaker.tripped = True
    assert d.poll_today("AAPL").empty
    r = d.warm_up(["AAPL"])
    assert r.failures and "circuit breaker" in r.failures["AAPL"]


def test_force_resets_breaker_and_refetches(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    d.breaker.tripped = True
    monkeypatch.setattr(d, "_download", lambda t, **k: _frame(250, freq="1D"))
    r = d.warm_up(["AAPL"], day=DAY, force=True)
    assert not d.breaker.tripped
    assert "AAPL" in r.data


# ── backoff ──────────────────────────────────────────────────────────────────

def test_with_backoff_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(dt._time, "sleep", lambda s: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("429 Too Many Requests")
        return "ok"

    assert dt.with_backoff(flaky, retries=4) == "ok"
    assert calls["n"] == 3


def test_with_backoff_reraises_after_exhausting_retries(monkeypatch):
    monkeypatch.setattr(dt._time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="always"):
        dt.with_backoff(lambda: (_ for _ in ()).throw(RuntimeError("always")), retries=2)


def test_rate_limit_detection():
    assert dt._is_rate_limited(RuntimeError("HTTP 429"))
    assert dt._is_rate_limited(RuntimeError("Too Many Requests"))
    assert not dt._is_rate_limited(RuntimeError("404 not found"))


# ── live poll TTL ────────────────────────────────────────────────────────────

def test_live_poll_uses_ttl_cache(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    calls = {"n": 0}

    def fake(ticker, **kwargs):
        calls["n"] += 1
        return _frame(10)

    monkeypatch.setattr(d, "_download", fake)

    d.poll_today("AAPL")
    d.poll_today("AAPL")
    d.poll_today("AAPL")
    assert calls["n"] == 1, "repeated reloads inside the TTL must not re-hit Yahoo"

    d.poll_today("AAPL", force=True)
    assert calls["n"] == 2, "force must bypass the TTL"


def test_live_poll_failure_records_breaker_and_returns_empty(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))

    def boom(ticker, **kwargs):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(d, "_download", boom)
    assert d.poll_today("AAPL").empty
    assert d.breaker.consecutive_failures == 1


# ── warm-up caching ──────────────────────────────────────────────────────────

def test_warmup_caches_to_parquet_and_reuses_it(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    calls = {"n": 0}

    def fake(ticker, **kwargs):
        calls["n"] += 1
        if kwargs.get("interval") == "5m":
            return _frame(500)
        return _frame(250, freq="1D")

    monkeypatch.setattr(d, "_download", fake)

    d.warm_up(["AAPL"], day=DAY, use_cache=False)
    first = calls["n"]
    assert first == 3, "daily_adj + daily_raw + intraday"
    assert len(list(tmp_path.glob("*.parquet"))) == 3

    d.warm_up(["AAPL"], day=DAY, use_cache=True)
    assert calls["n"] == first, "second warm-up must be served entirely from cache"


def test_warmup_records_per_ticker_failures_without_aborting(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))

    def fake(ticker, **kwargs):
        if ticker == "BAD":
            raise RuntimeError("no data")
        return _frame(250, freq="1D")

    monkeypatch.setattr(d, "_download", fake)
    r = d.warm_up(["AAPL", "BAD"], day=DAY, use_cache=False)
    assert "AAPL" in r.data
    assert "BAD" in r.failures
    assert "no data" in r.failures["BAD"]


# ── degraded detection (spec §8.5: degraded must never look like no-signal) ──

def test_missing_intraday_is_flagged_degraded(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))

    def fake(ticker, **kwargs):
        if kwargs.get("interval") == "5m":
            return pd.DataFrame()
        return _frame(250, freq="1D")

    monkeypatch.setattr(d, "_download", fake)
    r = d.warm_up(["AAPL"], day=DAY, use_cache=False)
    td = r.data["AAPL"]
    assert td.degraded
    assert any("intraday" in x for x in td.degraded_reasons)


def test_missing_extended_hours_bars_is_flagged_degraded(tmp_path, monkeypatch):
    """No overnight bars means overnight_high is unreliable — surface it."""
    d = DayTradingData(cache_dir=str(tmp_path))

    def fake(ticker, **kwargs):
        if kwargs.get("interval") == "5m":
            return _frame(78)  # regular session only, no extended hours
        return _frame(250, freq="1D")

    monkeypatch.setattr(d, "_download", fake)
    r = d.warm_up(["AAPL"], day=DAY, use_cache=False)
    td = r.data["AAPL"]
    assert td.degraded
    assert any("extended-hours" in x for x in td.degraded_reasons)
    assert td.extended_hours_bar_count(DAY) == 0


def test_extended_hours_bars_present_is_not_degraded(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    prev_pm = _frame(48, start="2026-08-27 16:00")  # prior-session post-market
    rth = _frame(78, start="2026-08-28 09:30")

    def fake(ticker, **kwargs):
        if kwargs.get("interval") == "5m":
            return pd.concat([prev_pm, rth])
        return _frame(250, freq="1D")

    monkeypatch.setattr(d, "_download", fake)
    r = d.warm_up(["AAPL"], day=DAY, use_cache=False)
    assert not r.data["AAPL"].degraded
    assert r.data["AAPL"].extended_hours_bar_count(DAY) == 48


# ── halt detection ───────────────────────────────────────────────────────────

def test_local_halt_backstop_on_zero_volume_completed_bar():
    rth = _frame(3, vol=1000.0)
    now = rth.index[-1].to_pydatetime() + timedelta(minutes=5)
    assert not looks_halted_locally(rth, now)

    halted = rth.copy()
    halted.iloc[-1, halted.columns.get_loc("Volume")] = 0.0
    assert looks_halted_locally(halted, now)


def test_local_halt_backstop_ignores_incomplete_bar():
    """An in-progress bar showing 0 volume is not evidence of a halt."""
    rth = _frame(2, vol=1000.0)
    rth.iloc[-1, rth.columns.get_loc("Volume")] = 0.0
    mid_bar = rth.index[-1].to_pydatetime() + timedelta(minutes=2)
    assert not looks_halted_locally(rth, mid_bar)


def test_halt_feed_filters_to_watchlist():
    assert DayTradingData._filter_halts(["AAPL", "GME"], ["AAPL", "SPY"]) == ["AAPL"]
    assert DayTradingData._filter_halts(["GME"], ["AAPL"]) == []
    assert DayTradingData._filter_halts(["AAPL"], None) == ["AAPL"]


def test_halt_feed_is_rate_limited_to_once_a_minute(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    calls = {"n": 0}

    class Resp:
        content = b"<rss><channel></channel></rss>"

        def raise_for_status(self):
            return None

    def fake_get(url, timeout=10):
        calls["n"] += 1
        return Resp()

    monkeypatch.setattr(dt.requests, "get", fake_get)
    d.halted_symbols(["AAPL"])
    d.halted_symbols(["AAPL"])
    d.halted_symbols(["AAPL"])
    assert calls["n"] == 1, "feed must not be polled more than once a minute"


def test_halt_feed_failure_returns_last_known_not_an_exception(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))

    def boom(url, timeout=10):
        raise RuntimeError("network down")

    monkeypatch.setattr(dt.requests, "get", boom)
    symbols, ts = d.halted_symbols(["AAPL"])
    assert symbols == [] and ts is None


# ── timezone conversion happens exactly once, at the boundary ───────────────

def test_to_et_index_localises_naive_as_utc():
    naive = pd.DataFrame({"Close": [1.0]}, index=pd.date_range("2026-08-28 13:30", periods=1))
    out = dt._to_et_index(naive)
    assert out.index.tz is not None
    assert out.index[0].strftime("%H:%M") == "09:30"


def test_to_et_index_is_idempotent():
    once = dt._to_et_index(_frame(3))
    twice = dt._to_et_index(once)
    pd.testing.assert_index_equal(once.index, twice.index)


# ── option chain fetch (spec §5.4) ──────────────────────────────────────────

def _prov(expirations, chains=None, exp_error=None, chain_error=None):
    class P:
        def get_options_expirations(self, ticker):
            if exp_error:
                raise RuntimeError(exp_error)
            return expirations

        def get_options_chain(self, ticker, expiration):
            if chain_error:
                raise RuntimeError(chain_error)
            return (chains or {}).get(expiration, pd.DataFrame()), pd.DataFrame()
    return P()


def _patch_provider(monkeypatch, provider):
    import agent.providers.factory as fac
    monkeypatch.setattr(fac, "build_options_provider", lambda cfg, log: provider)


def _call_row(strike=105.0, delta=0.65, oi=1000):
    return {"contractSymbol": "X", "strike": strike, "bid": 1.00, "ask": 1.02,
            "delta": delta, "theta": -0.05, "impliedVolatility": 0.30,
            "openInterest": oi, "volume": 25}


def test_fetch_call_candidates_only_uses_expiries_in_the_dte_window(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    good, too_near, too_far = date(2026, 9, 2), date(2026, 8, 29), date(2026, 9, 25)
    _patch_provider(monkeypatch, _prov(
        [too_near, good, too_far],
        {good: pd.DataFrame([_call_row()]),
         too_near: pd.DataFrame([_call_row(strike=1.0)]),
         too_far: pd.DataFrame([_call_row(strike=2.0)])},
    ))
    cands, err = d.fetch_call_candidates("AAPL", DAY, 100.0, {}, dte_min=3, dte_max=5)
    assert err is None
    assert {c.expiration for c in cands} == {good}, "only the 3-5 DTE expiry is fetched"


def test_fetch_call_candidates_reports_when_no_expiry_in_window(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    _patch_provider(monkeypatch, _prov([date(2026, 9, 25)]))
    cands, err = d.fetch_call_candidates("AAPL", DAY, 100.0, {}, dte_min=3, dte_max=5)
    assert cands == []
    assert "no expiration in the 3-5 DTE window" in err
    assert "28d" in err, "available expiries are named so the user can judge"


def test_fetch_call_candidates_surfaces_expiration_lookup_failure(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    _patch_provider(monkeypatch, _prov([], exp_error="upstream 500"))
    cands, err = d.fetch_call_candidates("AAPL", DAY, 100.0, {}, dte_min=3, dte_max=5)
    assert cands == [] and "expiration lookup failed" in err


def test_fetch_call_candidates_surfaces_chain_failure_as_error_not_empty(tmp_path, monkeypatch):
    """A failed chain must degrade, never look like 'no contract qualified'."""
    d = DayTradingData(cache_dir=str(tmp_path))
    _patch_provider(monkeypatch, _prov([date(2026, 9, 2)], chain_error="timeout"))
    cands, err = d.fetch_call_candidates("AAPL", DAY, 100.0, {}, dte_min=3, dte_max=5)
    assert cands == []
    assert err and "chain fetch failed" in err


def test_fetch_call_candidates_records_underlying_quote_time(tmp_path, monkeypatch):
    """Greeks are meaningless without the underlying price they were computed against."""
    d = DayTradingData(cache_dir=str(tmp_path))
    exp = date(2026, 9, 2)
    _patch_provider(monkeypatch, _prov([exp], {exp: pd.DataFrame([_call_row()])}))
    ts = datetime(2026, 8, 28, 10, 0, tzinfo=ET)
    cands, err = d.fetch_call_candidates("AAPL", DAY, 123.45, {}, dte_min=3, dte_max=5,
                                         underlying_quote_time=ts)
    assert err is None and len(cands) == 1
    assert cands[0].underlying_price == pytest.approx(123.45)
    assert cands[0].underlying_quote_time == ts
    assert cands[0].quote_time is not None


# ── simulation: as_of truncation must prevent lookahead ─────────────────────

def _daily_frame(days, start="2026-08-10"):
    idx = pd.date_range(start, periods=days, freq="B", tz=ET)
    return pd.DataFrame(
        {"Open": np.arange(days, dtype=float), "High": np.arange(days, dtype=float) + 1,
         "Low": np.arange(days, dtype=float) - 1, "Close": np.arange(days, dtype=float),
         "Volume": np.full(days, 1e6)},
        index=idx,
    )


def test_as_of_cuts_daily_frames_to_strictly_before_the_simulated_day():
    """Stage A is defined on the prior completed session, so `day` itself is excluded."""
    td = dt.TickerData("AAPL", daily_adj=_daily_frame(15), daily_raw=_daily_frame(15))
    sim_day = date(2026, 8, 20)
    out = td.as_of(sim_day, datetime(2026, 8, 20, 11, 0, tzinfo=ET))
    assert len(out.daily_adj) < len(td.daily_adj)
    assert all(i.date() < sim_day for i in out.daily_adj.index)
    assert all(i.date() < sim_day for i in out.daily_raw.index)


def test_as_of_prevents_daily_indicator_lookahead():
    """The replayed gate must not see prices from after the simulated date."""
    from agent.daytrading.indicators import daily_indicators
    full = _daily_frame(30)
    td = dt.TickerData("AAPL", daily_adj=full, daily_raw=full)
    sim_day = date(2026, 8, 25)

    live_close = daily_indicators(full).close
    sim_close = daily_indicators(td.as_of(sim_day, datetime(2026, 8, 25, 11, 0, tzinfo=ET)).daily_adj).close

    assert sim_close != live_close, "truncation must change what the gate sees"
    assert sim_close < live_close, "the rising fixture proves no future bar leaked in"


def test_as_of_cuts_intraday_at_the_simulated_clock():
    bars = _frame(78, start="2026-08-28 09:30")  # 09:30 -> 15:55
    td = dt.TickerData("AAPL", intraday=bars)
    out = td.as_of(DAY, datetime(2026, 8, 28, 11, 0, tzinfo=ET))
    assert out.intraday.index.max() <= pd.Timestamp("2026-08-28 11:00", tz=ET)
    assert len(out.intraday) < len(bars), "afternoon bars must be dropped"


def test_as_of_is_non_destructive():
    full = _daily_frame(20)
    bars = _frame(78, start="2026-08-28 09:30")
    td = dt.TickerData("AAPL", daily_adj=full, daily_raw=full, intraday=bars,
                       degraded_reasons=["x"])
    before = (len(td.daily_adj), len(td.intraday))
    td.as_of(date(2026, 8, 20), datetime(2026, 8, 20, 11, 0, tzinfo=ET))
    assert (len(td.daily_adj), len(td.intraday)) == before, "original must be untouched"


def test_as_of_carries_degraded_reasons_through():
    td = dt.TickerData("AAPL", daily_adj=_daily_frame(20),
                       degraded_reasons=["no intraday 5m bars returned"])
    out = td.as_of(date(2026, 8, 20), datetime(2026, 8, 20, 11, 0, tzinfo=ET))
    assert out.degraded and out.degraded_reasons == td.degraded_reasons


def test_as_of_handles_empty_frames():
    td = dt.TickerData("AAPL")
    out = td.as_of(DAY, datetime(2026, 8, 28, 11, 0, tzinfo=ET))
    assert out.daily_adj.empty and out.intraday.empty


# ── per-day static cache (optimisation must not change results) ─────────────

def test_static_context_is_computed_once_per_ticker_day(tmp_path, monkeypatch):
    d = DayTradingData(cache_dir=str(tmp_path))
    td = dt.TickerData("AAPL", daily_adj=_daily_frame(30), daily_raw=_daily_frame(30),
                       intraday=_frame(78, start="2026-08-28 09:30"))
    calls = {"n": 0}
    import agent.daytrading.indicators as ind
    real = ind.time_of_day_volume_baseline

    def counted(*a, **k):
        calls["n"] += 1
        return real(*a, **k)

    monkeypatch.setattr(ind, "time_of_day_volume_baseline", counted)

    a = d.static_context("AAPL", td, DAY)
    b = d.static_context("AAPL", td, DAY)
    assert a is b, "same (ticker, day) must return the cached object"
    assert calls["n"] == 1, "the expensive baseline must be computed once"

    d.static_context("AAPL", td, date(2026, 8, 27))
    assert calls["n"] == 2, "a different day is a different cache entry"


def test_clear_static_forces_recompute(tmp_path):
    d = DayTradingData(cache_dir=str(tmp_path))
    td = dt.TickerData("AAPL", daily_adj=_daily_frame(30), daily_raw=_daily_frame(30),
                       intraday=_frame(78, start="2026-08-28 09:30"))
    a = d.static_context("AAPL", td, DAY)
    d.clear_static()
    b = d.static_context("AAPL", td, DAY)
    assert a is not b


def test_static_context_matches_direct_computation(tmp_path):
    """The cache is an optimisation: values must be identical to recomputing."""
    from agent.daytrading.indicators import (
        daily_indicators, prev_close_raw, time_of_day_volume_baseline,
    )
    d = DayTradingData(cache_dir=str(tmp_path))
    td = dt.TickerData("AAPL", daily_adj=_daily_frame(40), daily_raw=_daily_frame(40),
                       intraday=_frame(78, start="2026-08-28 09:30"))
    ctx = d.static_context("AAPL", td, DAY)
    assert ctx.daily.close == daily_indicators(td.daily_adj).close
    assert ctx.prev_close == prev_close_raw(td.daily_raw)
    pd.testing.assert_series_equal(
        ctx.volume_baseline, time_of_day_volume_baseline(td.intraday, DAY))
