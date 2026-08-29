from datetime import date, timedelta

import pandas as pd
import pytest

from agent.signals.options_metrics import (
    annualized_yield,
    black_scholes_delta,
    breakeven,
    build_option_records,
    get_term_for_dte,
    select_expiration_dates,
    select_monthly_cc_expiration_dates,
    spread_pct,
)


def test_annualized_yield_put_uses_strike_collateral():
    # (1.4 * 100) / (90 * 100) * (365 / 30)
    y = annualized_yield("PUT", 1.4, 90.0, 100.0, 30)
    assert y == pytest.approx((140.0 / 9000.0) * (365.0 / 30.0))


def test_annualized_yield_call_uses_spot_collateral():
    y = annualized_yield("CALL", 1.4, 110.0, 100.0, 30)
    assert y == pytest.approx((140.0 / 10000.0) * (365.0 / 30.0))


def test_breakeven():
    assert breakeven("PUT", 90.0, 100.0, 1.4) == pytest.approx(88.6)
    assert breakeven("CALL", 110.0, 100.0, 1.4) == pytest.approx(111.4)


def test_spread_pct():
    assert spread_pct(1.0, 2.0) == pytest.approx(1.0 / 1.5)
    assert spread_pct(0.0, 0.0) is None
    assert spread_pct(2.0, 1.0) is None  # crossed market


def test_black_scholes_delta_signs_and_bounds():
    call = black_scholes_delta("CALL", 100.0, 105.0, 30, 0.4, 0.05)
    put = black_scholes_delta("PUT", 100.0, 95.0, 30, 0.4, 0.05)
    assert 0.0 < call < 1.0
    assert -1.0 < put < 0.0
    assert black_scholes_delta("CALL", 100.0, 105.0, 0, 0.4, 0.05) is None
    assert black_scholes_delta("CALL", 100.0, 105.0, 30, 0.0, 0.05) is None


def test_get_term_for_dte_boundaries():
    assert get_term_for_dte(14)[0] == "short_term"
    assert get_term_for_dte(15)[0] == "medium_term"
    assert get_term_for_dte(28)[0] == "medium_term"
    assert get_term_for_dte(29)[0] == "long_term"


def test_select_expiration_dates_fridays_beyond_14_dte():
    today = date(2026, 6, 8)  # a Monday
    expirations = [
        today,                          # same-day -> excluded
        today + timedelta(days=3),      # within 14 -> kept (any weekday)
        today + timedelta(days=18),     # Friday 2026-06-26 -> kept
        today + timedelta(days=17),     # Thursday -> excluded (>14, not Friday)
        today + timedelta(days=60),     # beyond max_dte -> excluded
    ]
    sel = select_expiration_dates(expirations, today, max_dte=45)
    assert today + timedelta(days=3) in sel
    assert today + timedelta(days=18) in sel
    assert today + timedelta(days=17) not in sel
    assert today + timedelta(days=60) not in sel
    assert today not in sel


def test_select_monthly_cc_one_friday_per_month():
    today = date(2026, 6, 8)
    fridays = [date(2026, 8, 7), date(2026, 8, 21), date(2026, 9, 18)]
    sel = select_monthly_cc_expiration_dates(fridays, today, min_dte=45, max_months=9)
    assert sel == [date(2026, 8, 7), date(2026, 9, 18)]


@pytest.fixture
def chain():
    return pd.DataFrame(
        [
            # wide-spread OTM put with theta
            {"contractSymbol": "T1", "strike": 90.0, "bid": 1.00, "ask": 2.00, "lastPrice": 1.5,
             "volume": 100, "openInterest": 500, "impliedVolatility": 0.60, "delta": -0.18, "theta": -0.08},
            # ITM put -> filtered (not OTM is allowed; but delta out of range filters it)
            {"contractSymbol": "T2", "strike": 110.0, "bid": 10.0, "ask": 11.0, "lastPrice": 10.5,
             "volume": 10, "openInterest": 50, "impliedVolatility": 0.50, "delta": -0.70, "theta": -0.02},
            # zero bid -> filtered
            {"contractSymbol": "T3", "strike": 80.0, "bid": 0.0, "ask": 0.5, "lastPrice": 0.2,
             "volume": 5, "openInterest": 10, "impliedVolatility": 0.55, "delta": -0.05, "theta": -0.01},
        ]
    )


def test_build_option_records_fill_price_and_new_fields(chain, technicals, config, logger):
    records = build_option_records(
        ticker="TEST", strategy="PUT", options_df=chain,
        expiration=date.today() + timedelta(days=10),
        bucket_name="short_term", bucket_label="Short-Term", spot=100.0,
        technicals=technicals, earnings_date=None, config=config, logger=logger,
    )
    assert len(records) == 1  # T2 delta out of range, T3 invalid bid
    r = records[0]
    assert r["fill_price"] == pytest.approx(1.0 + 0.4 * 1.0)  # bid + 0.4*spread
    assert r["mid"] == pytest.approx(1.5)
    # yield/breakeven/max_profit all use fill, not mid
    assert r["breakeven"] == pytest.approx(90.0 - 1.4)
    assert r["max_profit"] == pytest.approx(140.0)
    assert r["annualized_yield"] == pytest.approx((1.4 * 100 / 9000.0) * (365.0 / r["dte"]))
    # new analytics fields
    assert r["theta"] == pytest.approx(-0.08)
    assert r["theta_yield"] == pytest.approx(0.08 * 365.0 / 90.0, abs=1e-6)
    assert r["vrp"] == pytest.approx(0.60 / 0.45, abs=1e-4)


def test_build_option_records_min_yield_filter(chain, technicals, config, logger):
    config["min_annualized_yield"] = 10.0  # absurdly high -> everything filtered
    records = build_option_records(
        ticker="TEST", strategy="PUT", options_df=chain,
        expiration=date.today() + timedelta(days=10),
        bucket_name="short_term", bucket_label="Short-Term", spot=100.0,
        technicals=technicals, earnings_date=None, config=config, logger=logger,
    )
    assert records == []
