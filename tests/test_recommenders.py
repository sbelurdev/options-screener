from datetime import date, timedelta

import pytest

from agent.recommendation.cc_recommender import _recommend_monthly_cc, recommend_cc_for_ticker
from agent.recommendation.csp_recommender import _recommend_csp_for_term, compute_ivr_proxy
from tests.conftest import make_candidate

TODAY = date.today()


@pytest.fixture
def csp_config():
    return {"ivr_min": 0.0, "earnings_buffer_days": 7, "delta_min": 0.10,
            "delta_max": 0.25, "use_support_filter": False}


@pytest.fixture
def cc_config():
    return {"delta_min": 0.10, "delta_max": 0.25, "earnings_buffer_days": 7,
            "resistance_pct_buffer": 0.02, "max_suggestions_per_term": 3}


# ── CSP ──────────────────────────────────────────────────────────────────────

def test_csp_prefers_earnings_clear_candidate(price_df, technicals, csp_config):
    earnings = TODAY + timedelta(days=11)
    straddles = make_candidate(expiration=(TODAY + timedelta(days=12)).isoformat(), score=0.9)
    clear = make_candidate(expiration=(TODAY + timedelta(days=9)).isoformat(), score=0.5)
    res = _recommend_csp_for_term("TEST", "Short-Term", [straddles, clear], price_df,
                                  technicals, earnings, csp_config)
    assert res["expiration"] == clear["expiration"]
    assert "earnings" not in res["reason"]


def test_csp_hard_fails_when_all_straddle_earnings(price_df, technicals, csp_config):
    earnings = TODAY + timedelta(days=11)
    straddles = make_candidate(expiration=(TODAY + timedelta(days=12)).isoformat())
    res = _recommend_csp_for_term("TEST", "Short-Term", [straddles], price_df,
                                  technicals, earnings, csp_config)
    assert res["recommend"] == "No"
    assert "earnings" in res["reason"]


def test_csp_ivr_below_threshold_hard_fails(price_df, technicals, csp_config):
    csp_config["ivr_min"] = 99.0
    res = _recommend_csp_for_term("TEST", "Short-Term", [make_candidate()], price_df,
                                  technicals, None, csp_config,
                                  iv_rank=(50.0, "true IV rank (25 obs)"))
    assert res["recommend"] == "No"
    assert "IVR 50%" in res["reason"]


def test_csp_uses_passed_iv_rank_over_proxy(price_df, technicals, csp_config):
    res = _recommend_csp_for_term("TEST", "Short-Term", [make_candidate()], price_df,
                                  technicals, None, csp_config,
                                  iv_rank=(53.3, "true IV rank (25 obs)"))
    assert res["ivr"] == 53.3
    assert "true IV rank" in res["ivr_source"]


def test_csp_falls_back_to_proxy_when_rank_unavailable(price_df, technicals, csp_config):
    res = _recommend_csp_for_term("TEST", "Short-Term", [make_candidate()], price_df,
                                  technicals, None, csp_config,
                                  iv_rank=(None, "IV history 1/20 days"))
    assert "proxy" in res["ivr_source"]


def test_csp_delta_out_of_range_returns_no(price_df, technicals, csp_config):
    res = _recommend_csp_for_term("TEST", "Short-Term", [make_candidate(delta=-0.45)],
                                  price_df, technicals, None, csp_config)
    assert res["recommend"] == "No"
    assert "delta" in res["reason"]


def test_csp_premium_uses_fill_price(price_df, technicals, csp_config):
    res = _recommend_csp_for_term("TEST", "Short-Term",
                                  [make_candidate(mid=1.5, fill_price=1.4)],
                                  price_df, technicals, None, csp_config)
    assert res["premium"] == pytest.approx(1.4)
    assert res["breakeven"] == pytest.approx(85.0 - 1.4)


def test_compute_ivr_proxy_insufficient_history():
    import pandas as pd
    ivr, source = compute_ivr_proxy(pd.DataFrame({"Close": [1.0] * 10}), None)
    assert ivr is None
    assert "insufficient" in source


# ── CC ───────────────────────────────────────────────────────────────────────

def _call(**overrides):
    base = make_candidate(strategy="CALL", strike=110.0, delta=0.18,
                          annualized_yield=0.3, mid=2.0, fill_price=1.9)
    base.update(overrides)
    return base


def test_cc_monthly_picks_risk_adjusted_yield(price_df, technicals, cc_config):
    exp = (TODAY + timedelta(days=60)).isoformat()
    high_raw = _call(expiration=exp, strike=110.0, delta=0.40, annualized_yield=0.50)
    low_delta = _call(expiration=exp, strike=115.0, delta=0.15, annualized_yield=0.40)
    rows = _recommend_monthly_cc("TEST", [high_raw, low_delta], price_df, technicals,
                                 None, None, cc_config)
    # 0.50 × (1−0.40) = 0.30 < 0.40 × (1−0.15) = 0.34
    assert rows[0]["strike"] == 115.0


def test_cc_below_min_price_flagged_no(price_df, technicals, cc_config):
    recs = recommend_cc_for_ticker("TEST", [_call(dte=10)], price_df, technicals,
                                   None, min_acceptable_price=120.0, rec_config=cc_config)
    short = [r for r in recs if r["term"] == "Short-Term"][0]
    assert short["recommend"] == "No"
    assert "below min" in short["reason"]


def test_cc_delta_in_range_yes(price_df, technicals, cc_config):
    recs = recommend_cc_for_ticker("TEST", [_call(dte=10)], price_df, technicals,
                                   None, None, rec_config=cc_config)
    short = [r for r in recs if r["term"] == "Short-Term"][0]
    assert short["recommend"] == "Yes"
    assert short["premium"] == pytest.approx(1.9)  # fill price, not mid


def test_cc_always_returns_row_per_term(price_df, technicals, cc_config):
    recs = recommend_cc_for_ticker("TEST", [], price_df, technicals, None, None,
                                   rec_config=cc_config)
    assert {r["term"] for r in recs} == {"Short-Term", "Medium-Term", "Long-Term"}
    assert all(r["recommend"] == "No" for r in recs)
