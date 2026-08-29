import pytest

from agent.scoring.score import (
    DEFAULT_WEIGHTS,
    ev_yield,
    get_scoring_params,
    score_candidate,
    score_candidates,
)
from tests.conftest import make_candidate


def test_ev_yield_discounts_by_assignment_probability():
    assert ev_yield(make_candidate(annualized_yield=0.4, delta=-0.25)) == pytest.approx(0.3)
    # missing or zero delta leaves yield unadjusted
    assert ev_yield(make_candidate(annualized_yield=0.4, delta=None)) == pytest.approx(0.4)
    assert ev_yield(make_candidate(annualized_yield=0.4, delta=0.0)) == pytest.approx(0.4)


def test_higher_delta_scores_lower_at_same_yield(technicals, config):
    lo, _ = score_candidate(make_candidate(delta=-0.10), technicals, config)
    hi, _ = score_candidate(make_candidate(delta=-0.30), technicals, config)
    assert lo > hi


def test_vrp_component_rewards_rich_premium(technicals, config):
    rich, why_rich = score_candidate(make_candidate(vrp=1.6), technicals, config)
    cheap, _ = score_candidate(make_candidate(vrp=0.8), technicals, config)
    assert rich > cheap
    assert "IV/HV=1.60" in why_rich


def test_theta_component_rewards_decay(technicals, config):
    fast, why = score_candidate(make_candidate(theta_yield=0.5), technicals, config)
    none_, _ = score_candidate(make_candidate(theta_yield=0.001), technicals, config)
    assert fast > none_
    assert "theta-yield" in why


def test_earnings_penalty_applied(technicals, config):
    clean, _ = score_candidate(make_candidate(), technicals, config)
    risky, why = score_candidate(make_candidate(earnings_before_expiry=True), technicals, config)
    assert risky == pytest.approx(clean * 0.8)
    assert "earnings-risk penalty" in why


def test_weights_configurable_and_normalized():
    weights, target, cap = get_scoring_params(
        {"scoring": {"weights": {"income": 2.0, "delta": 1.0, "trend": 0.0,
                                 "liquidity": 0.0, "vrp": 0.0, "theta": 1.0},
                     "delta_target": 0.25, "income_yield_cap": 2.0}}
    )
    assert sum(weights.values()) == pytest.approx(1.0)
    assert weights["income"] == pytest.approx(0.5)
    assert target == pytest.approx(0.25)
    assert cap == pytest.approx(2.0)


def test_default_weights_used_without_config():
    weights, target, cap = get_scoring_params({})
    assert weights == pytest.approx({k: v / sum(DEFAULT_WEIGHTS.values()) for k, v in DEFAULT_WEIGHTS.items()})
    assert target == pytest.approx(0.20)


def test_delta_target_changes_preferred_strike(technicals):
    config_aggressive = {"earnings_risk_penalty": 0.2, "scoring": {"delta_target": 0.30}}
    config_safe = {"earnings_risk_penalty": 0.2, "scoring": {"delta_target": 0.10}}
    c = make_candidate(delta=-0.30)
    s_agg, _ = score_candidate(c, technicals, config_aggressive)
    s_safe, _ = score_candidate(c, technicals, config_safe)
    assert s_agg > s_safe


def test_score_candidates_percentile_differentiates_saturated_yields(technicals, config):
    # Both yields exceed the 150% absolute cap — only the percentile separates them
    rows = [
        make_candidate(annualized_yield=2.0, delta=-0.15),
        make_candidate(annualized_yield=4.0, delta=-0.15),
    ]
    score_candidates(rows, technicals, config)
    assert rows[1]["score"] > rows[0]["score"]
    assert all("why_ranked_high" in r for r in rows)


def test_score_candidates_single_row_and_empty(technicals, config):
    score_candidates([], technicals, config)  # no-op
    rows = [make_candidate()]
    score_candidates(rows, technicals, config)
    assert 0.0 <= rows[0]["score"] <= 1.0
