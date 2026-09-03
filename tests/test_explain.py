"""agent.scoring.explain — plain-English peer comparisons, replacing raw
"delta 0.22"-style restatements with an actual reason one strike beats
another. Fixture numbers mirror the real MSFT covered-call screenshot that
prompted this: three $5-apart strikes, same expiration, decreasing
delta/yield/score as strike moves further OTM."""

from agent.scoring.explain import criteria_rows, explain_group, explain_relative

MSFT_GROUP = [
    {"strike": 535.0, "delta": 0.222, "annualized_yield": 0.090, "_premium": 4.68,
     "score": 0.575, "expiration": "2026-10-09"},
    {"strike": 540.0, "delta": 0.189, "annualized_yield": 0.073, "_premium": 3.81,
     "score": 0.520, "expiration": "2026-10-09"},
    {"strike": 545.0, "delta": 0.162, "annualized_yield": 0.062, "_premium": 3.22,
     "score": 0.451, "expiration": "2026-10-09"},
]


def test_single_row_group_has_nothing_to_compare():
    assert explain_relative(MSFT_GROUP[0], [MSFT_GROUP[0]], "CALL") == ""


def test_best_row_states_the_trade_and_the_edge_over_the_next_safest():
    text = explain_relative(MSFT_GROUP[0], MSFT_GROUP, "CALL")
    assert "$535.00" in text
    assert "9.0%" in text
    assert "22%" in text  # assignment odds from delta
    assert "Best score" in text
    assert "$545" in text  # names which strike it's beating


def test_non_best_row_explains_the_yield_vs_safety_tradeoff():
    text = explain_relative(MSFT_GROUP[1], MSFT_GROUP, "CALL")
    assert "$540.00" in text
    assert "$535" in text  # names the better strike it's compared against
    assert "less" in text.lower() or "Gives up" in text


def test_worst_row_explains_it_gave_up_yield_for_not_enough_safety():
    text = explain_relative(MSFT_GROUP[2], MSFT_GROUP, "CALL")
    assert "$545.00" in text
    assert "$535" in text
    assert "less" in text.lower()


def test_put_strategy_uses_assignment_forced_to_buy_language():
    puts = [
        {"strike": 60.0, "delta": -0.20, "annualized_yield": 0.15, "_premium": 1.5,
         "score": 0.60, "expiration": "2026-10-09"},
        {"strike": 62.0, "delta": -0.30, "annualized_yield": 0.20, "_premium": 2.0,
         "score": 0.55, "expiration": "2026-10-09"},
    ]
    text = explain_relative(puts[0], puts, "PUT")
    assert "forced to buy" in text


def test_lower_score_despite_equal_or_better_yield_blames_other_factors_not_yield():
    """A row that pays as much or more but still scores lower must not claim
    a false "gave up yield" story - liquidity/vrp/theta must have been the
    reason, and the text should say so rather than mislead."""
    group = [
        {"strike": 100.0, "delta": 0.20, "annualized_yield": 0.10, "_premium": 2.0,
         "score": 0.70, "expiration": "2026-10-09"},
        {"strike": 105.0, "delta": 0.15, "annualized_yield": 0.12, "_premium": 2.2,
         "score": 0.60, "expiration": "2026-10-09"},  # higher yield, lower score
    ]
    text = explain_relative(group[1], group, "CALL")
    assert "spread" in text.lower() or "open interest" in text.lower() or "volatility" in text.lower()
    assert "Gives up" not in text  # would misstate - this row's yield is HIGHER, not given up


def test_explain_group_covers_every_row_by_position():
    result = explain_group(MSFT_GROUP, "CALL")
    assert set(result.keys()) == {0, 1, 2}
    assert all(result.values())  # a 3-row group always has something to say


def test_rows_missing_score_are_excluded_from_comparison():
    group = [
        {"strike": 100.0, "delta": 0.20, "annualized_yield": 0.10, "_premium": 2.0,
         "score": 0.70, "expiration": "2026-10-09"},
        {"strike": 105.0, "delta": 0.15, "annualized_yield": 0.08, "_premium": 1.5,
         "score": None, "expiration": "2026-10-09"},
    ]
    # Only one row actually has a score - nothing meaningful to compare.
    assert explain_relative(group[0], group, "CALL") == ""


# ── criteria_rows ─────────────────────────────────────────────────────────────

CONFIG = {
    "cc_recommendation": {"delta_min": 0.10, "delta_max": 0.25, "earnings_buffer_days": 7},
    "csp_recommendation": {"delta_min": 0.10, "delta_max": 0.25, "earnings_buffer_days": 7},
    "min_open_interest": 100,
    "max_spread_pct": 0.15,
}

GOOD_ROW = {
    "delta": 0.22, "annualized_yield": 0.090, "ivr": 62.0, "vrp": 1.12,
    "open_interest": 3183, "spread_pct": 0.058, "theta_yield": 0.117,
    "dte": 38, "earnings_before_expiry": False,
}


def _find(rows, name):
    return next(r for r in rows if r["name"] == name)


def test_criteria_rows_covers_all_nine_criteria():
    rows = criteria_rows(GOOD_ROW, CONFIG, "CALL")
    names = {r["name"] for r in rows}
    assert names == {"Delta", "Annualized Yield", "IV Rank", "VRP (IV/HV)", "Open Interest",
                     "Bid-Ask Spread", "Theta Yield", "Days to Expiration", "Earnings Risk"}


def test_delta_in_target_band_is_green():
    rows = criteria_rows(GOOD_ROW, CONFIG, "CALL")
    assert _find(rows, "Delta")["color"] == "green"
    assert _find(rows, "Delta")["value"] == "0.22"


def test_delta_far_outside_band_is_red():
    row = dict(GOOD_ROW, delta=0.60)
    rows = criteria_rows(row, CONFIG, "CALL")
    assert _find(rows, "Delta")["color"] == "red"


def test_thin_open_interest_is_red_once_below_the_screened_minimum():
    row = dict(GOOD_ROW, open_interest=50)
    rows = criteria_rows(row, CONFIG, "CALL")
    oi = _find(rows, "Open Interest")
    assert oi["color"] == "red"
    assert "100" in oi["note"]  # states the actual screened minimum


def test_wide_spread_is_red_beyond_the_configured_maximum():
    row = dict(GOOD_ROW, spread_pct=0.25)  # 25% > configured 15% max
    rows = criteria_rows(row, CONFIG, "CALL")
    assert _find(rows, "Bid-Ask Spread")["color"] == "red"


def test_tight_spread_is_green():
    row = dict(GOOD_ROW, spread_pct=0.03)
    rows = criteria_rows(row, CONFIG, "CALL")
    assert _find(rows, "Bid-Ask Spread")["color"] == "green"


def test_earnings_in_window_is_red_no_earnings_is_green():
    with_earnings = dict(GOOD_ROW, earnings_before_expiry=True)
    without = dict(GOOD_ROW, earnings_before_expiry=False)
    assert _find(criteria_rows(with_earnings, CONFIG, "CALL"), "Earnings Risk")["color"] == "red"
    assert _find(criteria_rows(without, CONFIG, "CALL"), "Earnings Risk")["color"] == "green"


def test_dte_sweet_spot_is_green_extremes_are_red():
    sweet = dict(GOOD_ROW, dte=35)
    too_short = dict(GOOD_ROW, dte=2)
    too_long = dict(GOOD_ROW, dte=120)
    assert _find(criteria_rows(sweet, CONFIG, "CALL"), "Days to Expiration")["color"] == "green"
    assert _find(criteria_rows(too_short, CONFIG, "CALL"), "Days to Expiration")["color"] == "red"
    assert _find(criteria_rows(too_long, CONFIG, "CALL"), "Days to Expiration")["color"] == "red"


def test_missing_fields_read_as_yellow_not_red():
    """Missing data must never look like a failed criterion."""
    sparse = {"delta": None, "annualized_yield": None, "ivr": None, "vrp": None,
             "open_interest": None, "spread_pct": None, "theta_yield": None,
             "dte": None, "earnings_before_expiry": None}
    rows = criteria_rows(sparse, CONFIG, "CALL")
    for r in rows:
        if r["name"] != "Earnings Risk":  # earnings has a real default (no known earnings = green)
            assert r["color"] != "red", f"{r['name']} went red on missing data"


def test_uses_csp_config_when_option_right_is_put():
    """delta_min/max should come from csp_recommendation, not cc_recommendation."""
    config = {
        "cc_recommendation": {"delta_min": 0.05, "delta_max": 0.10},
        "csp_recommendation": {"delta_min": 0.30, "delta_max": 0.40},
    }
    row = dict(GOOD_ROW, delta=-0.35)
    rows = criteria_rows(row, config, "PUT")
    assert _find(rows, "Delta")["color"] == "green"  # 0.35 is in the PUT band, not the CC band
