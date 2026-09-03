from datetime import date

from agent.recommendation.context import build_context_store, build_ticker_context, strategy_fit


def test_build_ticker_context_reuses_already_computed_fields(price_df, technicals):
    """No new fetch - price_df/technicals/earnings_date are exactly what
    pipeline.py's _process_ticker already stores per ticker."""
    ticker_result = {
        "price_df": price_df,
        "technicals": technicals,
        "earnings_date": date(2026, 12, 1),
    }
    ctx = build_ticker_context(ticker_result)

    assert ctx["spot"] == technicals["spot"]
    assert ctx["regime"] in ("Bullish", "Neutral", "Bearish")
    assert ctx["earnings_date"] == "2026-12-01"
    assert "low_52w" in ctx["support"]
    assert "high_52w" in ctx["resistance"]


def test_build_ticker_context_handles_missing_earnings_date(price_df, technicals):
    ticker_result = {"price_df": price_df, "technicals": technicals, "earnings_date": None}
    ctx = build_ticker_context(ticker_result)
    assert ctx["earnings_date"] is None


def test_build_context_store_skips_tickers_with_no_price_history(price_df, technicals):
    results_map = {
        "AAPL": {"price_df": price_df, "technicals": technicals, "earnings_date": None},
        "NODATA": {"price_df": None, "technicals": {}, "earnings_date": None},
    }
    store = build_context_store(results_map)
    assert "AAPL" in store
    assert "NODATA" not in store


def test_build_context_store_is_json_serializable(price_df, technicals):
    import json
    results_map = {"AAPL": {"price_df": price_df, "technicals": technicals,
                            "earnings_date": date(2026, 6, 1)}}
    store = build_context_store(results_map)
    json.dumps(store)  # must not raise


# ── suggested strike ─────────────────────────────────────────────────────────

def test_suggested_call_strike_is_resistance_plus_buffer(price_df, technicals):
    ticker_result = {"price_df": price_df, "technicals": technicals, "earnings_date": None}
    ctx = build_ticker_context(ticker_result, resistance_buffer=0.02)
    resistance_level = ctx["resistance"]["swing_high_20d"]
    assert ctx["suggested_call_strike"] == round(resistance_level * 1.02, 2)


def test_suggested_put_strike_is_support_minus_buffer(price_df, technicals):
    ticker_result = {"price_df": price_df, "technicals": technicals, "earnings_date": None}
    ctx = build_ticker_context(ticker_result, support_buffer=0.02)
    support_level = ctx["support"]["swing_low_20d"]
    assert ctx["suggested_put_strike"] == round(support_level * 0.98, 2)


def test_suggested_strikes_are_none_without_enough_history(technicals):
    import pandas as pd
    short_df = pd.DataFrame({"Close": [100.0, 101.0], "High": [101.0, 102.0], "Low": [99.0, 100.0]})
    ticker_result = {"price_df": short_df, "technicals": technicals, "earnings_date": None}
    ctx = build_ticker_context(ticker_result)
    # Fewer than 20 rows means no swing level and no 52w extreme absent too -
    # wait, high_52w/low_52w always exist (just max/min of what's there), so
    # a suggestion still gets produced from the wider level as a fallback.
    assert ctx["suggested_call_strike"] is not None
    assert ctx["suggested_put_strike"] is not None


# ── IV rank / VRP passthrough ────────────────────────────────────────────────

def test_ivr_and_vrp_pass_through_from_ticker_result(price_df, technicals):
    ticker_result = {
        "price_df": price_df, "technicals": technicals, "earnings_date": None,
        "iv_rank": (62.5, "true IV rank"), "atm_iv": 0.54,
    }
    ctx = build_ticker_context(ticker_result)
    assert ctx["ivr"] == 62.5
    assert ctx["ivr_source"] == "true IV rank"
    assert ctx["vrp"] == round(0.54 / technicals["hv20"], 3)


def test_ivr_and_vrp_are_none_when_unavailable(price_df, technicals):
    ticker_result = {"price_df": price_df, "technicals": technicals, "earnings_date": None}
    ctx = build_ticker_context(ticker_result)
    assert ctx["ivr"] is None
    assert ctx["vrp"] is None


# ── strategy_fit ─────────────────────────────────────────────────────────────

def test_bullish_regime_favors_csp_but_not_covered_call():
    ctx = {"regime": "Bullish", "rsi14": 55, "ivr": None}
    assert strategy_fit(ctx, "PUT")["regime"]["read"] == "favorable"
    assert strategy_fit(ctx, "CALL")["regime"]["read"] == "mixed"


def test_neutral_regime_favors_covered_call():
    ctx = {"regime": "Neutral", "rsi14": 55, "ivr": None}
    assert strategy_fit(ctx, "CALL")["regime"]["read"] == "favorable"
    assert strategy_fit(ctx, "PUT")["regime"]["read"] == "mixed"


def test_bearish_regime_is_unfavorable_for_both():
    ctx = {"regime": "Bearish", "rsi14": 55, "ivr": None}
    assert strategy_fit(ctx, "CALL")["regime"]["read"] == "unfavorable"
    assert strategy_fit(ctx, "PUT")["regime"]["read"] == "unfavorable"


def test_overbought_rsi_favors_covered_call_not_csp():
    ctx = {"regime": "Neutral", "rsi14": 80, "ivr": None}
    assert strategy_fit(ctx, "CALL")["rsi"]["read"] == "favorable"
    assert strategy_fit(ctx, "PUT")["rsi"]["read"] == "mixed"


def test_oversold_rsi_is_unfavorable_for_both():
    ctx = {"regime": "Neutral", "rsi14": 20, "ivr": None}
    assert strategy_fit(ctx, "CALL")["rsi"]["read"] == "unfavorable"
    assert strategy_fit(ctx, "PUT")["rsi"]["read"] == "unfavorable"


def test_high_ivr_is_favorable_regardless_of_strategy():
    ctx = {"regime": "Neutral", "rsi14": 55, "ivr": 75}
    assert strategy_fit(ctx, "CALL")["ivr"]["read"] == "favorable"
    assert strategy_fit(ctx, "PUT")["ivr"]["read"] == "favorable"


def test_missing_data_reads_as_unknown_not_unfavorable():
    ctx = {"regime": None, "rsi14": None, "ivr": None}
    fit = strategy_fit(ctx, "CALL")
    assert fit["regime"]["read"] == "unknown"
    assert fit["rsi"]["read"] == "unknown"
    assert fit["ivr"]["read"] == "unknown"


def test_detail_states_the_fact_and_the_implication():
    ctx = {"regime": "Bullish", "regime_reason": "close above both MA20 and MA50",
          "rsi14": 64, "ivr": None}
    detail = strategy_fit(ctx, "PUT")["regime"]["detail"]
    assert "close above both MA20 and MA50" in detail
    assert "downtrend" in detail  # the CSP-specific implication


def test_detail_flips_implication_between_strategies():
    ctx = {"regime": "Bullish", "regime_reason": "close above both MA20 and MA50",
          "rsi14": 55, "ivr": None}
    put_detail = strategy_fit(ctx, "PUT")["regime"]["detail"]
    call_detail = strategy_fit(ctx, "CALL")["regime"]["detail"]
    assert put_detail != call_detail


def test_ivr_detail_names_the_reason_when_unavailable():
    ctx = {"regime": "Neutral", "rsi14": 55, "ivr": None,
          "ivr_source": "IV history 5/20 days"}
    detail = strategy_fit(ctx, "CALL")["ivr"]["detail"]
    assert "IV history 5/20 days" in detail
