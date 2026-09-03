from agent.signals.technicals import classify_regime


def test_bullish_when_above_both_mas_and_not_overbought():
    label, reason = classify_regime(spot=100, ma20=95, ma50=90, rsi14=55)
    assert label == "Bullish"
    assert "MA20" in reason and "MA50" in reason


def test_bearish_when_below_both_mas():
    label, reason = classify_regime(spot=80, ma20=95, ma50=90, rsi14=50)
    assert label == "Bearish"
    assert "below both" in reason


def test_bearish_below_both_mas_notes_oversold_too():
    label, reason = classify_regime(spot=80, ma20=95, ma50=90, rsi14=25)
    assert label == "Bearish"
    assert "oversold" in reason


def test_bearish_when_overbought_but_still_below_ma50():
    # Above MA20 (short pop) but below MA50 (still in a longer downtrend) and
    # overbought - a stretched move against the larger trend, not a real breakout.
    label, reason = classify_regime(spot=92, ma20=90, ma50=95, rsi14=80)
    assert label == "Bearish"
    assert "overbought" in reason


def test_neutral_when_overbought_but_trend_otherwise_intact():
    label, reason = classify_regime(spot=100, ma20=95, ma50=90, rsi14=80)
    assert label == "Neutral"
    assert "overbought" in reason


def test_neutral_on_mixed_ma_signal():
    label, reason = classify_regime(spot=92, ma20=90, ma50=95, rsi14=50)
    assert label == "Neutral"
    assert "mixed" in reason
