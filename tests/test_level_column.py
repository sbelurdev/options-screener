"""_compute_level_column (app.py) and its rendering in _render_html_table
(agent/reporting/dashboard_table.py) — the signed distance-from-suggested-
strike column added to the Options tab's per-row table."""

import pandas as pd

from agent.reporting.dashboard_table import _render_html_table
from app import _compute_level_column


def test_call_strike_above_suggested_is_positive():
    raw = pd.DataFrame({"strike": [530.0]})
    context = {"suggested_call_strike": 527.0}
    result = _compute_level_column(raw, context, "CALL")
    assert result.iloc[0] == "+0.6%"


def test_call_strike_below_suggested_is_negative():
    raw = pd.DataFrame({"strike": [520.0]})
    context = {"suggested_call_strike": 527.0}
    result = _compute_level_column(raw, context, "CALL")
    assert result.iloc[0].startswith("-")


def test_put_strike_below_suggested_is_positive():
    """For a put, clearing the level means being BELOW it - sign convention
    flips vs calls so positive always means "favorable" either way."""
    raw = pd.DataFrame({"strike": [60.0]})
    context = {"suggested_put_strike": 63.0}
    result = _compute_level_column(raw, context, "PUT")
    assert result.iloc[0].startswith("+")


def test_put_strike_above_suggested_is_negative():
    raw = pd.DataFrame({"strike": [65.0]})
    context = {"suggested_put_strike": 63.0}
    result = _compute_level_column(raw, context, "PUT")
    assert result.iloc[0].startswith("-")


def test_missing_suggested_strike_is_a_dash():
    raw = pd.DataFrame({"strike": [100.0]})
    result = _compute_level_column(raw, {}, "CALL")
    assert result.iloc[0] == "-"


def test_empty_frame_returns_empty_series():
    result = _compute_level_column(pd.DataFrame(), {"suggested_call_strike": 100}, "CALL")
    assert len(result) == 0


def test_level_column_renders_with_positive_and_negative_css_classes():
    df = pd.DataFrame({"Rec": ["Yes", "No"], "Strike": ["$530.00", "$520.00"],
                       "Level": ["+0.6%", "-1.3%"]})
    html = _render_html_table(df, group_by_expiration=False)
    assert "lvl-pos" in html
    assert "lvl-neg" in html


# ── Rec badge / Score tooltips ───────────────────────────────────────────────

def test_rec_yes_shows_the_verdict_reason_as_a_tooltip():
    df = pd.DataFrame({"Rec": ["Yes"], "_verdict_why": ["delta 0.22"]})
    html = _render_html_table(df, group_by_expiration=False)
    assert "pe-yes" in html
    assert "delta 0.22" in html


def test_rec_blank_renders_a_neutral_badge_with_a_default_explanation():
    df = pd.DataFrame({"Rec": [""], "_verdict_why": [""]})
    html = _render_html_table(df, group_by_expiration=False)
    assert "pe-mid" in html
    assert "not this term" in html.lower()


def test_rec_blank_prefers_the_score_breakdown_when_present():
    """An unpromoted-but-scored row's _verdict_why is display_fn's Why
    column, which is the score breakdown for exactly this state."""
    df = pd.DataFrame({"Rec": [""], "_verdict_why": ["income=8.29%, delta 0.22, ..."]})
    html = _render_html_table(df, group_by_expiration=False)
    assert "income=8.29%" in html


def test_score_cell_carries_the_breakdown_as_a_tooltip():
    df = pd.DataFrame({"Score": ["0.596"], "_score_why": ["income=8.29%, delta 0.22"]})
    html = _render_html_table(df, group_by_expiration=False)
    assert "income=8.29%, delta 0.22" in html


# ── criteria hover card ──────────────────────────────────────────────────────

_SAMPLE_CRITERIA = [
    {"name": "Delta", "value": "0.22", "color": "green", "note": "Target band 0.10-0.25."},
    {"name": "Open Interest", "value": "50", "color": "red", "note": "Screened minimum: 100."},
]


def test_criteria_card_renders_one_row_per_criterion_with_color_class():
    df = pd.DataFrame({"Rec": ["Yes"], "_criteria": [_SAMPLE_CRITERIA]})
    html = _render_html_table(df, group_by_expiration=False)
    assert "<tr class='cg'>" in html   # green criterion
    assert "<tr class='cr'>" in html   # red criterion
    assert "Delta" in html and "Open Interest" in html
    assert "Target band 0.10-0.25." in html


def test_criteria_card_absent_when_no_data():
    df = pd.DataFrame({"Rec": ["Yes"], "_criteria": [None]})
    html = _render_html_table(df, group_by_expiration=False)
    assert "class='crit-card'" not in html


def test_criteria_card_appears_on_both_rec_and_score_cells():
    df = pd.DataFrame({"Rec": ["Yes"], "Score": ["0.575"], "_criteria": [_SAMPLE_CRITERIA]})
    html = _render_html_table(df, group_by_expiration=False)
    assert html.count("class='crit-card'") == 2
