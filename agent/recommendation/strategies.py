"""
Strategy registry: adds one new options strategy (LEAPs, spreads, ...) by
adding one new Strategy entry, not by editing pipeline.py/app.py/
dashboard_table.py separately. Mirrors the provider-factory pattern already
used for options/market/fundamentals providers (agent/providers/factory.py).

Deliberately a THIN adapter — it wraps the existing, unchanged
build_cc_recommendations/build_csp_recommendations (batch compute) and
build_calls_combined/build_puts_combined/_build_calls_display/
_build_puts_display (presentation) rather than reimplementing them. A
strategy whose recommend/combine/display logic differs enough from CC/CSP's
shape gets its own functions with the same signatures, not a rewrite of
these two.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from agent.recommendation.cc_recommender import build_cc_recommendations
from agent.recommendation.csp_recommender import build_csp_recommendations
from agent.reporting.dashboard_table import (
    _build_calls_display,
    _build_puts_display,
    build_calls_combined,
    build_puts_combined,
)


@dataclass(frozen=True)
class ColumnSpec:
    key: str                    # column name produced by display_fn
    label: Optional[str] = None  # header text; defaults to key if not renamed


@dataclass(frozen=True)
class Strategy:
    name: str                          # "covered_call" — registry key
    display_name: str                  # "Covered Call" — shown in the picker
    option_right: str                  # "CALL" | "PUT" — matches the "strategy" CSV column
    ticker_config_key: str             # "covered_call_tickers" in profile config
    recs_file_suffix: str              # "cc_recs" -> {date}_cc_recs.csv
    recs_state_key: str                # "last_cc_recs_path" in st.session_state
    recommend_fn: Callable[..., List[Dict[str, Any]]]
    combine_fn: Callable[..., Any]
    display_fn: Callable[..., Any]
    columns: List[ColumnSpec]          # per-contract columns shown in the table
    has_monthly: bool = False          # only covered_call reads the monthly-calls CSV


_CALL_COLUMNS = [ColumnSpec(k) for k in (
    "Rec", "AnnualYield", "Strike", "Level", "%OTM", "Expiration", "DTE",
    "Premium", "Delta", "IVR", "VRP", "ΘYld", "MaxProfit", "Breakeven", "Score",
)]
_PUT_COLUMNS = [ColumnSpec(k) for k in (
    "Rec", "AnnualYield", "Strike", "Level", "%ToStrike", "Expiration", "DTE",
    "Premium", "Delta", "IVR", "VRP", "ΘYld", "MaxProfit", "Breakeven",
    "CashRqd", "Score",
)]

STRATEGIES: Dict[str, Strategy] = {
    "covered_call": Strategy(
        name="covered_call",
        display_name="Covered Call",
        option_right="CALL",
        ticker_config_key="covered_call_tickers",
        recs_file_suffix="cc_recs",
        recs_state_key="last_cc_recs_path",
        recommend_fn=build_cc_recommendations,
        combine_fn=build_calls_combined,
        display_fn=_build_calls_display,
        columns=_CALL_COLUMNS,
        has_monthly=True,
    ),
    "cash_secured_put": Strategy(
        name="cash_secured_put",
        display_name="Cash-Secured Put",
        option_right="PUT",
        ticker_config_key="cash_secured_put_tickers",
        recs_file_suffix="csp_recs",
        recs_state_key="last_csp_recs_path",
        recommend_fn=build_csp_recommendations,
        combine_fn=build_puts_combined,
        display_fn=_build_puts_display,
        columns=_PUT_COLUMNS,
        has_monthly=False,
    ),
}
