import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def logger():
    return logging.getLogger("tests")


@pytest.fixture
def technicals():
    return {"spot": 100.0, "ma20": 95.0, "ma50": 90.0, "rsi14": 55.0, "hv20": 0.45}


@pytest.fixture
def config():
    return {
        "min_open_interest": None,
        "min_volume": None,
        "max_spread_pct": None,
        "min_annualized_yield": 0.12,
        "risk_free_rate": 0.05,
        "fill_price_factor": 0.4,
        "delta_put_min": -0.25,
        "delta_put_max": -0.10,
        "delta_call_min": 0.10,
        "delta_call_max": 0.25,
        "put_otm_pct_min": 0.05,
        "put_otm_pct_max": 0.15,
        "call_otm_pct_min": 0.05,
        "call_otm_pct_max": 0.15,
        "earnings_risk_penalty": 0.20,
    }


@pytest.fixture
def price_df():
    rng = np.random.default_rng(0)
    n = 250
    return pd.DataFrame(
        {
            "Close": np.linspace(80, 100, n) + rng.normal(0, 1.5, n),
            "Low": np.linspace(78, 98, n),
            "High": np.linspace(82, 102, n),
        },
        index=pd.bdate_range(end=date.today(), periods=n),
    )


def make_candidate(**overrides):
    """A PUT candidate that passes default CSP recommendation criteria."""
    base = {
        "strategy": "PUT",
        "ticker": "TEST",
        "expiration": (date.today() + timedelta(days=10)).isoformat(),
        "score": 0.8,
        "spot": 100.0,
        "strike": 85.0,
        "delta": -0.15,
        "mid": 1.5,
        "fill_price": 1.4,
        "implied_volatility": 0.5,
        "dte": 10,
        "annualized_yield": 0.4,
        "spread_pct": 0.05,
        "open_interest": 500,
        "volume": 100,
        "earnings_before_expiry": False,
    }
    base.update(overrides)
    return base
