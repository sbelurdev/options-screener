from datetime import date, timedelta

import pandas as pd
import pytest

from agent.signals.iv_history import compute_true_iv_rank, estimate_atm_iv, record_iv_snapshot
from agent.signals.technicals import compute_technicals
from agent.tracking.outcomes import evaluate_outcomes, record_recommendations

TODAY = date.today()


# ── IV history ───────────────────────────────────────────────────────────────

def test_estimate_atm_iv_nearest_strike():
    calls = pd.DataFrame({"strike": [95, 100, 105], "impliedVolatility": [0.50, 0.45, 0.42]})
    puts = pd.DataFrame({"strike": [90, 100, 110], "impliedVolatility": [0.55, 0.47, 0.40]})
    assert estimate_atm_iv(calls, puts, 101.0) == pytest.approx(0.46)


def test_estimate_atm_iv_guards():
    calls = pd.DataFrame({"strike": [100], "impliedVolatility": [0.5]})
    assert estimate_atm_iv(calls, None, float("nan")) is None
    assert estimate_atm_iv(calls, None, 0.0) is None
    assert estimate_atm_iv(None, None, 100.0) is None
    all_nan = pd.DataFrame({"strike": [None], "impliedVolatility": [None]})
    assert estimate_atm_iv(all_nan, all_nan, 100.0) is None


def test_iv_rank_needs_min_history(tmp_path, logger):
    path = tmp_path / "iv.csv"
    record_iv_snapshot(path, "T", TODAY, 0.46, 30, 100.0, logger)
    rank, source = compute_true_iv_rank(path, "T", 0.46, min_observations=20)
    assert rank is None
    assert "1/20" in source


def test_iv_rank_with_history(tmp_path, logger):
    path = tmp_path / "iv.csv"
    for i in range(25):
        record_iv_snapshot(path, "T", TODAY - timedelta(days=i + 1), 0.30 + 0.30 * i / 24, 30, 100.0, logger)
    rank, source = compute_true_iv_rank(path, "T", 0.45, min_observations=20)
    assert rank == pytest.approx((0.45 - 0.30) / 0.30 * 100, abs=0.1)
    assert "true IV rank" in source


def test_iv_snapshot_same_day_upsert(tmp_path, logger):
    path = tmp_path / "iv.csv"
    record_iv_snapshot(path, "T", TODAY, 0.40, 30, 100.0, logger)
    record_iv_snapshot(path, "T", TODAY, 0.50, 28, 101.0, logger)
    df = pd.read_csv(path)
    assert len(df) == 1
    assert df.iloc[0]["atm_iv"] == pytest.approx(0.50)


# ── technicals NaN handling ──────────────────────────────────────────────────

def test_technicals_skips_nan_current_session():
    closes = [100.0] * 60 + [float("nan")]
    df = pd.DataFrame({"Close": closes})
    t = compute_technicals(df)
    assert t["spot"] == pytest.approx(100.0)


def test_technicals_all_nan_returns_spot_zero():
    df = pd.DataFrame({"Close": [float("nan")] * 5})
    assert compute_technicals(df)["spot"] == 0.0


# ── outcome tracking ─────────────────────────────────────────────────────────

class FakeMarket:
    def __init__(self, close):
        self.close = close

    def get_price_history(self, ticker, period="1y", interval="1d"):
        idx = pd.bdate_range(end=TODAY, periods=200)
        return pd.DataFrame({"Close": [self.close] * 200}, index=idx)


def _recs(expiration, strategy="CSP", strike=95.0, premium=1.2):
    return [{"ticker": "T", "term": "Short-Term", "recommend": "Yes",
             "expiration": expiration, "strike": strike, "premium": premium,
             "delta": -0.15, "dte": 7, "annualized_yield": 0.35, "spot": 100.0}]


def test_record_skips_placeholder_rows(tmp_path, logger):
    n = record_recommendations(tmp_path / "o.csv", TODAY,
                               [{"ticker": "T", "strike": None, "expiration": None}], [], logger)
    assert n == 0


def test_record_same_day_upsert(tmp_path, logger):
    path = tmp_path / "o.csv"
    exp = (TODAY + timedelta(days=5)).isoformat()
    record_recommendations(path, TODAY, [], _recs(exp), logger)
    record_recommendations(path, TODAY, [], _recs(exp), logger)
    assert len(pd.read_csv(path)) == 1


def test_csp_expired_otm(tmp_path, logger):
    path = tmp_path / "o.csv"
    exp = (TODAY - timedelta(days=3)).isoformat()
    record_recommendations(path, TODAY, [], _recs(exp), logger)
    summary = evaluate_outcomes(path, FakeMarket(close=100.0), logger)
    row = pd.read_csv(path).iloc[0]
    assert row["outcome"] == "expired_otm"
    assert row["option_pnl"] == pytest.approx(120.0)
    assert summary["win_rate"] == 1.0


def test_csp_assigned(tmp_path, logger):
    path = tmp_path / "o.csv"
    exp = (TODAY - timedelta(days=3)).isoformat()
    record_recommendations(path, TODAY, [], _recs(exp), logger)
    evaluate_outcomes(path, FakeMarket(close=90.0), logger)
    row = pd.read_csv(path).iloc[0]
    assert row["outcome"] == "assigned"
    assert row["option_pnl"] == pytest.approx((90.0 - 95.0 + 1.2) * 100)


def test_cc_called_away(tmp_path, logger):
    path = tmp_path / "o.csv"
    exp = (TODAY - timedelta(days=3)).isoformat()
    cc = [{"ticker": "T", "term": "Short-Term", "recommend": "Yes", "expiration": exp,
           "strike": 105.0, "premium": 1.4, "delta": 0.2, "dte": 7,
           "annualized_yield": 0.3, "spot": 100.0}]
    record_recommendations(path, TODAY, cc, [], logger)
    evaluate_outcomes(path, FakeMarket(close=110.0), logger)
    row = pd.read_csv(path).iloc[0]
    assert row["outcome"] == "called_away"
    assert row["option_pnl"] == pytest.approx((1.4 - 5.0) * 100)


def test_future_expirations_stay_open(tmp_path, logger):
    path = tmp_path / "o.csv"
    exp = (TODAY + timedelta(days=5)).isoformat()
    record_recommendations(path, TODAY, [], _recs(exp), logger)
    summary = evaluate_outcomes(path, FakeMarket(close=100.0), logger)
    assert summary["open"] == 1
    assert summary["closed"] == 0
