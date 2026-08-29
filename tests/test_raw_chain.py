from datetime import date, timedelta

import pandas as pd
import pytest

from agent.tracking.raw_chain import record_raw_chain

TODAY = date.today()
EXP = TODAY + timedelta(days=10)


def _chain_df():
    return pd.DataFrame(
        {
            "contractSymbol": ["T240101C00100000"],
            "strike": [100.0],
            "bid": [1.2],
            "ask": [1.4],
            "lastPrice": [1.3],
            "impliedVolatility": [0.45],
        }
    )


def test_record_raw_chain_writes_one_file_per_ticker_and_strategy(tmp_path, logger):
    record_raw_chain(tmp_path, "AAPL", "CALL", TODAY, EXP, _chain_df(), logger)
    record_raw_chain(tmp_path, "AAPL", "PUT", TODAY, EXP, _chain_df(), logger)

    assert (tmp_path / "AAPL_CALL.csv").exists()
    assert (tmp_path / "AAPL_PUT.csv").exists()

    df = pd.read_csv(tmp_path / "AAPL_CALL.csv")
    assert list(df.columns[:2]) == ["run_date", "expiration"]
    assert df.iloc[0]["strike"] == 100.0


def test_record_raw_chain_skips_empty_df(tmp_path, logger):
    record_raw_chain(tmp_path, "AAPL", "CALL", TODAY, EXP, pd.DataFrame(), logger)
    record_raw_chain(tmp_path, "AAPL", "CALL", TODAY, EXP, None, logger)
    assert not (tmp_path / "AAPL_CALL.csv").exists()


def test_record_raw_chain_same_day_same_expiry_replaces(tmp_path, logger):
    df1 = _chain_df()
    df2 = _chain_df()
    df2["bid"] = [9.9]

    record_raw_chain(tmp_path, "AAPL", "CALL", TODAY, EXP, df1, logger)
    record_raw_chain(tmp_path, "AAPL", "CALL", TODAY, EXP, df2, logger)

    df = pd.read_csv(tmp_path / "AAPL_CALL.csv")
    assert len(df) == 1
    assert df.iloc[0]["bid"] == pytest.approx(9.9)


def test_record_raw_chain_accumulates_across_expirations(tmp_path, logger):
    record_raw_chain(tmp_path, "AAPL", "CALL", TODAY, EXP, _chain_df(), logger)
    record_raw_chain(tmp_path, "AAPL", "CALL", TODAY, EXP + timedelta(days=7), _chain_df(), logger)

    df = pd.read_csv(tmp_path / "AAPL_CALL.csv")
    assert len(df) == 2
    assert set(df["expiration"]) == {EXP.isoformat(), (EXP + timedelta(days=7)).isoformat()}
