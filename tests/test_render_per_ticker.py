from datetime import date, timedelta

from agent.reporting.render import write_per_ticker_reports, write_reports

TODAY = date.today()
EXP = (TODAY + timedelta(days=10)).isoformat()

DISCLAIMER = "Educational only."


def _candidate(ticker, strategy, **overrides):
    base = {
        "ticker": ticker,
        "bucket": "short",
        "bucket_label": "Short-Term",
        "strategy": strategy,
        "expiration": EXP,
        "spot": 100.0,
        "strike": 95.0 if strategy == "PUT" else 105.0,
        "mid": 1.5,
        "fill_price": 1.4,
        "dte": 10,
        "annualized_yield": 0.4,
        "otm_pct": 0.05,
        "delta": -0.15 if strategy == "PUT" else 0.15,
        "breakeven": 93.5,
        "score": 0.8,
        "why_ranked_high": "test",
    }
    base.update(overrides)
    return base


def _cc_rec(ticker, **overrides):
    base = {
        "ticker": ticker, "term": "Short-Term", "recommend": "Yes", "expiration": EXP,
        "strike": 105.0, "premium": 1.4, "delta": 0.15, "dte": 10,
        "annualized_yield": 0.3, "spot": 100.0, "reason": "test",
    }
    base.update(overrides)
    return base


def _csp_rec(ticker, **overrides):
    base = {
        "ticker": ticker, "term": "Short-Term", "recommend": "Yes", "expiration": EXP,
        "strike": 95.0, "premium": 1.4, "delta": -0.15, "dte": 10,
        "annualized_yield": 0.3, "spot": 100.0, "reason": "test",
    }
    base.update(overrides)
    return base


def test_write_per_ticker_reports_creates_one_file_per_ticker_and_strategy(tmp_path):
    config = {"output_dir": str(tmp_path), "active_profile": "prasanna"}
    candidates = [
        _candidate("MSFT", "CALL"),
        _candidate("NVDA", "CALL"),
        _candidate("TQQQ", "PUT"),
    ]
    cc_recs = [_cc_rec("MSFT"), _cc_rec("NVDA")]
    csp_recs = [_csp_rec("TQQQ")]

    written = write_per_ticker_reports(
        candidates, config, DISCLAIMER,
        csp_recommendations=csp_recs,
        cc_recommendations=cc_recs,
        cc_tickers=["MSFT", "NVDA"],
        csp_tickers=["TQQQ"],
    )

    assert (tmp_path / "MSFT-CALL.html").exists()
    assert (tmp_path / "NVDA-CALL.html").exists()
    assert (tmp_path / "TQQQ-CSP.html").exists()
    assert len(written) == 3
    assert not (tmp_path / "TQQQ-CALL.html").exists()


def test_per_ticker_file_only_contains_that_tickers_rows(tmp_path):
    config = {"output_dir": str(tmp_path), "active_profile": "prasanna"}
    candidates = [_candidate("MSFT", "CALL"), _candidate("NVDA", "CALL")]
    cc_recs = [_cc_rec("MSFT"), _cc_rec("NVDA")]

    write_per_ticker_reports(
        candidates, config, DISCLAIMER,
        cc_recommendations=cc_recs,
        cc_tickers=["MSFT", "NVDA"],
    )

    msft_html = (tmp_path / "MSFT-CALL.html").read_text(encoding="utf-8")
    assert "MSFT" in msft_html
    assert "NVDA" not in msft_html


def test_per_ticker_file_written_even_with_no_candidates_today(tmp_path):
    config = {"output_dir": str(tmp_path), "active_profile": "prasanna"}
    written = write_per_ticker_reports(
        [], config, DISCLAIMER,
        cc_tickers=["MSFT"],
        csp_tickers=["TQQQ"],
    )
    assert (tmp_path / "MSFT-CALL.html").exists()
    assert (tmp_path / "TQQQ-CSP.html").exists()
    assert "No candidates passed filters today." in (tmp_path / "MSFT-CALL.html").read_text(encoding="utf-8")


def test_per_ticker_call_page_uses_dashboard_table_format(tmp_path):
    config = {"output_dir": str(tmp_path), "active_profile": "prasanna"}
    candidates = [_candidate("MSFT", "CALL")]
    cc_recs = [_cc_rec("MSFT")]

    write_per_ticker_reports(
        candidates, config, DISCLAIMER,
        cc_recommendations=cc_recs,
        cc_tickers=["MSFT"],
    )

    html = (tmp_path / "MSFT-CALL.html").read_text(encoding="utf-8")
    # Same dark dashboard table as the Streamlit Calls/Puts tabs, not the old
    # light-theme collapsible-details layout.
    assert "class='ot-wrap'" in html
    assert "pe-badge pe-yes" in html  # matched recommendation -> YES badge
    assert "class='dot dg'" in html  # low-delta risk dot
    assert "· 1 contract" in html  # expiration group header
    assert "background:#0b1220" in html  # dark page background
    assert "Sell Call Candidates" not in html  # old section heading is gone


def test_per_ticker_put_page_uses_dashboard_table_format(tmp_path):
    config = {"output_dir": str(tmp_path), "active_profile": "prasanna"}
    candidates = [_candidate("TQQQ", "PUT")]
    csp_recs = [_csp_rec("TQQQ")]

    write_per_ticker_reports(
        candidates, config, DISCLAIMER,
        csp_recommendations=csp_recs,
        csp_tickers=["TQQQ"],
    )

    html = (tmp_path / "TQQQ-CSP.html").read_text(encoding="utf-8")
    assert "class='ot-wrap'" in html
    assert "pe-badge pe-yes" in html
    assert "%ToStrike" in html  # PUT-specific column header


def test_write_reports_still_produces_combined_csv_and_html(tmp_path):
    config = {"output_dir": str(tmp_path), "active_profile": "prasanna"}
    candidates = [_candidate("MSFT", "CALL"), _candidate("TQQQ", "PUT")]
    csv_path, html_path = write_reports(
        candidates, config, DISCLAIMER,
        csp_recommendations=[_csp_rec("TQQQ")],
        cc_recommendations=[_cc_rec("MSFT")],
    )
    assert (tmp_path / f"{TODAY.isoformat()}_options_report.csv").exists()
    html = (tmp_path / f"{TODAY.isoformat()}_options_report.html").read_text(encoding="utf-8")
    assert "MSFT" in html and "TQQQ" in html
    assert "Sell Call Candidates" in html
    assert "Sell Put Candidates" in html
