from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from agent.providers.base import FundamentalsProvider, MarketDataProvider, OptionsChainProvider
from agent.providers.factory import build_fundamentals_provider, build_market_provider, build_options_provider
from agent.recommendation.cc_recommender import build_cc_recommendations
from agent.recommendation.csp_recommender import build_csp_recommendations, compute_ivr_proxy

from agent.reporting.render import write_reports
from agent.scoring.score import score_candidate
from agent.signals.options_metrics import (
    build_option_records,
    get_dte,
    get_term_for_dte,
    select_expiration_dates,
)
from agent.signals.technicals import compute_technicals

DEFAULT_CONFIG: Dict[str, Any] = {
    "covered_call_tickers": ["SPY", "QQQ", "MSFT", "AAPL"],
    "cash_secured_put_tickers": ["SPY", "QQQ", "MSFT", "AAPL"],
    "max_candidates_per_ticker_per_bucket": 5,
    "delta_put_min": -0.35,
    "delta_put_max": -0.15,
    "delta_call_min": 0.15,
    "delta_call_max": 0.35,
    # DTE term boundaries (used for both expiration selection and candidate tagging)
    "max_dte": 45,             # hard cap — no expirations beyond this
    "short_term_max_dte": 14,  # DTE ≤ 14 → Short-Term  (all expirations fetched)
    "medium_term_max_dte": 28, # DTE ≤ 28 → Medium-Term (Fridays only beyond 14)
    "min_open_interest": None,
    "min_volume": None,
    "max_spread_pct": None,
    "html_min_mid_price": 0.5,
    "options_data_provider": "yfinance",
    "market_data_provider": "yfinance",
    "fundamentals_provider": "yfinance",
    "public_api_base_url": "https://api.public.com",
    "public_api_key_env_var": "PUBLIC_API_KEY",
    "public_access_token_validity_minutes": 15,
    "public_http_timeout_seconds": 20,
    "public_account_id": None,
    "public_underlying_instrument_type": "EQUITY",
    "min_annualized_yield": 0.12,
    "risk_free_rate": 0.05,
    "put_otm_pct_min": 0.05,
    "put_otm_pct_max": 0.15,
    "call_otm_pct_min": 0.05,
    "call_otm_pct_max": 0.15,
    "earnings_risk_penalty": 0.20,
    "output_dir": "./reports",
    "log_dir": "./logs",
    "cache_dir": "./cache",
    "price_history_period": "6mo",
    "price_history_interval": "1d",
    "cc_recommendation": {
        "enabled": True,
        "max_recommendations": 50,
        "max_suggestions_per_term": 3,
        "earnings_buffer_days": 7,
        "delta_min": 0.10,
        "delta_max": 0.25,
        "use_resistance_filter": True,
        "resistance_pct_buffer": 0.02,
        "min_acceptable_sale_prices": {},
    },
    "csp_recommendation": {
        "enabled": True,
        "max_recommendations": 30,
        "ivr_min": 30.0,
        "earnings_buffer_days": 7,
        "delta_min": 0.10,
        "delta_max": 0.25,
        "use_support_filter": True,
        "support_pct_buffer": 0.02,
    },
}

DISCLAIMER = (
    "Educational screening only - not financial advice. No guaranteed returns. "
    "Options involve assignment risk, gap risk, earnings/event risk, liquidity risk, and tail risk."
)


def _process_ticker(
    ticker: str,
    options_provider: OptionsChainProvider,
    market_provider: MarketDataProvider,
    fundamentals_provider: FundamentalsProvider,
    config: Dict[str, Any],
    logger,
    strategies: List[str],
) -> Dict[str, Any]:
    ticker_result: Dict[str, Any] = {"ticker": ticker, "selected_expirations": [], "candidates": []}

    hist = market_provider.get_price_history(
        ticker,
        period=config["price_history_period"],
        interval=config["price_history_interval"],
    )
    if hist.empty:
        logger.warning("%s: no price history, skipping", ticker)
        return ticker_result

    ticker_result["price_df"] = hist

    technicals = compute_technicals(hist)
    ticker_result["technicals"] = technicals
    spot = float(technicals["spot"])

    expirations = options_provider.get_options_expirations(ticker)
    max_dte = int(config.get("max_dte", 45))
    selected_dates = select_expiration_dates(expirations, date.today(), max_dte)
    ticker_result["selected_expirations"] = selected_dates

    earnings_date = fundamentals_provider.get_earnings_date(ticker)
    if earnings_date is None:
        logger.info("%s: earnings date unavailable", ticker)

    ticker_result["earnings_date"] = earnings_date

    max_n = int(config["max_candidates_per_ticker_per_bucket"])

    for expiry in selected_dates:
        dte_days = get_dte(expiry, date.today())
        bucket_name, bucket_label = get_term_for_dte(dte_days)

        calls_df, puts_df = options_provider.get_options_chain(ticker, expiry)

        put_candidates = (
            build_option_records(
                ticker=ticker,
                strategy="PUT",
                options_df=puts_df,
                expiration=expiry,
                bucket_name=bucket_name,
                bucket_label=bucket_label,
                spot=spot,
                technicals=technicals,
                earnings_date=earnings_date,
                config=config,
                logger=logger,
                decision_logger=lambda row: options_provider.log_option_screen_result(ticker, row),
            )
            if "PUT" in strategies
            else []
        )
        call_candidates = (
            build_option_records(
                ticker=ticker,
                strategy="CALL",
                options_df=calls_df,
                expiration=expiry,
                bucket_name=bucket_name,
                bucket_label=bucket_label,
                spot=spot,
                technicals=technicals,
                earnings_date=earnings_date,
                config=config,
                logger=logger,
                decision_logger=lambda row: options_provider.log_option_screen_result(ticker, row),
            )
            if "CALL" in strategies
            else []
        )

        all_expiry = put_candidates + call_candidates
        for row in all_expiry:
            score, why = score_candidate(row, technicals, config)
            row["score"] = round(score, 4)
            row["why_ranked_high"] = why

        top_puts = sorted(put_candidates, key=lambda x: x.get("score", 0.0), reverse=True)[:max_n]
        top_calls = sorted(call_candidates, key=lambda x: x.get("score", 0.0), reverse=True)[:max_n]

        ticker_result["candidates"].extend(top_puts)
        ticker_result["candidates"].extend(top_calls)

        logger.info(
            "%s term=%s expiration=%s puts=%d calls=%d",
            ticker,
            bucket_name,
            expiry.isoformat(),
            len(top_puts),
            len(top_calls),
        )

    # Attach ticker-level IVR (HV Rank) to every candidate for the detail table
    ticker_ivr, ticker_ivr_source = compute_ivr_proxy(hist, None)
    for c in ticker_result["candidates"]:
        c["ivr"] = ticker_ivr
        c["ivr_source"] = ticker_ivr_source

    return ticker_result


def validate_config(config: Dict[str, Any]) -> None:
    """Raise ValueError for config values that would cause crashes or nonsensical results."""
    penalty = float(config.get("earnings_risk_penalty", 0))
    if not 0.0 <= penalty < 1.0:
        raise ValueError(f"earnings_risk_penalty must be in [0, 1); got {penalty}")

    min_yield = float(config.get("min_annualized_yield", 0))
    if min_yield < 0:
        raise ValueError(f"min_annualized_yield must be >= 0; got {min_yield}")

    max_cand = int(config.get("max_candidates_per_ticker_per_bucket", 1))
    if max_cand < 1:
        raise ValueError(f"max_candidates_per_ticker_per_bucket must be >= 1; got {max_cand}")

    if float(config.get("delta_put_min", -1)) > float(config.get("delta_put_max", 0)):
        raise ValueError("delta_put_min must be <= delta_put_max")
    if float(config.get("delta_call_min", 0)) > float(config.get("delta_call_max", 1)):
        raise ValueError("delta_call_min must be <= delta_call_max")
    if float(config.get("put_otm_pct_min", 0)) > float(config.get("put_otm_pct_max", 1)):
        raise ValueError("put_otm_pct_min must be <= put_otm_pct_max")
    if float(config.get("call_otm_pct_min", 0)) > float(config.get("call_otm_pct_max", 1)):
        raise ValueError("call_otm_pct_min must be <= call_otm_pct_max")

    max_dte = int(config.get("max_dte", 45))
    if max_dte < 7:
        raise ValueError(f"max_dte must be >= 7; got {max_dte}")


def run_pipeline(config: Dict[str, Any], logger) -> None:
    validate_config(config)
    Path(config["output_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["log_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["cache_dir"]).mkdir(parents=True, exist_ok=True)

    options_provider = build_options_provider(config, logger)
    market_provider = build_market_provider(config, logger)
    fundamentals_provider = build_fundamentals_provider(config, logger)

    cc_tickers: List[str] = [str(t).upper() for t in config.get("covered_call_tickers", [])]
    csp_tickers: List[str] = [str(t).upper() for t in config.get("cash_secured_put_tickers", [])]

    ticker_strategies: Dict[str, List[str]] = {}
    for t in cc_tickers:
        ticker_strategies.setdefault(t, []).append("CALL")
    for t in csp_tickers:
        ticker_strategies.setdefault(t, []).append("PUT")
    all_tickers = list(ticker_strategies.keys())

    all_candidates: List[Dict[str, Any]] = []
    expiration_summary: Dict[str, List[date]] = {}
    ticker_results_map: Dict[str, Dict[str, Any]] = {}

    logger.info(
        "Providers selected options=%s market=%s fundamentals=%s",
        str(config.get("options_data_provider", "yfinance")).lower(),
        str(config.get("market_data_provider", "yfinance")).lower(),
        str(config.get("fundamentals_provider", "yfinance")).lower(),
    )
    logger.info("Starting options screener for tickers=%s", ",".join(all_tickers))

    for ticker, strategies in ticker_strategies.items():
        try:
            result = _process_ticker(
                ticker,
                options_provider=options_provider,
                market_provider=market_provider,
                fundamentals_provider=fundamentals_provider,
                config=config,
                logger=logger,
                strategies=strategies,
            )
            expiration_summary[ticker] = result.get("selected_expirations", [])
            all_candidates.extend(result.get("candidates", []))
            ticker_results_map[ticker] = result
        except Exception as exc:
            logger.exception("Failed processing %s: %s", ticker, exc)
            continue

    cc_recommendations = build_cc_recommendations(ticker_results_map, cc_tickers, config)
    csp_recommendations = build_csp_recommendations(ticker_results_map, csp_tickers, config)

    fallback_events = getattr(options_provider, "fallback_events", [])
    csv_path, html_path = write_reports(
        all_candidates, config, DISCLAIMER,
        csp_recommendations=csp_recommendations,
        cc_recommendations=cc_recommendations,
        fallback_events=fallback_events,
    )

    print("=" * 72)
    print("Options Screener Summary")
    print("=" * 72)
    print(f"Tickers processed: {', '.join(all_tickers)}")
    print("Selected expirations by ticker:")
    for t in all_tickers:
        dates = expiration_summary.get(t, [])
        if dates:
            dates_str = "  ".join(d.isoformat() for d in dates)
        else:
            dates_str = "none"
        print(f"  - {t}: {dates_str}")

    df = pd.DataFrame(all_candidates)
    if not df.empty:
        grouped = df.groupby(["ticker", "bucket", "strategy"]).size().reset_index(name="count")
        print("Candidate counts (post-filter, post-ranking):")
        for _, row in grouped.iterrows():
            print(f"  - {row['ticker']} {row['bucket']} {row['strategy']}: {int(row['count'])}")
        top3 = df.sort_values("score", ascending=False).head(3)
        print("Top 3 highlights:")
        for _, row in top3.iterrows():
            print(
                f"  - {row['ticker']} {row['strategy']} {row['bucket_label']} "
                f"strike={row['strike']:.2f} exp={row['expiration']} "
                f"yield={row['annualized_yield']:.2%} score={row['score']:.3f}"
            )
    else:
        print("No candidates passed filters today.")

    if cc_recommendations:
        print("\nCovered Call Recommendations:")
        for rec in cc_recommendations:
            verdict = rec["recommend"]
            strike = f"${rec['strike']:.2f}" if rec["strike"] else "—"
            ivr = f"{rec['ivr']:.0f}%" if rec["ivr"] is not None else "n/a"
            print(f"  {rec['ticker']:6s}  {rec['term']:12s}  {verdict:10s}  strike={strike}  IVR={ivr}  {rec['reason']}")

    if csp_recommendations:
        print("\nCSP Recommendations:")
        for rec in csp_recommendations:
            verdict = rec["recommend"]
            strike = f"${rec['strike']:.2f}" if rec["strike"] else "—"
            ivr = f"{rec['ivr']:.0f}%" if rec["ivr"] is not None else "n/a"
            print(f"  {rec['ticker']:6s}  {rec.get('term',''):12s}  {verdict:10s}  strike={strike}  IVR={ivr}  {rec['reason']}")

    print(f"\nCovered call tickers:      {', '.join(cc_tickers) or 'none'}")
    print(f"Cash-secured put tickers:  {', '.join(csp_tickers) or 'none'}")
    print(f"CSV report:  {csv_path}")
    print(f"HTML report: {html_path}")
    print("\nRisk warning:")
    print(DISCLAIMER)

    logger.info("Run completed. CSV=%s HTML=%s candidates=%d", csv_path, html_path, len(all_candidates))
