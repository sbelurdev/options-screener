from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from agent.providers.yfinance_provider import YFinanceProvider
from agent.reporting.render import write_reports
from agent.scoring.score import score_candidate
from agent.signals.options_metrics import build_option_records, select_expiration_buckets
from agent.signals.technicals import compute_technicals

DEFAULT_CONFIG: Dict[str, Any] = {
    "covered_call_tickers": ["SPY", "QQQ", "MSFT", "AAPL"],
    "cash_secured_put_tickers": ["SPY", "QQQ", "MSFT", "AAPL"],
    "max_candidates_per_ticker_per_bucket": 5,
    "delta_put_min": -0.35,
    "delta_put_max": -0.15,
    "delta_call_min": 0.15,
    "delta_call_max": 0.35,
    "dte_current_week_max_days": 7,
    "dte_next_week_min_days": 8,
    "dte_next_week_max_days": 14,
    "monthly_target_dte_min": 30,
    "monthly_target_dte_max": 45,
    "min_open_interest": None,
    "min_volume": None,
    "max_spread_pct": None,
    "min_annualized_yield": 0.12,
    "risk_free_rate": None,
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
}

DISCLAIMER = (
    "Educational screening only - not financial advice. No guaranteed returns. "
    "Options involve assignment risk, gap risk, earnings/event risk, liquidity risk, and tail risk."
)


def _process_ticker(ticker: str, provider: YFinanceProvider, config: Dict[str, Any], logger, strategies: List[str]) -> Dict[str, Any]:
    ticker_result: Dict[str, Any] = {"ticker": ticker, "buckets": {}, "candidates": []}

    hist = provider.get_price_history(
        ticker,
        period=config["price_history_period"],
        interval=config["price_history_interval"],
    )
    if hist.empty:
        logger.warning("%s: no price history, skipping", ticker)
        return ticker_result

    technicals = compute_technicals(hist)
    spot = float(technicals["spot"])

    expirations = provider.get_options_expirations(ticker)
    buckets = select_expiration_buckets(expirations, date.today(), config, logger)
    ticker_result["buckets"] = buckets

    earnings_date = provider.get_earnings_date(ticker)
    if earnings_date is None:
        logger.info("%s: earnings date unavailable", ticker)

    for bucket_name, bucket_meta in buckets.items():
        expiry = bucket_meta.get("expiration")
        if expiry is None:
            logger.warning("%s: bucket=%s has no expiration", ticker, bucket_name)
            continue

        calls_df, puts_df = provider.get_options_chain(ticker, expiry)

        put_candidates = (
            build_option_records(
                ticker=ticker,
                strategy="PUT",
                options_df=puts_df,
                expiration=expiry,
                bucket_name=bucket_name,
                bucket_label=bucket_meta.get("label", bucket_name),
                spot=spot,
                technicals=technicals,
                earnings_date=earnings_date,
                config=config,
                logger=logger,
                decision_logger=lambda row: provider.log_option_screen_result(ticker, row),
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
                bucket_label=bucket_meta.get("label", bucket_name),
                spot=spot,
                technicals=technicals,
                earnings_date=earnings_date,
                config=config,
                logger=logger,
                decision_logger=lambda row: provider.log_option_screen_result(ticker, row),
            )
            if "CALL" in strategies
            else []
        )

        all_bucket = put_candidates + call_candidates
        for row in all_bucket:
            score, why = score_candidate(row, technicals, config)
            row["score"] = round(score, 4)
            row["why_ranked_high"] = why

        max_n = int(config["max_candidates_per_ticker_per_bucket"])
        top_puts = sorted(put_candidates, key=lambda x: x.get("score", 0.0), reverse=True)[:max_n]
        top_calls = sorted(call_candidates, key=lambda x: x.get("score", 0.0), reverse=True)[:max_n]

        ticker_result["candidates"].extend(top_puts)
        ticker_result["candidates"].extend(top_calls)

        logger.info(
            "%s bucket=%s expiration=%s puts=%d calls=%d",
            ticker,
            bucket_name,
            expiry.isoformat(),
            len(top_puts),
            len(top_calls),
        )

    return ticker_result


def run_pipeline(config: Dict[str, Any], logger) -> None:
    Path(config["output_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["log_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["cache_dir"]).mkdir(parents=True, exist_ok=True)

    provider = YFinanceProvider(logger=logger, log_dir=config["log_dir"])

    cc_tickers: List[str] = [str(t).upper() for t in config.get("covered_call_tickers", [])]
    csp_tickers: List[str] = [str(t).upper() for t in config.get("cash_secured_put_tickers", [])]

    ticker_strategies: Dict[str, List[str]] = {}
    for t in cc_tickers:
        ticker_strategies.setdefault(t, []).append("CALL")
    for t in csp_tickers:
        ticker_strategies.setdefault(t, []).append("PUT")
    all_tickers = list(ticker_strategies.keys())

    all_candidates: List[Dict[str, Any]] = []
    bucket_selection_summary: Dict[str, Dict[str, Any]] = {}

    logger.info("Starting options screener for tickers=%s", ",".join(all_tickers))

    for ticker, strategies in ticker_strategies.items():
        try:
            result = _process_ticker(ticker, provider, config, logger, strategies)
            bucket_selection_summary[ticker] = result.get("buckets", {})
            all_candidates.extend(result.get("candidates", []))
        except Exception as exc:
            logger.exception("Failed processing %s: %s", ticker, exc)
            continue

    csv_path, html_path = write_reports(all_candidates, config, DISCLAIMER)

    print("=" * 72)
    print("Options Screener Summary")
    print("=" * 72)
    print(f"Tickers processed: {', '.join(all_tickers)}")
    print("Selected expirations by ticker:")
    for t in all_tickers:
        buckets = bucket_selection_summary.get(t, {})
        parts = []
        for bucket in ["current_week", "next_week", "monthly"]:
            meta = buckets.get(bucket, {})
            exp = meta.get("expiration")
            label = meta.get("label", bucket)
            exp_str = exp.isoformat() if exp else "N/A"
            parts.append(f"{label}: {exp_str}")
        print(f"  - {t}: " + " | ".join(parts))

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

    print(f"\nCovered call tickers:      {', '.join(cc_tickers) or 'none'}")
    print(f"Cash-secured put tickers:  {', '.join(csp_tickers) or 'none'}")
    print(f"CSV report:  {csv_path}")
    print(f"HTML report: {html_path}")
    print("\nRisk warning:")
    print(DISCLAIMER)

    logger.info("Run completed. CSV=%s HTML=%s candidates=%d", csv_path, html_path, len(all_candidates))
