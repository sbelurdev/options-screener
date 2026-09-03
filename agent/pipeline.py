from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from agent.providers.base import FundamentalsProvider, MarketDataProvider, OptionsChainProvider
from agent.providers.factory import build_fundamentals_provider, build_market_provider, build_options_provider
from agent.notify.email_report import send_report_email
from agent.recommendation.cc_recommender import build_cc_recommendations
from agent.recommendation.context import build_context_store
from agent.recommendation.csp_recommender import build_csp_recommendations, compute_ivr_proxy

from agent.reporting.render import write_per_ticker_reports, write_reports
from agent.scoring.score import score_candidates
from agent.signals.options_metrics import (
    build_option_records,
    get_dte,
    get_term_for_dte,
    select_expiration_dates,
    select_monthly_cc_expiration_dates,
)
from agent.signals.iv_history import compute_true_iv_rank, estimate_atm_iv, record_iv_snapshot
from agent.signals.technicals import compute_technicals
from agent.tracking.outcomes import evaluate_outcomes, record_recommendations
from agent.tracking.raw_chain import record_raw_chain

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
    "fill_price_factor": 0.5,  # expected fill = bid + factor × (ask − bid); 0.5 == mid
    "fetch_max_workers": 4,    # tickers processed concurrently (1 = sequential)
    "scoring": {
        # Weights are normalized by their sum; see agent/scoring/score.py
        "weights": {
            "income": 0.35,
            "delta": 0.20,
            "trend": 0.15,
            "liquidity": 0.10,
            "vrp": 0.10,
            "theta": 0.10,
        },
        "delta_target": 0.20,
        "income_yield_cap": 1.5,
    },

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
    # Shared across profiles so IV history accumulates from every scheduled run
    "iv_history_path": "./cache/iv_history.csv",
    # Raw provider chain snapshots — one CSV per ticker+strategy, e.g. AAPL_CALL.csv
    "raw_chain_dir": "./cache/raw_chains",
    "iv_rank_min_history_days": 20,  # observations before true IV Rank replaces the HV proxy
    "outcome_tracking": {
        "enabled": True,
        "path": "./cache/outcomes.csv",  # ledger of recommendations graded after expiry
    },
    # Shared sending account for report emails; every profile sends "from" this
    # mailbox. Per-profile opt-in / recipient is `notify_email` (unset = no
    # email for that profile). Credentials come from env vars, not this file.
    "email": {
        "enabled": True,
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "from_address": "Prasanna.Kudli@gmail.com",
        "smtp_user_env_var": "SMTP_USER",
        "smtp_password_env_var": "SMTP_PASSWORD",
    },
    "notify_email": None,  # set per-profile in config/users/<profile>.yaml to receive reports by email
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
        "min_strike_prices": {},
        "max_strike_prices": {},
        "min_yield": 0.10,
        "long_term_months": 9,
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

    # Resolve per-ticker CC strike bounds once — pre-filter the raw chain
    # before any per-row computation (delta, BS, yield) to avoid wasted work.
    _cc_rec_cfg: Dict[str, Any] = config.get("cc_recommendation", {})
    _cc_min_strikes: Dict[str, Any] = _cc_rec_cfg.get("min_strike_prices") or {}
    cc_min_strike = _cc_min_strikes.get(ticker) or _cc_min_strikes.get(ticker.upper())
    if cc_min_strike is not None:
        cc_min_strike = float(cc_min_strike)
        logger.info("%s: CC min strike filter = %.2f", ticker, cc_min_strike)

    _cc_max_strikes: Dict[str, Any] = _cc_rec_cfg.get("max_strike_prices") or {}
    cc_max_strike = _cc_max_strikes.get(ticker) or _cc_max_strikes.get(ticker.upper())
    if cc_max_strike is not None:
        cc_max_strike = float(cc_max_strike)
        logger.info("%s: CC max strike filter = %.2f", ticker, cc_max_strike)

    # cc_recommendation.min_yield, when set, IS the screening yield threshold for
    # CALLs — it overrides the global min_annualized_yield (which otherwise runs
    # first and silently starves the CC tables of lower-yield strikes).
    cc_min_yield: Optional[float] = None
    _raw_min_yield = _cc_rec_cfg.get("min_yield")
    call_config = config
    if _raw_min_yield is not None:
        cc_min_yield = float(_raw_min_yield)
        call_config = {**config, "min_annualized_yield": cc_min_yield}
        logger.info("%s: CC min yield filter = %.0f%% (overrides global screening threshold for calls)",
                    ticker, cc_min_yield * 100)

    raw_chain_dir = Path(str(config.get("raw_chain_dir") or "./cache/raw_chains"))

    # Best ATM IV observation for this run: (dte, iv) of the expiry nearest 30 DTE
    atm_iv_obs: Optional[Tuple[int, float]] = None

    # Records are collected per expiry but scored afterwards in one pool per
    # strategy, so the income percentile compares across all expirations.
    per_expiry: List[Tuple[date, List[Dict[str, Any]], List[Dict[str, Any]]]] = []

    for expiry in selected_dates:
        dte_days = get_dte(expiry, date.today())
        bucket_name, bucket_label = get_term_for_dte(dte_days)

        calls_df, puts_df = options_provider.get_options_chain(ticker, expiry)

        # Persist the untouched provider chain before any pre-filtering/scoring.
        try:
            record_raw_chain(raw_chain_dir, ticker, "CALL", date.today(), expiry, calls_df, logger)
            record_raw_chain(raw_chain_dir, ticker, "PUT", date.today(), expiry, puts_df, logger)
        except Exception as exc:
            logger.warning("%s %s: raw chain snapshot failed: %s", ticker, expiry.isoformat(), exc)

        # ATM IV snapshot — taken from the raw chain before any strike pre-filtering.
        # Auxiliary only: must never break screening for the ticker.
        try:
            est_iv = estimate_atm_iv(calls_df, puts_df, spot)
            if est_iv is not None and (atm_iv_obs is None or abs(dte_days - 30) < abs(atm_iv_obs[0] - 30)):
                atm_iv_obs = (dte_days, est_iv)
        except Exception as exc:
            logger.warning("%s %s: ATM IV estimate failed: %s", ticker, expiry.isoformat(), exc)

        # Pre-filter the calls DataFrame before any per-row computation.
        if calls_df is not None and not calls_df.empty and "strike" in calls_df.columns:
            strikes = calls_df["strike"].astype(float)
            if cc_min_strike is not None:
                calls_df = calls_df[strikes >= cc_min_strike]
                strikes = calls_df["strike"].astype(float)
            if cc_max_strike is not None:
                calls_df = calls_df[strikes <= cc_max_strike]

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
                config=call_config,
                logger=logger,
                decision_logger=lambda row: options_provider.log_option_screen_result(ticker, row),
            )
            if "CALL" in strategies
            else []
        )

        per_expiry.append((expiry, put_candidates, call_candidates))

    # Score each strategy's full pool (all expirations) so income percentiles
    # compare like with like, then keep the top N per expiry as before.
    score_candidates([p for _, ps, _ in per_expiry for p in ps], technicals, config)
    score_candidates([c for _, _, cs in per_expiry for c in cs], technicals, config)

    for expiry, put_candidates, call_candidates in per_expiry:
        top_puts = sorted(put_candidates, key=lambda x: x.get("score", 0.0), reverse=True)[:max_n]
        top_calls = sorted(call_candidates, key=lambda x: x.get("score", 0.0), reverse=True)[:max_n]

        ticker_result["candidates"].extend(top_puts)
        ticker_result["candidates"].extend(top_calls)

        logger.info(
            "%s term=%s expiration=%s puts=%d calls=%d",
            ticker,
            get_term_for_dte(get_dte(expiry, date.today()))[0],
            expiry.isoformat(),
            len(top_puts),
            len(top_calls),
        )

    # Fetch monthly CALL chains beyond max_dte for long-term CC analysis.
    # Stored separately so they don't pollute the short/medium/long detail tables.
    long_term_months = int(config.get("cc_recommendation", {}).get("long_term_months", 0))
    monthly_call_candidates: List[Dict[str, Any]] = []
    if "CALL" in strategies and long_term_months > 0:
        monthly_dates = select_monthly_cc_expiration_dates(
            expirations, date.today(), max_dte, long_term_months
        )
        logger.info("%s: monthly CC expirations (%d months): %s", ticker, long_term_months,
                    ", ".join(d.isoformat() for d in monthly_dates) or "none")
        for expiry in monthly_dates:
            calls_df, _ = options_provider.get_options_chain(ticker, expiry)
            try:
                record_raw_chain(raw_chain_dir, ticker, "CALL", date.today(), expiry, calls_df, logger)
            except Exception as exc:
                logger.warning("%s %s: raw chain snapshot failed: %s", ticker, expiry.isoformat(), exc)
            if calls_df is not None and not calls_df.empty and "strike" in calls_df.columns:
                strikes = calls_df["strike"].astype(float)
                if cc_min_strike is not None:
                    calls_df = calls_df[strikes >= cc_min_strike]
                    strikes = calls_df["strike"].astype(float)
                if cc_max_strike is not None:
                    calls_df = calls_df[strikes <= cc_max_strike]
            month_label = expiry.strftime("%b %Y")
            month_candidates = build_option_records(
                ticker=ticker,
                strategy="CALL",
                options_df=calls_df,
                expiration=expiry,
                bucket_name="monthly",
                bucket_label=f"Monthly - {month_label}",
                spot=spot,
                technicals=technicals,
                earnings_date=earnings_date,
                config=call_config,
                logger=logger,
                decision_logger=lambda row: options_provider.log_option_screen_result(ticker, row),
            )
            monthly_call_candidates.extend(month_candidates)
            logger.info("%s monthly expiry=%s candidates=%d", ticker, expiry.isoformat(), len(month_candidates))
        # One scoring pool across all monthly expirations
        score_candidates(monthly_call_candidates, technicals, config)
    ticker_result["monthly_call_candidates"] = monthly_call_candidates

    # Persist today's ATM IV observation and compute a true IV Rank once enough
    # history has accumulated; until then recommenders fall back to the HV proxy.
    iv_rank: Tuple[Optional[float], str] = (None, "no ATM IV observation this run")
    if atm_iv_obs is not None:
        iv_hist_path = Path(str(config.get("iv_history_path") or "./cache/iv_history.csv"))
        try:
            record_iv_snapshot(iv_hist_path, ticker, date.today(), atm_iv_obs[1], atm_iv_obs[0], spot, logger)
            iv_rank = compute_true_iv_rank(
                iv_hist_path,
                ticker,
                atm_iv_obs[1],
                min_observations=int(config.get("iv_rank_min_history_days", 20)),
            )
        except Exception as exc:
            logger.warning("%s: IV history update failed: %s", ticker, exc)
    ticker_result["iv_rank"] = iv_rank
    ticker_result["atm_iv"] = atm_iv_obs[1] if atm_iv_obs is not None else None

    # Attach ticker-level IVR to every candidate for the detail table:
    # true IV Rank when available, otherwise the HV-rank proxy.
    if iv_rank[0] is not None:
        ticker_ivr, ticker_ivr_source = iv_rank
    else:
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
    all_monthly_call_candidates: List[Dict[str, Any]] = []
    expiration_summary: Dict[str, List[date]] = {}
    ticker_results_map: Dict[str, Dict[str, Any]] = {}

    profile = str(config.get("active_profile") or "").strip()
    profile_label = f"  Profile       : {profile}" if profile else ""
    print(
        f"\n{'=' * 52}\n"
        f"  Options Screener  —  {date.today()}\n"
        + (f"{profile_label}\n" if profile_label else "")
        + f"  Covered Calls : {', '.join(cc_tickers) or '(none)'}\n"
        f"  Cash-Sec Puts : {', '.join(csp_tickers) or '(none)'}\n"
        f"  Provider      : {str(config.get('options_data_provider', 'yfinance')).lower()}\n"
        f"{'=' * 52}\n"
    )
    logger.info("Starting options screener for tickers=%s", ",".join(all_tickers))

    def _safe_process(ticker: str, strategies: List[str]) -> Optional[Dict[str, Any]]:
        try:
            return _process_ticker(
                ticker,
                options_provider=options_provider,
                market_provider=market_provider,
                fundamentals_provider=fundamentals_provider,
                config=config,
                logger=logger,
                strategies=strategies,
            )
        except Exception as exc:
            logger.exception("Failed processing %s: %s", ticker, exc)
            return None

    # Tickers are I/O-bound (HTTP chains/history), so process them concurrently;
    # results are collected per ticker and merged in config order below so
    # reports stay deterministic regardless of completion order.
    max_workers = max(int(config.get("fetch_max_workers", 4) or 1), 1)
    results_by_ticker: Dict[str, Optional[Dict[str, Any]]] = {}
    if max_workers > 1 and len(ticker_strategies) > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_safe_process, ticker, strategies): ticker
                for ticker, strategies in ticker_strategies.items()
            }
            for future in as_completed(futures):
                results_by_ticker[futures[future]] = future.result()
    else:
        for ticker, strategies in ticker_strategies.items():
            results_by_ticker[ticker] = _safe_process(ticker, strategies)

    for ticker in ticker_strategies:
        result = results_by_ticker.get(ticker)
        if result is None:
            continue
        expiration_summary[ticker] = result.get("selected_expirations", [])
        all_candidates.extend(result.get("candidates", []))
        all_monthly_call_candidates.extend(result.get("monthly_call_candidates", []))
        ticker_results_map[ticker] = result

    cc_recommendations = build_cc_recommendations(ticker_results_map, cc_tickers, config)
    csp_recommendations = build_csp_recommendations(ticker_results_map, csp_tickers, config)

    # Record today's recommendations and grade any whose expiration has passed
    outcome_summary = None
    tracking_cfg: Dict[str, Any] = config.get("outcome_tracking") or {}
    if tracking_cfg.get("enabled", True):
        ledger_path = Path(str(tracking_cfg.get("path") or "./cache/outcomes.csv"))
        try:
            record_recommendations(ledger_path, date.today(), cc_recommendations, csp_recommendations, logger)
            outcome_summary = evaluate_outcomes(ledger_path, market_provider, logger)
        except Exception as exc:
            logger.exception("Outcome tracking failed: %s", exc)

    fallback_events = getattr(options_provider, "fallback_events", [])
    csv_path, html_path = write_reports(
        all_candidates, config, DISCLAIMER,
        csp_recommendations=csp_recommendations,
        cc_recommendations=cc_recommendations,
        monthly_call_candidates=all_monthly_call_candidates,
        fallback_events=fallback_events,
    )
    per_ticker_html_paths = write_per_ticker_reports(
        all_candidates, config, DISCLAIMER,
        csp_recommendations=csp_recommendations,
        cc_recommendations=cc_recommendations,
        monthly_call_candidates=all_monthly_call_candidates,
        cc_tickers=cc_tickers,
        csp_tickers=csp_tickers,
    )

    try:
        send_report_email(config, [html_path, *per_ticker_html_paths], logger)
    except Exception as exc:
        logger.exception("Email notification failed: %s", exc)

    run_day = date.today().isoformat()
    _out = Path(config["output_dir"])
    if cc_recommendations:
        pd.DataFrame(cc_recommendations).to_csv(_out / f"{run_day}_cc_recs.csv", index=False)
    if csp_recommendations:
        pd.DataFrame(csp_recommendations).to_csv(_out / f"{run_day}_csp_recs.csv", index=False)
    if all_monthly_call_candidates:
        pd.DataFrame(all_monthly_call_candidates).to_csv(_out / f"{run_day}_monthly_calls.csv", index=False)

    # Per-ticker context (spot, earnings, regime, support/resistance, suggested
    # strike) for the Calls/Puts tabs' context panel — computed once per
    # ticker here rather than re-embedded into every candidate row's "reason"
    # string. Buffers match whatever each recommender actually filters by, so
    # the suggested price shown in the UI is the same number driving Yes/No.
    context_store = build_context_store(
        ticker_results_map,
        resistance_buffer=float((config.get("cc_recommendation") or {}).get("resistance_pct_buffer", 0.02)),
        support_buffer=float((config.get("csp_recommendation") or {}).get("support_pct_buffer", 0.02)),
    )
    if context_store:
        with (_out / f"{run_day}_context.json").open("w", encoding="utf-8") as f:
            json.dump(context_store, f)

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

    if outcome_summary:
        rate = outcome_summary.get("win_rate")
        rate_str = f", premium-kept rate {rate:.0%}" if rate is not None else ""
        print(
            f"\nOutcome ledger: {outcome_summary['open']} open, "
            f"{outcome_summary['closed']} closed"
            f" ({outcome_summary['evaluated_now']} graded this run){rate_str}"
        )

    print(f"\nCovered call tickers:      {', '.join(cc_tickers) or 'none'}")
    print(f"Cash-secured put tickers:  {', '.join(csp_tickers) or 'none'}")
    print(f"CSV report:  {csv_path}")
    print(f"HTML report: {html_path}")
    print("\nRisk warning:")
    print(DISCLAIMER)

    logger.info("Run completed. CSV=%s HTML=%s candidates=%d", csv_path, html_path, len(all_candidates))
