from __future__ import annotations

import os
import sys
from datetime import date
from typing import Any, Dict, List, Tuple

import pandas as pd

from agent.providers.base import FundamentalsProvider, MarketDataProvider, OptionsChainProvider
from agent.providers.public_provider import PublicOptionsProvider
from agent.providers.yfinance_provider import YFinanceProvider

# ANSI colours — stripped automatically on non-TTY (e.g. redirected output)
_YELLOW = "\033[33m" if sys.stderr.isatty() else ""
_BOLD   = "\033[1m"  if sys.stderr.isatty() else ""
_RESET  = "\033[0m"  if sys.stderr.isatty() else ""
_LINE   = "=" * 68


def _prominent_warning(logger, message: str) -> None:
    """Log at WARNING level AND print a visually distinct banner to stderr."""
    logger.warning(message)
    print(f"\n{_YELLOW}{_BOLD}{_LINE}", file=sys.stderr)
    print(f"  WARNING: {message}", file=sys.stderr)
    print(f"{_LINE}{_RESET}\n", file=sys.stderr)


def _provider_name(config: Dict[str, Any], key: str, default: str) -> str:
    return str(config.get(key, default)).strip().lower()


class _FallbackOptionsProvider(OptionsChainProvider):
    """Wraps PublicOptionsProvider and falls back to YFinanceProvider on empty results or errors.

    Also attempts to enrich delta from Public greeks even when the chain is
    served by yfinance, so Public delta is used wherever accessible.
    """

    def __init__(self, primary: PublicOptionsProvider, secondary: YFinanceProvider, logger) -> None:
        self._primary = primary
        self._secondary = secondary
        self._logger = logger
        self.fallback_events: List[str] = []  # collected by pipeline for HTML report

    def _record_fallback(self, message: str) -> None:
        _prominent_warning(self._logger, message)
        self.fallback_events.append(message)

    def _enrich_delta_from_public(self, calls: pd.DataFrame, puts: pd.DataFrame) -> None:
        """Best-effort: inject delta from Public greeks into yfinance chain DataFrames."""
        symbols: List[str] = []
        for df in [calls, puts]:
            if not df.empty and "contractSymbol" in df.columns:
                symbols.extend(
                    self._primary._normalize_osi_symbol(s)
                    for s in df["contractSymbol"].dropna()
                )
        if not symbols:
            return
        try:
            greeks = self._primary._get_greeks(symbols)
            if not greeks:
                return
            for df in [calls, puts]:
                if df.empty or "contractSymbol" not in df.columns:
                    continue
                delta_vals = [
                    (greeks.get(self._primary._normalize_osi_symbol(s)) or {}).get("delta")
                    for s in df["contractSymbol"]
                ]
                df["delta"] = delta_vals
            self._logger.info("Delta enriched from Public greeks for %d symbol(s)", len(greeks))
        except Exception as exc:
            self._logger.debug("Public delta enrichment failed (best-effort): %s", exc)

    def get_options_expirations(self, ticker: str) -> List[date]:
        try:
            result = self._primary.get_options_expirations(ticker)
        except Exception as exc:
            self._record_fallback(f"{ticker}: Public provider error fetching expirations ({exc}) — using yfinance")
            return self._secondary.get_options_expirations(ticker)
        if not result:
            self._record_fallback(f"{ticker}: Public provider returned no expirations — using yfinance")
            return self._secondary.get_options_expirations(ticker)
        return result

    def get_options_chain(self, ticker: str, expiration: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        try:
            calls, puts = self._primary.get_options_chain(ticker, expiration)
        except Exception as exc:
            self._record_fallback(f"{ticker} {expiration.isoformat()}: Public provider error fetching chain ({exc}) — using yfinance")
            calls, puts = self._secondary.get_options_chain(ticker, expiration)
            self._enrich_delta_from_public(calls, puts)
            return calls, puts
        if calls.empty and puts.empty:
            self._record_fallback(f"{ticker} {expiration.isoformat()}: Public provider returned empty chain — using yfinance")
            calls, puts = self._secondary.get_options_chain(ticker, expiration)
            self._enrich_delta_from_public(calls, puts)
            return calls, puts
        return calls, puts

    def log_option_screen_result(self, ticker: str, row: dict) -> None:
        self._primary.log_option_screen_result(ticker, row)


def build_options_provider(config: Dict[str, Any], logger) -> OptionsChainProvider:
    name = _provider_name(config, "options_data_provider", "yfinance")
    log_dir = str(config["log_dir"])
    if name == "yfinance":
        return YFinanceProvider(logger=logger, log_dir=log_dir)
    if name == "public":
        secret_env_var = str(config.get("public_api_key_env_var", "PUBLIC_API_KEY"))
        yf_provider = YFinanceProvider(logger=logger, log_dir=log_dir)
        if not os.environ.get(secret_env_var):
            _prominent_warning(
                logger,
                f"options_data_provider=public but env var '{secret_env_var}' is not set — falling back to yfinance",
            )
            return yf_provider
        public_provider = PublicOptionsProvider(logger=logger, config=config, log_dir=log_dir)
        return _FallbackOptionsProvider(primary=public_provider, secondary=yf_provider, logger=logger)
    raise ValueError(f"Unsupported options_data_provider='{name}'. Expected one of: yfinance, public")


def build_market_provider(config: Dict[str, Any], logger) -> MarketDataProvider:
    name = _provider_name(config, "market_data_provider", "yfinance")
    log_dir = str(config["log_dir"])
    if name == "yfinance":
        return YFinanceProvider(logger=logger, log_dir=log_dir)
    raise ValueError(f"Unsupported market_data_provider='{name}'. Expected one of: yfinance")


def build_fundamentals_provider(config: Dict[str, Any], logger) -> FundamentalsProvider:
    name = _provider_name(config, "fundamentals_provider", "yfinance")
    log_dir = str(config["log_dir"])
    if name == "yfinance":
        return YFinanceProvider(logger=logger, log_dir=log_dir)
    raise ValueError(f"Unsupported fundamentals_provider='{name}'. Expected one of: yfinance")
