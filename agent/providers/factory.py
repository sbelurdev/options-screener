from __future__ import annotations

from typing import Any, Dict

from agent.providers.base import FundamentalsProvider, MarketDataProvider, OptionsChainProvider
from agent.providers.public_provider import PublicOptionsProvider
from agent.providers.yfinance_provider import YFinanceProvider


def _provider_name(config: Dict[str, Any], key: str, default: str) -> str:
    return str(config.get(key, default)).strip().lower()


def build_options_provider(config: Dict[str, Any], logger) -> OptionsChainProvider:
    name = _provider_name(config, "options_data_provider", "yfinance")
    log_dir = str(config["log_dir"])
    if name == "yfinance":
        return YFinanceProvider(logger=logger, log_dir=log_dir)
    if name == "public":
        return PublicOptionsProvider(logger=logger, config=config, log_dir=log_dir)
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
