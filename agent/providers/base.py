from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import List, Optional, Tuple

import pandas as pd


class MarketDataProvider(ABC):
    @abstractmethod
    def get_price_history(self, ticker: str, period: str, interval: str) -> pd.DataFrame:
        raise NotImplementedError


class OptionsChainProvider(ABC):
    @abstractmethod
    def get_options_expirations(self, ticker: str) -> List[date]:
        raise NotImplementedError

    @abstractmethod
    def get_options_chain(self, ticker: str, expiration: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        raise NotImplementedError

    def log_option_screen_result(self, ticker: str, row: dict) -> None:
        # Optional provider hook for row-level diagnostics.
        return None


class FundamentalsProvider(ABC):
    @abstractmethod
    def get_earnings_date(self, ticker: str) -> Optional[date]:
        raise NotImplementedError


class OptionsDataProvider(MarketDataProvider, OptionsChainProvider, FundamentalsProvider, ABC):
    """Compatibility alias for providers that serve all data domains."""
