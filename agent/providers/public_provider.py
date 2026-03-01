from __future__ import annotations

from datetime import date
from typing import List, Tuple

import pandas as pd

from agent.providers.base import OptionsChainProvider


class PublicOptionsProvider(OptionsChainProvider):
    """Scaffold for Public.com options data integration."""

    def __init__(self, logger, log_dir: str = "./logs") -> None:
        self.logger = logger
        self.log_dir = log_dir
        self._warned_not_implemented = False

    def _warn_not_implemented(self) -> None:
        if not self._warned_not_implemented:
            self.logger.warning("public options provider scaffold is active; API calls are not implemented yet")
            self._warned_not_implemented = True

    def get_options_expirations(self, ticker: str) -> List[date]:
        self._warn_not_implemented()
        self.logger.info("%s: public provider returning no expirations (not implemented)", ticker)
        return []

    def get_options_chain(self, ticker: str, expiration: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        self._warn_not_implemented()
        self.logger.info(
            "%s %s: public provider returning empty option chain (not implemented)",
            ticker,
            expiration.isoformat(),
        )
        return pd.DataFrame(), pd.DataFrame()
