from __future__ import annotations

import logging
from datetime import date
from typing import List, Optional, Tuple

import pandas as pd
import yfinance as yf

from agent.providers.base import OptionsDataProvider


class YFinanceProvider(OptionsDataProvider):
    def __init__(self, logger) -> None:
        self.logger = logger
        # yfinance can emit noisy warnings for symbols without fundamentals (e.g., ETFs).
        logging.getLogger("yfinance").setLevel(logging.ERROR)

    def get_price_history(self, ticker: str, period: str, interval: str) -> pd.DataFrame:
        t = yf.Ticker(ticker)
        df = t.history(period=period, interval=interval, auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame()
        return df

    def get_options_expirations(self, ticker: str) -> List[date]:
        t = yf.Ticker(ticker)
        expirations = []
        for exp in list(t.options or []):
            try:
                expirations.append(date.fromisoformat(exp))
            except ValueError:
                self.logger.warning("%s: invalid expiration format from yfinance=%s", ticker, exp)
        return sorted(set(expirations))

    def get_options_chain(self, ticker: str, expiration: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        t = yf.Ticker(ticker)
        chain = t.option_chain(expiration.isoformat())
        calls = chain.calls.copy() if chain and chain.calls is not None else pd.DataFrame()
        puts = chain.puts.copy() if chain and chain.puts is not None else pd.DataFrame()
        return calls, puts

    def get_earnings_date(self, ticker: str) -> Optional[date]:
        t = yf.Ticker(ticker)

        # Skip earnings lookup for instrument types that do not report earnings.
        try:
            info = t.info or {}
            quote_type = str(info.get("quoteType", "")).upper()
            if quote_type in {"ETF", "INDEX", "MUTUALFUND", "CRYPTOCURRENCY", "CURRENCY"}:
                self.logger.info("%s: quoteType=%s does not have earnings; skipping", ticker, quote_type)
                return None
        except Exception as exc:
            self.logger.info("%s: quoteType lookup failed: %s", ticker, exc)

        try:
            cal = t.calendar
            if isinstance(cal, pd.DataFrame) and not cal.empty:
                for col in cal.columns:
                    val = cal[col].iloc[0]
                    dt = pd.to_datetime(val, errors="coerce")
                    if pd.notna(dt):
                        return dt.date()
        except Exception as exc:
            self.logger.info("%s: calendar earnings lookup failed: %s", ticker, exc)

        try:
            earnings = t.get_earnings_dates(limit=4)
            if isinstance(earnings, pd.DataFrame) and not earnings.empty:
                idx = earnings.index[0]
                dt = pd.to_datetime(idx, errors="coerce")
                if pd.notna(dt):
                    return dt.date()
        except Exception as exc:
            msg = str(exc)
            if (
                "No earnings dates found" in msg
                or "No fundamentals data found" in msg
                or "Not Found" in msg
            ):
                self.logger.info("%s: earnings not available for this symbol", ticker)
                return None
            self.logger.info("%s: earnings_dates lookup failed: %s", ticker, exc)

        return None
