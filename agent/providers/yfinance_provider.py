from __future__ import annotations

import csv
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Optional, Set, Tuple

import pandas as pd
import yfinance as yf

from agent.providers.base import OptionsDataProvider


def _retry(fn, retries: int = 3, delay: float = 2.0):
    """Call fn(); on exception retry up to `retries` times with exponential back-off."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as exc:
            if attempt == retries - 1:
                raise
            wait = delay * (2 ** attempt)
            logging.getLogger(__name__).warning(
                "yfinance call failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt + 1, retries, exc, wait,
            )
            time.sleep(wait)


class YFinanceProvider(OptionsDataProvider):
    CSV_FIELDS = [
        "timestamp_utc",
        "event",
        "ticker",
        "period",
        "interval",
        "history_rows",
        "history_start",
        "history_end",
        "expiration",
        "expiration_value",
        "option_type",
        "contractSymbol",
        "strike",
        "bid",
        "ask",
        "lastPrice",
        "volume",
        "openInterest",
        "impliedVolatility",
        "earnings_date",
        "status",
        "message",
        "filtered",
        "filter_reason",
    ]

    def __init__(self, logger, log_dir: str = "./logs") -> None:
        self.logger = logger
        # yfinance can emit noisy warnings for symbols without fundamentals (e.g., ETFs).
        logging.getLogger("yfinance").setLevel(logging.ERROR)
        self.ticker_data_dir = Path(log_dir) / "ticker_data"
        self.ticker_data_dir.mkdir(parents=True, exist_ok=True)
        # Cache of paths whose schema has already been verified this run
        self._schema_ok: Set[Path] = set()

    def _csv_path(self, ticker: str) -> Path:
        return self.ticker_data_dir / f"{ticker.upper()}_yfinance_data.csv"

    def _clean_value(self, value):
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    def _append_rows(self, ticker: str, rows: List[dict]) -> None:
        if not rows:
            return
        path = self._csv_path(ticker)
        if path not in self._schema_ok:
            self._ensure_schema(path)
            self._schema_ok.add(path)
        try:
            is_new = not path.exists()
            with path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
                if is_new:
                    writer.writeheader()
                for row in rows:
                    payload = {k: "" for k in self.CSV_FIELDS}
                    payload.update(row)
                    payload["timestamp_utc"] = datetime.now(timezone.utc).isoformat()
                    payload["ticker"] = ticker.upper()
                    payload = {k: self._clean_value(v) for k, v in payload.items()}
                    writer.writerow(payload)
        except PermissionError as exc:
            # Logging/tracing must not break screening if file is open in another app.
            self.logger.warning("%s: ticker CSV locked, skipping write (%s)", ticker, exc)
        except OSError as exc:
            self.logger.warning("%s: ticker CSV write failed (%s)", ticker, exc)

    def _ensure_schema(self, path: Path) -> None:
        if not path.exists():
            return
        try:
            with path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing_fields = reader.fieldnames or []
                if existing_fields == self.CSV_FIELDS:
                    return
                rows = list(reader)
            normalized_rows = []
            for row in rows:
                payload = {k: "" for k in self.CSV_FIELDS}
                for k, v in row.items():
                    if k in payload:
                        payload[k] = v
                normalized_rows.append(payload)
            with path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
                writer.writeheader()
                if normalized_rows:
                    writer.writerows(normalized_rows)
        except PermissionError as exc:
            self.logger.warning("Could not migrate schema for %s (locked): %s", path.name, exc)
        except OSError as exc:
            self.logger.warning("Could not migrate schema for %s: %s", path.name, exc)

    def log_option_screen_result(self, ticker: str, row: dict) -> None:
        self._append_rows(ticker, [row])

    def get_price_history(self, ticker: str, period: str, interval: str) -> pd.DataFrame:
        t = yf.Ticker(ticker)
        df = _retry(lambda: t.history(period=period, interval=interval, auto_adjust=False))
        if df is None or df.empty:
            self.logger.info(
                "%s: yfinance history returned empty (period=%s interval=%s)",
                ticker,
                period,
                interval,
            )
            self._append_rows(
                ticker,
                [
                    {
                        "event": "price_history",
                        "period": period,
                        "interval": interval,
                        "history_rows": 0,
                        "status": "empty",
                    }
                ],
            )
            return pd.DataFrame()

        start = str(df.index.min())
        end = str(df.index.max())
        latest_close = float(df["Close"].iloc[-1]) if "Close" in df.columns else None
        self._append_rows(
            ticker,
            [
                {
                    "event": "price_history",
                    "period": period,
                    "interval": interval,
                    "history_rows": len(df),
                    "history_start": start,
                    "history_end": end,
                    "lastPrice": latest_close,
                    "status": "ok",
                }
            ],
        )
        return df

    def get_options_expirations(self, ticker: str) -> List[date]:
        t = yf.Ticker(ticker)
        expirations = []
        for exp in list(_retry(lambda: t.options) or []):
            try:
                expirations.append(date.fromisoformat(exp))
            except ValueError:
                self.logger.warning("%s: invalid expiration format from yfinance=%s", ticker, exp)

        expirations = sorted(set(expirations))
        self._append_rows(
            ticker,
            [
                {
                    "event": "options_expirations",
                    "expiration_value": e.isoformat(),
                    "status": "ok",
                }
                for e in expirations
            ],
        )
        return expirations

    def get_options_chain(self, ticker: str, expiration: date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        t = yf.Ticker(ticker)
        chain = _retry(lambda: t.option_chain(expiration.isoformat()))
        calls = chain.calls.copy() if chain and chain.calls is not None else pd.DataFrame()
        puts = chain.puts.copy() if chain and chain.puts is not None else pd.DataFrame()

        rows = []
        for option_type, df in [("CALL", calls), ("PUT", puts)]:
            if df is None or df.empty:
                rows.append(
                    {
                        "event": "options_chain",
                        "expiration": expiration.isoformat(),
                        "option_type": option_type,
                        "status": "empty",
                    }
                )
                continue

            for _, r in df.iterrows():
                rows.append(
                    {
                        "event": "options_chain",
                        "expiration": expiration.isoformat(),
                        "option_type": option_type,
                        "contractSymbol": r.get("contractSymbol"),
                        "strike": r.get("strike"),
                        "bid": r.get("bid"),
                        "ask": r.get("ask"),
                        "lastPrice": r.get("lastPrice"),
                        "volume": r.get("volume"),
                        "openInterest": r.get("openInterest"),
                        "impliedVolatility": r.get("impliedVolatility"),
                        "status": "ok",
                    }
                )
        self._append_rows(ticker, rows)
        return calls, puts

    def get_earnings_date(self, ticker: str) -> Optional[date]:
        t = yf.Ticker(ticker)

        # Skip earnings lookup for instrument types that do not report earnings.
        try:
            info = _retry(lambda: t.info) or {}
            quote_type = str(info.get("quoteType", "")).upper()
            self._append_rows(
                ticker,
                [
                    {
                        "event": "earnings_lookup",
                        "status": "info",
                        "message": f"quoteType={quote_type}",
                    }
                ],
            )
            if quote_type in {"ETF", "INDEX", "MUTUALFUND", "CRYPTOCURRENCY", "CURRENCY"}:
                self.logger.info("%s: quoteType=%s does not have earnings; skipping", ticker, quote_type)
                self._append_rows(
                    ticker,
                    [
                        {
                            "event": "earnings_lookup",
                            "status": "skipped",
                            "message": "instrument type does not report earnings",
                        }
                    ],
                )
                return None
        except Exception as exc:
            self.logger.info("%s: quoteType lookup failed: %s", ticker, exc)
            self._append_rows(
                ticker,
                [
                    {
                        "event": "earnings_lookup",
                        "status": "error",
                        "message": f"quoteType lookup failed: {exc}",
                    }
                ],
            )

        try:
            cal = t.calendar
            today = date.today()
            if isinstance(cal, pd.DataFrame) and cal.empty:
                self._append_rows(
                    ticker,
                    [
                        {
                            "event": "earnings_lookup",
                            "status": "empty",
                            "message": "calendar empty",
                        }
                    ],
                )
            if isinstance(cal, pd.DataFrame) and not cal.empty:
                # Only look at rows whose index label mentions "Earnings" to avoid
                # returning ex-dividend or other dates.
                earnings_rows = [
                    idx for idx in cal.index
                    if "earning" in str(idx).lower()
                ]
                search_rows = earnings_rows if earnings_rows else list(cal.index)
                for row_label in search_rows:
                    for col in cal.columns:
                        val = cal.loc[row_label, col]
                        dt = pd.to_datetime(val, errors="coerce")
                        if pd.notna(dt) and dt.date() >= today:
                            self._append_rows(
                                ticker,
                                [
                                    {
                                        "event": "earnings_lookup",
                                        "status": "ok",
                                        "earnings_date": dt.date().isoformat(),
                                        "message": f"source=calendar row={row_label}",
                                    }
                                ],
                            )
                            return dt.date()
        except Exception as exc:
            self.logger.info("%s: calendar earnings lookup failed: %s", ticker, exc)
            self._append_rows(
                ticker,
                [
                    {
                        "event": "earnings_lookup",
                        "status": "error",
                        "message": f"calendar lookup failed: {exc}",
                    }
                ],
            )

        try:
            earnings = _retry(lambda: t.get_earnings_dates(limit=8))
            if isinstance(earnings, pd.DataFrame) and earnings.empty:
                self._append_rows(
                    ticker,
                    [
                        {
                            "event": "earnings_lookup",
                            "status": "empty",
                            "message": "earnings_dates empty",
                        }
                    ],
                )
            if isinstance(earnings, pd.DataFrame) and not earnings.empty:
                today = date.today()
                # Return the first FUTURE earnings date, skipping past ones.
                for idx in earnings.index:
                    dt = pd.to_datetime(idx, errors="coerce")
                    if pd.notna(dt) and dt.date() >= today:
                        self._append_rows(
                            ticker,
                            [
                                {
                                    "event": "earnings_lookup",
                                    "status": "ok",
                                    "earnings_date": dt.date().isoformat(),
                                    "message": "source=earnings_dates",
                                }
                            ],
                        )
                        return dt.date()
        except Exception as exc:
            msg = str(exc)
            if (
                "No earnings dates found" in msg
                or "No fundamentals data found" in msg
                or "Not Found" in msg
            ):
                self.logger.info("%s: earnings not available for this symbol", ticker)
                self._append_rows(
                    ticker,
                    [
                        {
                            "event": "earnings_lookup",
                            "status": "unavailable",
                            "message": "earnings not available",
                        }
                    ],
                )
                return None
            self.logger.info("%s: earnings_dates lookup failed: %s", ticker, exc)
            self._append_rows(
                ticker,
                [
                    {
                        "event": "earnings_lookup",
                        "status": "error",
                        "message": f"earnings_dates failed: {exc}",
                    }
                ],
            )

        return None
