"""
Raw options-chain snapshot persistence.

Every options chain pulled from the external provider (yfinance, public.com,
...) is appended to a per-ticker, per-strategy CSV under the raw-chain
directory, one file per (ticker, strategy) pair — e.g. AAPL_CALL.csv,
AAPL_PUT.csv. This is the unfiltered chain exactly as returned by the
provider (all of its columns), tagged with the run date and expiration so
snapshots accumulate into a history rather than being overwritten.

A re-run on the same day for the same (ticker, expiration) replaces its
earlier snapshot rather than duplicating it.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd


def _chain_path(dir_path: Path, ticker: str, strategy: str) -> Path:
    return dir_path / f"{ticker.upper()}_{strategy.upper()}.csv"


def record_raw_chain(
    dir_path: Path,
    ticker: str,
    strategy: str,
    run_date: date,
    expiration: date,
    df: Optional[pd.DataFrame],
    logger,
) -> None:
    """Append a raw chain snapshot (all provider columns, untouched) for one (ticker, strategy, expiration)."""
    if df is None or df.empty:
        return

    dir_path.mkdir(parents=True, exist_ok=True)
    path = _chain_path(dir_path, ticker, strategy)

    snapshot = df.copy()
    snapshot.insert(0, "expiration", expiration.isoformat())
    snapshot.insert(0, "run_date", run_date.isoformat())

    combined = snapshot
    if path.exists():
        try:
            existing = pd.read_csv(path)
        except Exception:
            existing = pd.DataFrame()
        if not existing.empty and {"run_date", "expiration"}.issubset(existing.columns):
            existing = existing[
                ~(
                    (existing["run_date"] == run_date.isoformat())
                    & (existing["expiration"] == expiration.isoformat())
                )
            ]
        if not existing.empty:
            combined = pd.concat([existing, snapshot], ignore_index=True)

    try:
        combined.to_csv(path, index=False)
    except PermissionError as exc:
        logger.warning("%s %s: raw chain CSV locked, skipping write (%s)", ticker, strategy, exc)
        return
    logger.info(
        "%s %s: raw chain snapshot (%d rows, exp=%s) saved to %s",
        ticker, strategy, len(snapshot), expiration.isoformat(), path,
    )
