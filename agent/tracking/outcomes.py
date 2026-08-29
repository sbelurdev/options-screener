"""
Recommendation outcome tracking.

Every run appends the day's CC/CSP recommendations (both Yes and No verdicts,
so their relative performance can be compared later) to a CSV ledger. On each
subsequent run, rows whose expiration has passed are graded against the
underlying's close on expiration day:

  CSP (short put):
    close >= strike → expired_otm   pnl = premium × 100
    close <  strike → assigned      pnl = (close − strike + premium) × 100

  CC (short call, option leg only — share P&L excluded):
    close <= strike → expired_otm   pnl = premium × 100
    close >  strike → called_away   pnl = (premium − (close − strike)) × 100

P&L is mark-to-expiry per contract and educational only — it ignores early
assignment, rolls, and fills better or worse than the recorded premium.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

LEDGER_COLUMNS = [
    "run_date",
    "strategy",
    "ticker",
    "term",
    "verdict",
    "expiration",
    "strike",
    "premium",
    "delta",
    "dte",
    "annualized_yield",
    "spot_at_rec",
    "status",
    "expiry_close",
    "outcome",
    "option_pnl",
    "evaluated_on",
]

# Same-contract duplicate key: a re-run on the same day replaces its earlier rows
_DEDUPE_KEY = ["run_date", "strategy", "ticker", "term", "expiration", "strike"]


def _load_ledger(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=LEDGER_COLUMNS)
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=LEDGER_COLUMNS)
    for col in LEDGER_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[LEDGER_COLUMNS]
    # Empty text columns round-trip through CSV as NaN/float64 — restore object
    # dtype so string statuses and outcomes can be assigned on evaluation.
    text_cols = ["run_date", "strategy", "ticker", "term", "verdict", "expiration", "status", "outcome", "evaluated_on"]
    for col in text_cols:
        df[col] = df[col].astype("object").where(df[col].notna(), "")
    return df


def record_recommendations(
    path: Path,
    run_date: date,
    cc_recommendations: Optional[List[Dict[str, Any]]],
    csp_recommendations: Optional[List[Dict[str, Any]]],
    logger,
) -> int:
    """Append today's recommendations to the ledger. Returns number of rows recorded."""
    rows: List[Dict[str, Any]] = []
    for strategy, recs in (("CC", cc_recommendations or []), ("CSP", csp_recommendations or [])):
        for r in recs:
            if not r.get("strike") or not r.get("expiration"):
                continue  # placeholder rows with no actual contract
            rows.append(
                {
                    "run_date": run_date.isoformat(),
                    "strategy": strategy,
                    "ticker": r.get("ticker"),
                    "term": r.get("term", ""),
                    "verdict": r.get("recommend", ""),
                    "expiration": str(r.get("expiration")),
                    "strike": float(r.get("strike")),
                    "premium": r.get("premium"),
                    "delta": r.get("delta"),
                    "dte": r.get("dte"),
                    "annualized_yield": r.get("annualized_yield"),
                    "spot_at_rec": r.get("spot"),
                    "status": "open",
                    "expiry_close": None,
                    "outcome": "",
                    "option_pnl": None,
                    "evaluated_on": "",
                }
            )
    if not rows:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    ledger = _load_ledger(path)
    new_df = pd.DataFrame(rows)

    # Same-day re-runs replace their earlier entries for the same contract
    if not ledger.empty:
        new_keys = set(map(tuple, new_df[_DEDUPE_KEY].astype(str).values))
        existing_keys = ledger[_DEDUPE_KEY].astype(str).apply(tuple, axis=1)
        ledger = ledger[~existing_keys.isin(new_keys)]

    ledger = pd.concat([ledger, new_df], ignore_index=True)
    ledger.to_csv(path, index=False)
    logger.info("Outcome ledger: recorded %d recommendation(s) to %s", len(rows), path)
    return len(rows)


def _close_on_expiration(hist: Optional[pd.DataFrame], expiration: date) -> Optional[float]:
    """Close on expiration day, or the last close before it (half-day/holiday tolerance)."""
    if hist is None or hist.empty or "Close" not in hist.columns:
        return None
    idx = pd.to_datetime(hist.index)
    try:
        idx = idx.tz_localize(None)
    except TypeError:
        pass
    mask = idx.normalize() <= pd.Timestamp(expiration)
    sel = hist["Close"][mask]
    if sel.empty:
        return None
    return float(sel.iloc[-1])


def evaluate_outcomes(path: Path, market_provider, logger) -> Optional[Dict[str, Any]]:
    """
    Grade ledger rows whose expiration has passed. Returns a summary dict
    (open/closed counts, premium-kept rate) or None when the ledger is empty.
    """
    ledger = _load_ledger(path)
    if ledger.empty:
        return None

    today = date.today()
    pending = ledger[(ledger["status"] == "open") & (ledger["expiration"] < today.isoformat())]

    hist_by_ticker: Dict[str, pd.DataFrame] = {}
    for t in pending["ticker"].dropna().unique():
        try:
            hist_by_ticker[str(t)] = market_provider.get_price_history(str(t), period="1y", interval="1d")
        except Exception as exc:
            logger.warning("Outcome ledger: price history failed for %s: %s", t, exc)

    evaluated = 0
    for i in pending.index:
        row = ledger.loc[i]
        try:
            exp = date.fromisoformat(str(row["expiration"]))
        except ValueError:
            ledger.loc[i, ["status", "outcome", "evaluated_on"]] = ["closed", "bad_expiration", today.isoformat()]
            continue

        close = _close_on_expiration(hist_by_ticker.get(str(row["ticker"])), exp)
        if close is None:
            # Expirations older than the 1y history window can never be graded
            if (today - exp).days > 350:
                ledger.loc[i, ["status", "outcome", "evaluated_on"]] = ["closed", "no_data", today.isoformat()]
            continue

        strike = float(row["strike"])
        premium = float(row["premium"] or 0)
        if str(row["strategy"]) == "CSP":
            if close >= strike:
                outcome, pnl = "expired_otm", premium * 100
            else:
                outcome, pnl = "assigned", (close - strike + premium) * 100
        else:  # CC — option leg only
            if close <= strike:
                outcome, pnl = "expired_otm", premium * 100
            else:
                outcome, pnl = "called_away", (premium - (close - strike)) * 100

        ledger.loc[i, "status"] = "closed"
        ledger.loc[i, "expiry_close"] = round(close, 4)
        ledger.loc[i, "outcome"] = outcome
        ledger.loc[i, "option_pnl"] = round(pnl, 2)
        ledger.loc[i, "evaluated_on"] = today.isoformat()
        evaluated += 1

    if evaluated:
        ledger.to_csv(path, index=False)
        logger.info("Outcome ledger: evaluated %d expired recommendation(s)", evaluated)

    closed = ledger[ledger["status"] == "closed"]
    graded = closed[closed["outcome"].isin(["expired_otm", "assigned", "called_away"])]
    win_rate = None
    if len(graded) > 0:
        win_rate = float((graded["outcome"] == "expired_otm").mean())
    return {
        "open": int((ledger["status"] == "open").sum()),
        "closed": int(len(closed)),
        "evaluated_now": evaluated,
        "win_rate": win_rate,
    }
