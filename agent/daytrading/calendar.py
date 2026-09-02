"""
NYSE session state: trading days, half days, holidays, early closes.

Backed by `pandas_market_calendars` (offline, no API, no key). Session state is
derived from the calendar plus `zoneinfo` — deliberately NOT from yfinance's
`.info["marketState"]`, which comes from Yahoo's scraped, rate-limited endpoint
and is not something a trading decision should depend on. See spec §4.

Every timestamp here is tz-aware America/New_York. Naive datetimes are rejected
rather than coerced, so a missing tz surfaces as an error instead of a silently
wrong comparison.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Literal, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

ET = ZoneInfo("America/New_York")

SessionState = Literal["closed", "premarket", "regular", "afterhours"]

# Pre-market is conventionally 04:00 ET; that is also where yfinance's
# extended-hours bars actually begin (see spec §1.3 audit).
PREMARKET_START = time(4, 0)
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)
AFTERHOURS_END = time(20, 0)

# On a half day the hard close moves in by this much from the early close.
HALF_DAY_HARD_CLOSE_OFFSET = timedelta(minutes=30)


@dataclass(frozen=True)
class SessionInfo:
    day: date
    is_trading_day: bool
    market_open: Optional[datetime]  # tz-aware ET
    market_close: Optional[datetime]  # tz-aware ET
    is_half_day: bool
    early_close_time: Optional[datetime]  # tz-aware ET, only when is_half_day


def require_et(ts: datetime, argname: str = "timestamp") -> datetime:
    """Guard: reject naive datetimes, normalise anything aware to ET.

    Spec §4: never store or compare naive datetimes. Coercing a naive value by
    assuming ET would hide the bug, so this raises instead.
    """
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        raise ValueError(
            f"{argname} must be timezone-aware (America/New_York); got naive {ts!r}"
        )
    return ts.astimezone(ET)


@lru_cache(maxsize=8)
def _schedule(year: int) -> pd.DataFrame:
    """NYSE schedule for one calendar year, indexed by date, times in ET."""
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=f"{year}-01-01", end_date=f"{year}-12-31")
    return sched


def session_info(d: date) -> SessionInfo:
    """Trading-day flags and open/close for a single calendar date."""
    sched = _schedule(d.year)
    key = pd.Timestamp(d)
    if key not in sched.index:
        return SessionInfo(
            day=d, is_trading_day=False, market_open=None, market_close=None,
            is_half_day=False, early_close_time=None,
        )

    row = sched.loc[key]
    market_open = row["market_open"].tz_convert(ET).to_pydatetime()
    market_close = row["market_close"].tz_convert(ET).to_pydatetime()
    is_half_day = market_close.time() < REGULAR_CLOSE

    return SessionInfo(
        day=d,
        is_trading_day=True,
        market_open=market_open,
        market_close=market_close,
        is_half_day=is_half_day,
        early_close_time=market_close if is_half_day else None,
    )


def session_state(now_et: datetime) -> SessionState:
    """Where in the session `now_et` falls. Requires a tz-aware timestamp."""
    now_et = require_et(now_et, "now_et")
    info = session_info(now_et.date())
    if not info.is_trading_day:
        return "closed"

    t = now_et.time()
    close_t = info.market_close.time() if info.market_close else REGULAR_CLOSE

    if t < PREMARKET_START:
        return "closed"
    if t < REGULAR_OPEN:
        return "premarket"
    if t < close_t:
        return "regular"
    if t < AFTERHOURS_END:
        return "afterhours"
    return "closed"


def previous_session(d: date) -> Optional[date]:
    """The most recent trading day strictly before `d`."""
    sched = _schedule(d.year)
    prior = sched.index[sched.index < pd.Timestamp(d)]
    if len(prior):
        return prior[-1].date()
    # Cross a year boundary (early January)
    sched_prev = _schedule(d.year - 1)
    if len(sched_prev.index):
        return sched_prev.index[-1].date()
    return None


def is_trading_day(d: date) -> bool:
    return session_info(d).is_trading_day


def next_session(d: date) -> Optional[date]:
    """The next trading day on or after `d`."""
    for year in (d.year, d.year + 1):
        sched = _schedule(year)
        later = sched.index[sched.index >= pd.Timestamp(d)]
        if len(later):
            return later[0].date()
    return None


def session_has_started(day: date, now: datetime) -> bool:
    """True once `day` is a trading day and its open has passed.

    Used to tell "the session has not begun" apart from "the data failed to
    load" — before the open there is legitimately no intraday data, and that
    must never render as a degraded feed.
    """
    now = require_et(now, "now")
    info = session_info(day)
    if not info.is_trading_day or info.market_open is None:
        return False
    return now >= info.market_open


def hard_close_time(d: date, configured_hard_close: str = "15:30") -> Optional[datetime]:
    """Hard close for the day, tz-aware ET.

    Normally the configured time (default 15:30). On a half day it moves to
    early_close - 30 min, and theta arrives faster; the UI surfaces a banner.
    """
    info = session_info(d)
    if not info.is_trading_day:
        return None

    hh, mm = (int(p) for p in configured_hard_close.split(":"))
    normal = datetime.combine(d, time(hh, mm), tzinfo=ET)

    if info.is_half_day and info.early_close_time is not None:
        return info.early_close_time - HALF_DAY_HARD_CLOSE_OFFSET
    return normal


def at_et(d: date, hhmm: str) -> datetime:
    """Build a tz-aware ET datetime from a date and an 'HH:MM' string."""
    hh, mm = (int(p) for p in hhmm.split(":"))
    return datetime.combine(d, time(hh, mm), tzinfo=ET)


def now_et() -> datetime:
    """Current time in ET. The only clock read in the package."""
    return datetime.now(ET)
