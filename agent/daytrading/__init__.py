"""
DayTrading — opening-range breakout evaluation over a fixed watchlist.

Decision support only: this package never places orders, authenticates to a
broker, or fires unattended. It surfaces at most one recommendation per ticker
per day and freezes it.

Layout mirrors the repo's existing `agent/<subpackage>/` convention rather than
the top-level `daytrading/` in the spec (see spec §2, which permits matching the
app's layout). Tests live in the repo's top-level `tests/`, as with every other
subpackage here.

Module responsibilities:
    config      watchlist persistence, thresholds, exclusions
    calendar    session state, half days, holidays (pandas_market_calendars)
    data        yfinance + Public fetch, caching, degraded-state detection
    indicators  MACD, ATR, RSI (Wilder), SMA, VWAP, opening range, volume baseline
    signals     gate + trigger evaluation
    contracts   option chain filtering and selection
    sizing      position size and exit levels
    views       the DayTrading tab UI

Purity contract — `indicators`, `signals`, and `sizing` are pure functions over
DataFrames: no network, no I/O, no clock reads. They take data and an explicit
timestamp as arguments. This is what makes them testable and what will later let
the same code drive a backtest.

Every timestamp in this package is tz-aware `America/New_York`. Conversion
happens once, at the data boundary in `data.py`; nothing downstream re-converts
and no naive datetime is ever compared.
"""
