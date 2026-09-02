"""
Data acquisition: two-stage fetch, caching, and degraded-state detection.

Stage 1 (warm-up, once per day, cached to parquet by date): 250 daily bars in
BOTH adjusted (indicators) and raw (reference levels) form, plus 60 days of 5m
bars including extended hours.

Stage 2 (live poll, every 5 min from 09:45 to the trigger cutoff): today's 5m
bars only, 60-second TTL.

This is the ONLY module in the package that touches the network or the clock. It
is also where UTC -> America/New_York conversion happens, exactly once.

Known feed limitations, established by the spec §1.3 audit against live data:
  * Extended-hours bars carry valid OHLC but volume is identically ZERO, so no
    pre-market volume figure is derivable. It is omitted rather than shown as 0.
  * No bars exist between 20:00 and 03:59 ET. The overnight window is therefore
    16:00-19:55 plus 04:00-~09:25; `OvernightLevels.covered_window` reports what
    was actually covered and the UI displays it next to the level.
"""

from __future__ import annotations

import random
import threading
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree

import pandas as pd
import requests
import yfinance as yf

from agent.daytrading.calendar import ET, now_et, previous_session

# Conservative: Yahoo throttles aggressively and publishes no quota, so the
# failure mode is a 429 rather than a slow response.
MAX_FETCH_WORKERS = 3
MAX_RETRIES = 4
BASE_BACKOFF_SECONDS = 1.5
CIRCUIT_BREAKER_THRESHOLD = 3
LIVE_POLL_TTL_SECONDS = 60
HALT_FEED_MIN_INTERVAL_SECONDS = 60
HALT_FEED_URL = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"


# ── failure tracking ─────────────────────────────────────────────────────────

@dataclass
class CircuitBreaker:
    """Stops polling after repeated failures; only a user refresh re-arms it."""
    threshold: int = CIRCUIT_BREAKER_THRESHOLD
    consecutive_failures: int = 0
    tripped: bool = False
    last_error: str = ""
    last_failure_at: Optional[datetime] = None

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.last_error = ""

    def record_failure(self, error: str) -> None:
        self.consecutive_failures += 1
        self.last_error = error
        self.last_failure_at = now_et()
        if self.consecutive_failures >= self.threshold:
            self.tripped = True

    def reset(self) -> None:
        self.consecutive_failures = 0
        self.tripped = False
        self.last_error = ""


def _is_rate_limited(exc: Exception) -> bool:
    text = str(exc).lower()
    return "429" in text or "too many requests" in text or "rate" in text


def with_backoff(fn, *, retries: int = MAX_RETRIES, logger=None):
    """Exponential backoff with jitter on 429 and connection errors."""
    last: Optional[Exception] = None
    for attempt in range(retries):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - provider raises many shapes
            last = exc
            if attempt == retries - 1:
                break
            wait = BASE_BACKOFF_SECONDS * (2 ** attempt) + random.uniform(0, 0.75)
            if logger:
                logger.warning(
                    "fetch failed (attempt %d/%d, rate_limited=%s): %s - retrying in %.1fs",
                    attempt + 1, retries, _is_rate_limited(exc), exc, wait,
                )
            _time.sleep(wait)
    raise last  # type: ignore[misc]


# ── frames ───────────────────────────────────────────────────────────────────

def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance returns a MultiIndex column frame for multi-ticker downloads."""
    if df is not None and isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


def _to_et_index(df: pd.DataFrame) -> pd.DataFrame:
    """The single UTC -> ET conversion point for the whole package."""
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    out = df.copy()
    idx = out.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.DatetimeIndex(idx)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    out.index = idx.tz_convert(ET)
    return out


@dataclass
class TickerData:
    """Everything one ticker needs for a full evaluation."""
    ticker: str
    daily_adj: pd.DataFrame = field(default_factory=pd.DataFrame)   # indicators
    daily_raw: pd.DataFrame = field(default_factory=pd.DataFrame)   # reference levels
    intraday: pd.DataFrame = field(default_factory=pd.DataFrame)    # 60d 5m, ET, prepost
    degraded_reasons: List[str] = field(default_factory=list)

    @property
    def degraded(self) -> bool:
        return bool(self.degraded_reasons)

    def as_of(self, day: date, now: datetime) -> "TickerData":
        """A copy truncated to what was actually knowable at `now` on `day`.

        Used by the simulation control to replay a past session. Without this
        the 250-day and 60-day frames still contain bars from AFTER the
        simulated date, and the daily gate — which reads the last row — would
        evaluate a past session using future prices. That lookahead would make
        every replayed day look better than it was.

        Daily frames are cut to strictly before `day`, because Stage A is
        defined on the prior completed session. Intraday is cut to `now`.
        """
        def _cut_daily(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return df if df is not None else pd.DataFrame()
            idx = df.index
            dates = idx.date if isinstance(idx, pd.DatetimeIndex) else pd.DatetimeIndex(idx).date
            return df[[d < day for d in dates]]

        def _cut_intraday(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return df if df is not None else pd.DataFrame()
            return df[df.index <= pd.Timestamp(now)]

        return TickerData(
            ticker=self.ticker,
            daily_adj=_cut_daily(self.daily_adj),
            daily_raw=_cut_daily(self.daily_raw),
            intraday=_cut_intraday(self.intraday),
            degraded_reasons=list(self.degraded_reasons),
        )

    def extended_hours_bar_count(self, day: date) -> int:
        """Extended-hours bars available for the overnight window into `day`."""
        if self.intraday is None or self.intraday.empty:
            return 0
        prev = previous_session(day)
        if prev is None:
            return 0
        start = pd.Timestamp(datetime.combine(prev, datetime.min.time()).replace(hour=16), tz=ET)
        end = pd.Timestamp(datetime.combine(day, datetime.min.time()).replace(hour=9, minute=29), tz=ET)
        window = self.intraday[(self.intraday.index >= start) & (self.intraday.index <= end)]
        return len(window)


@dataclass(frozen=True)
class StaticContext:
    """Per-(ticker, day) values that cannot change during the session.

    The daily indicators run on the prior completed session and the volume
    baseline uses only sessions before `day`, so both are fixed once the day
    starts. Computing them on every rerun cost ~220ms per ticker (~2s across a
    9-name watchlist) for values that never move; they are computed once and
    cached instead.
    """
    daily: Optional[Any]          # DailyIndicators
    prev_close: Optional[float]
    volume_baseline: Any          # pd.Series


@dataclass
class WarmupResult:
    as_of: date
    fetched_at: datetime
    data: Dict[str, TickerData]
    from_cache: bool = False
    elapsed_seconds: float = 0.0
    failures: Dict[str, str] = field(default_factory=dict)


# ── warm-up (once per day) ───────────────────────────────────────────────────

class DayTradingData:
    """Owns fetching, caching, the circuit breaker, and the halt feed."""

    def __init__(self, cache_dir: str = "./cache/daytrading", logger=None) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger
        self.breaker = CircuitBreaker()

        self._live_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}
        self._live_lock = threading.Lock()
        self._halt_cache: Tuple[Optional[datetime], List[str]] = (None, [])
        self._warmup: Optional[WarmupResult] = None
        self._static: Dict[Tuple, StaticContext] = {}

    def static_context(self, ticker: str, td: "TickerData", day: date,
                       trend_ma_type: str = "SMA",
                       trend_ma_period: int = 20) -> StaticContext:
        """Per-day constants, computed once per (ticker, day) and reused.

        Safe to cache for the whole session: both inputs look only at data
        strictly before `day`.
        """
        key = (ticker, day, str(trend_ma_type).upper(), int(trend_ma_period))
        hit = self._static.get(key)
        if hit is not None:
            return hit

        # Local import: indicators must stay free of any dependency on this
        # module, so the arrow only ever points one way.
        from agent.daytrading.indicators import (
            daily_indicators, prev_close_raw, time_of_day_volume_baseline,
        )
        ctx = StaticContext(
            daily=daily_indicators(td.daily_adj, trend_ma_type, trend_ma_period),
            prev_close=prev_close_raw(td.daily_raw),
            volume_baseline=time_of_day_volume_baseline(td.intraday, day),
        )
        self._static[key] = ctx
        return ctx

    def clear_static(self) -> None:
        """Drop the per-day cache (used by Refresh and when replaying a date)."""
        self._static.clear()

    # ── cache paths ──────────────────────────────────────────────────────────

    def _cache_path(self, day: date, ticker: str, kind: str) -> Path:
        return self.cache_dir / f"{day.isoformat()}_{ticker.upper()}_{kind}.parquet"

    def _read_cache(self, day: date, ticker: str, kind: str) -> Optional[pd.DataFrame]:
        p = self._cache_path(day, ticker, kind)
        if not p.exists():
            return None
        try:
            return pd.read_parquet(p)
        except Exception as exc:  # noqa: BLE001
            if self.logger:
                self.logger.warning("cache read failed for %s: %s", p.name, exc)
            return None

    def _write_cache(self, day: date, ticker: str, kind: str, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        try:
            df.to_parquet(self._cache_path(day, ticker, kind))
        except Exception as exc:  # noqa: BLE001
            # Caching is an optimisation; never let it break a run.
            if self.logger:
                self.logger.warning("cache write failed for %s %s: %s", ticker, kind, exc)

    # ── fetch primitives ─────────────────────────────────────────────────────

    def _download(self, ticker: str, **kwargs) -> pd.DataFrame:
        """Fetch one frame via `Ticker().history()`.

        Deliberately NOT `yf.download()`, which spec §5.1 specifies: that path
        has an insufficiently-keyed internal response cache. Verified against
        live data — a second `yf.download()` for the same ticker with different
        interval/period returns the FIRST call's frame (AMZN daily came back
        with 11,520 intraday rows), and concurrent calls merge frames into
        duplicate columns ('Close','Close','High','High',...). `Ticker().history()`
        is correct under concurrency and is the pattern the existing
        agent/providers/yfinance_provider.py already uses.
        """
        def _call():
            return yf.Ticker(ticker).history(**kwargs)
        return _flatten(with_backoff(_call, logger=self.logger))

    def _fetch_one(self, ticker: str, day: date, use_cache: bool) -> TickerData:
        td = TickerData(ticker=ticker)

        cached = {k: (self._read_cache(day, ticker, k) if use_cache else None)
                  for k in ("daily_adj", "daily_raw", "intraday")}

        if cached["daily_adj"] is not None:
            td.daily_adj = cached["daily_adj"]
        else:
            td.daily_adj = self._download(ticker, period="250d", interval="1d",
                                          auto_adjust=True)
            self._write_cache(day, ticker, "daily_adj", td.daily_adj)

        if cached["daily_raw"] is not None:
            td.daily_raw = cached["daily_raw"]
        else:
            td.daily_raw = self._download(ticker, period="250d", interval="1d",
                                          auto_adjust=False)
            self._write_cache(day, ticker, "daily_raw", td.daily_raw)

        if cached["intraday"] is not None:
            td.intraday = _to_et_index(cached["intraday"])
        else:
            # The only heavy call. Never re-pulled more than once per day.
            raw = self._download(ticker, interval="5m", period="60d", prepost=True,
                                 auto_adjust=False)
            self._write_cache(day, ticker, "intraday", raw)
            td.intraday = _to_et_index(raw)

        if td.daily_adj is None or td.daily_adj.empty:
            td.degraded_reasons.append("no daily (adjusted) bars returned")
        if td.daily_raw is None or td.daily_raw.empty:
            td.degraded_reasons.append("no daily (raw) bars returned")
        if td.intraday is None or td.intraday.empty:
            td.degraded_reasons.append("no intraday 5m bars returned")
        elif td.extended_hours_bar_count(day) == 0:
            # Spec §1.3: absent extended-hours bars make overnight_high
            # unreliable, and a wrong level is worse than a missing one.
            td.degraded_reasons.append(
                "no extended-hours bars for the overnight window - overnight_high unavailable"
            )
        return td

    def warm_up(self, tickers: List[str], day: Optional[date] = None,
                use_cache: bool = True, force: bool = False) -> WarmupResult:
        """Stage 1. Idempotent per day: re-running reads the parquet cache."""
        day = day or now_et().date()
        started = _time.perf_counter()

        if force:
            self.breaker.reset()
        if self.breaker.tripped and not force:
            return WarmupResult(day, now_et(), {}, False, 0.0,
                                {t: "circuit breaker tripped" for t in tickers})

        data: Dict[str, TickerData] = {}
        failures: Dict[str, str] = {}
        all_cached = True

        with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as pool:
            futures = {
                pool.submit(self._fetch_one, t, day, use_cache and not force): t
                for t in tickers
            }
            for fut in as_completed(futures):
                t = futures[fut]
                try:
                    data[t] = fut.result()
                    self.breaker.record_success()
                except Exception as exc:  # noqa: BLE001
                    failures[t] = str(exc)
                    self.breaker.record_failure(str(exc))
                    if self.logger:
                        self.logger.warning("warm-up failed for %s: %s", t, exc)

        if any(not self._cache_path(day, t, "intraday").exists() for t in tickers):
            all_cached = False

        elapsed = _time.perf_counter() - started
        result = WarmupResult(day, now_et(), data, all_cached, elapsed, failures)
        self._warmup = result
        return result

    @property
    def last_warmup(self) -> Optional[WarmupResult]:
        return self._warmup

    def cache_age(self) -> Optional[timedelta]:
        if self._warmup is None:
            return None
        return now_et() - self._warmup.fetched_at

    # ── live poll (60s TTL) ──────────────────────────────────────────────────

    def poll_today(self, ticker: str, force: bool = False) -> pd.DataFrame:
        """Stage 2. Today's 5m bars, ET-indexed, cached for 60s.

        The TTL exists so repeated UI reloads inside one bar do not re-hit Yahoo.
        """
        if self.breaker.tripped and not force:
            return pd.DataFrame()

        now = _time.time()
        with self._live_lock:
            hit = self._live_cache.get(ticker)
            if hit and not force and (now - hit[0]) < LIVE_POLL_TTL_SECONDS:
                return hit[1]

        try:
            raw = self._download(ticker, interval="5m", period="1d", prepost=True,
                                 auto_adjust=False)
            df = _to_et_index(raw)
            self.breaker.record_success()
        except Exception as exc:  # noqa: BLE001
            self.breaker.record_failure(str(exc))
            if self.logger:
                self.logger.warning("live poll failed for %s: %s", ticker, exc)
            return pd.DataFrame()

        with self._live_lock:
            self._live_cache[ticker] = (now, df)
        return df

    def poll_all(self, tickers: List[str], force: bool = False) -> Dict[str, pd.DataFrame]:
        out: Dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as pool:
            futures = {pool.submit(self.poll_today, t, force): t for t in tickers}
            for fut in as_completed(futures):
                t = futures[fut]
                try:
                    out[t] = fut.result()
                except Exception:  # noqa: BLE001
                    out[t] = pd.DataFrame()
        return out

    # ── halt status ──────────────────────────────────────────────────────────

    def halted_symbols(self, watchlist: Optional[List[str]] = None) -> Tuple[List[str], Optional[datetime]]:
        """Symbols currently halted, from the Nasdaq trade-halt RSS feed.

        Polled at most once a minute, per the feed's own refresh cadence.
        Returns (symbols, last_fetch_time).
        """
        last_fetch, cached = self._halt_cache
        if last_fetch is not None and (now_et() - last_fetch).total_seconds() < HALT_FEED_MIN_INTERVAL_SECONDS:
            return (self._filter_halts(cached, watchlist), last_fetch)

        try:
            resp = requests.get(HALT_FEED_URL, timeout=10)
            resp.raise_for_status()
            root = ElementTree.fromstring(resp.content)
            symbols: List[str] = []
            for item in root.iter("item"):
                # The feed nests the symbol in an ndaq: namespaced child; fall
                # back to scanning the title, whose format is "SYM - reason".
                sym = None
                for child in item:
                    if child.tag.lower().endswith("issuesymbol") and (child.text or "").strip():
                        sym = child.text.strip().upper()
                        break
                if sym is None:
                    title = (item.findtext("title") or "").strip()
                    if title:
                        sym = title.split()[0].strip().upper().rstrip("-,")
                if sym:
                    symbols.append(sym)
            self._halt_cache = (now_et(), symbols)
            return (self._filter_halts(symbols, watchlist), self._halt_cache[0])
        except Exception as exc:  # noqa: BLE001
            if self.logger:
                self.logger.warning("halt feed fetch failed: %s", exc)
            return (self._filter_halts(cached, watchlist), last_fetch)

    @staticmethod
    def _filter_halts(symbols: List[str], watchlist: Optional[List[str]]) -> List[str]:
        if watchlist is None:
            return list(symbols)
        wl = {t.upper() for t in watchlist}
        return [s for s in symbols if s in wl]


    # ── option chain (spec §5.4) ─────────────────────────────────────────────

    def fetch_call_candidates(
        self,
        ticker: str,
        as_of: date,
        underlying_price: float,
        app_config: Dict[str, Any],
        *,
        dte_min: int,
        dte_max: int,
        underlying_quote_time: Optional[datetime] = None,
    ):
        """Call contracts in the DTE window, via the EXISTING provider stack.

        Called only when a signal fires, for the one ticker that fired — never
        pre-emptively for the whole watchlist (spec §5.4). Reuses
        agent/providers/factory.build_options_provider, so the Public ->
        yfinance fallback and the Public greeks enrichment both apply here
        exactly as they do for the CC/CSP screener.

        Returns (candidates, error). `error` is non-None when the chain could
        not be fetched at all, so the caller can render degraded rather than
        "no contract found".
        """
        # Imported here rather than at module scope: this is the only path that
        # needs the provider stack, and it keeps import cost off every run.
        from agent.daytrading.contracts import build_candidates
        from agent.providers.factory import build_options_provider

        try:
            provider = build_options_provider(app_config, self.logger)
        except Exception as exc:  # noqa: BLE001
            return [], f"options provider unavailable: {exc}"

        try:
            expirations = provider.get_options_expirations(ticker)
        except Exception as exc:  # noqa: BLE001
            return [], f"expiration lookup failed: {exc}"

        targets = [e for e in expirations if dte_min <= (e - as_of).days <= dte_max]
        if not targets:
            have = ", ".join(f"{(e - as_of).days}d" for e in sorted(expirations)[:6]) or "none"
            return [], (
                f"no expiration in the {dte_min}-{dte_max} DTE window "
                f"(available: {have})"
            )

        out = []
        errors = []
        for exp in sorted(targets):
            try:
                calls, _ = provider.get_options_chain(ticker, exp)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{exp.isoformat()}: {exc}")
                continue
            out.extend(build_candidates(
                calls, exp, as_of, underlying_price,
                quote_time=now_et(), underlying_quote_time=underlying_quote_time,
            ))

        if not out and errors:
            return [], "chain fetch failed - " + "; ".join(errors)
        return out, None


def looks_halted_locally(rth: pd.DataFrame, now_et_ts: datetime) -> bool:
    """Local backstop: a completed regular-session 5m bar with zero volume.

    On a mega-cap that is a strong halt signal. Treated as blocking alongside
    the RSS feed, since either source alone can lag.
    """
    if rth is None or rth.empty:
        return False
    complete = rth[rth.index + timedelta(minutes=5) <= now_et_ts]
    if complete.empty:
        return False
    return float(complete["Volume"].iloc[-1]) == 0.0
