"""
Watchlist, thresholds, and exclusions for the DayTrading tab.

Persistence reuses the app's existing YAML profile mechanism: settings live
under a `daytrading:` key in config/users/<profile>.yaml, nested the same way
`cc_recommendation:` and `schedule:` already are. See spec §3.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, List, Optional

from agent.utils.config_io import load_merged_config, save_profile

CONFIG_KEY = "daytrading"

DEFAULT_WATCHLIST = ["SPY", "QQQ", "MSFT", "NVDA", "AAPL", "GOOGL", "AMZN", "META", "TSLA"]

# Index context shown at the top of the pre-market panel (§8.2). Always fetched
# and displayed; only a gate when require_market_gate is on.
MARKET_CONTEXT_TICKERS = ["SPY", "QQQ"]


@dataclass
class DayTradingConfig:
    watchlist: List[str] = field(default_factory=lambda: list(DEFAULT_WATCHLIST))
    exclusions: Dict[str, str] = field(default_factory=dict)  # ticker -> reason

    # Daily name gate. MACD is not a gate — it is computed and displayed as
    # context only, and never blocks a name.
    rsi_min: float = 50.0
    rsi_max: float = 75.0
    require_close_above_trend_ma: bool = True
    # Trend filter. A shorter/exponential average turns faster but sits closer
    # to price, which in a rising market makes this gate STRICTER, not looser.
    trend_ma_type: str = "SMA"     # SMA or EMA
    trend_ma_period: int = 20

    # Trigger window (all ET)
    or_start: str = "09:30"
    or_end: str = "09:40"  # inclusive; the 09:30, 09:35, 09:40 bars
    trigger_start: str = "09:45"
    trigger_cutoff: str = "11:00"  # no entries after this

    # Polling window (ET). Wider than the trigger window on purpose: before the
    # open it keeps overnight_high current, and after the cutoff it tracks an
    # open position against its stop/target. What gets polled is narrowed by
    # phase (see views._poll_set), so the wider window costs far less than
    # watchlist x cycles.
    poll_start: str = "08:45"
    poll_end: str = "15:45"

    # Contract
    dte_min: int = 3
    dte_max: int = 5
    delta_min: float = 0.60
    delta_max: float = 0.70
    delta_target: float = 0.65
    max_spread_pct_of_mid: float = 0.03
    min_open_interest: int = 500

    # Risk
    account_size: float = 0.0  # user-entered, ring-fenced amount
    risk_pct_per_trade: float = 0.005
    max_concurrent_positions: int = 2
    time_stop_minutes: int = 45
    hard_close: str = "15:30"

    # Optional gates
    block_on_earnings_in_window: bool = True
    require_market_gate: bool = False

    # Recipients for DayTrading alerts only. Empty means "fall back to the
    # profile's notify_email"; a non-empty list REPLACES it, so the CC/CSP
    # report keeps going wherever notify_email points regardless.
    notify_emails: List[str] = field(default_factory=list)

    # ── derived helpers ──────────────────────────────────────────────────────

    def active_watchlist(self) -> List[str]:
        """Watchlist minus exclusions — the only list that is ever evaluated.

        An excluded ticker is never evaluated and never produces a signal; it is
        still rendered (greyed, with its reason) by the UI.
        """
        excluded = {t.upper() for t in self.exclusions}
        return [t for t in self.watchlist if t.upper() not in excluded]

    def alert_recipients(self, profile_notify_email: Optional[str] = None) -> List[str]:
        """Who DayTrading alerts go to.

        `notify_emails` wins when set; otherwise the profile's single
        `notify_email` is used, so a profile that never configures a
        DayTrading-specific list still gets alerts.
        """
        if self.notify_emails:
            return [str(e).strip() for e in self.notify_emails if str(e).strip()]
        one = str(profile_notify_email or "").strip()
        return [one] if one else []

    def is_excluded(self, ticker: str) -> bool:
        return ticker.upper() in {t.upper() for t in self.exclusions}

    def exclusion_reason(self, ticker: str) -> str:
        for t, reason in self.exclusions.items():
            if t.upper() == ticker.upper():
                return reason
        return ""

    def validate(self) -> List[str]:
        """Return a list of human-readable problems; empty means usable."""
        problems: List[str] = []
        if not 0 <= self.rsi_min <= 100 or not 0 <= self.rsi_max <= 100:
            problems.append("RSI bounds must be within 0-100")
        if self.rsi_min > self.rsi_max:
            problems.append(f"rsi_min ({self.rsi_min}) must be <= rsi_max ({self.rsi_max})")
        if self.delta_min > self.delta_max:
            problems.append(f"delta_min ({self.delta_min}) must be <= delta_max ({self.delta_max})")
        if not self.delta_min <= self.delta_target <= self.delta_max:
            problems.append(
                f"delta_target ({self.delta_target}) must sit inside "
                f"[{self.delta_min}, {self.delta_max}]"
            )
        if self.dte_min > self.dte_max:
            problems.append(f"dte_min ({self.dte_min}) must be <= dte_max ({self.dte_max})")
        if self.dte_min < 0:
            problems.append("dte_min must be >= 0")
        if self.account_size < 0:
            problems.append("account_size must be >= 0")
        if not 0 < self.risk_pct_per_trade <= 1:
            problems.append("risk_pct_per_trade must be in (0, 1]")
        if self.max_concurrent_positions < 1:
            problems.append("max_concurrent_positions must be >= 1")
        if self.time_stop_minutes < 1:
            problems.append("time_stop_minutes must be >= 1")
        if self.max_spread_pct_of_mid <= 0:
            problems.append("max_spread_pct_of_mid must be > 0")
        if self.min_open_interest < 0:
            problems.append("min_open_interest must be >= 0")
        if str(self.trend_ma_type).strip().upper() not in ("SMA", "EMA"):
            problems.append(f"trend_ma_type must be SMA or EMA; got {self.trend_ma_type!r}")
        if self.trend_ma_period < 2:
            problems.append("trend_ma_period must be >= 2")
        return problems


def from_dict(raw: Dict[str, Any]) -> DayTradingConfig:
    """Build a config from a raw mapping, ignoring unknown keys."""
    known = {f.name for f in fields(DayTradingConfig)}
    kwargs = {k: v for k, v in (raw or {}).items() if k in known}

    if "watchlist" in kwargs and kwargs["watchlist"] is not None:
        kwargs["watchlist"] = [str(t).strip().upper() for t in kwargs["watchlist"] if str(t).strip()]
    if "notify_emails" in kwargs and kwargs["notify_emails"] is not None:
        kwargs["notify_emails"] = [str(e).strip() for e in kwargs["notify_emails"]
                                   if str(e).strip()]
    if "exclusions" in kwargs and kwargs["exclusions"] is not None:
        kwargs["exclusions"] = {
            str(k).strip().upper(): str(v) for k, v in (kwargs["exclusions"] or {}).items() if str(k).strip()
        }
    return DayTradingConfig(**kwargs)


def load_config(profile: str) -> DayTradingConfig:
    """Load the DayTrading block from the merged profile config."""
    merged = load_merged_config(profile)
    return from_dict(merged.get(CONFIG_KEY) or {})


def save_config(profile: str, cfg: DayTradingConfig) -> None:
    """Persist the DayTrading block back into the user's profile YAML."""
    payload = asdict(cfg)
    # Exclusions are a mapping; an emptied one must clear rather than merge —
    # deep_merge already treats an explicit empty map as a full override.
    save_profile(profile, {CONFIG_KEY: payload})
