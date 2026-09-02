# Build spec: DayTrading tab

Implement a new **DayTrading** tab in this application. It evaluates a fixed watchlist
against an opening-range breakout ruleset each morning and surfaces a single
recommendation per ticker per day. It is a decision-support tool: it never places orders.

Work through this document in order. Sections 1 and 2 are prerequisites — do not start
Section 3 until they are done, because Section 1 may change numbers the rest of the
system depends on.

---

## 0. Ground rules

**Do first, before writing any feature code:** read the existing codebase and report back
what you found for each of these. Match existing patterns rather than inventing new ones.

- Which UI framework the app uses and how existing tabs/pages are registered. This spec
  assumes Streamlit (`st.tabs`) as the likely case; if it is Flask, FastAPI + a JS frontend,
  Dash, or anything else, keep the module structure below and adapt only the view layer.
- How configuration is currently persisted (JSON file, SQLite, TOML, env). Reuse it.
- Where the existing yfinance and Public API clients live. Reuse them; do not write new
  HTTP clients.
- The existing RSI / SMA / historical-volatility implementations and where they live.

**Non-goals — do not build these:**
- No order placement, broker authentication, or execution of any kind.
- No auto-trading, no scheduled unattended firing, no notifications that could be mistaken
  for an instruction to trade.
- No backtesting engine in this tab. That is separate work.
- Do not modify the existing cash-secured-put or covered-call modules. If you need to
  refactor shared code, extract it rather than editing those modules' behaviour.

**New dependencies:** `pandas_market_calendars` only. If you believe anything else is
required, stop and ask rather than adding it.

**When ambiguous:** stop and ask. Do not guess at a threshold, a session window, or a
data source. Every number in this spec is deliberate.

---

## 1. Audit tasks — these may be corrupting existing output

Report findings for each before proceeding. Two of these likely affect code already in
production use elsewhere in the app.

### 1.1 RSI smoothing method

Locate the existing RSI(14) implementation. Determine whether it uses **Wilder's
smoothing** or a **simple rolling mean**.

```python
# CORRECT — Wilder
up = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
dn = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
rsi = 100 - 100 / (1 + up / dn)

# WRONG — simple rolling mean
up = delta.clip(lower=0).rolling(14).mean()
```

These diverge materially. On test data the two methods returned **57.7 vs 63.3** — a
5.6-point gap. The DayTrading gate bands RSI at 50–75, so a 5.6-point error changes which
days pass.

If the existing code uses a rolling mean: fix it to Wilder, and explicitly report that
historical RSI values displayed elsewhere in the app will change. Do not silently alter
behaviour other features depend on without flagging it.

### 1.2 yfinance `auto_adjust` default

Current yfinance defaults `auto_adjust=True`, so `Close` is the split- and
dividend-adjusted series. Audit every yfinance call in the codebase.

The rule is:
- **Indicators** (RSI, MACD, SMA, ATR) → adjusted series, `auto_adjust=True`.
- **Reference levels** (`prev_close`, `overnight_high`, strike selection, anything compared
  against a live quote or an option strike) → raw prints, `auto_adjust=False`.

Carry both frames. If `prev_close` is currently taken from an adjusted series, every
ex-dividend date silently shifts a level the strategy trades against, and the number still
looks plausible — so this will not surface as an obvious bug.

### 1.3 Extended-hours data availability

Write a throwaway script that calls:

```python
yf.download(tkr, interval="5m", period="60d", prepost=True,
            auto_adjust=False, progress=False)
```

for each watchlist ticker, and verify that bars actually exist in the 16:00→09:29 ET
window. Report the count of extended-hours bars per ticker per session.

If extended-hours bars are absent or sparse, `overnight_high` is unreliable. In that case
do **not** silently fall back to a regular-session high — surface the condition as a
degraded-data state in the UI (see §7.5). A wrong level is worse than a missing one.

---

## 2. Module structure

Create a `daytrading/` package (or match the app's existing layout convention):

```
daytrading/
  __init__.py
  config.py        # watchlist persistence, thresholds, exclusions
  calendar.py      # session state, half days, holidays  (pandas_market_calendars)
  data.py          # yfinance + Public fetch, caching, degraded-state detection
  indicators.py    # MACD, ATR, RSI(Wilder), SMA, VWAP, opening range, volume baseline
  signals.py       # gate + trigger evaluation, pure functions
  contracts.py     # option chain filtering and selection
  sizing.py        # position size and exit levels
  views.py         # the DayTrading tab UI
  tests/
```

`signals.py`, `indicators.py`, and `sizing.py` must be **pure functions over DataFrames** —
no network calls, no I/O, no clock reads. They take data and a timestamp as arguments.
This is what makes them testable and what will later let the same code run a backtest.

---

## 3. Configuration (`config.py`)

Persist using whatever mechanism the app already uses.

```python
@dataclass
class DayTradingConfig:
    watchlist: list[str]              # default: ["SPY","QQQ","AAPL","GOOGL","AMZN","META","TSLA"]
    exclusions: dict[str, str]        # ticker -> reason, e.g. {"MSFT": "employer stock"}

    # Daily name gate
    rsi_min: float = 50.0
    rsi_max: float = 75.0
    require_macd_above_signal: bool = True
    require_close_above_sma20: bool = True

    # Trigger window
    or_start: str = "09:30"           # ET
    or_end: str   = "09:40"           # inclusive; the 09:30, 09:35, 09:40 bars
    trigger_start: str = "09:45"
    trigger_cutoff: str = "11:00"     # no entries after this

    # Contract
    dte_min: int = 3
    dte_max: int = 5
    delta_min: float = 0.60
    delta_max: float = 0.70
    delta_target: float = 0.65
    max_spread_pct_of_mid: float = 0.03
    min_open_interest: int = 500

    # Risk
    account_size: float = 0.0         # user-entered, ring-fenced amount
    risk_pct_per_trade: float = 0.005
    max_concurrent_positions: int = 2
    time_stop_minutes: int = 45
    hard_close: str = "15:30"

    # Optional gates — see note below
    block_on_earnings_in_window: bool = True
    require_market_gate: bool = False
```

**Two toggles need explaining, because the defaults are a judgement call:**

- `block_on_earnings_in_window` defaults **True**. Holding a 3–5 DTE call through an
  earnings print is a materially different bet from the one this ruleset describes — the
  IV crush can take the position out even when the direction is right. Flip to False to
  disable.
- `require_market_gate` defaults **False**, matching the user's stated preference to keep
  index context out of the signal. SPY and QQQ context is still fetched and displayed
  (§7.2), just non-blocking. Flip to True to make it a hard gate.

Surface both as checkboxes in the UI so the choice is visible rather than buried.

**Exclusions** are a first-class concept, not a comment. A ticker in `exclusions` is shown
in the watchlist greyed out with its reason, is never evaluated, and never produces a
signal. Two reasons this exists:

1. **Employer stock.** Insider-trading policies commonly prohibit employees from
   short-term or speculative trading in company stock, and most ban derivatives on it
   outright.
2. **Names already held.** Day-trading calls on a name where the user holds long stock
   with a covered-call overlay creates rolling wash sales and can create offsetting
   positions under the straddle rules.

When a user adds a ticker, prompt once: *"Do you hold this, or work for this company?"*
with a one-click path to add it as an exclusion instead.

---

## 4. Market calendar (`calendar.py`)

Use `pandas_market_calendars` with the NYSE calendar. No API, no key, fully offline.

```python
def session_info(d: date) -> SessionInfo:
    """is_trading_day, market_open, market_close, is_half_day, early_close_time"""

def session_state(now_et: datetime) -> Literal["closed","premarket","regular","afterhours"]:
    ...
```

Do **not** use yfinance's `.info["marketState"]` — it comes from Yahoo's scraped, rate-limited
endpoint and is not something a trading decision should depend on. Derive state from the
calendar plus `zoneinfo`.

Verified reference values for NYSE 2026: 251 trading sessions, exactly two early closes —
**27 Nov 2026** and **24 Dec 2026**, both closing 13:00 ET. Use these in a test.

On a half day the hard close moves to `early_close - 30 minutes` and theta arrives faster;
surface a banner in the UI.

**Every timestamp in the system is `America/New_York`.** Convert once at the data boundary
and never again. Never store or compare naive datetimes.

---

## 5. Data layer (`data.py`)

### 5.1 Two-stage fetch

This split is what keeps the morning fast. Do not collapse it.

**Warm-up — run once per day, ideally ~08:45 ET, cache to parquet keyed by date:**

```python
# 250 daily bars, adjusted, for indicators
daily_adj = yf.download(tickers, period="250d", interval="1d",
                        auto_adjust=True,  progress=False)

# same window, raw, for reference levels
daily_raw = yf.download(tickers, period="250d", interval="1d",
                        auto_adjust=False, progress=False)

# 60 days of 5m bars incl. extended hours — the only heavy call
intraday = yf.download(tickers, interval="5m", period="60d", prepost=True,
                       auto_adjust=False, progress=False)
```

From the warm-up, compute and cache: daily indicators, `prev_close` (raw), and the
time-of-day volume baseline. ~20–40 s wall clock for 9 tickers.

**Live poll — every 5 minutes from 09:45 to the trigger cutoff:**

```python
today = yf.download(tickers, interval="5m", period="1d", prepost=True,
                    auto_adjust=False, progress=False)
```

192 rows per ticker. ~3–6 s. Cache with a 60-second TTL so repeated UI reloads within one
bar do not re-hit Yahoo.

### 5.2 Rate limiting

Yahoo throttles aggressively and yfinance has no published quota, so the failure mode is a
429, not a slow response. Required:

- Exponential backoff with jitter on 429 and on connection errors.
- Small thread count (start at 2–4, not the default).
- Never re-pull the 60-day frame more than once per day.
- A circuit breaker: after 3 consecutive failures, stop polling, surface the state in the
  UI, and do not retry until the user clicks refresh.

### 5.3 Halt status

Poll `http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts` — free, no auth, refreshed once
a minute. Do not poll more than once a minute. Parse for watchlist symbols.

Local backstop: a regular-session 5-minute bar with zero volume on a mega-cap is a strong
halt signal. Treat either as blocking.

### 5.4 Options chain

Use the existing Public client. Fetch only when a signal fires, for the one ticker that
fired — never pre-emptively for the whole watchlist. Pair with the underlying quote as the
existing code already does, and record both timestamps; greeks are meaningless without the
underlying price they were computed against.

Fallback if Public is unavailable: compute greeks with `py_vollib` from underlying, strike,
DTE and IV. At 3–5 DTE the risk-free rate barely moves delta — hardcode ~4% rather than
adding a rates dependency.

---

## 6. Indicators and signals

### 6.1 `indicators.py` — daily

All exponential and Wilder smoothing uses `adjust=False`, which is what charting platforms
display.

```python
ema = lambda s, n: s.ewm(span=n, adjust=False).mean()

macd_line   = ema(close, 12) - ema(close, 26)
macd_signal = ema(macd_line, 9)
macd_hist   = macd_line - macd_signal

def rsi(close, n=14):                     # Wilder
    d  = close.diff()
    up = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)

def atr(high, low, close, n=14):          # Wilder
    pc = close.shift()
    tr = pd.concat([high - low, (high - pc).abs(), (low - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/n, adjust=False).mean()

sma_20  = close.rolling(20).mean()
avg_vol = volume.rolling(20).mean()
```

250 daily bars is deliberate: MACD's EMAs need long warm-up to converge. Measured error vs
a 300-bar reference — 40 bars: 0.035, 100 bars: 0.0006, 150 bars: 0.00005. 250 is margin.

### 6.2 `indicators.py` — intraday

```python
bars.index = bars.index.tz_convert("America/New_York")
rth = bars[bars.index.normalize() == today].between_time("09:30", "15:55")

# Opening range: the bars stamped 09:30, 09:35, 09:40
orb        = rth.between_time("09:30", "09:40")
or_high    = orb.High.max()
or_low     = orb.Low.min()
or_height  = or_high - or_low
or_avg_vol = orb.Volume.mean()

# Session VWAP, anchored 09:30 — NOT anchored to the pre-market open
tp   = (rth.High + rth.Low + rth.Close) / 3
vwap = (tp * rth.Volume).cumsum() / rth.Volume.cumsum()

# Overnight high: 16:00 prior session -> 09:29:59 today
on = bars.loc[f"{prev_session} 16:00" : f"{today} 09:29"]
overnight_high = on.High.max()

# Time-of-day volume baseline, prior 20 sessions
tod = bars.between_time("09:30", "15:55").copy()
tod["slot"] = tod.index.strftime("%H:%M")
prior    = tod[tod.index.normalize() < today]
baseline = prior.groupby("slot").Volume.mean()
```

**Bar labelling:** confirm empirically whether a bar stamped 09:30 covers 09:30–09:35 or
09:25–09:30. Assert it in a test. An off-by-one convention silently shifts the opening
range by five minutes and nothing will look wrong.

VWAP from 5-minute typical price tracks 1-minute VWAP to ~0.04 basis points, so 5-minute
bars are sufficient. Do not add a 1-minute feed for this.

### 6.3 `signals.py` — evaluation order

Pure functions. Each returns a result object carrying **which condition failed and its
actual value**, not just a boolean — the UI shows the user why a name did not qualify.

**Stage A — daily name gate** (on the prior completed session):
- `rsi_min <= rsi_14_daily <= rsi_max`
- `macd_line > macd_signal`
- `close > sma_20`
- No earnings inside the option's life, if `block_on_earnings_in_window`

**Stage B — setup gate** (~09:29 ET):
- `today_open > prev_close` (raw, unadjusted). No ceiling on gap size.
- Record `overnight_high`.

**Stage C — trigger** (each completed 5m bar close, 09:45 → cutoff). ALL must hold on the
same bar:
- `bar.close > or_high`
- `bar.close > overnight_high`
- `bar.close > vwap` at that bar
- `bar.volume > or_avg_vol`

Evaluate on the **close of a completed bar only** — never mid-bar. A mid-bar evaluation
produces signals that vanish, which destroys trust in the tool faster than anything else.

Fire at most **once per ticker per day**. Record the firing bar and freeze it.

Compute and display `relative_volume` (bar volume ÷ time-of-day baseline) alongside, as
context. It is not a gate.

---

## 7. Contract selection, sizing, exits

### 7.1 `contracts.py`

Filter calls: `dte_min <= DTE <= dte_max`, `delta_min <= delta <= delta_max`,
`spread_pct_of_mid <= max_spread_pct_of_mid`, `open_interest >= min_open_interest`.
Rank by `abs(delta - delta_target)`, then by tightest spread.

If nothing qualifies, report **which filter eliminated the candidates and the closest
near-miss**. "No contract found" with no explanation is a dead end for the user.

Always display: strike, expiry, DTE, bid, ask, mid, spread as % of mid, delta, theta, IV,
open interest, today's option volume, and the underlying price at quote time.

### 7.2 `sizing.py`

```python
stop_level        = max(or_high, vwap_at_entry)   # whichever is hit first going down
stop_distance     = underlying_price - stop_level
risk_dollars      = account_size * risk_pct_per_trade
risk_per_contract = delta * stop_distance * 100
contracts         = floor(risk_dollars / risk_per_contract)
```

Show every intermediate value, not just the final contract count. If `account_size` is 0,
show the sizing formula with a prompt to enter it rather than suggesting a quantity.

Exit levels to display on the signal card:

| Level | Value |
|---|---|
| Stop | `max(or_high, vwap)` — on the **underlying**, never on the option price |
| Target 1 | `entry + or_height` (measured move); also show 1R = `entry + stop_distance` |
| Time stop | `entry_time + 45 min` — show as a live countdown |
| Hard close | 15:30 ET, or `early_close - 30 min` on a half day |

Label the stop explicitly as an underlying-price stop. Option quotes gap and spreads widen;
a stop on the option price triggers on noise.

---

## 8. The DayTrading tab (`views.py`)

Five panels. Match the app's existing visual conventions.

### 8.1 Watchlist configuration
Add/remove tickers. On add: validate the symbol resolves, has a listed option chain, and
is not already present. Prompt the hold/employer question from §3 and offer to add as an
exclusion. Excluded tickers render greyed with their reason and an "un-exclude" control.

### 8.2 Pre-market readiness (before 09:45)
One row per ticker: daily gate pass/fail with the actual RSI, MACD histogram and SMA-20
distance; gap vs `prev_close`; `overnight_high`; pre-market volume vs its own norm; next
earnings date; a ready / not-ready / excluded chip.

SPY and QQQ context — previous close, last, VWAP — displayed at the top of the panel
regardless of whether `require_market_gate` is on. These names run 0.6–0.9 correlated to
the index intraday, so this is the context every signal sits inside.

### 8.3 Live trigger monitor (09:45 → cutoff)
One row per qualifying ticker: OR high/low, current price, VWAP, last bar volume vs OR
average, relative volume, and trigger status. Show the countdown to the 11:00 cutoff and
grey the panel out after it passes.

### 8.4 Signal card (on fire)
Ticker, firing bar timestamp, all four trigger values as they were on that bar, the
selected contract, sizing breakdown, and the exit table. A prominent line stating this is a
recommendation and no order has been or will be placed.

### 8.5 Data health
Non-negotiable given the fragility of the upstream sources. Show: last warm-up time and
cache age; whether extended-hours bars were actually returned (per §1.3) and a clear
**degraded** state if not; consecutive API failure count and circuit-breaker state; halt
feed last-fetch time; and whether today is a half day.

If any input is degraded, the affected tickers must show as **degraded, not as no-signal**.
Silence that looks like "no setup today" when it actually means "the data did not load" is
the most dangerous failure this tool can have.

---

## 9. Tests

- Each indicator against a hand-checked fixture. Include an explicit test that Wilder RSI
  and rolling-mean RSI differ, so a future refactor cannot silently regress it.
- Bar-labelling convention assertion (§6.2).
- NYSE 2026 calendar: 251 sessions; early closes on 27 Nov and 24 Dec, both 13:00 ET.
- Golden-file test: a recorded session's 5m bars → expected OR, VWAP series, and trigger
  bar. Commit the fixture.
- Trigger fires at most once per ticker per day.
- Mid-bar evaluation never fires.
- Half-day path adjusts the hard close.
- "No qualifying contract" returns the eliminating filter and the near-miss.
- Degraded-data path surfaces degraded rather than no-signal.
- Timezone: no naive datetime reaches any comparison. Assert it.

---

## 10. Deliverable

When done, report:
1. Findings from all three §1 audit tasks, and anything you changed as a result.
2. The UI framework found and how the tab was registered.
3. Measured wall-clock for warm-up and for one live poll, on the real watchlist.
4. Anything in this spec that conflicted with the existing codebase, and how you resolved it.

Expected performance for reference — 9 tickers, measured on synthetic data at true volume:
warm-up compute 175 ms, live re-evaluation 5 ms. Compute is not the constraint; network
latency and rate limiting are. If your numbers are wildly different, something is wrong.
