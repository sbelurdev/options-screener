# Options Screener — Design Document

## Overview

An educational options screener that analyses covered call (CC) and cash-secured put (CSP) opportunities across a configurable list of tickers. It pulls market data, filters and scores options contracts, and produces ranked recommendations with an HTML/CSV report.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                         main.py                               │
│   Parse CLI args → Load config.yaml → Setup logging          │
└───────────────────────────┬───────────────────────────────────┘
                            │ run_pipeline(config, logger)
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                      pipeline.py                              │
│                                                               │
│  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────┐  │
│  │  OptionsChain   │  │   MarketData     │  │Fundamentals │  │
│  │    Provider     │  │    Provider      │  │  Provider   │  │
│  └────────┬────────┘  └────────┬─────────┘  └──────┬──────┘  │
│           │                    │                    │         │
│     options chain         price history        earnings date  │
│     expirations           OHLCV data                         │
│           │                    │                    │         │
│           └────────────────────┴────────────────────┘         │
│                                │                              │
│                    _process_ticker() × each ticker            │
│                                │                              │
│          ┌─────────────────────┼─────────────────────┐        │
│          ▼                     ▼                     ▼        │
│    technicals          expiration selection       earnings     │
│    (MA, RSI, HV)      (all ≤14d + Fridays)       date        │
│          │                     │                     │        │
│          └─────────────────────┼─────────────────────┘        │
│                                ▼                              │
│                   build_option_records() + score_candidate()  │
│                                │                              │
│              ┌─────────────────┴──────────────────┐           │
│              ▼                                     ▼          │
│   build_cc_recommendations()          build_csp_recommendations()
│              │                                     │          │
│              └──────────────────┬──────────────────┘          │
│                                 ▼                             │
│                          write_reports()                      │
│                     (CSV + HTML to ./reports)                 │
└───────────────────────────────────────────────────────────────┘
```

---

## Pulled from Providers vs. Calculated Locally

This is the single most important thing to understand about the data pipeline.

```
┌─────────────────────────────────────────────────────────────────────┐
│               PULLED FROM DATA PROVIDERS                            │
├─────────────┬───────────────────────────────────────────────────────┤
│ yfinance    │ Price history (OHLCV, 1y daily)                       │
│             │ Options expiration dates                               │
│             │ Options chain per expiration:                          │
│             │   contractSymbol, strike, bid, ask, lastPrice,        │
│             │   volume, openInterest, impliedVolatility              │
│             │ ⚠ Delta is NOT provided by yfinance                   │
│             │ Earnings date (calendar / earnings_dates endpoints)    │
├─────────────┼───────────────────────────────────────────────────────┤
│ Public.com  │ Options expiration dates                               │
│ (primary    │ Options chain per expiration (same fields as above)    │
│  if key set)│ Delta ← from dedicated greeks API endpoint            │
│             │   (/option-details/{account_id}/greeks)               │
│             │ Implied Volatility ← also from greeks endpoint        │
│             │   (used as fallback if not in chain response)          │
└─────────────┴───────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│               CALCULATED LOCALLY (never from any provider)          │
├──────────────────────────┬──────────────────────────────────────────┤
│ Spot price               │ Last close from price history            │
├──────────────────────────┼──────────────────────────────────────────┤
│ MA20, MA50               │ Rolling mean of close prices             │
│ RSI14                    │ Wilder's RSI on daily returns            │
│ HV20                     │ 20-day annualised std-dev of returns     │
├──────────────────────────┼──────────────────────────────────────────┤
│ IVR / HV Rank            │ (current HV − hv_low) / (hv_high−hv_low)│
│                          │ Uses 1-year HV series, never option IV   │
├──────────────────────────┼──────────────────────────────────────────┤
│ Delta (fallback only)    │ Black-Scholes when not from provider:    │
│                          │   needs IV + risk_free_rate in config    │
│                          │   → defaults to 0 if neither available  │
├──────────────────────────┼──────────────────────────────────────────┤
│ Annualized yield         │ (mid × 100) / collateral × (365 / DTE)  │
│ Breakeven                │ strike − premium (PUT) / spot − premium  │
│ OTM%                     │ (strike − spot) / spot                   │
│ Bid-ask spread %         │ (ask − bid) / mid                        │
├──────────────────────────┼──────────────────────────────────────────┤
│ Support levels           │ low_52w, swing_low_20d from price history│
│ Resistance levels        │ high_52w, swing_high_20d from price hist │
├──────────────────────────┼──────────────────────────────────────────┤
│ Candidate score [0–1]    │ Weighted: income + delta + trend +       │
│                          │ liquidity; earnings penalty applied      │
└──────────────────────────┴──────────────────────────────────────────┘
```

### Delta Source Priority Chain

```
For every option contract:

  1. Public.com greeks API        ← most accurate (real-time greeks)
       ↓ not available
  2. yfinance chain delta column  ← yfinance rarely populates this
       ↓ not available
  3. Black-Scholes (local)        ← requires IV + risk_free_rate in config
       d1 = (ln(S/K) + (r + σ²/2)t) / (σ√t)
       CALL delta = N(d1);  PUT delta = N(d1) − 1
       ↓ IV also missing
  4. OTM% range check             ← delta-less fallback for filter only
       ↓ fails range check
  5. Default 0.0                  ← logged as warning; contract kept
```

### Implied Volatility Source (per contract)

```
  Public.com option-chain response  ← preferred
       ↓ absent in response
  Public.com greeks endpoint        ← fallback within Public
       ↓ Public not available
  yfinance impliedVolatility column ← always populated by yfinance
```

### What IVR Is — and Is Not

IVR (shown in the report) is **not** a live IV Rank from a volatility data service. It is a locally-computed **HV Rank** proxy:

```
  HV series  = rolling 20-day annualised HV over the price history period (1y)
  IVR proxy  = (today's HV − min HV over period) / (max HV − min HV) × 100

  Option IV is shown alongside the HV rank for context but is NOT used
  in the formula — option IV includes a risk premium above realised HV,
  which would inflate the rank, especially for leveraged ETFs.
```

---

## Data Providers

### What Each Provider Returns

```
┌────────────────────────────────────────────────────────────────┐
│                    DATA PROVIDERS                              │
│                                                                │
│  MarketDataProvider (yfinance only)                            │
│  ─────────────────────────────────                            │
│  get_price_history(ticker, period="1y", interval="1d")         │
│  Returns: OHLCV DataFrame (Date, Open, High, Low, Close, Vol)  │
│  Used for: technicals (MA, RSI, HV), IVR proxy, support/       │
│            resistance levels                                   │
│                                                                │
│  OptionsChainProvider (yfinance or Public.com + fallback)      │
│  ──────────────────────────────────────────────────────────    │
│  get_options_expirations(ticker)                               │
│  Returns: List[date] — all available expiration dates          │
│                                                                │
│  get_options_chain(ticker, expiration)                         │
│  Returns: (calls_df, puts_df) with columns:                    │
│    contractSymbol, strike, bid, ask, lastPrice,               │
│    volume, openInterest, impliedVolatility, delta (if avail)  │
│                                                                │
│  FundamentalsProvider (yfinance only)                          │
│  ─────────────────────────────────                            │
│  get_earnings_date(ticker)                                     │
│  Returns: date | None — next earnings announcement date        │
└────────────────────────────────────────────────────────────────┘
```

### Provider Selection & Fallback

```
config: options_data_provider = "public"

          PUBLIC_API_KEY env var set?
                │
        ┌───────┴───────┐
       No              Yes
        │               │
        ▼               ▼
   Warn + use      _FallbackOptionsProvider
   yfinance             │
                 ┌──────┴──────────────────┐
                 │  1. Try Public.com API   │
                 │     get_expirations()    │
                 │     get_chain()          │
                 │     get_greeks()         │
                 │  2. On error/empty:      │
                 │     → yfinance fallback  │
                 │     → inject Public      │
                 │       delta into yf chain│
                 │  3. Log fallback events  │
                 │     → HTML warning banner│
                 └─────────────────────────┘

config: options_data_provider = "yfinance"
  → YFinanceProvider directly (no fallback layer)

market_data_provider: always yfinance
fundamentals_provider: always yfinance
```

> **Delta source priority:** Public greeks API → yfinance provided delta → Black-Scholes (if IV + risk_free_rate available) → OTM% fallback → default 0.0

---

## Step 1 — Expiration Date Selection

All available expiration dates are fetched from the provider. Dates beyond `max_dte` (default 45) are discarded (hard cap). The remainder are all fetched — no single-expiration-per-bucket picking.

```
All expirations from provider
          │
          ▼
  Filter: 1 ≤ DTE ≤ max_dte (45)   ← hard cap, excludes same-day & LEAPS
          │
          ▼
 ┌──────────────────────────────────────────────────────┐
 │  EXPIRATION SELECTION (options_metrics.py)           │
 │                                                      │
 │  DTE ≤ 14  (Short-Term window):                      │
 │    ALL available expirations included                │
 │    (captures daily and weekly options)               │
 │                                                      │
 │  14 < DTE ≤ 45  (Medium/Long-Term window):           │
 │    Friday expirations only (standard weekly/monthly) │
 └──────────────────────────────────────────────────────┘
          │
          ▼
  Selected dates × each ticker → fetch options chain per date

Each expiration is tagged with a term label based on DTE:
  DTE ≤ 14        → Short-Term
  15 ≤ DTE ≤ 28   → Medium-Term
  DTE > 28        → Long-Term
```

---

## Step 2 — Technical Indicators

Computed from price history once per ticker, attached to every candidate record.

```
Price history DataFrame (1y, 1d)
          │
          ▼
  compute_technicals()  (technicals.py)
  ─────────────────────────────────────
  spot    = Close.iloc[-1]
  ma20    = Close.rolling(20).mean().iloc[-1]
  ma50    = Close.rolling(50).mean().iloc[-1]
  rsi14   = Wilder's RSI(14) on daily returns
  hv20    = Close.pct_change().rolling(20).std() × √252
            (annualised 20-day historical volatility)
```

---

## Step 3 — Options Candidate Filtering

Each contract in the options chain passes through 8 sequential filters. First failure eliminates the contract. Logged to CSV for debugging.

```
Options chain (all strikes for one expiration)
          │
          ▼
  build_option_records()  (options_metrics.py L167+)

  ┌─ FILTER 1: Valid strike, bid, ask (all > 0)
  │
  ├─ FILTER 2: OTM only
  │    CALL: strike > spot
  │    PUT:  strike < spot
  │
  ├─ FILTER 3: Open interest ≥ min_open_interest  (if configured)
  │
  ├─ FILTER 4: Volume ≥ min_volume               (if configured)
  │
  ├─ FILTER 5: Bid-ask spread ≤ max_spread_pct   (if configured)
  │    spread_pct = (ask − bid) / mid
  │
  ├─ FILTER 6: Annualized yield ≥ min_annualized_yield (12%)
  │    PUT yield  = (mid × 100) / (strike × 100) × (365 / DTE)
  │    CALL yield = (mid × 100) / (spot   × 100) × (365 / DTE)
  │
  ├─ FILTER 7: Delta / OTM% in configured range
  │    Delta source priority:
  │      1. Provided by data source (Public greeks or yfinance)
  │      2. Black-Scholes: d1 = (ln(S/K) + (r+σ²/2)t) / (σ√t)
  │         CALL delta = N(d1);  PUT delta = N(d1) − 1
  │      3. OTM%: 5%–15% from spot (if no delta available)
  │    PUT range:  −0.25 ≤ delta ≤ −0.10
  │    CALL range:  0.10 ≤ delta ≤  0.25
  │
  └─ PASSED → build candidate record
               (30 fields including all greeks, technicals, earnings flag)
```

---

## Step 4 — Candidate Scoring

Surviving candidates are ranked. Top 5 per bucket per strategy flow to the recommendation engines.

```
  score_candidate()  (score.py)

  Score = weighted sum of 4 components, then earnings penalty

  ┌───────────────────────────────────────────────────────┐
  │  Component         Weight  Formula                    │
  ├───────────────────────────────────────────────────────┤
  │  Income            40%     log1p(yield) / log1p(1.0)  │
  │  Delta accuracy    25%     1 − |delta − 0.20| / 0.25  │
  │                            (target: ±0.20)            │
  │  Technical trend   20%     PUT/CALL: spot vs MA20/MA50 │
  │                            penalty if RSI14 > 75      │
  │  Liquidity         15%     spread + OI + volume       │
  └───────────────────────────────────────────────────────┘
                               ×
  Earnings multiplier:   1 − 0.20 (if earnings before expiry)

  Final score: [0, 1]  →  top 5 per bucket kept
```

### Technical Trend Score Detail

```
  PUT (sell put → want stock to stay flat or rise):
    base  = 0.55
    +0.20 if spot > MA20  (short-term uptrend)
    +0.20 if spot > MA50  (medium-term uptrend)
    −0.20 if RSI > 75     (overbought, pullback risk)

  CALL (sell call → want stock to stay below strike):
    base  = 0.55
    +0.15 if spot > MA20, else −0.15
    +0.15 if spot > MA50, else −0.15
    −0.20 if RSI > 75     (overbought → call-away risk)
```

---

## Step 5 — Recommendation Engines

### Covered Call Recommender

```
Input: top-scored CALL candidates per ticker, split by DTE into 3 pools

  recommend_cc_for_ticker()
  ─────────────────────────
  Per term (Short-Term DTE≤14 / Medium-Term 15-28 / Long-Term >28):

    1. Sort candidates: delta-qualified first, then by score
    2. Take top N (max_suggestions_per_term = 3)
    3. For each selected candidate → verdict:

       VERDICT LOGIC (IVR is NOT used here):
       ┌───────────────────────────────────────────┐
       │  All of these OK?            → YES        │
       │    |delta| in 0.10–0.25                   │
       │    No earnings within 7d of expiry        │
       │    Strike ≥ min_acceptable_price           │
       ├───────────────────────────────────────────┤
       │  Any issue above?            → BORDERLINE │
       │    Shows reason (delta OOB, earnings,     │
       │    below min price)                       │
       └───────────────────────────────────────────┘

    4. Always ≥1 suggestion per term, even if all Borderline

  IVR is computed and displayed (HV Rank proxy) but
  does NOT affect the CC verdict.

  Flags shown for context (not verdict-affecting):
    ▲ resistance  — strike within 2% of 52w high / 20d swing high
    ○ round#      — strike within 1% of nearest $5 increment
    ⚠ below $X   — strike below user's cost basis floor
```

### Cash-Secured Put Recommender

```
Input: top-scored PUT candidates per ticker, split by DTE into 3 pools

  recommend_csp_for_ticker()
  ──────────────────────────
  Returns one recommendation per term per ticker (3 total):
    Short-Term (DTE ≤ 14) / Medium-Term (15–28) / Long-Term (>28)

       VERDICT LOGIC:
       ┌───────────────────────────────────────────┐
       │  Hard fails (→ NO):                       │
       │    IVR < 30%  (HV Rank below threshold)   │
       │    Earnings within 7d of expiry            │
       │    No delta-qualified strike (0.10–0.25)   │
       ├───────────────────────────────────────────┤
       │  Soft flags (→ BORDERLINE):               │
       │    IVR unavailable / at 0% / at 100%      │
       │    Strike above support level              │
       ├───────────────────────────────────────────┤
       │  All checks pass  → YES                   │
       └───────────────────────────────────────────┘

  Support levels (from price history):
    low_52w       — lowest close over the price history period
    swing_low_20d — 20-day rolling low of lows

  IVR proxy (HV Rank):
    current_HV = 20-day annualised HV (most recent)
    IVR = (current_HV − hv_low) / (hv_high − hv_low) × 100
    (option IV is noted but NOT used in the formula)
```

---

## Step 6 — Report Generation

```
  write_reports()  (render.py)
  ─────────────────────────────
  Outputs:
    ./reports/{date}_options_report.csv   ← all candidate records
    ./reports/{date}_options_report.html  ← interactive report

  HTML layout (top to bottom):
  ┌────────────────────────────────────────────────────┐
  │  ⚠ Provider fallback warning (if Public→yf used)   │
  ├────────────────────────────────────────────────────┤
  │  [CC Recommendations table]                        │
  │    Blue theme; 1 row per suggestion                │
  │    Columns: Ticker | Term | Verdict | Spot |       │
  │    Strike | %OTM | Exp | DTE | Premium | Delta |   │
  │    IVR★ | MaxProfit | Breakeven | AnnYield |       │
  │    Flags | Why                                     │
  │    ★ IVR displayed for reference only              │
  ├────────────────────────────────────────────────────┤
  │  [CSP Recommendations table]                       │
  │    Gold theme; 1 row per term per ticker           │
  │    Columns: Ticker | Term | Verdict | Spot |       │
  │    Strike | %ToStrike | Exp | DTE | Premium |      │
  │    Delta | IVR★ | MaxProfit | Breakeven | CashReq │
  │    AnnYield | Why                                  │
  ├────────────────────────────────────────────────────┤
  │  [Covered Calls screening] (collapsible, purple)   │
  │    Per-ticker collapsible blocks                   │
  │    Columns match rec table (minus Recommend/Term): │
  │    Term | Trade | Spot | Strike | %OTM | Exp |    │
  │    DTE | Premium | Delta | IVR | MaxProfit |       │
  │    Breakeven | AnnYield | Why                      │
  ├────────────────────────────────────────────────────┤
  │  [Cash-Secured Puts screening] (collapsible, green)│
  │    Same layout as Covered Calls above              │
  └────────────────────────────────────────────────────┘

  Verdict colours:  ■ Yes = green   ■ Borderline = yellow   ■ No = red
  Links: each ticker links to Fidelity options research page
```

---

## Key Config Parameters

| Parameter | Default | Effect |
|---|---|---|
| `covered_call_tickers` | — | Tickers screened for CALL candidates |
| `cash_secured_put_tickers` | — | Tickers screened for PUT candidates |
| `delta_call_min/max` | 0.10 / 0.25 | Delta range for CALL screening filter |
| `delta_put_min/max` | -0.25 / -0.10 | Delta range for PUT screening filter |
| `max_dte` | 45 | Hard cap — expirations beyond this ignored |
| `short_term_max_dte` | 14 | DTE ≤ 14 → Short-Term (all expirations) |
| `medium_term_max_dte` | 28 | DTE ≤ 28 → Medium-Term (Fridays only) |
| `min_annualized_yield` | 12% | Contracts below this are dropped |
| `earnings_risk_penalty` | 20% | Score reduction when earnings before expiry |
| `risk_free_rate` | 5% | Used in Black-Scholes delta calculation |
| `price_history_period` | 1y | Used for MA, RSI, HV, IVR proxy |
| `max_candidates_per_ticker_per_bucket` | 5 | Top N kept after scoring per bucket |
| `cc_recommendation.max_suggestions_per_term` | 3 | Suggestions shown per term in CC table |
| `cc_recommendation.delta_min/max` | 0.10 / 0.25 | Delta range for CC verdict |
| `csp_recommendation.ivr_min` | 30% | IVR hard floor for CSP verdict |
| `options_data_provider` | yfinance | `yfinance` or `public` |

---

## File Map

```
options-screener/
├── main.py                          ← CLI entry point
├── config.yaml                      ← all user configuration
│
├── agent/
│   ├── pipeline.py                  ← orchestrates the full run
│   │
│   ├── providers/
│   │   ├── base.py                  ← abstract interfaces
│   │   ├── yfinance_provider.py     ← yfinance implementation
│   │   ├── public_provider.py       ← Public.com API implementation
│   │   └── factory.py               ← provider selection + fallback wrapper
│   │
│   ├── signals/
│   │   ├── options_metrics.py       ← expiration selection, filtering, BS delta
│   │   └── technicals.py            ← MA20, MA50, RSI14, HV20
│   │
│   ├── scoring/
│   │   └── score.py                 ← multi-factor scoring (income/delta/trend/liquidity)
│   │
│   ├── recommendation/
│   │   ├── cc_recommender.py        ← covered call verdict engine
│   │   └── csp_recommender.py       ← cash-secured put verdict engine + IVR proxy
│   │
│   ├── reporting/
│   │   └── render.py                ← HTML + CSV report generation
│   │
│   └── utils/
│       ├── dates.py                 ← is_third_friday()
│       ├── env.py                   ← .env file loader
│       └── logging.py               ← logger setup (file + console)
│
└── logs/
    └── {ticker}_data.csv            ← per-ticker audit log of all API calls
```
