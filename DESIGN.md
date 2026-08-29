# Options Screener — Design Document

## Overview

An educational options screener that analyses covered call (CC) and cash-secured put (CSP) opportunities across a configurable list of tickers. It pulls market data, filters and scores options contracts, and produces ranked recommendations with an HTML/CSV report.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                         main.py                               │
│   Parse CLI args → Load config(s) / profile → Setup logging  │
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
│       _process_ticker() × each ticker (ThreadPool,            │
│       fetch_max_workers concurrent; results merged in          │
│       config order so reports stay deterministic)              │
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
│   (Short/Med/Long + Monthly CC)       (Short/Med/Long)        │
│              │                                     │          │
│              └──────────────────┬──────────────────┘          │
│                                 ▼                             │
│              record_recommendations() + evaluate_outcomes()   │
│              (outcome ledger: grade expired recs vs close)    │
│                                 │                             │
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
│  if key set)│ Delta + Theta ← from dedicated greeks API endpoint    │
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
│ ATM IV snapshot          │ Mean IV of strike nearest spot (call+put)│
│                          │ from expiry nearest 30 DTE; persisted to │
│                          │ iv_history.csv each run                  │
├──────────────────────────┼──────────────────────────────────────────┤
│ IVR                      │ True IV Rank over recorded IV history    │
│                          │ once ≥20 obs; until then HV-rank proxy:  │
│                          │ (current HV − hv_low) / (hv_high−hv_low)│
├──────────────────────────┼──────────────────────────────────────────┤
│ Delta (fallback only)    │ Black-Scholes when not from provider:    │
│                          │   needs IV + risk_free_rate in config    │
│                          │   → defaults to 0 if neither available  │
├──────────────────────────┼──────────────────────────────────────────┤
│ Expected fill price      │ bid + fill_price_factor × (ask − bid)    │
│                          │ (default 0.4 — sellers concede spread;   │
│                          │ all premium metrics below use this)      │
│ Annualized yield         │ (fill × 100) / collateral × (365 / DTE) │
│ Breakeven                │ strike − premium (PUT) / spot − premium  │
│ OTM%                     │ (strike − spot) / spot                   │
│ Bid-ask spread %         │ (ask − bid) / mid                        │
│ Theta yield              │ |theta| × 365 / collateral per share     │
│                          │ (theta from Public greeks; None on yf)   │
│ VRP                      │ option IV / HV20 — premium richness      │
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

### What IVR Is — Two-Tier: True IV Rank, then HV-Rank Proxy

IVR (shown in the report and used in the CSP verdict) comes from a two-tier source:

```
  TIER 1 — True IV Rank (preferred, from self-recorded IV history)
  ───────────────────────────────────────────────────────────────
  Every run records one ATM IV snapshot per ticker into
  iv_history_path (./cache/iv_history.csv, shared across profiles):
    - ATM IV  = mean IV of the strike nearest spot (call + put side),
                taken from the expiration closest to 30 DTE (IV30 style)
    - One row per (date, ticker); same-day re-runs overwrite

  Once ≥ iv_rank_min_history_days (20) observations exist in the
  trailing year:
    IV Rank = (current IV − 1yr low IV) / (1yr high IV − 1yr low IV) × 100

  TIER 2 — HV Rank proxy (fallback while history accumulates)
  ───────────────────────────────────────────────────────────
  HV series  = rolling 20-day annualised HV over the price history period
  IVR proxy  = (today's HV − min HV over period) / (max HV − min HV) × 100

  Option IV is shown alongside for context but is NOT used in the proxy
  formula — option IV includes a risk premium above realised HV, which
  would inflate the rank, especially for leveraged ETFs.

  The ivr_source string in reports identifies which tier produced the
  value ("true IV rank (N obs; ...)" vs "proxy: HV rank (...)").
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
                 │       delta + theta      │
                 │       into yf chain      │
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
  ├─ FILTER 6: Annualized yield ≥ threshold
  │    PUT threshold:  min_annualized_yield (12%)
  │    CALL threshold: cc_recommendation.min_yield when set (it REPLACES
  │                    the global for calls), else min_annualized_yield
  │    fill = bid + fill_price_factor × (ask − bid)   ← expected fill, not mid
  │    PUT yield  = (fill × 100) / (strike × 100) × (365 / DTE)
  │    CALL yield = (fill × 100) / (spot   × 100) × (365 / DTE)
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
  score_candidates()  (score.py) — pooled per ticker + strategy

  Score = weighted sum of 6 components, then earnings penalty.
  Weights, delta target, and income cap are configurable under the
  scoring: block in config (weights are normalized by their sum).

  ┌────────────────────────────────────────────────────────────────┐
  │  Component   Default  Formula                                  │
  ├────────────────────────────────────────────────────────────────┤
  │  Income        35%    50/50 blend of:                          │
  │                       · absolute: log1p(ev_yield)/log1p(cap)   │
  │                         (cap = income_yield_cap, default 150%) │
  │                       · percentile of ev_yield within the      │
  │                         ticker+strategy pool (all expirations) │
  │                       ev_yield = yield × (1 − |Δ|)             │
  │                       (|Δ| ≈ P(assignment))                    │
  │  Delta         20%    1 − |delta − delta_target| / 0.25        │
  │                       (delta_target default ±0.20)             │
  │  Trend         15%    PUT/CALL: spot vs MA20/MA50,             │
  │                       penalty if RSI14 > 75                    │
  │  Liquidity     10%    spread + OI + volume                     │
  │  VRP           10%    (IV/HV20 − 0.8) / 0.8, clamped 0–1       │
  │                       (premium richness vs realised vol;       │
  │                       0.8x → 0, 1.6x → 1; missing → 0.5)       │
  │  Theta         10%    log1p(theta_yield)/log1p(cap) where      │
  │                       theta_yield = |theta| × 365 / collateral │
  │                       (Public greeks only; missing → 0.5)      │
  └────────────────────────────────────────────────────────────────┘
                               ×
  Earnings multiplier:   1 − 0.20 (if earnings before expiry)

  The income percentile is why scoring is batched: candidates are
  collected across ALL expirations for a ticker first, then scored as
  one pool per strategy — this keeps differentiation on leveraged ETFs
  whose yields all exceed the absolute cap.

  Final score: [0, 1]  →  top 5 per expiration per strategy kept
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
       + optional monthly candidates beyond max_dte

  recommend_cc_for_ticker()
  ─────────────────────────
  Per standard term (Short-Term DTE≤14 / Medium-Term 15-28 / Long-Term >28):

    1. Separate candidates into delta-qualified vs out-of-range
    2. Sort each group by composite score (descending)
    3. Merge: delta-qualified first, then out-of-range fills remaining slots
    4. Take top N (max_suggestions_per_term = 3)
    5. Per selected candidate → VERDICT:

       ┌────────────────────────────────────────────────────────┐
       │  VERDICT LOGIC (IVR is NOT used):                      │
       │                                                        │
       │  Collect all issues:                                   │
       │    Issue A: |delta| outside [delta_min, delta_max]     │
       │    Issue B: earnings_date ≤ expiry AND                 │
       │             (expiry − earnings_date) ≤ buffer days     │
       │    Issue C: strike < min_acceptable_price (if set)     │
       │                                                        │
       │  Any issues?  → NO  (reason = joined issue list)       │
       │  No issues?   → YES (reason includes delta + flags)    │
       └────────────────────────────────────────────────────────┘

    6. Always returns ≥1 row per term; even if all are "No", the best
       available candidate is shown so the user can review it manually.

  Computed fields per suggestion:
    max_profit        = (strike − spot + premium) × 100
    downside_breakeven = strike + premium   (effective call-away price)

  IVR (true IV Rank when history suffices, else HV Rank proxy) is
  shown for context — does NOT affect the CC verdict.

  Flags shown for context (not verdict-affecting):
    near_resistance  — strike within resistance_pct_buffer (2%) of
                       52w high or 20d swing high
    near_round_number — strike within 1% of nearest $5 increment
    below_min_price  — strike < user's min_acceptable_sale_price

Monthly CC (beyond max_dte):
  ─────────────────────────────────────────────────────────────
  Fetched via select_monthly_cc_expiration_dates() and processed
  separately in _recommend_monthly_cc():

    1. Group candidates by expiration date
    2. Per expiration month, select the single best candidate by
       RISK-ADJUSTED annualized yield = yield × (1 − |delta|)
       (primary key) — NOT composite score
    3. Apply the same verdict logic as standard terms
    4. term_label set to "Monthly (Mon YYYY)" e.g. "Monthly (Sep 2026)"
    5. Results appended after Short/Medium/Long-Term rows, sorted by date

  Monthly rows are output one per expiration month found in the data.
```

### Cash-Secured Put Recommender

```
Input: top-scored PUT candidates per ticker, split by DTE into 3 pools

  recommend_csp_for_ticker()
  ──────────────────────────
  Returns one recommendation per term per ticker (3 total):
    Short-Term (DTE ≤ 14) / Medium-Term (15–28) / Long-Term (>28)

  Per term, _recommend_csp_for_term() runs:

  Step A — Resolve IVR for the ticker
    Preferred: true IV Rank from recorded IV history (ticker-level,
      computed once in the pipeline and passed in) — see
      "What IVR Is" section.
    Fallback:  HV Rank proxy from the term's candidates:
      current_HV = 20-day annualised HV (most recent)
      IVR = (current_HV − hv_low) / (hv_high − hv_low) × 100
      (option IV is noted but NOT used in the proxy formula)

  Step B — Filter candidates

    Primary filter (strict):
      |delta| in [delta_min, delta_max]   (default 0.10–0.25)
      AND strike at/below support level   (if use_support_filter = True)
        where "at/below support" means ANY of:
          strike ≤ low_52w × (1 + support_buffer)
          strike ≤ swing_low_20d × (1 + support_buffer)
          (spot − strike) / spot ≥ 5%   (≥5% OTM)

    Support relaxation fallback:
      If primary filter yields nothing, retry with delta-only filter
      (support requirement dropped).  support_relaxed = True is flagged.

    Earnings preference:
      Earnings proximity is checked against EACH candidate's own
      expiration (a term pool can mix several expirations).  Candidates
      clear of earnings are preferred; only if every qualified candidate
      straddles earnings does the earnings hard-fail apply.

    If still no candidates → return "No" with combined reason from:
      IVR below threshold (if applicable) + earnings risk + delta issue.

  Step C — Pick best candidate
    best = max(qualified, key=composite_score)

  Step D — Verdict

       ┌────────────────────────────────────────────────────────┐
       │  Hard fails (checked first → NO):                      │
       │    Earnings within earnings_buffer_days of the BEST    │
       │    candidate's own expiration                          │
       │    IVR < ivr_min (HV Rank below threshold)             │
       │                                                        │
       │  Soft fails (checked if no hard fails → NO):           │
       │    IVR unavailable (insufficient price history)        │
       │    IVR at ceiling (100%) — likely overstated           │
       │    IVR at floor (0%) — current vol may be understated  │
       │    support_relaxed — strike above support levels       │
       │                                                        │
       │  All pass (no hard or soft fails) → YES                │
       │    reason = "IVR N%; delta D; strike at/below support" │
       └────────────────────────────────────────────────────────┘

  Support levels (from price history):
    low_52w       — lowest Low over the price history period
    swing_low_20d — 20-day rolling minimum of Lows

  Computed fields:
    max_profit   = premium × 100
    breakeven    = strike − premium
    cash_required = strike × 100
```

---

## Step 6 — Report Generation

```
  write_reports()  (render.py)
  ─────────────────────────────
  Outputs:
    {output_dir}/{date}_options_report.csv   ← all candidate records
    {output_dir}/{date}_options_report.html  ← interactive report

  HTML layout (top to bottom):
  ┌────────────────────────────────────────────────────┐
  │  ⚠ Provider fallback warning (if Public→yf used)   │
  ├────────────────────────────────────────────────────┤
  │  Heading shows active profile when selected        │
  │  Expand All / Collapse All controls                │
  ├────────────────────────────────────────────────────┤
  │  [Sell Call Recommendations]                       │
  │    Blue theme; collapsed by default                │
  │    Nested by term: Short Term / Medium Term /      │
  │    Long Term                                       │
  │    Columns: Ticker(color=yes/no) | AnnualYield |   │
  │    Current | Strike | %OTM | Exp | DTE | Premium | │
  │    Delta | IVR | MaxProfit | Breakeven | Flags |   │
  │    Why                                             │
  │    IVR displayed for reference only                │
  ├────────────────────────────────────────────────────┤
  │  [Sell Put Recommendations]                        │
  │    Gold theme; collapsed by default                │
  │    Nested by term: Short Term / Medium Term /      │
  │    Long Term                                       │
  │    Columns: Ticker(color=yes/no) | AnnualYield |   │
  │    Current | Strike | %ToStrike | Exp | DTE |      │
  │    Premium | Delta | IVR | MaxProfit | Breakeven | │
  │    CashRqd | Why                                   │
  ├────────────────────────────────────────────────────┤
  │  [Sell Call Candidates] (collapsible, purple)      │
  │    Nested by term: Short Term / Medium Term /      │
  │    Long Term                                       │
  │    Columns: Ticker | AnnualYield | Current |       │
  │    Strike | %OTM | Exp | DTE | Premium | Delta |   │
  │    IVR | MaxProfit | Breakeven | Why               │
  ├────────────────────────────────────────────────────┤
  │  [Sell Put Candidates] (collapsible, green)        │
  │    Nested by term with same layout as above        │
  └────────────────────────────────────────────────────┘

  Verdict colours:  ■ Yes = green   ■ No = red
  Links: each ticker links to Fidelity options research page

  Candidate tables include analytics columns: VRP (IV/HV20),
  ThetaYld (annualized decay per $ collateral), Score (composite).
```

### Streamlit Dashboard (app.py) — "PremiumEdge"

```
  Hero:      full-width banner — gradient-text title "PremiumEdge" ·
             tagline · last-run timestamp.
             Background: assets/hero_bg.jpg|png if present (downscaled
             once via Pillow, dark gradient overlay for readability);
             otherwise a built-in deterministic SVG candlestick scene.
  Controls:  ▶ Run · profile selector · last-run status · next schedule
             Hero meta shows last-run timestamp + duration.
             On load, the dashboard adopts the newest
             *_options_report.csv in the profile's output dir — so
             scheduled/headless runs appear without pressing Run.
  Configure: tickers, CC min yield / strike ranges, provider, schedule
             (saved back to config/users/<profile>.yaml)

  Controls zone background: the Run/profile row and the Configure
  expander share one keyed st.container (.st-key-pe-controls) carrying
  a finance image background (config/Wall Street Bull Image.png, or
  assets/controls_bg.jpg|png; Pillow-optimized + cached).
  background-size:cover with a fixed focal point means the expander
  collapsing/expanding just reveals less/more of the image — no JS.
  Left-heavy gradient overlay keeps controls readable; the expander
  body is translucent so the image shows through. No image → plain
  container (no CSS emitted).

  Tabs:
  ┌─ 📈 Calls / 📉 Puts ────────────────────────────────────────────┐
  │  Candidate tables sorted by expiration with recommendations     │
  │  merged inline.                                                 │
  │  Table visuals:                                                 │
  │    · expiration group header rows (📅 date · DTE · count) when  │
  │      in default expiration order (hidden when custom-sorted)    │
  │    · row banding alternates per expiration group and shows      │
  │      through verdict tints (continuous group colouring)         │
  │    · verdicts as YES/NO pill badges + translucent row tint +    │
  │      green/red left accent stripe (not solid-colour rows)       │
  │    · numeric columns right-aligned with tabular numerals;       │
  │      AnnualYield bold amber; row hover highlight                │
  │    · %OTM/%ToStrike cells carry a micro distance bar            │
  │      (saturates at 25% OTM)                                     │
  │    · Delta cells carry a risk dot: green ≤0.15 · amber ≤0.25 ·  │
  │      red above                                                  │
  │  Filters: ticker multiselect · DTE bucket · Sort-by dropdown    │
  │    (Expiration default / AnnualYield / Score / Premium /        │
  │     Delta / DTE) · "✓ YES only" toggle                          │
  │  Columns include E⚠ (earnings before expiry), VRP, ΘYld, Score │
  │  Ticker cells link to Fidelity options research.                │
  │  IVR cell: hover tooltip shows source; trailing * marks the     │
  │  HV-rank proxy (vs true IV Rank).                               │
  ├─ 📊 Performance ────────────────────────────────────────────────┤
  │  Reads cache/outcomes.csv + cache/iv_history.csv directly       │
  │  (shown even before the first run of a session).                │
  │  KPIs: open · closed (graded) · premium-kept rate · option P&L  │
  │  Breakdown by strategy × verdict (do Yes calls beat No calls?)  │
  │  Cumulative option P&L line chart (once ≥2 expirations graded)  │
  │  Open positions (days left) and closed outcomes tables.         │
  │  ATM IV history line chart per ticker + current/low/high table. │
  └─────────────────────────────────────────────────────────────────┘

  The scheduler fragment polls every 30 s — auto-runs fire only while
  the dashboard is open in a browser. For unattended runs use
  main.py --headless via OS scheduling.
```

---

## Step 7 — Outcome Tracking

Every run appends the day's CC/CSP recommendations (Yes **and** No verdicts, so
their relative performance can be compared later) to a CSV ledger, then grades
any previously recorded rows whose expiration has passed.

```
  record_recommendations() + evaluate_outcomes()  (tracking/outcomes.py)
  ──────────────────────────────────────────────────────────────────────
  Ledger: outcome_tracking.path (./cache/outcomes.csv)
    One row per (run_date, strategy, ticker, term, expiration, strike)
    Same-day re-runs replace their earlier rows (upsert)
    Placeholder rows without a contract (no strike/expiration) skipped

  Grading (each run, for open rows with expiration < today):
    expiry_close = underlying Close on expiration day
                   (or last close before it — half-day/holiday tolerance)

    CSP (short put):
      close ≥ strike → expired_otm   pnl = premium × 100
      close < strike → assigned      pnl = (close − strike + premium) × 100

    CC (short call, option leg only — share P&L excluded):
      close ≤ strike → expired_otm   pnl = premium × 100
      close > strike → called_away   pnl = (premium − (close − strike)) × 100

  P&L is mark-to-expiry per contract, educational only — ignores early
  assignment, rolls, and fills better/worse than the recorded premium.

  Console summary per run:  N open, M closed, premium-kept rate
  (premium-kept rate = share of graded rows that expired OTM)
```

---

## Key Config Parameters

| Parameter | Default | Effect |
|---|---|---|
| `covered_call_tickers` | — | Tickers screened for CALL candidates |
| `cash_secured_put_tickers` | — | Tickers screened for PUT candidates |
| `delta_call_min/max` | 0.15 / 0.35 | Delta range for CALL screening filter |
| `delta_put_min/max` | -0.35 / -0.15 | Delta range for PUT screening filter |
| `max_dte` | 45 | Hard cap — expirations beyond this ignored |
| `short_term_max_dte` | 14 | DTE ≤ 14 → Short Term (all expirations) |
| `medium_term_max_dte` | 28 | DTE ≤ 28 → Medium Term (Fridays only beyond 14) |
| `min_annualized_yield` | 12% | Contracts below this are dropped (screening filter) |
| `fill_price_factor` | 0.4 | Expected fill = bid + factor × (ask − bid); basis for all premium metrics (0.5 = mid) |
| `earnings_risk_penalty` | 20% | Score multiplier reduction when earnings before expiry |
| `risk_free_rate` | 5% | Used in Black-Scholes delta calculation |
| `price_history_period` | 6mo | Used for MA, RSI, HV, IVR proxy (yfinance period string) |
| `max_candidates_per_ticker_per_bucket` | 5 | Top N kept after scoring per bucket |
| `cc_recommendation.max_suggestions_per_term` | 3 | Suggestions shown per term in CC table |
| `cc_recommendation.delta_min/max` | 0.10 / 0.25 | Delta range for CC verdict (tighter than screening) |
| `cc_recommendation.min_acceptable_sale_prices` | {} | Per-ticker dict: strike floor for CC verdict |
| `cc_recommendation.min_strike_prices` | {} | Per-ticker dict: pre-filter strikes below this before processing |
| `cc_recommendation.max_strike_prices` | {} | Per-ticker dict: pre-filter strikes above this before processing |
| `cc_recommendation.min_yield` | 10% | CALL screening yield threshold — replaces global `min_annualized_yield` for calls when set |
| `cc_recommendation.long_term_months` | 9 | Months ahead to fetch monthly CC expirations beyond max_dte |
| `cc_recommendation.resistance_pct_buffer` | 2% | Buffer for near-resistance flag on CC strikes |
| `csp_recommendation.ivr_min` | 30% | IVR hard floor for CSP verdict |
| `csp_recommendation.use_support_filter` | True | Require strike at/below support for CSP (relaxed if no matches) |
| `csp_recommendation.support_pct_buffer` | 2% | Buffer above support level still considered "at support" |
| `csp_recommendation.delta_min/max` | 0.10 / 0.25 | Delta range for CSP verdict |
| `options_data_provider` | yfinance | `yfinance` or `public` |
| `iv_history_path` | ./cache/iv_history.csv | ATM IV snapshot store (shared across profiles) |
| `iv_rank_min_history_days` | 20 | Observations needed before true IV Rank replaces HV proxy |
| `outcome_tracking.enabled` | true | Record + grade recommendations after expiry |
| `outcome_tracking.path` | ./cache/outcomes.csv | Recommendation outcome ledger |
| `fetch_max_workers` | 4 | Tickers processed concurrently (1 = sequential) |
| `scoring.weights.*` | see Step 4 | Component weights (normalized by their sum) |
| `scoring.delta_target` | 0.20 | \|delta\| the delta component scores highest at |
| `scoring.income_yield_cap` | 1.5 | Absolute income axis saturates at this annualized yield |

---

## File Map

```
options-screener/
├── main.py                          ← CLI entry point
├── config.yaml                      ← legacy single-file config fallback
├── config/
│   ├── base.yaml                    ← shared defaults
│   └── users/
│       ├── vatsa.yaml               ← profile overrides
│       └── prasanna.yaml            ← profile overrides
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
│   │   ├── options_metrics.py       ← expiration selection (incl. monthly CC), filtering, BS delta
│   │   ├── iv_history.py            ← ATM IV snapshots + true IV Rank
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
│   ├── tracking/
│   │   └── outcomes.py              ← recommendation ledger + expiry grading
│   │
│   └── utils/
│       ├── dates.py                 ← is_third_friday()
│       ├── env.py                   ← .env file loader
│       └── logging.py               ← logger setup (file + console)
│
├── tests/                           ← pytest suite (metrics, scoring, recommenders,
│                                      IV history, outcome tracking, technicals)
│
├── cache/
│   ├── iv_history.csv               ← ATM IV snapshots (one per ticker per day)
│   └── outcomes.csv                 ← recommendation outcome ledger
│
└── logs/
    └── {ticker}_data.csv            ← per-ticker audit log of all API calls
```
