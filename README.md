# options-screener

## Data Source Map (Hybrid)

You can configure providers by role in `config.yaml` or in profile-based config files under `config/`:

- `options_data_provider`: `yfinance` or `public`
- `market_data_provider`: `yfinance`
- `fundamentals_provider`: `yfinance`

Current source ownership:

| Data domain | Fields used by screener | Source |
|---|---|---|
| Options expirations + chain | `expiration`, `contractSymbol`, `strike`, `bid`, `ask`, `lastPrice`, `volume`, `openInterest`, `impliedVolatility`, optional `delta` | `options_data_provider` |
| Price history / technicals | `spot`, `ma20`, `ma50`, `rsi14`, `hv20` | `market_data_provider` |
| Earnings | `earnings_date`, `earnings_before_expiry` | `fundamentals_provider` |
| Derived metrics (local) | `mid`, `spread_pct`, `dte`, `annualized_yield`, `breakeven`, `otm_pct`, `score`, `why_ranked_high` | Computed in app |

## Public.com API Setup (Options Provider)

To use Public for options data (`options_data_provider: public`), set your API key in an environment variable.

PowerShell (current session):

```powershell
$env:PUBLIC_API_KEY="your_public_secret_here"
python main.py
```

Or create a local `.env` file in the repo root (already gitignored):

```dotenv
PUBLIC_API_KEY=your_public_secret_here
```

Public-related config keys in config:

- `public_api_base_url` (default `https://api.public.com`)
- `public_api_key_env_var` (default `PUBLIC_API_KEY`)
- `public_access_token_validity_minutes` (default `15`)
- `public_http_timeout_seconds` (default `20`)
- `public_account_id` (optional; if omitted, app discovers brokerage account)
- `public_underlying_instrument_type` (default `EQUITY`)

## Run in a Python virtual environment (PowerShell)

From the project root (`c:\Users\sbelu\OneDrive\Documents\Git\options-screener`):
From the project root (`c:\Users\prasa\OneDrive\Documents\Git\options-screener`):

```powershell 
# 1) Create venv (first time only) -- ONE TIME
python -m venv .venv
```

If activation is blocked:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

.\.venv\Scripts\Activate.ps1
```

Install dependencies and run:

```powershell
pip install -r requirements.txt
```

Optional: run with explicit legacy config file:

```powershell
# 2) Activate it - Every RUN
.\.venv\Scripts\Activate.ps1

python main.py --config config.yaml
```

Profile-based runs:

```powershell
python main.py --profile vatsa
python main.py --profile prasanna
```

You can also set a default profile for your shell session:

```powershell
$env:OPTIONS_SCREENER_PROFILE="vatsa"
python main.py
```

Profile loading order:

- `DEFAULT_CONFIG` in code
- `config/base.yaml`
- `config/users/<profile>.yaml` when `--profile` or `OPTIONS_SCREENER_PROFILE` is set
- `config.yaml` as a legacy fallback when no profile is selected
- CLI overrides such as `--tickers` and `--output-dir`

Each profile can keep its own:

- ticker lists
- screening thresholds such as delta ranges
- recommendation settings
- `output_dir`, `log_dir`, and `cache_dir`

Current starter profiles:

- `vatsa`
- `prasanna`

## Report UI

The HTML report now includes:

- a heading that shows the active profile, e.g. `Daily Options Screening Report (for vatsa)`
- `Expand All` and `Collapse All` controls at the top
- fully collapsed sections by default
- recommendation sections organized as:
  - top-level strategy section
  - nested `Short Term`, `Medium Term`, and `Long Term` sections
  - a table inside each term
- candidate sections organized with the same term-first hierarchy

Recommendation tables use ticker-cell color to encode verdict:

- green ticker cell = `Yes`
- red ticker cell = `No`

Current recommendation/candidate column labels include names such as:

- `AnnualYield`
- `Current`
- `%OTM`
- `%ToStrike`
- `MaxProfit`
- `CashRqd`

Provider smoke test (runs auth/account/expirations check, then exits):

```powershell
python main.py --provider-smoke-test --smoke-ticker SPY
```
