# options-screener

## Data Source Map (Hybrid)

You can configure providers by role in `config.yaml`:

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

Public-related config keys in `config.yaml`:

- `public_api_base_url` (default `https://api.public.com`)
- `public_api_key_env_var` (default `PUBLIC_API_KEY`)
- `public_access_token_validity_minutes` (default `15`)
- `public_http_timeout_seconds` (default `20`)
- `public_account_id` (optional; if omitted, app discovers brokerage account)
- `public_underlying_instrument_type` (default `EQUITY`)

## Run in a Python virtual environment (PowerShell)

From the project root (`c:\Users\sbelu\OneDrive\Documents\Git\options-screener`):

```powershell
# 1) Create venv (first time only)
python -m venv .venv

# 2) Activate it
.\.venv\Scripts\Activate.ps1
```

If activation is blocked:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Install dependencies and run:

```powershell
pip install -r requirements.txt
python main.py
```

Optional: run with explicit config file:

```powershell
python main.py --config config.yaml
```

Provider smoke test (runs auth/account/expirations check, then exits):

```powershell
python main.py --provider-smoke-test --smoke-ticker SPY
```
