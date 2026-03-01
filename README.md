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
