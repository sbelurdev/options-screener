# options-screener

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
