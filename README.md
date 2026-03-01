# options-screener

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

Optional: run with explicit config file:

```powershell
# 2) Activate it - Every RUN
.\.venv\Scripts\Activate.ps1

python main.py --config config.yaml
```
