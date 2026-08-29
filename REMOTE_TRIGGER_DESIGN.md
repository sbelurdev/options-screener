# Remote Trigger + Email Report — Design

## Goal

Let the screener be triggered from a phone, away from the local network, without
exposing the PC to the public internet — and have the resulting HTML report
land in an inbox automatically when the run finishes.

Execution stays entirely on the local PC (existing `main.py --headless`
pipeline, existing `cache/*` history files, existing config profiles). Only
two things are added: a way to fire a run remotely, and a way to deliver the
result without opening the Streamlit dashboard.

---

## Architecture

```
┌─────────────┐        Tailscale private mesh         ┌──────────────────────────┐
│   Phone      │  (WireGuard, encrypted, no public IP) │   PC (always-on)          │
│              │ ─────────────────────────────────────▶│                          │
│ Home-screen  │  GET https://pc.<tailnet>.ts.net/run   │  trigger_server.py        │
│ bookmark tap │           ?t=<short token>             │  127.0.0.1:PORT           │
└─────────────┘                                         │        │                 │
                                                          │        ▼                 │
                                                          │  run_pipeline(config)     │
                                                          │  (existing pipeline.py)   │
                                                          │        │                 │
                                                          │        ▼                 │
                                                          │  {output_dir}/            │
                                                          │  {date}_options_report    │
                                                          │  .html / .csv             │
                                                          │        │                 │
                                                          │        ▼                 │
                                                          │  send_report_email()      │
                                                          │  (SMTP)                   │
                                                          └───────────┬──────────────┘
                                                                      ▼
                                                              Inbox (configured
                                                              "to" address)
```

Nothing here is reachable from the open internet. The only entry point is
Tailscale's private mesh, which is limited to devices logged into the same
Tailscale account.

---

## Components

### 1. Trigger server — `agent/remote/trigger_server.py`

Started via `python main.py --serve [--profile prasanna]`, kept running via a
Windows Task Scheduler entry ("At log on" / "At startup", restart on
failure) — same pattern the README already documents for headless scheduled
runs.

- Python stdlib `http.server.ThreadingHTTPServer`. No new dependency.
- Binds to `127.0.0.1` only. It is never given a public port; Tailscale Serve
  is what makes it reachable to your own devices (see below) — the process
  itself has no awareness of being "exposed."
- Route: `GET /run?t=<token>`
  - Token compared against `REMOTE_TRIGGER_TOKEN` (env var, `.env` file —
    same convention as `PUBLIC_API_KEY`). Mismatch → `401`.
  - No `profile=` parameter accepted from the request. The token is tied to
    one profile fixed at server start — a query string should never be able
    to select which config runs.
  - A non-blocking `threading.Lock` rejects a second trigger while one is
    already running → `409 Conflict`. Mirrors the `is_running` guard
    `app.py` already uses for the Streamlit auto-run fragment.
  - On accept: `202 Accepted` returned immediately; the run happens in a
    background thread. The **email is the completion signal** — no
    status/polling endpoint needed for v1.
- Route: `GET /health` → `200 OK`, for Tailscale/keepalive checks.
- On success: locate `{output_dir}/{today}_options_report.html` (deterministic
  name already produced by `render.py`) and call `send_report_email(...)`.
- On failure: catch the exception, send a short plain-text failure email
  instead, so "no email" never silently means "nothing happened."

### 2. Reachability — Tailscale Serve (not Funnel)

| | Tailscale **Serve** (chosen) | Tailscale **Funnel** |
|---|---|---|
| Exposure | Only devices logged into your Tailscale account | Public HTTPS URL — anyone on the internet |
| Security boundary | Tailscale account + device identity (WireGuard) | The bearer token, and only the token |
| Public attack surface | None — no open port, nothing to port-scan | Full public HTTP server |

`tailscale serve` proxies `https://<device>.<tailnet>.ts.net/` to
`127.0.0.1:PORT` on the PC, with a Let's Encrypt cert Tailscale provisions
automatically (browsers trust it, no warnings). It does **not** open any
port on your router or public IP — Cloudflare Tunnel is not needed and has
been dropped from this design entirely.

The query-string token is kept as cheap defense-in-depth (covers a lost/
unlocked phone), not as the primary control — the primary control is
Tailscale network membership. Protect that with 2FA on the Tailscale
account.

### 3. Email delivery — `agent/notify/email_report.py`

`send_report_email(config, html_path, logger)`:

- Stdlib `smtplib` + `email.message.EmailMessage`. No new dependency.
- The HTML report has no external image/CSS references (confirmed in
  `render.py`), so it's sent as the message's HTML body directly (opens
  inline in the inbox) with the CSV attached for the raw data.
- Config, mirroring the existing `outcome_tracking` block:

```yaml
notifications:
  email:
    enabled: true
    to: prasanna.kudli@outlook.com
    smtp_host: smtp.office365.com
    smtp_port: 587
    smtp_user_env_var: SMTP_USER
    smtp_password_env_var: SMTP_PASSWORD   # Outlook app password, in .env
```

---

## Setup — PC side

1. Install Tailscale (<https://tailscale.com/download>) and sign in (enable
   2FA on this account — it's now the access boundary).
2. Confirm MagicDNS is on (Tailscale admin console → DNS) so the device gets
   a stable `https://pc-name.<tailnet>.ts.net` name.
3. Add `REMOTE_TRIGGER_TOKEN`, `SMTP_USER`, `SMTP_PASSWORD` to `.env`.
4. `python main.py --serve --profile prasanna` (bound to `127.0.0.1:PORT`).
5. `tailscale serve https / http://127.0.0.1:PORT` — publishes it to your
   tailnet at `https://pc-name.<tailnet>.ts.net/`.
6. Register both the Tailscale service and step 4 as Task Scheduler entries
   ("At startup", auto-restart) so they survive reboots without manual
   relaunch.

## Setup — phone side (how to "make the call")

1. Install the Tailscale app (iOS App Store / Google Play) and sign in with
   the **same account** used on the PC. Leave it connected — it runs as a
   normal background VPN profile with negligible battery/data cost.
2. Open Safari/Chrome and visit:
   `https://pc-name.<tailnet>.ts.net/run?t=<your token>`
   once, to confirm it returns `202` (or check the inbox for the report a
   minute later).
3. Save it as a one-tap home-screen icon:
   - **iOS (Safari):** open the URL → Share sheet → **Add to Home Screen** →
     name it e.g. "Run Screener". Tapping the icon fires the request with no
     browser chrome, no typing.
   - **Android (Chrome):** open the URL → ⋮ menu → **Add to Home screen**.
4. Triggering a run going forward is: tap the icon on your home screen. The
   HTML report arrives by email when the pipeline finishes — no need to
   check back on the phone.

No app, no headers, no curl/Postman — the token lives in the saved URL, and
the URL only resolves at all while your phone is on the same Tailscale
account.

---

## Failure handling

- Pipeline exception → failure email (plain text, includes the exception
  message) instead of silence.
- SMTP send exception → logged; does not crash the trigger server (the
  report files still exist locally in `output_dir` either way).
- Concurrent trigger while a run is in progress → `409`, no second run
  started.

## Out of scope for v1

- Multiple profiles triggerable from one endpoint
- A status/progress endpoint instead of "wait for the email"
- Retry queue if SMTP send fails
