"""
Signal alerts for the DayTrading tab.

Emails a fired signal — rationale, trigger values, the selected contract, sizing
and exits — using the same opt-in mail settings as the CC/CSP report
(`notify_email` in the profile; see agent/notify/email_report.py).

Every email this module sends shares the same shape:
  1. Option Trade Suggestions — every signal fired so far today, at the top,
     so a single email is a complete picture even if it isn't the one that
     announced a given signal.
  2. The email's own content (warm-up summary, stopped-ticker list, or the
     full detail for one signal).
  3. Monitoring status — one row per ticker, one column per gate/trigger
     criterion, so a failure is self-explanatory without opening the tab.

Rules this module exists to enforce:
  * One alert per (day, ticker, firing bar) for signals; one warm-up email per
    day; each ticker reported "stopped" once. The UI re-renders constantly,
    so without dedup a single event would email repeatedly.
  * Never send for a SIMULATED session, and never on a non-trading day —
    replaying history, or a weekend tab left open, must not put mail in the
    inbox (see views.poll_phase, which returns "closed" on non-trading days
    regardless of clock time, so the caller finds nothing to report).
  * A mail failure never breaks the tab. Alerts are best-effort.

The email states plainly that this is decision support and no order was placed.
"""

from __future__ import annotations

import json
from datetime import date
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from agent.daytrading.calendar import is_trading_day, now_et
from agent.notify.email_report import send_html_email


def _mail_log_path(cache_dir: Any, profile: str) -> Path:
    return Path(cache_dir) / f"daytrading_mail_log_{profile}.json"


def _read_mail_log(cache_dir: Any, profile: str) -> Dict[str, Dict[str, str]]:
    path = _mail_log_path(cache_dir, profile)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    by_kind = data.get("by_kind") if isinstance(data, dict) else None
    return by_kind if isinstance(by_kind, dict) else {}


def record_email_sent(cache_dir: Any, profile: str, kind: str, day: date) -> None:
    """Persist the fact an email actually sent, for the UI's "last email" line
    and for cross-session same-day dedup (see was_kind_sent_today).

    Keyed by `kind`, not a single overwritten "last" slot — a session commonly
    sends more than one kind (warm-up, then polling-stopped); a single-slot
    record would let the second overwrite the first, silently erasing the
    fact a warm-up already went out today and letting a later session re-send
    it. A file, not st.session_state — that resets on every server restart,
    which would make "already sent today" forgettable within the same day.
    """
    path = _mail_log_path(cache_dir, profile)
    path.parent.mkdir(parents=True, exist_ok=True)
    by_kind = _read_mail_log(cache_dir, profile)
    by_kind[kind] = {"day": day.isoformat(), "sent_at": now_et().isoformat()}
    path.write_text(json.dumps({"by_kind": by_kind}), encoding="utf-8")


def last_email_sent(cache_dir: Any, profile: str) -> Optional[Dict[str, str]]:
    """{"kind", "day", "sent_at"} for the most recently sent kind, or None."""
    by_kind = _read_mail_log(cache_dir, profile)
    if not by_kind:
        return None
    kind, rec = max(by_kind.items(), key=lambda kv: kv[1].get("sent_at", ""))
    return {"kind": kind, **rec}


def was_kind_sent_today(cache_dir: Any, profile: str, kind: str, day: date) -> bool:
    """True if `kind` was already recorded as sent for `day` (any session)."""
    rec = _read_mail_log(cache_dir, profile).get(kind)
    return bool(rec and rec.get("day") == day.isoformat())

_STYLE = (
    "font-family:-apple-system,'Segoe UI',Arial,sans-serif;font-size:13px;color:#1f2937;"
)
_TH = ("text-align:left;padding:6px 10px;background:#f1f5f9;color:#475569;"
       "font-size:10.5px;letter-spacing:0.04em;text-transform:uppercase;"
       "border-bottom:1px solid #e2e8f0;white-space:nowrap;")
_TD = "padding:6px 10px;border-bottom:1px solid #f1f5f9;vertical-align:top;"

# (email column header, CriteriaRow attribute) for six of the eight per-ticker
# criteria. RSI and the trend MA are deliberately NOT here — both get a
# dynamic, config-derived header (the RSI band, the MA type+period) built
# separately, so they are rendered once, up front, rather than through this
# generic list.
_CRITERIA_COLS = [
    ("Earnings", "earnings"),
    ("Gap Up", "gap"),
    (">OR High", "or_high"),
    (">Overnight High", "overnight_high"),
    (">VWAP", "vwap"),
    ("Vol>OR Avg", "volume"),
]


def _rows(pairs) -> str:
    return "".join(
        f"<tr><td style=\"{_TD}color:#64748b;white-space:nowrap;\">{escape(str(k))}</td>"
        f"<td style=\"{_TD}\"><b>{escape(str(v))}</b></td></tr>"
        for k, v in pairs
    )


def _f(v, fmt=",.2f", dash="-") -> str:
    try:
        return format(float(v), fmt)
    except (TypeError, ValueError):
        return dash


def _crit_cell(cond) -> str:
    """Format one Condition as 'PASS/FAIL actual OP threshold'.

    Both sides of the comparison are shown whenever there is a numeric
    threshold, so a failure like an overnight-high miss is legible on its own
    — 'FAIL 356.61 < overnight_high 516.66' — without cross-referencing the
    tab. `None` (the check never ran) is rendered as '-', which must never be
    confused with FAIL: a ticker that failed its daily gate shows '-' for the
    trigger columns because Stage C never started, not because it lost there.
    """
    if cond is None:
        return "-"
    status = "PASS" if cond.passed else "FAIL"
    if cond.threshold is not None and cond.actual is not None:
        fmt = ",.0f" if "volume" in cond.name else ",.2f"
        op = ">" if cond.actual >= cond.threshold else "<"
        return f"{status} {format(cond.actual, fmt)} {op} {format(cond.threshold, fmt)}"
    if cond.actual is not None:
        return f"{status} {cond.actual:,.4g}"
    if not cond.passed and cond.detail:
        return f"{status} ({cond.detail})"
    return status


def criteria_table_html(rows) -> str:
    """The per-ticker monitoring table: one row per ticker, one column per
    criterion. Appended to every email so any single message shows the
    complete state, not just the one event that triggered it."""
    if not rows:
        return ""
    ma_label = rows[0].trend_ma_label
    rsi_label = f"RSI ({rows[0].rsi_band})" if rows[0].rsi_band else "RSI"
    headers = ["Ticker", "Monitored", rsi_label, ma_label] + \
              [h for h, _ in _CRITERIA_COLS] + ["Reason"]

    out = [
        "<h3 style='font-size:14px;margin:18px 0 6px'>Monitoring status</h3>",
        "<div style='overflow-x:auto'>",
        "<table style='border-collapse:collapse;width:100%;font-size:11px'>",
        "<tr>" + "".join(f"<th style=\"{_TH}\">{escape(h)}</th>" for h in headers) + "</tr>",
    ]
    for r in rows:
        mon_colour = "#166534" if r.monitored else "#94a3b8"
        cells = [
            f"<td style=\"{_TD}white-space:nowrap\"><b>{escape(r.ticker)}</b></td>",
            f"<td style=\"{_TD}color:{mon_colour};font-weight:700\">"
            f"{'YES' if r.monitored else 'NO'}</td>",
        ]
        for cond in (r.rsi, r.trend_ma, r.earnings, r.gap,
                     r.or_high, r.overnight_high, r.vwap, r.volume):
            text = _crit_cell(cond)
            colour = "#991b1b" if (cond is not None and not cond.passed) else "#374151"
            cells.append(
                f"<td style=\"{_TD}white-space:nowrap;color:{colour}\">{escape(text)}</td>")
        cells.append(f"<td style=\"{_TD}color:#475569;white-space:normal;min-width:160px\">"
                     f"{escape(r.reason)}</td>")
        out.append("<tr>" + "".join(cells) + "</tr>")
    out.append("</table></div>")
    return "".join(out)


def criteria_table_text(rows) -> str:
    """Plain-text fallback: a block per ticker rather than a jammed 11-column
    table, since fixed-width alignment across that many columns is unreadable
    in a mail client."""
    if not rows:
        return ""
    lines = ["", "Monitoring status:"]
    for r in rows:
        lines.append(f"  {r.ticker} [{'MONITORED' if r.monitored else 'STOPPED'}]"
                     + (f" - {r.reason}" if r.reason else ""))
        rsi_label = f"RSI ({r.rsi_band})" if r.rsi_band else "RSI"
        lines.append(f"      {rsi_label}: {_crit_cell(r.rsi)}")
        lines.append(f"      {r.trend_ma_label}: {_crit_cell(r.trend_ma)}")
        for label, attr in _CRITERIA_COLS:
            lines.append(f"      {label}: {_crit_cell(getattr(r, attr))}")
    return "\n".join(lines)


def trade_suggestions_html(fired_today) -> str:
    """Every signal fired today, compact. Always present, even when empty, so
    the absence of a suggestion is stated rather than implied by a missing
    section."""
    header = "<h3 style='font-size:14px;margin:0 0 8px'>Option Trade Suggestions</h3>"
    if not fired_today:
        return (header +
                "<div style='padding:9px 12px;border-radius:6px;background:#f8fafc;"
                "border:1px solid #e2e8f0;color:#64748b;margin-bottom:16px'>"
                "No trade signals yet today.</div>")

    blocks = []
    for ev, sel in fired_today:
        fire = ev.trigger.fire
        c = sel.contract if (sel and sel.found) else None
        if c is not None:
            head = (f"<b>{escape(ev.ticker)}</b> &nbsp; {c.strike:.0f}C exp "
                    f"{c.expiration:%d %b} ({c.dte} DTE) &nbsp; delta {c.delta:.2f}")
            if c.spread_pct_of_mid is not None:
                head += f" &nbsp; spread {c.spread_pct_of_mid:.1%}"
        else:
            head = f"<b>{escape(ev.ticker)}</b> &nbsp; no qualifying contract"

        detail = f"entry ${fire.bar_close:,.2f} @ {fire.bar_time:%H:%M} ET"
        exits = getattr(ev, "_email_exits", None)
        if exits is not None:
            detail += (f" &middot; stop ${exits.stop_level:,.2f} ({exits.stop_basis})"
                      f" &middot; target ${exits.target_1:,.2f}"
                      f" &middot; time stop {exits.time_stop:%H:%M} ET")
        size = getattr(ev, "_email_size", None)
        if size is not None and size.sizeable:
            detail += f" &middot; {size.contracts} contract{'s' if size.contracts != 1 else ''}"

        blocks.append(
            f"<div style='padding:8px 10px;border:1px solid #e2e8f0;border-radius:6px;"
            f"margin-bottom:6px;background:#f8fafc'>{head}<br>"
            f"<span style='color:#64748b;font-size:12px'>{detail}</span></div>")
    return header + "".join(blocks)


def trade_suggestions_text(fired_today) -> str:
    if not fired_today:
        return "Option Trade Suggestions: none yet today."
    lines = ["Option Trade Suggestions:"]
    for ev, sel in fired_today:
        fire = ev.trigger.fire
        c = sel.contract if (sel and sel.found) else None
        if c is not None:
            lines.append(
                f"  {ev.ticker}: {c.strike:.0f}C exp {c.expiration} ({c.dte} DTE) "
                f"delta {c.delta:.2f} - entry ${fire.bar_close:,.2f} @ {fire.bar_time:%H:%M} ET")
        else:
            lines.append(f"  {ev.ticker}: no qualifying contract - "
                         f"entry ${fire.bar_close:,.2f} @ {fire.bar_time:%H:%M} ET")
    return "\n".join(lines)


def build_signal_email(ev, cfg, selection, criteria_rows, fired_today,
                       day: date, profile: str) -> tuple:
    """Return (subject, html_body, text_body) for one fired signal."""
    fire = ev.trigger.fire
    t = ev.ticker

    contract = selection.contract if (selection and selection.found) else None
    if contract is not None:
        subject = (f"DayTrading signal: {t} {fire.bar_time:%H:%M} ET - "
                   f"{contract.strike:.0f}C {contract.expiration:%d %b} "
                   f"({contract.dte}DTE, delta {contract.delta:.2f})")
    else:
        subject = f"DayTrading signal: {t} {fire.bar_time:%H:%M} ET - no qualifying contract"

    parts: List[str] = [
        f"<div style=\"{_STYLE}max-width:820px\">",
        f"<h2 style='margin:0 0 2px;font-size:18px'>{escape(t)} &mdash; opening-range breakout</h2>",
        f"<div style='color:#64748b;margin-bottom:14px'>"
        f"triggered {fire.bar_time:%a %d %b %Y %H:%M} ET &middot; profile {escape(profile)}</div>",
        "<div style='padding:9px 12px;border-radius:6px;background:#fef2f2;"
        "border:1px solid #fecaca;color:#991b1b;margin-bottom:16px'>"
        "<b>Recommendation only.</b> No order has been placed and none will be. "
        "This tool cannot trade.</div>",
        trade_suggestions_html(fired_today),
    ]

    # Why
    reasons = ev.rationale()
    if reasons:
        parts.append("<h3 style='font-size:14px;margin:14px 0 6px'>Why this signal</h3>")
        parts.append("<table style='border-collapse:collapse;width:100%'>")
        parts.append(f"<tr><th style=\"{_TH}\">Stage</th><th style=\"{_TH}\">Reason</th></tr>")
        for stage, text in reasons:
            parts.append(f"<tr><td style=\"{_TD}white-space:nowrap\"><b>{escape(stage)}</b></td>"
                         f"<td style=\"{_TD}color:#475569\">{escape(text)}</td></tr>")
        parts.append("</table>")

    # Contract
    parts.append("<h3 style='font-size:14px;margin:18px 0 6px'>Contract</h3>")
    if contract is not None:
        c = contract
        parts.append("<table style='border-collapse:collapse;width:100%'>")
        parts.append(_rows([
            ("Strike", f"${c.strike:,.2f}"),
            ("Expiry", f"{c.expiration} ({c.dte} DTE)"),
            ("Bid / Ask / Mid", f"${c.bid:,.2f} / ${c.ask:,.2f} / ${c.mid:,.2f}"),
            ("Spread", f"{c.spread_pct_of_mid:.2%} of mid" if c.spread_pct_of_mid is not None else "-"),
            ("Delta", _f(c.delta, ".3f")),
            ("Theta", _f(c.theta, ".3f")),
            ("Implied vol", f"{c.implied_volatility:.1%}" if c.implied_volatility else "-"),
            ("Open interest", f"{c.open_interest:,}" if c.open_interest else "-"),
            ("Option volume", f"{c.option_volume:,}" if c.option_volume else "-"),
            ("Underlying at quote", f"${c.underlying_price:,.2f}"),
            ("Greeks source", c.delta_source),
        ]))
        parts.append("</table>")
        if selection.selection_reason:
            parts.append(f"<div style='color:#64748b;font-size:12px;margin-top:6px'>"
                         f"<b>Why this strike:</b> {escape(selection.selection_reason)}</div>")
    else:
        why = selection.explain() if selection else "contract lookup not run"
        parts.append(f"<div style='padding:9px 12px;border-radius:6px;background:#fffbeb;"
                     f"border:1px solid #fde68a;color:#92400e'>No contract selected. "
                     f"{escape(why)}</div>")

    # Sizing
    size = getattr(ev, "_email_size", None)
    if size is not None:
        parts.append("<h3 style='font-size:14px;margin:18px 0 6px'>Sizing</h3>")
        parts.append("<table style='border-collapse:collapse;width:100%'>")
        parts.append(_rows([
            ("Stop level", f"${size.stop_level:,.2f} ({size.stop_basis})"),
            ("Stop distance", f"${size.stop_distance:,.2f}"),
            ("Risk budget", f"${size.risk_dollars:,.2f} "
                            f"({size.account_size:,.0f} x {size.risk_pct_per_trade:.2%})"),
            ("Risk per contract", f"${size.risk_per_contract:,.2f}"),
            ("Contracts", str(size.contracts) if size.sizeable else "not sized"),
        ]))
        parts.append("</table>")
        if not size.sizeable:
            parts.append(f"<div style='color:#92400e;font-size:12px;margin-top:6px'>"
                         f"No quantity suggested &mdash; {escape(size.blocked_reason or '')}.</div>")

    # Exits
    exits = getattr(ev, "_email_exits", None)
    if exits is not None:
        parts.append("<h3 style='font-size:14px;margin:18px 0 6px'>Exits</h3>")
        parts.append("<table style='border-collapse:collapse;width:100%'>")
        parts.append(_rows([
            ("Stop", f"${exits.stop_level:,.2f} on the UNDERLYING ({exits.stop_basis})"),
            ("Target 1", f"${exits.target_1:,.2f} (entry + OR height)"),
            ("1R", f"${exits.target_1r:,.2f}"),
            ("Time stop", f"{exits.time_stop:%H:%M} ET"),
            ("Hard close", f"{exits.hard_close:%H:%M} ET" if exits.hard_close else "-"),
        ]))
        parts.append("</table>")
        parts.append("<div style='color:#64748b;font-size:12px;margin-top:6px'>"
                     "The stop is on the underlying price, never the option price &mdash; "
                     "option quotes gap and spreads widen, so an option-price stop "
                     "triggers on noise.</div>")

    parts.append(criteria_table_html(criteria_rows))
    parts.append("<hr style='border:none;border-top:1px solid #e2e8f0;margin:20px 0 10px'>")
    parts.append("<div style='color:#94a3b8;font-size:11.5px'>Educational decision support "
                 "only &mdash; not financial advice. Options carry assignment, gap, event "
                 "and total-loss risk.</div></div>")

    # Plain-text fallback for clients that will not render HTML.
    text_lines = [trade_suggestions_text(fired_today), "",
                  f"{t} - opening-range breakout, triggered {fire.bar_time:%Y-%m-%d %H:%M} ET",
                  "RECOMMENDATION ONLY - no order has been or will be placed.", ""]
    for stage, txt in reasons:
        text_lines += [f"{stage}:", f"  {txt}"]
    if contract is not None:
        c = contract
        text_lines += ["", "Contract:",
                       f"  strike ${c.strike:,.2f}  expiry {c.expiration} ({c.dte} DTE)",
                       f"  bid/ask ${c.bid:,.2f}/${c.ask:,.2f}  delta {_f(c.delta,'.3f')}"]
    text_lines.append(criteria_table_text(criteria_rows))
    return subject, "".join(parts), "\n".join(text_lines)


def build_warmup_email(evals, criteria_rows, fired_today, day: date, profile: str,
                       warmup) -> tuple:
    """The one scheduled email of the day: what the watchlist looks like at the open."""
    ready = [t for t, e in evals.items() if getattr(e, "gate_ok", False)]
    subject = (f"DayTrading warm-up {day:%d %b} - {len(ready)} of {len(evals)} "
               f"passed the daily gate")

    parts = [
        f"<div style=\"{_STYLE}max-width:820px\">",
        f"<h2 style='margin:0 0 2px;font-size:18px'>DayTrading warm-up &mdash; {day:%a %d %b %Y}</h2>",
        f"<div style='color:#64748b;margin-bottom:14px'>profile {escape(profile)}"
        + (f" &middot; data loaded in {warmup.elapsed_seconds:.1f}s" if warmup else "")
        + "</div>",
        "<div style='padding:9px 12px;border-radius:6px;background:#eff6ff;"
        "border:1px solid #bfdbfe;color:#1e40af;margin-bottom:16px'>"
        "Watchlist is loaded and being tracked. You will only hear from this tool "
        "again today if a trade signal fires, or if polling stops for a ticker.</div>",
        trade_suggestions_html(fired_today),
    ]

    parts.append("<h3 style='font-size:14px;margin:14px 0 6px'>Daily gate</h3>")
    parts.append("<table style='border-collapse:collapse;width:100%'>")
    parts.append(f"<tr><th style=\"{_TH}\">Ticker</th><th style=\"{_TH}\">Gate</th>"
                 f"<th style=\"{_TH}\">Detail</th></tr>")
    for t in sorted(evals):
        e = evals[t]
        ok = getattr(e, "gate_ok", False)
        d = getattr(e, "daily", None)
        detail = (f"RSI {d.rsi_14:.1f}, close {d.trend_ma_distance_pct * 100:+.1f}% "
                  f"vs {d.trend_ma_label}"
                  if d and d.rsi_14 is not None and d.trend_ma_distance_pct is not None
                  else e.blocking_reason()[:80])
        parts.append(
            f"<tr><td style=\"{_TD}white-space:nowrap\"><b>{escape(t)}</b></td>"
            f"<td style=\"{_TD}color:{'#166534' if ok else '#94a3b8'};font-weight:700;"
            f"font-size:11px\">{'PASS' if ok else 'FAIL'}</td>"
            f"<td style=\"{_TD}color:#475569\">{escape(str(detail))}</td></tr>")
    parts.append("</table>")

    parts.append(criteria_table_html(criteria_rows))
    parts.append("<hr style='border:none;border-top:1px solid #e2e8f0;margin:20px 0 10px'>")
    parts.append("<div style='color:#94a3b8;font-size:11.5px'>Educational decision support "
                 "only &mdash; no order has been or will be placed.</div></div>")

    text = [trade_suggestions_text(fired_today), "",
            f"DayTrading warm-up {day} ({profile})",
            f"{len(ready)} of {len(evals)} passed the daily gate: {', '.join(ready) or 'none'}",
            "You will only hear from this tool again today if a signal fires or polling stops.",
            criteria_table_text(criteria_rows)]
    return subject, "".join(parts), "\n".join(text)


def build_polling_stopped_email(stopped, criteria_rows, fired_today, day: date,
                                profile: str) -> tuple:
    """Sent when tickers drop out of polling, so silence is never ambiguous."""
    names = ", ".join(r.ticker for r in stopped)
    subject = f"DayTrading {day:%d %b} - polling stopped for {names}"
    parts = [
        f"<div style=\"{_STYLE}max-width:820px\">",
        f"<h2 style='margin:0 0 12px;font-size:18px'>Polling stopped &mdash; "
        f"{escape(names)}</h2>",
        trade_suggestions_html(fired_today),
        "<table style='border-collapse:collapse;width:100%'>",
        f"<tr><th style=\"{_TH}\">Ticker</th><th style=\"{_TH}\">Why it stopped</th></tr>",
    ]
    for r in stopped:
        parts.append(f"<tr><td style=\"{_TD}white-space:nowrap\"><b>{escape(r.ticker)}</b></td>"
                     f"<td style=\"{_TD}color:#475569\">{escape(r.reason)}</td></tr>")
    parts.append("</table>")
    parts.append(criteria_table_html(criteria_rows))
    parts.append("<hr style='border:none;border-top:1px solid #e2e8f0;margin:20px 0 10px'>")
    parts.append("<div style='color:#94a3b8;font-size:11.5px'>These names cannot produce a "
                 "signal for the rest of today. No order has been or will be placed.</div></div>")

    text = [trade_suggestions_text(fired_today), "", f"Polling stopped for: {names}", ""]
    text += [f"  {r.ticker}: {r.reason}" for r in stopped]
    text.append(criteria_table_text(criteria_rows))
    return subject, "".join(parts), "\n".join(text)


def maybe_send_warmup_email(app_config, evals, criteria_rows, fired_today, day, profile,
                            state, logger, warmup=None, *, simulated: bool = False,
                            recipients=None, cache_dir=None) -> bool:
    """One warm-up email per day. `state` is a dict persisted across reruns
    *within one browser session only* — it does not survive a server restart
    or a second, independent session opening the same day. `cache_dir`, when
    given, adds a same-day check against the file record_email_sent() writes,
    so a second session (or a restarted server) does not resend a warm-up the
    first session already sent for today.

    Checks the trading-day itself, not just `simulated` — a caller that runs
    headless (a scheduler, a direct test invocation) has no UI toggle to
    default-protect it the way the Streamlit tab's Simulate switch does.
    """
    if simulated or not is_trading_day(day):
        return False
    key = f"warmup:{day.isoformat()}"
    if state.get(key):
        return False
    if cache_dir is not None and was_kind_sent_today(cache_dir, profile, "Warm-up", day):
        state[key] = True
        return False
    try:
        subject, html, text = build_warmup_email(evals, criteria_rows, fired_today, day,
                                                 profile, warmup)
        ok = send_html_email(app_config, subject, html, text, logger, recipients)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Warm-up email failed: %s", exc)
        ok = False
    state[key] = True  # mark either way; a broken mailbox must not retry every rerun
    return ok


def maybe_send_polling_stopped_email(app_config, poll_rows, criteria_rows, fired_today,
                                     day, profile, state, logger, *,
                                     simulated: bool = False, recipients=None) -> bool:
    """Report tickers that have newly stopped polling, once each.

    Names that stopped because they FIRED are excluded — the signal email already
    covers those, and reporting them here would double up.
    """
    if simulated or not is_trading_day(day):
        return False
    already: Set[str] = state.setdefault(f"stopped:{day.isoformat()}", set())
    newly = [r for r in poll_rows
             if not r.polling and r.category == "stopped" and r.ticker not in already]
    if not newly:
        return False

    try:
        subject, html, text = build_polling_stopped_email(newly, criteria_rows, fired_today,
                                                           day, profile)
        ok = send_html_email(app_config, subject, html, text, logger, recipients)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Polling-stopped email failed: %s", exc)
        ok = False
    already.update(r.ticker for r in newly)
    return ok


def maybe_send_signal_email(
    app_config: Dict[str, Any],
    ev,
    cfg,
    selection,
    criteria_rows,
    fired_today,
    day: date,
    profile: str,
    sent_keys: Set[str],
    logger,
    *,
    simulated: bool = False,
    recipients=None,
) -> bool:
    """Email a fired signal once. Returns True only if a mail was actually sent."""
    if simulated or not is_trading_day(day):
        return False  # replaying history must never raise a live alert
    if ev.trigger is None or not ev.trigger.fired:
        return False

    fire = ev.trigger.fire
    key = f"{day.isoformat()}:{ev.ticker}:{fire.bar_time:%H%M}"
    if key in sent_keys:
        return False

    try:
        subject, html, text = build_signal_email(ev, cfg, selection, criteria_rows,
                                                fired_today, day, profile)
        ok = send_html_email(app_config, subject, html, text, logger, recipients)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Signal email failed for %s: %s", ev.ticker, exc)
        return False

    # Mark as handled either way: a persistent SMTP problem must not turn every
    # rerun into another send attempt.
    sent_keys.add(key)
    return ok
