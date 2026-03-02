from __future__ import annotations

from datetime import date
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "run_date",
            "ticker",
            "bucket",
            "bucket_label",
            "expiration",
            "strategy",
            "contract_symbol",
            "spot",
            "strike",
            "bid",
            "ask",
            "mid",
            "spread_pct",
            "volume",
            "open_interest",
            "implied_volatility",
            "delta",
            "delta_source",
            "dte",
            "annualized_yield",
            "breakeven",
            "otm_pct",
            "earnings_date",
            "earnings_before_expiry",
            "ma20",
            "ma50",
            "rsi14",
            "hv20",
            "score",
            "why_ranked_high",
        ]
    )


def _fmt_money(val: Optional[float], prefix: str = "$") -> str:
    if val is None:
        return "—"
    return f"{prefix}{val:,.2f}"


def _fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "—"
    return f"{val:.1f}%"


def _render_csp_recommendations(
    recommendations: List[Dict[str, Any]],
    html_parts: List[str],
) -> None:
    """Render the CSP recommendation table at the top of the report."""
    if not recommendations:
        return

    yes_count = sum(1 for r in recommendations if r["recommend"] == "Yes")
    borderline_count = sum(1 for r in recommendations if r["recommend"] == "Borderline")
    total = len(recommendations)

    html_parts.append("<details open class='section section-rec'>")
    html_parts.append(
        f"<summary>CSP Recommendations"
        f"<span class='count'>"
        f"{yes_count} Yes &nbsp;·&nbsp; {borderline_count} Borderline &nbsp;·&nbsp; {total} total"
        f"</span></summary>"
    )

    html_parts.append("<div class='rec-body'>")
    html_parts.append("<table class='rec-table'>")
    html_parts.append(
        "<thead><tr>"
        "<th>Ticker</th>"
        "<th>Recommend</th>"
        "<th>Current Price</th>"
        "<th>Strike</th>"
        "<th>% to Strike</th>"
        "<th>Expiration</th>"
        "<th>DTE</th>"
        "<th>Premium</th>"
        "<th>Delta</th>"
        "<th>IVR *</th>"
        "<th>Max Profit</th>"
        "<th>Breakeven</th>"
        "<th>Cash Req.</th>"
        "<th>Ann. Yield</th>"
        "<th>Why</th>"
        "</tr></thead><tbody>"
    )

    verdict_class = {"Yes": "rec-yes", "No": "rec-no", "Borderline": "rec-borderline"}

    for rec in recommendations:
        ticker = rec["ticker"]
        verdict = rec["recommend"]
        fidelity_url = (
            f"https://digital.fidelity.com/ftgw/digital/options-research/?symbol={ticker}"
        )
        ivr_display = _fmt_pct(rec.get("ivr"))
        if rec.get("ivr_source"):
            ivr_display += " ★" if "proxy" in (rec.get("ivr_source") or "") else ""

        ann_yield = rec.get("annualized_yield")
        yield_display = f"{ann_yield:.1%}" if ann_yield is not None else "—"

        near_flags = []
        if rec.get("near_support"):
            near_flags.append("✓ support")
        if rec.get("near_round_number"):
            near_flags.append("○ round#")
        near_str = " ".join(near_flags)
        reason_full = rec.get("reason", "")
        if near_str:
            reason_full = f"{reason_full} [{near_str}]" if reason_full else near_str

        css = verdict_class.get(verdict, "")
        delta_raw = rec.get("delta")
        delta_display = f"{abs(float(delta_raw)):.3f}" if delta_raw is not None else "—"
        spot_val = rec.get("spot")
        strike_val = rec.get("strike")
        pct_to_strike = (
            f"{(strike_val - spot_val) / spot_val * 100:.1f}%"
            if spot_val and strike_val
            else "—"
        )
        html_parts.append(
            f"<tr>"
            f"<td><a href='{escape(fidelity_url)}' target='_blank' rel='noopener noreferrer'><strong>{escape(ticker)}</strong></a></td>"
            f"<td class='{css}'><strong>{escape(verdict)}</strong></td>"
            f"<td>{_fmt_money(spot_val)}</td>"
            f"<td>{_fmt_money(strike_val)}</td>"
            f"<td>{escape(pct_to_strike)}</td>"
            f"<td>{escape(str(rec.get('expiration') or '—'))}</td>"
            f"<td>{rec.get('dte') or '—'}</td>"
            f"<td>{_fmt_money(rec.get('premium'))}</td>"
            f"<td>{delta_display}</td>"
            f"<td>{escape(ivr_display)}</td>"
            f"<td>{_fmt_money(rec.get('max_profit'))}</td>"
            f"<td>{_fmt_money(rec.get('breakeven'))}</td>"
            f"<td>{_fmt_money(rec.get('cash_required'))}</td>"
            f"<td>{escape(yield_display)}</td>"
            f"<td class='reason-cell'>{escape(reason_full)}</td>"
            f"</tr>"
        )

    html_parts.append("</tbody></table>")

    # IVR footnote
    has_proxy = any("proxy" in (r.get("ivr_source") or "") for r in recommendations)
    if has_proxy:
        html_parts.append(
            "<p class='rec-footnote'>★ IVR is a proxy calculated from the option IV (or current 20-day HV) "
            "relative to the historical HV range over the available price history. "
            "True IV Rank requires historical implied volatility data not available via yfinance.</p>"
        )

    # Exit rules
    html_parts.append(
        "<div class='exit-rules'>"
        "<strong>Exit Rules:</strong>"
        "<ul>"
        "<li>Close the position at <strong>50–70% of max profit</strong> — lock in gains early.</li>"
        "<li>Close or roll if unrealised loss reaches <strong>2× the premium received</strong>.</li>"
        "</ul>"
        "</div>"
    )

    html_parts.append("</div>")  # rec-body
    html_parts.append("</details>")


def write_reports(
    candidates: List[Dict[str, Any]],
    config: Dict[str, Any],
    disclaimer: str,
    csp_recommendations: Optional[List[Dict[str, Any]]] = None,
    fallback_events: Optional[List[str]] = None,
) -> Tuple[str, str]:
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    run_day = date.today().isoformat()
    csv_path = output_dir / f"{run_day}_options_report.csv"
    html_path = output_dir / f"{run_day}_options_report.html"

    df = pd.DataFrame(candidates) if candidates else _empty_df()
    if not df.empty:
        df = df.sort_values(["ticker", "bucket", "strategy", "score"], ascending=[True, True, True, False])
    df.to_csv(csv_path, index=False)

    horizon_map = {"current_week": "Current Week", "next_week": "Next Week", "monthly": "Monthly"}
    horizon_order = {"current_week": 0, "next_week": 1, "monthly": 2}
    trade_map = {"PUT": "SELL PUT", "CALL": "Covered CALL"}

    html_parts: List[str] = []
    html_parts.append("<!DOCTYPE html><html><head><meta charset='utf-8'>")
    html_parts.append("<title>Options Report</title>")
    html_parts.append(
        "<style>"
        "body{font-family:Segoe UI,Arial,sans-serif;margin:20px;background:#fff;}"
        "h1{margin-bottom:8px;}"
        "details{margin-bottom:6px;}"
        "summary{cursor:pointer;padding:7px 12px;border-radius:4px;user-select:none;list-style:none;display:flex;align-items:center;gap:6px;}"
        "summary::-webkit-details-marker{display:none;}"
        "summary::before{content:'▶';font-size:10px;transition:transform 0.15s;}"
        "details[open]>summary::before{transform:rotate(90deg);}"
        ".section>summary{font-size:16px;font-weight:700;background:#0969da;color:#fff;border:none;}"
        ".section>summary:hover{background:#0860ca;}"
        ".section>summary::before{color:#fff;}"
        ".ticker-block{margin-left:20px;}"
        ".ticker-block>summary{font-size:13px;font-weight:600;background:#f6f8fa;border:1px solid #d0d7de;color:#24292f;}"
        ".ticker-block>summary:hover{background:#eaf0fb;}"
        "table{border-collapse:collapse;width:auto;margin:6px 0 6px 20px;}"
        "th,td{border:1px solid #d0d7de;padding:3px 6px;font-size:12px;text-align:left;white-space:nowrap;vertical-align:top;}"
        "th{background:#f6f8fa;font-weight:600;}"
        "tr:nth-child(even){background:#f9f9f9;}"
        ".count{font-weight:400;font-size:13px;opacity:0.85;margin-left:6px;}"
        ".note{font-size:12px;color:#444;padding:8px;background:#fff8c5;border:1px solid #e3b341;border-radius:4px;margin-bottom:12px;}"
        "a{color:inherit;}"
        ".section-puts>summary{background:#1a7f37;}"
        ".section-puts>summary:hover{background:#166f30;}"
        ".section-calls>summary{background:#6639ba;}"
        ".section-calls>summary:hover{background:#5b33a8;}"
        ".section-rec>summary{background:#9a6700;color:#fff;font-size:17px;}"
        ".section-rec>summary:hover{background:#875d00;}"
        ".section-rec>summary::before{color:#fff;}"
        ".rec-body{padding:8px 12px 12px;}"
        ".rec-table{margin:0 0 10px 0;width:100%;}"
        ".rec-table th{background:#fdf0d5;font-size:12px;}"
        ".rec-table td{font-size:12px;}"
        ".rec-yes{background:#d4edda;color:#155724;font-weight:600;}"
        ".rec-no{background:#f8d7da;color:#721c24;font-weight:600;}"
        ".rec-borderline{background:#fff3cd;color:#856404;font-weight:600;}"
        ".reason-cell{white-space:normal;min-width:180px;max-width:320px;font-size:11px;color:#444;}"
        ".rec-footnote{font-size:11px;color:#666;margin:4px 0 8px;font-style:italic;}"
        ".exit-rules{font-size:12px;background:#f0f7ff;border:1px solid #b6d4fe;border-radius:4px;padding:8px 12px;margin-top:4px;}"
        ".exit-rules ul{margin:4px 0 0 16px;padding:0;}"
        ".exit-rules li{margin-bottom:2px;}"
        ".warn-banner{background:#fff3cd;border:2px solid #ffc107;border-radius:6px;padding:10px 14px;margin-bottom:14px;}"
        ".warn-banner h3{margin:0 0 6px;color:#856404;font-size:14px;}"
        ".warn-banner ul{margin:4px 0 0 18px;padding:0;color:#6c4a00;font-size:13px;}"
        ".warn-banner li{margin-bottom:3px;}"
        "</style></head><body>"
    )
    html_parts.append(f"<h1>Daily Options Screening Report &mdash; {run_day}</h1>")
    html_parts.append(f"<p class='note'>{escape(disclaimer)}</p>")

    # ── Provider fallback warnings ─────────────────────────────────────────────
    if fallback_events:
        html_parts.append("<div class='warn-banner'>")
        html_parts.append("<h3>&#9888; Data Provider Warning: Public provider was inaccessible for some requests — yfinance was used as fallback</h3>")
        html_parts.append("<ul>")
        for event in fallback_events:
            html_parts.append(f"<li>{escape(event)}</li>")
        html_parts.append("</ul>")
        html_parts.append("</div>")

    # ── CSP Recommendations (top of page) ─────────────────────────────────────
    if csp_recommendations:
        _render_csp_recommendations(csp_recommendations, html_parts)

    # ── Screening results ──────────────────────────────────────────────────────
    if df.empty:
        html_parts.append("<p>No candidates passed filters today.</p>")
    else:
        def render_ticker_block(tdf: pd.DataFrame, ticker: str) -> None:
            fidelity_url = (
                f"https://digital.fidelity.com/ftgw/digital/options-research/?symbol={ticker}"
            )
            n = len(tdf)
            tdf = tdf.copy()
            tdf["time_horizon"] = tdf["bucket"].map(horizon_map).fillna(tdf["bucket_label"])
            tdf["trade_type"] = tdf["strategy"].map(trade_map).fillna(tdf["strategy"])
            tdf["horizon_rank"] = tdf["bucket"].map(horizon_order).fillna(99)
            tdf = tdf.sort_values(["horizon_rank", "score"], ascending=[True, False])

            view = tdf[[
                "time_horizon", "trade_type", "expiration", "strike", "spot", "mid",
                "annualized_yield", "delta", "implied_volatility", "delta_source", "dte", "spread_pct",
                "volume", "open_interest", "earnings_before_expiry", "score", "why_ranked_high",
            ]].copy()
            view["annualized_yield"] = (view["annualized_yield"] * 100).map(lambda x: f"{x:.2f}%")
            view["implied_volatility"] = view["implied_volatility"].map(
                lambda x: "" if pd.isna(x) else f"{x * 100:.2f}%"
            )
            view["spread_pct"] = (view["spread_pct"] * 100).map(lambda x: f"{x:.2f}%")
            view["delta"] = view["delta"].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
            view["score"] = view["score"].map(lambda x: f"{x:.3f}")
            view = view.rename(columns={"implied_volatility": "impliedVolatility", "why_ranked_high": "why"})

            count_label = f"{n} candidate{'s' if n != 1 else ''}"
            html_parts.append("<details class='ticker-block'>")
            html_parts.append(
                f"<summary>"
                f"<a href='{fidelity_url}' target='_blank' rel='noopener noreferrer'>{ticker}</a>"
                f"<span class='count'>({count_label})</span>"
                f"</summary>"
            )
            html_parts.append(view.to_html(index=False, escape=True, classes="table"))
            html_parts.append("</details>")

        def render_section(section_df: pd.DataFrame, title: str, css_class: str) -> None:
            n = len(section_df)
            count_label = f"{n} candidate{'s' if n != 1 else ''}"
            html_parts.append(f"<details class='section {css_class}'>")
            html_parts.append(
                f"<summary>{title}<span class='count'>({count_label})</span></summary>"
            )
            for ticker in sorted(section_df["ticker"].unique()):
                render_ticker_block(section_df[section_df["ticker"] == ticker], ticker)
            html_parts.append("</details>")

        puts_df = df[df["strategy"] == "PUT"]
        calls_df = df[df["strategy"] == "CALL"]

        if not puts_df.empty:
            render_section(puts_df, "Cash-Secured Puts", "section-puts")
        if not calls_df.empty:
            render_section(calls_df, "Covered Calls", "section-calls")

    html_parts.append("<hr>")
    html_parts.append(
        "<p><strong>Risk reminders:</strong> Assignment risk, overnight gaps, earnings/event shocks, "
        "liquidity deterioration, and tail-risk moves can cause losses.</p>"
    )
    html_parts.append("</body></html>")

    html_path.write_text("\n".join(html_parts), encoding="utf-8")
    return str(csv_path), str(html_path)
