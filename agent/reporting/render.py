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
            "max_profit",
            "otm_pct",
            "ivr",
            "ivr_source",
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
        return "-"
    return f"{prefix}{val:,.2f}"


def _fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "-"
    return f"{val:.1f}%"


def _term_groups(recommendations: List[Dict[str, Any]]) -> List[Tuple[str, List[Dict[str, Any]]]]:
    term_order = {"Short-Term": 0, "Medium-Term": 1, "Long-Term": 2}
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for rec in recommendations:
        term = rec.get("term") or "Other"
        grouped.setdefault(term, []).append(rec)
    return sorted(grouped.items(), key=lambda item: term_order.get(item[0], 99))


def _display_term_label(term: str) -> str:
    return term.replace("-", " ")


def _render_sell_call_term(term: str, recommendations: List[Dict[str, Any]], html_parts: List[str]) -> None:
    verdict_class = {"Yes": "rec-yes", "No": "rec-no"}
    group_colors = ("#ffffff", "#f1f3f5")

    html_parts.append("<details class='rec-term'>")
    html_parts.append(
        f"<summary>{escape(_display_term_label(term))}"
        f"<span class='count'>({len(recommendations)} row{'s' if len(recommendations) != 1 else ''})</span></summary>"
    )
    html_parts.append("<table class='cc-rec-table'>")
    html_parts.append(
        "<thead><tr>"
        "<th>Ticker</th>"
        "<th>AnnualYield</th>"
        "<th>Current</th>"
        "<th>Strike</th>"
        "<th>%OTM</th>"
        "<th>Expiration</th>"
        "<th>DTE</th>"
        "<th>Premium</th>"
        "<th>Delta</th>"
        "<th>IVR</th>"
        "<th>MaxProfit</th>"
        "<th>Breakeven</th>"
        "<th>Flags</th>"
        "<th>Why</th>"
        "</tr></thead><tbody>"
    )

    prev_ticker = None
    group_idx = -1
    for rec in recommendations:
        ticker = rec["ticker"]
        if ticker != prev_ticker:
            group_idx += 1
            prev_ticker = ticker
        row_bg = group_colors[group_idx % 2]
        fidelity_url = f"https://digital.fidelity.com/ftgw/digital/options-research/?symbol={ticker}"

        spot_val = rec.get("spot")
        strike_val = rec.get("strike")
        ann_yield = rec.get("annualized_yield")
        yield_display = f"{ann_yield:.1%}" if ann_yield is not None else "-"
        otm_pct = (
            f"{(strike_val - spot_val) / spot_val * 100:.1f}%"
            if spot_val and strike_val
            else "-"
        )
        ivr_display = _fmt_pct(rec.get("ivr"))
        delta_raw = rec.get("delta")
        delta_display = f"{abs(float(delta_raw)):.3f}" if delta_raw is not None else "-"

        flags: List[str] = []
        if rec.get("near_resistance"):
            flags.append("<span class='flag-res' title='Strike near resistance level'>&#9650; resistance</span>")
        if rec.get("near_round_number"):
            flags.append("<span class='flag-round' title='Strike near $5 round number'>&#9675; round#</span>")
        if rec.get("below_min_price"):
            min_p = rec.get("min_acceptable_price")
            min_str = f"${min_p:.2f}" if min_p is not None else "min"
            flags.append(
                f"<span class='flag-below' title='Strike below your minimum acceptable price'>&#9888; below {min_str}</span>"
            )
        flags_html = " ".join(flags) if flags else "-"

        verdict = rec["recommend"]
        css = verdict_class.get(verdict, "")
        html_parts.append(
            f"<tr style='background-color:{row_bg}'>"
            f"<td class='{css}'><a href='{escape(fidelity_url)}' target='_blank' rel='noopener noreferrer'><strong>{escape(ticker)}</strong></a></td>"
            f"<td>{escape(yield_display)}</td>"
            f"<td>{_fmt_money(spot_val)}</td>"
            f"<td>{_fmt_money(strike_val)}</td>"
            f"<td>{escape(otm_pct)}</td>"
            f"<td>{escape(str(rec.get('expiration') or '-'))}</td>"
            f"<td>{rec.get('dte') or '-'}</td>"
            f"<td>{_fmt_money(rec.get('premium'))}</td>"
            f"<td>{delta_display}</td>"
            f"<td>{escape(ivr_display)}</td>"
            f"<td>{_fmt_money(rec.get('max_profit'))}</td>"
            f"<td>{_fmt_money(rec.get('downside_breakeven'))}</td>"
            f"<td>{flags_html}</td>"
            f"<td class='reason-cell'>{escape(rec.get('reason', ''))}</td>"
            f"</tr>"
        )

    html_parts.append("</tbody></table>")
    html_parts.append("</details>")


def _render_cc_recommendations(recommendations: List[Dict[str, Any]], html_parts: List[str]) -> None:
    """Render the sell call recommendation table."""
    if not recommendations:
        return

    yes_count = sum(1 for r in recommendations if r["recommend"] == "Yes")
    no_count = sum(1 for r in recommendations if r["recommend"] == "No")
    total = len(recommendations)

    html_parts.append("<details class='section section-cc-rec'>")
    html_parts.append(
        f"<summary>Sell Call Recommendations"
        f"<span class='count'>{yes_count} Yes &nbsp;.&nbsp; {no_count} No &nbsp;.&nbsp; {total} total</span></summary>"
    )
    html_parts.append("<div class='cc-rec-body'>")

    for term, term_recs in _term_groups(recommendations):
        _render_sell_call_term(term, term_recs, html_parts)

    has_proxy = any("proxy" in (r.get("ivr_source") or "") for r in recommendations)
    if has_proxy:
        html_parts.append(
            "<p class='rec-footnote'>IVR shown for reference only - it does <strong>not</strong> affect the "
            "recommendation verdict. Computed as HV Rank (current 20-day HV vs 1-year HV range). "
            "True IV Rank requires historical implied volatility data.</p>"
        )

    html_parts.append(
        "<div class='exit-rules'>"
        "<strong>Exit Rules (Sell Calls):</strong>"
        "<ul>"
        "<li>Close the position at <strong>70% of max profit</strong> - capture most of the premium early.</li>"
        "<li>Close or roll up/out if the stock rallies and unrealised loss reaches <strong>2&times; the premium received</strong>.</li>"
        "<li>Roll to a <strong>higher strike or later expiration</strong> if assignment is imminent and you want to retain the shares.</li>"
        "</ul>"
        "</div>"
    )

    html_parts.append("</div>")
    html_parts.append("</details>")


def _render_sell_put_term(term: str, recommendations: List[Dict[str, Any]], html_parts: List[str]) -> None:
    verdict_class = {"Yes": "rec-yes", "No": "rec-no"}
    group_colors = ("#ffffff", "#f1f3f5")

    html_parts.append("<details class='rec-term'>")
    html_parts.append(
        f"<summary>{escape(_display_term_label(term))}"
        f"<span class='count'>({len(recommendations)} row{'s' if len(recommendations) != 1 else ''})</span></summary>"
    )
    html_parts.append("<table class='rec-table'>")
    html_parts.append(
        "<thead><tr>"
        "<th>Ticker</th>"
        "<th>AnnualYield</th>"
        "<th>Current</th>"
        "<th>Strike</th>"
        "<th>%ToStrike</th>"
        "<th>Expiration</th>"
        "<th>DTE</th>"
        "<th>Premium</th>"
        "<th>Delta</th>"
        "<th>IVR</th>"
        "<th>MaxProfit</th>"
        "<th>Breakeven</th>"
        "<th>CashRqd</th>"
        "<th>Why</th>"
        "</tr></thead><tbody>"
    )

    prev_ticker = None
    group_idx = -1
    for rec in recommendations:
        ticker = rec["ticker"]
        if ticker != prev_ticker:
            group_idx += 1
            prev_ticker = ticker
        row_bg = group_colors[group_idx % 2]
        fidelity_url = f"https://digital.fidelity.com/ftgw/digital/options-research/?symbol={ticker}"

        ivr_display = _fmt_pct(rec.get("ivr"))
        ann_yield = rec.get("annualized_yield")
        yield_display = f"{ann_yield:.1%}" if ann_yield is not None else "-"

        near_flags: List[str] = []
        if rec.get("near_support"):
            near_flags.append("support")
        if rec.get("near_round_number"):
            near_flags.append("round#")
        near_str = " ".join(near_flags)
        reason_full = rec.get("reason", "")
        if near_str:
            reason_full = f"{reason_full} [{near_str}]" if reason_full else near_str

        verdict = rec["recommend"]
        css = verdict_class.get(verdict, "")
        delta_raw = rec.get("delta")
        delta_display = f"{abs(float(delta_raw)):.3f}" if delta_raw is not None else "-"
        spot_val = rec.get("spot")
        strike_val = rec.get("strike")
        pct_to_strike = (
            f"{(strike_val - spot_val) / spot_val * 100:.1f}%"
            if spot_val and strike_val
            else "-"
        )
        html_parts.append(
            f"<tr style='background-color:{row_bg}'>"
            f"<td class='{css}'><a href='{escape(fidelity_url)}' target='_blank' rel='noopener noreferrer'><strong>{escape(ticker)}</strong></a></td>"
            f"<td>{escape(yield_display)}</td>"
            f"<td>{_fmt_money(spot_val)}</td>"
            f"<td>{_fmt_money(strike_val)}</td>"
            f"<td>{escape(pct_to_strike)}</td>"
            f"<td>{escape(str(rec.get('expiration') or '-'))}</td>"
            f"<td>{rec.get('dte') or '-'}</td>"
            f"<td>{_fmt_money(rec.get('premium'))}</td>"
            f"<td>{delta_display}</td>"
            f"<td>{escape(ivr_display)}</td>"
            f"<td>{_fmt_money(rec.get('max_profit'))}</td>"
            f"<td>{_fmt_money(rec.get('breakeven'))}</td>"
            f"<td>{_fmt_money(rec.get('cash_required'))}</td>"
            f"<td class='reason-cell'>{escape(reason_full)}</td>"
            f"</tr>"
        )

    html_parts.append("</tbody></table>")
    html_parts.append("</details>")


def _render_csp_recommendations(recommendations: List[Dict[str, Any]], html_parts: List[str]) -> None:
    """Render the sell put recommendation table at the top of the report."""
    if not recommendations:
        return

    yes_count = sum(1 for r in recommendations if r["recommend"] == "Yes")
    no_count = sum(1 for r in recommendations if r["recommend"] == "No")
    total = len(recommendations)

    html_parts.append("<details class='section section-rec'>")
    html_parts.append(
        f"<summary>Sell Put Recommendations"
        f"<span class='count'>{yes_count} Yes &nbsp;.&nbsp; {no_count} No &nbsp;.&nbsp; {total} total</span></summary>"
    )

    html_parts.append("<div class='rec-body'>")
    for term, term_recs in _term_groups(recommendations):
        _render_sell_put_term(term, term_recs, html_parts)

    has_proxy = any("proxy" in (r.get("ivr_source") or "") for r in recommendations)
    if has_proxy:
        html_parts.append(
            "<p class='rec-footnote'>IVR is a proxy calculated from the option IV (or current 20-day HV) "
            "relative to the historical HV range over the available price history. "
            "True IV Rank requires historical implied volatility data not available via yfinance.</p>"
        )

    html_parts.append(
        "<div class='exit-rules'>"
        "<strong>Exit Rules:</strong>"
        "<ul>"
        "<li>Close the position at <strong>50-70% of max profit</strong> - lock in gains early.</li>"
        "<li>Close or roll if unrealised loss reaches <strong>2x the premium received</strong>.</li>"
        "</ul>"
        "</div>"
    )

    html_parts.append("</div>")
    html_parts.append("</details>")


def write_reports(
    candidates: List[Dict[str, Any]],
    config: Dict[str, Any],
    disclaimer: str,
    csp_recommendations: Optional[List[Dict[str, Any]]] = None,
    cc_recommendations: Optional[List[Dict[str, Any]]] = None,
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
        "summary::before{content:'▸';font-size:14px;transition:transform 0.15s;}"
        "details[open]>summary::before{transform:rotate(90deg);}"
        ".section>summary{font-size:16px;font-weight:700;background:#0969da;color:#fff;border:none;}"
        ".section>summary:hover{background:#0860ca;}"
        ".section>summary::before{color:#fff;}"
        ".ticker-block{margin-left:20px;}"
        ".ticker-block>summary{font-size:13px;font-weight:600;background:#f6f8fa;border:1px solid #d0d7de;color:#24292f;}"
        ".ticker-block>summary:hover{background:#eaf0fb;}"
        ".rec-term{margin:0 0 8px 12px;}"
        ".rec-term>summary{font-size:13px;font-weight:600;background:#f6f8fa;border:1px solid #d0d7de;color:#24292f;}"
        ".rec-term>summary:hover{background:#eaf0fb;}"
        ".report-controls{display:flex;gap:8px;margin:8px 0 14px;}"
        ".report-controls button{border:1px solid #d0d7de;background:#f6f8fa;color:#24292f;border-radius:6px;padding:6px 12px;font-size:12px;font-weight:600;cursor:pointer;}"
        ".report-controls button:hover{background:#eaeef2;}"
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
        ".reason-cell{white-space:normal;min-width:180px;max-width:320px;font-size:11px;color:#444;}"
        ".rec-footnote{font-size:11px;color:#666;margin:4px 0 8px;font-style:italic;}"
        ".exit-rules{font-size:12px;background:#f0f7ff;border:1px solid #b6d4fe;border-radius:4px;padding:8px 12px;margin-top:4px;}"
        ".exit-rules ul{margin:4px 0 0 16px;padding:0;}"
        ".exit-rules li{margin-bottom:2px;}"
        ".warn-banner{background:#fff3cd;border:2px solid #ffc107;border-radius:6px;padding:10px 14px;margin-bottom:14px;}"
        ".warn-banner h3{margin:0 0 6px;color:#856404;font-size:14px;}"
        ".warn-banner ul{margin:4px 0 0 18px;padding:0;color:#6c4a00;font-size:13px;}"
        ".warn-banner li{margin-bottom:3px;}"
        ".section-cc-rec>summary{background:#0d6efd;color:#fff;font-size:17px;}"
        ".section-cc-rec>summary:hover{background:#0b5ed7;}"
        ".section-cc-rec>summary::before{color:#fff;}"
        ".cc-rec-body{padding:8px 12px 12px;}"
        ".cc-rec-table{margin:0 0 10px 0;width:100%;}"
        ".cc-rec-table th{background:#dbeafe;font-size:12px;}"
        ".cc-rec-table td{font-size:12px;}"
        ".flag-res{color:#0d6efd;font-weight:600;}"
        ".flag-round{color:#6c757d;}"
        ".flag-below{color:#dc3545;font-weight:600;}"
        "</style></head><body>"
    )
    profile_name = str(config.get("active_profile") or "").strip()
    profile_suffix = f" (for {profile_name})" if profile_name else ""
    html_parts.append(f"<h1>Daily Options Screening Report{escape(profile_suffix)} &mdash; {run_day}</h1>")
    html_parts.append(
        "<div class='report-controls'>"
        "<button type='button' onclick=\"document.querySelectorAll('details').forEach(d => d.open = true)\">Expand All</button>"
        "<button type='button' onclick=\"document.querySelectorAll('details').forEach(d => d.open = false)\">Collapse All</button>"
        "</div>"
    )
    html_parts.append(f"<p class='note'>{escape(disclaimer)}</p>")

    if fallback_events:
        affected_tickers = sorted(set(e.split(':')[0].split(' ')[0] for e in fallback_events))
        tickers_str = ", ".join(affected_tickers) if affected_tickers else "some tickers"
        html_parts.append("<div class='warn-banner'>")
        html_parts.append(
            f"<h3>&#9888; Data Provider Warning: Public provider was inaccessible &mdash; "
            f"yfinance used as fallback for: {escape(tickers_str)}</h3>"
            f"<p style='margin:0;font-size:12px;color:#6c4a00;'>Check the run log for details.</p>"
        )
        html_parts.append("</div>")

    if csp_recommendations:
        _render_csp_recommendations(csp_recommendations, html_parts)

    if cc_recommendations:
        _render_cc_recommendations(cc_recommendations, html_parts)

    if df.empty:
        html_parts.append("<p>No candidates passed filters today.</p>")
    else:
        def render_candidate_term(term_df: pd.DataFrame, term_label: str) -> None:
            count_label = f"{len(term_df)} candidate{'s' if len(term_df) != 1 else ''}"
            html_parts.append("<details class='rec-term'>")
            html_parts.append(
                f"<summary>{escape(_display_term_label(term_label))}<span class='count'>({count_label})</span></summary>"
            )

            tdf = term_df.copy()
            for col in ("ivr", "max_profit"):
                if col not in tdf.columns:
                    tdf[col] = None

            tdf = tdf.sort_values(
                ["ticker", "expiration", "annualized_yield"], ascending=[True, True, False], na_position="last"
            )

            view = tdf[
                [
                    "ticker",
                    "annualized_yield",
                    "spot",
                    "strike",
                    "otm_pct",
                    "expiration",
                    "dte",
                    "mid",
                    "delta",
                    "ivr",
                    "max_profit",
                    "breakeven",
                    "why_ranked_high",
                ]
            ].copy()
            view["annualized_yield"] = (view["annualized_yield"] * 100).map(
                lambda x: f"{x:.2f}%" if pd.notna(x) else "-"
            )
            view["otm_pct"] = view["otm_pct"].map(
                lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "-"
            )
            view["delta"] = view["delta"].map(lambda x: "-" if pd.isna(x) else f"{x:.3f}")
            view["ivr"] = view["ivr"].map(
                lambda x: "-" if pd.isna(x) or x is None else f"{x:.1f}%"
            )
            view["max_profit"] = view["max_profit"].map(
                lambda x: "-" if pd.isna(x) or x is None else f"${x:,.2f}"
            )
            view["breakeven"] = view["breakeven"].map(
                lambda x: "-" if pd.isna(x) or x is None else f"${x:,.2f}"
            )
            view = view.rename(
                columns={
                    "ticker": "Ticker",
                    "annualized_yield": "AnnualYield",
                    "spot": "Current",
                    "strike": "Strike",
                    "otm_pct": "%OTM",
                    "expiration": "Expiration",
                    "dte": "DTE",
                    "mid": "Premium",
                    "delta": "Delta",
                    "ivr": "IVR",
                    "max_profit": "MaxProfit",
                    "breakeven": "Breakeven",
                    "why_ranked_high": "Why",
                }
            )

            display_cols = list(view.columns)
            tbl_html = ["<table class='table'><thead><tr>"]
            for col in display_cols:
                tbl_html.append(f"<th>{escape(col)}</th>")
            tbl_html.append("</tr></thead><tbody>")

            group_colors = ("#ffffff", "#f1f3f5")
            prev_ticker = None
            grp_idx = -1
            for _, row in view.iterrows():
                ticker_val = str(row.get("Ticker", ""))
                if ticker_val != prev_ticker:
                    grp_idx += 1
                    prev_ticker = ticker_val
                row_bg = group_colors[grp_idx % 2]
                tbl_html.append(f"<tr style='background-color:{row_bg}'>")
                for col in display_cols:
                    cell = row[col]
                    if col in ("Current", "Strike", "Premium"):
                        try:
                            cell_str = f"${float(cell):,.2f}"
                        except (ValueError, TypeError):
                            cell_str = "-"
                    elif col == "DTE":
                        try:
                            cell_str = str(int(float(cell)))
                        except (ValueError, TypeError):
                            cell_str = "-"
                    elif col == "Ticker":
                        fidelity_url = f"https://digital.fidelity.com/ftgw/digital/options-research/?symbol={cell}"
                        cell_str = (
                            f"<a href='{escape(fidelity_url)}' target='_blank' rel='noopener noreferrer'>"
                            f"<strong>{escape(str(cell))}</strong></a>"
                        )
                        tbl_html.append(f"<td>{cell_str}</td>")
                        continue
                    else:
                        try:
                            cell_str = "-" if pd.isna(cell) else str(cell)
                        except TypeError:
                            cell_str = str(cell) if cell is not None else "-"
                    tbl_html.append(f"<td>{escape(cell_str)}</td>")
                tbl_html.append("</tr>")
            tbl_html.append("</tbody></table>")
            html_parts.append("".join(tbl_html))
            html_parts.append("</details>")

        def render_section(section_df: pd.DataFrame, title: str, css_class: str) -> None:
            n = len(section_df)
            count_label = f"{n} candidate{'s' if n != 1 else ''}"
            html_parts.append(f"<details class='section {css_class}'>")
            html_parts.append(f"<summary>{title}<span class='count'>({count_label})</span></summary>")
            for term_label in ("Short-Term", "Medium-Term", "Long-Term"):
                term_df = section_df[section_df["bucket_label"] == term_label]
                if not term_df.empty:
                    render_candidate_term(term_df, term_label)
            html_parts.append("</details>")

        puts_df = df[df["strategy"] == "PUT"]
        calls_df = df[df["strategy"] == "CALL"]

        if not puts_df.empty:
            render_section(puts_df, "Sell Put Candidates", "section-puts")
        if not calls_df.empty:
            render_section(calls_df, "Sell Call Candidates", "section-calls")

    html_parts.append("<hr>")
    html_parts.append(
        "<p><strong>Risk reminders:</strong> Assignment risk, overnight gaps, earnings/event shocks, "
        "liquidity deterioration, and tail-risk moves can cause losses.</p>"
    )
    html_parts.append("</body></html>")

    html_path.write_text("\n".join(html_parts), encoding="utf-8")
    return str(csv_path), str(html_path)
