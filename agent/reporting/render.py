from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


def write_reports(candidates: List[Dict[str, Any]], config: Dict[str, Any], disclaimer: str) -> Tuple[str, str]:
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
        "</style></head><body>"
    )
    html_parts.append(f"<h1>Daily Options Screening Report &mdash; {run_day}</h1>")
    html_parts.append(f"<p class='note'>{disclaimer}</p>")

    if df.empty:
        html_parts.append("<p>No candidates passed filters today.</p>")
    else:
        def render_ticker_block(tdf: pd.DataFrame, ticker: str) -> None:
            fidelity_url = f"https://digital.fidelity.com/ftgw/digital/options-research/?symbol={ticker}"
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
            html_parts.append(f"<summary>{title}<span class='count'>({count_label})</span></summary>")
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
