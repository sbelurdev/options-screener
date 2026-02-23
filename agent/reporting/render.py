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

    html_parts: List[str] = []
    html_parts.append("<html><head><meta charset='utf-8'>")
    html_parts.append("<title>Options Report</title>")
    html_parts.append(
        "<style>body{font-family:Segoe UI,Arial,sans-serif;margin:20px;}"
        "h1,h2,h3{margin-bottom:8px;}"
        "table{border-collapse:collapse;width:auto;table-layout:auto;margin-bottom:12px;}"
        "th,td{border:1px solid #d0d7de;padding:3px 6px;font-size:12px;text-align:left;white-space:nowrap;vertical-align:top;}"
        "th{background:#f6f8fa;}"
        ".note{font-size:12px;color:#444;padding:8px;background:#fff8c5;border:1px solid #e3b341;}"
        "</style></head><body>"
    )
    html_parts.append(f"<h1>Daily Options Screening Report - {run_day}</h1>")
    html_parts.append(f"<p class='note'>{disclaimer}</p>")

    if df.empty:
        html_parts.append("<p>No candidates passed filters today.</p>")
    else:
        horizon_map = {
            "current_week": "Current Week",
            "next_week": "Next Week",
            "monthly": "Monthly",
        }
        horizon_order = {"current_week": 0, "next_week": 1, "monthly": 2}
        trade_map = {"PUT": "SELL PUT", "CALL": "Covered CALL"}

        for ticker in sorted(df["ticker"].unique()):
            fidelity_url = f"https://digital.fidelity.com/ftgw/digital/options-research/?symbol={ticker}"
            html_parts.append(f"<h2><a href='{fidelity_url}' target='_blank' rel='noopener noreferrer'>{ticker}</a></h2>")
            tdf = df[df["ticker"] == ticker].copy()
            if tdf.empty:
                continue

            tdf["time_horizon"] = tdf["bucket"].map(horizon_map).fillna(tdf["bucket_label"])
            tdf["trade_type"] = tdf["strategy"].map(trade_map).fillna(tdf["strategy"])
            tdf["horizon_rank"] = tdf["bucket"].map(horizon_order).fillna(99)
            tdf = tdf.sort_values(["horizon_rank", "strategy", "score"], ascending=[True, True, False])

            view = tdf[
                [
                    "time_horizon",
                    "trade_type",
                    "expiration",
                    "strike",
                    "spot",
                    "mid",
                    "annualized_yield",
                    "delta",
                    "delta_source",
                    "dte",
                    "spread_pct",
                    "volume",
                    "open_interest",
                    "earnings_before_expiry",
                    "score",
                    "why_ranked_high",
                ]
            ].copy()
            view["annualized_yield"] = (view["annualized_yield"] * 100).map(lambda x: f"{x:.2f}%")
            view["spread_pct"] = (view["spread_pct"] * 100).map(lambda x: f"{x:.2f}%")
            view["delta"] = view["delta"].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
            view["score"] = view["score"].map(lambda x: f"{x:.3f}")
            view = view.rename(columns={"why_ranked_high": "why"})
            html_parts.append(view.to_html(index=False, escape=True, classes="table"))

    html_parts.append("<hr>")
    html_parts.append(
        "<p><strong>Risk reminders:</strong> Assignment risk, overnight gaps, earnings/event shocks, "
        "liquidity deterioration, and tail-risk moves can cause losses.</p>"
    )
    html_parts.append("</body></html>")

    html_path.write_text("\n".join(html_parts), encoding="utf-8")
    return str(csv_path), str(html_path)
