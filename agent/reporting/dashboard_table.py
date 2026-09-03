"""
Shared "dashboard" table rendering — the dark, monospace, expiration-grouped
table with YES/NO verdict tinting, delta risk dots, and %OTM micro-bars.

This is the single source of truth for that table so the Streamlit dashboard
(app.py) and the static per-ticker HTML files (agent/reporting/render.py)
render identically instead of drifting into two look-alike implementations.
"""

from __future__ import annotations

from datetime import datetime
from html import escape as _esc
from typing import Optional

import pandas as pd


def _fmt_money(val) -> str:
    try:
        v = float(val)
        return f"${v:,.2f}" if not pd.isna(v) else "-"
    except (TypeError, ValueError):
        return "-"


def _fmt_pct(val, scale: float = 1.0) -> str:
    try:
        v = float(val)
        return f"{v * scale:.1f}%" if not pd.isna(v) else "-"
    except (TypeError, ValueError):
        return "-"


def _merge_recs(
    combined: pd.DataFrame,
    recs: pd.DataFrame,
    breakeven_field: str = "downside_breakeven",
) -> None:
    recs = recs[recs["strike"].notna() & recs["expiration"].notna()]
    combined["_exp_str"] = combined["expiration"].astype(str)

    for _, rec in recs.iterrows():
        t = str(rec.get("ticker", ""))
        exp = str(rec.get("expiration", ""))
        strike_raw = rec.get("strike")
        if not t or not exp or pd.isna(strike_raw):
            continue
        strike = float(strike_raw)
        mask = (
            (combined["ticker"].astype(str) == t)
            & (combined["_exp_str"] == exp)
            & combined["strike"].notna()
            & ((combined["strike"] - strike).abs() < 0.01)
        )
        if not mask.any():
            continue
        combined.loc[mask, "_rec"] = str(rec.get("recommend", ""))
        why = str(rec.get("reason", ""))
        if why:
            combined.loc[mask, "_why"] = why
        be = rec.get(breakeven_field)
        if pd.notna(be):
            combined.loc[mask, "_breakeven"] = float(be)
        pm = rec.get("premium")
        if pd.notna(pm):
            combined.loc[mask, "_premium"] = float(pm)

    combined.drop(columns=["_exp_str"], inplace=True, errors="ignore")


def build_calls_combined(
    candidates_df: Optional[pd.DataFrame],
    monthly_df: Optional[pd.DataFrame],
    recs_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """CALL candidates (regular + monthly) with recommendation verdict/flags merged in."""
    pieces = [d for d in (candidates_df, monthly_df) if d is not None and not d.empty]
    if not pieces:
        return pd.DataFrame()

    combined = pd.concat(pieces, ignore_index=True)
    combined["_rec"] = ""
    combined["_flags"] = ""
    combined["_why"] = combined["why_ranked_high"].fillna("") if "why_ranked_high" in combined.columns else ""
    combined["_breakeven"] = combined["breakeven"].copy() if "breakeven" in combined.columns else pd.Series(dtype=float)
    if "fill_price" in combined.columns:
        combined["_premium"] = combined["fill_price"].fillna(combined.get("mid"))
    else:
        combined["_premium"] = combined["mid"].copy() if "mid" in combined.columns else pd.Series(dtype=float)

    if recs_df is not None and not recs_df.empty:
        _merge_recs(combined, recs_df, breakeven_field="downside_breakeven")
        recs2 = recs_df[recs_df["strike"].notna() & recs_df["expiration"].notna()]
        combined["_exp_str2"] = combined["expiration"].astype(str)
        for _, rec in recs2.iterrows():
            t = str(rec.get("ticker", ""))
            exp = str(rec.get("expiration", ""))
            strike_raw = rec.get("strike")
            if not t or not exp or pd.isna(strike_raw):
                continue
            mask = (
                (combined["ticker"].astype(str) == t)
                & (combined["_exp_str2"] == exp)
                & combined["strike"].notna()
                & ((combined["strike"] - float(strike_raw)).abs() < 0.01)
            )
            if not mask.any():
                continue
            flags = []
            if rec.get("near_resistance"):
                flags.append("▲ resistance")
            if rec.get("near_round_number"):
                flags.append("○ round#")
            if rec.get("below_min_price"):
                flags.append("⚠ below min")
            combined.loc[mask, "_flags"] = " ".join(flags)
        combined.drop(columns=["_exp_str2"], inplace=True, errors="ignore")

    combined["_exp_dt"] = pd.to_datetime(combined["expiration"], errors="coerce")
    combined = combined.sort_values(["_exp_dt", "strike"], ascending=[True, True]).reset_index(drop=True)
    combined.drop(columns=["_exp_dt"], inplace=True, errors="ignore")
    return combined


def build_puts_combined(
    candidates_df: Optional[pd.DataFrame],
    recs_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """PUT candidates with recommendation verdict/flags/cash-required merged in."""
    if candidates_df is None or candidates_df.empty:
        return pd.DataFrame()

    combined = candidates_df.copy()
    combined["_rec"] = ""
    combined["_flags"] = ""
    combined["_why"] = combined["why_ranked_high"].fillna("") if "why_ranked_high" in combined.columns else ""
    combined["_breakeven"] = combined["breakeven"].copy() if "breakeven" in combined.columns else pd.Series(dtype=float)
    if "fill_price" in combined.columns:
        combined["_premium"] = combined["fill_price"].fillna(combined.get("mid"))
    else:
        combined["_premium"] = combined["mid"].copy() if "mid" in combined.columns else pd.Series(dtype=float)
    combined["_cash_req"] = pd.Series(dtype=float)

    if recs_df is not None and not recs_df.empty:
        _merge_recs(combined, recs_df, breakeven_field="breakeven")
        recs2 = recs_df[recs_df["strike"].notna() & recs_df["expiration"].notna()]
        combined["_exp_str2"] = combined["expiration"].astype(str)
        for _, rec in recs2.iterrows():
            t = str(rec.get("ticker", ""))
            exp = str(rec.get("expiration", ""))
            strike_raw = rec.get("strike")
            if not t or not exp or pd.isna(strike_raw):
                continue
            mask = (
                (combined["ticker"].astype(str) == t)
                & (combined["_exp_str2"] == exp)
                & combined["strike"].notna()
                & ((combined["strike"] - float(strike_raw)).abs() < 0.01)
            )
            if not mask.any():
                continue
            cr = rec.get("cash_required")
            if pd.notna(cr):
                combined.loc[mask, "_cash_req"] = float(cr)
            flags = []
            if rec.get("near_support"):
                flags.append("▼ support")
            if rec.get("near_round_number"):
                flags.append("○ round#")
            combined.loc[mask, "_flags"] = " ".join(flags)
        combined.drop(columns=["_exp_str2"], inplace=True, errors="ignore")

    combined["_exp_dt"] = pd.to_datetime(combined["expiration"], errors="coerce")
    combined = combined.sort_values(["_exp_dt", "strike"], ascending=[True, True]).reset_index(drop=True)
    combined.drop(columns=["_exp_dt"], inplace=True, errors="ignore")
    return combined


def _dte_num(df: pd.DataFrame) -> pd.Series:
    if "dte" in df.columns:
        return df["dte"].apply(lambda x: int(float(x)) if pd.notna(x) else 9999)
    return pd.Series(9999, index=df.index)


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """Column as float Series, or NaN series when absent (older CSVs)."""
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce")
    return pd.Series(float("nan"), index=df.index)


def _earnings_flag(df: pd.DataFrame) -> pd.Series:
    if "earnings_before_expiry" not in df.columns:
        return pd.Series("", index=df.index)
    return df["earnings_before_expiry"].apply(
        lambda x: "⚠" if x in (True, "True", "true", 1, 1.0) else ""
    )


def _attach_sort_keys(out: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Hidden numeric columns used by the Sort-by control and tooltips."""
    out["_dte_num"] = _dte_num(df)
    out["_yield_num"] = _col(df, "annualized_yield").fillna(-1.0)
    out["_score_num"] = _col(df, "score").fillna(-1.0)
    out["_prem_num"] = pd.to_numeric(df["_premium"], errors="coerce").fillna(-1.0)
    out["_delta_num"] = _col(df, "delta").abs().fillna(9.0)
    out["_ivr_src"] = df["ivr_source"].fillna("").astype(str) if "ivr_source" in df.columns else ""
    return out


def _build_calls_display(df: pd.DataFrame) -> pd.DataFrame:
    def fmt_otm(row):
        v = row.get("otm_pct")
        try:
            if pd.notna(v):
                return f"{float(v) * 100:.1f}%"
        except (TypeError, ValueError):
            pass
        s, k = row.get("spot"), row.get("strike")
        try:
            if pd.notna(s) and pd.notna(k) and float(s) > 0:
                return f"{(float(k) - float(s)) / float(s) * 100:.1f}%"
        except (TypeError, ValueError):
            pass
        return "-"

    out = pd.DataFrame({
        "Rec":         df["_rec"].fillna(""),
        "Ticker":      df["ticker"].astype(str) if "ticker" in df.columns else "",
        "AnnualYield": df["annualized_yield"].apply(lambda x: _fmt_pct(x, 100)) if "annualized_yield" in df.columns else "-",
        "Current":     df["spot"].apply(_fmt_money) if "spot" in df.columns else "-",
        "Strike":      df["strike"].apply(_fmt_money) if "strike" in df.columns else "-",
        "%OTM":        df.apply(fmt_otm, axis=1),
        "Expiration":  df["expiration"].astype(str) if "expiration" in df.columns else "",
        "DTE":         df["dte"].apply(lambda x: str(int(x)) if pd.notna(x) else "-") if "dte" in df.columns else "-",
        "E⚠":          _earnings_flag(df),
        "Premium":     df["_premium"].apply(_fmt_money),
        "Delta":       df["delta"].apply(lambda x: f"{abs(float(x)):.3f}" if pd.notna(x) else "-") if "delta" in df.columns else "-",
        "IVR":         df["ivr"].apply(lambda x: _fmt_pct(x)) if "ivr" in df.columns else "-",
        "VRP":         _col(df, "vrp").apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-"),
        "ΘYld":        _col(df, "theta_yield").apply(lambda x: f"{x * 100:.0f}%" if pd.notna(x) else "-"),
        "MaxProfit":   df["max_profit"].apply(_fmt_money) if "max_profit" in df.columns else "-",
        "Breakeven":   df["_breakeven"].apply(_fmt_money),
        "Score":       _col(df, "score").apply(lambda x: f"{x:.3f}" if pd.notna(x) else "-"),
        "Flags":       df["_flags"].fillna(""),
        "Why":         df["_why"].fillna(""),
    })
    return _attach_sort_keys(out, df)


def _build_puts_display(df: pd.DataFrame) -> pd.DataFrame:
    def fmt_to_strike(row):
        s, k = row.get("spot"), row.get("strike")
        try:
            if pd.notna(s) and pd.notna(k) and float(s) > 0:
                return f"{(float(k) - float(s)) / float(s) * 100:.1f}%"
        except (TypeError, ValueError):
            pass
        return "-"

    out = pd.DataFrame({
        "Rec":         df["_rec"].fillna(""),
        "Ticker":      df["ticker"].astype(str) if "ticker" in df.columns else "",
        "AnnualYield": df["annualized_yield"].apply(lambda x: _fmt_pct(x, 100)) if "annualized_yield" in df.columns else "-",
        "Current":     df["spot"].apply(_fmt_money) if "spot" in df.columns else "-",
        "Strike":      df["strike"].apply(_fmt_money) if "strike" in df.columns else "-",
        "%ToStrike":   df.apply(fmt_to_strike, axis=1),
        "Expiration":  df["expiration"].astype(str) if "expiration" in df.columns else "",
        "DTE":         df["dte"].apply(lambda x: str(int(x)) if pd.notna(x) else "-") if "dte" in df.columns else "-",
        "E⚠":          _earnings_flag(df),
        "Premium":     df["_premium"].apply(_fmt_money),
        "Delta":       df["delta"].apply(lambda x: f"{abs(float(x)):.3f}" if pd.notna(x) else "-") if "delta" in df.columns else "-",
        "IVR":         df["ivr"].apply(lambda x: _fmt_pct(x)) if "ivr" in df.columns else "-",
        "VRP":         _col(df, "vrp").apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-"),
        "ΘYld":        _col(df, "theta_yield").apply(lambda x: f"{x * 100:.0f}%" if pd.notna(x) else "-"),
        "MaxProfit":   df["max_profit"].apply(_fmt_money) if "max_profit" in df.columns else "-",
        "Breakeven":   df["_breakeven"].apply(_fmt_money),
        "CashRqd":     df["_cash_req"].apply(_fmt_money),
        "Score":       _col(df, "score").apply(lambda x: f"{x:.3f}" if pd.notna(x) else "-"),
        "Why":         df["_why"].fillna(""),
    })
    return _attach_sort_keys(out, df)


_BUCKET_LABELS = ["0–14 days", "15–45 days", "Long term (46d+)"]


def _dte_bucket(n: int) -> str:
    if n <= 14:
        return "0–14 days"
    if n <= 45:
        return "15–45 days"
    return "Long term (46d+)"


# Columns rendered right-aligned with tabular numerals
_NUMERIC_COLS = {
    "AnnualYield", "Current", "Strike", "%OTM", "%ToStrike", "DTE", "Premium",
    "Delta", "IVR", "VRP", "ΘYld", "MaxProfit", "Breakeven", "CashRqd", "Score",
    "Level",
}

_TABLE_CSS = (
    "<style>"
    ".ot-wrap{border:1px solid rgba(148,163,184,0.16);border-radius:12px;"
    # overflow-y explicit (not left to default-with-x) so the criteria hover
    # card below can escape vertically instead of being clipped by the same
    # box that scrolls the table horizontally.
    "overflow-x:auto;overflow-y:visible;background:rgba(15,23,42,0.35);}"
    ".ot{border-collapse:separate;border-spacing:0;width:100%;font-size:12.5px;"
    "font-family:ui-monospace,'Segoe UI Mono',Consolas,monospace;}"
    ".ot th{padding:8px 10px;text-align:left;white-space:nowrap;"
    "font-weight:600;font-size:10.5px;letter-spacing:0.08em;text-transform:uppercase;"
    "color:#94a3b8;background:rgba(30,41,59,0.85);"
    "border-bottom:1px solid rgba(148,163,184,0.25);}"
    ".ot th.num,.ot td.num{text-align:right;font-variant-numeric:tabular-nums;}"
    ".ot td{padding:5px 10px;white-space:nowrap;vertical-align:top;"
    "border-bottom:1px solid rgba(148,163,184,0.07);}"
    # Expiration banding (on tr, shows through the translucent verdict tints)
    ".ot tr.g1{background:rgba(99,130,200,0.07);}"
    # Verdict rows: translucent tint + accent stripe, not a solid block
    ".ot tr.ry td{background:rgba(34,197,94,0.13);}"
    ".ot tr.ry td:first-child{box-shadow:inset 3px 0 0 #22c55e;}"
    ".ot tr.rn td{background:rgba(239,68,68,0.09);}"
    ".ot tr.rn td:first-child{box-shadow:inset 3px 0 0 #ef4444;}"
    ".ot tbody tr:not(.gh):hover td{background:rgba(148,163,184,0.13);}"
    # Expiration group header rows
    ".ot tr.gh td{background:linear-gradient(90deg,rgba(249,115,22,0.10),rgba(30,41,59,0.6) 45%);"
    "color:#cbd5e1;font-weight:700;font-size:11px;letter-spacing:0.07em;"
    "padding:6px 12px;border-top:1px solid rgba(148,163,184,0.18);"
    "border-bottom:1px solid rgba(148,163,184,0.18);}"
    ".ot tr.gh td .ghd{color:#fbbf24;}"
    # Verdict badges
    ".pe-badge{display:inline-block;padding:1px 8px;border-radius:999px;"
    "font-size:10px;font-weight:700;letter-spacing:0.06em;}"
    ".pe-yes{background:rgba(34,197,94,0.18);color:#4ade80;border:1px solid rgba(34,197,94,0.45);}"
    ".pe-no{background:rgba(239,68,68,0.14);color:#f87171;border:1px solid rgba(239,68,68,0.40);}"
    ".pe-mid{background:rgba(148,163,184,0.14);color:#94a3b8;border:1px solid rgba(148,163,184,0.35);}"
    ".ot td[title]{cursor:help;}"
    # Per-criterion hover card (pure CSS, no JS): a hidden panel revealed by
    # :hover on its anchor. A compact 3-column table — name, value, note all
    # on one line per criterion — colored green/yellow/red against the same
    # thresholds the recommender actually screens by.
    ".ot .crit-anchor{position:relative;display:inline-block;cursor:help;}"
    ".ot .crit-card{display:none;position:absolute;z-index:100;top:100%;"
    "margin-top:6px;background:#0b1220;border:1px solid rgba(148,163,184,0.32);"
    "border-radius:10px;padding:6px;width:400px;box-shadow:0 14px 32px rgba(0,0,0,0.55);"
    "white-space:normal;text-align:left;}"
    ".ot .crit-anchor.left .crit-card{left:0;}"
    ".ot .crit-anchor.right .crit-card{right:0;}"
    ".ot .crit-anchor:hover .crit-card{display:block;}"
    ".ot .crit-table{border-collapse:collapse;width:100%;}"
    ".ot .crit-table td{padding:3px 6px;font-size:10.5px;border-left:3px solid transparent;"
    "vertical-align:baseline;}"
    ".ot .crit-table tr.cg td{border-left-color:#22c55e;background:rgba(34,197,94,0.09);}"
    ".ot .crit-table tr.cy td{border-left-color:#fbbf24;background:rgba(251,191,36,0.07);}"
    ".ot .crit-table tr.cr td{border-left-color:#ef4444;background:rgba(239,68,68,0.09);}"
    ".ot .crit-table tr+tr td{border-top:1px solid rgba(148,163,184,0.08);}"
    ".ot .crit-name{font-weight:700;color:#e2e8f0;white-space:nowrap;}"
    ".ot .crit-val{font-weight:700;white-space:nowrap;text-align:right;}"
    ".ot .crit-table tr.cg .crit-val{color:#4ade80;}"
    ".ot .crit-table tr.cy .crit-val{color:#fbbf24;}"
    ".ot .crit-table tr.cr .crit-val{color:#f87171;}"
    ".ot .crit-note{color:#94a3b8;line-height:1.3;}"
    ".ot td.yld{font-weight:700;color:#fbbf24;}"
    ".ot td.wc{white-space:normal;min-width:160px;max-width:300px;"
    "font-size:11px;line-height:1.35;color:#94a3b8;}"
    ".ot td.fc{font-size:11px;opacity:0.85;}"
    ".ot a{color:inherit;text-decoration:underline dotted;text-underline-offset:3px;}"
    ".ot a:hover{color:#fbbf24;}"
    # Distance-to-strike micro-bar (under %OTM / %ToStrike values)
    ".ot .dbar{height:3px;width:54px;margin:3px 0 0 auto;border-radius:2px;"
    "background:rgba(148,163,184,0.15);}"
    ".ot .dbar i{display:block;height:100%;border-radius:2px;"
    "background:linear-gradient(90deg,#f97316,#fbbf24);}"
    # Delta risk dot: green ≤0.15 · amber ≤0.25 · red beyond
    ".ot .dot{display:inline-block;width:7px;height:7px;border-radius:50%;"
    "margin-right:6px;vertical-align:1px;}"
    ".ot .dot.dg{background:#22c55e;}.ot .dot.da{background:#fbbf24;}"
    ".ot .dot.dr{background:#ef4444;}"
    # "Level" column: signed distance from the suggested strike (context
    # panel) — positive/green means this row's strike clears the level,
    # negative/red means it's on the wrong side of it.
    ".ot td.lvl-pos{color:#4ade80;font-weight:600;}"
    ".ot td.lvl-neg{color:#f87171;font-weight:600;}"
    "</style>"
)


def _pct_from_cell(cell: str) -> Optional[float]:
    try:
        return float(cell.replace("%", "").strip())
    except (TypeError, ValueError):
        return None


_CRIT_CLASS = {"green": "cg", "yellow": "cy", "red": "cr"}


def _render_criteria_card(criteria, align: str) -> str:
    """The hover-card HTML for a row's per-criterion breakdown (see
    agent.scoring.explain.criteria_rows) — a compact 3-column table (name,
    value, note all on one line per criterion), colored against the same
    thresholds the recommender actually screens by, with the typical range
    stated so the color is never a black box. `align` ("left"/"right")
    anchors the card so it opens away from the table edge it's nearest to."""
    if not isinstance(criteria, list) or not criteria:
        return ""
    rows_html = []
    for c in criteria:
        cls = _CRIT_CLASS.get(c.get("color"), "cy")
        note = f"<td class='crit-note'>{_esc(c.get('note', ''))}</td>" if c.get("note") else "<td></td>"
        rows_html.append(
            f"<tr class='{cls}'>"
            f"<td class='crit-name'>{_esc(c.get('name', ''))}</td>"
            f"<td class='crit-val'>{_esc(c.get('value', ''))}</td>"
            f"{note}</tr>"
        )
    return (f"<span class='crit-anchor {align}'>ⓘ<span class='crit-card'>"
           f"<table class='crit-table'>" + "".join(rows_html) + "</table></span></span>")


def _exp_group_label(exp: str, dte, count: int) -> str:
    try:
        weekday = datetime.strptime(exp, "%Y-%m-%d").strftime("%a")
        date_part = f"{exp} ({weekday})"
    except ValueError:
        date_part = exp
    dte_part = f" · {dte} DTE" if dte not in (None, "", "-") else ""
    return f"<span class='ghd'>📅 {date_part}</span>{dte_part} · {count} contract{'s' if count != 1 else ''}"


def _render_html_table(display_df: pd.DataFrame, group_by_expiration: bool = True) -> str:
    if display_df.empty:
        return "<em style='color:inherit;opacity:0.6;'>No data available for the selected filters.</em>"

    # visible columns only
    cols = [c for c in display_df.columns if not c.startswith("_")]
    group_sizes = (
        display_df.groupby("Expiration").size().to_dict()
        if "Expiration" in display_df.columns
        else {}
    )

    parts = [_TABLE_CSS, "<div class='ot-wrap'><table class='ot'><thead><tr>"]
    for col in cols:
        num_cls = " class='num'" if col in _NUMERIC_COLS else ""
        parts.append(f"<th{num_cls}>{_esc(col)}</th>")
    parts.append("</tr></thead><tbody>")

    group_idx = -1
    prev_exp: Optional[str] = None
    for _, row in display_df.iterrows():
        exp = str(row.get("Expiration", ""))
        if exp != prev_exp:
            group_idx += 1
            prev_exp = exp
            if group_by_expiration and exp:
                label = _exp_group_label(exp, row.get("DTE"), int(group_sizes.get(exp, 0)))
                parts.append(f"<tr class='gh'><td colspan='{len(cols)}'>{label}</td></tr>")

        rec = str(row.get("Rec", "")).strip()
        band = "g1" if group_idx % 2 else ""
        verdict = "ry" if rec == "Yes" else ("rn" if rec == "No" else "")
        parts.append(f"<tr class='{(band + ' ' + verdict).strip()}'>")

        for col in cols:
            cell = _esc(str(row.get(col, "") if row.get(col, "") is not None else ""))
            num_cls = "num" if col in _NUMERIC_COLS else ""
            if col == "Rec":
                # _compare_why is the plain-English "why this trade vs its
                # same-expiration neighbors" sentence (agent.scoring.explain);
                # _verdict_why (the recommender's raw reason, or the score
                # breakdown for an unpromoted row) is only a fallback for a
                # single-row peer group with nothing to compare against.
                # _criteria (also agent.scoring.explain) drives the rich,
                # color-coded per-criterion card on the ⓘ icon.
                explain = (str(row.get("_compare_why", "") or "")
                          or str(row.get("_verdict_why", "") or ""))
                title = f" title='{_esc(explain)}'" if explain else ""
                if rec == "Yes":
                    badge = "<span class='pe-badge pe-yes'>YES</span>"
                elif rec == "No":
                    badge = "<span class='pe-badge pe-no'>NO</span>"
                else:
                    badge = "<span class='pe-badge pe-mid'>—</span>"
                    if not title:
                        title = " title='Not this term\\'s top pick - ranked by score, not by a Yes/No verdict'"
                card = _render_criteria_card(row.get("_criteria"), "left")
                parts.append(f"<td{title}>{badge} {card}</td>")
            elif col == "Why":
                parts.append(f"<td class='wc'>{cell}</td>")
            elif col == "Flags":
                parts.append(f"<td class='fc'>{cell}</td>")
            elif col == "AnnualYield":
                parts.append(f"<td class='num yld'>{cell}</td>")
            elif col in ("%OTM", "%ToStrike"):
                pct = _pct_from_cell(cell)
                bar = ""
                if pct is not None:
                    width = max(4, min(100, abs(pct) / 25.0 * 100))  # saturates at 25% OTM
                    bar = f"<div class='dbar'><i style='width:{width:.0f}%'></i></div>"
                parts.append(f"<td class='num'>{cell}{bar}</td>")
            elif col == "Delta":
                d = _pct_from_cell(cell)  # plain float parse; cell has no % sign
                dot = ""
                if d is not None:
                    risk = "dg" if abs(d) <= 0.15 else ("da" if abs(d) <= 0.25 else "dr")
                    dot = f"<span class='dot {risk}'></span>"
                parts.append(f"<td class='num'>{dot}{cell}</td>")
            elif col == "Ticker" and cell:
                url = f"https://digital.fidelity.com/ftgw/digital/options-research/?symbol={cell}"
                parts.append(
                    f"<td><a href='{_esc(url)}' target='_blank' rel='noopener noreferrer'>"
                    f"<strong>{cell}</strong></a></td>"
                )
            elif col == "IVR":
                # Hover shows where the IVR came from; * marks the HV-rank proxy
                src = str(row.get("_ivr_src", "") or "")
                marker = "*" if "proxy" in src.lower() else ""
                title = f" title='{_esc(src)}'" if src else ""
                parts.append(f"<td class='num'{title}>{cell}{marker}</td>")
            elif col == "Score":
                # Hover shows the plain-English comparison; the raw
                # component breakdown is a fallback with nothing to compare.
                explain = (str(row.get("_compare_why", "") or "")
                          or str(row.get("_score_why", "") or ""))
                title = f" title='{_esc(explain)}'" if explain else ""
                card = _render_criteria_card(row.get("_criteria"), "right")
                parts.append(f"<td class='num'{title}>{cell} {card}</td>")
            elif col == "Level":
                sign_cls = "lvl-pos" if cell.startswith("+") else ("lvl-neg" if cell.startswith("-") else "")
                cls_attr = f" class='num {sign_cls}'".rstrip() if sign_cls else " class='num'"
                parts.append(f"<td{cls_attr}>{cell}</td>")
            else:
                cls_attr = f" class='{num_cls}'" if num_cls else ""
                parts.append(f"<td{cls_attr}>{cell}</td>")
        parts.append("</tr>")

    parts.append("</tbody></table></div>")
    return "".join(parts)
