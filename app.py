from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
import yaml

sys.path.insert(0, str(Path(__file__).parent))
from agent.utils.env import load_dotenv_if_present
load_dotenv_if_present(".env")

BASE_CONFIG_PATH = Path("config/base.yaml")
USERS_CONFIG_DIR = Path("config/users")
META_FILE = Path(".last_run_meta.json")


# ── Config helpers ─────────────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {} if not value else _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def get_profiles() -> list[str]:
    if not USERS_CONFIG_DIR.exists():
        return []
    return sorted(p.stem for p in USERS_CONFIG_DIR.glob("*.yaml"))


def load_yaml(path: Path) -> dict:
    if path.exists():
        with path.open(encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def load_merged_config(profile: str) -> dict:
    base = load_yaml(BASE_CONFIG_PATH)
    if profile:
        return _deep_merge(base, load_yaml(USERS_CONFIG_DIR / f"{profile}.yaml"))
    return base


def save_profile(profile: str, updates: dict) -> None:
    path = USERS_CONFIG_DIR / f"{profile}.yaml"
    merged = _deep_merge(load_yaml(path), updates)
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(merged, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def build_run_config(profile: str, overrides: dict) -> dict:
    cfg = _deep_merge(load_merged_config(profile), overrides)
    cfg["active_profile"] = profile
    return cfg


# ── Run meta persistence ───────────────────────────────────────────────────────

def save_run_meta(meta: dict) -> None:
    with META_FILE.open("w", encoding="utf-8") as f:
        json.dump(meta, f)


def load_run_meta() -> dict:
    if META_FILE.exists():
        try:
            with META_FILE.open(encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


# ── UI helpers ─────────────────────────────────────────────────────────────────

def tickers_to_text(tickers: list) -> str:
    return "\n".join(str(t).upper() for t in tickers)


def text_to_tickers(text: str) -> list[str]:
    return [t.strip().upper() for t in text.strip().splitlines() if t.strip()]


def build_strike_df(tickers: list[str], min_strikes: dict, max_strikes: dict) -> pd.DataFrame:
    seen: set[str] = set()
    rows = []
    for t in list(tickers) + list(min_strikes) + list(max_strikes):
        key = t.upper()
        if key not in seen:
            seen.add(key)
            rows.append({
                "Ticker": key,
                "Min Strike ($)": float(min_strikes[key]) if key in min_strikes else None,
                "Max Strike ($)": float(max_strikes[key]) if key in max_strikes else None,
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        [{"Ticker": "", "Min Strike ($)": None, "Max Strike ($)": None}]
    )


def extract_strike_dicts(df: pd.DataFrame) -> tuple[dict, dict]:
    mn_d, mx_d = {}, {}
    for _, row in df.iterrows():
        ticker = str(row.get("Ticker", "")).strip().upper()
        if not ticker:
            continue
        mn, mx = row.get("Min Strike ($)"), row.get("Max Strike ($)")
        if mn is not None and not (isinstance(mn, float) and pd.isna(mn)):
            mn_d[ticker] = float(mn)
        if mx is not None and not (isinstance(mx, float) and pd.isna(mx)):
            mx_d[ticker] = float(mx)
    return mn_d, mx_d


# ── Data loading + merging ─────────────────────────────────────────────────────

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


def _load_calls_view(
    csv_path: Optional[str],
    monthly_path: Optional[str],
    recs_path: Optional[str],
) -> pd.DataFrame:
    pieces = []
    if csv_path and Path(csv_path).exists():
        df = pd.read_csv(csv_path)
        if "strategy" in df.columns:
            c = df[df["strategy"] == "CALL"].copy()
            if not c.empty:
                pieces.append(c)
    if monthly_path and Path(monthly_path).exists():
        m = pd.read_csv(monthly_path)
        if not m.empty:
            pieces.append(m)
    if not pieces:
        return pd.DataFrame()

    combined = pd.concat(pieces, ignore_index=True)
    combined["_rec"] = ""
    combined["_flags"] = ""
    combined["_why"] = combined["why_ranked_high"].fillna("") if "why_ranked_high" in combined.columns else ""
    combined["_breakeven"] = combined["breakeven"].copy() if "breakeven" in combined.columns else pd.Series(dtype=float)
    combined["_premium"] = combined["mid"].copy() if "mid" in combined.columns else pd.Series(dtype=float)

    if recs_path and Path(recs_path).exists():
        recs = pd.read_csv(recs_path)
        _merge_recs(combined, recs, breakeven_field="downside_breakeven")
        recs2 = recs[recs["strike"].notna() & recs["expiration"].notna()]
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


def _load_puts_view(
    csv_path: Optional[str],
    recs_path: Optional[str],
) -> pd.DataFrame:
    pieces = []
    if csv_path and Path(csv_path).exists():
        df = pd.read_csv(csv_path)
        if "strategy" in df.columns:
            p = df[df["strategy"] == "PUT"].copy()
            if not p.empty:
                pieces.append(p)
    if not pieces:
        return pd.DataFrame()

    combined = pd.concat(pieces, ignore_index=True)
    combined["_rec"] = ""
    combined["_flags"] = ""
    combined["_why"] = combined["why_ranked_high"].fillna("") if "why_ranked_high" in combined.columns else ""
    combined["_breakeven"] = combined["breakeven"].copy() if "breakeven" in combined.columns else pd.Series(dtype=float)
    combined["_premium"] = combined["mid"].copy() if "mid" in combined.columns else pd.Series(dtype=float)
    combined["_cash_req"] = pd.Series(dtype=float)

    if recs_path and Path(recs_path).exists():
        recs = pd.read_csv(recs_path)
        _merge_recs(combined, recs, breakeven_field="breakeven")
        recs2 = recs[recs["strike"].notna() & recs["expiration"].notna()]
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
        "Premium":     df["_premium"].apply(_fmt_money),
        "Delta":       df["delta"].apply(lambda x: f"{abs(float(x)):.3f}" if pd.notna(x) else "-") if "delta" in df.columns else "-",
        "IVR":         df["ivr"].apply(lambda x: _fmt_pct(x)) if "ivr" in df.columns else "-",
        "MaxProfit":   df["max_profit"].apply(_fmt_money) if "max_profit" in df.columns else "-",
        "Breakeven":   df["_breakeven"].apply(_fmt_money),
        "Flags":       df["_flags"].fillna(""),
        "Why":         df["_why"].fillna(""),
    })
    out["_dte_num"] = _dte_num(df)
    return out


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
        "Premium":     df["_premium"].apply(_fmt_money),
        "Delta":       df["delta"].apply(lambda x: f"{abs(float(x)):.3f}" if pd.notna(x) else "-") if "delta" in df.columns else "-",
        "IVR":         df["ivr"].apply(lambda x: _fmt_pct(x)) if "ivr" in df.columns else "-",
        "MaxProfit":   df["max_profit"].apply(_fmt_money) if "max_profit" in df.columns else "-",
        "Breakeven":   df["_breakeven"].apply(_fmt_money),
        "CashRqd":     df["_cash_req"].apply(_fmt_money),
        "Why":         df["_why"].fillna(""),
    })
    out["_dte_num"] = _dte_num(df)
    return out


# ── Display ────────────────────────────────────────────────────────────────────

_BUCKET_LABELS = ["0–14 days", "15–45 days", "Long term (46d+)"]


def _dte_bucket(n: int) -> str:
    if n <= 14:
        return "0–14 days"
    if n <= 45:
        return "15–45 days"
    return "Long term (46d+)"


def _render_html_table(display_df: pd.DataFrame) -> str:
    from html import escape as _esc
    if display_df.empty:
        return "<em style='color:inherit;opacity:0.6;'>No data available for the selected filters.</em>"

    exps = sorted(display_df["Expiration"].unique()) if "Expiration" in display_df.columns else []
    exp_alt = {exp: (i % 2 == 1) for i, exp in enumerate(exps)}

    # visible columns only
    cols = [c for c in display_df.columns if not c.startswith("_")]

    css = (
        "<style>"
        ".ot{border-collapse:collapse;width:100%;font-size:12.5px;"
        "font-family:ui-monospace,'Segoe UI Mono',Consolas,monospace;}"
        ".ot th{padding:5px 10px;text-align:left;"
        "border-bottom:2px solid rgba(128,128,128,0.35);"
        "white-space:nowrap;font-weight:600;font-size:12px;"
        "background:rgba(128,128,128,0.12);}"
        ".ot td{padding:4px 10px;"
        "border-bottom:1px solid rgba(128,128,128,0.1);"
        "white-space:nowrap;vertical-align:top;}"
        ".ot tr.ry td{background:#1e7e4e !important;color:#cff5dd !important;}"
        ".ot tr.rn td{background:#9b2027 !important;color:#f8d7da !important;}"
        ".ot tr.ra td{background:rgba(110,130,200,0.09);}"
        ".ot td.wc{white-space:normal;min-width:160px;max-width:300px;"
        "font-size:11px;line-height:1.35;}"
        ".ot td.fc{font-size:11px;opacity:0.85;}"
        "</style>"
    )

    parts = [css, "<table class='ot'><thead><tr>"]
    for col in cols:
        parts.append(f"<th>{_esc(col)}</th>")
    parts.append("</tr></thead><tbody>")

    for _, row in display_df.iterrows():
        rec = str(row.get("Rec", "")).strip()
        exp = str(row.get("Expiration", ""))
        if rec == "Yes":
            cls = "ry"
        elif rec == "No":
            cls = "rn"
        elif exp_alt.get(exp, False):
            cls = "ra"
        else:
            cls = ""
        parts.append(f"<tr class='{cls}'>")
        for col in cols:
            cell = _esc(str(row.get(col, "") if row.get(col, "") is not None else ""))
            if col == "Why":
                parts.append(f"<td class='wc'>{cell}</td>")
            elif col == "Flags":
                parts.append(f"<td class='fc'>{cell}</td>")
            else:
                parts.append(f"<td>{cell}</td>")
        parts.append("</tr>")

    parts.append("</tbody></table>")
    return "".join(parts)


def _show_tab(display_df: pd.DataFrame, key_prefix: str) -> None:
    if display_df.empty:
        st.info("No data available.")
        return

    all_tickers = sorted(display_df["Ticker"].unique()) if "Ticker" in display_df.columns else []

    # Determine which buckets have data
    bucket_counts: dict[str, int] = {"0–14 days": 0, "15–45 days": 0, "Long term (46d+)": 0}
    if "_dte_num" in display_df.columns:
        for n in display_df["_dte_num"]:
            bucket_counts[_dte_bucket(int(n))] += 1
    available_buckets = [b for b in _BUCKET_LABELS if bucket_counts[b] > 0]

    f1, f2 = st.columns(2)
    sel_tickers = f1.multiselect("Ticker", all_tickers, default=all_tickers, key=f"{key_prefix}_tickers")
    sel_buckets = f2.multiselect(
        "Expiration range",
        available_buckets,
        default=available_buckets,
        key=f"{key_prefix}_buckets",
    )

    mask = pd.Series(True, index=display_df.index)
    if "Ticker" in display_df.columns:
        mask &= display_df["Ticker"].isin(sel_tickers)
    if "_dte_num" in display_df.columns and sel_buckets:
        mask &= display_df["_dte_num"].apply(lambda n: _dte_bucket(int(n)) in sel_buckets)

    view = display_df[mask].reset_index(drop=True)
    st.html(_render_html_table(view))
    st.caption(f"{len(view)} rows — 🟢 Yes · 🔴 No · alternating tint = expiration group")


# ── Schedule helpers ───────────────────────────────────────────────────────────

def _next_scheduled_time(times: list) -> str:
    """Return 'Next: HH:MM' label for the nearest upcoming scheduled time."""
    from datetime import timedelta
    now = datetime.now()
    upcoming = []
    for t in times:
        try:
            parts = str(t).strip().split(":")
            h, m = int(parts[0]), int(parts[1])
            candidate = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            upcoming.append((candidate, f"{h:02d}:{m:02d}"))
        except Exception:
            pass
    if not upcoming:
        return ""
    upcoming.sort()
    return f"Next: {upcoming[0][1]}"


@st.fragment(run_every=30)
def _schedule_watcher() -> None:
    """Runs every 30 s; triggers a pipeline run when the clock matches a schedule entry."""
    if st.session_state.is_running or st.session_state.should_run:
        return
    active_profile = st.session_state.get("_active_profile", "")
    cfg = load_merged_config(active_profile)
    sched = cfg.get("schedule", {})
    if not sched.get("enabled", False):
        return
    times = sched.get("times", []) or []
    now = datetime.now()
    today = now.date().isoformat()
    for t in times:
        try:
            parts = str(t).strip().split(":")
            h, m = int(parts[0]), int(parts[1])
        except Exception:
            continue
        if now.hour == h and now.minute == m:
            key = f"{today}_{h:02d}:{m:02d}"
            if st.session_state._last_auto_run_key != key:
                st.session_state._last_auto_run_key = key
                run_cfg = load_merged_config(active_profile)
                run_cfg["active_profile"] = active_profile
                st.session_state.pending_run_config = run_cfg
                st.session_state.should_run = True
                st.session_state.is_running = True
                st.rerun()


# ── Page setup ─────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Options Screener",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    "<style>"
    "[data-testid='stSidebar'],[data-testid='collapsedControl'],"
    "[data-testid='stSidebarCollapsedControl']{display:none !important;}"
    ".block-container{padding-top:0.6rem !important;padding-bottom:1rem !important;}"
    "#MainMenu,footer,header{visibility:hidden;height:0;}"
    "</style>",
    unsafe_allow_html=True,
)

for key, default in [
    ("last_run_log", ""),
    ("last_run_ok", None),
    ("last_csv_path", None),
    ("last_cc_recs_path", None),
    ("last_csp_recs_path", None),
    ("last_monthly_calls_path", None),
    ("last_run_timestamp", None),
    ("is_running", False),
    ("should_run", False),
    ("pending_run_config", None),
    ("_last_auto_run_key", ""),
    ("_active_profile", ""),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# Restore last run from disk on fresh session load
if st.session_state.last_run_ok is None:
    _meta = load_run_meta()
    if _meta:
        st.session_state.last_run_ok = _meta.get("ok")
        st.session_state.last_csv_path = _meta.get("csv_path")
        st.session_state.last_cc_recs_path = _meta.get("cc_recs_path")
        st.session_state.last_csp_recs_path = _meta.get("csp_recs_path")
        st.session_state.last_monthly_calls_path = _meta.get("monthly_calls_path")
        st.session_state.last_run_timestamp = _meta.get("timestamp")


# ── Top bar ────────────────────────────────────────────────────────────────────

profiles = get_profiles()
default_profile = os.getenv("OPTIONS_SCREENER_PROFILE", profiles[0] if profiles else "")

col_run, col_profile, col_title, _col_pad = st.columns([0.7, 1.5, 4, 1.5])

profile = col_profile.selectbox(
    "Profile",
    options=profiles,
    index=profiles.index(default_profile) if default_profile in profiles else 0,
    label_visibility="collapsed",
)
st.session_state._active_profile = profile

run_clicked = col_run.button(
    "▶ Run",
    type="primary",
    use_container_width=True,
    disabled=st.session_state.is_running,
)
if st.session_state.is_running:
    col_run.markdown(
        "<div style='font-size:10px;color:#f97316;text-align:center;margin-top:2px;'>⏳ Running…</div>",
        unsafe_allow_html=True,
    )
elif st.session_state.last_run_ok is True:
    col_run.markdown(
        "<div style='font-size:10px;color:#22c55e;text-align:center;margin-top:2px;'>✓ Success</div>",
        unsafe_allow_html=True,
    )
elif st.session_state.last_run_ok is False:
    col_run.markdown(
        "<div style='font-size:10px;color:#ef4444;text-align:center;margin-top:2px;'>✗ Failed</div>",
        unsafe_allow_html=True,
    )

_sched_cfg_top = load_merged_config(profile).get("schedule", {})
if _sched_cfg_top.get("enabled") and _sched_cfg_top.get("times"):
    _next = _next_scheduled_time(_sched_cfg_top["times"])
    if _next:
        col_run.markdown(
            f"<div style='font-size:9px;color:#6b7280;text-align:center;margin-top:1px;'>⏰ {_next}</div>",
            unsafe_allow_html=True,
        )

ts = st.session_state.last_run_timestamp
ts_line = (
    f"<div style='font-size:11px;color:#6b7280;margin-top:5px;letter-spacing:0.02em;'>"
    f"Last run: {ts}</div>"
) if ts else ""
col_title.markdown(
    f"<div style='text-align:center;padding-top:0.1rem;'>"
    f"<span style='font-size:30px;font-weight:800;font-style:italic;color:#f1f5f9;letter-spacing:-0.4px;"
    f"border-bottom:3px solid #f97316;padding-bottom:4px;'>"
    f"📈 Options Screener</span>"
    f"{ts_line}"
    f"</div>",
    unsafe_allow_html=True,
)

cfg = load_merged_config(profile)
cc_cfg = cfg.get("cc_recommendation", {})

_schedule_watcher()

# ── Config expander ────────────────────────────────────────────────────────────

with st.expander("⚙️ Configure", expanded=False):
    col_cc, col_csp, col_settings = st.columns([5, 3, 2])

    with col_cc:
        st.subheader("📈 Covered Calls")
        cc_tickers_text = st.text_area(
            "Tickers (one per line)",
            value=tickers_to_text(cfg.get("covered_call_tickers", [])),
            height=80,
            key=f"cc_tickers_{profile}",
        )
        c1, c2 = st.columns(2)
        min_yield_pct = c1.number_input(
            "Min Annual Yield %",
            min_value=0.0, max_value=100.0,
            value=round(float(cc_cfg.get("min_yield", 0.10)) * 100, 1),
            step=0.5, format="%.1f",
            key=f"cc_min_yield_{profile}",
        )
        long_term_months = c2.number_input(
            "Monthly chains beyond 45d",
            min_value=0, max_value=24,
            value=int(cc_cfg.get("long_term_months", 9)),
            step=1,
            key=f"cc_ltm_{profile}",
        )
        st.caption("Strike range per ticker (blank = no limit)")
        cc_tickers_live = text_to_tickers(cc_tickers_text)
        strike_df = build_strike_df(
            cc_tickers_live,
            cc_cfg.get("min_strike_prices") or {},
            cc_cfg.get("max_strike_prices") or {},
        )
        edited_strikes = st.data_editor(
            strike_df,
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "Ticker": st.column_config.TextColumn("Ticker", width="small"),
                "Min Strike ($)": st.column_config.NumberColumn("Min ($)", format="$%.0f"),
                "Max Strike ($)": st.column_config.NumberColumn("Max ($)", format="$%.0f"),
            },
            use_container_width=True,
            key=f"strikes_{profile}",
        )

    with col_csp:
        st.subheader("📉 Cash-Secured Puts")
        csp_tickers_text = st.text_area(
            "Tickers (one per line)",
            value=tickers_to_text(cfg.get("cash_secured_put_tickers", [])),
            height=80,
            key=f"csp_tickers_{profile}",
        )

    with col_settings:
        st.subheader("⚙️ Settings")
        providers = ["public", "yfinance"]
        cur_provider = str(cfg.get("options_data_provider", "yfinance")).lower()
        provider = st.selectbox(
            "Options Provider",
            options=providers,
            index=providers.index(cur_provider) if cur_provider in providers else 1,
            key=f"provider_{profile}",
        )

        st.divider()
        st.caption("🕐 Auto-Run Schedule")
        sched_cfg = cfg.get("schedule", {})
        sched_enabled = st.checkbox(
            "Enabled",
            value=bool(sched_cfg.get("enabled", True)),
            key=f"sched_enabled_{profile}",
        )
        sched_times_text = st.text_area(
            "Times (24h, one per line)",
            value="\n".join(str(t) for t in (sched_cfg.get("times") or [])),
            height=80,
            placeholder="07:45\n12:00",
            key=f"sched_times_{profile}",
            help="24-hour format, e.g. 07:45 or 14:00",
        )

        st.write("")
        if st.button("💾 Save to Profile", use_container_width=True, disabled=st.session_state.is_running):
            min_s, max_s = extract_strike_dicts(edited_strikes)
            parsed_times = [t.strip() for t in sched_times_text.strip().splitlines() if t.strip()]
            save_profile(profile, {
                "covered_call_tickers": text_to_tickers(cc_tickers_text),
                "cash_secured_put_tickers": text_to_tickers(csp_tickers_text),
                "options_data_provider": provider,
                "cc_recommendation": {
                    "min_yield": min_yield_pct / 100,
                    "min_strike_prices": min_s,
                    "max_strike_prices": max_s,
                    "long_term_months": int(long_term_months),
                },
                "schedule": {
                    "enabled": sched_enabled,
                    "times": parsed_times,
                },
            })
            st.success("Saved!")


# ── Run ────────────────────────────────────────────────────────────────────────

if run_clicked and not st.session_state.is_running:
    min_strikes, max_strikes = extract_strike_dicts(edited_strikes)
    st.session_state.pending_run_config = build_run_config(profile, {
        "covered_call_tickers": text_to_tickers(cc_tickers_text),
        "cash_secured_put_tickers": text_to_tickers(csp_tickers_text),
        "options_data_provider": provider,
        "cc_recommendation": {
            "min_yield": min_yield_pct / 100,
            "min_strike_prices": min_strikes,
            "max_strike_prices": max_strikes,
            "long_term_months": int(long_term_months),
        },
    })
    st.session_state.should_run = True
    st.session_state.is_running = True
    st.rerun()


# ── Results ────────────────────────────────────────────────────────────────────

if st.session_state.is_running:
    st.info("⏳ Pipeline running — showing previous results, will refresh when complete…")

if st.session_state.last_run_ok:
    tab_calls, tab_puts = st.tabs(["📈 Calls", "📉 Puts"])

    with tab_calls:
        calls_raw = _load_calls_view(
            st.session_state.last_csv_path,
            st.session_state.last_monthly_calls_path,
            st.session_state.last_cc_recs_path,
        )
        calls_display = _build_calls_display(calls_raw) if not calls_raw.empty else pd.DataFrame()
        _show_tab(calls_display, "calls")

    with tab_puts:
        puts_raw = _load_puts_view(
            st.session_state.last_csv_path,
            st.session_state.last_csp_recs_path,
        )
        puts_display = _build_puts_display(puts_raw) if not puts_raw.empty else pd.DataFrame()
        _show_tab(puts_display, "puts")

elif st.session_state.last_run_ok is False and not st.session_state.is_running:
    with st.expander("📝 Log", expanded=True):
        st.code(st.session_state.last_run_log or "(no log)", language=None)


# ── Execute queued run ─────────────────────────────────────────────────────────

if st.session_state.should_run and st.session_state.pending_run_config is not None:
    st.session_state.should_run = False
    run_cfg = st.session_state.pending_run_config

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
        yaml.dump(run_cfg, tmp, default_flow_style=False, allow_unicode=True)
        tmp_path = tmp.name

    log_placeholder = st.empty()
    log_lines: list[str] = []
    ok = False
    try:
        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        proc = subprocess.Popen(
            [sys.executable, "main.py", "--headless", "--config", tmp_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        for line in proc.stdout:
            log_lines.append(line.rstrip())
            log_placeholder.code("\n".join(log_lines[-35:]), language=None)
        proc.wait()
        ok = proc.returncode == 0
    except Exception as exc:
        log_lines.append(f"ERROR: {exc}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)
        st.session_state.is_running = False

    st.session_state.last_run_log = "\n".join(log_lines)
    st.session_state.last_run_ok = ok

    report_dir = Path(run_cfg.get("output_dir", "./reports"))
    today_str = date.today().isoformat()
    csv_p = report_dir / f"{today_str}_options_report.csv"
    cc_recs_p = report_dir / f"{today_str}_cc_recs.csv"
    csp_recs_p = report_dir / f"{today_str}_csp_recs.csv"
    monthly_p = report_dir / f"{today_str}_monthly_calls.csv"

    st.session_state.last_csv_path = str(csv_p) if csv_p.exists() else None
    st.session_state.last_cc_recs_path = str(cc_recs_p) if cc_recs_p.exists() else None
    st.session_state.last_csp_recs_path = str(csp_recs_p) if csp_recs_p.exists() else None
    st.session_state.last_monthly_calls_path = str(monthly_p) if monthly_p.exists() else None

    if ok:
        log_placeholder.empty()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        st.session_state.last_run_timestamp = timestamp
        save_run_meta({
            "ok": True,
            "csv_path": st.session_state.last_csv_path,
            "cc_recs_path": st.session_state.last_cc_recs_path,
            "csp_recs_path": st.session_state.last_csp_recs_path,
            "monthly_calls_path": st.session_state.last_monthly_calls_path,
            "timestamp": timestamp,
        })

    st.rerun()
