from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from functools import lru_cache
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
import yaml

sys.path.insert(0, str(Path(__file__).parent))
from agent.utils.env import load_dotenv_if_present
from agent.utils.logging import setup_logging
from agent.reporting.dashboard_table import (
    _BUCKET_LABELS,
    _NUMERIC_COLS,
    _TABLE_CSS,
    _build_calls_display,
    _build_puts_display,
    _col,
    _dte_bucket,
    _exp_group_label,
    _fmt_money,
    _fmt_pct,
    _pct_from_cell,
    _render_html_table,
    build_calls_combined,
    build_puts_combined,
)
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


def _file_mtime(p) -> float:
    try:
        return Path(str(p)).stat().st_mtime
    except (OSError, TypeError, ValueError):
        return -1.0


def discover_latest_report(cfg: dict) -> dict:
    """
    Newest report set in the profile's output dir. Lets the dashboard pick up
    runs produced outside this session (scheduled/headless) without re-running.
    """
    out_dir = Path(str(cfg.get("output_dir") or "./reports"))
    if not out_dir.exists():
        return {}
    csvs = sorted(out_dir.glob("*_options_report.csv"))  # date-prefixed names sort chronologically
    if not csvs:
        return {}
    latest = csvs[-1]
    day = latest.name.split("_")[0]
    mtime = latest.stat().st_mtime
    return {
        "csv_path": str(latest),
        "cc_recs_path": str(out_dir / f"{day}_cc_recs.csv"),
        "csp_recs_path": str(out_dir / f"{day}_csp_recs.csv"),
        "monthly_calls_path": str(out_dir / f"{day}_monthly_calls.csv"),
        "timestamp": datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M"),
        "_mtime": mtime,
    }


# ── Server-side scheduler ────────────────────────────────────────────────────
#
# Runs in a background daemon thread started once per server process (see
# _start_scheduler_thread below), not in a browser-driven st.fragment. A
# browser tab's auto-refresh timer is throttled or paused whenever the tab is
# backgrounded, minimized, or the machine briefly sleeps, which made the old
# approach fire "sometimes" rather than reliably. This thread runs for the
# life of the `streamlit run app.py` process regardless of what any browser
# tab is doing — the only thing it still can't survive is the process itself
# not running (machine fully asleep, or the server not started).

_SCHEDULE_GRACE_MINUTES = 30  # catch a slot the thread reaches late (e.g. just after process start)


def _execute_headless_pipeline(profile: str, log) -> dict:
    """Runs `main.py --headless` for one profile and returns a result dict.

    No Streamlit/session_state dependency, so this is safe to call from the
    background scheduler thread (which has no browser session at all) as well
    as from the interactive script. Email delivery happens inside main.py
    itself, so a successful run here has already sent it.
    """
    run_cfg = load_merged_config(profile)
    run_cfg["active_profile"] = profile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
        yaml.dump(run_cfg, tmp, default_flow_style=False, allow_unicode=True)
        tmp_path = tmp.name

    ok = False
    log_lines: list[str] = []
    started = time.time()
    try:
        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        proc = subprocess.Popen(
            [sys.executable, "main.py", "--headless", "--config", tmp_path],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env,
        )
        for line in proc.stdout:
            log_lines.append(line.rstrip())
        proc.wait()
        ok = proc.returncode == 0
    except Exception as exc:  # noqa: BLE001
        log_lines.append(f"ERROR: {exc}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    duration_s = round(time.time() - started, 1)
    result = {"ok": ok, "log": "\n".join(log_lines), "duration_s": duration_s}

    report_dir = Path(run_cfg.get("output_dir", "./reports"))
    today_str = date.today().isoformat()
    for field, name in (("csv_path", "options_report"), ("cc_recs_path", "cc_recs"),
                        ("csp_recs_path", "csp_recs"), ("monthly_calls_path", "monthly_calls")):
        p = report_dir / f"{today_str}_{name}.csv"
        result[field] = str(p) if p.exists() else None

    if ok:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        result["timestamp"] = timestamp
        save_run_meta({
            "ok": True,
            "csv_path": result["csv_path"],
            "cc_recs_path": result["cc_recs_path"],
            "csp_recs_path": result["csp_recs_path"],
            "monthly_calls_path": result["monthly_calls_path"],
            "timestamp": timestamp,
            "duration_s": duration_s,
        })
    log.info(f"[scheduler] Headless run for '{profile}' finished: ok={ok} "
             f"duration={duration_s}s")
    if not ok:
        log.warning(f"[scheduler] Headless run for '{profile}' failed; tail of output:\n"
                    + "\n".join(log_lines[-20:]))
    return result


def _scheduler_profile() -> str:
    """The single profile this server instance runs under — same lookup the
    UI uses to pick its default profile (OPTIONS_SCREENER_PROFILE, else the
    first profile alphabetically). Deliberately NOT every profile under
    config/users/: this is a single-tenant deployment (one instance per
    machine/user), and auto-running every other profile that merely inherits
    base.yaml's schedule default (nobody had to opt in) burns API quota on
    reports nobody asked for and nowhere configured to email."""
    profiles = get_profiles()
    return os.getenv("OPTIONS_SCREENER_PROFILE", profiles[0] if profiles else "")


def _run_scheduler_loop() -> None:
    """Forever loop in its own thread: checks the server's own profile's
    schedule, independent of what any browser session has selected."""
    fired_today: set = set()
    while True:
        try:
            profile = _scheduler_profile()
            if profile:
                now = datetime.now()
                today = now.date().isoformat()
                cfg = load_merged_config(profile)
                sched = cfg.get("schedule", {})
                if sched.get("enabled", False):
                    log = setup_logging(cfg)
                    for t in (sched.get("times") or []):
                        try:
                            parts = str(t).strip().split(":")
                            h, m = int(parts[0]), int(parts[1])
                        except Exception:
                            continue
                        key = (profile, today, f"{h:02d}:{m:02d}")
                        if key in fired_today:
                            continue
                        scheduled_at = now.replace(hour=h, minute=m, second=0, microsecond=0)
                        elapsed_min = (now - scheduled_at).total_seconds() / 60.0
                        if elapsed_min < 0:
                            continue
                        if elapsed_min > _SCHEDULE_GRACE_MINUTES:
                            fired_today.add(key)
                            log.warning(
                                f"[scheduler] {profile} {h:02d}:{m:02d} missed - the scheduler "
                                f"thread did not reach it within {_SCHEDULE_GRACE_MINUTES} min "
                                f"(process may have just started, or was busy with another run)."
                            )
                            continue
                        fired_today.add(key)
                        log.info(f"[scheduler] Triggering headless run for '{profile}' "
                                 f"(scheduled {h:02d}:{m:02d}, actual {now:%H:%M:%S}).")
                        try:
                            _execute_headless_pipeline(profile, log)
                        except Exception as exc:  # noqa: BLE001
                            log.exception(f"[scheduler] Headless run for '{profile}' raised: {exc}")
        except Exception:  # noqa: BLE001
            # A bug here must never kill the loop - a dead scheduler thread is
            # exactly the silent-failure mode this whole redesign exists to avoid.
            pass
        time.sleep(20)


@st.cache_resource
def _start_scheduler_thread() -> threading.Thread:
    """Runs the target exactly once per server process (st.cache_resource is
    shared across all sessions), regardless of how many browser tabs open,
    reconnect, or close."""
    t = threading.Thread(target=_run_scheduler_loop, name="options-scheduler", daemon=True)
    t.start()
    return t


@st.cache_resource
def _start_daytrading_scheduler_thread() -> threading.Thread:
    """Same single-process-lifetime guarantee as _start_scheduler_thread, for
    the DayTrading tab's warm-up/polling/signal-email pipeline — see
    agent.daytrading.scheduler for why that needed the same fix as the CC/CSP
    scheduler above. Scoped to the same single profile this server runs
    under, for the same reason (see _scheduler_profile)."""
    from agent.daytrading.scheduler import start_scheduler_thread
    return start_scheduler_thread(_scheduler_profile())


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
# _fmt_money/_fmt_pct/table-building live in agent/reporting/dashboard_table.py,
# shared with the static per-ticker HTML files so both render identically.

def _load_calls_view(
    csv_path: Optional[str],
    monthly_path: Optional[str],
    recs_path: Optional[str],
) -> pd.DataFrame:
    candidates_df = pd.DataFrame()
    if csv_path and Path(csv_path).exists():
        df = pd.read_csv(csv_path)
        if "strategy" in df.columns:
            candidates_df = df[df["strategy"] == "CALL"].copy()

    monthly_df = pd.DataFrame()
    if monthly_path and Path(monthly_path).exists():
        monthly_df = pd.read_csv(monthly_path)

    recs_df = pd.DataFrame()
    if recs_path and Path(recs_path).exists():
        recs_df = pd.read_csv(recs_path)

    return build_calls_combined(candidates_df, monthly_df, recs_df)


def _load_puts_view(
    csv_path: Optional[str],
    recs_path: Optional[str],
) -> pd.DataFrame:
    candidates_df = pd.DataFrame()
    if csv_path and Path(csv_path).exists():
        df = pd.read_csv(csv_path)
        if "strategy" in df.columns:
            candidates_df = df[df["strategy"] == "PUT"].copy()

    recs_df = pd.DataFrame()
    if recs_path and Path(recs_path).exists():
        recs_df = pd.read_csv(recs_path)

    return build_puts_combined(candidates_df, recs_df)


# ── Display ────────────────────────────────────────────────────────────────────
# _dte_bucket/_NUMERIC_COLS/_TABLE_CSS/_render_html_table etc. are imported from
# agent/reporting/dashboard_table.py (see top of file).


def _show_tab(display_df: pd.DataFrame, key_prefix: str, data_version: str = "") -> None:
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

    # Widget keys are namespaced by data_version (the loaded report's run
    # timestamp) so a fresh run gets fresh filter widgets. Streamlit only
    # honors `default=` the first time a given key is created — on every
    # later rerun it silently restores whatever was last selected for that
    # key instead, so without this a new run's data would keep being
    # filtered by stale selections from the previous report (or from a
    # different profile) rather than showing everything by default.
    version_tag = "".join(ch for ch in str(data_version) if ch.isalnum()) or "v0"
    widget_ns = f"{key_prefix}_{version_tag}"

    f1, f2, f3, f4 = st.columns([2, 2, 1.2, 0.9])
    sel_tickers = f1.multiselect("Ticker", all_tickers, default=all_tickers, key=f"{widget_ns}_tickers")
    sel_buckets = f2.multiselect(
        "Expiration range",
        available_buckets,
        default=available_buckets,
        key=f"{widget_ns}_buckets",
    )
    sort_by = f3.selectbox(
        "Sort by",
        ["Expiration (default)", "Annual Yield ↓", "Score ↓", "Premium ↓", "Delta ↑", "DTE ↑"],
        key=f"{widget_ns}_sort",
    )
    f4.write("")  # vertical alignment with the labelled controls
    yes_only = f4.toggle("✓ YES only", key=f"{widget_ns}_yesonly")

    mask = pd.Series(True, index=display_df.index)
    if "Ticker" in display_df.columns:
        mask &= display_df["Ticker"].isin(sel_tickers)
    if "_dte_num" in display_df.columns and sel_buckets:
        mask &= display_df["_dte_num"].apply(lambda n: _dte_bucket(int(n)) in sel_buckets)
    if yes_only and "Rec" in display_df.columns:
        mask &= display_df["Rec"].astype(str).str.strip() == "Yes"

    view = display_df[mask].reset_index(drop=True)

    _SORT_KEYS = {
        "Annual Yield ↓": ("_yield_num", False),
        "Score ↓": ("_score_num", False),
        "Premium ↓": ("_prem_num", False),
        "Delta ↑": ("_delta_num", True),
        "DTE ↑": ("_dte_num", True),
    }
    custom_sorted = sort_by in _SORT_KEYS
    if custom_sorted:
        col, asc = _SORT_KEYS[sort_by]
        if col in view.columns:
            view = view.sort_values(col, ascending=asc).reset_index(drop=True)

    # Expiration group headers only make sense in expiration order
    st.html(_render_html_table(view, group_by_expiration=not custom_sorted))
    st.caption(
        f"{len(view)} rows · YES/NO = recommendation verdict · banded rows = alternating "
        "expiration groups · IVR* = HV-rank proxy (hover for source) · E⚠ = earnings before expiry"
    )


# ── Performance tab ────────────────────────────────────────────────────────────

def _show_performance(cfg: dict) -> None:
    """Outcome ledger summary + ATM IV history, independent of the last run."""
    tracking = cfg.get("outcome_tracking") or {}
    ledger_path = Path(str(tracking.get("path") or "./cache/outcomes.csv"))
    iv_path = Path(str(cfg.get("iv_history_path") or "./cache/iv_history.csv"))

    ledger = pd.read_csv(ledger_path) if ledger_path.exists() else pd.DataFrame()

    if ledger.empty:
        st.info("No recommendations recorded yet — the outcome ledger fills in as runs complete.")
    else:
        open_df = ledger[ledger["status"] == "open"].copy()
        closed = ledger[ledger["status"] == "closed"].copy()
        graded = closed[closed["outcome"].isin(["expired_otm", "assigned", "called_away"])].copy()

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Open positions", len(open_df))
        m2.metric("Closed (graded)", len(graded))
        kept_rate = (graded["outcome"] == "expired_otm").mean() if len(graded) else None
        m3.metric("Premium-kept rate", f"{kept_rate:.0%}" if kept_rate is not None else "—",
                  help="Share of graded recommendations that expired OTM (full premium kept)")
        total_pnl = pd.to_numeric(graded["option_pnl"], errors="coerce").sum() if len(graded) else 0.0
        m4.metric("Option P&L (closed)", f"${total_pnl:,.0f}",
                  help="Mark-to-expiry, option leg only — ignores rolls and early assignment")

        if len(graded):
            st.caption("By strategy and verdict — do the screener's Yes calls beat its No calls?")
            grp = (
                graded.groupby(["strategy", "verdict"])
                .agg(
                    trades=("outcome", "size"),
                    premium_kept=("outcome", lambda s: (s == "expired_otm").mean()),
                    total_pnl=("option_pnl", lambda s: pd.to_numeric(s, errors="coerce").sum()),
                )
                .reset_index()
            )
            grp["premium_kept"] = grp["premium_kept"].apply(lambda x: f"{x:.0%}")
            grp["total_pnl"] = grp["total_pnl"].apply(lambda x: f"${x:,.0f}")
            st.dataframe(grp, hide_index=True, use_container_width=True)

            # Cumulative option P&L by expiration, once there's a trend to see
            pnl_t = graded[["expiration", "option_pnl"]].copy()
            pnl_t["option_pnl"] = pd.to_numeric(pnl_t["option_pnl"], errors="coerce")
            pnl_t = pnl_t.dropna().sort_values("expiration")
            if pnl_t["expiration"].nunique() >= 2:
                cum = pnl_t.groupby("expiration")["option_pnl"].sum().cumsum()
                cum.name = "Cumulative option P&L ($)"
                st.line_chart(cum)

        col_open, col_closed = st.columns(2)
        with col_open:
            st.subheader("Open")
            if open_df.empty:
                st.caption("None")
            else:
                open_df["days_left"] = (
                    pd.to_datetime(open_df["expiration"], errors="coerce") - pd.Timestamp(date.today())
                ).dt.days
                show = open_df[["run_date", "strategy", "ticker", "term", "verdict",
                                "expiration", "days_left", "strike", "premium", "delta"]]
                st.dataframe(show.sort_values("expiration"), hide_index=True, use_container_width=True)
        with col_closed:
            st.subheader("Closed")
            if graded.empty:
                st.caption("None graded yet")
            else:
                show = graded[["run_date", "strategy", "ticker", "term", "verdict",
                               "expiration", "strike", "premium", "expiry_close",
                               "outcome", "option_pnl"]]
                st.dataframe(show.sort_values("expiration", ascending=False),
                             hide_index=True, use_container_width=True)

    st.divider()
    st.subheader("ATM IV history")
    st.caption(
        "One snapshot per ticker per run day (strike nearest spot, expiry nearest 30 DTE). "
        "True IV Rank replaces the HV-rank proxy once ~20 observations accumulate."
    )
    iv = pd.read_csv(iv_path) if iv_path.exists() else pd.DataFrame()
    if iv.empty:
        st.info("No IV snapshots recorded yet.")
        return
    pivot = iv.pivot_table(index="date", columns="ticker", values="atm_iv")
    st.line_chart(pivot * 100.0, y_label="ATM IV %")
    latest = (
        iv.sort_values("date")
        .groupby("ticker")
        .agg(observations=("atm_iv", "size"),
             current_iv=("atm_iv", "last"),
             low=("atm_iv", "min"),
             high=("atm_iv", "max"))
        .reset_index()
    )
    for col in ("current_iv", "low", "high"):
        latest[col] = latest[col].apply(lambda x: f"{x * 100:.0f}%")
    st.dataframe(latest, hide_index=True, use_container_width=True)


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


@st.fragment(run_every=45)
def _live_status_refresh() -> None:
    """Pure display refresh — the actual scheduling runs in the background
    thread (_run_scheduler_loop), independent of this or any other tab. This
    just re-checks disk periodically so a run that thread (or a headless CLI
    invocation) produced shows up here without the user clicking anything,
    the same way discover_latest_report already does once at page load.
    """
    if st.session_state.is_running or st.session_state.should_run:
        return
    active_profile = st.session_state.get("_active_profile", "")
    if not active_profile:
        return
    _disk = discover_latest_report(load_merged_config(active_profile))
    if _disk and _disk["_mtime"] > _file_mtime(st.session_state.last_csv_path):
        st.session_state.last_run_ok = True
        st.session_state.last_csv_path = _disk["csv_path"]
        st.session_state.last_cc_recs_path = _disk["cc_recs_path"]
        st.session_state.last_csp_recs_path = _disk["csp_recs_path"]
        st.session_state.last_monthly_calls_path = _disk["monthly_calls_path"]
        st.session_state.last_run_timestamp = _disk["timestamp"]
        st.session_state.last_run_duration = None
        st.rerun()


# ── Page setup ─────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="PremiumEdge",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── Hero banner ────────────────────────────────────────────────────────────────

# Drop a finance image at any of these paths to use it as the hero background
# (a dark gradient is layered on top so the title stays readable). Without one,
# the built-in SVG candlestick scene is used.
_HERO_IMG_CANDIDATES = (
    Path("assets/hero_bg.jpg"),
    Path("assets/hero_bg.jpeg"),
    Path("assets/hero_bg.png"),
)
# Background image for the controls zone (Run/profile/Configure). First match wins.
_CONTROLS_BG_CANDIDATES = (
    Path("config/Wall Street Bull Image.png"),
    Path("assets/controls_bg.jpg"),
    Path("assets/controls_bg.png"),
)


@lru_cache(maxsize=8)
def _optimized_image_b64(path_str: str, mtime: float, cache_name: str) -> str:
    """Base64 of an image, downscaled once via Pillow (cache keyed by mtime)."""
    src = Path(path_str)
    try:
        from PIL import Image

        opt = Path("assets") / cache_name
        opt.parent.mkdir(exist_ok=True)
        if not opt.exists() or opt.stat().st_mtime < mtime:
            img = Image.open(src).convert("RGB")
            img.thumbnail((1800, 1200))
            img.save(opt, "JPEG", quality=72)
        data = opt.read_bytes()
    except Exception:
        data = src.read_bytes()
    return base64.b64encode(data).decode("ascii")


def _controls_zone_css() -> str:
    """
    CSS giving the controls zone (Run/profile + Configure expander) a finance
    image background. background-size:cover with a fixed focal point handles
    the expander collapsing/expanding: the zone's height changes, and the
    image simply reveals more or less of itself — no JS, no reflow artifacts.
    """
    for p in _CONTROLS_BG_CANDIDATES:
        if p.exists():
            b64 = _optimized_image_b64(str(p), p.stat().st_mtime, ".controls_bg_optimized.jpg")
            return (
                "<style>"
                ".st-key-pe-controls{"
                "border:1px solid rgba(148,163,184,0.18);border-radius:14px;"
                "padding:16px 18px 14px 18px;margin-bottom:14px;"
                # Left-heavy gradient: controls live on the left, the bull and
                # ticker numbers stay visible on the right.
                "background-image:linear-gradient(100deg,rgba(8,12,24,0.94) 0%,"
                "rgba(8,12,24,0.86) 40%,rgba(10,16,30,0.55) 100%),"
                f"url(data:image/jpeg;base64,{b64});"
                "background-repeat:no-repeat,no-repeat;"
                "background-size:cover,cover;"
                # Focal point ~30% from the top keeps the rising arrow and the
                # ticker board in frame even when collapsed to a slim strip.
                "background-position:center,center 30%;}"
                # Let the image shimmer through the expander instead of a solid block
                ".st-key-pe-controls [data-testid='stExpander'] details{"
                "background:rgba(11,18,32,0.62);"
                "border:1px solid rgba(148,163,184,0.22);border-radius:12px;}"
                "</style>"
            )
    return ""


def _hero_background() -> Tuple[str, str, str]:
    """(background-image layers, positions, sizes) for the hero banner."""
    for p in _HERO_IMG_CANDIDATES:
        if p.exists():
            b64 = _optimized_image_b64(str(p), p.stat().st_mtime, ".hero_bg_optimized.jpg")
            layers = (
                "linear-gradient(90deg,rgba(8,12,24,0.95) 0%,rgba(8,12,24,0.78) 40%,rgba(8,12,24,0.38) 100%),"
                f"url(data:image/jpeg;base64,{b64})"
            )
            return layers, "center,center 30%", "auto,cover"
    layers = (
        f"url(data:image/svg+xml;base64,{_hero_svg_b64()}),"
        "linear-gradient(115deg,#0b1220 0%,#13203c 55%,#0d1526 100%)"
    )
    return layers, "right center,center", "auto 100%,cover"


def _hero_svg_b64() -> str:
    """Deterministic candlestick walk + trend line, embedded as base64 SVG."""
    import random

    rng = random.Random(7)
    candles, closes = [], []
    y = 118.0
    for i in range(15):
        x = 620 + i * 38
        o = y
        c = y + rng.randint(-20, 13)  # downward bias in y = upward price drift
        c = max(24.0, min(140.0, c))
        hi = min(o, c) - rng.randint(5, 14)
        lo = max(o, c) + rng.randint(5, 14)
        color = "#22c55e" if c < o else "#ef4444"
        top, height = min(o, c), max(abs(c - o), 2.5)
        candles.append(
            f"<line x1='{x + 6}' y1='{hi:.0f}' x2='{x + 6}' y2='{lo:.0f}' stroke='{color}' stroke-width='1.5'/>"
            f"<rect x='{x}' y='{top:.0f}' width='12' height='{height:.0f}' fill='{color}' rx='1.5'/>"
        )
        closes.append((x + 6, c))
        y = c

    trend = " ".join(f"{px},{py - 16:.0f}" for px, py in closes)
    grid = "".join(
        f"<line x1='0' y1='{gy}' x2='1200' y2='{gy}' stroke='rgba(148,163,184,0.08)' stroke-width='1'/>"
        for gy in (40, 80, 120)
    ) + "".join(
        f"<line x1='{gx}' y1='0' x2='{gx}' y2='160' stroke='rgba(148,163,184,0.05)' stroke-width='1'/>"
        for gx in range(100, 1200, 100)
    )
    svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' width='1200' height='160' viewBox='0 0 1200 160'>"
        f"{grid}<g opacity='0.4'>{''.join(candles)}</g>"
        f"<polyline points='{trend}' fill='none' stroke='#f97316' stroke-width='2.5' "
        "stroke-linecap='round' stroke-linejoin='round' opacity='0.85'/>"
        "</svg>"
    )
    return base64.b64encode(svg.encode("utf-8")).decode("ascii")


def _render_hero(last_run_ts: Optional[str], duration_s: Optional[float] = None) -> str:
    if last_run_ts:
        meta = f"Last run: {last_run_ts}"
        if duration_s:
            meta += f" · {duration_s:.0f}s"
    else:
        meta = "No runs yet today"
    bg_layers, bg_pos, bg_size = _hero_background()
    return (
        "<style>"
        ".pe-hero{position:relative;border-radius:14px;overflow:hidden;"
        "padding:20px 28px 16px 28px;margin-bottom:12px;"
        "border:1px solid rgba(148,163,184,0.18);"
        f"background-image:{bg_layers};"
        "background-repeat:no-repeat,no-repeat;"
        f"background-position:{bg_pos};background-size:{bg_size};}}"
        ".pe-title{font-size:36px;font-weight:800;letter-spacing:-0.5px;line-height:1.05;"
        "font-style:italic;display:inline-block;"
        "background:linear-gradient(90deg,#f8fafc 0%,#fcd34d 55%,#f97316 100%);"
        "-webkit-background-clip:text;background-clip:text;"
        "-webkit-text-fill-color:transparent;color:transparent;}"
        ".pe-tag{color:#94a3b8;font-size:12px;letter-spacing:0.22em;"
        "text-transform:uppercase;margin-top:2px;}"
        ".pe-meta{position:absolute;right:24px;bottom:14px;color:#64748b;"
        "font-size:11.5px;letter-spacing:0.04em;}"
        "</style>"
        "<div class='pe-hero'>"
        "<div class='pe-title'>📈 PremiumEdge</div>"
        "<div class='pe-tag'>Find the richest premium, risk-adjusted &nbsp;·&nbsp; "
        "covered calls &amp; cash-secured puts</div>"
        f"<div class='pe-meta'>{meta}</div>"
        "</div>"
    )

st.markdown(
    "<style>"
    "[data-testid='stSidebar'],[data-testid='collapsedControl'],"
    "[data-testid='stSidebarCollapsedControl']{display:none !important;}"
    ".block-container{padding-top:0.6rem !important;padding-bottom:1rem !important;}"
    "#MainMenu,footer,header{visibility:hidden;height:0;}"
    # Tabs: brand-orange active state and underline
    ".stTabs [data-baseweb='tab-list']{gap:6px;border-bottom:1px solid rgba(148,163,184,0.18);}"
    ".stTabs [data-baseweb='tab']{font-weight:600;letter-spacing:0.02em;padding:6px 14px;}"
    ".stTabs [aria-selected='true']{color:#f97316 !important;}"
    ".stTabs [data-baseweb='tab-highlight']{background-color:#f97316;}"
    # KPI metric cards
    "[data-testid='stMetric']{background:rgba(30,41,59,0.45);"
    "border:1px solid rgba(148,163,184,0.16);border-radius:12px;"
    "padding:10px 14px 8px 14px;}"
    "[data-testid='stMetricLabel']{font-size:11px;letter-spacing:0.06em;"
    "text-transform:uppercase;color:#94a3b8;}"
    "[data-testid='stMetricDelta']{font-size:11.5px;}"
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
    ("last_run_duration", None),
    ("is_running", False),
    ("should_run", False),
    ("pending_run_config", None),
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
        st.session_state.last_run_duration = _meta.get("duration_s")


# ── Top bar ────────────────────────────────────────────────────────────────────

profiles = get_profiles()
default_profile = os.getenv("OPTIONS_SCREENER_PROFILE", profiles[0] if profiles else "")
_boot_profile = st.session_state.get("profile_sel") or default_profile

# Pick up reports produced outside this session (scheduled/headless runs) so the
# dashboard shows the latest data without pressing Run.
if not st.session_state.is_running:
    _disk = discover_latest_report(load_merged_config(_boot_profile))
    if _disk and _disk["_mtime"] > _file_mtime(st.session_state.last_csv_path):
        st.session_state.last_run_ok = True
        st.session_state.last_csv_path = _disk["csv_path"]
        st.session_state.last_cc_recs_path = _disk["cc_recs_path"]
        st.session_state.last_csp_recs_path = _disk["csp_recs_path"]
        st.session_state.last_monthly_calls_path = _disk["monthly_calls_path"]
        st.session_state.last_run_timestamp = _disk["timestamp"]
        st.session_state.last_run_duration = None

st.markdown(
    _render_hero(st.session_state.last_run_timestamp, st.session_state.last_run_duration),
    unsafe_allow_html=True,
)

# Controls zone: Run/profile row + Configure expander share one keyed container
# so the finance background spans both and flexes with the expander state.
_zone_css = _controls_zone_css()
if _zone_css:
    st.markdown(_zone_css, unsafe_allow_html=True)
_controls = st.container(key="pe-controls")

col_run, col_profile, _col_pad = _controls.columns([0.7, 1.5, 5.5])

profile = col_profile.selectbox(
    "Profile",
    options=profiles,
    index=profiles.index(default_profile) if default_profile in profiles else 0,
    label_visibility="collapsed",
    key="profile_sel",
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

cfg = load_merged_config(profile)
cc_cfg = cfg.get("cc_recommendation", {})

# Starts the server-side scheduler threads exactly once per process (a no-op
# on every call after the first — see their cache_resource decorators); the
# actual triggering lives there, this tab only needs to stay visually in sync
# with whatever they produce.
_start_scheduler_thread()
_start_daytrading_scheduler_thread()
_live_status_refresh()

# ── Config expander ────────────────────────────────────────────────────────────

with _controls.expander("⚙️ Configure", expanded=False):
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

if st.session_state.last_run_ok is False and not st.session_state.is_running:
    with st.expander("📝 Log", expanded=True):
        st.code(st.session_state.last_run_log or "(no log)", language=None)

calls_raw = pd.DataFrame()
puts_raw = pd.DataFrame()
if st.session_state.last_run_ok:
    calls_raw = _load_calls_view(
        st.session_state.last_csv_path,
        st.session_state.last_monthly_calls_path,
        st.session_state.last_cc_recs_path,
    )
    puts_raw = _load_puts_view(
        st.session_state.last_csv_path,
        st.session_state.last_csp_recs_path,
    )

tab_calls, tab_puts, tab_perf, tab_day = st.tabs(
    ["📈 Calls", "📉 Puts", "📊 Performance", "⚡ DayTrading"]
)

# Ties each tab's filter widgets to the report actually being shown (profile +
# run) so switching profiles or completing a new run starts filters fresh
# instead of carrying over a stale ticker/expiration selection — see _show_tab.
_data_version = f"{profile}_{st.session_state.last_run_timestamp or ''}"

with tab_calls:
    if st.session_state.last_run_ok:
        calls_display = _build_calls_display(calls_raw) if not calls_raw.empty else pd.DataFrame()
        _show_tab(calls_display, "calls", data_version=_data_version)
    else:
        st.info("Run the screener to see call candidates.")

with tab_puts:
    if st.session_state.last_run_ok:
        puts_display = _build_puts_display(puts_raw) if not puts_raw.empty else pd.DataFrame()
        _show_tab(puts_display, "puts", data_version=_data_version)
    else:
        st.info("Run the screener to see put candidates.")

with tab_perf:
    _show_performance(cfg)

with tab_day:
    # Imported lazily so a DayTrading-only problem can never stop the existing
    # Calls/Puts/Performance tabs from rendering.
    try:
        from agent.daytrading.views import render_daytrading_tab
        render_daytrading_tab(profile)
    except Exception as exc:  # noqa: BLE001
        st.error(f"DayTrading tab failed to load: {exc}")
        st.exception(exc)


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
    run_started = time.time()
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
        duration_s = round(time.time() - run_started, 1)
        st.session_state.last_run_timestamp = timestamp
        st.session_state.last_run_duration = duration_s
        save_run_meta({
            "ok": True,
            "csv_path": st.session_state.last_csv_path,
            "cc_recs_path": st.session_state.last_cc_recs_path,
            "csp_recs_path": st.session_state.last_csp_recs_path,
            "monthly_calls_path": st.session_state.last_monthly_calls_path,
            "timestamp": timestamp,
            "duration_s": duration_s,
        })

    st.rerun()
