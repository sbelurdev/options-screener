"""Signal-alert tests: dedup, simulation suppression, content (network-free)."""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from agent.daytrading import config as dc
from agent.daytrading import notify as nt
from agent.daytrading import signals as sig
from agent.daytrading.indicators import DailyIndicators, OpeningRange, OvernightLevels

ET = ZoneInfo("America/New_York")
DAY = date(2026, 8, 28)


class _Ev:
    """Minimal stand-in for TickerEvaluation."""

    def __init__(self, fired=True):
        self.ticker = "AAPL"
        cond = sig.Condition("close_above_or_high", True, 101.0, "> or_high 100.00",
                             threshold=100.0)
        fire = sig.TriggerFire(
            ticker="AAPL", bar_time=datetime(2026, 8, 28, 10, 0, tzinfo=ET),
            bar_close=101.0, bar_volume=5000.0, or_high=100.0, overnight_high=100.5,
            vwap_at_bar=99.0, or_avg_volume=1000.0, relative_volume=2.0,
            conditions=[cond],
        )
        self.trigger = sig.TriggerResult(fired, fire if fired else None) if fired else \
            sig.TriggerResult(False)
        self.opening_range = OpeningRange(100.0, 98.0, 2.0, 1000.0, 3)
        self.vwap = None
        self._email_size = None
        self._email_exits = None

    def rationale(self):
        return [("Daily gate (trend intact)", "RSI 60.0; close 3.0% above SMA20")]


def _cfg():
    return dc.DayTradingConfig(watchlist=["AAPL"])


def _app(**over):
    a = {"active_profile": "p", "notify_email": "me@example.com",
         "email": {"enabled": True, "smtp_host": "smtp.example.com", "smtp_port": 587,
                   "from_address": "bot@example.com",
                   "smtp_user_env_var": "SMTP_USER",
                   "smtp_password_env_var": "SMTP_PASSWORD"}}
    a.update(over)
    return a


@pytest.fixture
def sent(monkeypatch):
    box = []
    monkeypatch.setattr(
        nt, "send_html_email",
        lambda cfg, s, h, t, log, rec=None: (box.append((s, h, t, rec)), True)[1])
    return box


# ── criteria table cell formatting ──────────────────────────────────────────

def test_crit_cell_none_is_dash_not_fail():
    """A criterion that never ran must render as '-', never as FAIL."""
    assert nt._crit_cell(None) == "-"


def test_crit_cell_shows_both_sides_of_a_passing_comparison():
    c = sig.Condition("close_above_ema9", True, 356.61, "close > EMA9", threshold=352.24)
    assert nt._crit_cell(c) == "PASS 356.61 > 352.24"


def test_crit_cell_shows_both_sides_of_a_failing_comparison():
    """The user's motivating example: an overnight-high miss shows both values."""
    c = sig.Condition("close_above_overnight_high", False, 515.77,
                      "> overnight_high 516.66", threshold=516.66)
    assert nt._crit_cell(c) == "FAIL 515.77 < 516.66"


def test_crit_cell_uses_integer_formatting_for_volume():
    c = sig.Condition("volume_above_or_avg", False, 638351.0, "> or_avg_volume 1,006,111",
                      threshold=1006111.0)
    assert nt._crit_cell(c) == "FAIL 638,351 < 1,006,111"


def test_crit_cell_rsi_has_no_threshold_but_shows_the_value():
    c = sig.Condition("rsi_14", False, 47.2, "50 <= rsi <= 75")
    assert nt._crit_cell(c) == "FAIL 47.2"


def test_crit_cell_earnings_shows_detail_only_on_failure():
    passed = sig.Condition("no_earnings_in_window", True, None,
                           "no earnings on/before option expiry", detail="no earnings date known")
    failed = sig.Condition("no_earnings_in_window", False, None,
                           "no earnings on/before option expiry", detail="earnings 2026-08-31")
    assert nt._crit_cell(passed) == "PASS"
    assert nt._crit_cell(failed) == "FAIL (earnings 2026-08-31)"


# ── criteria table (per-ticker, per-criterion) ──────────────────────────────

def _crow(ticker="AAPL", monitored=True, reason="", **conds):
    """Build a CriteriaRow-shaped object without importing views (avoids the
    Streamlit import chain in a notify-focused test module)."""
    class _Row:
        pass
    r = _Row()
    r.ticker, r.monitored, r.reason = ticker, monitored, reason
    r.trend_ma_label, r.rsi_band = "EMA9", "50-75"
    for name in ("rsi", "trend_ma", "earnings", "gap", "or_high",
                 "overnight_high", "vwap", "volume"):
        setattr(r, name, conds.get(name))
    return r


def test_criteria_table_empty_is_empty_string():
    assert nt.criteria_table_html([]) == ""
    assert nt.criteria_table_text([]) == ""


def test_criteria_table_html_marks_failed_criteria_and_dashes_unreached_ones():
    rsi_fail = sig.Condition("rsi_14", False, 47.2, "50 <= rsi <= 75")
    row = _crow("MSFT", monitored=False, reason="RSI failed - cannot recover today",
               rsi=rsi_fail)
    html = nt.criteria_table_html([row])
    assert "MSFT" in html and "NO" in html
    assert "FAIL 47.2" in html
    # Trigger columns never ran for a name that failed Stage A - shown as '-'
    assert html.count(">-<") >= 4 or ">-</td>" in html


def test_criteria_table_html_headers_use_the_configured_labels():
    row = _crow()
    html = nt.criteria_table_html([row])
    assert "EMA9" in html
    assert "50-75" in html  # RSI band in the header


def test_criteria_table_html_header_count_matches_every_row_cell_count():
    """Regression: Trend MA was briefly both a dedicated dynamic column AND
    left in the generic criteria list, producing 12 headers but 11 cells per
    row (a silent one-column misalignment, not an exception)."""
    passed = sig.Condition("close_above_ema9", True, 356.61, "close > EMA9", threshold=352.24)
    row = _crow("TSLA", trend_ma=passed)
    html = nt.criteria_table_html([row])
    n_headers = html.count("<th ")
    n_row_cells = html.split("</tr>")[1].count("<td ")  # first (only) data row
    assert n_headers == n_row_cells, (
        f"{n_headers} headers but {n_row_cells} cells in a data row")
    assert "Trend MA</th>" not in html, "the generic label must not also appear"


def test_criteria_table_text_is_one_block_per_ticker():
    passed = sig.Condition("close_above_ema9", True, 356.61, "close > EMA9", threshold=352.24)
    row = _crow("TSLA", reason="ready - watching for a trigger", trend_ma=passed)
    text = nt.criteria_table_text([row])
    assert "TSLA [MONITORED]" in text
    assert "EMA9: PASS 356.61 > 352.24" in text


# ── trade suggestions (top of every email) ──────────────────────────────────

class _Sel:
    def __init__(self, found=False, contract=None, selection_reason=""):
        self.found, self.contract, self.selection_reason = found, contract, selection_reason

    def explain(self):
        return "no contract qualified"


class _Contract:
    strike, dte, delta = 355.0, 4, 0.65
    expiration = date(2026, 9, 2)
    spread_pct_of_mid = 0.018
    bid, ask, mid, theta, implied_volatility = 5.10, 5.20, 5.15, -0.08, 0.35
    open_interest, option_volume, underlying_price = 1200, 88, 356.61
    delta_source = "provider"


def test_trade_suggestions_empty_says_so_plainly():
    html = nt.trade_suggestions_html([])
    text = nt.trade_suggestions_text([])
    assert "No trade signals yet today" in html
    assert "none yet today" in text


def test_trade_suggestions_lists_a_fired_signal_with_contract():
    ev = _Ev()
    sel = _Sel(found=True, contract=_Contract())
    html = nt.trade_suggestions_html([(ev, sel)])
    text = nt.trade_suggestions_text([(ev, sel)])
    assert "AAPL" in html and "355C" in html and "4 DTE" in html
    assert "10:00 ET" in html
    assert "AAPL" in text and "355C" in text


def test_trade_suggestions_handles_no_qualifying_contract():
    ev = _Ev()
    sel = _Sel(found=False)
    html = nt.trade_suggestions_html([(ev, sel)])
    assert "no qualifying contract" in html


def test_trade_suggestions_includes_exits_when_attached():
    ev = _Ev()
    ev._email_exits = type("Exits", (), {
        "stop_level": 352.24, "stop_basis": "vwap", "target_1": 358.85,
        "time_stop": datetime(2026, 8, 28, 10, 45, tzinfo=ET),
    })()
    html = nt.trade_suggestions_html([(ev, _Sel(found=True, contract=_Contract()))])
    assert "352.24" in html and "358.85" in html and "10:45" in html


# ── signal email (full detail) ──────────────────────────────────────────────

def test_sends_once_then_dedups(sent, logger):
    keys = set()
    ev, cfg = _Ev(), _cfg()
    assert nt.maybe_send_signal_email(_app(), ev, cfg, None, [], [], DAY, "p", keys, logger)
    assert len(sent) == 1
    # Re-render of the same signal must not send again
    assert not nt.maybe_send_signal_email(_app(), ev, cfg, None, [], [], DAY, "p", keys, logger)
    assert len(sent) == 1
    assert keys == {"2026-08-28:AAPL:1000"}


def test_simulated_session_never_sends(sent, logger):
    assert not nt.maybe_send_signal_email(_app(), _Ev(), _cfg(), None, [], [], DAY, "p",
                                          set(), logger, simulated=True)
    assert sent == []


def test_signal_email_not_sent_on_a_weekend(sent, logger):
    saturday = date(2026, 8, 29)
    assert not nt.maybe_send_signal_email(_app(), _Ev(), _cfg(), None, [], [], saturday, "p",
                                          set(), logger)
    assert sent == []


def test_unfired_signal_never_sends(sent, logger):
    assert not nt.maybe_send_signal_email(_app(), _Ev(fired=False), _cfg(), None, [], [],
                                          DAY, "p", set(), logger)
    assert sent == []


def test_send_failure_is_still_marked_to_avoid_retry_storm(monkeypatch, logger):
    monkeypatch.setattr(nt, "send_html_email", lambda *a, **k: False)
    keys = set()
    nt.maybe_send_signal_email(_app(), _Ev(), _cfg(), None, [], [], DAY, "p", keys, logger)
    assert keys, "a failed send must still be recorded so every rerun does not retry"


def test_exception_while_building_does_not_propagate(monkeypatch, logger):
    monkeypatch.setattr(nt, "build_signal_email",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    assert not nt.maybe_send_signal_email(_app(), _Ev(), _cfg(), None, [], [], DAY, "p",
                                          set(), logger)


def test_email_body_carries_the_no_order_disclaimer(sent, logger):
    nt.maybe_send_signal_email(_app(), _Ev(), _cfg(), None, [], [], DAY, "p", set(), logger)
    subject, html, text, _rec = sent[0]
    assert "AAPL" in subject
    assert "No order has been placed" in html
    assert "RECOMMENDATION ONLY" in text
    assert "Why this signal" in html
    assert "Option Trade Suggestions" in html  # top section always present


def test_subject_names_the_contract_when_one_was_selected(sent, logger):
    class _C:
        strike, dte, delta = 105.0, 4, 0.65
        expiration = date(2026, 9, 2)
        bid, ask, mid = 1.0, 1.02, 1.01
        spread_pct_of_mid, theta, implied_volatility = 0.02, -0.05, 0.30
        open_interest, option_volume, underlying_price = 1000, 42, 100.0
        delta_source = "provider"

    class _SelFull:
        found, contract, selection_reason = True, _C(), "delta 0.650 closest to target"

    nt.maybe_send_signal_email(_app(), _Ev(), _cfg(), _SelFull(), [], [], DAY, "p", set(),
                               logger)
    subject, html, _, _rec = sent[0]
    assert "105C" in subject and "4DTE" in subject
    assert "Why this strike" in html
    assert "delta 0.650" in html


def test_signal_email_includes_the_criteria_table(sent, logger):
    row = _crow("MSFT", monitored=False, reason="RSI failed - cannot recover today",
               rsi=sig.Condition("rsi_14", False, 47.2, "50 <= rsi <= 75"))
    nt.maybe_send_signal_email(_app(), _Ev(), _cfg(), None, [row], [], DAY, "p", set(), logger)
    _, html, text, _rec = sent[0]
    assert "Monitoring status" in html and "MSFT" in html
    assert "MSFT" in text


def test_signal_email_top_section_lists_all_of_todays_fired_signals():
    """Not just the ticker this particular email is about."""
    ev1, ev2 = _Ev(), _Ev()
    ev2.ticker = "TSLA"
    fired_today = [(ev1, _Sel(found=True, contract=_Contract())),
                  (ev2, _Sel(found=False))]
    subject, html, text = nt.build_signal_email(ev1, _cfg(), _Sel(found=True, contract=_Contract()),
                                                [], fired_today, DAY, "p")
    assert "AAPL" in html and "TSLA" in html  # both, not just ev1


# ── warm-up email ────────────────────────────────────────────────────────────

class _E:
    """Configurable TickerEvaluation stand-in for evaluate/build tests."""

    def __init__(self, **kw):
        self.excluded = kw.get("excluded", False)
        self.exclusion_reason = kw.get("exclusion_reason", "")
        self.degraded = kw.get("degraded", False)
        self.session_pending = kw.get("session_pending", False)
        self.pending_reason = kw.get("pending_reason", "")
        self.daily_gate = kw.get("daily_gate")
        self.setup = kw.get("setup")
        self.daily = kw.get("daily")

    @property
    def gate_ok(self):
        return bool(self.daily_gate and self.daily_gate.passed)

    def blocking_reason(self):
        return "some reason"


def _gate(passed):
    c = sig.Condition("rsi_14", passed, 47.2, "50 <= rsi <= 75")
    return sig.GateResult(passed, [c])


def _setup(passed):
    c = sig.Condition("gap_up", passed, 121.0, "today_open > prev_close", threshold=121.5)
    return sig.SetupResult(sig.GateResult(passed, [c]))


def test_warmup_email_sends_once_per_day(sent, logger):
    state = {}
    evs = {"AAPL": _E(daily_gate=_gate(True))}
    assert nt.maybe_send_warmup_email(_app(), evs, [], [], DAY, "p", state, logger)
    assert not nt.maybe_send_warmup_email(_app(), evs, [], [], DAY, "p", state, logger)
    assert len(sent) == 1
    subject, html, text, _rec = sent[0]
    assert "warm-up" in subject.lower()
    assert "Option Trade Suggestions" in html


def test_warmup_email_not_sent_in_simulation(sent, logger):
    assert not nt.maybe_send_warmup_email(_app(), {}, [], [], DAY, "p", {}, logger,
                                          simulated=True)
    assert sent == []


def test_warmup_email_skips_a_second_session_that_already_sent_today(sent, logger, tmp_path):
    """A server restart (or a second browser tab) starts with an empty in-memory
    `state`, so the in-memory dedup alone would resend the warm-up. cache_dir
    adds a same-day check against the persisted record from the first send."""
    evs = {"AAPL": _E(daily_gate=_gate(True))}
    nt.record_email_sent(tmp_path, "p", "Warm-up", DAY)  # simulates the earlier session's send
    fresh_state = {}
    assert not nt.maybe_send_warmup_email(_app(), evs, [], [], DAY, "p", fresh_state, logger,
                                          cache_dir=tmp_path)
    assert sent == []


def test_warmup_dedup_survives_a_later_polling_stopped_send_same_session(sent, logger, tmp_path):
    """Regression: record_email_sent used to overwrite a single "last sent"
    slot, so Warm-up then Polling-stopped in the same session erased the
    Warm-up record - a second session's dedup check then saw no Warm-up
    record at all and resent it. Caught via a real duplicate send in testing."""
    nt.record_email_sent(tmp_path, "p", "Warm-up", DAY)
    nt.record_email_sent(tmp_path, "p", "Polling-stopped", DAY)
    assert nt.was_kind_sent_today(tmp_path, "p", "Warm-up", DAY)

    evs = {"AAPL": _E(daily_gate=_gate(True))}
    assert not nt.maybe_send_warmup_email(_app(), evs, [], [], DAY, "p", {}, logger,
                                          cache_dir=tmp_path)
    assert sent == []


def test_warmup_email_not_sent_on_a_weekend(sent, logger):
    """The function must guard the trading day itself, not just `simulated` -
    a caller with no UI Simulate toggle (a headless test script, a future
    scheduler) has nothing else protecting a non-trading day."""
    saturday = date(2026, 8, 29)
    evs = {"AAPL": _E(daily_gate=_gate(True))}
    assert not nt.maybe_send_warmup_email(_app(), evs, [], [], saturday, "p", {}, logger)
    assert sent == []


def test_warmup_email_includes_criteria_table(sent, logger):
    row = _crow("AAPL")
    nt.maybe_send_warmup_email(_app(), {"AAPL": _E(daily_gate=_gate(True))}, [row], [],
                               DAY, "p", {}, logger)
    _, html, text, _rec = sent[0]
    assert "Monitoring status" in html


# ── polling-stopped email ────────────────────────────────────────────────────

from agent.daytrading.views import PollRow  # noqa: E402


def _poll_rows():
    return [PollRow("AAPL", True, "ready", "ready"),
            PollRow("MSFT", False, "daily gate failed", "stopped")]


def test_polling_stopped_email_reports_each_ticker_once(sent, logger):
    state = {}
    rows = _poll_rows()
    assert nt.maybe_send_polling_stopped_email(_app(), rows, [], [], DAY, "p", state, logger)
    assert "MSFT" in sent[0][0]
    # Same set again -> nothing new to report
    assert not nt.maybe_send_polling_stopped_email(_app(), rows, [], [], DAY, "p", state, logger)
    assert len(sent) == 1

    rows2 = rows + [PollRow("NVDA", False, "setup gate failed", "stopped")]
    assert nt.maybe_send_polling_stopped_email(_app(), rows2, [], [], DAY, "p", state, logger)
    assert "NVDA" in sent[1][0] and "MSFT" not in sent[1][0]


def test_polling_stopped_email_ignores_fired_names(sent, logger):
    """A fired ticker is covered by the signal email, not reported as 'stopped'."""
    rows = [PollRow("TSLA", True, "signal fired 10:00 - frozen", "position")]
    assert not nt.maybe_send_polling_stopped_email(_app(), rows, [], [], DAY, "p", {}, logger)
    assert sent == []


def test_polling_stopped_email_not_sent_on_a_weekend(sent, logger):
    saturday = date(2026, 8, 29)
    rows = _poll_rows()
    assert not nt.maybe_send_polling_stopped_email(_app(), rows, [], [], saturday, "p", {},
                                                    logger)
    assert sent == []


def test_no_email_when_nothing_happened(sent, logger):
    """Post warm-up silence: all polling, nothing fired -> no mail."""
    state = {f"warmup:{DAY.isoformat()}": True}
    rows = [PollRow("AAPL", True, "ready", "ready")]
    assert not nt.maybe_send_warmup_email(_app(), {}, [], [], DAY, "p", state, logger)
    assert not nt.maybe_send_polling_stopped_email(_app(), rows, [], [], DAY, "p", state, logger)
    assert sent == []


# ── DayTrading-only recipient list ──────────────────────────────────────────

def test_alert_recipients_prefers_the_daytrading_list():
    c = dc.DayTradingConfig(notify_emails=["a@x.com", "b@y.com"])
    assert c.alert_recipients("profile@z.com") == ["a@x.com", "b@y.com"]


def test_alert_recipients_falls_back_to_profile_notify_email():
    c = dc.DayTradingConfig()
    assert c.alert_recipients("profile@z.com") == ["profile@z.com"]
    assert c.alert_recipients(None) == []


def test_alert_recipients_strips_blanks():
    c = dc.DayTradingConfig(notify_emails=["  a@x.com ", "", "   "])
    assert c.alert_recipients(None) == ["a@x.com"]


def test_from_dict_normalises_notify_emails():
    c = dc.from_dict({"notify_emails": [" a@x.com", "b@y.com ", ""]})
    assert c.notify_emails == ["a@x.com", "b@y.com"]


def test_recipients_override_reaches_the_smtp_layer(monkeypatch, logger):
    """The override must replace notify_email, not append to it."""
    from agent.notify import email_report as er
    got = {}
    monkeypatch.setenv("SMTP_USER", "u")
    monkeypatch.setenv("SMTP_PASSWORD", "p")

    class _S:
        def __init__(self, *a, **k): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def starttls(self): pass
        def login(self, u, p): pass
        def send_message(self, msg): got["to"] = msg["To"]

    monkeypatch.setattr(er.smtplib, "SMTP", _S)
    cfg = {"active_profile": "p", "notify_email": "only-cc@x.com",
           "email": {"enabled": True, "smtp_host": "h", "smtp_port": 587,
                     "from_address": "bot@x.com"}}
    assert er.send_html_email(cfg, "s", "<p>h</p>", "t", logger,
                              recipients=["dt1@x.com", "dt2@y.com"])
    assert got["to"] == "dt1@x.com, dt2@y.com"
    assert "only-cc@x.com" not in got["to"], "CC/CSP recipient must not leak in"


def test_daytrading_senders_pass_recipients_through(monkeypatch, logger):
    seen = {}
    monkeypatch.setattr(nt, "send_html_email",
                        lambda cfg, s, h, t, log, rec=None: (seen.update({"rec": rec}), True)[1])
    nt.maybe_send_warmup_email(_app(), {}, [], [], DAY, "p", {}, logger,
                               recipients=["x@a.com", "y@b.com"])
    assert seen["rec"] == ["x@a.com", "y@b.com"]


# ── persisted "last email sent" record ──────────────────────────────────────

def test_last_email_sent_is_none_before_any_record(tmp_path):
    assert nt.last_email_sent(tmp_path, "p") is None


def test_last_email_sent_round_trips_the_most_recent_record(tmp_path):
    nt.record_email_sent(tmp_path, "p", "Warm-up", DAY)
    nt.record_email_sent(tmp_path, "p", "Signal (AAPL)", DAY)  # recorded alongside, not over, Warm-up
    last = nt.last_email_sent(tmp_path, "p")
    assert last["kind"] == "Signal (AAPL)"
    assert last["day"] == DAY.isoformat()


def test_last_email_sent_is_scoped_per_profile(tmp_path):
    nt.record_email_sent(tmp_path, "prasanna", "Warm-up", DAY)
    assert nt.last_email_sent(tmp_path, "other_profile") is None
