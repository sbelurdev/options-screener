import smtplib

import pytest

from agent.notify.email_report import send_report_email


class _FakeSMTP:
    sent = []  # class-level, so tests can inspect after the `with` block exits

    def __init__(self, host, port, timeout=30):
        self.host = host
        self.port = port

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        pass

    def login(self, user, password):
        self.user = user
        self.password = password

    def send_message(self, msg):
        _FakeSMTP.sent.append(msg)


def _base_config(**overrides):
    cfg = {
        "active_profile": "prasanna",
        "notify_email": "prasanna.kudli@outlook.com",
        "email": {
            "enabled": True,
            "smtp_host": "smtp-mail.outlook.com",
            "smtp_port": 587,
            "from_address": "optionsscreener@outlook.com",
            "smtp_user_env_var": "SMTP_USER",
            "smtp_password_env_var": "SMTP_PASSWORD",
        },
    }
    cfg.update(overrides)
    return cfg


@pytest.fixture(autouse=True)
def _reset_fake_smtp():
    _FakeSMTP.sent = []
    yield
    _FakeSMTP.sent = []


@pytest.fixture
def smtp_env(monkeypatch):
    monkeypatch.setenv("SMTP_USER", "optionsscreener@outlook.com")
    monkeypatch.setenv("SMTP_PASSWORD", "app-password")


def test_skips_when_notify_email_missing(logger, tmp_path, caplog):
    report = tmp_path / "report.html"
    report.write_text("<html></html>", encoding="utf-8")
    cfg = _base_config(notify_email=None)

    with caplog.at_level("WARNING"):
        sent = send_report_email(cfg, [str(report)], logger)

    assert sent is False
    assert "missing 'notify_email' in prasanna.yaml" in caplog.text


def test_skips_when_credentials_missing(logger, tmp_path, monkeypatch):
    monkeypatch.delenv("SMTP_USER", raising=False)
    monkeypatch.delenv("SMTP_PASSWORD", raising=False)
    report = tmp_path / "report.html"
    report.write_text("<html></html>", encoding="utf-8")

    sent = send_report_email(_base_config(), [str(report)], logger)
    assert sent is False


def test_skips_when_disabled(logger, tmp_path, smtp_env):
    report = tmp_path / "report.html"
    report.write_text("<html></html>", encoding="utf-8")
    cfg = _base_config(email={**_base_config()["email"], "enabled": False})

    sent = send_report_email(cfg, [str(report)], logger)
    assert sent is False


def test_skips_when_no_files_exist(logger, tmp_path, smtp_env):
    sent = send_report_email(_base_config(), [str(tmp_path / "missing.html")], logger)
    assert sent is False


def test_sends_with_attachments(logger, tmp_path, smtp_env, monkeypatch):
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    report = tmp_path / "MSFT-CALL.html"
    report.write_text("<html><body>hi</body></html>", encoding="utf-8")

    sent = send_report_email(_base_config(), [str(report)], logger)

    assert sent is True
    assert len(_FakeSMTP.sent) == 1
    msg = _FakeSMTP.sent[0]
    assert msg["From"] == "optionsscreener@outlook.com"
    assert msg["To"] == "prasanna.kudli@outlook.com"
    attachments = list(msg.iter_attachments())
    assert len(attachments) == 1
    assert attachments[0].get_filename() == "MSFT-CALL.html"


def test_skips_missing_files_but_sends_existing_ones(logger, tmp_path, smtp_env, monkeypatch):
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    report = tmp_path / "MSFT-CALL.html"
    report.write_text("<html></html>", encoding="utf-8")

    sent = send_report_email(
        _base_config(), [str(report), str(tmp_path / "does-not-exist.html")], logger
    )

    assert sent is True
    assert len(list(_FakeSMTP.sent[0].iter_attachments())) == 1
