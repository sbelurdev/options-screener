"""
Email delivery for the HTML reports a run produces.

Sending is entirely opt-in per profile: it only fires when the profile's
config YAML sets `notify_email`. A profile with no `notify_email` is treated
as "this user doesn't want emails" — the run proceeds normally and a warning
is logged so it's visible in the log without failing the run.

The sending account itself (SMTP host/credentials) is shared config under
`email:` (see DEFAULT_CONFIG in agent/pipeline.py / config/base.yaml) — every
profile sends "from" the same mailbox; only the "to" address is per-profile.
"""

from __future__ import annotations

import os
import smtplib
from datetime import date
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List


def resolve_smtp(config: Dict[str, Any], logger, recipients: Optional[List[str]] = None):
    """Validate mail settings and return the SMTP tuple, or None if unusable.

    Shared by the end-of-run report email and the DayTrading alerts so both
    honour the same opt-in rule: no recipient means no mail, logged as a
    warning rather than an error.

    `recipients` overrides the profile's `notify_email` — the DayTrading tab
    uses it to send to its own list without changing where CC/CSP reports go.

    Returns (recipient_header, from_address, host, port, user, password) or None.
    """
    profile = str(config.get("active_profile") or "").strip() or "(unknown profile)"

    if recipients:
        clean = [str(r).strip() for r in recipients if str(r).strip()]
        recipient = ", ".join(clean)
    else:
        recipient = str(config.get("notify_email") or "").strip()
    if not recipient:
        logger.warning(
            "Email notification skipped: missing 'notify_email' in %s.yaml", profile
        )
        return None

    email_cfg: Dict[str, Any] = config.get("email") or {}
    if not email_cfg.get("enabled", True):
        logger.info("Email notification skipped: email.enabled is false")
        return None

    smtp_host = str(email_cfg.get("smtp_host") or "").strip()
    from_address = str(email_cfg.get("from_address") or "").strip()
    if not smtp_host or not from_address:
        logger.warning(
            "Email notification skipped: email.smtp_host / email.from_address not configured"
        )
        return None
    smtp_port = int(email_cfg.get("smtp_port", 587))

    user_env = str(email_cfg.get("smtp_user_env_var") or "SMTP_USER")
    pass_env = str(email_cfg.get("smtp_password_env_var") or "SMTP_PASSWORD")
    smtp_user = os.environ.get(user_env)
    smtp_password = os.environ.get(pass_env)
    if not smtp_user or not smtp_password:
        logger.warning(
            "Email notification skipped: %s / %s environment variables not set",
            user_env, pass_env,
        )
        return None

    return (recipient, from_address, smtp_host, smtp_port, smtp_user, smtp_password)


def send_html_email(config: Dict[str, Any], subject: str, html_body: str,
                    text_body: str, logger,
                    recipients: Optional[List[str]] = None) -> bool:
    """Send one HTML email using the shared mail settings. Never raises.

    `recipients` overrides the profile's `notify_email` for this message only.
    """
    smtp = resolve_smtp(config, logger, recipients)
    if smtp is None:
        return False
    recipient, from_address, smtp_host, smtp_port, smtp_user, smtp_password = smtp

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = recipient
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Email send failed: %s", exc)
        return False

    logger.info("Emailed %r to %s", subject, recipient)
    return True


def send_report_email(config: Dict[str, Any], report_paths: List[str], logger) -> bool:
    """Email the given HTML report files to config['notify_email'], if configured.

    Returns True if an email was sent, False if it was skipped or failed
    (both cases are logged; neither raises, so a notification problem never
    breaks the screening run itself).
    """
    profile = str(config.get("active_profile") or "").strip() or "(unknown profile)"

    smtp = resolve_smtp(config, logger)
    if smtp is None:
        return False
    recipient, from_address, smtp_host, smtp_port, smtp_user, smtp_password = smtp

    attachments = [p for p in (Path(str(rp)) for rp in report_paths) if p.exists()]
    if not attachments:
        logger.warning("Email notification skipped: no report files found to attach")
        return False

    run_day = date.today().isoformat()
    msg = EmailMessage()
    msg["Subject"] = f"Options Screener Report - {run_day} ({profile})"
    msg["From"] = from_address
    msg["To"] = recipient
    msg.set_content(
        f"Attached: {len(attachments)} report file(s) from today's options screener run ({profile}).\n\n"
        "Educational screening only - not financial advice."
    )
    for path in attachments:
        msg.add_attachment(
            path.read_bytes(), maintype="text", subtype="html", filename=path.name
        )

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
    except Exception as exc:
        logger.warning("Email notification failed: %s", exc)
        return False

    logger.info("Emailed %d report file(s) to %s", len(attachments), recipient)
    return True
