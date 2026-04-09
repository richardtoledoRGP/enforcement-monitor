import logging
import os
import smtplib
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from models import EnforcementAction, ScrapeResult

logger = logging.getLogger(__name__)

CATEGORY_LABELS = {
    "federal_banking": "Federal Banking Regulators",
    "federal_other": "Other Federal Regulators",
    "state_banking": "State Banking / Financial Regulators",
    "state_insurance": "State Insurance Regulators",
}


def build_alert_email(
    new_actions: list[EnforcementAction],
    results: list[ScrapeResult],
    source_categories: dict[str, str],
) -> str:
    actions_by_category = defaultdict(list)
    for action in new_actions:
        cat = source_categories.get(action.source, "other")
        actions_by_category[cat].append(action)

    succeeded = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    html = """
    <html><body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto;">
    <h1 style="color: #1a5276;">Enforcement Action Alert</h1>
    <p style="color: #555;">Daily scan detected <strong>{count}</strong> new enforcement action(s).</p>
    """.format(count=len(new_actions))

    if new_actions:
        for cat_key in ["federal_banking", "federal_other", "state_banking", "state_insurance"]:
            cat_actions = actions_by_category.get(cat_key, [])
            if not cat_actions:
                continue
            label = CATEGORY_LABELS.get(cat_key, cat_key)
            html += f'<h2 style="color: #2e86c1; border-bottom: 1px solid #ddd; padding-bottom: 5px;">{label}</h2>'
            for a in cat_actions:
                penalty = f" | Penalty: ${a.penalty_amount:,.0f}" if a.penalty_amount else ""
                date = f" | {a.date}" if a.date else ""
                html += f"""
                <div style="margin-bottom: 12px; padding: 10px; background: #f8f9fa; border-left: 3px solid #2e86c1;">
                    <strong>{a.source}</strong>{date}{penalty}<br>
                    <a href="{a.url}" style="color: #2e86c1;">{a.title}</a>
                </div>
                """
    else:
        html += '<p style="color: #888;">No new enforcement actions detected today.</p>'

    # Health report
    html += '<hr style="margin-top: 30px;">'
    html += f'<h3 style="color: #555;">Source Health Report</h3>'
    html += f'<p>Sources checked: {len(results)} | Succeeded: {len(succeeded)} | Failed: {len(failed)}</p>'

    if failed:
        html += '<details><summary style="cursor: pointer; color: #c0392b;">Failed sources ({count})</summary><ul>'.format(
            count=len(failed)
        )
        for r in failed:
            html += f"<li><strong>{r.source_name}</strong>: {r.error}</li>"
        html += "</ul></details>"

    html += "</body></html>"
    return html


def send_email(subject: str, html_body: str):
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    email_to = os.environ.get("ALERT_EMAIL_TO", "")
    email_from = os.environ.get("ALERT_EMAIL_FROM", smtp_user)

    if not all([smtp_user, smtp_pass, email_to]):
        logger.error("SMTP credentials not configured — cannot send email")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(email_from, email_to.split(","), msg.as_string())
        logger.info(f"Alert email sent to {email_to}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def send_alert(
    new_actions: list[EnforcementAction],
    results: list[ScrapeResult],
    source_categories: dict[str, str],
):
    count = len(new_actions)
    subject = f"[Enforcement Monitor] {count} new action(s) detected" if count else "[Enforcement Monitor] Daily scan — no new actions"
    html = build_alert_email(new_actions, results, source_categories)
    return send_email(subject, html)
