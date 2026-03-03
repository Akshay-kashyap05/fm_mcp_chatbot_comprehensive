"""
Email sender for CEAT-Nagpur report. Uses Gmail SMTP.
Set in .env: EMAIL_SENDER (e.g. akshaykashyap0545@gmail.com), EMAIL_APP_PASSWORD (Gmail App Password).
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

EMAIL_SENDER = os.environ.get("EMAIL_SENDER", "akshaykashyap0545@gmail.com")
EMAIL_APP_PASSWORD = os.environ.get("EMAIL_APP_PASSWORD", "")
SMTP_HOST = os.environ.get("EMAIL_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("EMAIL_SMTP_PORT", "587"))


def _get_connection():
    if not EMAIL_APP_PASSWORD:
        raise RuntimeError(
            "EMAIL_APP_PASSWORD not set. Add it to .env (Gmail App Password from Google Account → Security → App passwords)."
        )
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(EMAIL_SENDER, EMAIL_APP_PASSWORD)
    return server


def send_email(pdf_filename, recipients, Bcc, email_subject):
    """Send email with PDF attachment. recipients and Bcc are lists of email addresses."""
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = email_subject
    msg.attach(MIMEText("Please find the report attached.", "plain"))

    if os.path.isfile(pdf_filename):
        with open(pdf_filename, "rb") as f:
            part = MIMEBase("application", "pdf")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=os.path.basename(pdf_filename))
        msg.attach(part)

    to_addrs = list(recipients) + list(Bcc or [])
    server = _get_connection()
    try:
        server.sendmail(EMAIL_SENDER, to_addrs, msg.as_string())
    finally:
        server.quit()


def send_email1(recipients, Bcc, message):
    """Send plain text email (no attachment). Used when report data is empty."""
    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = "CEAT-Nagpur Report - No Data"
    msg.attach(MIMEText(message or "No data for the report period.", "plain"))

    to_addrs = list(recipients) + list(Bcc or [])
    server = _get_connection()
    try:
        server.sendmail(EMAIL_SENDER, to_addrs, msg.as_string())
    finally:
        server.quit()
