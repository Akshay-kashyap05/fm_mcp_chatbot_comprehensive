"""
Email sender using AWS SES.
Sender: ati.alert@atimotors.com (AUTOMATED SUPPORT no_reply)
AWS credentials read from env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SES_REGION
"""

import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

import boto3

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

_EMAIL_SENDER      = "ati.alert@atimotors.com"
_EMAIL_DISPLAY     = "AUTOMATED SUPPORT (no_reply) <ati.alert@atimotors.com>"
_AWS_ACCESS_KEY    = os.environ.get("AWS_ACCESS_KEY_ID")
_AWS_SECRET_KEY    = os.environ.get("AWS_SECRET_ACCESS_KEY")
_AWS_SES_REGION    = os.environ.get("AWS_SES_REGION", "ap-south-1")


def _ses_client():
    return boto3.client(
        "ses",
        region_name=_AWS_SES_REGION,
        aws_access_key_id=_AWS_ACCESS_KEY,
        aws_secret_access_key=_AWS_SECRET_KEY,
    )


def send_email(pdf_filename, recipients, Bcc, email_subject):
    """Send email with PDF attachment via SES. recipients and Bcc are lists of email addresses."""
    msg = MIMEMultipart()
    msg["From"]    = _EMAIL_DISPLAY
    msg["To"]      = ", ".join(recipients)
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
    _ses_client().send_raw_email(
        Source=_EMAIL_SENDER,
        Destinations=to_addrs,
        RawMessage={"Data": msg.as_string()},
    )

    try:
        os.remove(pdf_filename)
    except OSError:
        pass


def send_email1(recipients, Bcc, message):
    """Send plain text email (no attachment) via SES. Used when report data is empty."""
    msg = MIMEMultipart()
    msg["From"]    = _EMAIL_DISPLAY
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = "CEAT-Nagpur Report - No Data"
    msg.attach(MIMEText(message or "No data for the report period.", "plain"))

    to_addrs = list(recipients) + list(Bcc or [])
    _ses_client().send_raw_email(
        Source=_EMAIL_SENDER,
        Destinations=to_addrs,
        RawMessage={"Data": msg.as_string()},
    )
