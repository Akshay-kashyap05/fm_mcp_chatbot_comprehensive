"""Email sender using AWS SES (Simple Email Service).

Credentials set in .env:
    AWS_ACCESS_KEY_ID     - AWS access key
    AWS_SECRET_ACCESS_KEY - AWS secret key
    AWS_SES_REGION        - SES region (default: ap-south-1)
"""

import os
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

_EMAIL_SENDER  = "ati.alert@atimotors.com"
_EMAIL_DISPLAY = "AUTOMATED SUPPORT (no_reply) <ati.alert@atimotors.com>"
_AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
_AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
_AWS_SES_REGION = os.environ.get("AWS_SES_REGION", "ap-south-1")


def _ses_client():
    return boto3.client(
        "ses",
        region_name=_AWS_SES_REGION,
        aws_access_key_id=_AWS_ACCESS_KEY,
        aws_secret_access_key=_AWS_SECRET_KEY,
    )


def send_email(pdf_filename, recipients, Bcc, email_subject):
    """Send email with PDF attachment via AWS SES.

    Parameters
    ----------
    pdf_filename : str   Path to the PDF file to attach.
    recipients   : list  Primary To: recipients.
    Bcc          : list  BCC recipients (included in Destinations, hidden from To).
    email_subject: str   Subject line.
    """
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
        part.add_header(
            "Content-Disposition", "attachment",
            filename=os.path.basename(pdf_filename),
        )
        msg.attach(part)

    all_destinations = list(recipients) + list(Bcc or [])
    _ses_client().send_raw_email(
        Source=_EMAIL_SENDER,
        Destinations=all_destinations,
        RawMessage={"Data": msg.as_string()},
    )


def send_email1(recipients, Bcc, message):
    """Send plain-text email (no attachment) via AWS SES."""
    msg = MIMEMultipart()
    msg["From"]    = _EMAIL_DISPLAY
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = "Analytics Report - No Data"
    msg.attach(MIMEText(message or "No data for the report period.", "plain"))

    all_destinations = list(recipients) + list(Bcc or [])
    _ses_client().send_raw_email(
        Source=_EMAIL_SENDER,
        Destinations=all_destinations,
        RawMessage={"Data": msg.as_string()},
    )
