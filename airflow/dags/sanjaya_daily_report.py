"""
Airflow DAG: run Sanjaya analytics report daily and email to REPORT_RECIPIENT.

Uses src/report_builder.py (build_pdf, send_report_email) and the Sanjaya API client
directly. Same flow as the MCP server when it generates and emails a report.

Where prompts come from (priority order):
  1) File: project_root/scheduled_report_prompts.json — written by chat when the user
     asks for a report in chat_client (MCP sanjaya_chat). So prompts you give in chat
     are automatically used by Airflow on its next run (scheduled or manual).
  2) Airflow Variable sanjaya_report_prompts — JSON array of prompt strings (set in UI).
  3) Fallback: one report using .env (SANJAYA_DEFAULT_CLIENT, SANJAYA_DEFAULT_FLEET) and TIME_RANGE.

Each prompt is parsed with the same NLU as chat (parse_query) to get client_name, fleet_name,
time_phrase, timezone — then basic_analytics is called and a PDF is built and emailed.

Requires:
  - DAG file under <project>/airflow/dags/ so project root is inferred (no Variable needed).
  - .env in project root: SANJAYA_*, EMAIL_*, REPORT_RECIPIENT. For fallback mode also SANJAYA_DEFAULT_CLIENT/FLEET.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Phrases that are resolved relative to run date (dynamic). If the prompt's time_phrase
# does not contain any of these, we use TIME_RANGE so scheduled runs get fresh data.
RELATIVE_TIME_KEYWORDS = (
    "today", "yesterday", "day before yesterday",
    "last hour", "last week", "last month", "last quarter",
    "this week", "previous week", "this month", "previous month", "previous quarter",
    "last ",  # "last 3 days", "last 5 hours" etc.
)

# Project root: always inferred from DAG file path (parent of airflow/dags). We do not use
# Variable.get("sanjaya_report_project_root") so DAG parse never fails with "Variable not found".
_PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
_PROJECT_ROOT = os.path.normpath(os.path.abspath(_PROJECT_ROOT))

# Fallback when no prompts Variable: single report time range (e.g. "today", "yesterday")
TIME_RANGE = "yesterday"

# Daily at 08:00 (scheduler timezone). Change as needed, e.g. "0 9 * * *" for 09:00
SCHEDULE_CRON = "0 * * * *"


PROMPTS_FILENAME = "scheduled_report_prompts.json"


def _run_scheduled_report(project_root: str, **kwargs) -> None:
    """Generate one or more reports. Prompts are read from (1) file written by chat,
    (2) Airflow Variable sanjaya_report_prompts, (3) .env defaults. Each prompt is
    parsed with the same NLU as chat; then basic_analytics is called and PDF emailed.
    Uses the DAG run's logical_date as 'now' so 'today'/'yesterday' are dynamic.
    """
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    os.chdir(project_root)

    from dotenv import load_dotenv
    load_dotenv(os.path.join(project_root, ".env"))

    from src.time_parse import parse_time_range
    from src.sanjaya_client import SanjayaAPI
    from src.nlu import parse_query
    from src.report_builder import build_pdf, send_report_email

    base_url = os.environ.get("SANJAYA_BASE_URL", "https://sanjaya.atimotors.com")
    default_tz = os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata")
    default_client = (os.environ.get("SANJAYA_DEFAULT_CLIENT") or "ceat-nagpur").strip()
    default_fleet = (os.environ.get("SANJAYA_DEFAULT_FLEET") or "CEAT-Nagpur-North-Plant").strip()
    # Airflow-specific recipient — separate from the chat/MCP report recipient
    airflow_recipient_raw = os.environ.get("AIRFLOW_REPORT_RECIPIENT", "").strip()
    airflow_recipients = [r.strip() for r in airflow_recipient_raw.split(",") if r.strip()] if airflow_recipient_raw else None

    # Use Airflow run date as reference so "today"/"yesterday" are relative to the run
    logical_date = kwargs.get("logical_date") or kwargs.get("execution_date")
    if logical_date:
        tz = ZoneInfo(default_tz)
        if logical_date.tzinfo is None:
            logical_date = logical_date.replace(tzinfo=ZoneInfo("UTC"))
        reference_now = logical_date.astimezone(tz)
    else:
        reference_now = None

    # 1) File written by chat when user asks for a report (same prompt -> same data)
    prompts_file = os.path.join(project_root, PROMPTS_FILENAME)
    prompts: list = []
    if os.path.isfile(prompts_file):
        try:
            with open(prompts_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            prompts = data if isinstance(data, list) else data.get("prompts", [])
        except (TypeError, json.JSONDecodeError, OSError):
            pass
    if not isinstance(prompts, list):
        prompts = []
    # 2) If no prompts from file, use Airflow Variable
    if not prompts:
        try:
            prompts_json = Variable.get("sanjaya_report_prompts")
            prompts = json.loads(prompts_json) if isinstance(prompts_json, str) else prompts_json
        except Exception:
            prompts = []
        if not isinstance(prompts, list):
            prompts = []

    def _is_relative_time_phrase(phrase: str) -> bool:
        """True if phrase is relative (today, yesterday, etc.) so we keep it; else use TIME_RANGE."""
        if not (phrase and phrase.strip()):
            return False
        lower = phrase.strip().lower()
        return any(kw in lower for kw in RELATIVE_TIME_KEYWORDS)

    async def _fetch_and_send_one(
        client_name: str,
        fleet_name: str,
        time_phrase: str,
        timezone: str,
        recipients: list | None = None,
    ) -> None:
        api = SanjayaAPI(base_url, debug_http=False)
        await api.ensure_token()
        # Same as MCP: resolve sherpa names for fleet so API returns trip data (not empty)
        sherpa_names = await api.get_sherpa_names_for_fleet(client_name, fleet_name)
        api_sherpa = sherpa_names if sherpa_names else None
        tr = parse_time_range(time_phrase, time_zone=timezone, now=reference_now)
        time_strings = tr.to_strings()
        data = await api.basic_analytics(
            fm_client_name=client_name,
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=timezone,
            fleet_name=fleet_name,
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=api_sherpa,
        )
        if isinstance(data.get("data"), dict):
            data = data["data"]
        # Log when API returns empty so you can debug client/fleet/date
        st = data.get("sherpa_wise_trips") or []
        av = data.get("availability") or []
        util = data.get("utilization") or []
        has_data = (
            len(st) > 0
            or data.get("total_trips") is not None
            or data.get("total_distance_km") is not None
            or len(av) > 0
            or len(util) > 0
        )
        if not has_data:
            logger.warning(
                "basic_analytics returned no data for client=%s fleet=%s range=%s to %s",
                client_name, fleet_name, time_strings.get("start_time"), time_strings.get("end_time"),
            )
        safe_name = (client_name + "_" + fleet_name).replace(" ", "-")[:50]
        pdf_filename = f"Analytics-Report-{safe_name}-{datetime.now().strftime('%Y-%m-%d')}.pdf"
        pdf_path = os.path.join(project_root, pdf_filename)
        build_pdf(
            data, client_name, fleet_name, time_phrase, time_strings,
            pdf_path, report_dir=project_root,
        )
        subject = f"Analytics Report - {fleet_name} - {time_phrase}"
        send_report_email(pdf_path, subject, report_dir=project_root, recipients=recipients)

    async def _run_all() -> None:
        if prompts:
            defaults = {
                "fm_client_name": default_client,
                "fleet_name": default_fleet,
                "timezone": default_tz,
                "time_phrase": TIME_RANGE,
            }
            for prompt_text in prompts:
                if not (prompt_text and isinstance(prompt_text, str) and prompt_text.strip()):
                    continue
                parsed = await parse_query(prompt_text.strip(), defaults=defaults)
                client_name = (parsed.fm_client_name or default_client).strip()
                fleet_name = (parsed.fleet_name or default_fleet).strip()
                time_phrase = (parsed.time_phrase or TIME_RANGE).strip()
                # For scheduled runs: use dynamic date. If prompt has a fixed date (no relative
                # keyword), override to TIME_RANGE so each run gets fresh data.
                if not _is_relative_time_phrase(time_phrase):
                    time_phrase = TIME_RANGE
                timezone = parsed.timezone or default_tz
                if not client_name or not fleet_name:
                    continue
                await _fetch_and_send_one(client_name, fleet_name, time_phrase, timezone, recipients=airflow_recipients)
        else:
            await _fetch_and_send_one(
                default_client,
                default_fleet,
                TIME_RANGE,
                default_tz,
                recipients=airflow_recipients,
            )

    asyncio.run(_run_all())


with DAG(
    dag_id="sanjaya_daily_report",
    description="Generate Sanjaya analytics PDF(s) from prompts or env defaults; email to REPORT_RECIPIENT daily",
    schedule=SCHEDULE_CRON,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sanjaya", "report", "email"],
) as dag:
    run_report = PythonOperator(
        task_id="generate_and_email_report",
        python_callable=_run_scheduled_report,
        op_kwargs={"project_root": _PROJECT_ROOT},
        retries=2,
        retry_delay=__import__("datetime").timedelta(minutes=5),
    )
