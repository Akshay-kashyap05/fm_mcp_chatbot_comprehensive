"""
Airflow DAG: run Sanjaya analytics reports and email to recipients.

Two independent report sources run every DAG execution:

  1) client_report_config.json — static per-client config (client_name, fleet_name,
     sections, time_phrase, timezone, optional run_hour). Edit this file to add/remove
     clients or change what sections they receive. Each entry can have its own run_hour
     so different clients get reports at different times of day.

  2) scheduled_report_prompts.json — written by chat when the user asks for a report
     in chat_client (MCP sanjaya_chat). This is for ad-hoc one-off reports triggered
     from chat. The file always holds only the latest chat request.

Fallback (if neither file has entries): one report using .env defaults.

Requires:
  - DAG file under <project>/airflow/dags/ so project root is inferred.
  - .env in project root: SANJAYA_*, EMAIL_*, REPORT_RECIPIENT.
  - client_report_config.json in project root (array of client config objects).
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
    "last 20 minutes",
)

# Project root: always inferred from DAG file path (parent of airflow/dags). We do not use
# Variable.get("sanjaya_report_project_root") so DAG parse never fails with "Variable not found".
_PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
_PROJECT_ROOT = os.path.normpath(os.path.abspath(_PROJECT_ROOT))

# Fallback when no prompts Variable: single report time range (e.g. "today", "yesterday")
TIME_RANGE = "yesterday"

# Daily at 08:00 (scheduler timezone). Change as needed, e.g. "0 9 * * *" for 09:00
SCHEDULE_CRON = "*/10 * * * *"


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
    from src.report_builder import (
        build_pdf, send_report_email,
        SECTION_TRIPS, SECTION_AVAILABILITY, SECTION_UTILIZATION,
        SECTION_DISTANCE, SECTION_UPTIME, SECTION_ROUTE_ANALYTICS,
    )

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

    # Items that require route_analytics instead of basic_analytics
    _ROUTE_ANALYTICS_ITEMS = frozenset({
        "takt_time", "average_takt_time", "top_10_routes_takt", "top_routes_takt",
        "route_utilization", "avg_obstacle_per_sherpa", "avg_obstacle_time", "avg_obstacle_per_route",
    })

    # Map section name strings (from config file) to section constants
    _SECTION_NAME_MAP = {
        "trips": SECTION_TRIPS,
        "distance": SECTION_DISTANCE,
        "availability": SECTION_AVAILABILITY,
        "utilization": SECTION_UTILIZATION,
        "uptime": SECTION_UPTIME,
        "route_analytics": SECTION_ROUTE_ANALYTICS,
    }

    def _sections_for_item(item):
        """Map a parsed NLU item to the set of report sections to include (None = all)."""
        if not item:
            return None  # full report
        if item in ("total_trips", "sherpa_wise_trips"):
            return {SECTION_TRIPS}
        if item in ("total_distance_km", "sherpa_wise_distance"):
            return {SECTION_DISTANCE}
        if item == "availability":
            return {SECTION_AVAILABILITY}
        if item == "utilization":
            return {SECTION_UTILIZATION}
        if item in ("uptime", "uptime_percentage"):
            return {SECTION_UPTIME}
        if item in _ROUTE_ANALYTICS_ITEMS:
            return {SECTION_ROUTE_ANALYTICS}
        return None  # unknown item → full report

    def _sections_from_config(section_list):
        """Convert a list of section name strings from config to a set of section constants."""
        if not section_list:
            return None  # empty list → full report
        result = set()
        for name in section_list:
            constant = _SECTION_NAME_MAP.get(str(name).strip().lower())
            if constant:
                result.add(constant)
        return result if result else None

    async def _fetch_and_send_one(
        client_name: str,
        fleet_name: str,
        time_phrase: str,
        timezone: str,
        item: str | None = None,
        sections_override: set | None = None,
        recipients: list | None = None,
    ) -> None:
        # sections_override (from client config) takes priority over NLU item
        sections = sections_override if sections_override is not None else _sections_for_item(item)
        api = SanjayaAPI(base_url, debug_http=False)
        await api.ensure_token()
        # Same as MCP: resolve sherpa names for fleet so API returns trip data (not empty)
        sherpa_names = await api.get_sherpa_names_for_fleet(client_name, fleet_name)
        api_sherpa = sherpa_names if sherpa_names else None
        tr = parse_time_range(time_phrase, time_zone=timezone, now=reference_now)
        time_strings = tr.to_strings()
        api_kwargs = dict(
            fm_client_name=client_name,
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=timezone,
            fleet_name=fleet_name,
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=api_sherpa,
        )

        merged: dict = {}

        # Decide which APIs to call based on what sections are needed
        needs_route = (
            sections is None  # full report → always call both
            or SECTION_ROUTE_ANALYTICS in (sections or set())
        )
        needs_basic = (
            sections is None
            or bool((sections or set()) - {SECTION_ROUTE_ANALYTICS})
        )

        # Call basic_analytics unless only route analytics sections are needed
        if needs_basic and item not in _ROUTE_ANALYTICS_ITEMS:
            basic_raw = await api.basic_analytics(**api_kwargs)
            basic_inner = basic_raw.get("data") if isinstance(basic_raw.get("data"), dict) else basic_raw
            merged.update(basic_inner)

        # Call route_analytics for full reports or when route analytics sections needed
        if needs_route or item in _ROUTE_ANALYTICS_ITEMS:
            try:
                route_raw = await api.route_analytics(**api_kwargs)
                route_inner = route_raw.get("data") if isinstance(route_raw.get("data"), dict) else route_raw
                merged.update(route_inner)
            except Exception as exc:
                logger.warning("route_analytics failed for client=%s fleet=%s: %s", client_name, fleet_name, exc)

        # Log when API returns empty so you can debug client/fleet/date
        has_data = (
            len(merged.get("sherpa_wise_trips") or []) > 0
            or merged.get("total_trips") is not None
            or merged.get("total_distance_km") is not None
            or len(merged.get("availability") or []) > 0
            or len(merged.get("utilization") or []) > 0
            or len(merged.get("avg_takt_per_sherpa") or []) > 0
        )
        if not has_data:
            logger.warning(
                "analytics returned no data for client=%s fleet=%s range=%s to %s",
                client_name, fleet_name, time_strings.get("start_time"), time_strings.get("end_time"),
            )
        safe_name = (client_name + "_" + fleet_name).replace(" ", "-")[:50]
        pdf_filename = f"Analytics-Report-{safe_name}-{datetime.now().strftime('%Y-%m-%d')}.pdf"
        pdf_path = os.path.join(project_root, pdf_filename)
        build_pdf(
            merged, client_name, fleet_name, time_phrase, time_strings,
            pdf_path, report_dir=project_root, sections_to_include=sections,
        )
        subject = f"Analytics Report - {fleet_name} - {time_phrase}"
        send_report_email(pdf_path, subject, report_dir=project_root, recipients=recipients)

    # Current run hour (used for per-client schedule matching)
    current_run_hour = reference_now.hour if reference_now else datetime.now().hour

    # Manual triggers from the Airflow UI should always run every configured entry,
    # ignoring the run_hour / run_day gates (those exist only to throttle scheduled runs).
    dag_run = kwargs.get("dag_run")
    is_manual_trigger = dag_run is not None and getattr(dag_run, "run_type", None) == "manual"
    if is_manual_trigger:
        logger.info("Manual trigger detected — bypassing all schedule gates (run_hour / run_day)")

    # Load client_report_config.json — defines which clients get scheduled reports
    client_configs: list = []
    config_file = os.path.join(project_root, "client_report_config.json")
    if os.path.isfile(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                client_configs = json.load(f)
            if not isinstance(client_configs, list):
                client_configs = []
        except Exception as exc:
            logger.warning("Could not load client_report_config.json: %s", exc)

    async def _run_all() -> None:
        ran_any = False

        # 1) Per-client scheduled reports from client_report_config.json
        for cfg in client_configs:
            if not isinstance(cfg, dict):
                continue
            client_name = (cfg.get("client_name") or "").strip()
            fleet_name = (cfg.get("fleet_name") or "").strip()
            if not client_name or not fleet_name:
                logger.warning("client_report_config.json entry missing client_name or fleet_name: %s", cfg)
                continue
            # Schedule gate: honour schedule_type (hourly / daily / weekly).
            # Entries without schedule_type default to legacy "daily" behaviour.
            # Manual UI triggers bypass all gates so every entry always runs.
            if not is_manual_trigger:
                schedule_type = (cfg.get("schedule_type") or "daily").lower()
                run_hour = cfg.get("run_hour")
                run_day = cfg.get("run_day")

                if schedule_type == "every_20min":
                    # Fire in any 10-min window that starts at a 20-min boundary
                    # (:00-:09, :20-:29, :40-:49). Using a window instead of exact
                    # match avoids failures from Airflow logical_date offsets or
                    # task start lag that shift the actual minute by a few seconds/minutes.
                    ref = reference_now if reference_now else datetime.now()
                    if ref.minute % 20 >= 10:
                        continue
                elif schedule_type == "hourly":
                    # Fire only in the first cycle of each hour (minute 0–9).
                    # The DAG runs every 10 min, so this gives exactly one run/hour.
                    ref = reference_now if reference_now else datetime.now()
                    if ref.minute >= 10:
                        continue
                elif schedule_type == "weekly":
                    ref = reference_now if reference_now else datetime.now()
                    current_weekday = ref.weekday()  # 0=Monday, 6=Sunday
                    if run_day is not None:
                        try:
                            if int(run_day) != current_weekday:
                                continue
                        except (ValueError, TypeError):
                            pass
                    if run_hour is not None:
                        try:
                            if int(run_hour) != current_run_hour:
                                continue
                        except (ValueError, TypeError):
                            pass
                else:  # "daily" (default / legacy)
                    ref = reference_now if reference_now else datetime.now()
                    # Only fire in the first 10-minute window of the scheduled hour
                    # (DAG ticks every 10 min, so this gives exactly one run/day)
                    if ref.minute >= 10:
                        continue
                    if run_hour is not None:
                        try:
                            if int(run_hour) != current_run_hour:
                                continue
                        except (ValueError, TypeError):
                            pass
            time_phrase = (cfg.get("time_phrase") or TIME_RANGE).strip()
            if not _is_relative_time_phrase(time_phrase):
                time_phrase = TIME_RANGE
            timezone = (cfg.get("timezone") or default_tz).strip()
            sections = _sections_from_config(cfg.get("sections"))
            logger.info("Running configured report: client=%s fleet=%s sections=%s", client_name, fleet_name, cfg.get("sections"))
            await _fetch_and_send_one(
                client_name, fleet_name, time_phrase, timezone,
                sections_override=sections, item=None, recipients=airflow_recipients,
            )
            ran_any = True

        # 2) Ad-hoc report from scheduled_report_prompts.json (written by chat)
        if prompts:
            defaults = {
                "fm_client_name": default_client,
                "fleet_name": default_fleet,
                "timezone": default_tz,
                "time_phrase": TIME_RANGE,
            }
            for prompt_entry in prompts:
                if not prompt_entry:
                    continue
                # Dict entry: written by chat via add_resolved_prompt_for_airflow
                if isinstance(prompt_entry, dict):
                    client_name = (prompt_entry.get("client_name") or default_client).strip()
                    fleet_name = (prompt_entry.get("fleet_name") or default_fleet).strip()
                    time_phrase = (prompt_entry.get("time_phrase") or TIME_RANGE).strip()
                    if not _is_relative_time_phrase(time_phrase):
                        time_phrase = TIME_RANGE
                    timezone = (prompt_entry.get("timezone") or default_tz).strip()
                    if not client_name or not fleet_name:
                        continue
                    await _fetch_and_send_one(client_name, fleet_name, time_phrase, timezone, item=None, recipients=airflow_recipients)
                    ran_any = True
                    continue
                # String entry: parse with NLU to extract intent and item
                if not isinstance(prompt_entry, str) or not prompt_entry.strip():
                    continue
                parsed = await parse_query(prompt_entry.strip(), defaults=defaults)
                client_name = (parsed.fm_client_name or default_client).strip()
                fleet_name = (parsed.fleet_name or default_fleet).strip()
                time_phrase = (parsed.time_phrase or TIME_RANGE).strip()
                if not _is_relative_time_phrase(time_phrase):
                    time_phrase = TIME_RANGE
                timezone = parsed.timezone or default_tz
                if not client_name or not fleet_name:
                    continue
                item = parsed.item if parsed.intent == "basic_analytics_item" else None
                await _fetch_and_send_one(client_name, fleet_name, time_phrase, timezone, item=item, recipients=airflow_recipients)
                ran_any = True

        # 3) Fallback: nothing configured → use .env defaults
        if not ran_any:
            await _fetch_and_send_one(
                default_client, default_fleet, TIME_RANGE, default_tz,
                item=None, recipients=airflow_recipients,
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
