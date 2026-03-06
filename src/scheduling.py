"""Scheduling helpers: parse user schedule commands, persist pending state,
and update client_report_config.json for the Airflow DAG.

Extracted from mcp_server.py so it can be imported by both mcp_server.py
and src/chat_handler.py without circular dependencies.
"""

from __future__ import annotations

import json as _json
import logging
import os
import re
from datetime import datetime
from typing import Optional

logger = logging.getLogger("fm_mcp")

# Project root is one directory above this file (src/ → project root)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PENDING_REPORT_FILE    = os.path.join(_PROJECT_ROOT, "pending_report.json")
PENDING_SCHEDULE_FILE  = os.path.join(_PROJECT_ROOT, "pending_schedule.json")
CLIENT_REPORT_CONFIG_FILE = os.path.join(_PROJECT_ROOT, "client_report_config.json")


# ── Section mapping ────────────────────────────────────────────────────────

def _item_to_section_names(item: Optional[str]) -> Optional[list]:
    """Map an NLU item to section name strings for client_report_config.json.
    Returns None for full report (no specific item requested).
    """
    if not item:
        return None
    if item in ("total_trips", "sherpa_wise_trips"):
        return ["trips"]
    if item in ("total_distance_km", "sherpa_wise_distance"):
        return ["distance"]
    if item == "availability":
        return ["availability"]
    if item == "utilization":
        return ["utilization"]
    if item in ("uptime", "uptime_percentage"):
        return ["uptime"]
    return ["route_analytics"]


# ── Day mappings ──────────────────────────────────────────────────────────

_DAY_MAP = {
    "monday": 0, "mon": 0,
    "tuesday": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "thurs": 3,
    "friday": 4, "fri": 4,
    "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}
_DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


# ── Schedule command parser ────────────────────────────────────────────────

def _parse_schedule_command(text: str) -> Optional[dict]:
    """Parse a schedule preference string from the user.

    Returns:
      None                                          → skip (don't schedule)
      {"schedule_type": "every_20min"}              → every 20 minutes (testing)
      {"schedule_type": "hourly"}                   → every hour
      {"schedule_type": "daily",  "run_hour": int}  → every day at given hour
      {"schedule_type": "weekly", "run_hour": int, "run_day": int}
      {"error": "<message>"}                        → couldn't parse, ask again
    """
    t = text.strip().lower()

    if t in ("skip", "no", "no schedule", "don't schedule", "dont schedule", "none", "not now"):
        return None

    hour_match = re.search(r"\b(\d{1,2})\b", t)
    hour = max(0, min(23, int(hour_match.group(1)))) if hour_match else datetime.now().hour

    for day_name, day_idx in _DAY_MAP.items():
        if day_name in t:
            return {"schedule_type": "weekly", "run_hour": hour, "run_day": day_idx}

    if re.search(r"every\s+20\s*min", t) or "20 min" in t or "20min" in t:
        return {"schedule_type": "every_20min", "run_hour": None, "run_day": None}

    if any(kw in t for kw in ("hourly", "every hour", "each hour", "per hour")):
        return {"schedule_type": "hourly", "run_hour": None, "run_day": None}

    if any(kw in t for kw in ("daily", "every day", "each day", "per day")):
        return {"schedule_type": "daily", "run_hour": hour, "run_day": None}

    if re.fullmatch(r"\s*\d{1,2}\s*", t):
        return {"schedule_type": "daily", "run_hour": hour, "run_day": None}

    return {"error": f"Couldn't understand '{text}'. Try: `daily 8`, `hourly`, `weekly monday 8`, or `skip`."}


# Fallback time_phrase per cadence (used only when user's query had no time range)
_SCHEDULE_TIME_PHRASE = {
    "every_20min": "last 20 minutes",
    "hourly":      "last hour",
    "daily":       "yesterday",
    "weekly":      "last week",
}


# ── Pending report state ───────────────────────────────────────────────────

def _save_pending_report(
    client_name: str,
    fleet_name: str,
    time_phrase: str,
    timezone: str,
    time_strings: dict,
    report_text: str,
    prompt_text: str,
    sections: Optional[list] = None,
) -> None:
    data = {
        "client_name": client_name,
        "fleet_name": fleet_name,
        "time_phrase": time_phrase,
        "timezone": timezone,
        "time_strings": time_strings,
        "report_text": report_text,
        "prompt_text": prompt_text,
        "sections": sections,
        "saved_at": datetime.now().isoformat(),
    }
    with open(PENDING_REPORT_FILE, "w", encoding="utf-8") as f:
        _json.dump(data, f, indent=2)


def _load_pending_report() -> Optional[dict]:
    if not os.path.isfile(PENDING_REPORT_FILE):
        return None
    try:
        with open(PENDING_REPORT_FILE, "r", encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return None


def _clear_pending_report() -> None:
    try:
        if os.path.isfile(PENDING_REPORT_FILE):
            os.remove(PENDING_REPORT_FILE)
    except Exception:
        pass


# ── Pending schedule state ─────────────────────────────────────────────────

def _save_pending_schedule(
    client_name: str,
    fleet_name: str,
    sections: Optional[list],
    time_phrase: str,
    timezone: str,
) -> None:
    data = {
        "client_name": client_name,
        "fleet_name": fleet_name,
        "sections": sections,
        "time_phrase": time_phrase,
        "timezone": timezone,
        "saved_at": datetime.now().isoformat(),
    }
    with open(PENDING_SCHEDULE_FILE, "w", encoding="utf-8") as f:
        _json.dump(data, f, indent=2)


def _load_pending_schedule() -> Optional[dict]:
    if not os.path.isfile(PENDING_SCHEDULE_FILE):
        return None
    try:
        with open(PENDING_SCHEDULE_FILE, "r", encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return None


def _clear_pending_schedule() -> None:
    try:
        if os.path.isfile(PENDING_SCHEDULE_FILE):
            os.remove(PENDING_SCHEDULE_FILE)
    except Exception:
        pass


# ── Client report config update ───────────────────────────────────────────

def _add_or_update_client_config(
    client_name: str,
    fleet_name: str,
    time_phrase: str,
    timezone: str,
    sections: Optional[list],
    schedule_type: str = "daily",
    run_hour: Optional[int] = None,
    run_day: Optional[int] = None,
) -> None:
    """Add or update an entry in client_report_config.json for Airflow scheduling."""
    configs: list = []
    if os.path.isfile(CLIENT_REPORT_CONFIG_FILE):
        try:
            with open(CLIENT_REPORT_CONFIG_FILE, "r", encoding="utf-8") as f:
                configs = _json.load(f)
            if not isinstance(configs, list):
                configs = []
        except Exception:
            configs = []

    new_entry: dict = {
        "client_name": client_name,
        "fleet_name": fleet_name,
        "sections": sections if sections is not None else [],
        "time_phrase": time_phrase,
        "timezone": timezone,
        "schedule_type": schedule_type,
    }
    if run_hour is not None:
        new_entry["run_hour"] = run_hour
    if run_day is not None:
        new_entry["run_day"] = run_day

    updated = False
    for i, cfg in enumerate(configs):
        if (
            isinstance(cfg, dict)
            and cfg.get("client_name", "").lower() == client_name.lower()
            and cfg.get("fleet_name", "").lower() == fleet_name.lower()
        ):
            configs[i] = new_entry
            updated = True
            break
    if not updated:
        configs.append(new_entry)

    with open(CLIENT_REPORT_CONFIG_FILE, "w", encoding="utf-8") as f:
        _json.dump(configs, f, indent=2)
    logger.info(
        "Scheduled report %s in client_report_config.json: client=%s fleet=%s type=%s",
        "updated" if updated else "added",
        client_name, fleet_name, schedule_type,
    )
