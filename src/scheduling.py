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

PENDING_REPORT_FILE         = os.path.join(_PROJECT_ROOT, "pending_report.json")
PENDING_SCHEDULE_FILE       = os.path.join(_PROJECT_ROOT, "pending_schedule.json")
PENDING_CLARIFICATION_FILE  = os.path.join(_PROJECT_ROOT, "pending_clarification.json")
CLIENT_REPORT_CONFIG_FILE   = os.path.join(_PROJECT_ROOT, "client_report_config.json")


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

def _parse_hour(text: str) -> Optional[int]:
    """Extract hour (0-23) from text, supporting HH:MM, Hpm/Ham, and bare integers."""
    t = text.strip().lower()

    # HH:MM format — e.g. "14:30" or "9:00"
    hm = re.search(r"\b(\d{1,2}):(\d{2})\b", t)
    if hm:
        return max(0, min(23, int(hm.group(1))))

    # 12-hour am/pm — e.g. "2pm", "9am", "11 pm"
    ampm = re.search(r"\b(\d{1,2})\s*(am|pm)\b", t)
    if ampm:
        h = int(ampm.group(1))
        suffix = ampm.group(2)
        if suffix == "pm" and h != 12:
            h += 12
        elif suffix == "am" and h == 12:
            h = 0
        return max(0, min(23, h))

    # Bare integer — e.g. "8", "14"
    bare = re.search(r"\b(\d{1,2})\b", t)
    if bare:
        return max(0, min(23, int(bare.group(1))))

    return None


def _parse_schedule_command(text: str) -> Optional[dict]:
    """Parse a schedule preference string from the user.

    Returns:
      None                                          → skip (don't schedule)
      {"schedule_type": "every_20min"}              → every 20 minutes (testing)
      {"schedule_type": "hourly"}                   → every hour
      {"schedule_type": "daily",  "run_hour": int}  → every day at given hour
      {"schedule_type": "weekly", "run_hour": int, "run_day": int}
      {"error": "<message>"}                        → couldn't parse, ask again

    Supported time formats:
      - Bare hour:    daily 8   /  weekly monday 14
      - 12-hour:      daily 2pm /  weekly friday 9am
      - 24-hour HH:MM: daily 14:30  →  uses the hour, ignores minutes
    """
    t = text.strip().lower()

    if t in ("skip", "no", "no schedule", "don't schedule", "dont schedule", "none", "not now"):
        return None

    _h = _parse_hour(t)
    hour = _h if _h is not None else datetime.now().hour

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

    return {"error": f"Couldn't understand '{text}'. Try: `daily 8`, `daily 2pm`, `daily 14:30`, `hourly`, `weekly monday 9am`, or `skip`."}


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


# ── Pending clarification state ───────────────────────────────────────────
# Saved when NLU can't extract a required field (client_name, time_phrase).
# Stores the partial ParsedQuery so chat_handler can resume once the user answers.

def _save_pending_clarification(missing_field: str, partial_pq: dict) -> None:
    """Save what's missing and the partial parsed query so we can resume."""
    try:
        with open(PENDING_CLARIFICATION_FILE, "w", encoding="utf-8") as f:
            _json.dump({"missing_field": missing_field, "partial_pq": partial_pq}, f)
    except Exception:
        pass


def _load_pending_clarification() -> Optional[dict]:
    if not os.path.isfile(PENDING_CLARIFICATION_FILE):
        return None
    try:
        with open(PENDING_CLARIFICATION_FILE, encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return None


def _clear_pending_clarification() -> None:
    try:
        if os.path.isfile(PENDING_CLARIFICATION_FILE):
            os.remove(PENDING_CLARIFICATION_FILE)
    except Exception:
        pass


# ── Client report config update ───────────────────────────────────────────

def _cfg_fleet_names(cfg: dict) -> list:
    """Return fleet names from either new fleet_names (list) or legacy fleet_name (str) field."""
    if "fleet_names" in cfg:
        fl = cfg["fleet_names"]
        return list(fl) if isinstance(fl, list) else ([fl] if fl else [])
    fn = cfg.get("fleet_name", "")
    return [fn] if fn else []


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
    """Add or update an entry in client_report_config.json for Airflow scheduling.

    Uniqueness key: (client_name, schedule_type, run_hour, run_day).
    Multiple fleets for the same client+schedule slot are stored as a list
    under fleet_names, so Airflow sends ONE combined report per slot.
    """
    configs: list = []
    if os.path.isfile(CLIENT_REPORT_CONFIG_FILE):
        try:
            with open(CLIENT_REPORT_CONFIG_FILE, "r", encoding="utf-8") as f:
                configs = _json.load(f)
            if not isinstance(configs, list):
                configs = []
        except Exception:
            configs = []

    # Migrate legacy fleet_name (str) entries to fleet_names (list) on read
    for cfg in configs:
        if isinstance(cfg, dict) and "fleet_name" in cfg and "fleet_names" not in cfg:
            cfg["fleet_names"] = _cfg_fleet_names(cfg)
            cfg.pop("fleet_name", None)

    def _same_slot(cfg: dict) -> bool:
        return (
            isinstance(cfg, dict)
            and cfg.get("client_name", "").lower() == client_name.lower()
            and (cfg.get("schedule_type") or "daily").lower() == schedule_type.lower()
            and cfg.get("run_hour") == run_hour
            and cfg.get("run_day") == run_day
        )

    updated = False
    for i, cfg in enumerate(configs):
        if _same_slot(cfg):
            existing_fleets = _cfg_fleet_names(cfg)
            if fleet_name not in existing_fleets:
                existing_fleets.append(fleet_name)

            existing_sections = cfg.get("sections") or []
            if sections is not None:
                merged_sections = list(dict.fromkeys(existing_sections + sections))
            else:
                merged_sections = existing_sections

            configs[i] = {
                "client_name": client_name,
                "fleet_names": existing_fleets,
                "sections": merged_sections,
                "time_phrase": time_phrase,
                "timezone": timezone,
                "schedule_type": schedule_type,
                **({"run_hour": run_hour} if run_hour is not None else {}),
                **({"run_day": run_day} if run_day is not None else {}),
            }
            updated = True
            break

    if not updated:
        new_entry: dict = {
            "client_name": client_name,
            "fleet_names": [fleet_name],
            "sections": sections if sections is not None else [],
            "time_phrase": time_phrase,
            "timezone": timezone,
            "schedule_type": schedule_type,
        }
        if run_hour is not None:
            new_entry["run_hour"] = run_hour
        if run_day is not None:
            new_entry["run_day"] = run_day
        configs.append(new_entry)

    with open(CLIENT_REPORT_CONFIG_FILE, "w", encoding="utf-8") as f:
        _json.dump(configs, f, indent=2)
    logger.info(
        "Scheduled report %s in client_report_config.json: client=%s fleet=%s type=%s",
        "updated" if updated else "added",
        client_name, fleet_name, schedule_type,
    )
