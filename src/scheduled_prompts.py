"""Bridge: prompts from chat are written here so Airflow DAG can use the same prompts.

When the user asks for a report in chat (sanjaya_chat), we write the **resolved**
parameters (client_name, fleet_name, time_phrase, timezone) that the MCP server
actually used. Only the latest prompt is kept: each new addition replaces the
file content so older prompts are removed. The Airflow DAG reads this file and
uses those values directly—no re-parsing. File can also contain a legacy string
prompt; DAG will parse that with NLU.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, List

logger = logging.getLogger(__name__)

FILENAME = "scheduled_report_prompts.json"


def _file_path(project_root: str) -> str:
    return os.path.join(project_root, FILENAME)


def add_resolved_prompt_for_airflow(
    client_name: str,
    fleet_name: str,
    time_phrase: str,
    timezone: str,
    project_root: str,
) -> None:
    """Write the exact (client, fleet, time_phrase, timezone) the chat used.
    Replaces any existing prompt in the file so only this latest one is kept.
    Airflow will use it directly—same as chat, no re-parsing.
    """
    if not (client_name and fleet_name):
        return
    entry = {
        "client_name": str(client_name).strip(),
        "fleet_name": str(fleet_name).strip(),
        "time_phrase": str(time_phrase).strip(),
        "timezone": str(timezone or "Asia/Kolkata").strip(),
    }
    path = _file_path(project_root)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump([entry], f, indent=2)
        logger.info(
            "Set resolved prompt for Airflow: client=%s fleet=%s time=%s",
            entry["client_name"], entry["fleet_name"], entry["time_phrase"],
        )
    except Exception as e:
        logger.warning("Could not set resolved prompt for Airflow: %s", e)


def add_prompt_for_airflow(prompt: str, project_root: str) -> None:
    """Write a string prompt (legacy). Replaces any existing prompt. DAG will parse with NLU."""
    if not (prompt and isinstance(prompt, str)):
        return
    prompt = prompt.strip()
    if not prompt:
        return
    path = _file_path(project_root)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump([prompt], f, indent=2)
        logger.info("Set prompt for Airflow: %s", prompt[:80])
    except Exception as e:
        logger.warning("Could not set prompt for Airflow: %s", e)


def get_prompts(project_root: str) -> List[Any]:
    """Return the list from the file. Items are either dicts (resolved) or strings (parse with NLU)."""
    path = _file_path(project_root)
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data if isinstance(data, list) else data.get("prompts", [])
        return items if isinstance(items, list) else []
    except Exception:
        return []
