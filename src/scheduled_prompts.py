"""Bridge: prompts from chat are written here so Airflow DAG can use the same prompts.

When the user asks for a report in chat (sanjaya_chat), we append their prompt to
scheduled_report_prompts.json in the project root. The Airflow DAG reads this file
first (then falls back to Airflow Variable, then .env). So "prompts you give in
chat" become the prompts Airflow uses on its next run (scheduled or manual).
"""

from __future__ import annotations

import json
import logging
import os
from typing import List

logger = logging.getLogger(__name__)

FILENAME = "scheduled_report_prompts.json"


def _file_path(project_root: str) -> str:
    return os.path.join(project_root, FILENAME)


def add_prompt_for_airflow(prompt: str, project_root: str) -> None:
    """Append the given prompt to the list Airflow reads. Dedupes by exact string."""
    if not (prompt and isinstance(prompt, str)):
        return
    prompt = prompt.strip()
    if not prompt:
        return
    path = _file_path(project_root)
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            prompts = data if isinstance(data, list) else data.get("prompts", [])
        else:
            prompts = []
        if not isinstance(prompts, list):
            prompts = []
        if prompt not in prompts:
            prompts.append(prompt)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(prompts, f, indent=2)
            logger.info("Added prompt for Airflow: %s", prompt[:80])
    except Exception as e:
        logger.warning("Could not add prompt for Airflow: %s", e)


def get_prompts(project_root: str) -> List[str]:
    """Return the list of prompts from the file (for tests or DAG)."""
    path = _file_path(project_root)
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        prompts = data if isinstance(data, list) else data.get("prompts", [])
        return prompts if isinstance(prompts, list) else []
    except Exception:
        return []
