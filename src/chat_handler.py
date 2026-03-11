"""sanjaya_chat implementation: 3-turn flow (query → proceed/cancel → schedule).

Extracted from mcp_server.py so the file stays thin. Receives all external
dependencies (api client, caches, helper callables) as parameters to avoid
circular imports.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from src.nlu import parse_query
from src.scheduling import (
    CLIENT_REPORT_CONFIG_FILE,
    _DAY_NAMES,
    _SCHEDULE_TIME_PHRASE,
    _add_or_update_client_config,
    _clear_pending_report,
    _clear_pending_schedule,
    _item_to_section_names,
    _load_pending_report,
    _load_pending_schedule,
    _parse_schedule_command,
    _save_pending_report,
    _save_pending_schedule,
)

logger = logging.getLogger("fm_mcp")


async def handle_chat(
    text: str,
    *,
    api,
    client_cache,
    client_details_cache,
    sherpa_cache,
    project_root: str,
    defaults: dict,
    get_metric_data_fn: Callable,
    fetch_analytics_fn: Callable,
    send_text_report_fn: Callable,
) -> str:
    """Main 3-turn chat flow: query → proceed/cancel → schedule.

    Parameters
    ----------
    api                 SanjayaAPI instance
    client_cache        TTLCache for client list
    client_details_cache TTLCache for per-client details
    sherpa_cache        TTLCache for sherpa lists
    project_root        Absolute path to project root (for file I/O)
    defaults            Dict from _defaults() — env-based fallback values
    get_metric_data_fn  Callable = _get_metric_response_and_data
    fetch_analytics_fn  Callable = _fetch_analytics_data_and_summary
    send_text_report_fn Callable = _generate_and_send_text_report
    """
    logger.info("sanjaya_chat called with query: %s", text)

    # ── Proceed / Cancel ──────────────────────────────────────────────────
    cmd = text.strip().lower()
    if cmd in ("proceed", "yes", "proceed to email", "send", "confirm"):
        pending = _load_pending_report()
        if not pending:
            return "No pending report found. Please request a report first."
        try:
            send_text_report_fn(
                pending["report_text"],
                pending["client_name"],
                pending["fleet_name"],
                pending["time_phrase"],
                pending["timezone"],
                pending["time_strings"],
            )
            recipient = os.environ.get("REPORT_RECIPIENT", "")
            _save_pending_schedule(
                client_name=pending["client_name"],
                fleet_name=pending["fleet_name"],
                sections=pending.get("sections"),
                time_phrase=pending.get("time_phrase", "yesterday"),
                timezone=pending.get("timezone", "Asia/Kolkata"),
            )
            _clear_pending_report()
            return (
                f"PDF report sent to {recipient}.\n\n"
                "**How would you like to schedule this report?**\n"
                "- `daily 8` — every day at 08:00\n"
                "- `daily` — every day at the same hour as now\n"
                "- `hourly` — every hour\n"
                "- `every 20 mins` — every 20 minutes (for testing)\n"
                "- `weekly monday 8` — every Monday at 08:00\n"
                "- `skip` — don't schedule (one-time send only)\n"
            )
        except Exception as e:
            logger.warning("Failed to send pending report: %s", e)
            return f"Failed to send report: {e}"

    if cmd in ("cancel", "no", "skip") and _load_pending_report():
        _clear_pending_report()
        return "Report cancelled. No email sent."

    # ── Schedule response (after email was sent) ──────────────────────────
    pending_sched = _load_pending_schedule()
    if pending_sched:
        schedule = _parse_schedule_command(text.strip())

        if schedule is None:
            _clear_pending_schedule()
            return "OK — report was sent once, no recurring schedule added."

        if "error" in schedule:
            return (
                schedule["error"] + "\n\n"
                "Please try again:\n"
                "- `daily 8` — every day at 08:00\n"
                "- `hourly` — every hour\n"
                "- `every 20 mins` — every 20 minutes (for testing)\n"
                "- `weekly monday 8` — every Monday at 08:00\n"
                "- `skip` — no scheduling\n"
            )

        sched_type = schedule["schedule_type"]
        time_phrase = pending_sched.get("time_phrase") or _SCHEDULE_TIME_PHRASE.get(sched_type, "yesterday")
        try:
            _add_or_update_client_config(
                client_name=pending_sched["client_name"],
                fleet_name=pending_sched["fleet_name"],
                time_phrase=time_phrase,
                timezone=pending_sched.get("timezone", "Asia/Kolkata"),
                sections=pending_sched.get("sections"),
                schedule_type=sched_type,
                run_hour=schedule.get("run_hour"),
                run_day=schedule.get("run_day"),
            )
            _clear_pending_schedule()

            if sched_type == "every_20min":
                cadence_desc = "every 20 minutes [testing mode]"
            elif sched_type == "hourly":
                cadence_desc = "every hour"
            elif sched_type == "daily":
                h = schedule.get("run_hour", datetime.now().hour)
                cadence_desc = f"every day at {h:02d}:00"
            else:
                day_label = _DAY_NAMES[schedule.get("run_day", 0)]
                h = schedule.get("run_hour", datetime.now().hour)
                cadence_desc = f"every {day_label} at {h:02d}:00"
            sched_desc = f"{cadence_desc} (data: {time_phrase})"

            return (
                f"Scheduled! Airflow will deliver this report **{sched_desc}**.\n\n"
                f"The `client_report_config.json` has been updated — "
                f"you can change or remove the entry there at any time."
            )
        except Exception as e:
            logger.warning("Failed to write schedule config: %s", e)
            return f"Failed to save schedule: {e}"

    # ── Main query ────────────────────────────────────────────────────────
    try:
        clean_text = text.replace("[sherpa:all]", "").strip()
        empty_defaults = {
            "fm_client_name": "",
            "fleet_name": "",
            "timezone": defaults.get("timezone", "Asia/Kolkata"),
            "time_phrase": defaults.get("time_phrase", "today"),
        }
        pq = await parse_query(clean_text, defaults=empty_defaults)

        t_lower = clean_text.lower()

        # If user explicitly said "basic analytics" or "analytics summary", always return full summary
        # regardless of what Ollama returned (it often hallucinates a specific metric).
        if "basic analytics" in t_lower or "analytics summary" in t_lower:
            pq.intent = "basic_analytics"
            pq.item = None
        elif pq.intent == "basic_analytics" and pq.item in (None, ""):
            # NLU returned full-summary intent — promote to item intent if text mentions one metric
            if "total trips" in t_lower and "summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "total_trips"
            elif ("total distance" in t_lower or ("distance" in t_lower and "total" in t_lower)) and "summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "total_distance_km"
            elif ("sherpa status" in t_lower or ("status" in t_lower and ("sherpa" in t_lower or "tug" in t_lower))) and "summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "sherpa_status"
        elif pq.intent == "basic_analytics" and pq.item:
            pq.intent = "basic_analytics_item"

        logger.info(
            "Parsed query: intent=%s, item=%s, client=%s, fleet=%s, time_phrase=%s, sherpa_hint=%s",
            pq.intent, pq.item, pq.fm_client_name, pq.fleet_name, pq.time_phrase, pq.sherpa_hint,
        )

        if pq.intent == "help":
            return """Sanjaya Analytics MCP Server

Available Tools:
- get_analytics_summary: Get comprehensive analytics for a fleet
- get_metric: Get a specific metric value
- resolve_client_name: Resolve partial client names
- resolve_fleet_name: Resolve partial fleet names

Available Resources:
- sanjaya://clients: List all clients
- sanjaya://clients/{client_id}/fleets: List fleets for a client
- sanjaya://fleets/{fleet_id}/sherpas: List sherpas for a fleet

Available Prompts:
- analytics_summary_query: Template for analytics queries
- metric_query: Template for metric queries
"""

        # Apply defaults — track whether client came from NLU or fallback
        client_from_nlu = bool(pq.fm_client_name)
        client_name = pq.fm_client_name or defaults.get("fm_client_name")
        fleet_name = pq.fleet_name
        if not fleet_name and not pq.fm_client_name:
            fleet_name = defaults.get("fleet_name")
        time_phrase = pq.time_phrase or defaults.get("time_phrase", "today")
        timezone = pq.timezone
        if not timezone or str(timezone).lower() in ("null", "none", ""):
            timezone = defaults.get("timezone") or "Asia/Kolkata"
        else:
            timezone = str(timezone)

        # ── Client/fleet swap correction ──────────────────────────────────
        # Fires when: NLU left client empty and put the client name in fleet_name.
        # Also fires when client came from defaults (not NLU) and fleet_name matches a real client.
        try:
            await api.ensure_token()
            all_clients = await client_cache.get_or_set("all_clients", api.get_clients)
            if (not client_from_nlu) and fleet_name:
                for c in all_clients:
                    if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == fleet_name.lower():
                        logger.info("Correcting NLU swap: treating fleet_name '%s' as client_name", fleet_name)
                        client_name = c.get("fm_client_name")
                        fleet_name = None
                        break
            if client_name:
                for c in all_clients:
                    if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == client_name.lower():
                        client_name = c.get("fm_client_name")
                        break
        except Exception:
            pass

        # ── Fleet auto-resolution ─────────────────────────────────────────
        if client_name and not fleet_name:
            try:
                all_clients = await client_cache.get_or_set("all_clients", api.get_clients)
                client_id = None
                for c in all_clients:
                    if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == client_name.lower():
                        client_id = c.get("fm_client_id")
                        break
                if client_id:
                    cache_key = f"client_details_{client_id}"
                    client_details = await client_details_cache.get_or_set(
                        cache_key, api.get_client_by_id, client_id
                    )
                    fleet_names = client_details.get("fm_fleet_names", [])
                    if len(fleet_names) == 1:
                        fleet_name = fleet_names[0]
                        logger.info("Auto-resolved fleet for client %s: %s", client_name, fleet_name)
                    elif fleet_names:
                        import json as _json
                        if os.path.isfile(CLIENT_REPORT_CONFIG_FILE):
                            try:
                                with open(CLIENT_REPORT_CONFIG_FILE, "r", encoding="utf-8") as _f:
                                    _configs = _json.load(_f)
                                for _cfg in _configs:
                                    if isinstance(_cfg, dict) and _cfg.get("client_name", "").lower() == client_name.lower():
                                        fleet_name = _cfg.get("fleet_name")
                                        logger.info("Resolved fleet from config for client %s: %s", client_name, fleet_name)
                                        break
                            except Exception:
                                pass
                        if not fleet_name:
                            return (
                                f"Client **{client_name}** has multiple fleets: "
                                + ", ".join(f"`{f}`" for f in fleet_names)
                                + ". Please specify the fleet in your query."
                            )
            except Exception as e:
                logger.warning("Fleet auto-resolve failed: %s", e)

        if not client_name or not fleet_name:
            return (
                "Error: Client name and fleet name are required. "
                "Use resolve_client_name and resolve_fleet_name tools to find them, "
                "or specify in your query."
            )

        api_sherpa_name = pq.sherpa_hint
        if isinstance(api_sherpa_name, str) and api_sherpa_name.lower() in ("null", "none", ""):
            api_sherpa_name = None

        # ── Multi-metric: call each endpoint and combine ──────────────────
        if pq.intent == "multi_metric" and len(pq.items) > 1:
            parts, time_strings = [], {}
            for metric in pq.items:
                metric_response, _, ts = await get_metric_data_fn(
                    metric=metric,
                    client_name=client_name,
                    fleet_name=fleet_name,
                    time_range=time_phrase,
                    timezone=timezone,
                    sherpa_name=api_sherpa_name,
                )
                parts.append(metric_response)
                time_strings = ts
            combined = "\n\n".join(parts)
            _save_pending_report(
                client_name, fleet_name, time_phrase, timezone,
                time_strings, combined, clean_text, sections=None,
            )
            return combined + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."

        # ── Single metric ─────────────────────────────────────────────────
        if pq.intent == "basic_analytics_item" and pq.item:
            metric_response, data, time_strings = await get_metric_data_fn(
                metric=pq.item,
                client_name=client_name,
                fleet_name=fleet_name,
                time_range=time_phrase,
                timezone=timezone,
                sherpa_name=api_sherpa_name,
            )
            _save_pending_report(
                client_name, fleet_name, time_phrase, timezone,
                time_strings, metric_response, clean_text,
                sections=_item_to_section_names(pq.item),
            )
            return metric_response + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."

        # ── Full analytics summary ────────────────────────────────────────
        else:
            summary_text, data, time_strings = await fetch_analytics_fn(
                client_name, fleet_name, time_phrase, timezone
            )
            _save_pending_report(
                client_name, fleet_name, time_phrase, timezone,
                time_strings, summary_text, clean_text,
                sections=None,
            )
            return summary_text + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."

    except Exception as e:
        logger.error("Error in sanjaya_chat: %s", e)
        return f"Error: {str(e)}"
