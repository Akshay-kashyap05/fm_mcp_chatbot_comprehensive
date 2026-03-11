"""Shared analytics helpers used by both mcp_server.py and streamlit_app.py.

All functions accept api/cache objects as keyword-only parameters to allow
both the MCP server and the Streamlit app to share logic without circular imports.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.time_parse import parse_time_range
from src.formatting import summarize_basic_analytics, extract_item_value
from src.report_builder import build_pdf_from_text, send_report_email

logger = logging.getLogger("fm_mcp")

ROUTE_ANALYTICS_ITEMS = (
    "takt_time", "average_takt_time",
    "avg_obstacle_per_sherpa", "avg_obstacle_time", "obstacle_time",
    "top_10_routes_takt", "top_routes_takt",
    "route_utilization",
    "avg_obstacle_per_route",
)

_SHERPA_LIST_VALUE_KEY: Dict[str, tuple] = {
    "uptime":                  ("uptime_percentage",       "%"),
    "uptime_percentage":       ("uptime_percentage",       "%"),
    "availability":            ("availability_percentage", "%"),
    "availability_percentage": ("availability_percentage", "%"),
    "utilization":             ("utilization",             "%"),
    "battery":                 ("battery_level",           "%"),
    "battery_level":           ("battery_level",           "%"),
}

_METRIC_DISPLAY_LABEL: Dict[str, str] = {
    "uptime":                  "Uptime",
    "uptime_percentage":       "Uptime",
    "availability":            "Availability",
    "availability_percentage": "Availability",
    "utilization":             "Utilization",
    "total_trips":             "Total Trips",
    "total_distance_km":       "Total Distance (km)",
    "battery":                 "Battery Level",
    "battery_level":           "Battery Level",
}


def format_metric_value(metric: str, val: Any, note: str, use_markdown: bool = False) -> str:
    """Format a metric value for display.

    use_markdown=True  → renders per-sherpa lists as a markdown table (for Streamlit).
    use_markdown=False → fixed-width ASCII table (for terminal / MCP).
    """
    label = _METRIC_DISPLAY_LABEL.get(metric, metric.replace("_", " ").title())

    if isinstance(val, list) and val and isinstance(val[0], dict):
        key, unit = _SHERPA_LIST_VALUE_KEY.get(metric, (None, ""))

        rows = []
        for entry in val:
            sherpa = entry.get("sherpa_name") or entry.get("sherpa", "Unknown")
            if key:
                v = entry.get(key)
            else:
                v = next(
                    (entry[k] for k in entry
                     if k not in ("sherpa_name", "sherpa") and isinstance(entry[k], (int, float))),
                    None,
                )
            v_str = f"{v}{unit}" if v is not None else "N/A"
            rows.append((sherpa, v_str))

        if use_markdown:
            lines = [
                f"**{label}**\n",
                "| Sherpa | Value |",
                "|--------|-------|",
            ]
            for sherpa, v_str in rows:
                lines.append(f"| {sherpa} | {v_str} |")
            result = "\n".join(lines)
        else:
            col_w = 44
            lines = [
                f"{label}:",
                f"  {'Sherpa':<{col_w}} {'Value':>8}",
                f"  {'-' * col_w} {'------':>8}",
            ]
            for sherpa, v_str in rows:
                lines.append(f"  {sherpa:<{col_w}} {v_str:>8}")
            result = "\n".join(lines)
    else:
        if use_markdown:
            result = f"**{label}:** {val}"
        else:
            result = f"{label}: {val}"

    if note:
        result += f"\n{'> ' if use_markdown else ''}Note: {note}"
    return result


async def resolve_sherpa_names_for_fleet(
    client_name: str,
    fleet_name: str,
    *,
    api,
    client_cache,
    sherpa_cache,
) -> Optional[List[str]]:
    """Resolve client + fleet to a list of sherpa names. Returns None if unresolved."""
    all_clients = await client_cache.get_or_set("all_clients", api.get_clients)
    client_id = None
    for c in all_clients:
        if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == client_name.lower():
            client_id = c.get("fm_client_id")
            break
    if not client_id:
        for c in all_clients:
            if isinstance(c, dict):
                c_name = (c.get("fm_client_name") or "").lower()
                if client_name.lower() in c_name or c_name in client_name.lower():
                    client_id = c.get("fm_client_id")
                    break
    if not client_id:
        return None
    cache_key = f"sherpas_client_{client_id}"
    all_sherpas = await sherpa_cache.get_or_set(cache_key, api.get_sherpas_by_client_id, client_id)
    matching = [
        s for s in all_sherpas
        if isinstance(s, dict) and (s.get("fleet_name") or "").lower() == fleet_name.lower()
    ]
    names = [s.get("sherpa_name") for s in matching if s.get("sherpa_name")]
    return names if names else None


async def fetch_analytics_data_and_summary(
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    *,
    api,
    client_cache,
    sherpa_cache,
) -> Tuple[str, Dict[str, Any], Dict[str, str]]:
    """Fetch basic_analytics and return (summary_text, data, time_strings)."""
    await api.ensure_token()
    tr = parse_time_range(time_range, timezone=timezone)
    time_strings = tr.to_strings()
    sherpa_names = await resolve_sherpa_names_for_fleet(
        client_name, fleet_name,
        api=api, client_cache=client_cache, sherpa_cache=sherpa_cache,
    )
    api_sherpa = sherpa_names if sherpa_names else None
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
    summary = summarize_basic_analytics(data)
    summary_text = f"Analytics Summary for {fleet_name} ({time_range}):\n\n{summary}"
    return summary_text, data, time_strings


async def get_metric_response_and_data(
    metric: str,
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    *,
    api,
    client_cache,
    sherpa_cache,
    sherpa_name: Optional[str] = None,
    use_markdown: bool = False,
) -> Tuple[str, Dict[str, Any], Dict[str, str]]:
    """Fetch a specific metric and return (response_text, data, time_strings)."""
    await api.ensure_token()
    tr = parse_time_range(time_range, timezone=timezone)
    time_strings = tr.to_strings()

    api_sherpa_name = sherpa_name
    if isinstance(api_sherpa_name, str) and api_sherpa_name.lower() in ("null", "none", ""):
        api_sherpa_name = None

    if api_sherpa_name is None:
        try:
            all_clients = await client_cache.get_or_set("all_clients", api.get_clients)
            client_id = None
            client_name_lower = client_name.lower()
            for c in all_clients:
                if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == client_name_lower:
                    client_id = c.get("fm_client_id")
                    break
            if not client_id:
                for c in all_clients:
                    if isinstance(c, dict):
                        c_name = (c.get("fm_client_name") or "").lower()
                        if client_name_lower in c_name or c_name in client_name_lower:
                            client_id = c.get("fm_client_id")
                            break
            if client_id:
                cache_key = f"sherpas_client_{client_id}"
                all_sherpas = await sherpa_cache.get_or_set(
                    cache_key, api.get_sherpas_by_client_id, client_id
                )
                matching_sherpas = [
                    s for s in all_sherpas
                    if isinstance(s, dict) and (s.get("fleet_name") or "").lower() == fleet_name.lower()
                ]
                if matching_sherpas:
                    sherpa_names = [s.get("sherpa_name") for s in matching_sherpas if s.get("sherpa_name")]
                    api_sherpa_name = sherpa_names if sherpa_names else ""
                    if sherpa_names:
                        logger.info(
                            "Querying all %d sherpas for client %s, fleet %s",
                            len(sherpa_names), client_name, fleet_name,
                        )
                else:
                    api_sherpa_name = ""
            else:
                api_sherpa_name = ""
        except Exception as e:
            logger.error("Error fetching sherpas for client %s: %s", client_name, e)
            api_sherpa_name = ""

    if metric in ROUTE_ANALYTICS_ITEMS:
        data = await api.route_analytics(
            fm_client_name=client_name,
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=timezone,
            fleet_name=fleet_name,
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=api_sherpa_name,
        )
    else:
        data = await api.basic_analytics(
            fm_client_name=client_name,
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=timezone,
            fleet_name=fleet_name,
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=api_sherpa_name,
        )

    extraction_sherpa_hint = (
        None if api_sherpa_name == ""
        else (api_sherpa_name if isinstance(api_sherpa_name, str) else None)
    )
    val, note = extract_item_value(data, item=metric, sherpa_hint=extraction_sherpa_hint)
    result = format_metric_value(metric, val, note, use_markdown=use_markdown)
    return result, data, time_strings


def generate_and_send_text_report(
    report_text: str,
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    time_strings: Dict[str, str],
    *,
    project_root: str,
) -> None:
    """Build a PDF from terminal/chat text and email it."""
    try:
        safe_name = (client_name + "_" + fleet_name).replace(" ", "-")[:50]
        pdf_filename = f"Analytics-Report-{safe_name}-{datetime.now().strftime('%Y-%m-%d')}.pdf"
        pdf_path = os.path.join(project_root, pdf_filename)
        build_pdf_from_text(
            report_text, client_name, fleet_name, time_range,
            time_strings, pdf_path, report_dir=project_root,
        )
        subject = f"Analytics Report - {fleet_name} - {time_range}"
        send_report_email(pdf_path, subject, report_dir=project_root)
        logger.info("Text-based report generated and sent: %s", pdf_filename)
    except Exception as e:
        logger.warning("Text-based report generation/send failed: %s", e)
