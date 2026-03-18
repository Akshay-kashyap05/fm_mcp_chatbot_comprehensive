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
from src.client_match import resolve_client, resolve_sherpa

logger = logging.getLogger("fm_mcp")

ROUTE_ANALYTICS_ITEMS = (
    "takt_time", "average_takt_time",
    "avg_obstacle_per_sherpa", "avg_obstacle_time", "obstacle_time",
    "top_10_routes_takt", "top_routes_takt",
    "route_utilization",
    "avg_obstacle_per_route",
    "route_analytics",  # generic "route analytics" query — handled in extract_item_value
)

_SHERPA_LIST_VALUE_KEY: Dict[str, tuple] = {
    "uptime":                  ("uptime_percentage",       "%"),
    "uptime_percentage":       ("uptime_percentage",       "%"),
    "availability":            ("availability_percentage", "%"),
    "availability_percentage": ("availability_percentage", "%"),
    "utilization":             ("utilization",             "%"),
    "battery":                 ("battery_level",           "%"),
    "battery_level":           ("battery_level",           "%"),
    "sherpa_wise_trips":       ("trip_count",              ""),
    "sherpa_wise_distance":    ("distance_km",             " km"),
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
    "sherpa_wise_trips":       "Trips per Sherpa",
    "sherpa_wise_distance":    "Distance per Sherpa (km)",
}


def format_metric_value(metric: str, val: Any, note: str, use_markdown: bool = False) -> str:
    """Format a metric value for display.

    use_markdown=True  → renders per-sherpa lists as a markdown table (for Streamlit).
    use_markdown=False → fixed-width ASCII table (for terminal / MCP).
    """
    label = _METRIC_DISPLAY_LABEL.get(metric, metric.replace("_", " ").title())

    if isinstance(val, list) and not val:
        # Empty list — no data returned for this metric
        result = f"**{label}:** No data available" if use_markdown else f"{label}: No data available"
    elif isinstance(val, list) and isinstance(val[0], dict):
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
            result = f"**{label}:** {val}" if val is not None else f"**{label}:** No data available"
        else:
            result = f"{label}: {val}" if val is not None else f"{label}: No data available"

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
    matched = resolve_client(client_name, all_clients)
    if not matched:
        logger.warning("resolve_sherpa_names_for_fleet: no client match for '%s'", client_name)
        return None
    client_id = matched.get("fm_client_id")
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
    selected_sherpas: Optional[List[str]] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, str]]:
    """Fetch basic_analytics and return (summary_text, data, time_strings)."""
    await api.ensure_token()
    tr = parse_time_range(time_range, timezone=timezone)
    time_strings = tr.to_strings()
    if selected_sherpas:
        # User explicitly selected sherpas from dropdown — use directly
        api_sherpa = selected_sherpas
    else:
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
    selected_sherpas: Optional[List[str]] = None,
    use_markdown: bool = True,
) -> Tuple[str, Dict[str, Any], Dict[str, str]]:
    """Fetch a specific metric and return (response_text, data, time_strings)."""
    await api.ensure_token()
    tr = parse_time_range(time_range, timezone=timezone)
    time_strings = tr.to_strings()

    # Dropdown selection takes priority — use directly, skip all resolve logic
    if selected_sherpas:
        api_sherpa_name = selected_sherpas
        logger.info("Using %d selected sherpas for metric '%s': %s", len(selected_sherpas), metric, selected_sherpas)
    else:
        api_sherpa_name = sherpa_name
        if isinstance(api_sherpa_name, str) and api_sherpa_name.lower() in ("null", "none", ""):
            api_sherpa_name = None

    # ── Sherpa hint provided — resolve to exact API name ─────────────────────
    if not selected_sherpas and isinstance(api_sherpa_name, str) and api_sherpa_name:
        try:
            all_clients = await client_cache.get_or_set("all_clients", api.get_clients)
            matched_client = resolve_client(client_name, all_clients)
            if matched_client:
                client_id = matched_client.get("fm_client_id")
                cache_key = f"sherpas_client_{client_id}"
                all_sherpas = await sherpa_cache.get_or_set(
                    cache_key, api.get_sherpas_by_client_id, client_id
                )
                fleet_sherpas = [
                    s for s in all_sherpas
                    if isinstance(s, dict) and (s.get("fleet_name") or "").lower() == fleet_name.lower()
                ]
                resolved = resolve_sherpa(api_sherpa_name, fleet_sherpas)
                if resolved:
                    logger.info("Resolved sherpa hint '%s' → '%s'", api_sherpa_name, resolved)
                    api_sherpa_name = [resolved]
                else:
                    logger.warning("Could not resolve sherpa hint '%s' for fleet %s", api_sherpa_name, fleet_name)
        except Exception as e:
            logger.warning("Sherpa hint resolution failed: %s", e)

    if not selected_sherpas and api_sherpa_name is None:
        try:
            all_clients = await client_cache.get_or_set("all_clients", api.get_clients)
            matched = resolve_client(client_name, all_clients)
            client_id = matched.get("fm_client_id") if matched else None
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

    # Post-filter: if specific sherpas were selected, drop any unselected entries
    if selected_sherpas and isinstance(val, list) and val and isinstance(val[0], dict):
        selected_lower = {s.lower() for s in selected_sherpas}
        val = [
            entry for entry in val
            if (entry.get("sherpa_name") or entry.get("sherpa", "")).lower() in selected_lower
        ]

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
        raise
