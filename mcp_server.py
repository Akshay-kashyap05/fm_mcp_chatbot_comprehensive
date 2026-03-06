#!/usr/bin/env python3
"""Standard MCP server implementation for Sanjaya Analytics.

This follows MCP best practices:
- Focused, composable tools
- Resources for data exposure
- Prompts for common patterns
- Standard MCP content types
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

# Ensure project root is on path so "src" is found when run from any cwd or on server
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent
from dotenv import load_dotenv

load_dotenv()

from src.logging_config import setup_logging
from src.nlu import parse_query, ParsedQuery
from src.sanjaya_client import SanjayaAPI
from src.cache import TTLCache
from src.time_parse import parse_time_range
from src.formatting import summarize_basic_analytics, extract_item_value
from src.report_builder import build_pdf, build_pdf_from_text, send_report_email

setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("fm_mcp")
PROJECT_ROOT = _PROJECT_ROOT

# Create FastMCP server
mcp = FastMCP(
    "sanjaya_analytics_mcp",
    instructions=(
        "You are a Sanjaya Analytics assistant. Help users query analytics data for fleets and sherpas.\n\n"
        "Use the available tools to get analytics data. Resources provide lists of clients, fleets, and sherpas.\n"
        "Prompts provide templates for common query patterns."
    )
)

BASE_URL = "https://sanjaya.atimotors.com"

# File that holds a report the user has seen but not yet confirmed to schedule
PENDING_REPORT_FILE = os.path.join(_PROJECT_ROOT, "pending_report.json")
# File that holds schedule-preference state between the two turns (email sent → awaiting schedule choice)
PENDING_SCHEDULE_FILE = os.path.join(_PROJECT_ROOT, "pending_schedule.json")
# Shared client schedule config (also read by Airflow DAG)
CLIENT_REPORT_CONFIG_FILE = os.path.join(_PROJECT_ROOT, "client_report_config.json")

# Constants
ROUTE_ANALYTICS_ITEMS = (
    "takt_time", "average_takt_time",
    "avg_obstacle_per_sherpa", "avg_obstacle_time", "obstacle_time",
    "top_10_routes_takt", "top_routes_takt",
    "route_utilization",
    "avg_obstacle_per_route",
)

PER_SHERPA_ITEMS = ("avg_obstacle_per_sherpa",)

# Initialize caches
client_cache = TTLCache(ttl_seconds=600.0, max_size=100)
client_details_cache = TTLCache(ttl_seconds=600.0, max_size=100)
sherpa_cache = TTLCache(ttl_seconds=300.0, max_size=200)

# Initialize API client with debug logging if enabled
debug_http = os.environ.get("SANJAYA_HTTP_TRACE", "0") == "1"
client = SanjayaAPI(BASE_URL, debug_http=debug_http)


def _defaults() -> Dict[str, Any]:
    """Get default configuration from environment."""
    fleet_id_str = os.environ.get("SANJAYA_DEFAULT_FLEET_ID", "")
    fleet_id = int(fleet_id_str) if fleet_id_str and fleet_id_str.isdigit() else None
    return {
        "fm_client_name": os.environ.get("SANJAYA_DEFAULT_CLIENT", ""),
        "fleet_name": os.environ.get("SANJAYA_DEFAULT_FLEET", ""),
        "fleet_id": fleet_id,
        "timezone": os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata"),
        "time_phrase": os.environ.get("SANJAYA_DEFAULT_TIME", "today"),
    }


async def _resolve_sherpa_names_for_fleet(client_name: str, fleet_name: str) -> Optional[List[str]]:
    """Resolve client+fleet to list of sherpa names (same as get_metric). Returns None if unresolved."""
    all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
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
    all_sherpas = await sherpa_cache.get_or_set(cache_key, client.get_sherpas_by_client_id, client_id)
    matching = [
        s for s in all_sherpas
        if isinstance(s, dict) and (s.get("fleet_name") or "").lower() == fleet_name.lower()
    ]
    names = [s.get("sherpa_name") for s in matching if s.get("sherpa_name")]
    return names if names else None


async def _fetch_analytics_data_and_summary(
    client_name: str, fleet_name: str, time_range: str, timezone: str
) -> tuple[str, Dict[str, Any], Dict[str, str]]:
    """Fetch basic_analytics and return (summary_text, data, time_strings). Uses same sherpa list as get_metric so report matches chat."""
    await client.ensure_token()
    tr = parse_time_range(time_range, timezone=timezone)
    time_strings = tr.to_strings()
    sherpa_names = await _resolve_sherpa_names_for_fleet(client_name, fleet_name)
    api_sherpa = sherpa_names if sherpa_names else None
    data = await client.basic_analytics(
        fm_client_name=client_name,
        start_time=time_strings["start_time"],
        end_time=time_strings["end_time"],
        timezone=timezone,
        fleet_name=fleet_name,
        status=["succeeded", "failed", "cancelled"],
        sherpa_name=api_sherpa,
    )
    # Unwrap so summary and report see the same structure (API may return { "data": { ... } })
    if isinstance(data.get("data"), dict):
        data = data["data"]
    summary = summarize_basic_analytics(data)
    summary_text = f"Analytics Summary for {fleet_name} ({time_range}):\n\n{summary}"
    return summary_text, data, time_strings


def _generate_and_send_report(
    data: Dict[str, Any],
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    time_strings: Dict[str, str],
) -> None:
    """Build PDF from analytics data and email to REPORT_RECIPIENT. Runs in same process (no separate script).
    sections_to_include: if set, only these sections are included (prompt-specific report); if None, all sections with data.
    """
    try:
        safe_name = (client_name + "_" + fleet_name).replace(" ", "-")[:50]
        pdf_filename = f"Analytics-Report-{safe_name}-{datetime.now().strftime('%Y-%m-%d')}.pdf"
        pdf_path = os.path.join(PROJECT_ROOT, pdf_filename)
        build_pdf(data, client_name, fleet_name, time_range, time_strings, pdf_path, report_dir=PROJECT_ROOT)
        subject = f"Analytics Report - {fleet_name} - {time_range}"
        send_report_email(pdf_path, subject, report_dir=PROJECT_ROOT)
        logger.info("Report generated and sent: %s", pdf_filename)
    except Exception as e:
        logger.warning("Report generation/send failed: %s", e)


def _generate_and_send_text_report(
    report_text: str,
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    time_strings: Dict[str, str],
) -> None:
    """Build PDF directly from the terminal/chat text and email it.

    This follows the \"single source of truth\" approach: whatever text we
    return to the client for a prompt is exactly what goes into the PDF.
    """
    try:
        safe_name = (client_name + "_" + fleet_name).replace(" ", "-")[:50]
        pdf_filename = f"Analytics-Report-{safe_name}-{datetime.now().strftime('%Y-%m-%d')}.pdf"
        pdf_path = os.path.join(PROJECT_ROOT, pdf_filename)
        build_pdf_from_text(
            report_text,
            client_name,
            fleet_name,
            time_range,
            time_strings,
            pdf_path,
            report_dir=PROJECT_ROOT,
        )
        subject = f"Analytics Report - {fleet_name} - {time_range}"
        send_report_email(pdf_path, subject, report_dir=PROJECT_ROOT)
        logger.info("Text-based report generated and sent: %s", pdf_filename)
    except Exception as e:
        logger.warning("Text-based report generation/send failed: %s", e)


# ============================================================================
# RESOURCES - Expose data to LLMs
# ============================================================================

@mcp.resource("sanjaya://clients")
async def list_clients() -> str:
    """List all available clients."""
    await client.ensure_token()
    clients = await client_cache.get_or_set("all_clients", client.get_clients)
    
    if not clients:
        return "No clients available."
    
    lines = ["Available Clients:"]
    for c in sorted(clients, key=lambda x: x.get("fm_client_name", "")):
        if isinstance(c, dict):
            client_name = c.get("fm_client_name", "Unknown")
            display_name = c.get("display_name", client_name)
            client_id = c.get("fm_client_id", "N/A")
            lines.append(f"  • {client_name} (ID: {client_id}, Display: {display_name})")
    
    return "\n".join(lines)


@mcp.resource("sanjaya://clients/{client_id}/fleets")
async def list_fleets_for_client(client_id: int) -> str:
    """List all fleets for a specific client."""
    await client.ensure_token()
    
    cache_key = f"client_details_{client_id}"
    client_details = await client_details_cache.get_or_set(
        cache_key, client.get_client_by_id, client_id
    )
    
    fleet_names = client_details.get("fm_fleet_names", [])
    client_name = client_details.get("fm_client_name", f"Client {client_id}")
    
    if not fleet_names:
        return f"No fleets found for client {client_name}."
    
    lines = [f"Fleets for {client_name}:"]
    for fleet in sorted(fleet_names):
        lines.append(f"  • {fleet}")
    
    return "\n".join(lines)


@mcp.resource("sanjaya://fleets/{fleet_id}/sherpas")
async def list_sherpas_for_fleet(fleet_id: int) -> str:
    """List all sherpas for a specific fleet."""
    await client.ensure_token()
    
    cache_key = f"sherpas_fleet_{fleet_id}"
    sherpas = await sherpa_cache.get_or_set(
        cache_key, client.get_sherpas_by_fleet_id, fleet_id
    )
    
    if not sherpas:
        return f"No sherpas found for fleet ID {fleet_id}."
    
    # Group by fleet name
    by_fleet: Dict[str, List[str]] = {}
    for s in sherpas:
        if isinstance(s, dict):
            fleet_name = s.get("fleet_name", "Unknown")
            sherpa_name = s.get("sherpa_name", "")
            if sherpa_name:
                by_fleet.setdefault(fleet_name, []).append(sherpa_name)
    
    lines = []
    for fleet_name, sherpa_list in sorted(by_fleet.items()):
        lines.append(f"\nFleet: {fleet_name}")
        for sherpa in sorted(sherpa_list):
            lines.append(f"  • {sherpa}")
    
    return "\n".join(lines) if lines else f"No sherpas found for fleet ID {fleet_id}."


# ============================================================================
# PROMPTS - Reusable query templates
# ============================================================================

@mcp.prompt()
def analytics_summary_query(
    client_name: str,
    fleet_name: str,
    time_range: str = "today"
) -> List[Dict[str, str]]:
    """Generate a prompt for getting analytics summary.
    
    Args:
        client_name: Name of the client
        fleet_name: Name of the fleet
        time_range: Time range (e.g., "today", "yesterday", "last week")
    """
    return [
        {
            "role": "user",
            "content": f"Get analytics summary for client {client_name} and fleet {fleet_name} for {time_range}."
        }
    ]


@mcp.prompt()
def metric_query(
    metric: str,
    client_name: str,
    fleet_name: str,
    sherpa_name: Optional[str] = None,
    time_range: str = "today"
) -> List[Dict[str, str]]:
    """Generate a prompt for getting a specific metric.
    
    Args:
        metric: Metric name (e.g., "total trips", "utilization", "takt time")
        client_name: Name of the client
        fleet_name: Name of the fleet
        sherpa_name: Optional sherpa name
        time_range: Time range (e.g., "today", "yesterday")
    """
    query = f"Get {metric} for client {client_name} and fleet {fleet_name}"
    if sherpa_name:
        query += f" for sherpa {sherpa_name}"
    query += f" for {time_range}."
    
    return [
        {
            "role": "user",
            "content": query
        }
    ]


# ============================================================================
# Helper Functions
# ============================================================================

async def _resolve_sherpa_for_fleet(
    fleet_id: int,
    fleet_name: str,
    sherpa_hint: Optional[str] = None,
) -> tuple[Optional[str | List[str]], str]:
    """Resolve sherpa name for a given fleet.
    
    NOTE: This function is NOT used in the main analytics query flow.
    The analytics API accepts fleet_name directly and returns all sherpas
    when sherpa_name="" is passed, so we don't need fleet_id for queries.
    This function is kept for potential future use by resources that have fleet_id.
    
    Args:
        fleet_id: The fleet ID to fetch sherpas for
        fleet_name: The fleet name (for filtering)
        sherpa_hint: Optional partial sherpa name to resolve (e.g., "tug-107")
        
    Returns:
        Tuple of (resolved_sherpa_name, actual_fleet_name)
        - resolved_sherpa_name: Full sherpa name, list of sherpa names, or None
        - actual_fleet_name: The actual fleet name from API (correct case)
    """
    try:
        # Use cached sherpa list
        cache_key = f"sherpas_fleet_{fleet_id}"
        all_sherpas = await sherpa_cache.get_or_set(
            cache_key, client.get_sherpas_by_fleet_id, fleet_id
        )
        
        # Filter sherpas by fleet_name to match the requested fleet (case-insensitive)
        matching_sherpas = [
            s for s in all_sherpas 
            if isinstance(s, dict) and s.get("fleet_name", "").lower() == fleet_name.lower()
        ]
        
        if not matching_sherpas:
            logger.warning(f"No matching sherpas found for fleet_name: {fleet_name}")
            return None, fleet_name
        
        # Use the actual fleet_name from the API response (correct case)
        actual_fleet_name = matching_sherpas[0].get("fleet_name", fleet_name)
        
        # If sherpa_hint provided, try to resolve it to a full name
        if sherpa_hint:
            # Check if it's a partial name (e.g., "tug-107")
            if "-" in sherpa_hint and len(sherpa_hint.split("-")) <= 3:
                for s in matching_sherpas:
                    full_name = s.get("sherpa_name", "").lower()
                    if full_name == sherpa_hint.lower() or full_name.startswith(sherpa_hint.lower()):
                        resolved_name = s.get("sherpa_name")  # Use full name
                        logger.debug(f"Resolved '{sherpa_hint}' to full name '{resolved_name}'")
                        return resolved_name, actual_fleet_name
            else:
                # Try exact match
                for s in matching_sherpas:
                    if s.get("sherpa_name", "").lower() == sherpa_hint.lower():
                        return s.get("sherpa_name"), actual_fleet_name
        
        # No sherpa hint - return all sherpa names for the fleet as a list
        sherpa_names_list = [s.get("sherpa_name") for s in matching_sherpas if s.get("sherpa_name")]
        logger.debug(f"Found {len(sherpa_names_list)} sherpas for fleet {actual_fleet_name}")
        return sherpa_names_list, actual_fleet_name
        
    except Exception as e:
        logger.warning(f"Failed to fetch sherpas for fleet_id {fleet_id}: {e}")
        return None, fleet_name


# ============================================================================
# TOOLS - Focused, composable actions
# ============================================================================

@mcp.tool()
async def get_analytics_summary(
    client_name: str,
    fleet_name: str,
    time_range: str = "today",
    timezone: Optional[str] = None
) -> str:
    """Get a comprehensive analytics summary for a fleet.
    
    Args:
        client_name: Name of the client (e.g., "ceat-nagpur")
        fleet_name: Name of the fleet (e.g., "CEAT-Nagpur-North-Plant")
        time_range: Time range (e.g., "today", "yesterday", "last week", "10th Jan 2026")
        timezone: Timezone (default: "Asia/Kolkata")
    """
    # Ensure timezone always has a valid value
    if not timezone:
        timezone = "Asia/Kolkata"
    
    logger.info("get_analytics_summary called: client=%s, fleet=%s, time_range=%s, timezone=%s",
                client_name, fleet_name, time_range, timezone)
    try:
        summary_text, _data, _time_strings = await _fetch_analytics_data_and_summary(
            client_name, fleet_name, time_range, timezone
        )
        return summary_text
    except Exception as e:
        logger.error(f"Error getting analytics summary: {e}")
        return f"Error: {str(e)}"


# Maps metric name → (dict key that holds the numeric value, display unit suffix)
_SHERPA_LIST_VALUE_KEY: Dict[str, tuple] = {
    "uptime":                  ("uptime_percentage",    "%"),
    "uptime_percentage":       ("uptime_percentage",    "%"),
    "availability":            ("availability_percentage", "%"),
    "availability_percentage": ("availability_percentage", "%"),
    "utilization":             ("utilization",          "%"),
    "battery":                 ("battery_level",        "%"),
    "battery_level":           ("battery_level",        "%"),
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


def _format_metric_value(metric: str, val: Any, note: str) -> str:
    """Format a metric value for the terminal / PDF.  Converts per-sherpa lists to a table."""
    label = _METRIC_DISPLAY_LABEL.get(metric, metric.replace("_", " ").title())

    if isinstance(val, list) and val and isinstance(val[0], dict):
        key, unit = _SHERPA_LIST_VALUE_KEY.get(metric, (None, ""))
        col_w = 44  # sherpa column width
        lines = [
            f"{label}:",
            f"  {'Sherpa':<{col_w}} {'Value':>8}",
            f"  {'-' * col_w} {'------':>8}",
        ]
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
            lines.append(f"  {sherpa:<{col_w}} {v_str:>8}")
        result = "\n".join(lines)
    else:
        result = f"{label}: {val}"

    if note:
        result += f"\nNote: {note}"
    return result


async def _get_metric_response_and_data(
    metric: str,
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    sherpa_name: Optional[str] = None,
) -> tuple[str, Dict[str, Any], Dict[str, str]]:
    """Internal: same logic as get_metric but returns (response_text, data, time_strings) so report can use same data."""
    await client.ensure_token()
    tr = parse_time_range(time_range, timezone=timezone)
    time_strings = tr.to_strings()
    api_sherpa_name = sherpa_name
    if isinstance(api_sherpa_name, str) and api_sherpa_name.lower() in ("null", "none", ""):
        api_sherpa_name = None

    if api_sherpa_name is None:
        try:
            all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
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
                all_sherpas = await sherpa_cache.get_or_set(cache_key, client.get_sherpas_by_client_id, client_id)
                matching_sherpas = [
                    s for s in all_sherpas
                    if isinstance(s, dict) and (s.get("fleet_name") or "").lower() == fleet_name.lower()
                ]
                if matching_sherpas:
                    sherpa_names = [s.get("sherpa_name") for s in matching_sherpas if s.get("sherpa_name")]
                    if sherpa_names:
                        api_sherpa_name = sherpa_names
                        logger.info(f"Querying all {len(sherpa_names)} sherpas for client {client_name}, fleet {fleet_name}")
                    else:
                        api_sherpa_name = ""
                else:
                    api_sherpa_name = ""
            else:
                api_sherpa_name = ""
        except Exception as e:
            logger.error(f"Error fetching sherpas for client {client_name}: {e}")
            api_sherpa_name = ""

    if metric in ROUTE_ANALYTICS_ITEMS:
        data = await client.route_analytics(
            fm_client_name=client_name,
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=timezone,
            fleet_name=fleet_name,
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=api_sherpa_name,
        )
    else:
        data = await client.basic_analytics(
            fm_client_name=client_name,
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=timezone,
            fleet_name=fleet_name,
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=api_sherpa_name,
        )
        # Log API response shape so we can see if report gets the same data as chat
        st = data.get("sherpa_wise_trips") if isinstance(data, dict) else None
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            st = data["data"].get("sherpa_wise_trips")
        logger.info(
            "basic_analytics response: total_trips=%s, sherpa_wise_trips len=%s, sherpa_name param was list=%s",
            data.get("total_trips") if isinstance(data, dict) else "?",
            len(st) if isinstance(st, list) else 0,
            isinstance(api_sherpa_name, list),
        )

    extraction_sherpa_hint = None if api_sherpa_name == "" else (api_sherpa_name if isinstance(api_sherpa_name, str) else None)
    val, note = extract_item_value(data, item=metric, sherpa_hint=extraction_sherpa_hint)
    result = _format_metric_value(metric, val, note)
    return result, data, time_strings


@mcp.tool()
async def get_metric(
    metric: str,
    client_name: str,
    fleet_name: str,
    time_range: str = "today",
    timezone: Optional[str] = None,
    sherpa_name: Optional[str] = None
) -> str:
    """Get a specific metric value for a fleet or sherpa.
    
    Args:
        metric: Metric name (e.g., "total_trips", "utilization", "takt_time", "uptime")
        client_name: Name of the client
        fleet_name: Name of the fleet
        time_range: Time range (e.g., "today", "yesterday")
        timezone: Timezone (default: "Asia/Kolkata")
        sherpa_name: Optional sherpa name for sherpa-specific metrics
    """
    if not timezone:
        timezone = "Asia/Kolkata"
    logger.info("get_metric called: metric=%s, client=%s, fleet=%s, time_range=%s, timezone=%s, sherpa=%s",
                metric, client_name, fleet_name, time_range, timezone, sherpa_name)
    try:
        result, _data, _time_strings = await _get_metric_response_and_data(
            metric, client_name, fleet_name, time_range, timezone, sherpa_name
        )
        return result
    except Exception as e:
        logger.error(f"Error getting metric {metric}: {e}")
        return f"Error: {str(e)}"


@mcp.tool()
async def resolve_client_name(partial_name: str) -> str:
    """Resolve a partial client name to the full client name.
    
    Args:
        partial_name: Partial client name (e.g., "nagpur" -> "ceat-nagpur")
    """
    try:
        await client.ensure_token()
        clients = await client_cache.get_or_set("all_clients", client.get_clients)
        
        partial_lower = partial_name.lower().strip()
        matches = []
        
        for c in clients:
            if not isinstance(c, dict):
                continue
            client_name = c.get("fm_client_name", "")
            if not client_name:
                continue
            
            client_lower = client_name.lower()
            if client_lower == partial_lower:
                return client_name
            if client_lower.startswith(partial_lower) or partial_lower in client_lower:
                matches.append(client_name)
        
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            prefix_matches = [m for m in matches if m.lower().startswith(partial_lower)]
            if len(prefix_matches) == 1:
                return prefix_matches[0]
            return f"Multiple matches found: {', '.join(matches[:5])}"
        
        return f"No client found matching '{partial_name}'"
    
    except Exception as e:
        logger.error(f"Error resolving client name: {e}")
        return f"Error: {str(e)}"


@mcp.tool()
async def resolve_fleet_name(
    partial_fleet_name: str,
    client_name: str
) -> str:
    """Resolve a partial fleet name to the full fleet name for a client.
    
    Args:
        partial_fleet_name: Partial fleet name (e.g., "north" -> "CEAT-Nagpur-North-Plant")
        client_name: Full client name
    """
    try:
        await client.ensure_token()
        
        # Get client ID
        clients = await client_cache.get_or_set("all_clients", client.get_clients)
        client_id = None
        for c in clients:
            if isinstance(c, dict) and c.get("fm_client_name", "").lower() == client_name.lower():
                client_id = c.get("fm_client_id")
                break
        
        if not client_id:
            return f"Client '{client_name}' not found"
        
        # Get client details
        cache_key = f"client_details_{client_id}"
        client_details = await client_details_cache.get_or_set(
            cache_key, client.get_client_by_id, client_id
        )
        
        fleet_names = client_details.get("fm_fleet_names", [])
        partial_lower = partial_fleet_name.lower().strip()
        matches = []
        
        for fleet_name in fleet_names:
            if not fleet_name:
                continue
            fleet_lower = fleet_name.lower()
            if fleet_lower == partial_lower:
                return fleet_name
            if fleet_lower.startswith(partial_lower) or partial_lower in fleet_lower:
                matches.append(fleet_name)
        
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            prefix_matches = [m for m in matches if m.lower().startswith(partial_lower)]
            if len(prefix_matches) == 1:
                return prefix_matches[0]
            return f"Multiple matches found: {', '.join(matches[:5])}"
        
        return f"No fleet found matching '{partial_fleet_name}' for client '{client_name}'"
    
    except Exception as e:
        logger.error(f"Error resolving fleet name: {e}")
        return f"Error: {str(e)}"


def _item_to_section_names(item: Optional[str]) -> Optional[list]:
    """Map an NLU item to a list of section name strings for client_report_config.json.
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
    # All route analytics items
    return ["route_analytics"]


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


def _parse_schedule_command(text: str) -> Optional[dict]:
    """Parse a schedule preference string from the user.

    Returns:
      None                                  → skip (don't schedule)
      {"schedule_type": "hourly"}           → every hour
      {"schedule_type": "daily",  "run_hour": int, "run_day": None}
      {"schedule_type": "weekly", "run_hour": int, "run_day": int}
      {"error": "<message>"}                → couldn't parse, ask again
    """
    import re
    t = text.strip().lower()

    # Skip
    if t in ("skip", "no", "no schedule", "don't schedule", "dont schedule", "none", "not now"):
        return None

    # Extract first number present (treated as hour)
    hour_match = re.search(r"\b(\d{1,2})\b", t)
    hour = max(0, min(23, int(hour_match.group(1)))) if hour_match else datetime.now().hour

    # Weekly: must contain a day name
    for day_name, day_idx in _DAY_MAP.items():
        if day_name in t:
            return {"schedule_type": "weekly", "run_hour": hour, "run_day": day_idx}

    # Every 20 minutes (testing cadence)
    if re.search(r"every\s+20\s*min", t) or "20 min" in t or "20min" in t:
        return {"schedule_type": "every_20min", "run_hour": None, "run_day": None}

    # Hourly
    if any(kw in t for kw in ("hourly", "every hour", "each hour", "per hour")):
        return {"schedule_type": "hourly", "run_hour": None, "run_day": None}

    # Daily
    if any(kw in t for kw in ("daily", "every day", "each day", "per day")):
        return {"schedule_type": "daily", "run_hour": hour, "run_day": None}

    # Bare number → daily at that hour
    if re.fullmatch(r"\s*\d{1,2}\s*", t):
        return {"schedule_type": "daily", "run_hour": hour, "run_day": None}

    return {"error": f"Couldn't understand '{text}'. Try: `daily 8`, `hourly`, `weekly monday 8`, or `skip`."}


# Fallback time_phrase when user's query had no time range (cadence defines the window)
_SCHEDULE_TIME_PHRASE = {
    "every_20min": "last 20 minutes",
    "hourly": "last hour",
    "daily": "yesterday",
    "weekly": "last week",
}


def _save_pending_report(
    client_name: str, fleet_name: str, time_phrase: str,
    timezone: str, time_strings: dict, report_text: str, prompt_text: str,
    sections: Optional[list] = None,
) -> None:
    import json as _json
    data = {
        "client_name": client_name,
        "fleet_name": fleet_name,
        "time_phrase": time_phrase,
        "timezone": timezone,
        "time_strings": time_strings,
        "report_text": report_text,
        "prompt_text": prompt_text,
        "sections": sections,  # list of section names, or None = full report
        "saved_at": datetime.now().isoformat(),
    }
    with open(PENDING_REPORT_FILE, "w", encoding="utf-8") as f:
        _json.dump(data, f, indent=2)


def _load_pending_report() -> Optional[dict]:
    import json as _json
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


def _save_pending_schedule(
    client_name: str, fleet_name: str, sections: Optional[list],
    time_phrase: str, timezone: str,
) -> None:
    import json as _json
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
    import json as _json
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

    schedule_type: "hourly" | "daily" | "weekly"
    run_hour: 0-23 for daily/weekly; None for hourly
    run_day:  0=Monday … 6=Sunday for weekly; None otherwise
    sections: list of section name strings; [] or None = full report
    """
    import json as _json

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


# ============================================================================
# Optional: Simple natural language interface (returns plain text)
# ============================================================================

@mcp.tool()
async def sanjaya_chat(text: str) -> str:
    """Simple natural language query interface.
    
    This is a convenience wrapper that parses natural language and calls the appropriate tool.
    For better control, use the focused tools directly: get_analytics_summary and get_metric.
    
    Args:
        text: Natural language query (e.g., "total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant")
    
    Returns:
        Plain text response with the result
    """
    logger.info("sanjaya_chat called with query: %s", text)
    defaults = _defaults()

    # ── Proceed / Cancel ────────────────────────────────────────────────────
    cmd = text.strip().lower()
    if cmd in ("proceed", "yes", "proceed to email", "send", "confirm"):
        pending = _load_pending_report()
        if not pending:
            return "No pending report found. Please request a report first."
        try:
            _generate_and_send_text_report(
                pending["report_text"],
                pending["client_name"],
                pending["fleet_name"],
                pending["time_phrase"],
                pending["timezone"],
                pending["time_strings"],
            )
            recipient = os.environ.get("REPORT_RECIPIENT", "")
            # Save state for the scheduling dialog (next turn)
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

    # ── Schedule response (after email was sent) ─────────────────────────────
    pending_sched = _load_pending_schedule()
    if pending_sched:
        schedule = _parse_schedule_command(text.strip())

        # User wants to skip scheduling
        if schedule is None:
            _clear_pending_schedule()
            return "OK — report was sent once, no recurring schedule added."

        # Couldn't parse → ask again, leave pending_schedule intact
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

        # Use the time_phrase from the original chat query; cadence only controls *when* to run
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

            # Build human-readable description
            if sched_type == "every_20min":
                cadence_desc = "every 20 minutes [testing mode]"
            elif sched_type == "hourly":
                cadence_desc = "every hour"
            elif sched_type == "daily":
                h = schedule.get("run_hour", datetime.now().hour)
                cadence_desc = f"every day at {h:02d}:00"
            else:  # weekly
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
    # ────────────────────────────────────────────────────────────────────────

    try:
        clean_text = text.replace("[sherpa:all]", "").strip()
        empty_defaults = {
            "fm_client_name": "",
            "fleet_name": "",
            "timezone": defaults.get("timezone", "Asia/Kolkata"),
            "time_phrase": defaults.get("time_phrase", "today"),
        }
        pq = await parse_query(clean_text, defaults=empty_defaults)
        # If user clearly asked for one metric but NLU returned full summary, override so terminal and report show only what was asked
        t_lower = clean_text.lower()
        if pq.intent == "basic_analytics" and pq.item in (None, ""):
            if "total trips" in t_lower and "summary" not in t_lower and "analytics summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "total_trips"
            elif ("total distance" in t_lower or ("distance" in t_lower and "total" in t_lower)) and "summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "total_distance_km"
            elif ("sherpa status" in t_lower or ("status" in t_lower and ("sherpa" in t_lower or "tug" in t_lower))) and "summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "sherpa_status"
        elif pq.intent == "basic_analytics" and pq.item:
            # NLU already identified a specific item but labelled the intent wrong —
            # treat it as a single-metric request so only that section is shown.
            pq.intent = "basic_analytics_item"
        logger.info("Parsed query: intent=%s, item=%s, client=%s, fleet=%s, time_phrase=%s, sherpa_hint=%s",
                    pq.intent, pq.item, pq.fm_client_name, pq.fleet_name, pq.time_phrase, pq.sherpa_hint)
        
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
        
        # Apply defaults - ensure timezone always has a valid value
        client_name = pq.fm_client_name or defaults.get("fm_client_name")
        # Only fall back to the default fleet when the client also came from defaults.
        # If the user explicitly named a client, leave fleet empty so we can auto-resolve it below.
        fleet_name = pq.fleet_name
        if not fleet_name and not pq.fm_client_name:
            fleet_name = defaults.get("fleet_name")
        time_phrase = pq.time_phrase or defaults.get("time_phrase", "today")
        # Handle None, "null", "None" strings from NLU
        timezone = pq.timezone
        if not timezone or str(timezone).lower() in ("null", "none", ""):
            timezone = defaults.get("timezone") or "Asia/Kolkata"
        else:
            timezone = str(timezone)

        # ── Client/fleet swap correction ──────────────────────────────────────
        # Ollama sometimes puts the client name in fleet_name and leaves client empty.
        # Detect this by checking if fleet_name matches a known client in the API.
        try:
            await client.ensure_token()
            all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
            if not client_name and fleet_name:
                for c in all_clients:
                    if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == fleet_name.lower():
                        logger.info("Correcting NLU swap: treating fleet_name '%s' as client_name", fleet_name)
                        client_name = c.get("fm_client_name")
                        fleet_name = None  # will be auto-resolved below
                        break
            # Normalize client name case
            if client_name:
                for c in all_clients:
                    if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == client_name.lower():
                        client_name = c.get("fm_client_name")
                        break
        except Exception:
            pass

        # ── Fleet auto-resolution ─────────────────────────────────────────────
        # If fleet is still empty but we know the client, look it up from the API.
        if client_name and not fleet_name:
            try:
                all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
                client_id = None
                for c in all_clients:
                    if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == client_name.lower():
                        client_id = c.get("fm_client_id")
                        break
                if client_id:
                    cache_key = f"client_details_{client_id}"
                    client_details = await client_details_cache.get_or_set(
                        cache_key, client.get_client_by_id, client_id
                    )
                    fleet_names = client_details.get("fm_fleet_names", [])
                    if len(fleet_names) == 1:
                        fleet_name = fleet_names[0]
                        logger.info("Auto-resolved fleet for client %s: %s", client_name, fleet_name)
                    elif fleet_names:
                        # Check client_report_config.json for a known fleet for this client
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
                            # Multiple fleets and no config hint — ask user
                            return (
                                f"Client **{client_name}** has multiple fleets: "
                                + ", ".join(f"`{f}`" for f in fleet_names)
                                + ". Please specify the fleet in your query."
                            )
            except Exception as e:
                logger.warning("Fleet auto-resolve failed: %s", e)

        if not client_name or not fleet_name:
            return "Error: Client name and fleet name are required. Use resolve_client_name and resolve_fleet_name tools to find them, or specify in your query."
        
        if pq.intent == "basic_analytics_item" and pq.item:
            # Handle None, "null", "None" strings from NLU
            api_sherpa_name = pq.sherpa_hint
            if isinstance(api_sherpa_name, str) and api_sherpa_name.lower() in ("null", "none", ""):
                api_sherpa_name = None
            # Single API call: get response and data; report is built from the same text we return to the client
            metric_response, data, time_strings = await _get_metric_response_and_data(
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
        else:
            # Fetch once: return summary to user and wait for confirmation before emailing
            summary_text, data, time_strings = await _fetch_analytics_data_and_summary(
                client_name, fleet_name, time_phrase, timezone
            )
            _save_pending_report(
                client_name, fleet_name, time_phrase, timezone,
                time_strings, summary_text, clean_text,
                sections=None,  # full report
            )
            return summary_text + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."
    
    except Exception as e:
        logger.error(f"Error in sanjaya_chat: {e}")
        return f"Error: {str(e)}"


if __name__ == "__main__":
    logger.info("Starting Sanjaya Analytics MCP Server (standard MCP patterns)...")
    mcp.run()

