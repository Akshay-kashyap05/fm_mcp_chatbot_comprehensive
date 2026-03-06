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
from src.sanjaya_client import SanjayaAPI
from src.cache import TTLCache
from src.time_parse import parse_time_range
from src.formatting import summarize_basic_analytics, extract_item_value
from src.report_builder import build_pdf, build_pdf_from_text, send_report_email
from src.chat_handler import handle_chat

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



# ============================================================================
# Simple natural language interface (returns plain text)
# ============================================================================

@mcp.tool()
async def sanjaya_chat(text: str) -> str:
    """Simple natural language query interface.

    Parses natural language and runs the 3-turn flow: query → proceed/cancel → schedule.
    For programmatic access use the focused tools: get_analytics_summary and get_metric.

    Args:
        text: Natural language query (e.g., "total trips today for ceat-nagpur")
    """
    return await handle_chat(
        text,
        api=client,
        client_cache=client_cache,
        client_details_cache=client_details_cache,
        sherpa_cache=sherpa_cache,
        project_root=_PROJECT_ROOT,
        defaults=_defaults(),
        get_metric_data_fn=_get_metric_response_and_data,
        fetch_analytics_fn=_fetch_analytics_data_and_summary,
        send_text_report_fn=_generate_and_send_text_report,
    )


if __name__ == "__main__":
    logger.info("Starting Sanjaya Analytics MCP Server (standard MCP patterns)...")
    mcp.run()

