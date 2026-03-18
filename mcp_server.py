#!/usr/bin/env python3
"""Standard MCP server implementation for Sanjaya Analytics.

This follows MCP best practices:
- Focused, composable tools
- Resources for data exposure
- Prompts for common patterns
- Standard MCP content types
"""

from __future__ import annotations

import json as _json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

# Ensure project root is on path so "src" is found when run from any cwd or on server
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

from src.logging_config import setup_logging
from src.sanjaya_client import SanjayaAPI
from src.cache import TTLCache
from src.report_builder import build_pdf
from src.chat_handler import handle_chat
from src.analytics import (
    ROUTE_ANALYTICS_ITEMS as _ROUTE_ANALYTICS_ITEMS,
    resolve_sherpa_names_for_fleet as _resolve_sherpa_names_raw,
    fetch_analytics_data_and_summary as _fetch_analytics_raw,
    get_metric_response_and_data as _get_metric_raw,
    generate_and_send_text_report as _generate_and_send_text_raw,
)

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
# Disable DNS rebinding protection so Docker containers can connect by service name
mcp.settings.transport_security.enable_dns_rebinding_protection = False

BASE_URL = "https://sanjaya.atimotors.com"

ROUTE_ANALYTICS_ITEMS = _ROUTE_ANALYTICS_ITEMS
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
    return await _resolve_sherpa_names_raw(
        client_name, fleet_name,
        api=client, client_cache=client_cache, sherpa_cache=sherpa_cache,
    )


async def _fetch_analytics_data_and_summary(
    client_name: str, fleet_name: str, time_range: str, timezone: str,
    selected_sherpas: Optional[List[str]] = None,
) -> tuple[str, Dict[str, Any], Dict[str, str]]:
    return await _fetch_analytics_raw(
        client_name, fleet_name, time_range, timezone,
        api=client, client_cache=client_cache, sherpa_cache=sherpa_cache,
        selected_sherpas=selected_sherpas,
    )


def _generate_and_send_report(
    data: Dict[str, Any],
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    time_strings: Dict[str, str],
) -> None:
    """Build PDF from raw analytics data and email it."""
    from src.report_builder import send_report_email
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
    return _generate_and_send_text_raw(
        report_text, client_name, fleet_name, time_range, timezone, time_strings,
        project_root=PROJECT_ROOT,
    )


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


async def _get_metric_response_and_data(
    metric: str,
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    sherpa_name: Optional[str] = None,
    selected_sherpas: Optional[List[str]] = None,
) -> tuple[str, Dict[str, Any], Dict[str, str]]:
    return await _get_metric_raw(
        metric, client_name, fleet_name, time_range, timezone,
        api=client, client_cache=client_cache, sherpa_cache=sherpa_cache,
        sherpa_name=sherpa_name,
        selected_sherpas=selected_sherpas,
    )


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
# List tools — return JSON arrays for programmatic clients (e.g. Streamlit)
# ============================================================================

@mcp.tool()
async def list_clients_tool() -> str:
    """Return all client names as a JSON array.

    Example response: ["YOKOHAMA-DAHEJ", "CEAT-Nagpur"]
    """
    await client.ensure_token()
    clients = await client_cache.get_or_set("all_clients", client.get_clients)
    names = sorted(
        c.get("fm_client_name") for c in clients
        if isinstance(c, dict) and c.get("fm_client_name")
    )
    return _json.dumps(names)


@mcp.tool()
async def list_fleets_tool(client_name: str) -> str:
    """Return all fleet names for a client as a JSON array.

    Args:
        client_name: Exact client name (e.g., "YOKOHAMA-DAHEJ")
    """
    await client.ensure_token()
    clients = await client_cache.get_or_set("all_clients", client.get_clients)
    client_id = next(
        (c.get("fm_client_id") for c in clients
         if isinstance(c, dict)
         and (c.get("fm_client_name") or "").lower() == client_name.lower()),
        None,
    )
    if not client_id:
        return _json.dumps([])
    details = await client_details_cache.get_or_set(
        f"client_details_{client_id}", client.get_client_by_id, client_id
    )
    return _json.dumps(sorted(details.get("fm_fleet_names", [])))


@mcp.tool()
async def list_sherpas_tool(client_name: str, fleet_names: List[str]) -> str:
    """Return all sherpa names for a client across the given fleets as a JSON array.

    Args:
        client_name: Exact client name
        fleet_names: List of fleet names to include
    """
    await client.ensure_token()
    clients = await client_cache.get_or_set("all_clients", client.get_clients)
    client_id = next(
        (c.get("fm_client_id") for c in clients
         if isinstance(c, dict)
         and (c.get("fm_client_name") or "").lower() == client_name.lower()),
        None,
    )
    if not client_id:
        return _json.dumps([])
    cache_key = f"sherpas_client_{client_id}"
    all_sherpas = await sherpa_cache.get_or_set(
        cache_key, client.get_sherpas_by_client_id, client_id
    )
    fleet_lower = {f.lower() for f in fleet_names}
    names = sorted(set(
        s.get("sherpa_name") for s in all_sherpas
        if isinstance(s, dict)
        and (s.get("fleet_name") or "").lower() in fleet_lower
        and s.get("sherpa_name")
    ))
    return _json.dumps(names)


# ============================================================================
# Multi-fleet helper (used by sanjaya_chat when fleet_names has 2+ entries)
# ============================================================================

async def _handle_multi_fleet_chat(
    text: str,
    client_name: str,
    fleet_names: List[str],
    time_phrase: str,
    timezone: str,
    selected_sherpas: Optional[List[str]] = None,
) -> str:
    from src.nlu import parse_query as _nlu_parse
    from src.scheduling import _save_pending_report, _item_to_section_names

    nlu_defaults = {
        "fm_client_name": "", "fleet_name": "",
        "timezone": timezone, "time_phrase": time_phrase,
    }
    try:
        pq = await _nlu_parse(text, defaults=nlu_defaults)
    except Exception:
        pq = None

    fleet_parts, time_strings = [], {}
    for fleet in fleet_names:
        try:
            if pq and pq.intent == "multi_metric" and len(pq.items) > 1:
                parts = []
                for metric in pq.items:
                    t, _, ts = await _get_metric_response_and_data(
                        metric, client_name, fleet, time_phrase, timezone,
                        selected_sherpas=selected_sherpas,
                    )
                    parts.append(t)
                    time_strings = ts
                fleet_text = "\n\n".join(parts)
            elif pq and pq.intent == "basic_analytics_item" and pq.item:
                fleet_text, _, time_strings = await _get_metric_response_and_data(
                    pq.item, client_name, fleet, time_phrase, timezone,
                    selected_sherpas=selected_sherpas,
                )
            else:
                fleet_text, _, time_strings = await _fetch_analytics_data_and_summary(
                    client_name, fleet, time_phrase, timezone, selected_sherpas
                )
        except Exception as e:
            fleet_text = f"Error fetching data for {fleet}: {e}"

        fleet_parts.append(f"**Fleet: {fleet}**\n\n{fleet_text}")

    combined = "\n\n---\n\n".join(fleet_parts)

    # Compute sections from NLU result
    if pq and pq.intent == "multi_metric" and pq.items:
        sections: Optional[List[str]] = []
        for item in pq.items:
            for sec in (_item_to_section_names(item) or []):
                if sec not in sections:
                    sections.append(sec)
        sections = sections or None
    elif pq and pq.intent == "basic_analytics_item" and pq.item:
        sections = _item_to_section_names(pq.item) or None
    else:
        sections = None

    _save_pending_report(
        client_name, ", ".join(fleet_names), time_phrase, timezone,
        time_strings, combined, text, sections=sections,
    )
    return combined + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."


# ============================================================================
# Natural language chat interface
# ============================================================================

@mcp.tool()
async def sanjaya_chat(
    text: str,
    client_name: Optional[str] = None,
    fleet_name: Optional[str] = None,
    fleet_names: Optional[List[str]] = None,
    sherpa_name: Optional[str] = None,
    sherpa_names: Optional[List[str]] = None,
    time_phrase: Optional[str] = None,
    timezone: Optional[str] = None,
    recipient_email: Optional[str] = None,
) -> str:
    """Natural language chat interface — single entry point for all queries.

    Handles single/multi-fleet, single/multi-metric, proceed/cancel, and scheduling.
    Sidebar context (client, fleet, sherpa, time) passed as optional params overrides env defaults.

    Args:
        text:            Natural language query or control command (proceed, cancel, daily 8, …)
        client_name:     Client to query (overrides SANJAYA_DEFAULT_CLIENT env var)
        fleet_name:      Single fleet (overrides SANJAYA_DEFAULT_FLEET env var)
        fleet_names:     Multiple fleets — triggers per-fleet fan-out when 2+ provided
        sherpa_name:     Filter to a specific sherpa (overrides NLU extraction)
        time_phrase:     Time range string (e.g. "yesterday", "last 7 days")
        timezone:        IANA timezone (e.g. "Asia/Kolkata")
        recipient_email: Override REPORT_RECIPIENT for this request only
    """
    d = _defaults()
    if client_name:
        d["fm_client_name"] = client_name
        # Sidebar selected a specific client but no fleet → clear the env default fleet
        # so handle_chat's auto-resolution fetches all fleets for this client instead.
        if not fleet_name and not fleet_names:
            d["fleet_name"] = ""
    if fleet_name:
        d["fleet_name"] = fleet_name
    elif fleet_names and len(fleet_names) == 1:
        d["fleet_name"] = fleet_names[0]
    if sherpa_name:
        d["sherpa_hint"] = sherpa_name
    if sherpa_names:
        d["selected_sherpas"] = sherpa_names
    if time_phrase:
        d["time_phrase"] = time_phrase
    if timezone:
        d["timezone"] = timezone

    # Temporarily override email recipient if provided
    _old_recipient = os.environ.get("REPORT_RECIPIENT", "")
    if recipient_email:
        os.environ["REPORT_RECIPIENT"] = recipient_email

    try:
        # Multi-fleet: fan out per fleet on the server side
        if fleet_names and len(fleet_names) > 1:
            resolved_tz     = d.get("timezone", "Asia/Kolkata")
            resolved_time   = d.get("time_phrase", "yesterday")
            resolved_client = d.get("fm_client_name", "")
            return await _handle_multi_fleet_chat(
                text, resolved_client, fleet_names, resolved_time, resolved_tz,
                selected_sherpas=d.get("selected_sherpas") or None,
            )

        return await handle_chat(
            text,
            api=client,
            client_cache=client_cache,
            client_details_cache=client_details_cache,
            sherpa_cache=sherpa_cache,
            project_root=_PROJECT_ROOT,
            defaults=d,
            get_metric_data_fn=_get_metric_response_and_data,
            fetch_analytics_fn=_fetch_analytics_data_and_summary,
            send_text_report_fn=_generate_and_send_text_report,
        )
    finally:
        os.environ["REPORT_RECIPIENT"] = _old_recipient


if __name__ == "__main__":
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if len(sys.argv) > 1 and sys.argv[1] in ("stdio", "sse", "streamable-http"):
        transport = sys.argv[1]
    logger.info("Starting Sanjaya Analytics MCP Server (transport=%s)...", transport)
    if transport == "sse":
        import uvicorn
        uvicorn.run(mcp.sse_app(), host="0.0.0.0", port=8000)
    elif transport == "streamable-http":
        import uvicorn
        uvicorn.run(mcp.streamable_http_app(), host="0.0.0.0", port=8000)
    else:
        mcp.run()

