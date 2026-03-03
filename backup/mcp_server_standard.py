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
from typing import Any, Dict, List, Optional

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

setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("fm_mcp")

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

# Initialize API client
client = SanjayaAPI(BASE_URL)


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
# TOOLS - Focused, composable actions
# ============================================================================

@mcp.tool()
async def get_analytics_summary(
    client_name: str,
    fleet_name: str,
    time_range: str = "today",
    timezone: str = "Asia/Kolkata"
) -> str:
    """Get a comprehensive analytics summary for a fleet.
    
    Args:
        client_name: Name of the client (e.g., "ceat-nagpur")
        fleet_name: Name of the fleet (e.g., "CEAT-Nagpur-North-Plant")
        time_range: Time range (e.g., "today", "yesterday", "last week", "10th Jan 2026")
        timezone: Timezone (default: "Asia/Kolkata")
    """
    try:
        await client.ensure_token()
        
        # Parse time range
        tr = parse_time_range(time_range, timezone=timezone)
        time_strings = tr.to_strings()
        
        # Get analytics data
        data = await client.basic_analytics(
            fm_client_name=client_name,
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=timezone,
            fleet_name=fleet_name,
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=None,
        )
        
        summary = summarize_basic_analytics(data)
        return f"Analytics Summary for {fleet_name} ({time_range}):\n\n{summary}"
    
    except Exception as e:
        logger.error(f"Error getting analytics summary: {e}")
        return f"Error: {str(e)}"


@mcp.tool()
async def get_metric(
    metric: str,
    client_name: str,
    fleet_name: str,
    time_range: str = "today",
    timezone: str = "Asia/Kolkata",
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
    try:
        await client.ensure_token()
        
        # Parse time range
        tr = parse_time_range(time_range, timezone=timezone)
        time_strings = tr.to_strings()
        
        # Determine which API to call
        if metric in ROUTE_ANALYTICS_ITEMS:
            data = await client.route_analytics(
                fm_client_name=client_name,
                start_time=time_strings["start_time"],
                end_time=time_strings["end_time"],
                timezone=timezone,
                fleet_name=fleet_name,
                status=["succeeded", "failed", "cancelled"],
                sherpa_name=sherpa_name,
            )
        else:
            data = await client.basic_analytics(
                fm_client_name=client_name,
                start_time=time_strings["start_time"],
                end_time=time_strings["end_time"],
                timezone=timezone,
                fleet_name=fleet_name,
                status=["succeeded", "failed", "cancelled"],
                sherpa_name=sherpa_name,
            )
        
        # Extract metric value
        val, note = extract_item_value(data, item=metric, sherpa_hint=sherpa_name)
        
        result = f"{metric}: {val}"
        if note:
            result += f"\nNote: {note}"
        
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
# Legacy compatibility - Keep sanjaya_chat for backward compatibility
# ============================================================================

@mcp.tool()
async def sanjaya_chat(text: str) -> str:
    """Natural language query interface (legacy compatibility).
    
    This tool provides backward compatibility with the original chat interface.
    For new integrations, prefer using the focused tools: get_analytics_summary and get_metric.
    
    Args:
        text: Natural language query (e.g., "total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant")
    """
    # Import the original implementation logic
    # This is a simplified version that calls the focused tools
    defaults = _defaults()
    
    try:
        clean_text = text.replace("[sherpa:all]", "").strip()
        empty_defaults = {
            "fm_client_name": "",
            "fleet_name": "",
            "timezone": defaults.get("timezone", "Asia/Kolkata"),
            "time_phrase": defaults.get("time_phrase", "today"),
        }
        pq = await parse_query(clean_text, defaults=empty_defaults)
        
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
        
        # Apply defaults
        client_name = pq.fm_client_name or defaults.get("fm_client_name")
        fleet_name = pq.fleet_name or defaults.get("fleet_name")
        time_phrase = pq.time_phrase or defaults.get("time_phrase", "today")
        timezone = pq.timezone or defaults.get("timezone", "Asia/Kolkata")
        
        if not client_name or not fleet_name:
            return "Error: Client name and fleet name are required. Use resolve_client_name and resolve_fleet_name tools to find them."
        
        if pq.intent == "basic_analytics_item" and pq.item:
            # Use get_metric
            return await get_metric(
                metric=pq.item,
                client_name=client_name,
                fleet_name=fleet_name,
                time_range=time_phrase,
                timezone=timezone,
                sherpa_name=pq.sherpa_hint
            )
        else:
            # Use get_analytics_summary
            return await get_analytics_summary(
                client_name=client_name,
                fleet_name=fleet_name,
                time_range=time_phrase,
                timezone=timezone
            )
    
    except Exception as e:
        logger.error(f"Error in sanjaya_chat: {e}")
        return f"Error: {str(e)}"


if __name__ == "__main__":
    logger.info("Starting Sanjaya Analytics MCP Server (standard MCP patterns)...")
    mcp.run()

