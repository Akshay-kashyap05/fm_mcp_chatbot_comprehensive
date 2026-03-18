"""MCP HTTP client for Sanjaya Analytics.

Streamlit (and any other non-stdio client) uses this module to call MCP tools
via the HTTP/SSE transport instead of importing SanjayaAPI or analytics.py directly.

The MCP server is the single gateway to Sanjaya's API — this client is how
everything outside the server talks to it.

Usage:
    import src.mcp_client as mcp_client

    # List data (for UI dropdowns)
    clients   = await mcp_client.list_clients(server_url=url)
    fleets    = await mcp_client.list_fleets("YOKOHAMA-DAHEJ", server_url=url)
    sherpas   = await mcp_client.list_sherpas("YOKOHAMA-DAHEJ", ["BEAD"], server_url=url)

    # Analytics queries (natural language)
    response  = await mcp_client.chat(
        "uptime and obstacle time for last 7 days",
        client_name="YOKOHAMA-DAHEJ",
        fleet_name="BEAD",
        time_phrase="last 7 days",
        server_url=url,
    )

    # Direct metric / summary calls (bypass NLU)
    summary   = await mcp_client.get_analytics_summary("YOKOHAMA-DAHEJ", "BEAD", "yesterday", tz)
    metric    = await mcp_client.get_metric("uptime", "YOKOHAMA-DAHEJ", "BEAD", "yesterday", tz)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, List, Optional

logger = logging.getLogger("mcp_client")

_DEFAULT_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8000/sse")


async def _call_tool(
    tool_name: str,
    arguments: dict,
    *,
    server_url: str = _DEFAULT_SERVER_URL,
) -> Any:
    """Call a tool on the MCP server and return the parsed result."""
    from mcp import ClientSession
    from mcp.client.sse import sse_client

    try:
        async with sse_client(server_url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, arguments)
                if not result.content:
                    return None
                raw = result.content[0]
                text = raw.text if hasattr(raw, "text") else str(raw)
                try:
                    return json.loads(text)
                except (json.JSONDecodeError, ValueError):
                    return text
    except BaseExceptionGroup as eg:
        # Unwrap anyio TaskGroup to surface the real error
        raise eg.exceptions[0] from None


# ── List tools (for UI dropdowns) ─────────────────────────────────────────────

async def list_clients(*, server_url: str = _DEFAULT_SERVER_URL) -> List[str]:
    """Return all client names."""
    result = await _call_tool("list_clients_tool", {}, server_url=server_url)
    return result if isinstance(result, list) else []


async def list_fleets(
    client_name: str,
    *,
    server_url: str = _DEFAULT_SERVER_URL,
) -> List[str]:
    """Return all fleet names for a client."""
    result = await _call_tool(
        "list_fleets_tool", {"client_name": client_name}, server_url=server_url
    )
    return result if isinstance(result, list) else []


async def list_sherpas(
    client_name: str,
    fleet_names: List[str],
    *,
    server_url: str = _DEFAULT_SERVER_URL,
) -> List[str]:
    """Return all sherpa names for a client across the given fleets."""
    result = await _call_tool(
        "list_sherpas_tool",
        {"client_name": client_name, "fleet_names": fleet_names},
        server_url=server_url,
    )
    return result if isinstance(result, list) else []


# ── Chat (natural language, all query types) ───────────────────────────────────

async def chat(
    text: str,
    *,
    client_name: str = "",
    fleet_name: str = "",
    fleet_names: Optional[List[str]] = None,
    sherpa_name: str = "",
    sherpa_names: Optional[List[str]] = None,
    time_phrase: str = "yesterday",
    timezone: str = "Asia/Kolkata",
    recipient_email: Optional[str] = None,
    server_url: str = _DEFAULT_SERVER_URL,
) -> str:
    """Send a natural language query or control command to the MCP server.

    The server handles: NLU parsing, single/multi-metric, single/multi-fleet,
    proceed/cancel, and scheduling. Sidebar context is passed as keyword args.
    """
    args: dict = {
        "text": text,
        "client_name": client_name,
        "fleet_name": fleet_name,
        "time_phrase": time_phrase,
        "timezone": timezone,
    }
    if fleet_names:
        args["fleet_names"] = fleet_names
    if sherpa_name:
        args["sherpa_name"] = sherpa_name
    if sherpa_names:
        args["sherpa_names"] = sherpa_names
    if recipient_email:
        args["recipient_email"] = recipient_email

    result = await _call_tool("sanjaya_chat", args, server_url=server_url)
    return str(result) if result is not None else "No response from server."


# ── Direct analytics calls (bypass NLU, for specific use cases) ───────────────

async def get_analytics_summary(
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    *,
    server_url: str = _DEFAULT_SERVER_URL,
) -> str:
    """Fetch a full analytics summary for a fleet (bypasses NLU)."""
    result = await _call_tool(
        "get_analytics_summary",
        {
            "client_name": client_name,
            "fleet_name": fleet_name,
            "time_range": time_range,
            "timezone": timezone,
        },
        server_url=server_url,
    )
    return str(result) if result is not None else ""


async def get_metric(
    metric: str,
    client_name: str,
    fleet_name: str,
    time_range: str,
    timezone: str,
    *,
    sherpa_name: Optional[str] = None,
    server_url: str = _DEFAULT_SERVER_URL,
) -> str:
    """Fetch a specific metric value for a fleet (bypasses NLU)."""
    args: dict = {
        "metric": metric,
        "client_name": client_name,
        "fleet_name": fleet_name,
        "time_range": time_range,
        "timezone": timezone,
    }
    if sherpa_name:
        args["sherpa_name"] = sherpa_name
    result = await _call_tool("get_metric", args, server_url=server_url)
    return str(result) if result is not None else ""
