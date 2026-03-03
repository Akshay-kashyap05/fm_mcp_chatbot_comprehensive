#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
import asyncio

load_dotenv()  # loads .env from current working directory

from src.logging_config import setup_logging
from src.nlu import parse_query, ParsedQuery
#from src.sanjaya_client import SanjayaClient
from src.sanjaya_client import SanjayaAPI
from src.cache import TTLCache

from src.time_parse import parse_time_range
from src.formatting import summarize_basic_analytics, extract_item_value


setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("fm_mcp")

# Create FastMCP server with instructions for clients
mcp = FastMCP(
    "sanjaya_analytics_mcp",
    instructions=(
        "You are a Sanjaya Analytics assistant. Help users query analytics data for fleets and sherpas.\n\n"
        "IMPORTANT: Always ask for client name (fm_client_name) and fleet name if not provided in the query.\n\n"
        "Example queries:\n"
        "- 'total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant'\n"
        "- 'analytics summary yesterday for fleet CEAT-Nagpur-South-Plant'\n"
        "- 'uptime of tug-107 today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant'\n"
        "- 'total distance for sherpa tug-107-ceat-nagpur-12 today'\n\n"
        "Time ranges supported: today, yesterday, last week, this month, previous month, specific dates like '10th Jan 2026'.\n\n"
        "If parameters are missing, ask the user to specify them. Show available options when possible."
    )
)
BASE_URL = "https://sanjaya.atimotors.com"

# Constants for metric classification
ROUTE_ANALYTICS_ITEMS = (
    "takt_time", "average_takt_time",
    "avg_obstacle_per_sherpa", "avg_obstacle_time", "obstacle_time",
    "top_10_routes_takt", "top_routes_takt",
    "route_utilization",
    "avg_obstacle_per_route",
)

PER_SHERPA_ITEMS = ("avg_obstacle_per_sherpa",)

# Initialize TTL caches for API responses
# Cache clients for 10 minutes (they don't change often)
client_cache = TTLCache(ttl_seconds=600.0, max_size=100)
# Cache client details for 10 minutes
client_details_cache = TTLCache(ttl_seconds=600.0, max_size=100)
# Cache sherpa lists for 5 minutes (may change more frequently)
sherpa_cache = TTLCache(ttl_seconds=300.0, max_size=200)

def _defaults() -> Dict[str, Any]:
    fleet_id_str = os.environ.get("SANJAYA_DEFAULT_FLEET_ID", "")
    fleet_id = int(fleet_id_str) if fleet_id_str and fleet_id_str.isdigit() else None
    return {
        "fm_client_name": os.environ.get("SANJAYA_DEFAULT_CLIENT", ""),
        "fleet_name": os.environ.get("SANJAYA_DEFAULT_FLEET", ""),
        "fleet_id": fleet_id,
        "timezone": os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata"),
        # optional: default time phrase if user doesn't specify
        "time_phrase": os.environ.get("SANJAYA_DEFAULT_TIME", "today"),
    }


async def _resolve_partial_client_name(partial_name: str) -> Optional[str]:
    """Resolve a partial client name to the full client name.
    
    Args:
        partial_name: Partial or full client name (e.g., "ceat" or "ceat-nagpur")
        
    Returns:
        Full client name if a unique match is found, None otherwise
    """
    if not partial_name or not partial_name.strip():
        return None
    
    try:
        await client.ensure_token()
        # Use cached clients list
        all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
        if not all_clients:
            return None
        
        partial_lower = partial_name.lower().strip()
        matches = []
        
        for c in all_clients:
            if not isinstance(c, dict):
                continue
            client_name = c.get("fm_client_name", "")
            if not client_name:
                continue
            
            client_lower = client_name.lower()
            # Exact match
            if client_lower == partial_lower:
                return client_name
            # Partial match (starts with or contains)
            if client_lower.startswith(partial_lower) or partial_lower in client_lower:
                matches.append(client_name)
        
        # If exactly one match, return it
        if len(matches) == 1:
            logger.debug(f"Resolved partial client name '{partial_name}' to '{matches[0]}'")
            return matches[0]
        # If multiple matches, prefer exact prefix match
        elif len(matches) > 1:
            # Prefer matches that start with the partial name
            prefix_matches = [m for m in matches if m.lower().startswith(partial_lower)]
            if len(prefix_matches) == 1:
                logger.debug(f"Resolved partial client name '{partial_name}' to '{prefix_matches[0]}' (prefix match)")
                return prefix_matches[0]
            else:
                logger.debug(f"Multiple client matches for '{partial_name}': {matches}")
                return None
        
        return None
    except Exception as e:
        logger.debug(f"Could not resolve partial client name '{partial_name}': {e}")
        return None


async def _resolve_partial_fleet_name(partial_fleet_name: str, client_name: str) -> Optional[str]:
    """Resolve a partial fleet name to the full fleet name for a given client.
    
    Args:
        partial_fleet_name: Partial or full fleet name (e.g., "north" or "CEAT-Nagpur-North-Plant")
        client_name: Full client name to look up fleets for
        
    Returns:
        Full fleet name if a unique match is found, None otherwise
    """
    if not partial_fleet_name or not partial_fleet_name.strip() or not client_name:
        return None
    
    try:
        await client.ensure_token()
        # First, get all clients to find the client_id (cached)
        all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
        client_id = None
        for c in all_clients:
            if isinstance(c, dict) and c.get("fm_client_name", "").lower() == client_name.lower():
                client_id = c.get("fm_client_id")
                break
        
        if not client_id:
            return None
        
        # Get client details to fetch fleet names (cached)
        cache_key = f"client_details_{client_id}"
        client_details = await client_details_cache.get_or_set(
            cache_key, client.get_client_by_id, client_id
        )
        fleet_names = client_details.get("fm_fleet_names", [])
        if not fleet_names or not isinstance(fleet_names, list):
            return None
        
        partial_lower = partial_fleet_name.lower().strip()
        matches = []
        
        for fleet_name in fleet_names:
            if not fleet_name:
                continue
            
            fleet_lower = fleet_name.lower()
            # Exact match
            if fleet_lower == partial_lower:
                return fleet_name
            # Partial match (starts with or contains)
            if fleet_lower.startswith(partial_lower) or partial_lower in fleet_lower:
                matches.append(fleet_name)
        
        # If exactly one match, return it
        if len(matches) == 1:
            logger.debug(f"Resolved partial fleet name '{partial_fleet_name}' to '{matches[0]}' for client '{client_name}'")
            return matches[0]
        # If multiple matches, prefer exact prefix match
        elif len(matches) > 1:
            # Prefer matches that start with the partial name
            prefix_matches = [m for m in matches if m.lower().startswith(partial_lower)]
            if len(prefix_matches) == 1:
                logger.debug(f"Resolved partial fleet name '{partial_fleet_name}' to '{prefix_matches[0]}' (prefix match) for client '{client_name}'")
                return prefix_matches[0]
            else:
                logger.debug(f"Multiple fleet matches for '{partial_fleet_name}' in client '{client_name}': {matches}")
                return None
        
        return None
    except Exception as e:
        logger.debug(f"Could not resolve partial fleet name '{partial_fleet_name}' for client '{client_name}': {e}")
        return None


async def _resolve_sherpa_for_fleet(
    fleet_id: int,
    fleet_name: str,
    sherpa_hint: Optional[str] = None,
) -> tuple[Optional[str], str]:
    """Resolve sherpa name for a given fleet.
    
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
        
        # No sherpa hint - return all sherpa names for the fleet
        sherpa_names_list = [s.get("sherpa_name") for s in matching_sherpas if s.get("sherpa_name")]
        logger.debug(f"Found {len(sherpa_names_list)} sherpas for fleet {actual_fleet_name}")
        return sherpa_names_list, actual_fleet_name
        
    except Exception as e:
        logger.warning(f"Failed to fetch sherpas for fleet_id {fleet_id}: {e}")
        return None, fleet_name


async def _check_missing_params(pq: ParsedQuery, defaults: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Check if required parameters are missing and return a clarification message with options if so."""
    missing = []
    suggestions = []
    options = {}
    
    # Check fm_client_name - only check what user provided, NOT defaults
    if not pq.fm_client_name or not pq.fm_client_name.strip():
        missing.append("client name (fm_client_name)")
        if defaults.get("fm_client_name"):
            suggestions.append(f"Default available: {defaults['fm_client_name']} (use 'for client {defaults['fm_client_name']}' to use it)")
        else:
            suggestions.append("Set SANJAYA_DEFAULT_CLIENT environment variable or specify in query")
            
            # Fetch all clients to show as options (cached)
            try:
                await client.ensure_token()
                all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
                if all_clients:
                    client_names = sorted([
                        c.get("fm_client_name") for c in all_clients 
                        if isinstance(c, dict) and c.get("fm_client_name")
                    ])
                    if client_names:
                        options["client_names"] = client_names
                        suggestions.append(f"\nAvailable clients: {', '.join(client_names[:10])}{'...' if len(client_names) > 10 else ''}")
            except Exception as e:
                logger.debug(f"Could not fetch client options: {e}")
    
    # Check fleet_name - only check what user provided, NOT defaults
    if not pq.fleet_name or not pq.fleet_name.strip():
        missing.append("fleet name")
        if defaults.get("fleet_name"):
            suggestions.append(f"Default available: {defaults['fleet_name']} (use 'for fleet {defaults['fleet_name']}' to use it)")
        else:
            suggestions.append("Set SANJAYA_DEFAULT_FLEET environment variable or specify in query")
        
            # Try to fetch fleet options based on client name (from user or defaults)
            client_name = pq.fm_client_name or defaults.get("fm_client_name")
            if client_name:
                try:
                    # Resolve partial client name first
                    resolved_client = await _resolve_partial_client_name(client_name)
                    if resolved_client:
                        client_name = resolved_client
                    
                    await client.ensure_token()
                    # First, get all clients to find the client_id (cached)
                    all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
                    client_id = None
                    for c in all_clients:
                        if isinstance(c, dict) and c.get("fm_client_name", "").lower() == client_name.lower():
                            client_id = c.get("fm_client_id")
                            break
                    
                    # If we found the client_id, get client details to fetch fleet names (cached)
                    if client_id:
                        cache_key = f"client_details_{client_id}"
                        client_details = await client_details_cache.get_or_set(
                            cache_key, client.get_client_by_id, client_id
                        )
                        fleet_names = client_details.get("fm_fleet_names", [])
                        if fleet_names and isinstance(fleet_names, list):
                            options["fleet_names"] = sorted(fleet_names)
                            suggestions.append(f"\nAvailable fleets for client '{client_name}': {', '.join(fleet_names[:5])}{'...' if len(fleet_names) > 5 else ''}")
                except Exception as e:
                    logger.debug(f"Could not fetch fleet options for client {client_name}: {e}")
            
            # Fallback: Try to fetch fleet options if we have fleet_id (old method)
            if not options.get("fleet_names"):
                fleet_id = defaults.get("fleet_id")
                if fleet_id:
                    try:
                        await client.ensure_token()
                        # Use cached sherpa list
                        cache_key = f"sherpas_fleet_{fleet_id}"
                        all_sherpas = await sherpa_cache.get_or_set(
                            cache_key, client.get_sherpas_by_fleet_id, fleet_id
                        )
                        # Extract unique fleet names
                        fleet_names = sorted(set(
                            s.get("fleet_name") for s in all_sherpas 
                            if isinstance(s, dict) and s.get("fleet_name")
                        ))
                        if fleet_names:
                            options["fleet_names"] = fleet_names
                            suggestions.append(f"\nAvailable fleets: {', '.join(fleet_names[:5])}{'...' if len(fleet_names) > 5 else ''}")
                    except Exception as e:
                        logger.debug(f"Could not fetch fleet options: {e}")
    
    if missing:
        message_parts = []
        message_parts.append("❌ Missing Required Information")
        message_parts.append("=" * 50)
        message_parts.append("")
        message_parts.append(f"I need the following to process your request:")
        for param in missing:
            message_parts.append(f"  • {param}")
        message_parts.append("")
        
        if options.get("client_names"):
            message_parts.append("📋 Available Client Options:")
            for client in options["client_names"]:
                message_parts.append(f"  • {client}")
            message_parts.append("")
        
        if options.get("fleet_names"):
            message_parts.append("📋 Available Fleet Options:")
            for fleet in options["fleet_names"]:
                message_parts.append(f"  • {fleet}")
            message_parts.append("")
        
        message_parts.append("💡 How to Provide Missing Information:")
        message_parts.append("")
        
        # Build example queries based on what's missing
        examples = []
        if "client name" in str(missing) and "fleet name" in str(missing):
            if options.get("client_names") and options.get("fleet_names"):
                example_client = options["client_names"][0]
                example_fleet = options["fleet_names"][0]
                examples.append(f"  'total trips today for client {example_client} and fleet {example_fleet}'")
                examples.append(f"  'analytics summary yesterday for client {example_client} and fleet {example_fleet}'")
            elif options.get("client_names"):
                example_client = options["client_names"][0]
                examples.append(f"  'total trips today for client {example_client} and fleet [fleet_name]'")
            else:
                examples.append(f"  'total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant'")
        elif "fleet name" in str(missing):
            if options.get("fleet_names"):
                example_fleet = options["fleet_names"][0]
                examples.append(f"  'total trips today for fleet {example_fleet}'")
                examples.append(f"  'analytics summary for fleet {example_fleet}'")
            else:
                examples.append(f"  'total trips today for fleet CEAT-Nagpur-North-Plant'")
        elif "client name" in str(missing):
            if options.get("client_names"):
                example_client = options["client_names"][0]
                examples.append(f"  'total trips today for client {example_client}'")
            else:
                examples.append(f"  'total trips today for client ceat-nagpur'")
        
        for example in examples:
            message_parts.append(example)
        message_parts.append("")
        
        # Add default information if available
        if defaults.get("fm_client_name") or defaults.get("fleet_name"):
            message_parts.append("⚙️  Default Configuration Available:")
            if defaults.get("fm_client_name"):
                message_parts.append(f"  • Default client: {defaults['fm_client_name']}")
            if defaults.get("fleet_name"):
                message_parts.append(f"  • Default fleet: {defaults['fleet_name']}")
            message_parts.append("")
            message_parts.append("  Note: You can use defaults by explicitly mentioning them in your query.")
            message_parts.append("")
        
        message = "\n".join(message_parts)
        
        return {
            "type": "clarification",
            "message": message,
            "missing_params": missing,
            "options": options,
        }
    
    return None


#client = SanjayaClient()
client = SanjayaAPI(BASE_URL)


async def _aggregate_analytics_for_all_sherpas(
    sherpa_names: List[str],
    fm_client_name: str,
    start_time: str,
    end_time: str,
    timezone: str,
    fleet_name: str,
    status: List[str],
) -> Dict[str, Any]:
    """Make API calls for each sherpa and aggregate the results."""
    # Make concurrent API calls for all sherpas
    tasks = [
        client.basic_analytics(
            fm_client_name=fm_client_name,
            start_time=start_time,
            end_time=end_time,
            timezone=timezone,
            fleet_name=fleet_name,
            status=status,
            sherpa_name=sherpa_name,
        )
        for sherpa_name in sherpa_names
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Aggregate the results
    aggregated = {
        "total_trips": 0,
        "total_distance_km": 0.0,
        "sherpa_wise_trips": [],
        "sherpa_wise_distance": [],
        "utilization": [],
        "uptime": [],
        "availability": [],
        "sherpa_status": [],
        "activity": [],
    }
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.warning(f"Failed to get analytics for {sherpa_names[i]}: {result}")
            continue
        
        if not isinstance(result, dict):
            continue
            
        # Sum totals
        aggregated["total_trips"] += result.get("total_trips", 0)
        aggregated["total_distance_km"] += result.get("total_distance_km", 0.0)
        
        # Combine lists
        for key in ["sherpa_wise_trips", "sherpa_wise_distance", "utilization", 
                   "uptime", "availability", "sherpa_status", "activity"]:
            items = result.get(key, [])
            if isinstance(items, list):
                aggregated[key].extend(items)
    
    return aggregated

@mcp.tool()
async def sanjaya_login() -> Dict[str, Any]:
    """Login to Sanjaya and cache the x-user-token.

    Reads credentials from environment:
    - SANJAYA_USERNAME
    - SANJAYA_PASSWORD
    - SANJAYA_SOURCE (optional)
    """
    token = await client.ensure_token()
    return {"ok": True, "x_user_token": token}


@mcp.tool()
async def sanjaya_basic_analytics(
    fm_client_name: str,
    start_time: str,
    end_time: str,
    timezone: str,
    fleet_name: str,
    status: List[str],
    sherpa_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Calls /analytics/basic_analytics/{fm_client_name} and returns full JSON payload."""
    return await client.basic_analytics(
        fm_client_name=fm_client_name,
        start_time=start_time,
        end_time=end_time,
        timezone=timezone,
        fleet_name=fleet_name,
        status=status,
        sherpa_name=sherpa_name,
    )


@mcp.tool()
async def sanjaya_basic_analytics_item(
    item: str,
    fm_client_name: str,
    start_time: str,
    end_time: str,
    timezone: str,
    fleet_name: str,
    status: List[str],
    sherpa_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Calls basic_analytics and extracts a single item from the response."""
    data = await client.basic_analytics(
        fm_client_name=fm_client_name,
        start_time=start_time,
        end_time=end_time,
        timezone=timezone,
        fleet_name=fleet_name,
        status=status,
        sherpa_name=sherpa_name,
    )
    val, note = extract_item_value(data, item=item, sherpa_hint=sherpa_name)
    return {"item": item, "value": val, "note": note}


@mcp.tool()
async def sanjaya_chat(text: str) -> Dict[str, Any]:
    """Chat endpoint: natural language -> calls the right Sanjaya APIs -> returns a friendly response.

    This is the tool your UI should call.

    Notes:
    - Uses a local LLM via Ollama if enabled (OLLAMA_ENABLE=1), otherwise heuristics.
    - Never hard-codes client/fleet defaults: you set SANJAYA_DEFAULT_CLIENT / SANJAYA_DEFAULT_FLEET.
    """
    defaults = _defaults()
    
    # Clean up any sherpa markers from the query before parsing
    clean_text = text.replace("[sherpa:all]", "").strip()
    original_query_text = clean_text  # Keep original for "per sherpa" detection
    
    # Parse query WITHOUT applying defaults first - we want to check what user actually specified
    # Temporarily pass empty defaults to see what user actually specified
    empty_defaults = {
        "fm_client_name": "",
        "fleet_name": "",
        "timezone": defaults.get("timezone", "Asia/Kolkata"),
        "time_phrase": defaults.get("time_phrase", "today"),
    }
    pq = await parse_query(clean_text, defaults=empty_defaults)
    
    if pq.intent == "help":
        defaults = _defaults()
        help_message = []
        help_message.append("📖 Sanjaya Analytics - How to Ask Questions")
        help_message.append("=" * 60)
        help_message.append("")
        help_message.append("✅ REQUIRED: Always specify client and fleet in your queries")
        help_message.append("")
        help_message.append("📝 Query Format:")
        help_message.append("  '[metric] [time] for client [client_name] and fleet [fleet_name]'")
        help_message.append("")
        help_message.append("📊 Available Metrics:")
        help_message.append("  • total trips")
        help_message.append("  • total distance")
        help_message.append("  • utilization")
        help_message.append("  • uptime")
        help_message.append("  • availability")
        help_message.append("  • takt time / average takt time")
        help_message.append("  • avg obstacle time per sherpa")
        help_message.append("  • top 10 routes takt time")
        help_message.append("  • route utilization")
        help_message.append("  • avg obstacle time per route")
        help_message.append("  • battery")
        help_message.append("  • mode / status")
        help_message.append("  • analytics summary (full report)")
        help_message.append("")
        help_message.append("⏰ Time Ranges:")
        help_message.append("  • today, yesterday, day before yesterday")
        help_message.append("  • this week, last week, previous week")
        help_message.append("  • this month, last month, previous month")
        help_message.append("  • this quarter, last quarter")
        help_message.append("  • Specific dates: '10th Jan 2026', '10-01-26', '2026/01/10'")
        help_message.append("")
        help_message.append("🤖 Sherpa-Specific Queries:")
        help_message.append("  • 'total distance for sherpa tug-107-ceat-nagpur-12 today'")
        help_message.append("  • 'uptime of tug-104 for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant'")
        help_message.append("")
        help_message.append("💡 Example Queries:")
        help_message.append("  • 'total trips today for client ceat-nagpur and fleet CEAT-Nagpur-North-Plant'")
        help_message.append("  • 'analytics summary yesterday for fleet CEAT-Nagpur-South-Plant'")
        help_message.append("  • 'utilization this week for client ceat-nagpur and fleet CEAT-TBM-Plant-Fleet'")
        help_message.append("")
        
        # Show available fleets if we can fetch them
        fleet_id = defaults.get("fleet_id")
        if fleet_id:
            try:
                await client.ensure_token()
                # Use cached sherpa list
                cache_key = f"sherpas_fleet_{fleet_id}"
                all_sherpas = await sherpa_cache.get_or_set(
                    cache_key, client.get_sherpas_by_fleet_id, fleet_id
                )
                fleet_names = sorted(set(
                    s.get("fleet_name") for s in all_sherpas 
                    if isinstance(s, dict) and s.get("fleet_name")
                ))
                if fleet_names:
                    help_message.append("📋 Available Fleets:")
                    for fleet in fleet_names:
                        help_message.append(f"  • {fleet}")
                    help_message.append("")
            except Exception:
                pass
        
        if defaults.get("fm_client_name") or defaults.get("fleet_name"):
            help_message.append("⚙️  Default Configuration:")
            if defaults.get("fm_client_name"):
                help_message.append(f"  • Default client: {defaults['fm_client_name']}")
            if defaults.get("fleet_name"):
                help_message.append(f"  • Default fleet: {defaults['fleet_name']}")
            help_message.append("")
            help_message.append("  Note: You must still explicitly mention defaults in your query.")
            help_message.append("")
        
        return {
            "type": "help",
            "message": "\n".join(help_message),
        }
    
    # Check for missing required parameters BEFORE applying defaults
    clarification = await _check_missing_params(pq, defaults)
    if clarification:
        return clarification
    
    # Now apply defaults for values that weren't provided by user
    # (Only after user has been prompted and confirmed, or explicitly mentioned in query)
    if not pq.fm_client_name:
        pq.fm_client_name = defaults.get("fm_client_name")
    if not pq.fleet_name:
        pq.fleet_name = defaults.get("fleet_name")
    if not pq.timezone:
        pq.timezone = defaults.get("timezone")
    if not pq.time_phrase:
        pq.time_phrase = defaults.get("time_phrase")
    
    # Validate required parameters after defaults are applied
    if not pq.fm_client_name or not pq.fm_client_name.strip():
        return {
            "ok": False,
            "error": "Client name (fm_client_name) is required. Please specify a client name in your query.",
            "type": "error",
        }
    
    if not pq.fleet_name or not pq.fleet_name.strip():
        return {
            "ok": False,
            "error": "Fleet name is required. Please specify a fleet name in your query.",
            "type": "error",
        }
    
    # Resolve partial client name if provided
    if pq.fm_client_name and pq.fm_client_name.strip():
        resolved_client = await _resolve_partial_client_name(pq.fm_client_name)
        if resolved_client:
            if resolved_client != pq.fm_client_name:
                logger.info(f"Resolved partial client name '{pq.fm_client_name}' to '{resolved_client}'")
            pq.fm_client_name = resolved_client
    
    # Resolve partial fleet name if we have a client name
    if pq.fleet_name and pq.fleet_name.strip() and pq.fm_client_name:
        resolved_fleet = await _resolve_partial_fleet_name(pq.fleet_name, pq.fm_client_name)
        if resolved_fleet:
            if resolved_fleet != pq.fleet_name:
                logger.info(f"Resolved partial fleet name '{pq.fleet_name}' to '{resolved_fleet}'")
            pq.fleet_name = resolved_fleet
        else:
            # If partial resolution failed, try case normalization
            try:
                await client.ensure_token()
                # Get all clients to find the client_id (cached)
                all_clients = await client_cache.get_or_set("all_clients", client.get_clients)
                client_id = None
                for c in all_clients:
                    if isinstance(c, dict) and c.get("fm_client_name", "").lower() == pq.fm_client_name.lower():
                        client_id = c.get("fm_client_id")
                        break
                
                # If we found the client_id, get client details to fetch fleet names with correct case (cached)
                if client_id:
                    cache_key = f"client_details_{client_id}"
                    client_details = await client_details_cache.get_or_set(
                        cache_key, client.get_client_by_id, client_id
                    )
                    fleet_names = client_details.get("fm_fleet_names", [])
                    if fleet_names and isinstance(fleet_names, list):
                        # Find matching fleet name (case-insensitive) and use the correct case from API
                        for api_fleet_name in fleet_names:
                            if api_fleet_name.lower() == pq.fleet_name.lower():
                                if api_fleet_name != pq.fleet_name:
                                    logger.debug(f"Normalizing fleet_name case: '{pq.fleet_name}' -> '{api_fleet_name}'")
                                    pq.fleet_name = api_fleet_name
                                break
            except Exception as e:
                logger.debug(f"Could not normalize fleet name case: {e}")
    
    # Clear sherpa_hint if it matches the client name (common parsing error)
    if pq.sherpa_hint and pq.fm_client_name:
        if pq.sherpa_hint.lower() == pq.fm_client_name.lower() or pq.fm_client_name.lower().endswith(pq.sherpa_hint.lower()):
            logger.debug(f"Clearing sherpa_hint '{pq.sherpa_hint}' as it matches client name '{pq.fm_client_name}'")
            pq.sherpa_hint = None

    # time range
    tz = pq.timezone or defaults.get("timezone") or "Asia/Kolkata"
    if not tz or tz.strip() == "":
        tz = "Asia/Kolkata"
    tr = parse_time_range(pq.time_phrase or defaults.get("time_phrase") or "today", timezone=tz)
    time_strings = tr.to_strings()

    # default statuses
    statuses = ["succeeded", "failed", "cancelled"]

    # Ensure token (auto-refresh)
    await client.ensure_token()

    # Determine sherpa_name: use hint if provided, otherwise fetch all sherpas for the fleet
    # If the item is a "per_sherpa" metric OR the query contains "per sherpa", don't use a specific sherpa
    # Check both the item name and the original query text
    is_per_sherpa_query = (
        (pq.item and pq.item in PER_SHERPA_ITEMS) or
        ("per sherpa" in original_query_text.lower() or "per-sherpa" in original_query_text.lower())
    )
    
    if is_per_sherpa_query:
        # "per sherpa" queries should not filter by a specific sherpa - get data for all sherpas
        logger.debug(f"Query is a per_sherpa query, clearing sherpa_name filter")
        sherpa_name = None
        # Also clear sherpa_hint so extraction doesn't try to match a specific sherpa
        pq.sherpa_hint = None
    else:
        sherpa_name = pq.sherpa_hint
    
    fleet_name = pq.fleet_name or defaults["fleet_name"]
    fleet_id = defaults.get("fleet_id")
    
    # Resolve sherpa name using the extracted helper function
    if fleet_id:
        resolved_sherpa, actual_fleet_name = await _resolve_sherpa_for_fleet(
            fleet_id=fleet_id,
            fleet_name=fleet_name,
            sherpa_hint=sherpa_name,
        )
        if resolved_sherpa is not None:
            sherpa_name = resolved_sherpa
        if actual_fleet_name != fleet_name:
            logger.debug(f"Using API fleet_name '{actual_fleet_name}' instead of '{fleet_name}'")
            fleet_name = actual_fleet_name
        
        # If we got a list of sherpas, log it
        if isinstance(sherpa_name, list):
            logger.info(f"Using all {len(sherpa_name)} sherpas for fleet {fleet_name}")
    
    # Use route_analytics for route-related metrics, basic_analytics for everything else
    if pq.intent == "basic_analytics_item" and pq.item in ROUTE_ANALYTICS_ITEMS:
        data = await client.route_analytics(
            fm_client_name=pq.fm_client_name or defaults["fm_client_name"],
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=tz,
            fleet_name=fleet_name,
            status=statuses,
            sherpa_name=sherpa_name,
        )
    else:
        data = await client.basic_analytics(
            fm_client_name=pq.fm_client_name or defaults["fm_client_name"],
            start_time=time_strings["start_time"],
            end_time=time_strings["end_time"],
            timezone=tz,
            fleet_name=fleet_name,
            status=statuses,
            sherpa_name=sherpa_name,
        )
    
    if pq.intent == "basic_analytics_item" and pq.item:
        # For "per sherpa" queries, don't pass sherpa_hint to extraction (it should return all sherpas)
        extraction_sherpa_hint = None if is_per_sherpa_query else pq.sherpa_hint
        val, note = extract_item_value(data, item=pq.item, sherpa_hint=extraction_sherpa_hint)
        return {
            "type": "item",
            "item": pq.item,
            "sherpa": pq.sherpa_hint,
            "time_range": {"start_time": time_strings["start_time"], "end_time": time_strings["end_time"], "timezone": tz},
            "value": val,
            "note": note,
        }

    summary = summarize_basic_analytics(data)
    return {
        "type": "summary",
        "time_range": {"start_time": time_strings["start_time"], "end_time": time_strings["end_time"], "timezone": tz},
        "summary": summary,
        "raw": data,
    }


if __name__ == "__main__":
    # IMPORTANT: FastMCP will speak JSON-RPC over stdout/stderr properly.
    # Do not print anything to stdout here.
    logger.info("Starting Sanjaya Analytics MCP Server (stdio)...")
    mcp.run()
