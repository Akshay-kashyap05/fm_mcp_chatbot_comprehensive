from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger("nlu")


@dataclass
class ParsedQuery:
    intent: str  # 'basic_analytics' | 'basic_analytics_item' | 'login' | 'help' | 'chat'
    item: Optional[str] = None
    sherpa_hint: Optional[str] = None
    fm_client_name: Optional[str] = None
    fleet_name: Optional[str] = None
    timezone: Optional[str] = None
    time_phrase: Optional[str] = None


ALLOWED_ITEMS = {
    "total_trips",
    "total_distance_km",
    "sherpa_wise_trips",
    "sherpa_wise_distance",
    "utilization",
    "uptime",
    "availability",
    "sherpa_status",
    "activity",
    "takt_time",
    "average_takt_time",
    "avg_obstacle_per_sherpa",
    "avg_obstacle_time",
    "obstacle_time",
    "top_10_routes_takt",
    "top_routes_takt",
    "route_utilization",
    "avg_obstacle_per_route",
}


def _heuristic_parse(text: str) -> ParsedQuery:
    original_text = text.strip()
    t = original_text.lower()

    if t in {"help", "?", "what can i ask", "what else can i ask"}:
        return ParsedQuery(intent="help")

    # Extract client name - look for "for client X" or "client X"
    # Stop before " and fleet" or " fleet" so we don't capture "and" as part of client name
    fm_client = None
    m = re.search(r"\b(?:for\s+)?client\s+([A-Za-z0-9\s_-]+?)(?:\s+and\s+fleet\b|\s+fleet\b|$)", t, re.IGNORECASE)
    if m:
        m_orig = re.search(r"\b(?:for\s+)?client\s+([A-Za-z0-9\s_-]+?)(?:\s+and\s+fleet\b|\s+fleet\b|$)", original_text, re.IGNORECASE)
        if m_orig:
            fm_client = m_orig.group(1).strip()
    
    # Extract fleet name - look for "for fleet X" or "fleet X"
    # Use original text to preserve case
    fleet = None
    # Fleet names can have spaces, underscores, hyphens, and special chars, so match after "fleet" until end or next keyword
    m = re.search(r"\b(?:for\s+)?fleet\s+([A-Za-z0-9\s_-]+?)(?:\s+(?:and|for|today|yesterday|this|last|previous|summary|analytics|total|distance|trips|uptime|utilization|availability|status|battery|mode|activity|sherpa|tug)\b|$)", t, re.IGNORECASE)
    if m:
        # Extract from original text to preserve case
        m_orig = re.search(r"\b(?:for\s+)?fleet\s+([A-Za-z0-9\s_-]+?)(?:\s+(?:and|for|today|yesterday|this|last|previous|summary|analytics|total|distance|trips|uptime|utilization|availability|status|battery|mode|activity|sherpa|tug)\b|$)", original_text, re.IGNORECASE)
        if m_orig:
            fleet = m_orig.group(1).strip()
    
    # common phrasings (check these first for better matching)
    pq = None
    # Check for takt time variations FIRST (must be before other checks to avoid falling through to summary)
    # Match: "takt time", "average takt time", "takt", "average takt", etc.
    if "takt time" in t or ("takt" in t and "time" in t) or "average takt" in t or ("takt" in t and "average" in t):
        # Check if it's "top routes takt" or "top 10 routes takt"
        if "top" in t and ("route" in t or "routes" in t):
            pq = ParsedQuery(intent="basic_analytics_item", item="top_10_routes_takt")
        else:
            pq = ParsedQuery(intent="basic_analytics_item", item="takt_time")
    elif "obstacle time" in t or ("obstacle" in t and "time" in t):
        # Check if it's per route or per sherpa
        if "route" in t or "routes" in t:
            pq = ParsedQuery(intent="basic_analytics_item", item="avg_obstacle_per_route")
        elif "sherpa" in t or "per sherpa" in t or "per-sherpa" in t:
            pq = ParsedQuery(intent="basic_analytics_item", item="avg_obstacle_per_sherpa")
            # "per sherpa" means all sherpas, so clear any sherpa hint
            pq.sherpa_hint = None
        else:
            pq = ParsedQuery(intent="basic_analytics_item", item="avg_obstacle_time")
    elif "route utilization" in t or ("route" in t and "utilization" in t):
        pq = ParsedQuery(intent="basic_analytics_item", item="route_utilization")
    elif "total trips" in t:
        pq = ParsedQuery(intent="basic_analytics_item", item="total_trips")
    elif "total distance" in t or "distance" in t:
        pq = ParsedQuery(intent="basic_analytics_item", item="total_distance_km")
    elif "uptime" in t:
        pq = ParsedQuery(intent="basic_analytics_item", item="uptime")
    elif "availability" in t:
        pq = ParsedQuery(intent="basic_analytics_item", item="availability")
    elif "utilization" in t or "utilisation" in t:
        pq = ParsedQuery(intent="basic_analytics_item", item="utilization")
    elif "status" in t and ("tug" in t or "sherpa" in t):
        pq = ParsedQuery(intent="basic_analytics_item", item="sherpa_status")
    
    if pq:
        pq.fm_client_name = fm_client
        pq.fleet_name = fleet
        # If item is "per_sherpa" metric, clear sherpa_hint (means all sherpas)
        if pq.item and ("per_sherpa" in pq.item or "per-sherpa" in pq.item):
            pq.sherpa_hint = None
        return pq
    
    # items - check ALLOWED_ITEMS for other matches
    # Prioritize takt_time if "takt" is mentioned
    if "takt" in t:
        for item in ["takt_time", "average_takt_time"]:
            if item in ALLOWED_ITEMS:
                pq = ParsedQuery(intent="basic_analytics_item", item="takt_time")
                pq.fm_client_name = fm_client
                pq.fleet_name = fleet
                return pq
    
    for item in ALLOWED_ITEMS:
        if item.replace("_", " ") in t or item in t:
            # if query is clearly asking one metric, return item intent
            pq = ParsedQuery(intent="basic_analytics_item", item=item)
            pq.fm_client_name = fm_client
            pq.fleet_name = fleet
            # If item is "per_sherpa" metric, clear sherpa_hint (means all sherpas)
            if "per_sherpa" in item or "per-sherpa" in item:
                pq.sherpa_hint = None
            return pq

    # general summary - only if no specific metric was found
    # Be careful: "analytics" should not match "average" (they're different words)
    if "summary" in t or ("analytics" in t and "takt" not in t):
        pq = ParsedQuery(intent="basic_analytics")
        pq.fm_client_name = fm_client
        pq.fleet_name = fleet
        return pq

    # If we haven't extracted client/fleet yet, try again (for general analytics queries)
    if not fm_client:
        m = re.search(r"\b(?:for\s+)?client\s+([A-Za-z0-9\s-]+?)(?:\s+fleet\b|$)", t, re.IGNORECASE)
        if m:
            m_orig = re.search(r"\b(?:for\s+)?client\s+([A-Za-z0-9\s-]+?)(?:\s+fleet\b|$)", original_text, re.IGNORECASE)
            if m_orig:
                fm_client = m_orig.group(1).strip()
    
    if not fleet:
        m = re.search(r"\b(?:for\s+)?fleet\s+([A-Za-z0-9\s-]+?)(?:\s+(?:and|for|today|yesterday|this|last|previous|summary|analytics|total|distance|trips|uptime|utilization|availability|status|battery|mode|activity|sherpa|tug)\b|$)", t, re.IGNORECASE)
        if m:
            m_orig = re.search(r"\b(?:for\s+)?fleet\s+([A-Za-z0-9\s-]+?)(?:\s+(?:and|for|today|yesterday|this|last|previous|summary|analytics|total|distance|trips|uptime|utilization|availability|status|battery|mode|activity|sherpa|tug)\b|$)", original_text, re.IGNORECASE)
            if m_orig:
                fleet = m_orig.group(1).strip()
    
    # sherpa hint, e.g., 'tug-104' or 'tug-107-ceat-nagpur-12'
    # Only extract if query doesn't say "per sherpa" (which means all sherpas)
    sherpa = None
    if "per sherpa" not in t and "per-sherpa" not in t:
        # Try full pattern first (tug-XXX-...), then short pattern (tug-XXX)
        # Stop at word boundaries or keywords like "for", "client", "fleet", etc.
        # Pattern: tug followed by optional dash/space, then digits, then optional dashes and alphanumeric parts
        # But stop before keywords like "for", "client", "fleet"
        m = re.search(r"\b(tug[- ]?\d+(?:[- ][a-z0-9-]+)*?)(?:\s+(?:for|client|fleet|today|yesterday|this|last|previous|summary|analytics|total|distance|trips|uptime|utilization|availability|status|battery|mode|activity|sherpa|tug)\b|$)", t, re.IGNORECASE)
        if m:
            sherpa = m.group(1).replace(" ", "-").lower()

    pq = ParsedQuery(intent="basic_analytics")
    pq.fm_client_name = fm_client
    pq.fleet_name = fleet
    pq.sherpa_hint = sherpa

    # time phrase hints
    for key in [
        "today",
        "yesterday",
        "day before yesterday",
        "last hour",
        "last 24 hours",
        "this week",
        "previous week",
        "last week",
        "this month",
        "previous month",
        "last month",
        "this quarter",
        "last quarter",
    ]:
        if key in t:
            pq.time_phrase = key
            break

    # Explicit date range: "X to Y" (e.g. "1 Jan 2026 to 5 Jan 2026")
    if not pq.time_phrase:
        # Match patterns like: "1 jan 2026 to 5 jan 2026" | "2026-01-01 to 2026-01-05"
        # | "jan 1 2026 to jan 5 2026" | "1st jan to 5th jan 2026"
        date_range_m = re.search(
            r"("
            r"\d{4}-\d{2}-\d{2}"           # ISO date: 2026-01-01
            r"|(?:\d{1,2}(?:st|nd|rd|th)?\s+)?\w+\s+\d{4}"  # "1 Jan 2026" / "Jan 2026"
            r"|(?:\d{1,2}(?:st|nd|rd|th)?\s+)?\w+\s+\d{2}"  # short year "1 Jan 26"
            r")"
            r"\s+to\s+"
            r"("
            r"\d{4}-\d{2}-\d{2}"
            r"|(?:\d{1,2}(?:st|nd|rd|th)?\s+)?\w+\s+\d{4}"
            r"|(?:\d{1,2}(?:st|nd|rd|th)?\s+)?\w+\s+\d{2}"
            r")",
            original_text, re.IGNORECASE,
        )
        if date_range_m:
            pq.time_phrase = date_range_m.group(0).strip()

    # Single explicit date: "1 Jan 2026" / "2026-01-01" / "1st Jan 2026"
    if not pq.time_phrase:
        single_m = re.search(
            r"\b("
            r"\d{4}-\d{2}-\d{2}"
            r"|\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{4}"
            r"|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}"
            r")\b",
            original_text, re.IGNORECASE,
        )
        if single_m:
            pq.time_phrase = single_m.group(0).strip()

    return pq


async def _ollama_json(prompt: str, model: str) -> Dict[str, Any]:
    base = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
    url = f"{base.rstrip('/')}/api/generate"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            url,
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "format": "json",
            },
        )
        r.raise_for_status()
        data = r.json()
        # Ollama returns JSON in 'response'
        raw = data.get("response", "{}").strip()
        return json.loads(raw)


async def parse_query(text: str, defaults: Dict[str, str]) -> ParsedQuery:
    """Parse user text into a structured query.

    If OLLAMA is available (OLLAMA_ENABLE=1), use it for better intent extraction.
    Otherwise fall back to a deterministic heuristic parser.
    """
    use_ollama = os.environ.get("OLLAMA_ENABLE", "1") == "1"
    model = os.environ.get("OLLAMA_MODEL", "llama3.2:3b")

    if not use_ollama:
        pq = _heuristic_parse(text)
        _apply_defaults(pq, defaults)
        return pq

    schema = {
        "intent": "basic_analytics | basic_analytics_item | help",
        "item": "one of allowed items or null",
        "sherpa_hint": "string like tug-104 or null",
        "fm_client_name": "string or null",
        "fleet_name": "string or null",
        "timezone": "IANA timezone or null",
        "time_phrase": "string or null",
    }

    prompt = (
        "You are a parser for a robotics analytics chatbot. "
        "Return ONLY valid JSON matching this schema: "
        f"{json.dumps(schema)}\n\n"
        "Allowed items: "
        f"{sorted(ALLOWED_ITEMS)}\n\n"
        "User text: "
        f"{text}\n"
    )

    try:
        obj = await _ollama_json(prompt, model=model)
        pq = ParsedQuery(
            intent=str(obj.get("intent") or "basic_analytics"),
            item=obj.get("item"),
            sherpa_hint=obj.get("sherpa_hint"),
            fm_client_name=obj.get("fm_client_name"),
            fleet_name=obj.get("fleet_name"),
            timezone=obj.get("timezone"),
            time_phrase=obj.get("time_phrase"),
        )
    except Exception as e:
        logger.warning("Ollama parse failed, falling back to heuristics: %s", e)
        pq = _heuristic_parse(text)

    # Post-process: Fix route analytics metrics if Ollama missed them
    text_lower = text.lower()
    
    # Check for obstacle time queries
    if "obstacle" in text_lower and "time" in text_lower:
        if pq.intent != "basic_analytics_item" or pq.item not in ("avg_obstacle_per_sherpa", "avg_obstacle_time", "avg_obstacle_per_route"):
            logger.debug("Fixing Ollama parse: detected obstacle time query")
            pq.intent = "basic_analytics_item"
            if "route" in text_lower or "routes" in text_lower:
                pq.item = "avg_obstacle_per_route"
            elif "sherpa" in text_lower or "per sherpa" in text_lower:
                pq.item = "avg_obstacle_per_sherpa"
            else:
                pq.item = "avg_obstacle_time"
    
    # Check for top routes takt time
    elif "top" in text_lower and ("route" in text_lower or "routes" in text_lower) and "takt" in text_lower:
        if pq.intent != "basic_analytics_item" or pq.item not in ("top_10_routes_takt", "top_routes_takt"):
            logger.debug("Fixing Ollama parse: detected top routes takt time query")
            pq.intent = "basic_analytics_item"
            pq.item = "top_10_routes_takt"
    
    # Check for route utilization
    elif "route" in text_lower and "utilization" in text_lower:
        if pq.intent != "basic_analytics_item" or pq.item != "route_utilization":
            logger.debug("Fixing Ollama parse: detected route utilization query")
            pq.intent = "basic_analytics_item"
            pq.item = "route_utilization"
    
    # Check for takt time queries
    elif ("takt" in text_lower and "time" in text_lower) or "average takt" in text_lower:
        if pq.intent != "basic_analytics_item" or pq.item not in ("takt_time", "average_takt_time"):
            logger.debug("Fixing Ollama parse: detected takt time query")
            pq.intent = "basic_analytics_item"
            pq.item = "takt_time"
    
    # Check for total trips queries
    elif "total trips" in text_lower or ("trips" in text_lower and "total" in text_lower):
        if pq.intent != "basic_analytics_item" or pq.item != "total_trips":
            logger.debug("Fixing Ollama parse: detected total trips query")
            pq.intent = "basic_analytics_item"
            pq.item = "total_trips"
    
    # Check for total distance queries
    elif "total distance" in text_lower or ("distance" in text_lower and "total" in text_lower):
        if pq.intent != "basic_analytics_item" or pq.item != "total_distance_km":
            logger.debug("Fixing Ollama parse: detected total distance query")
            pq.intent = "basic_analytics_item"
            pq.item = "total_distance_km"

    # Post-process: Fix swapped client/fleet names
    # Check if client and fleet names might be swapped by looking at the original text
    if pq.fm_client_name and pq.fleet_name:
        text_lower = text.lower()
        # Find positions of "client" and "fleet" keywords
        client_pos = text_lower.find("client")
        fleet_pos = text_lower.find("fleet")
        
        if client_pos != -1 and fleet_pos != -1 and client_pos < fleet_pos:
            # "client" comes before "fleet" in the query
            # Extract what comes after "client" and before "fleet" (this should be the client name)
            # Extract what comes after "fleet" (this should be the fleet name)
            client_section = text[client_pos:fleet_pos].lower()
            fleet_section = text[fleet_pos:].lower()
            
            client_name_lower = pq.fm_client_name.lower()
            fleet_name_lower = pq.fleet_name.lower()
            
            # Check if the parsed client_name actually appears in the client section
            # and if the parsed fleet_name actually appears in the fleet section
            client_name_in_client_section = client_name_lower in client_section
            fleet_name_in_fleet_section = fleet_name_lower in fleet_section
            
            # Also check the reverse - if they're swapped, the client_name would be in fleet section
            client_name_in_fleet_section = client_name_lower in fleet_section
            fleet_name_in_client_section = fleet_name_lower in client_section
            
            # If client_name contains underscores (like "owens_corning5"), it's likely a fleet name
            # If fleet_name contains spaces (like "Owens Corning-Taloja"), it's likely a client name
            # OR if the names appear in the wrong sections, swap them
            should_swap = False
            if "_" in pq.fm_client_name and " " in pq.fleet_name:
                # Heuristic: client names typically have spaces, fleet names typically have underscores
                should_swap = True
                logger.debug(f"Fixing swapped client/fleet names (heuristic): client={pq.fm_client_name}, fleet={pq.fleet_name}")
            elif (client_name_in_fleet_section and fleet_name_in_client_section) and \
                 not (client_name_in_client_section and fleet_name_in_fleet_section):
                # The names appear in the wrong sections - definitely swapped
                should_swap = True
                logger.debug(f"Fixing swapped client/fleet names (position check): client={pq.fm_client_name}, fleet={pq.fleet_name}")
            
            if should_swap:
                # Swap them
                pq.fm_client_name, pq.fleet_name = pq.fleet_name, pq.fm_client_name
                logger.debug(f"After swap: client={pq.fm_client_name}, fleet={pq.fleet_name}")

    # Post-process: Validate sherpa_hint - clear it if it doesn't appear in the original query
    # This prevents Ollama from hallucinating sherpa names that weren't mentioned
    if pq.sherpa_hint:
        text_lower = text.lower()
        sherpa_hint_lower = pq.sherpa_hint.lower().strip()
        
        # First, check if the full sherpa hint appears in the query (allowing for variations)
        # This is the most reliable check
        if sherpa_hint_lower not in text_lower:
            # Check if a significant portion appears (e.g., "tug-104" pattern)
            # Extract numeric part (e.g., "104" from "tug-104")
            import re
            numeric_match = re.search(r'\d+', sherpa_hint_lower)
            if numeric_match:
                numeric_part = numeric_match.group(0)
                # Check if the numeric part appears in the query (e.g., "104" in "tug-104")
                if numeric_part not in text_lower:
                    # Numeric part not found - definitely not in query
                    logger.info(f"Clearing invalid sherpa_hint '{pq.sherpa_hint}' - numeric part '{numeric_part}' not found in query text")
                    pq.sherpa_hint = None
                else:
                    # Numeric part found, but check if it's part of a sherpa pattern
                    # Look for pattern like "tug-104" or "tug 104" in query
                    # Also check for "tug" followed by the number (with optional dash/space)
                    tug_pattern = r'\btug[- ]?' + re.escape(numeric_part) + r'\b'
                    if not re.search(tug_pattern, text_lower):
                        # Pattern doesn't match - might be a false positive (e.g., "104" appears in a date)
                        logger.info(f"Clearing invalid sherpa_hint '{pq.sherpa_hint}' - sherpa pattern not found in query text")
                        pq.sherpa_hint = None
            else:
                # No numeric part - check if any significant word appears
                sherpa_parts = sherpa_hint_lower.replace("-", " ").replace("_", " ").split()
                # Require at least one part with 4+ chars to match (to avoid false positives with "tug")
                significant_parts = [p for p in sherpa_parts if len(p) >= 4]
                if significant_parts and not any(part in text_lower for part in significant_parts):
                    logger.info(f"Clearing invalid sherpa_hint '{pq.sherpa_hint}' - significant parts not found in query text")
                    pq.sherpa_hint = None
        else:
            # Full sherpa hint found in query - validate it's actually a sherpa mention
            # Check if it's part of a valid sherpa pattern (not just a random substring)
            # For example, "tug-104" should appear as a word, not as part of "something-tug-104-something"
            tug_pattern = r'\btug[- ]?\d+'
            if re.search(tug_pattern, text_lower):
                # Valid sherpa pattern found in query
                logger.debug(f"Valid sherpa_hint '{pq.sherpa_hint}' found in query")
            else:
                # Sherpa hint appears in query but not as a valid sherpa pattern
                logger.debug(f"Sherpa hint '{pq.sherpa_hint}' appears in query but not as valid pattern - keeping it")

    # sanitize
    if pq.item and pq.item not in ALLOWED_ITEMS:
        pq.item = None
        pq.intent = "basic_analytics"

    if pq.intent not in {"basic_analytics", "basic_analytics_item", "help"}:
        pq.intent = "basic_analytics"

    _apply_defaults(pq, defaults)
    return pq


def _apply_defaults(pq: ParsedQuery, defaults: Dict[str, str]) -> None:
    pq.fm_client_name = pq.fm_client_name or defaults.get("fm_client_name")
    pq.fleet_name = pq.fleet_name or defaults.get("fleet_name")
    pq.timezone = pq.timezone or defaults.get("timezone")
    if pq.time_phrase is None:
        pq.time_phrase = defaults.get("time_phrase")
