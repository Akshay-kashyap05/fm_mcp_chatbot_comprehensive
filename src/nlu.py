from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger("nlu")

# ── Load metric config from JSON (edit this file to add aliases, no code change needed) ──
_CONFIG_PATH = Path(__file__).parent / "metrics_config.json"
_ALLOWED_ITEMS_DEFAULT = {
    "total_trips", "total_distance_km", "sherpa_wise_trips", "sherpa_wise_distance",
    "utilization", "uptime", "availability", "sherpa_status", "activity",
    "takt_time", "average_takt_time", "avg_obstacle_per_sherpa", "avg_obstacle_time",
    "obstacle_time", "top_10_routes_takt", "top_routes_takt", "route_utilization",
    "avg_obstacle_per_route",
}
_METRIC_ALIASES_DEFAULT: Dict[str, str] = {
    "tug wise distance": "sherpa_wise_distance", "tug-wise distance": "sherpa_wise_distance",
    "bot wise distance": "sherpa_wise_distance", "bot-wise distance": "sherpa_wise_distance",
    "robot wise distance": "sherpa_wise_distance", "per tug distance": "sherpa_wise_distance",
    "distance per tug": "sherpa_wise_distance", "distance per sherpa": "sherpa_wise_distance",
    "sherpa wise distance": "sherpa_wise_distance",
    "tug wise trips": "sherpa_wise_trips", "tug-wise trips": "sherpa_wise_trips",
    "bot wise trips": "sherpa_wise_trips", "bot-wise trips": "sherpa_wise_trips",
    "robot wise trips": "sherpa_wise_trips", "per tug trips": "sherpa_wise_trips",
    "trips per tug": "sherpa_wise_trips", "trips per sherpa": "sherpa_wise_trips",
    "sherpa wise trips": "sherpa_wise_trips",
    "tug wise": "sherpa_wise_trips", "bot wise": "sherpa_wise_trips",
    "per bot": "sherpa_wise_trips", "per tug": "sherpa_wise_trips",
    "route takt": "top_10_routes_takt", "takt per route": "top_10_routes_takt",
    "top routes takt": "top_10_routes_takt", "top 10 routes takt": "top_10_routes_takt",
    "average takt": "average_takt_time", "avg takt": "average_takt_time",
    "route usage": "route_utilization", "obstacle count": "avg_obstacle_per_sherpa",
    "obstacle per sherpa": "avg_obstacle_per_sherpa", "obstacle per route": "avg_obstacle_per_route",
    "obstacle time": "avg_obstacle_time", "takt time": "takt_time",
}
try:
    with _CONFIG_PATH.open() as _f:
        _metrics_cfg = json.load(_f)
    ALLOWED_ITEMS: set = set(_metrics_cfg["allowed_items"])
    _METRIC_ALIASES: Dict[str, str] = _metrics_cfg["aliases"]
except FileNotFoundError:
    logger.warning("metrics_config.json not found at %s — using built-in defaults", _CONFIG_PATH)
    ALLOWED_ITEMS = _ALLOWED_ITEMS_DEFAULT
    _METRIC_ALIASES = _METRIC_ALIASES_DEFAULT


@dataclass
class ParsedQuery:
    intent: str  # 'basic_analytics' | 'basic_analytics_item' | 'multi_metric' | 'help'
    item: Optional[str] = None          # primary (first) metric, or None
    items: list = field(default_factory=list)  # all metrics when user asks for 2+
    sherpa_hint: Optional[str] = None
    fm_client_name: Optional[str] = None
    fleet_name: Optional[str] = None
    timezone: Optional[str] = None
    time_phrase: Optional[str] = None
    client_from_text: bool = False  # True only when NLU found client in prompt (not from defaults)


# Keywords that terminate a client/fleet name match
_NAME_STOP = (
    r"and\s+fleet|fleet\b|yesterday|today|this\s+week|last\s+week|this\s+month|"
    r"last\s+month|this\s+quarter|last\s+quarter|last\s+\d+|uptime|utilization|"
    r"utilisation|obstacle|takt|trips|distance|availability|status|route|sherpa|"
    r"analytics|summary|show|give|tell|get|me\b|please|what|how|for\s+\d|,|\."
)


def _extract_names(original_text: str) -> tuple[Optional[str], Optional[str]]:
    """Extract (client_name, fleet_name) from text using deterministic patterns.

    Tries multiple patterns in priority order; returns original-case strings.
    Never calls the LLM — all extraction is regex-based.

    Patterns tried for client (in order):
    1. "client <name>" or "for client <name>"
    2. "for <UPPER-HYPHEN-NAME>," or "for <UPPER-HYPHEN-NAME> <keyword>"
    3. Start of text: "<UPPER-HYPHEN-NAME> <keyword>"
    """
    fm_client: Optional[str] = None
    fleet: Optional[str] = None

    # ── Pattern 1: explicit "client" keyword (name AFTER "client") ──────────
    # Negative lookahead prevents capturing common verbs as names
    # e.g. "client give me..." → lookahead blocks; "client CEAT-Nagpur..." → matches
    _VERB_LA = r"(?!(?:give|tell|get|show|please|what|how|is|are|was|were|i\b|we\b|the\b)\b)"
    pat_client = (
        r"\b(?:for\s+)?client\s+"
        + _VERB_LA +
        r"([A-Za-z0-9][A-Za-z0-9\s_\-]*?)"
        r"(?:\s+(?:" + _NAME_STOP + r")|$)"
    )
    m = re.search(pat_client, original_text, re.IGNORECASE)
    if m:
        fm_client = m.group(1).strip().rstrip(",.")

    # ── Pattern 2: "for <UPPER-OR-HYPHEN>," or "for <NAME> <metric-word>" ───
    if not fm_client:
        # Matches: "for YOKOHAMA-DAHEJ," or "for CEAT-Nagpur uptime"
        # Optional article (the/a/an) between "for" and the name is stripped.
        m2 = re.search(
            r"\bfor\s+(?:the\s+|a\s+|an\s+)?([A-Za-z][A-Za-z0-9\-]{2,}(?:\s+[A-Za-z][A-Za-z0-9\-]+)*?)"
            r"(?:\s*[,.]|\s+(?:" + _NAME_STOP + r")|$)",
            original_text, re.IGNORECASE,
        )
        if m2:
            candidate = m2.group(1).strip().rstrip(",.")
            # Accept if it contains a hyphen (typical client format) or is all-uppercase
            if "-" in candidate or candidate == candidate.upper():
                fm_client = candidate

    # ── Pattern 3: name at start of prompt, before a metric keyword ──────────
    if not fm_client:
        m3 = re.match(
            r"^([A-Za-z][A-Za-z0-9\-]{2,}(?:\s+[A-Za-z][A-Za-z0-9\-]+)*?)"
            r"\s+(?:uptime|utilization|utilisation|obstacle|takt|trips|distance|"
            r"availability|status|route|analytics|summary|show)",
            original_text, re.IGNORECASE,
        )
        if m3:
            candidate = m3.group(1).strip()
            if "-" in candidate or candidate == candidate.upper():
                fm_client = candidate

    # ── Pattern 4: "<name> client" — the keyword comes AFTER the name ────────
    # e.g. "tvs hasur client uptime" or "uptime for yokohama-dahej client"
    # "client" right after the name is a strong signal → relax hyphen/uppercase guard.
    if not fm_client:
        m4 = re.search(
            r"([A-Za-z][A-Za-z0-9\-]{2,}(?:\s+[A-Za-z][A-Za-z0-9\-]+)*?)\s+client\b",
            original_text, re.IGNORECASE,
        )
        if m4:
            candidate = m4.group(1).strip().rstrip(",.")
            cwords = candidate.split()
            # Accept multi-word names, hyphenated names, or names ≥ 4 chars
            if len(cwords) >= 2 or "-" in candidate or len(candidate) >= 4:
                fm_client = candidate

    # ── Fleet: "fleet <name>" or "for fleet <name>" ──────────────────────────
    pat_fleet = (
        r"\b(?:for\s+)?fleet\s+"
        r"([A-Za-z0-9][A-Za-z0-9\s_\-]*?)"
        r"(?:\s+(?:and\b|for\b|today|yesterday|this|last|previous|summary|"
        r"analytics|total|distance|trips|uptime|utilization|availability|"
        r"status|battery|mode|activity|sherpa|tug)|$)"
    )
    m = re.search(pat_fleet, original_text, re.IGNORECASE)
    if m:
        fleet = m.group(1).strip().rstrip(",.")

    return fm_client, fleet


def _extract_metrics(t: str) -> list:
    """Return all ALLOWED_ITEMS metrics mentioned in lowercased text t.

    Checks _METRIC_ALIASES first (longer phrases win), then keyword checks.
    "sherpa wise" / "by sherpa" / "per sherpa" anywhere in text upgrades
    total_trips → sherpa_wise_trips and total_distance_km → sherpa_wise_distance.
    Returns [] if nothing matches.
    """
    t_orig = t  # save before alias scan modifies it
    found = []

    # ── Alias scan first (sorted longest → shortest to avoid partial matches) ─
    for phrase in sorted(_METRIC_ALIASES, key=len, reverse=True):
        if phrase in t:
            canonical = _METRIC_ALIASES[phrase]
            if canonical not in found:
                found.append(canonical)
            # Remove the matched phrase from further matching to avoid double-hits
            t = t.replace(phrase, " ", 1)

    # High-specificity checks first
    if "top" in t and ("route" in t or "routes" in t) and "takt" in t:
        found.append("top_10_routes_takt")
    if "route" in t and ("utilization" in t or "utilisation" in t):
        found.append("route_utilization")
    if "obstacle" in t and ("route" in t or "routes" in t):
        found.append("avg_obstacle_per_route")
    if "obstacle" in t and ("sherpa" in t or "per sherpa" in t or "per-sherpa" in t):
        found.append("avg_obstacle_per_sherpa")
    if ("obstacle" in t and "time" in t) and "avg_obstacle_per_sherpa" not in found and "avg_obstacle_per_route" not in found:
        found.append("avg_obstacle_time")
    if ("takt time" in t or ("takt" in t and "time" in t) or "average takt" in t) and "top_10_routes_takt" not in found:
        found.append("takt_time")
    if "total trips" in t or ("trips" in t and "total" in t):
        found.append("total_trips")
    elif "trips" in t:
        found.append("total_trips")
    if "total distance" in t or ("distance" in t and "total" in t):
        found.append("total_distance_km")
    elif "distance" in t:
        found.append("total_distance_km")
    if "uptime" in t:
        found.append("uptime")
    if "availability" in t:
        found.append("availability")
    if "utilization" in t or "utilisation" in t:
        if "route_utilization" not in found:
            found.append("utilization")
    if "status" in t and ("tug" in t or "sherpa" in t):
        found.append("sherpa_status")

    # Deduplicate while preserving order
    seen = set()
    result = []
    for m in found:
        if m not in seen:
            seen.add(m)
            result.append(m)

    # "sherpa wise" / "by sherpa" / "per sherpa" used as a global modifier:
    # upgrades total_trips → sherpa_wise_trips, total_distance_km → sherpa_wise_distance
    _SHERPA_MOD = ("sherpa wise", "sherpa-wise", "by sherpa", "per sherpa")
    if any(mod in t_orig for mod in _SHERPA_MOD):
        _UPGRADE = {
            "total_trips":      "sherpa_wise_trips",
            "total_distance_km": "sherpa_wise_distance",
        }
        result = [_UPGRADE.get(m, m) for m in result]
        # Re-deduplicate in case both aliases resolved to the same canonical
        seen2, result2 = set(), []
        for m in result:
            if m not in seen2:
                seen2.add(m)
                result2.append(m)
        result = result2

    return result


def _extract_sherpa(t: str) -> Optional[str]:
    """Extract tug-NNN style sherpa hint from lowercased text."""
    if "per sherpa" in t or "per-sherpa" in t or "by sherpa" in t:
        return None  # "all sherpas" mode — no specific hint
    m = re.search(
        r"\b(tug[- ]?\d+(?:[- ][a-z0-9\-]+)*?)"
        r"(?:\s+(?:for|client|fleet|today|yesterday|this|last|previous|summary|"
        r"analytics|total|distance|trips|uptime|utilization|availability|"
        r"status|battery|mode|activity|sherpa|tug)\b|$)",
        t, re.IGNORECASE,
    )
    return m.group(1).replace(" ", "-").lower() if m else None


def _extract_time(t: str, original_text: str) -> Optional[str]:
    """Extract time phrase from text; returns None if not found."""
    # Normalize "a.m."/"a.m"/"p.m."/"p.m" → "am"/"pm" (trailing dot optional)
    t            = re.sub(r'\ba\.m\.?', 'am', t,             flags=re.IGNORECASE)
    t            = re.sub(r'\bp\.m\.?', 'pm', t,             flags=re.IGNORECASE)
    original_text = re.sub(r'\ba\.m\.?', 'am', original_text, flags=re.IGNORECASE)
    original_text = re.sub(r'\bp\.m\.?', 'pm', original_text, flags=re.IGNORECASE)

    # ── Hour-qualified ranges FIRST (before keyword shortcuts) ───────────────
    # Catches: "yesterday 7am to 7pm", "today 9:00 to 17:00", "7am to 7pm yesterday"
    _TIME_TOKEN = r"\d{1,2}(?::\d{2})?\s*(?:am|pm)|\d{1,2}:\d{2}"
    _DAY_TOKEN  = r"yesterday|today|day before yesterday"
    hour_range = re.search(
        rf"\b({_DAY_TOKEN})\s+({_TIME_TOKEN})\s+to\s+({_TIME_TOKEN})\b",
        t, re.IGNORECASE,
    )
    if hour_range:
        return hour_range.group(0).strip()
    # Also: "7am to 7pm yesterday/today"
    hour_range2 = re.search(
        rf"\b({_TIME_TOKEN})\s+to\s+({_TIME_TOKEN})\s+({_DAY_TOKEN})\b",
        t, re.IGNORECASE,
    )
    if hour_range2:
        return hour_range2.group(0).strip()

    for key in [
        "today", "yesterday", "day before yesterday",
        "last hour", "last 24 hours",
        "this week", "previous week", "last week",
        "this month", "previous month", "last month",
        "this quarter", "last quarter",
    ]:
        if key in t:
            return key

    # last N hours / days
    m = re.search(r"\blast\s+(\d+)\s*(hours?|days?)\b", t)
    if m:
        return m.group(0)

    # ── Date+time range: "7 march 8am to 12 march 8pm" (already normalized) ──
    _DATE_PART = r"\d{1,2}(?:st|nd|rd|th)?\s+\w+"          # "7 march", "12th april"
    _OPT_YEAR  = r"(?:\s+\d{4})?"                           # optional year
    _TIME_PART = r"\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)"      # "8am", "8:30 pm"
    datetime_range_m = re.search(
        rf"({_DATE_PART}{_OPT_YEAR}{_TIME_PART})"
        r"\s+to\s+"
        rf"({_DATE_PART}{_OPT_YEAR}{_TIME_PART})",
        original_text, re.IGNORECASE,
    )
    if datetime_range_m:
        return datetime_range_m.group(0).strip()

    # Explicit date range: "1 Jan 2026 to 5 Jan 2026"
    date_range_m = re.search(
        r"("
        r"\d{4}-\d{2}-\d{2}"
        r"|(?:\d{1,2}(?:st|nd|rd|th)?\s+)?\w+\s+\d{4}"
        r"|(?:\d{1,2}(?:st|nd|rd|th)?\s+)?\w+\s+\d{2}"
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
        return date_range_m.group(0).strip()

    # Single explicit date
    single_m = re.search(
        r"\b("
        r"\d{4}-\d{2}-\d{2}"
        r"|\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{4}"
        r"|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}"
        r")\b",
        original_text, re.IGNORECASE,
    )
    return single_m.group(0).strip() if single_m else None


def _heuristic_parse(text: str) -> ParsedQuery:
    original_text = text.strip()
    t = original_text.lower()

    if t in {"help", "?", "what can i ask", "what else can i ask"}:
        return ParsedQuery(intent="help")

    fm_client, fleet = _extract_names(original_text)
    sherpa = _extract_sherpa(t)
    time_phrase = _extract_time(t, original_text)
    metrics = _extract_metrics(t)
    _from_text = bool(fm_client)  # True only when regex found a client in the prompt

    # ── Multi-metric ─────────────────────────────────────────────────────────
    if len(metrics) > 1:
        pq = ParsedQuery(
            intent="multi_metric",
            item=metrics[0],
            items=metrics,
            fm_client_name=fm_client,
            fleet_name=fleet,
            sherpa_hint=sherpa,
            time_phrase=time_phrase,
            client_from_text=_from_text,
        )
        return pq

    # ── Single metric ─────────────────────────────────────────────────────────
    if len(metrics) == 1:
        pq = ParsedQuery(
            intent="basic_analytics_item",
            item=metrics[0],
            items=metrics,
            fm_client_name=fm_client,
            fleet_name=fleet,
            sherpa_hint=sherpa,
            time_phrase=time_phrase,
            client_from_text=_from_text,
        )
        if "per_sherpa" in metrics[0] or "per-sherpa" in metrics[0]:
            pq.sherpa_hint = None
        return pq

    # ── Analytics summary ─────────────────────────────────────────────────────
    if "summary" in t or ("analytics" in t and "takt" not in t):
        return ParsedQuery(
            intent="basic_analytics",
            fm_client_name=fm_client,
            fleet_name=fleet,
            sherpa_hint=sherpa,
            time_phrase=time_phrase,
            client_from_text=_from_text,
        )

    # ── Fallback: general summary ─────────────────────────────────────────────
    return ParsedQuery(
        intent="basic_analytics",
        fm_client_name=fm_client,
        fleet_name=fleet,
        sherpa_hint=sherpa,
        time_phrase=time_phrase,
        client_from_text=_from_text,
    )


async def ollama_pick_client(
    text: str,
    known_clients: list,
    model: Optional[str] = None,
) -> tuple:
    """Tier 2: Ask Ollama to identify the client from a constrained known-clients list.

    Returns (exact_client_name | None, confidence_score 0–100).
    confidence is derived by how well the picked name matches the original query
    text — so a hallucinated pick that doesn't appear in the text gets a low score.
    Falls back to (None, 0.0) if Ollama is unavailable or picks nothing plausible.
    """
    if not known_clients or not text.strip():
        return None, 0.0

    use_ollama = os.environ.get("OLLAMA_ENABLE", "1") == "1"
    if not use_ollama:
        return None, 0.0

    _model = model or os.environ.get("OLLAMA_MODEL", "llama3.2:3b")

    prompt_str = (
        "You are a client name extractor for a robotics fleet analytics system.\n"
        "A user typed a query. Identify which client they are referring to.\n"
        "IMPORTANT: You MUST pick from the provided list only, or return null.\n"
        "Return ONLY valid JSON: {\"client\": \"<exact name from list>\"} or {\"client\": null}\n\n"
        f"Known clients: {json.dumps(known_clients)}\n\n"
        f"User query: {text}\n"
    )

    try:
        obj = await _ollama_json(prompt_str, model=_model)
        picked = obj.get("client")
        if not picked or not isinstance(picked, str):
            return None, 0.0

        # Step 1: Find the exact match in known_clients (Ollama may alter case/spacing)
        picked_lower = picked.strip().lower()
        exact_match = next(
            (n for n in known_clients if n.lower() == picked_lower), None
        )

        if not exact_match:
            # Step 2: Fuzzy-find closest in list (handles minor Ollama edits)
            from rapidfuzz import fuzz as _fuzz
            best_score, best_name = 0.0, None
            for name in known_clients:
                s = max(
                    _fuzz.WRatio(picked_lower, name.lower()),
                    _fuzz.token_sort_ratio(picked_lower, name.lower()),
                )
                if s > best_score:
                    best_score, best_name = s, name
            if best_score < 85 or best_name is None:
                logger.info("Ollama Tier-2: picked '%s' not close to any known client (best=%.1f)", picked, best_score)
                return None, 0.0
            exact_match = best_name

        # Step 3: Validate — how much of the picked name appears in the query text?
        # This prevents using a plausible-but-wrong pick when the name isn't in the text.
        from rapidfuzz import fuzz as _fuzz
        norm_pick  = re.sub(r"[-_]+", " ", exact_match).lower()
        norm_query = re.sub(r"[-_]+", " ", text).lower()
        confidence = max(
            _fuzz.partial_ratio(norm_pick, norm_query),
            _fuzz.token_set_ratio(norm_pick, norm_query),
        )

        logger.info(
            "Ollama Tier-2 pick: '%s' → '%s' (confidence=%.1f)",
            picked, exact_match, confidence,
        )
        return exact_match, float(confidence)

    except Exception as exc:
        logger.warning("ollama_pick_client failed: %s", exc)
        return None, 0.0


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
        "intent": "basic_analytics | basic_analytics_item | multi_metric | help",
        "items": (
            "List of canonical metric names from the allowed list. "
            "IMPORTANT: if the user asks for multiple metrics (comma- or 'and'-separated), "
            "include ALL of them and set intent to 'multi_metric'. "
            "For a single metric set intent to 'basic_analytics_item'. "
            "Empty list for summary/help."
        ),
        "sherpa_hint": "string like tug-104 or null",
        "timezone": "IANA timezone or null",
        "time_phrase": "string or null",
    }

    # Build alias examples from config so Ollama learns every synonym
    _alias_lines = "\n".join(
        f'  "{phrase}" → {canonical}'
        for phrase, canonical in _METRIC_ALIASES.items()
    )

    prompt = (
        "You are a parser for a robotics analytics chatbot. "
        "Return ONLY valid JSON matching this schema: "
        f"{json.dumps(schema)}\n\n"
        "Allowed metric names (use ONLY these):\n"
        f"{sorted(ALLOWED_ITEMS)}\n\n"
        "Metric synonyms — map user phrases to the canonical name:\n"
        f"{_alias_lines}\n\n"
        "Examples:\n"
        '- "uptime and obstacle time" → {"intent":"multi_metric","items":["uptime","avg_obstacle_time"],...}\n'
        '- "total trips, distance and takt time" → {"intent":"multi_metric","items":["total_trips","total_distance_km","takt_time"],...}\n'
        '- "uptime" → {"intent":"basic_analytics_item","items":["uptime"],...}\n'
        '- "basic analytics" → {"intent":"basic_analytics","items":[],...}\n'
        '- "tug wise distance" or "bot wise distance" → {"intent":"basic_analytics_item","items":["sherpa_wise_distance"],...}\n'
        '- "tug wise trips" or "bot wise" or "per tug" → {"intent":"basic_analytics_item","items":["sherpa_wise_trips"],...}\n'
        '- "obstacle time" or "takt time" → use canonical names avg_obstacle_time / takt_time\n'
        '- "route utilization" or "route usage" → {"intent":"basic_analytics_item","items":["route_utilization"],...}\n\n'
        "User text: "
        f"{text}\n"
    )

    # Always extract names deterministically — never trust the LLM for these
    hq = _heuristic_parse(text)

    try:
        obj = await _ollama_json(prompt, model=model)

        # Parse items array from Ollama response
        raw_items = obj.get("items") or []
        if isinstance(raw_items, str):
            raw_items = [raw_items]
        valid_items = [i for i in raw_items if isinstance(i, str) and i in ALLOWED_ITEMS]

        if len(valid_items) > 1:
            resolved_intent = "multi_metric"
        elif len(valid_items) == 1:
            resolved_intent = "basic_analytics_item"
        else:
            resolved_intent = str(obj.get("intent") or "basic_analytics")

        # Safety net 1: heuristic found sherpa-wise — always trust it (alias upgrade is reliable)
        if hq.items and any("sherpa_wise" in i for i in hq.items):
            use_items = hq.items
        else:
            use_items = valid_items

        # Safety net 2: if Ollama found nothing but heuristic did, use heuristic result
        if not use_items and hq.items:
            logger.debug("Ollama returned no items; falling back to heuristic items: %s", hq.items)
            use_items = hq.items

        if len(use_items) > 1:
            resolved_intent = "multi_metric"
        elif len(use_items) == 1:
            resolved_intent = "basic_analytics_item"

        pq = ParsedQuery(
            intent=resolved_intent,
            item=use_items[0] if use_items else obj.get("item"),
            items=use_items,
            # Names always come from heuristic parser, never the LLM
            fm_client_name=hq.fm_client_name,
            fleet_name=hq.fleet_name,
            # Prefer heuristic sherpa (more precise tug-NNN check), fall back to Ollama
            sherpa_hint=hq.sherpa_hint or obj.get("sherpa_hint"),
            timezone=obj.get("timezone") or hq.timezone,
            # Prefer heuristic time (handles hour-ranges Ollama can't), fall back to Ollama
            time_phrase=hq.time_phrase or obj.get("time_phrase"),
            # client_from_text always comes from heuristic — it reflects actual text extraction
            client_from_text=hq.client_from_text,
        )
    except Exception as e:
        logger.warning("Ollama parse failed, falling back to heuristics: %s", e)
        pq = hq

    # Ensure items is always in sync with item
    if pq.item and not pq.items:
        pq.items = [pq.item]

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
                # Hint appears in text (e.g. "sherpa", "by") but is not a specific tug ID — clear it
                logger.info(f"Clearing sherpa_hint '{pq.sherpa_hint}' — appears in query but not as a valid tug-NNN pattern")
                pq.sherpa_hint = None

    # sanitize
    if pq.item and pq.item not in ALLOWED_ITEMS:
        pq.item = None
        pq.intent = "basic_analytics"

    # Sanitize items list — remove anything not in ALLOWED_ITEMS
    pq.items = [i for i in pq.items if i in ALLOWED_ITEMS]

    if pq.intent not in {"basic_analytics", "basic_analytics_item", "multi_metric", "help"}:
        pq.intent = "basic_analytics"

    _apply_defaults(pq, defaults)
    return pq


def _apply_defaults(pq: ParsedQuery, defaults: Dict[str, str]) -> None:
    pq.fm_client_name = pq.fm_client_name or defaults.get("fm_client_name")
    pq.fleet_name = pq.fleet_name or defaults.get("fleet_name")
    pq.timezone = pq.timezone or defaults.get("timezone")
    if pq.time_phrase is None:
        pq.time_phrase = defaults.get("time_phrase")
