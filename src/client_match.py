"""Client and sherpa name resolution using exact + fuzzy matching (RapidFuzz).

Used everywhere we need to map a user-typed name to the real API name.
Keeps all matching logic in one place so thresholds and scorers are consistent.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

from rapidfuzz import fuzz

logger = logging.getLogger("fm_mcp")

# Minimum similarity score to accept a fuzzy match (0–100).
DEFAULT_MIN_SCORE: int = 85

# Lower threshold for prompt scanning (substring n-gram matching).
PROMPT_SCAN_MIN_SCORE: int = 78


def _normalize(s: str) -> str:
    """Lowercase + collapse hyphens/underscores to spaces for robust matching.

    "YOKOHAMA-DAHEJ" → "yokohama dahej"
    "CEAT_Nagpur"    → "ceat nagpur"
    """
    return re.sub(r"[-_]+", " ", s).strip().lower()


def _best_score(needle: str, choice: str) -> float:
    """Return the best score across multiple RapidFuzz scorers.

    Uses both the raw strings and their normalised forms so hyphens vs spaces
    never cause a miss.
    """
    n_norm = _normalize(needle)
    c_norm = _normalize(choice)
    return max(
        fuzz.WRatio(needle, choice),
        fuzz.token_sort_ratio(n_norm, c_norm),
        fuzz.partial_ratio(n_norm, c_norm),
    )


def resolve_client(
    user_client_name: str,
    all_clients: List[Dict],
    min_score: int = DEFAULT_MIN_SCORE,
) -> Optional[Dict]:
    """Return the best-matching client dict for the given user-typed name.

    Resolution order:
    1. Exact match (case-insensitive).
    2. Normalized exact match (hyphens/underscores → spaces).
    3. Multi-scorer fuzzy match (WRatio + token_sort + partial, on normalized forms).
    4. Returns None if no match above threshold.
    """
    if not user_client_name or not all_clients:
        return None

    name_map: Dict[str, Dict] = {}
    for c in all_clients:
        if not isinstance(c, dict):
            continue
        name = c.get("fm_client_name") or ""
        if name:
            name_map[name.lower()] = c

    if not name_map:
        return None

    needle = user_client_name.strip().lower()
    needle_norm = _normalize(needle)

    # ── Step 1: exact case-insensitive ───────────────────────────────────────
    if needle in name_map:
        logger.debug("Client exact match: '%s'", user_client_name)
        return name_map[needle]

    # ── Step 2: normalized exact (hyphens→spaces) ────────────────────────────
    for raw_name, client_dict in name_map.items():
        if _normalize(raw_name) == needle_norm:
            logger.info("Client normalized match: '%s' → '%s'", user_client_name, raw_name)
            return client_dict

    # ── Step 3: multi-scorer fuzzy match ─────────────────────────────────────
    best_raw_score = 0.0
    best_choice: Optional[str] = None

    for raw_name in name_map:
        score = _best_score(needle, raw_name)
        if score > best_raw_score:
            best_raw_score = score
            best_choice = raw_name

    if best_raw_score >= min_score and best_choice is not None:
        matched_client = name_map[best_choice]
        logger.info(
            "Client fuzzy match: '%s' → '%s' (score=%.1f)",
            user_client_name,
            matched_client.get("fm_client_name"),
            best_raw_score,
        )
        return matched_client

    # Log top-3 for debugging
    scored = sorted(
        ((raw, _best_score(needle, raw)) for raw in name_map),
        key=lambda x: -x[1],
    )[:3]
    logger.info(
        "No client match for '%s' (threshold=%d). Top candidates: %s",
        user_client_name,
        min_score,
        [(n, round(s, 1)) for n, s in scored],
    )
    return None


def resolve_client_candidates(
    user_client_name: str,
    all_clients: List[Dict],
    min_score: int = DEFAULT_MIN_SCORE,
) -> List[Tuple[Dict, float]]:
    """Return all clients that score >= min_score, sorted by score descending.

    Use this instead of resolve_client when you want to detect ambiguity
    (multiple close matches) and present them to the user for disambiguation.

    Returns
    -------
    List of (client_dict, score) tuples, best match first.
    Empty list if nothing meets the threshold.
    """
    if not user_client_name or not all_clients:
        return []

    name_map: Dict[str, Dict] = {}
    for c in all_clients:
        if not isinstance(c, dict):
            continue
        name = c.get("fm_client_name") or ""
        if name:
            name_map[name.lower()] = c

    if not name_map:
        return []

    needle = user_client_name.strip().lower()
    needle_norm = _normalize(needle)

    # ── Step 1: exact case-insensitive → single definitive match ─────────────
    if needle in name_map:
        logger.debug("Client exact match: '%s'", user_client_name)
        return [(name_map[needle], 100.0)]

    # ── Step 2: normalized exact ──────────────────────────────────────────────
    for raw_name, client_dict in name_map.items():
        if _normalize(raw_name) == needle_norm:
            logger.info("Client normalized match: '%s' → '%s'", user_client_name, raw_name)
            return [(client_dict, 100.0)]

    # ── Step 3: fuzzy — collect ALL above threshold ───────────────────────────
    scored = [
        (client_dict, _best_score(needle, raw_name))
        for raw_name, client_dict in name_map.items()
    ]
    candidates = [
        (d, s) for d, s in scored if s >= min_score
    ]
    candidates.sort(key=lambda x: -x[1])

    if candidates:
        for d, s in candidates:
            logger.info(
                "Client fuzzy candidate: '%s' → '%s' (score=%.1f)",
                user_client_name, d.get("fm_client_name"), s,
            )
    else:
        top3 = sorted(scored, key=lambda x: -x[1])[:3]
        logger.info(
            "No client candidates for '%s' (threshold=%d). Top: %s",
            user_client_name,
            min_score,
            [(d.get("fm_client_name"), round(s, 1)) for d, s in top3],
        )

    return candidates


def resolve_sherpa(
    user_sherpa_name: str,
    all_sherpas: List[Dict],
    min_score: int = DEFAULT_MIN_SCORE,
) -> Optional[str]:
    """Resolve a user-typed sherpa name to the exact API sherpa_name string.

    Resolution order:
    1. Exact match (case-insensitive).
    2. Normalized exact match.
    3. Prefix match — "tug-104" matches "tug-104-ceat-nagpur-12".
    4. Multi-scorer fuzzy match.
    5. Returns None if nothing meets min_score.

    Parameters
    ----------
    user_sherpa_name : str
        e.g. "tug-104", "tug 104", "TUG104"
    all_sherpas : list of dict
        Each dict should have a "sherpa_name" key.
    min_score : int
        Minimum score threshold (default 85).

    Returns
    -------
    str | None
        Exact API sherpa name, or None if no match.
    """
    if not user_sherpa_name or not all_sherpas:
        return None

    # Build lookup: lower → actual name
    name_map: Dict[str, str] = {}
    for s in all_sherpas:
        if not isinstance(s, dict):
            continue
        name = s.get("sherpa_name") or ""
        if name:
            name_map[name.lower()] = name

    if not name_map:
        return None

    needle = user_sherpa_name.strip().lower()
    needle_norm = _normalize(needle)

    # ── Step 1: exact ────────────────────────────────────────────────────────
    if needle in name_map:
        logger.debug("Sherpa exact match: '%s'", user_sherpa_name)
        return name_map[needle]

    # ── Step 2: normalized exact (hyphens→spaces) ────────────────────────────
    for raw, actual in name_map.items():
        if _normalize(raw) == needle_norm:
            logger.info("Sherpa normalized match: '%s' → '%s'", user_sherpa_name, actual)
            return actual

    # ── Step 3: prefix — "tug-104" → "tug-104-ceat-nagpur-12" ───────────────
    for raw, actual in name_map.items():
        if raw.startswith(needle) or needle.startswith(raw):
            logger.info("Sherpa prefix match: '%s' → '%s'", user_sherpa_name, actual)
            return actual

    # ── Step 4: multi-scorer fuzzy ────────────────────────────────────────────
    best_raw_score = 0.0
    best_choice: Optional[str] = None
    for raw in name_map:
        score = _best_score(needle, raw)
        if score > best_raw_score:
            best_raw_score = score
            best_choice = raw

    if best_raw_score >= min_score and best_choice is not None:
        actual = name_map[best_choice]
        logger.info(
            "Sherpa fuzzy match: '%s' → '%s' (score=%.1f)",
            user_sherpa_name,
            actual,
            best_raw_score,
        )
        return actual

    scored = sorted(
        ((raw, _best_score(needle, raw)) for raw in name_map),
        key=lambda x: -x[1],
    )[:3]
    logger.info(
        "No sherpa match for '%s' (threshold=%d). Top candidates: %s",
        user_sherpa_name,
        min_score,
        [(n, round(s, 1)) for n, s in scored],
    )
    return None


def scan_prompt_for_client(
    prompt_text: str,
    all_clients: List[Dict],
    min_score: int = PROMPT_SCAN_MIN_SCORE,
    max_ngram: int = 4,
) -> tuple[Optional[Dict], float]:
    """Scan a free-text prompt for any substring that fuzzy-matches a known client name.

    Called when NLU returns fm_client_name=None.

    Returns
    -------
    (dict | None, float)
        Tuple of (best-matching client dict or None, best score 0–100).
    """
    if not prompt_text or not all_clients:
        return None, 0.0

    name_map: Dict[str, Dict] = {}
    for c in all_clients:
        if not isinstance(c, dict):
            continue
        name = c.get("fm_client_name") or ""
        if name:
            name_map[name.lower()] = c

    if not name_map:
        return None, 0.0

    words = re.findall(r"[a-zA-Z0-9][a-zA-Z0-9\-]*", prompt_text)

    _STOP = {
        "for", "the", "a", "an", "of", "in", "at", "to", "on", "by",
        "with", "and", "or", "is", "are", "was", "were", "get", "show",
        "give", "what", "which", "how", "tell", "me", "us", "all", "fleet",
        "client", "today", "yesterday", "this", "last", "week", "month",
        "year", "time", "date", "report", "summary", "analytics", "data",
        "basic", "uptime", "trips", "distance", "utilization", "status",
        "sherpa", "tug", "route",
    }

    best_score = 0.0
    best_client: Optional[Dict] = None

    # Also score the full normalised prompt against each client name
    prompt_norm = _normalize(prompt_text)
    for raw_name, client_dict in name_map.items():
        score = _best_score(prompt_norm, _normalize(raw_name))
        if score > best_score and score >= min_score:
            best_score = score
            best_client = client_dict

    # N-gram scan
    for n in range(1, max_ngram + 1):
        for i in range(len(words) - n + 1):
            ngram = " ".join(words[i: i + n]).lower()
            if all(w in _STOP for w in ngram.split()):
                continue
            if n == 1 and len(ngram) < 3:
                continue
            ngram_norm = _normalize(ngram)
            for raw_name, client_dict in name_map.items():
                score = _best_score(ngram_norm, _normalize(raw_name))
                if score > best_score:
                    best_score = score
                    best_client = client_dict if score >= min_score else best_client

    if best_client and best_score >= min_score:
        logger.info(
            "Prompt scan found client: '%s' (score=%.1f) in prompt: '%s'",
            best_client.get("fm_client_name"),
            best_score,
            prompt_text,
        )
        return best_client, best_score

    logger.info(
        "Prompt scan: no client found in '%s' (threshold=%d, best=%.1f)",
        prompt_text,
        min_score,
        best_score,
    )
    return None, 0.0
