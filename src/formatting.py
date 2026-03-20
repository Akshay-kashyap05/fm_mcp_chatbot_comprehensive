from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("fm_mcp")


def _find_sherpa_entry(
    entries: List[Dict[str, Any]],
    sherpa_hint: str,
    name_key: str = "sherpa_name",
) -> Optional[Dict[str, Any]]:
    """Fuzzy match by startswith/contains for things like tug-104 -> tug-104-ceat-nagpur-11."""
    hint = (sherpa_hint or "").strip().lower()
    if not hint:
        return None

    # Exact
    for e in entries:
        if str(e.get(name_key, "")).lower() == hint:
            return e

    # Startswith
    for e in entries:
        if str(e.get(name_key, "")).lower().startswith(hint):
            return e

    # Contains
    for e in entries:
        if hint in str(e.get(name_key, "")).lower():
            return e

    return None


def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    """Build a GitHub-flavoured markdown table."""
    sep = ["-" * max(len(h), 3) for h in headers]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(sep) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def summarize_basic_analytics(payload: Dict[str, Any]) -> str:
    """Return a Markdown-formatted analytics summary."""
    if isinstance(payload.get("data"), dict):
        payload = payload["data"]

    sections: List[str] = []

    # ── Headline numbers ──────────────────────────────────────────────────────
    total_trips    = payload.get("total_trips")
    total_distance = payload.get("total_distance_km")
    headline_rows  = []
    if total_trips    is not None: headline_rows.append(["Total Trips",       str(total_trips)])
    if total_distance is not None: headline_rows.append(["Total Distance (km)", str(total_distance)])
    if headline_rows:
        sections.append(_md_table(["Metric", "Value"], headline_rows))

    # ── Trips by Sherpa ───────────────────────────────────────────────────────
    st = payload.get("sherpa_wise_trips") or []
    if st:
        top = sorted(st, key=lambda x: x.get("trip_count", 0), reverse=True)[:10]
        rows = [[r.get("sherpa_name", "?"), str(r.get("trip_count", 0))] for r in top]
        sections.append("**Trips by Sherpa** (top 10)\n" + _md_table(["Sherpa", "Trips"], rows))

    # ── Availability ──────────────────────────────────────────────────────────
    av = payload.get("availability") or []
    if av:
        rows = [
            [r.get("sherpa_name", "?"),
             f"{r.get('availability_percentage') or r.get('availability', 0):.1f}%"]
            for r in av
        ]
        sections.append("**Availability**\n" + _md_table(["Sherpa", "Availability"], rows))

    # ── Utilization ───────────────────────────────────────────────────────────
    util = payload.get("utilization") or []
    if util:
        rows = [
            [r.get("sherpa_name", "?"), f"{r.get('utilization', 0):.1f}%"]
            for r in util
        ]
        sections.append("**Utilization**\n" + _md_table(["Sherpa", "Utilization"], rows))

    # ── Sherpa-wise distance ──────────────────────────────────────────────────
    swd = payload.get("sherpa_wise_distance") or []
    if swd:
        rows = []
        for r in swd:
            dist = r.get("distance_km") or r.get("total_distance_km") or r.get("distance", 0)
            rows.append([r.get("sherpa_name", "?"), f"{dist:.2f}" if isinstance(dist, float) else str(dist)])
        sections.append("**Distance by Sherpa (km)**\n" + _md_table(["Sherpa", "Distance (km)"], rows))

    # ── Uptime ────────────────────────────────────────────────────────────────
    upt = payload.get("uptime") or []
    if upt:
        rows = [
            [r.get("sherpa_name", "?"),
             f"{r.get('uptime_percentage') or r.get('uptime', 0):.1f}%"]
            for r in upt
        ]
        sections.append("**Uptime**\n" + _md_table(["Sherpa", "Uptime"], rows))

    # ── Avg Takt Time per Sherpa ──────────────────────────────────────────────
    takt = payload.get("avg_takt_per_sherpa") or []
    if takt:
        rows = []
        for r in sorted(takt, key=lambda x: x.get("avg_takt_time_minutes", 0), reverse=True)[:10]:
            rows.append([
                r.get("sherpa", "?"),
                f"{r.get('avg_takt_time_minutes', 0):.2f}",
                f"{r.get('min_takt_time_minutes', 0):.2f}",
                f"{r.get('max_takt_time_minutes', 0):.2f}",
                str(r.get("total_trips", 0)),
            ])
        sections.append(
            "**Avg Takt Time per Sherpa (min)**\n"
            + _md_table(["Sherpa", "Avg", "Min", "Max", "Trips"], rows)
        )

    # ── Avg Obstacle Time per Sherpa ──────────────────────────────────────────
    obs_sherpa = payload.get("avg_obstacle_per_sherpa") or []
    if obs_sherpa:
        rows = [
            [r.get("sherpa_name", "?"), f"{r.get('avg_obstacle_time_min', 0):.2f}"]
            for r in sorted(obs_sherpa, key=lambda x: x.get("avg_obstacle_time_min", 0), reverse=True)
        ]
        sections.append("**Avg Obstacle Time per Sherpa (min)**\n" + _md_table(["Sherpa", "Avg Obstacle (min)"], rows))

    # ── Top Routes by Takt Time ───────────────────────────────────────────────
    top_routes = payload.get("top_10_routes_takt") or []
    if top_routes:
        rows = []
        for r in top_routes[:10]:
            route = r.get("route", [])
            route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
            rows.append([route_str, f"{r.get('avg_takt_time_minutes', 0):.2f}"])
        sections.append("**Top Routes by Takt Time (min)**\n" + _md_table(["Route", "Avg Takt (min)"], rows))

    # ── Route Utilization ─────────────────────────────────────────────────────
    route_util = payload.get("route_utilization") or []
    if route_util:
        rows = []
        for r in sorted(route_util, key=lambda x: x.get("utilization", 0), reverse=True):
            route = r.get("route", [])
            route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
            rows.append([route_str, f"{r.get('utilization', 0):.2f}%"])
        sections.append("**Route Utilization**\n" + _md_table(["Route", "Utilization"], rows))

    # ── Avg Obstacle Time per Route ───────────────────────────────────────────
    obs_route = payload.get("avg_obstacle_per_route") or []
    if obs_route:
        rows = []
        for r in sorted(obs_route, key=lambda x: x.get("avg_obstacle_time_min", 0), reverse=True):
            route = r.get("route", [])
            route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
            rows.append([route_str, f"{r.get('avg_obstacle_time_min', 0):.2f}"])
        sections.append("**Avg Obstacle Time per Route (min)**\n" + _md_table(["Route", "Avg Obstacle (min)"], rows))

    return "\n\n".join(sections) if sections else "No data available."


def extract_item_value(payload: Dict[str, Any], item: str, sherpa_hint: Optional[str] = None) -> Tuple[Any, str]:
    """Return (value, note). Note explains any fuzzy match applied."""
    item = (item or "").strip()
    note = ""

    # Handle total_distance_km - check sherpa_wise_distance first (even if no sherpa hint)
    # This allows returning all sherpas when sherpa_hint is None
    if item in ("total_distance_km", "total_distance", "distance"):
        if sherpa_hint:
            entries = payload.get("sherpa_wise_distance") or []
            e = _find_sherpa_entry(entries, sherpa_hint)
            if e:
                if str(e.get('sherpa_name','')).lower() != sherpa_hint.lower():
                    note = f"Matched '{sherpa_hint}' to '{e.get('sherpa_name')}'."
                return e.get("distance_km") or e.get("total_distance_km") or e.get("distance"), note
            return 0, f"No matching Sherpa found in sherpa_wise_distance list."
        # No sherpa hint - prefer top-level total_distance_km if available
        # The API returns total_distance_km at the top level when querying all sherpas
        top_level_total = payload.get("total_distance_km") or payload.get("total_distance") or payload.get("distance")
        if top_level_total is not None and top_level_total != 0:
            return top_level_total, note
        
        # If top-level is 0 or missing, try summing sherpa_wise_distance
        entries = payload.get("sherpa_wise_distance") or []
        if entries:
            total = 0.0
            for entry in entries:
                distance = entry.get("distance_km") or entry.get("total_distance_km") or entry.get("distance", 0)
                if isinstance(distance, (int, float)):
                    total += float(distance)
            if total > 0:
                return total, "Sum of all sherpas"
        
        # Fallback to top-level value (even if 0)
        return top_level_total if top_level_total is not None else 0, note

    # Handle total_trips - check sherpa_wise_trips first (even if no sherpa hint)
    # This allows returning all sherpas when sherpa_hint is None
    if item in ("total_trips", "trips"):
        if sherpa_hint:
            entries = payload.get("sherpa_wise_trips") or []
            e = _find_sherpa_entry(entries, sherpa_hint)
            if e:
                if str(e.get('sherpa_name','')).lower() != sherpa_hint.lower():
                    note = f"Matched '{sherpa_hint}' to '{e.get('sherpa_name')}'."
                return e.get("trip_count") or e.get("total_trips") or e.get("trips"), note
            return 0, f"No matching Sherpa found in sherpa_wise_trips list."
        # No sherpa hint — return list of dicts so format_metric_value can render a table
        entries = payload.get("sherpa_wise_trips") or []
        if entries:
            return entries, "All sherpas"
        # Fallback to total if sherpa_wise_trips not available
        return payload.get("total_trips"), note

    # Direct top-level keys (for items that don't have sherpa-wise breakdowns)
    # Exclude route-analytics items that need their own handlers below (they return
    # lists of dicts keyed on "route", not "sherpa_name", and would render as "Unknown"
    # if allowed to fall through to format_metric_value's generic sherpa-table path).
    _NEEDS_ROUTE_HANDLER = {
        "takt_time", "average_takt_time",
        "avg_obstacle_per_sherpa", "avg_obstacle_time", "obstacle_time",
        "top_10_routes_takt", "top_routes_takt",
        "route_utilization",
        "avg_obstacle_per_route",
    }
    if item in payload and sherpa_hint is None and item not in _NEEDS_ROUTE_HANDLER:
        return payload.get(item), note

    # Normalized mapping for common queries
    # - uptime, availability, utilization are arrays of {sherpa_name, <metric>}
    if item in ("uptime", "uptime_percentage"):
        entries = payload.get("uptime") or []
        if sherpa_hint:
            e = _find_sherpa_entry(entries, sherpa_hint)
            if e:
                if str(e.get('sherpa_name','')).lower() != sherpa_hint.lower():
                    note = f"Matched '{sherpa_hint}' to '{e.get('sherpa_name')}'."
                return e.get("uptime_percentage"), note
            return 0, "No matching Sherpa found in uptime list."
        return entries, note

    if item in ("availability", "availability_percentage"):
        entries = payload.get("availability") or []
        if sherpa_hint:
            e = _find_sherpa_entry(entries, sherpa_hint)
            if e:
                if str(e.get('sherpa_name','')).lower() != sherpa_hint.lower():
                    note = f"Matched '{sherpa_hint}' to '{e.get('sherpa_name')}'."
                return e.get("availability_percentage"), note
            return 0, "No matching Sherpa found in availability list."
        return entries, note

    if item in ("utilization",):
        entries = payload.get("utilization") or []
        if sherpa_hint:
            e = _find_sherpa_entry(entries, sherpa_hint)
            if e:
                if str(e.get('sherpa_name','')).lower() != sherpa_hint.lower():
                    note = f"Matched '{sherpa_hint}' to '{e.get('sherpa_name')}'."
                return e.get("utilization"), note
            return 0, "No matching Sherpa found in utilization list."
        return entries, note

    if item in ("battery", "battery_level"):
        entries = payload.get("sherpa_status") or []
        if sherpa_hint:
            e = _find_sherpa_entry(entries, sherpa_hint)
            if e:
                if str(e.get('sherpa_name','')).lower() != sherpa_hint.lower():
                    note = f"Matched '{sherpa_hint}' to '{e.get('sherpa_name')}'."
                return e.get("battery_level"), note
            return 0, "No matching Sherpa found in sherpa_status list."
        return entries, note

    # Handle takt_time - API returns avg_takt_per_sherpa from route_analytics
    if item in ("takt_time", "average_takt_time", "takt", "avg_takt_time"):
        # API returns avg_takt_per_sherpa array
        entries = payload.get("avg_takt_per_sherpa") or []
        if isinstance(entries, list):
            if sherpa_hint:
                # Find matching sherpa (note: API uses "sherpa" not "sherpa_name")
                e = _find_sherpa_entry(entries, sherpa_hint, name_key="sherpa")
                if e:
                    sherpa_name = e.get("sherpa", "")
                    if str(sherpa_name).lower() != sherpa_hint.lower():
                        note = f"Matched '{sherpa_hint}' to '{sherpa_name}'."
                    # Return avg_takt_time_minutes (primary metric)
                    avg_takt = e.get("avg_takt_time_minutes")
                    if avg_takt is not None:
                        return avg_takt, note
                    # Fallback to other takt time fields
                    return e.get("min_takt_time_minutes") or e.get("max_takt_time_minutes"), note
                return 0, f"No matching Sherpa found in avg_takt_per_sherpa list."
            # No sherpa hint - return formatted summary of all sherpas
            if entries:
                # Format as a readable summary
                formatted_lines = []
                for entry in sorted(entries, key=lambda x: x.get("avg_takt_time_minutes", 0), reverse=True):
                    sherpa = entry.get("sherpa", "Unknown")
                    avg_takt = entry.get("avg_takt_time_minutes", 0)
                    min_takt = entry.get("min_takt_time_minutes", 0)
                    max_takt = entry.get("max_takt_time_minutes", 0)
                    trips = entry.get("total_trips", 0)
                    formatted_lines.append(
                        f"{sherpa}: {avg_takt:.2f} min (min: {min_takt:.2f}, max: {max_takt:.2f}), {trips} trips"
                    )
                return "\n".join(formatted_lines), note
            return "No takt time data available.", note
        
        # Fallback to direct key (if API structure changes)
        return payload.get("takt_time") or payload.get("average_takt_time") or payload.get("avg_takt_time"), note

    # Handle avg_obstacle_per_sherpa - from route_analytics
    if item in ("avg_obstacle_per_sherpa", "avg_obstacle_time", "obstacle_time"):
        entries = payload.get("avg_obstacle_per_sherpa") or []
        if isinstance(entries, list):
            # For avg_obstacle_per_sherpa item, always return all sherpas (ignore sherpa_hint)
            if item == "avg_obstacle_per_sherpa":
                if entries:
                    formatted_lines = []
                    for entry in sorted(entries, key=lambda x: x.get("avg_obstacle_time_min", 0), reverse=True):
                        sherpa = entry.get("sherpa_name", "Unknown")
                        obstacle_time = entry.get("avg_obstacle_time_min", 0)
                        formatted_lines.append(f"{sherpa}: {obstacle_time:.2f} min")
                    return "\n".join(formatted_lines), note
                return "No obstacle time data available.", note
            # For other obstacle_time queries, allow sherpa filtering
            elif sherpa_hint:
                # Find matching sherpa (note: API uses "sherpa_name" here)
                e = _find_sherpa_entry(entries, sherpa_hint, name_key="sherpa_name")
                if e:
                    sherpa_name = e.get("sherpa_name", "")
                    if str(sherpa_name).lower() != sherpa_hint.lower():
                        note = f"Matched '{sherpa_hint}' to '{sherpa_name}'."
                    return e.get("avg_obstacle_time_min"), note
                return 0, f"No matching Sherpa found in avg_obstacle_per_sherpa list."
            # No sherpa hint - return formatted summary
            if entries:
                formatted_lines = []
                for entry in sorted(entries, key=lambda x: x.get("avg_obstacle_time_min", 0), reverse=True):
                    sherpa = entry.get("sherpa_name", "Unknown")
                    obstacle_time = entry.get("avg_obstacle_time_min", 0)
                    formatted_lines.append(f"{sherpa}: {obstacle_time:.2f} min")
                return "\n".join(formatted_lines), note
            return "No obstacle time data available.", note
        return payload.get("avg_obstacle_time") or payload.get("obstacle_time"), note

    # Handle top_10_routes_takt - from route_analytics
    if item in ("top_10_routes_takt", "top_routes_takt"):
        entries = payload.get("top_10_routes_takt") or []
        if isinstance(entries, list) and entries:
            formatted_lines = []
            for entry in entries[:10]:  # Already top 10, but limit to be safe
                route = entry.get("route", [])
                avg_takt = entry.get("avg_takt_time_minutes", 0)
                route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
                formatted_lines.append(f"{route_str}: {avg_takt:.2f} min")
            return "\n".join(formatted_lines), note
        return "No route takt time data available.", note

    # Handle route_utilization - from route_analytics
    if item in ("route_utilization",):
        entries = payload.get("route_utilization") or []
        if isinstance(entries, list) and entries:
            formatted_lines = []
            for entry in sorted(entries, key=lambda x: x.get("utilization", 0), reverse=True):
                route = entry.get("route", [])
                utilization = entry.get("utilization", 0)
                route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
                label = route_str if route_str else entry.get("sherpa_name") or entry.get("sherpa", "(unknown)")
                formatted_lines.append(f"{label}: {utilization:.2f}%")
            return "\n".join(formatted_lines), note
        return "No route utilization data available.", note

    # Handle avg_obstacle_per_route - from route_analytics
    if item in ("avg_obstacle_per_route",):
        entries = payload.get("avg_obstacle_per_route") or []
        if isinstance(entries, list) and entries:
            formatted_lines = []
            for entry in sorted(entries, key=lambda x: x.get("avg_obstacle_time_min", 0), reverse=True):
                route = entry.get("route", [])
                obstacle_time = entry.get("avg_obstacle_time_min", 0)
                route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
                label = route_str if route_str else entry.get("sherpa_name") or entry.get("sherpa", "(unknown)")
                formatted_lines.append(f"{label}: {obstacle_time:.2f} min")
            return "\n".join(formatted_lines), note
        return "Not currently available — no obstacle-per-route data in your system yet.", note

    # Generic route analytics — return combined summary of all route sub-sections
    if item in ("route_analytics",):
        parts = []
        # Takt time per sherpa
        takt = payload.get("avg_takt_per_sherpa") or []
        if takt:
            lines = []
            for r in sorted(takt, key=lambda x: x.get("avg_takt_time_minutes", 0), reverse=True)[:10]:
                sherpa = r.get("sherpa", "?")
                lines.append(f"{sherpa}: {r.get('avg_takt_time_minutes', 0):.2f} min "
                              f"(min: {r.get('min_takt_time_minutes', 0):.2f}, "
                              f"max: {r.get('max_takt_time_minutes', 0):.2f}), "
                              f"{r.get('total_trips', 0)} trips")
            parts.append("**Takt Time per Sherpa**\n" + "\n".join(lines))
        # Route utilization
        ru = payload.get("route_utilization") or []
        if ru:
            lines = []
            for r in sorted(ru, key=lambda x: x.get("utilization", 0), reverse=True)[:10]:
                route = r.get("route", [])
                route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
                lines.append(f"{route_str or '(unknown route)'}: {r.get('utilization', 0):.2f}%")
            parts.append("**Route Utilization**\n" + "\n".join(lines))
        # Top routes by takt
        top = payload.get("top_10_routes_takt") or []
        if top:
            lines = []
            for r in top[:10]:
                route = r.get("route", [])
                route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
                lines.append(f"{route_str}: {r.get('avg_takt_time_minutes', 0):.2f} min")
            parts.append("**Top Routes by Takt Time**\n" + "\n".join(lines))
        # Obstacle per route
        opr = payload.get("avg_obstacle_per_route") or []
        if opr:
            lines = []
            for r in sorted(opr, key=lambda x: x.get("avg_obstacle_time_min", 0), reverse=True)[:10]:
                route = r.get("route", [])
                route_str = " → ".join(str(s) for s in route) if isinstance(route, list) else str(route)
                lines.append(f"{route_str or '(unknown route)'}: {r.get('avg_obstacle_time_min', 0):.2f} min")
            parts.append("**Avg Obstacle Time per Route**\n" + "\n".join(lines))
        return "\n\n".join(parts) if parts else "No route analytics data available.", note

    # default fallback
    return payload.get(item), note
