"""sanjaya_chat implementation: 3-turn flow (query → proceed/cancel → schedule).

Extracted from mcp_server.py so the file stays thin. Receives all external
dependencies (api client, caches, helper callables) as parameters to avoid
circular imports.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from src.nlu import parse_query, ollama_pick_client
from src.client_match import resolve_client, resolve_client_candidates, scan_prompt_for_client, scan_prompt_for_client_candidates
from src.scheduling import (
    CLIENT_REPORT_CONFIG_FILE,
    _DAY_NAMES,
    _SCHEDULE_TIME_PHRASE,
    _add_or_update_client_config,
    _clear_pending_clarification,
    _clear_pending_report,
    _clear_pending_schedule,
    _item_to_section_names,
    _load_pending_clarification,
    _load_pending_report,
    _load_pending_schedule,
    _parse_email_recipients,
    _parse_schedule_command,
    _save_pending_clarification,
    _save_pending_report,
    _save_pending_schedule,
)

logger = logging.getLogger("fm_mcp")


def _fmt_time_header(time_strings: dict, client_name: str, fleet_name: str) -> str:
    """Return a one-line markdown header showing the query period."""
    start = time_strings.get("start_time", "")
    end   = time_strings.get("end_time",   "")
    # Convert "YYYY-MM-DD HH:MM:SS" → "DD Mon YYYY HH:MM"
    def _nice(ts: str) -> str:
        try:
            from datetime import datetime
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%-d %b %Y %H:%M")
        except Exception:
            return ts
    context = f"{client_name}"
    if fleet_name:
        context += f" / {fleet_name}"
    return f"🔍 Understood: **{context}** &nbsp;|&nbsp; 📅 {_nice(start)} → {_nice(end)}\n\n---\n"


def _sections_for_items(items: List[str]) -> Optional[List[str]]:
    """Return deduplicated section list for a list of metric items.
    Returns None only when items is empty (= full report)."""
    if not items:
        return None
    result: List[str] = []
    for item in items:
        s = _item_to_section_names(item)
        if s:
            for sec in s:
                if sec not in result:
                    result.append(sec)
    return result if result else None


_SCHEDULE_WORDS = {"daily", "weekly", "hourly", "skip", "every", "monday", "tuesday",
                   "wednesday", "thursday", "friday", "saturday", "sunday"}
_ANALYTICS_WORDS = {
    "utilization", "uptime", "trips", "distance", "analytics", "status",
    "takt", "obstacle", "sherpa", "tug", "bot", "fleet", "client",
    "give", "show", "get", "what", "how", "report", "summary",
    "yesterday", "today", "week", "month", "last", "this",
}


def _looks_like_new_query(text: str) -> bool:
    """Return True if text looks like an analytics query rather than a schedule response."""
    words = text.lower().strip().split()
    # Short texts that only contain schedule words → let the schedule handler deal with it
    if len(words) <= 3 and any(w in _SCHEDULE_WORDS for w in words):
        return False
    # Any analytics indicator → new query
    if any(w in _ANALYTICS_WORDS for w in words):
        return True
    # Long sentences are almost certainly new queries
    return len(words) > 5


async def handle_chat(
    text: str,
    *,
    api,
    client_cache,
    client_details_cache,
    sherpa_cache,
    project_root: str,
    defaults: dict,
    get_metric_data_fn: Callable,
    fetch_analytics_fn: Callable,
    send_text_report_fn: Callable,
) -> str:
    """Main 3-turn chat flow: query → proceed/cancel → schedule.

    Parameters
    ----------
    api                 SanjayaAPI instance
    client_cache        TTLCache for client list
    client_details_cache TTLCache for per-client details
    sherpa_cache        TTLCache for sherpa lists
    project_root        Absolute path to project root (for file I/O)
    defaults            Dict from _defaults() — env-based fallback values
    get_metric_data_fn  Callable = _get_metric_response_and_data
    fetch_analytics_fn  Callable = _fetch_analytics_data_and_summary
    send_text_report_fn Callable = _generate_and_send_text_report
    """
    logger.info("sanjaya_chat called with query: %s", text)

    # ── Pending clarification: user is answering a question we asked ───────
    # e.g. we asked "which client?" and user replied "YOKOHAMA-DAHEJ"
    pending_clarif = _load_pending_clarification()
    _GLOBAL_CTRL = {
        "proceed", "yes", "confirm", "send", "proceed to email",
        "cancel", "no", "skip",
    }
    if pending_clarif and (text.strip().lower() in _GLOBAL_CTRL or _looks_like_new_query(text)):
        # Control commands (proceed/cancel/schedule) and new analytics queries
        # always override a stale pending clarification.
        logger.info("Clearing pending_clarification: text=%r is a control cmd or new query.", text)
        _clear_pending_clarification()
        pending_clarif = None
    if pending_clarif:
        missing_field = pending_clarif.get("missing_field")
        partial_pq    = pending_clarif.get("partial_pq", {})
        answer        = text.strip()
        _clear_pending_clarification()
        logger.info("Resuming from clarification: missing_field=%s, answer=%s", missing_field, answer)

        # ── Email recipients: finalize the schedule with To/CC addresses ──────
        if missing_field == "email_recipients":
            sched_data   = partial_pq.get("pending_sched", {})
            schedule     = partial_pq.get("schedule", {})
            raw_fleet    = partial_pq.get("raw_fleet", "")
            fleet_list   = partial_pq.get("fleet_list", [])
            time_phrase  = partial_pq.get("time_phrase", "yesterday")
            cadence_desc = partial_pq.get("cadence_desc", "")
            sched_desc   = partial_pq.get("sched_desc", "")
            fleet_label  = partial_pq.get("fleet_label", "")

            parsed_emails = _parse_email_recipients(answer)
            to_emails = parsed_emails["to_emails"]
            cc_emails = parsed_emails["cc_emails"]

            try:
                for fl in fleet_list:
                    _add_or_update_client_config(
                        client_name=sched_data["client_name"],
                        fleet_name=fl,
                        time_phrase=time_phrase,
                        timezone=sched_data.get("timezone", "Asia/Kolkata"),
                        sections=sched_data.get("sections"),
                        schedule_type=schedule["schedule_type"],
                        run_hour=schedule.get("run_hour"),
                        run_day=schedule.get("run_day"),
                        to_emails=to_emails,
                        cc_emails=cc_emails,
                    )
                _clear_pending_schedule()

                email_desc = ""
                if to_emails:
                    email_desc = f"\n\n📧 **To:** {', '.join(to_emails)}"
                if cc_emails:
                    email_desc += f"\n📧 **CC:** {', '.join(cc_emails)}"
                if not to_emails and not cc_emails:
                    email_desc = "\n\nUsing the default recipient from `.env`."

                return (
                    f"Scheduled! Airflow will deliver the report for **{fleet_label}** **{sched_desc}**."
                    + email_desc
                    + f"\n\nThe `client_report_config.json` has been updated — "
                    "you can change or remove the entries there at any time."
                )
            except Exception as e:
                logger.warning("Failed to write schedule config: %s", e)
                return f"Failed to save schedule: {e}"

        if missing_field == "client_name_confirm":
            # User is confirming (or correcting) a suggested client name
            if answer.lower() in ("yes", "y", "confirm", "yeah", "yep", "ok"):
                # Confirmed — use the pre-suggested client name
                partial_pq["fm_client_name"] = partial_pq.get("suggested_client")
            else:
                # User typed the real name; treat it as the new client name
                partial_pq["fm_client_name"] = answer
        elif missing_field == "client_name":
            partial_pq["fm_client_name"] = answer
        elif missing_field == "time_phrase":
            partial_pq["time_phrase"] = answer

        # Reconstruct a synthetic prompt and re-enter the main flow
        # by injecting the answer into the partial query fields
        from src.nlu import ParsedQuery
        pq_resumed = ParsedQuery(
            intent      = partial_pq.get("intent", "basic_analytics"),
            item        = partial_pq.get("item"),
            items       = partial_pq.get("items", []),
            sherpa_hint = partial_pq.get("sherpa_hint"),
            fm_client_name = partial_pq.get("fm_client_name"),
            fleet_name  = partial_pq.get("fleet_name"),
            timezone    = partial_pq.get("timezone"),
            time_phrase = partial_pq.get("time_phrase"),
        )
        # Fall through to the main query block using the resumed ParsedQuery
        return await _execute_query(
            pq=pq_resumed,
            original_text=partial_pq.get("original_text", text),
            defaults=defaults,
            api=api,
            client_cache=client_cache,
            client_details_cache=client_details_cache,
            sherpa_cache=sherpa_cache,
            get_metric_data_fn=get_metric_data_fn,
            fetch_analytics_fn=fetch_analytics_fn,
            send_text_report_fn=send_text_report_fn,
        )

    # ── Proceed / Cancel ──────────────────────────────────────────────────
    cmd = text.strip().lower()
    if cmd in ("proceed", "yes", "proceed to email", "send", "confirm"):
        pending = _load_pending_report()
        if not pending:
            return "No pending report found. Please request a report first."
        try:
            send_text_report_fn(
                pending["report_text"],
                pending["client_name"],
                pending["fleet_name"],
                pending["time_phrase"],
                pending["timezone"],
                pending["time_strings"],
            )
            recipient = os.environ.get("REPORT_RECIPIENT", "")
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
                "- `daily 8` or `daily 8am` — every day at 08:00\n"
                "- `daily 14:30` — every day at 14:00 (minutes ignored, hour used)\n"
                "- `daily 2pm` — every day at 14:00\n"
                "- `hourly` — every hour\n"
                "- `every 20 mins` — every 20 minutes (for testing)\n"
                "- `weekly monday 9am` — every Monday at 09:00\n"
                "- `skip` — don't schedule (one-time send only)\n"
            )
        except Exception as e:
            logger.warning("Failed to send pending report: %s", e)
            return f"Failed to send report: {e}"

    if cmd in ("cancel", "no", "skip"):
        if _load_pending_report():
            _clear_pending_report()
            return "Report cancelled. No email sent."
        if not _load_pending_schedule():
            # Nothing pending at all — return gracefully instead of falling through to NLU
            return "No active report to cancel."
        # pending_sched exists: fall through to the schedule handler below

    # ── Schedule response (after email was sent) ──────────────────────────
    pending_sched = _load_pending_schedule()
    if pending_sched:
        schedule = _parse_schedule_command(text.strip())

        if schedule is None:
            _clear_pending_schedule()
            return "OK — report was sent once, no recurring schedule added."

        if "error" in schedule:
            # If the message looks like a new analytics query (not a schedule response),
            # clear stale pending_sched and fall through to NLU instead of showing an error.
            if _looks_like_new_query(text):
                logger.info("Stale pending_schedule detected; message looks like a new query — clearing state.")
                _clear_pending_schedule()
                # Fall through to the main NLU block below (skip the rest of if pending_sched block)
            else:
                return (
                    schedule["error"] + "\n\n"
                    "Please try again:\n"
                    "- `daily 8` — every day at 08:00\n"
                    "- `hourly` — every hour\n"
                    "- `every 20 mins` — every 20 minutes (for testing)\n"
                    "- `weekly monday 8` — every Monday at 08:00\n"
                    "- `skip` — no scheduling\n"
                )
        else:
            sched_type = schedule["schedule_type"]
            time_phrase = pending_sched.get("time_phrase") or _SCHEDULE_TIME_PHRASE.get(sched_type, "yesterday")

            # fleet_name may be comma-joined (multi-fleet)
            raw_fleet  = pending_sched.get("fleet_name", "")
            fleet_list = [f.strip() for f in raw_fleet.split(",") if f.strip()]

            if sched_type == "every_20min":
                cadence_desc = "every 20 minutes [testing mode]"
            elif sched_type == "hourly":
                cadence_desc = "every hour"
            elif sched_type == "daily":
                h = schedule.get("run_hour", datetime.now().hour)
                cadence_desc = f"every day at {h:02d}:00"
            else:
                day_label = _DAY_NAMES[schedule.get("run_day", 0)]
                h = schedule.get("run_hour", datetime.now().hour)
                cadence_desc = f"every {day_label} at {h:02d}:00"
            sched_desc  = f"{cadence_desc} (data: {time_phrase})"
            fleet_label = raw_fleet if len(fleet_list) == 1 else ", ".join(fleet_list)

            # Ask for email recipients before saving config (Turn 4)
            _save_pending_clarification(
                missing_field="email_recipients",
                partial_pq={
                    "pending_sched":  dict(pending_sched),
                    "schedule":       dict(schedule),
                    "raw_fleet":      raw_fleet,
                    "fleet_list":     fleet_list,
                    "time_phrase":    time_phrase,
                    "cadence_desc":   cadence_desc,
                    "sched_desc":     sched_desc,
                    "fleet_label":    fleet_label,
                },
            )
            return (
                f"Got it — **{fleet_label}** will be scheduled **{sched_desc}**.\n\n"
                "**Who should receive this report?**\n\n"
                "Provide **To** and optional **CC** email addresses:\n"
                "- `to: manager@company.com` — single address\n"
                "- `to: a@company.com, b@company.com cc: boss@company.com` — multiple\n"
                "- `skip` — use the default recipient from `.env`\n"
            )

    # ── Main query ────────────────────────────────────────────────────────
    try:
        clean_text = text.replace("[sherpa:all]", "").strip()
        effective_defaults = {
            "fm_client_name": defaults.get("fm_client_name", ""),
            "fleet_name":     defaults.get("fleet_name", ""),
            "timezone":       defaults.get("timezone", "Asia/Kolkata"),
            "time_phrase":    defaults.get("time_phrase", "today"),
        }
        pq = await parse_query(clean_text, defaults=effective_defaults)

        t_lower = clean_text.lower()

        # If user explicitly said "basic analytics" or "analytics summary", always return full summary
        # regardless of what Ollama returned (it often hallucinates a specific metric).
        if "basic analytics" in t_lower or "analytics summary" in t_lower:
            pq.intent = "basic_analytics"
            pq.item = None
        elif pq.intent == "basic_analytics" and pq.item in (None, ""):
            # NLU returned full-summary intent — promote to item intent if text mentions one metric
            if "total trips" in t_lower and "summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "total_trips"
            elif ("total distance" in t_lower or ("distance" in t_lower and "total" in t_lower)) and "summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "total_distance_km"
            elif ("sherpa status" in t_lower or ("status" in t_lower and ("sherpa" in t_lower or "tug" in t_lower))) and "summary" not in t_lower:
                pq.intent = "basic_analytics_item"
                pq.item = "sherpa_status"
        elif pq.intent == "basic_analytics" and pq.item:
            pq.intent = "basic_analytics_item"

        logger.info(
            "Parsed query: intent=%s, item=%s, client=%s, fleet=%s, time_phrase=%s, sherpa_hint=%s",
            pq.intent, pq.item, pq.fm_client_name, pq.fleet_name, pq.time_phrase, pq.sherpa_hint,
        )

        # If Ollama named a metric that isn't in our supported list AND the heuristic
        # also found nothing → the user asked for something we don't support yet.
        # Tell them clearly instead of silently returning the full analytics summary.
        if pq.unrecognized_metric and not pq.items:
            from src.nlu import ALLOWED_ITEMS as _ALLOWED
            metric_label = pq.unrecognized_metric.replace("_", " ").title()
            supported = ", ".join(sorted(_ALLOWED))
            return (
                f"**{metric_label}** is not currently a supported metric in this system.\n\n"
                "Once it's added to the data pipeline it will appear here automatically.\n\n"
                f"Supported metrics: {supported}"
            )

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

        return await _execute_query(
            pq=pq,
            original_text=clean_text,
            defaults=defaults,
            api=api,
            client_cache=client_cache,
            client_details_cache=client_details_cache,
            sherpa_cache=sherpa_cache,
            get_metric_data_fn=get_metric_data_fn,
            fetch_analytics_fn=fetch_analytics_fn,
            send_text_report_fn=send_text_report_fn,
        )

    except Exception as e:
        logger.error("Error in sanjaya_chat: %s", e)
        return f"Error: {str(e)}"


async def _execute_query(
    pq,
    original_text: str,
    *,
    defaults: dict,
    api,
    client_cache,
    client_details_cache,
    sherpa_cache,
    get_metric_data_fn: Callable,
    fetch_analytics_fn: Callable,
    send_text_report_fn: Callable,
) -> str:
    """Resolve names, validate required fields (asking user if missing), then fetch data."""

    # Dropdown priority: if sidebar provided a value, it overrides NLU.
    # NLU is the fallback when the dropdown is empty/unselected.
    sidebar_client = defaults.get("fm_client_name", "")
    sidebar_fleet  = defaults.get("fleet_name", "")
    sidebar_time   = defaults.get("time_phrase", "")

    client_from_nlu = bool(sidebar_client) or bool(pq.fm_client_name)
    client_name = sidebar_client or pq.fm_client_name or None
    fleet_name  = sidebar_fleet  or pq.fleet_name  or None
    time_phrase = sidebar_time   or pq.time_phrase  or None
    timezone    = pq.timezone
    if not timezone or str(timezone).lower() in ("null", "none", ""):
        timezone = defaults.get("timezone") or "Asia/Kolkata"
    else:
        timezone = str(timezone)

    # ── Client name resolution (exact + fuzzy via RapidFuzz) ──────────
    # Also detects NLU swap where client name ended up in fleet_name.
    try:
        await api.ensure_token()
        all_clients = await client_cache.get_or_set("all_clients", api.get_clients)

        # Swap correction: NLU put a client name into fleet_name
        if (not client_from_nlu) and fleet_name:
            swap_match = resolve_client(fleet_name, all_clients)
            if swap_match:
                logger.info("Correcting NLU swap: treating fleet_name '%s' as client_name", fleet_name)
                client_name = swap_match.get("fm_client_name")
                fleet_name = None

        # ── Ambiguity check: even when sidebar has a specific client, ────────
        # if the prompt text matches multiple clients at similar scores,
        # ask the user to pick (e.g. "schneider electric" → Chino vs Chennai).
        if sidebar_client:
            ambig_candidates = scan_prompt_for_client_candidates(original_text, all_clients)
            if len(ambig_candidates) >= 2:
                options = [d.get("fm_client_name") for d, _ in ambig_candidates]
                options_list = "\n".join(f"- `{n}`" for n in options)
                _save_pending_clarification(
                    missing_field="client_name",
                    partial_pq={
                        "intent":        pq.intent,
                        "item":          pq.item,
                        "items":         pq.items,
                        "sherpa_hint":   pq.sherpa_hint,
                        "fleet_name":    fleet_name,
                        "timezone":      timezone,
                        "time_phrase":   time_phrase,
                        "original_text": original_text,
                    },
                )
                return (
                    f"I found multiple clients matching your query:\n\n"
                    + options_list
                    + "\n\nWhich one did you mean? Type the exact name."
                )

        # Pre-resolution prompt scan: run when no sidebar client AND NLU didn't find a
        # client in the prompt text (pq.fm_client_name may just be the env default).
        # Three-threshold approach:
        #   score >= 88 → use directly (obvious match like "tvs hosur" → TVS-Hosur)
        #   score 78-87 → ask "Did you mean X?" confirmation
        #   score < 78  → tell user to specify a valid client name (no silent default)
        if not sidebar_client and not pq.client_from_text:
            scan_match, scan_score = scan_prompt_for_client(original_text, all_clients)
            if scan_match and scan_score >= 88:
                # High confidence — use directly without asking
                suggested = scan_match.get("fm_client_name")
                logger.info("High-confidence prompt scan: using '%s' directly (score=%.1f)", suggested, scan_score)
                client_name = suggested
            elif scan_match and scan_score >= 78:
                # Medium confidence — ask for confirmation
                suggested = scan_match.get("fm_client_name")
                logger.info("Medium-confidence prompt scan: asking confirmation for '%s' (score=%.1f)", suggested, scan_score)
                _save_pending_clarification(
                    missing_field="client_name_confirm",
                    partial_pq={
                        "intent":           pq.intent,
                        "item":             pq.item,
                        "items":            pq.items,
                        "sherpa_hint":      pq.sherpa_hint,
                        "fleet_name":       fleet_name,
                        "timezone":         timezone,
                        "time_phrase":      time_phrase,
                        "original_text":    original_text,
                        "suggested_client": suggested,
                    },
                )
                return (
                    f"Did you mean client **{suggested}**?\n\n"
                    "Reply `yes` to confirm, or type the correct client name."
                )
            else:
                # Tier 2: Ollama constrained pick from known client list
                client_names = [c.get("fm_client_name") for c in all_clients if isinstance(c, dict) and c.get("fm_client_name")]
                ollama_pick, ollama_score = await ollama_pick_client(original_text, client_names)

                if ollama_pick and ollama_score >= 88:
                    # High confidence — use directly
                    logger.info("Tier 2 Ollama high-confidence: '%s' (score=%.1f)", ollama_pick, ollama_score)
                    client_name = ollama_pick
                elif ollama_pick and ollama_score >= 78:
                    # Medium confidence — ask confirmation
                    logger.info("Tier 2 Ollama medium-confidence: asking '%s' (score=%.1f)", ollama_pick, ollama_score)
                    _save_pending_clarification(
                        missing_field="client_name_confirm",
                        partial_pq={
                            "intent":           pq.intent,
                            "item":             pq.item,
                            "items":            pq.items,
                            "sherpa_hint":      pq.sherpa_hint,
                            "fleet_name":       fleet_name,
                            "timezone":         timezone,
                            "time_phrase":      time_phrase,
                            "original_text":    original_text,
                            "suggested_client": ollama_pick,
                        },
                    )
                    return (
                        f"Did you mean client **{ollama_pick}**?\n\n"
                        "Reply `yes` to confirm, or type the correct client name."
                    )
                else:
                    # All tiers exhausted — tell the user
                    examples = ", ".join(f"`{n}`" for n in sorted(client_names)[:5])
                    return (
                        "I couldn't identify a client name in your query. "
                        "Please mention the client name — for example:\n\n"
                        f"> *give me uptime for **TVS-Hosur** today*\n\n"
                        f"Known clients (partial list): {examples}"
                    )

        # Normalise client_name to exact API spelling (handles wrong case / typos)
        if client_name:
            candidates = resolve_client_candidates(client_name, all_clients)
            if len(candidates) == 1:
                # Unambiguous match
                matched = candidates[0][0]
                client_name = matched.get("fm_client_name")
                client_id_for_fleet = matched.get("fm_client_id")
            elif len(candidates) > 1:
                # Multiple close matches — ask the user to pick
                options = [d.get("fm_client_name") for d, _ in candidates]
                options_list = "\n".join(f"- `{n}`" for n in options)
                _save_pending_clarification(
                    missing_field="client_name",
                    partial_pq={
                        "intent":        pq.intent,
                        "item":          pq.item,
                        "items":         pq.items,
                        "sherpa_hint":   pq.sherpa_hint,
                        "fleet_name":    fleet_name,
                        "timezone":      timezone,
                        "time_phrase":   time_phrase,
                        "original_text": original_text,
                    },
                )
                return (
                    f"I found multiple close matches for **\"{client_name}\"**:\n\n"
                    + options_list
                    + "\n\nWhich one did you mean? Type the exact name."
                )
            else:
                # No match above threshold — tell the user instead of guessing
                names = sorted(
                    c.get("fm_client_name") for c in all_clients
                    if isinstance(c, dict) and c.get("fm_client_name")
                )
                names_list = "\n".join(f"- `{n}`" for n in names)
                return (
                    f"I couldn't find a client close to **\"{client_name}\"**. "
                    "Please check the spelling or choose one from the list:\n\n"
                    + names_list
                )
        else:
            client_id_for_fleet = None

        # ── Fleet name case correction ─────────────────────────────────
        # Ollama often returns wrong case (e.g. "Bead" instead of "BEAD").
        if client_name and fleet_name and client_id_for_fleet:
            try:
                cache_key = f"client_details_{client_id_for_fleet}"
                client_details = await client_details_cache.get_or_set(
                    cache_key, api.get_client_by_id, client_id_for_fleet
                )
                if client_details:
                    for real_fleet in client_details.get("fm_fleet_names", []):
                        if real_fleet.lower() == fleet_name.lower():
                            if real_fleet != fleet_name:
                                logger.info("Correcting fleet name case: '%s' → '%s'", fleet_name, real_fleet)
                                fleet_name = real_fleet
                            break
            except Exception:
                pass
    except Exception:
        pass

    # ── Missing client_name — ask the user ────────────────────────────
    if not client_name:
        try:
            all_clients = await client_cache.get_or_set("all_clients", api.get_clients)
            names = sorted(
                c.get("fm_client_name") for c in all_clients
                if isinstance(c, dict) and c.get("fm_client_name")
            )
        except Exception:
            names = []

        # Save partial query so we can resume when the user answers
        _save_pending_clarification(
            missing_field="client_name",
            partial_pq={
                "intent":       pq.intent,
                "item":         pq.item,
                "items":        pq.items,
                "sherpa_hint":  pq.sherpa_hint,
                "fleet_name":   fleet_name,
                "timezone":     timezone,
                "time_phrase":  time_phrase,
                "original_text": original_text,
            },
        )
        if names:
            names_list = "\n".join(f"- `{n}`" for n in names)
            return (
                "I couldn't find a client name in your message. "
                "Which client do you want? Just type the name.\n\n"
                + names_list
            )
        return (
            "I couldn't find a client name in your message. "
            "Please type the client name (e.g. `YOKOHAMA-DAHEJ`)."
        )

    # ── Missing time_phrase — ask the user ────────────────────────────
    # Only ask when no time phrase at all was extracted (not even a default).
    # We skip this check if time_phrase already has a value from defaults.
    if not time_phrase:
        _save_pending_clarification(
            missing_field="time_phrase",
            partial_pq={
                "intent":        pq.intent,
                "item":          pq.item,
                "items":         pq.items,
                "sherpa_hint":   pq.sherpa_hint,
                "fm_client_name": client_name,
                "fleet_name":    fleet_name,
                "timezone":      timezone,
                "time_phrase":   None,
                "original_text": original_text,
            },
        )
        return (
            "What time period do you want data for?\n\n"
            "- `today`\n"
            "- `yesterday`\n"
            "- `this week`\n"
            "- `this month`\n"
            "- `last month`\n"
            "- `1 Jan 2026 to 10 Jan 2026` (custom range)"
        )

    # ── Fleet auto-resolution ─────────────────────────────────────────
    # Builds all_fleet_names so we can query all fleets when none is specified.
    # Reuses client_id_for_fleet resolved above; falls back to a fresh lookup.
    all_fleet_names: List[str] = []
    if client_name and not fleet_name:
        try:
            await api.ensure_token()
            client_id = client_id_for_fleet  # already resolved above (may be None)
            if not client_id:
                # Fallback: re-resolve (handles the case where correction block was skipped)
                all_clients = await client_cache.get_or_set("all_clients", api.get_clients)
                matched = resolve_client(client_name, all_clients)
                client_id = matched.get("fm_client_id") if matched else None
            if client_id:
                cache_key = f"client_details_{client_id}"
                client_details = await client_details_cache.get_or_set(
                    cache_key, api.get_client_by_id, client_id
                )
                if client_details:
                    all_fleet_names = client_details.get("fm_fleet_names", [])
                if len(all_fleet_names) == 1:
                    fleet_name = all_fleet_names[0]
                    logger.info("Auto-resolved single fleet for client %s: %s", client_name, fleet_name)
                elif all_fleet_names:
                    # Multiple fleets and none specified — query all of them
                    logger.info("No fleet specified for client %s — will query all %d fleets: %s", client_name, len(all_fleet_names), all_fleet_names)
            else:
                logger.warning("Client '%s' not found in client list (client_id is None)", client_name)
        except Exception as e:
            logger.warning("Fleet auto-resolve failed: %s", e)

    # Dropdown priority for sherpa — sidebar selection overrides NLU
    sidebar_sherpa  = defaults.get("sherpa_hint", "")
    api_sherpa_name = sidebar_sherpa or pq.sherpa_hint
    if isinstance(api_sherpa_name, str) and api_sherpa_name.lower() in ("null", "none", ""):
        api_sherpa_name = None

    # Multi-sherpa list from dropdown (for full analytics filtering)
    selected_sherpas: Optional[List[str]] = defaults.get("selected_sherpas") or None

    # ── All-fleets mode: no fleet specified, query each fleet and combine ──
    fleets_to_query: List[str] = [fleet_name] if fleet_name else all_fleet_names
    if not fleets_to_query:
        return (
            f"I couldn't find any fleets for client **{client_name}**. "
            "Please check the client name or contact support."
        )

    if len(fleets_to_query) > 1:
        logger.info("Querying all %d fleets for client %s: %s", len(fleets_to_query), client_name, fleets_to_query)
        fleet_parts, time_strings = [], {}
        for fl in fleets_to_query:
            try:
                if pq.intent == "multi_metric" and len(pq.items) > 1:
                    metric_parts = []
                    for metric in pq.items:
                        resp, _, ts = await get_metric_data_fn(
                            metric=metric, client_name=client_name, fleet_name=fl,
                            time_range=time_phrase, timezone=timezone, sherpa_name=api_sherpa_name,
                            selected_sherpas=selected_sherpas,
                        )
                        metric_parts.append(resp)
                        time_strings = ts
                    fleet_parts.append(f"**Fleet: {fl}**\n\n" + "\n\n".join(metric_parts))
                elif pq.intent == "basic_analytics_item" and pq.item:
                    resp, _, ts = await get_metric_data_fn(
                        metric=pq.item, client_name=client_name, fleet_name=fl,
                        time_range=time_phrase, timezone=timezone, sherpa_name=api_sherpa_name,
                        selected_sherpas=selected_sherpas,
                    )
                    time_strings = ts
                    fleet_parts.append(f"**Fleet: {fl}**\n\n{resp}")
                else:
                    resp, _, ts = await fetch_analytics_fn(client_name, fl, time_phrase, timezone, selected_sherpas)
                    time_strings = ts
                    # Strip "Analytics Summary for X (Y):\n\n" wrapper — fleet banner replaces it
                    if resp.startswith("Analytics Summary for "):
                        resp = resp[resp.find("\n\n") + 2:].strip() if "\n\n" in resp else resp
                    fleet_parts.append(f"**Fleet: {fl}**\n\n{resp}")
            except Exception as e:
                logger.warning("Failed to fetch data for fleet %s: %s", fl, e)
                fleet_parts.append(f"**Fleet: {fl}** — data unavailable ({e})")

        combined = "\n\n---\n\n".join(fleet_parts)
        header = _fmt_time_header(time_strings, client_name, "all fleets")
        _save_pending_report(
            client_name, ", ".join(fleets_to_query), time_phrase, timezone,
            time_strings, combined, original_text,
            sections=_sections_for_items(pq.items if pq.intent == "multi_metric" else ([pq.item] if pq.item else [])),
        )
        return header + combined + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."

    # ── Single fleet (normal path) ────────────────────────────────────
    fleet_name = fleets_to_query[0]

    # ── Multi-metric: call each endpoint and combine ──────────────────
    if pq.intent == "multi_metric" and len(pq.items) > 1:
        parts, time_strings = [], {}
        for metric in pq.items:
            metric_response, _, ts = await get_metric_data_fn(
                metric=metric,
                client_name=client_name,
                fleet_name=fleet_name,
                time_range=time_phrase,
                timezone=timezone,
                sherpa_name=api_sherpa_name,
                selected_sherpas=selected_sherpas,
            )
            parts.append(metric_response)
            time_strings = ts
        combined = "\n\n".join(parts)
        header = _fmt_time_header(time_strings, client_name, fleet_name)
        _save_pending_report(
            client_name, fleet_name, time_phrase, timezone,
            time_strings, combined, original_text,
            sections=_sections_for_items(pq.items),
        )
        return header + combined + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."

    # ── Single metric ─────────────────────────────────────────────────
    if pq.intent == "basic_analytics_item" and pq.item:
        metric_response, data, time_strings = await get_metric_data_fn(
            metric=pq.item,
            client_name=client_name,
            fleet_name=fleet_name,
            time_range=time_phrase,
            timezone=timezone,
            sherpa_name=api_sherpa_name,
            selected_sherpas=selected_sherpas,
        )
        header = _fmt_time_header(time_strings, client_name, fleet_name)
        _save_pending_report(
            client_name, fleet_name, time_phrase, timezone,
            time_strings, metric_response, original_text,
            sections=_item_to_section_names(pq.item),
        )
        return header + metric_response + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."

    # ── Full analytics summary ────────────────────────────────────────
    else:
        summary_text, data, time_strings = await fetch_analytics_fn(
            client_name, fleet_name, time_phrase, timezone, selected_sherpas
        )
        header = _fmt_time_header(time_strings, client_name, fleet_name)
        _save_pending_report(
            client_name, fleet_name, time_phrase, timezone,
            time_strings, summary_text, original_text,
            sections=None,
        )
        return header + summary_text + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."
