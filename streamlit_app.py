#!/usr/bin/env python3
"""Sanjaya Analytics — Streamlit web UI.

Runs alongside the MCP server: both call the same analytics engine.
Start with: streamlit run streamlit_app.py
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import re
import sys
from datetime import date, timedelta
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv

load_dotenv()

import streamlit as st

from src.logging_config import setup_logging
from src.nlu import parse_query
from src.sanjaya_client import SanjayaAPI
from src.cache import TTLCache
from src.chat_handler import handle_chat
from src.analytics import (
    fetch_analytics_data_and_summary,
    get_metric_response_and_data,
    generate_and_send_text_report,
)
from src.scheduling import _save_pending_report, _item_to_section_names

setup_logging(os.environ.get("LOG_LEVEL", "WARNING"))

BASE_URL = "https://sanjaya.atimotors.com"

# "__custom__" is a sentinel for the custom date picker
TIME_OPTIONS: dict[str, str] = {
    "Today":         "today",
    "Yesterday":     "yesterday",
    "Last 7 Days":   "last 7 days",
    "Last 30 Days":  "last 30 days",
    "Custom Range":  "__custom__",
}

QUICK_QUERIES = [
    ("📊 Basic Analytics",  "basic analytics"),
    ("🚀 Total Trips",      "total trips"),
    ("🤖 Sherpa Status",    "sherpa status"),
    ("⚡ Utilization",      "utilization"),
    ("⏱️ Uptime",           "uptime"),
    ("📏 Total Distance",   "total distance"),
    ("🛤️ Takt Time",        "takt time"),
    ("🚧 Obstacle Time",    "obstacle time"),
]

SCHEDULE_OPTIONS = [
    ("Daily at 8am",    "daily 8"),
    ("Daily at 9am",    "daily 9"),
    ("Hourly",          "hourly"),
    ("Every 20 min",    "every 20 mins"),
    ("Weekly Mon 8am",  "weekly monday 8"),
    ("Skip",            "skip"),
]

# Ordered longest-first so "sherpa status" matches before "status"
_METRIC_PATTERNS: list[tuple[str, str]] = [
    (r"\bsherpa[\s_]?status\b",    "sherpa_status"),
    (r"\broute[\s_]?utilization\b", "route_utilization"),
    (r"\bobstacle[\s_]?time\b",    "obstacle_time"),
    (r"\btakt(?:[\s_]?time)?\b",   "takt_time"),
    (r"\btotal[\s_]?trips\b|\btrips\b",    "total_trips"),
    (r"\btotal[\s_]?distance\b|\bdistance\b", "total_distance_km"),
    (r"\bavailability\b",          "availability"),
    (r"\butilization\b",           "utilization"),
    (r"\bactivity\b",              "activity"),
    (r"\buptime\b",                "uptime"),
]

_CONTROL_CMDS = {
    "proceed", "yes", "confirm", "send", "proceed to email",
    "cancel", "no", "skip",
}


# ── Async runner ──────────────────────────────────────────────────────────────

def _run(coro):
    """Run async coroutine safely from Streamlit's synchronous context."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


# ── Session state ─────────────────────────────────────────────────────────────

def _init_session() -> None:
    defaults: dict = {
        "api":                  SanjayaAPI(BASE_URL),
        "client_cache":         TTLCache(ttl_seconds=600.0, max_size=100),
        "client_details_cache": TTLCache(ttl_seconds=600.0, max_size=100),
        "sherpa_cache":         TTLCache(ttl_seconds=300.0, max_size=200),
        "messages":             [],
        "clients_list":         [],
        "fleet_map":            {},
        "sherpa_map":           {},
        "pending_action":       None,
        "pending_confirmation": None,   # dict when awaiting user confirmation
        "recipient_email":      os.environ.get("REPORT_RECIPIENT", ""),
        # Context stored per render for button-click processing
        "_ctx_client":          "",
        "_ctx_fleets":          [],
        "_ctx_sherpas":         [],
        "_ctx_time":            "yesterday",
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ── API helpers ───────────────────────────────────────────────────────────────

def _load_clients() -> list[str]:
    api   = st.session_state.api
    cache = st.session_state.client_cache

    async def _f():
        await api.ensure_token()
        clients = await cache.get_or_set("all_clients", api.get_clients)
        return sorted(
            c.get("fm_client_name") for c in clients
            if isinstance(c, dict) and c.get("fm_client_name")
        )
    return _run(_f())


def _load_fleets(client_name: str) -> list[str]:
    api = st.session_state.api
    cc  = st.session_state.client_cache
    dc  = st.session_state.client_details_cache

    async def _f():
        await api.ensure_token()
        all_clients = await cc.get_or_set("all_clients", api.get_clients)
        client_id = next(
            (c.get("fm_client_id") for c in all_clients
             if isinstance(c, dict)
             and (c.get("fm_client_name") or "").lower() == client_name.lower()),
            None,
        )
        if not client_id:
            return []
        details = await dc.get_or_set(
            f"client_details_{client_id}", api.get_client_by_id, client_id
        )
        return sorted(details.get("fm_fleet_names", []))
    return _run(_f())


def _load_sherpas(client_name: str, fleet_names: list[str]) -> list[str]:
    if not fleet_names:
        return []
    api = st.session_state.api
    cc  = st.session_state.client_cache
    sc  = st.session_state.sherpa_cache

    async def _f():
        await api.ensure_token()
        all_clients = await cc.get_or_set("all_clients", api.get_clients)
        client_id = next(
            (c.get("fm_client_id") for c in all_clients
             if isinstance(c, dict)
             and (c.get("fm_client_name") or "").lower() == client_name.lower()),
            None,
        )
        if not client_id:
            return []
        all_sherpas = await sc.get_or_set(
            f"sherpas_client_{client_id}", api.get_sherpas_by_client_id, client_id
        )
        fleet_lower = {f.lower() for f in fleet_names}
        return sorted(set(
            s.get("sherpa_name") for s in all_sherpas
            if isinstance(s, dict)
            and (s.get("fleet_name") or "").lower() in fleet_lower
            and s.get("sherpa_name")
        ))
    return _run(_f())


# ── NLU parse for confirmation ────────────────────────────────────────────────

def _parse_for_confirmation(
    prompt: str,
    client_name: str,
    fleet_name: str,
    time_phrase: str,
    timezone: str,
    fleet_names: list[str],
    sherpa_names: list[str],
) -> dict:
    """Parse query with NLU and merge with sidebar context for confirmation card."""
    nlu_defaults = {
        "fm_client_name": "",   # keep empty so NLU shows what IT extracted
        "fleet_name":     "",
        "timezone":       timezone,
        "time_phrase":    time_phrase,
    }

    async def _f():
        return await parse_query(prompt, defaults=nlu_defaults)

    pq = _run(_f())

    # Apply sidebar context where NLU returned nothing
    resolved_client = pq.fm_client_name or client_name
    resolved_fleet  = pq.fleet_name  or fleet_name
    resolved_time   = pq.time_phrase or time_phrase
    tz_raw = str(pq.timezone or "")
    resolved_tz = tz_raw if tz_raw.lower() not in ("null", "none", "") else timezone

    # Normalise intent (mirrors chat_handler.py logic)
    intent = pq.intent
    item   = pq.item
    t_low  = prompt.lower()
    if "basic analytics" in t_low or "analytics summary" in t_low:
        intent, item = "basic_analytics", None
    elif intent == "basic_analytics" and item:
        intent = "basic_analytics_item"

    return {
        "original_prompt": prompt,
        "client_name":     resolved_client,
        "fleet_name":      resolved_fleet,
        "time_phrase":     resolved_time,
        "timezone":        resolved_tz,
        "intent":          intent,
        "item":            item,
        "items":           pq.items,   # all metrics (2+ when multi_metric)
        "sherpa_hint":     pq.sherpa_hint or None,
        "fleet_names":     fleet_names,
        "sherpa_names":    sherpa_names,
    }


# ── Multi-metric detection ────────────────────────────────────────────────────

def _detect_metrics(text: str) -> list[str]:
    t = text.lower()
    found, seen = [], set()
    for pattern, metric in _METRIC_PATTERNS:
        if re.search(pattern, t) and metric not in seen:
            found.append(metric)
            seen.add(metric)
    return found if len(found) >= 2 else []


# ── Helper factories (called in Streamlit thread, closures carry captured objects) ──

def _make_fetch_fn(api, cc, sc):
    async def _fn(cn, fn, tr, tz):
        return await fetch_analytics_data_and_summary(
            cn, fn, tr, tz, api=api, client_cache=cc, sherpa_cache=sc,
        )
    return _fn


def _make_metric_fn(api, cc, sc):
    # Parameter names must match the keyword args handle_chat uses when calling get_metric_data_fn:
    # get_metric_data_fn(metric=..., client_name=..., fleet_name=..., time_range=..., timezone=..., sherpa_name=...)
    async def _fn(metric, client_name, fleet_name, time_range, timezone, sherpa_name=None):
        return await get_metric_response_and_data(
            metric, client_name, fleet_name, time_range, timezone,
            api=api, client_cache=cc, sherpa_cache=sc,
            sherpa_name=sherpa_name, use_markdown=True,
        )
    return _fn


def _make_send_fn(recipient: Optional[str] = None):
    def _fn(report_text, cn, fn, tr, tz, ts):
        old = os.environ.get("REPORT_RECIPIENT", "")
        try:
            if recipient:
                os.environ["REPORT_RECIPIENT"] = recipient
            generate_and_send_text_report(
                report_text, cn, fn, tr, tz, ts, project_root=_PROJECT_ROOT,
            )
        finally:
            os.environ["REPORT_RECIPIENT"] = old
    return _fn


# ── Chat state detection ──────────────────────────────────────────────────────

def _get_chat_state() -> str:
    if st.session_state.get("pending_confirmation"):
        return "awaiting_confirmation"
    msgs = st.session_state.messages
    if not msgs or msgs[-1]["role"] != "assistant":
        return "idle"
    content = msgs[-1]["content"].lower()
    if "type **proceed**" in content or "type proceed" in content:
        return "awaiting_proceed"
    if "how would you like to schedule" in content:
        return "awaiting_schedule"
    return "idle"


# ── Multi-fleet and multi-metric async handlers ───────────────────────────────

async def _run_multi_fleet_query(
    prompt: str, client_name: str, fleet_names: list[str],
    time_phrase: str, timezone: str, sherpa_hint: Optional[str],
    *, api, client_cache, sherpa_cache,
) -> str:
    """Run any query (summary, single metric, or multi-metric) across multiple fleets."""
    from src.nlu import parse_query as _parse_query

    nlu_defaults = {
        "fm_client_name": "", "fleet_name": "",
        "timezone": timezone, "time_phrase": time_phrase,
    }
    try:
        pq = await _parse_query(prompt, defaults=nlu_defaults)
    except Exception:
        pq = None

    fleet_parts, all_texts, time_strings = [], [], {}

    for fleet in fleet_names:
        if pq and pq.intent == "multi_metric" and len(pq.items) > 1:
            metric_parts = []
            for metric in pq.items:
                text, _, ts = await get_metric_response_and_data(
                    metric, client_name, fleet, time_phrase, timezone,
                    api=api, client_cache=client_cache, sherpa_cache=sherpa_cache,
                    sherpa_name=sherpa_hint, use_markdown=True,
                )
                metric_parts.append(text)
                time_strings = ts
            fleet_text = "\n\n".join(metric_parts)
        elif pq and pq.intent == "basic_analytics_item" and pq.item:
            fleet_text, _, time_strings = await get_metric_response_and_data(
                pq.item, client_name, fleet, time_phrase, timezone,
                api=api, client_cache=client_cache, sherpa_cache=sherpa_cache,
                sherpa_name=sherpa_hint, use_markdown=True,
            )
        else:
            fleet_text, _, time_strings = await fetch_analytics_data_and_summary(
                client_name, fleet, time_phrase, timezone,
                api=api, client_cache=client_cache, sherpa_cache=sherpa_cache,
            )

        fleet_parts.append(f"### Fleet: {fleet}\n\n{fleet_text}")
        all_texts.append(fleet_text)

    combined = "\n\n---\n\n".join(fleet_parts)
    _save_pending_report(
        client_name, ", ".join(fleet_names), time_phrase, timezone,
        time_strings, "\n\n---\n\n".join(all_texts),
        prompt, sections=None,
    )
    return combined + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."


async def _run_multi_metric(
    metrics: list[str], client_name: str, fleet_name: str,
    time_phrase: str, timezone: str, sherpa_hint: Optional[str],
    *, api, client_cache, sherpa_cache,
) -> str:
    parts, time_strings = [], {}
    for metric in metrics:
        text, _, ts = await get_metric_response_and_data(
            metric, client_name, fleet_name, time_phrase, timezone,
            api=api, client_cache=client_cache, sherpa_cache=sherpa_cache,
            sherpa_name=sherpa_hint, use_markdown=True,
        )
        parts.append(text)
        time_strings = ts
    combined = "\n\n".join(parts)
    _save_pending_report(
        client_name, fleet_name, time_phrase, timezone,
        time_strings, combined,
        f"metrics: {', '.join(metrics)}", sections=None,
    )
    return combined + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."


# ── Execute a user-confirmed query directly (no NLU re-parse) ─────────────────

async def _execute_confirmed(
    client_name: str, fleet_name: str, time_phrase: str, timezone: str,
    intent: str, item: Optional[str], sherpa_hint: Optional[str],
    fleet_names: list[str], sherpa_names: list[str],
    *, api, client_cache, sherpa_cache,
) -> str:
    """Run analytics with the parameters the user approved in the confirmation card."""

    # Multi-fleet basic analytics
    if not item and len(fleet_names) > 1:
        return await _run_multi_fleet(
            client_name, fleet_names, time_phrase, timezone,
            api=api, client_cache=client_cache, sherpa_cache=sherpa_cache,
        )

    sherpa_arg = sherpa_hint or (sherpa_names[0] if len(sherpa_names) == 1 else None)

    # Specific metric
    if intent == "basic_analytics_item" and item:
        text, _, ts = await get_metric_response_and_data(
            item, client_name, fleet_name, time_phrase, timezone,
            api=api, client_cache=client_cache, sherpa_cache=sherpa_cache,
            sherpa_name=sherpa_arg, use_markdown=True,
        )
        _save_pending_report(
            client_name, fleet_name, time_phrase, timezone,
            ts, text, f"{item} query",
            sections=_item_to_section_names(item),
        )
        return text + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."

    # Basic analytics (single fleet)
    text, _, ts = await fetch_analytics_data_and_summary(
        client_name, fleet_name, time_phrase, timezone,
        api=api, client_cache=client_cache, sherpa_cache=sherpa_cache,
    )
    _save_pending_report(
        client_name, fleet_name, time_phrase, timezone,
        ts, text, "basic analytics", sections=None,
    )
    return text + "\n\n---\nType **proceed** to email this as a PDF report, or **cancel** to skip."


# ── Main message processor ────────────────────────────────────────────────────

def _process_message(
    prompt: str,
    client_name: str,
    fleet_names: list[str],
    sherpa_names: list[str],
    time_phrase: str,
    recipient_email: Optional[str] = None,
) -> str:
    api = st.session_state.api
    cc  = st.session_state.client_cache
    dcc = st.session_state.client_details_cache
    sc  = st.session_state.sherpa_cache
    timezone      = os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata")
    primary_fleet = fleet_names[0] if fleet_names else ""

    defaults = {
        "fm_client_name": client_name,
        "fleet_name":     primary_fleet,
        "fleet_id":       None,
        "timezone":       timezone,
        "time_phrase":    time_phrase,
    }

    cmd        = prompt.strip().lower()
    is_control = cmd in _CONTROL_CMDS
    is_sched   = not is_control and any(
        kw in cmd for kw in ("daily", "hourly", "weekly", "every 20", "every hour")
    )

    if is_control or is_sched:
        return _run(handle_chat(
            prompt,
            api=api, client_cache=cc, client_details_cache=dcc, sherpa_cache=sc,
            project_root=_PROJECT_ROOT,
            defaults=defaults,
            get_metric_data_fn=_make_metric_fn(api, cc, sc),
            fetch_analytics_fn=_make_fetch_fn(api, cc, sc),
            send_text_report_fn=_make_send_fn(recipient_email),
        ))

    sherpa_hint: Optional[str] = sherpa_names[0] if len(sherpa_names) == 1 else None

    # Multi-fleet: handle_chat can only query one fleet at a time, so we fan out here
    if len(fleet_names) > 1:
        return _run(_run_multi_fleet_query(
            prompt, client_name, fleet_names, time_phrase, timezone, sherpa_hint,
            api=api, client_cache=cc, sherpa_cache=sc,
        ))

    # Inject single sherpa into query text (NLU picks it up)
    modified = prompt
    if sherpa_hint and not re.search(r"\bsherpa\b|\btug\b", prompt.lower()):
        modified = f"{prompt} for {sherpa_hint}"

    return _run(handle_chat(
        modified,
        api=api, client_cache=cc, client_details_cache=dcc, sherpa_cache=sc,
        project_root=_PROJECT_ROOT,
        defaults=defaults,
        get_metric_data_fn=_make_metric_fn(api, cc, sc),
        fetch_analytics_fn=_make_fetch_fn(api, cc, sc),
        send_text_report_fn=_make_send_fn(),
    ))


def _process_confirmed(data: dict) -> str:
    """Execute analytics with the user-approved parameters from the confirmation card."""
    api = st.session_state.api
    cc  = st.session_state.client_cache
    sc  = st.session_state.sherpa_cache

    # The text the user saw / edited in the confirmation card
    prompt_text  = data.get("edited_prompt") or data.get("original_prompt", "")
    client_name  = data["client_name"]
    fleet_name   = data.get("fleet_name", "")
    fleet_names  = data.get("fleet_names", [fleet_name])
    sherpa_names = data.get("sherpa_names", [])
    time_phrase  = data["time_phrase"]
    timezone     = data["timezone"]
    sherpa_hint  = data.get("sherpa_hint")
    primary_fleet = fleet_names[0] if fleet_names else fleet_name

    effective_sherpa = sherpa_hint or (sherpa_names[0] if len(sherpa_names) == 1 else None)

    # Multi-fleet: fan out across all selected fleets
    if len(fleet_names) > 1:
        return _run(_run_multi_fleet_query(
            prompt_text, client_name, fleet_names, time_phrase, timezone, effective_sherpa,
            api=api, client_cache=cc, sherpa_cache=sc,
        ))

    # Multi-metric: use items from NLU parse (handles cross-endpoint queries like
    # "uptime and obstacle time" which need basic_analytics + route_analytics endpoints)
    items = data.get("items") or []
    if len(items) > 1:
        return _run(_run_multi_metric(
            items, client_name, primary_fleet, time_phrase, timezone, effective_sherpa,
            api=api, client_cache=cc, sherpa_cache=sc,
        ))

    # Single metric or basic analytics
    return _run(_execute_confirmed(
        client_name, fleet_name, time_phrase, timezone,
        data["intent"], data.get("item"), sherpa_hint,
        fleet_names, sherpa_names,
        api=api, client_cache=cc, sherpa_cache=sc,
    ))


# ── Pending action processor ──────────────────────────────────────────────────

def _handle_pending_actions() -> None:
    action = st.session_state.pending_action
    if not action:
        return
    st.session_state.pending_action = None

    action_type, action_value = action
    client      = st.session_state._ctx_client
    fleets      = st.session_state._ctx_fleets
    sherpas     = st.session_state._ctx_sherpas
    time_phrase = st.session_state._ctx_time

    if action_type == "proceed":
        cmd, email, label = "proceed", action_value, "📧 Send Report"
    elif action_type == "cancel":
        cmd, email, label = "cancel", None, "❌ Cancel"
    elif action_type == "schedule":
        cmd, email, label = action_value, None, f"⏰ {action_value}"
    elif action_type == "quick_query":
        # Quick buttons skip confirmation — execute immediately
        st.session_state.messages.append({"role": "user", "content": action_value})
        try:
            response = _process_message(action_value, client, fleets, sherpas, time_phrase)
        except Exception as e:
            response = f"⚠️ Error: {e}"
        st.session_state.messages.append({"role": "assistant", "content": response})
        return
    elif action_type == "confirmed":
        # Executing user-approved query from confirmation card
        st.session_state.messages.append({"role": "assistant", "content": "⏳ Running confirmed query…"})
        try:
            response = _process_confirmed(action_value)
        except Exception as e:
            response = f"⚠️ Error: {e}"
        # Replace the placeholder with the real response
        st.session_state.messages[-1] = {"role": "assistant", "content": response}
        return
    else:
        return

    st.session_state.messages.append({"role": "user", "content": label})
    try:
        response = _process_message(cmd, client, fleets, sherpas, time_phrase, recipient_email=email)
    except Exception as e:
        response = f"⚠️ Error: {e}"
    st.session_state.messages.append({"role": "assistant", "content": response})


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _render_sidebar() -> tuple[str, list[str], list[str], str]:
    with st.sidebar:
        st.markdown("## 📊 Sanjaya Analytics")
        st.divider()

        if not st.session_state.clients_list:
            with st.spinner("Loading clients…"):
                try:
                    st.session_state.clients_list = _load_clients()
                except Exception as e:
                    st.error(f"Could not load clients: {e}")

        st.subheader("Query Context")

        selected_client: str = st.selectbox(
            "Client",
            options=[""] + st.session_state.clients_list,
            format_func=lambda x: "— select client —" if x == "" else x,
            key="sel_client",
        )

        # Fleet multiselect
        fleet_options: list[str] = []
        if selected_client:
            if selected_client not in st.session_state.fleet_map:
                with st.spinner("Loading fleets…"):
                    try:
                        st.session_state.fleet_map[selected_client] = _load_fleets(selected_client)
                    except Exception as e:
                        st.error(f"Could not load fleets: {e}")
                        st.session_state.fleet_map[selected_client] = []
            fleet_options = st.session_state.fleet_map.get(selected_client, [])

        selected_fleets: list[str] = st.multiselect(
            "Fleet(s)",
            options=fleet_options,
            disabled=not selected_client,
            placeholder="Select one or more fleets…",
            key="sel_fleets",
        )

        # Sherpa multiselect
        sherpa_options: list[str] = []
        if selected_client and selected_fleets:
            sherpa_key = f"{selected_client}::{':'.join(sorted(selected_fleets))}"
            if sherpa_key not in st.session_state.sherpa_map:
                with st.spinner("Loading sherpas…"):
                    try:
                        st.session_state.sherpa_map[sherpa_key] = _load_sherpas(
                            selected_client, selected_fleets
                        )
                    except Exception as e:
                        st.error(f"Could not load sherpas: {e}")
                        st.session_state.sherpa_map[sherpa_key] = []
            sherpa_options = st.session_state.sherpa_map.get(sherpa_key, [])

        selected_sherpas: list[str] = st.multiselect(
            "Sherpa(s)",
            options=sherpa_options,
            disabled=not selected_fleets,
            placeholder="All sherpas (default)…",
            key="sel_sherpas",
        )

        # ── Time range with custom date picker ────────────────────────────────
        time_label: str = st.selectbox(
            "Time Range",
            options=list(TIME_OPTIONS.keys()),
            index=1,  # default: Yesterday
            key="sel_time",
        )

        if time_label == "Custom Range":
            st.caption("Pick a date range:")
            col1, col2 = st.columns(2)
            with col1:
                start_d: date = st.date_input(
                    "From",
                    value=date.today() - timedelta(days=7),
                    max_value=date.today(),
                    key="custom_start",
                )
            with col2:
                end_d: date = st.date_input(
                    "To",
                    value=date.today() - timedelta(days=1),
                    max_value=date.today(),
                    key="custom_end",
                )
            if start_d <= end_d:
                # Format: "01 Jan 2026 to 07 Jan 2026" — parse_time_range handles this
                time_phrase = (
                    f"{start_d.strftime('%d %b %Y')} to {end_d.strftime('%d %b %Y')}"
                )
                time_display = f"{start_d} → {end_d}"
            else:
                st.error("Start date must be ≤ end date.")
                time_phrase = "yesterday"
                time_display = "yesterday (fallback)"
        else:
            time_phrase   = TIME_OPTIONS[time_label]
            time_display  = time_label

        st.divider()

        # Active context summary
        ready = bool(selected_client and selected_fleets)
        if ready:
            st.success(f"**{selected_client}**")
            for f in selected_fleets:
                st.caption(f"📍 {f}")
            if selected_sherpas:
                st.caption(f"🤖 {', '.join(selected_sherpas)}")
            else:
                st.caption("🤖 All sherpas")
            st.caption(f"📅 {time_display}")
        else:
            st.info("Select a client and fleet(s) to start.")

        st.divider()

        if st.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state.messages = []
            st.session_state.pending_confirmation = None
            st.rerun()

        default_email = os.environ.get("REPORT_RECIPIENT", "")
        if default_email:
            st.caption(f"📧 Default email: `{default_email}`")

    return selected_client, selected_fleets, selected_sherpas, time_phrase


# ── Confirmation panel ────────────────────────────────────────────────────────

def _build_confirmation_text(c: dict) -> str:
    """Build a natural-language one-liner describing what was understood."""
    intent = c.get("intent", "")
    item   = c.get("item") or ""
    items  = c.get("items") or []
    client = c.get("client_name", "")
    fleet  = c.get("fleet_name", "")
    time_p = c.get("time_phrase", "")
    sherpa = c.get("sherpa_hint") or ""

    # Use NLU's items list for multi-metric display
    if len(items) > 1:
        metric_text = " and ".join(m.replace("_", " ") for m in items)
    elif intent == "basic_analytics":
        metric_text = "basic analytics"
    elif item:
        metric_text = item.replace("_", " ")
    else:
        metric_text = c.get("original_prompt", "analytics").lower()

    parts = [metric_text]
    if client:
        parts.append(f"for {client}")
    if fleet:
        parts.append(f"/ {fleet}")
    if sherpa:
        parts.append(f"sherpa {sherpa}")
    if time_p:
        parts.append(f"for {time_p}")
    return " ".join(parts)


def _render_confirmation_panel(confirmation: dict) -> None:
    """Show a single editable line with what the bot understood; user can tweak and confirm."""
    c = confirmation
    summary = _build_confirmation_text(c)

    with st.container(border=True):
        st.markdown(
            "**🔍 Here's what I understood — edit the text below if needed, then run:**"
        )
        edited = st.text_area(
            label="Query summary",
            value=summary,
            height=70,
            key="conf_text",
            help=(
                "You can freely edit this. "
                "Examples: change the client name, fleet, time period, or metrics. "
                "Separate multiple metrics with 'and'."
            ),
            label_visibility="collapsed",
        )
        st.caption(
            "💡 Tip: you can write anything — "
            "e.g. *uptime and obstacle time for last 7 days*, "
            "*total trips for FLEET-X for 01 Jan 2026 to 07 Jan 2026*"
        )

        btn1, btn2 = st.columns(2)
        with btn1:
            if st.button("✅ Run Query", type="primary", use_container_width=True):
                updated = {**c, "edited_prompt": edited.strip()}
                # Re-parse edited text for intent/item (handles edits gracefully)
                if edited.strip() != summary:
                    try:
                        reparsed = _parse_for_confirmation(
                            edited.strip(),
                            c["client_name"], c["fleet_name"],
                            c["time_phrase"], c["timezone"],
                            c.get("fleet_names", []), c.get("sherpa_names", []),
                        )
                        updated = {**reparsed, "edited_prompt": edited.strip()}
                    except Exception:
                        pass  # keep original parsed data, multi-metric detection still handles it
                st.session_state.pending_action       = ("confirmed", updated)
                st.session_state.pending_confirmation = None
                st.rerun()
        with btn2:
            if st.button("❌ Cancel", use_container_width=True):
                st.session_state.pending_confirmation = None
                if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
                    st.session_state.messages.pop()
                st.rerun()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Sanjaya Analytics",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    _init_session()
    _handle_pending_actions()

    selected_client, selected_fleets, selected_sherpas, time_phrase = _render_sidebar()

    # Persist sidebar context for the next render's button-click processing
    st.session_state._ctx_client  = selected_client
    st.session_state._ctx_fleets  = selected_fleets
    st.session_state._ctx_sherpas = selected_sherpas
    st.session_state._ctx_time    = time_phrase

    ready = bool(selected_client and selected_fleets)

    # ── Chat area ─────────────────────────────────────────────────────────────
    st.markdown("# Sanjaya Analytics Chat")

    if not st.session_state.messages and not st.session_state.pending_confirmation:
        if ready:
            st.markdown("**Quick queries — click to run instantly:**")
            cols = st.columns(4)
            for i, (label, query) in enumerate(QUICK_QUERIES):
                with cols[i % 4]:
                    if st.button(label, use_container_width=True, key=f"qk_{i}"):
                        st.session_state.pending_action = ("quick_query", query)
                        st.rerun()
            st.divider()
        else:
            st.info(
                "👋 **Welcome!** Select a **Client** and **Fleet(s)** from the sidebar, "
                "then tap a quick query or type below.\n\n"
                "**Examples:** `basic analytics` · `total trips` · `uptime and obstacle time`"
            )

    # Render chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # ── Smart panels (replace chat input when active) ──────────────────────────
    chat_state = _get_chat_state()

    if chat_state == "awaiting_confirmation":
        _render_confirmation_panel(st.session_state.pending_confirmation)

    elif chat_state == "awaiting_proceed":
        st.divider()
        with st.container(border=True):
            st.markdown("**📧 Email this report as PDF**")
            default_email = os.environ.get("REPORT_RECIPIENT", "")
            email = st.text_input(
                "Recipient email address",
                value=st.session_state.get("recipient_email") or default_email,
                placeholder="email@example.com",
                help="Enter the address that should receive this PDF report",
                key="email_input",
            )
            if default_email:
                st.caption(
                    f"Default from `.env`: **{default_email}** — edit above to override for this report"
                )
            col1, col2 = st.columns(2)
            with col1:
                if st.button("📧 Send Report", type="primary", use_container_width=True):
                    st.session_state.recipient_email  = email
                    st.session_state.pending_action   = ("proceed", email)
                    st.rerun()
            with col2:
                if st.button("❌ Cancel", use_container_width=True):
                    st.session_state.pending_action = ("cancel", None)
                    st.rerun()

    elif chat_state == "awaiting_schedule":
        st.divider()
        with st.container(border=True):
            st.markdown("**⏰ Choose a recurring schedule for this report:**")
            cols = st.columns(3)
            for i, (label, cmd) in enumerate(SCHEDULE_OPTIONS):
                with cols[i % 3]:
                    if st.button(label, use_container_width=True, key=f"sched_{i}"):
                        st.session_state.pending_action = ("schedule", cmd)
                        st.rerun()

    else:
        # Normal chat input
        placeholder = (
            "Ask about analytics — e.g. 'basic analytics', 'uptime and obstacle time'…"
            if ready
            else "Select a client and fleet(s) from the sidebar first…"
        )
        prompt = st.chat_input(placeholder, disabled=not ready)

        if prompt:
            # Add user message to history immediately
            st.session_state.messages.append({"role": "user", "content": prompt})

            cmd = prompt.strip().lower()
            is_control = cmd in _CONTROL_CMDS
            is_sched   = not is_control and any(
                kw in cmd for kw in ("daily", "hourly", "weekly", "every 20", "every hour")
            )

            if is_control or is_sched:
                # Control/schedule commands skip confirmation
                with st.chat_message("user"):
                    st.markdown(prompt)
                with st.chat_message("assistant"):
                    with st.spinner("Processing…"):
                        try:
                            response = _process_message(
                                prompt, selected_client, selected_fleets,
                                selected_sherpas, time_phrase,
                            )
                        except Exception as e:
                            response = f"⚠️ Error: {e}"
                    st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()
            else:
                # Parse query and show confirmation card
                with st.spinner("Parsing your query…"):
                    try:
                        primary_fleet = selected_fleets[0] if selected_fleets else ""
                        timezone      = os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata")
                        confirmation  = _parse_for_confirmation(
                            prompt, selected_client, primary_fleet,
                            time_phrase, timezone,
                            selected_fleets, selected_sherpas,
                        )
                        st.session_state.pending_confirmation = confirmation
                    except Exception as e:
                        # Parsing failed — fall back to direct processing
                        st.session_state.pending_confirmation = None
                        with st.chat_message("assistant"):
                            with st.spinner("Fetching analytics…"):
                                try:
                                    response = _process_message(
                                        prompt, selected_client, selected_fleets,
                                        selected_sherpas, time_phrase,
                                    )
                                except Exception as ex:
                                    response = f"⚠️ Error: {ex}"
                            st.markdown(response)
                        st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()


if __name__ == "__main__":
    main()
