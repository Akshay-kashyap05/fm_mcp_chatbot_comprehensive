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
from datetime import date, datetime as _dt, time as _time, timedelta
from typing import Optional

try:
    import pandas as pd
    _PANDAS_OK = True
except ImportError:
    _PANDAS_OK = False

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from dotenv import load_dotenv

load_dotenv()

import streamlit as st

from src.logging_config import setup_logging
from src.nlu import parse_query
import src.mcp_client as mcp_client

setup_logging(os.environ.get("LOG_LEVEL", "WARNING"))

# MCP server URL — set MCP_SERVER_URL in .env (or docker-compose environment)
_MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8000/sse")

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
        "mcp_url":              _MCP_SERVER_URL,
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
    url = st.session_state.mcp_url
    return _run(mcp_client.list_clients(server_url=url))


def _load_fleets(client_name: str) -> list[str]:
    url = st.session_state.mcp_url
    return _run(mcp_client.list_fleets(client_name, server_url=url))


def _load_sherpas(client_name: str, fleet_names: list[str]) -> list[str]:
    if not fleet_names:
        return []
    url = st.session_state.mcp_url
    return _run(mcp_client.list_sherpas(client_name, fleet_names, server_url=url))


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



# ── Main message processor ────────────────────────────────────────────────────

def _process_message(
    prompt: str,
    client_name: str       = "",   # reserved for dropdown-backup mode
    fleet_names: list[str] = None, # reserved for dropdown-backup mode
    sherpa_names: list[str]= None, # reserved for dropdown-backup mode
    time_phrase: str       = "",   # reserved for dropdown-backup mode
    recipient_email: Optional[str] = None,
) -> str:
    """Send any message (analytics query, proceed, cancel, schedule) to the MCP server.

    Prompt-first mode: client/fleet/time are extracted by NLU on the server side.
    The sidebar params (client_name, fleet_names, time_phrase) are accepted but NOT
    forwarded — re-enable the commented lines below to use them as fallbacks.
    """
    url      = st.session_state.mcp_url
    timezone = os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata")
    # Control commands AND schedule commands must NOT carry fleet_names.
    # sanjaya_chat routes on len(fleet_names) > 1 BEFORE checking the command,
    # which causes multi-fleet analytics re-run instead of email send / schedule.
    _sched_prefixes = ("daily", "hourly", "weekly", "every 20", "every hour")
    _p = prompt.strip().lower()
    is_ctrl = _p in _CONTROL_CMDS or any(_p.startswith(kw) for kw in _sched_prefixes)
    return _run(mcp_client.chat(
        prompt,
        # Dropdown values take priority; NLU fallback when empty
        client_name="" if is_ctrl else client_name,
        fleet_name="" if is_ctrl else (fleet_names[0] if fleet_names else ""),
        fleet_names=None if is_ctrl else (fleet_names if fleet_names and len(fleet_names) > 1 else None),
        sherpa_name="" if is_ctrl else (sherpa_names[0] if sherpa_names else ""),
        sherpa_names=None if is_ctrl else (sherpa_names if sherpa_names else None),
        time_phrase="" if is_ctrl else time_phrase,
        timezone=timezone,
        recipient_email=recipient_email or None,
        server_url=url,
    ))


def _process_confirmed(data: dict) -> str:
    """Execute analytics with the user-approved text from the confirmation card.

    Prompt-first mode: the edited prompt text is sent as-is; the MCP server re-runs
    NLU to extract client, fleet, time, and metrics.  The NLU-resolved values from
    the confirmation card (data["client_name"] etc.) are available here if you want
    to re-enable sidebar fallback — see commented lines below.
    """
    url         = st.session_state.mcp_url
    prompt_text = data.get("edited_prompt") or data.get("original_prompt", "")
    timezone    = data.get("timezone", os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata"))
    fl_list = data.get("fleet_names") or ([data.get("fleet_name", "")] if data.get("fleet_name") else [])
    return _run(mcp_client.chat(
        prompt_text,
        client_name=data.get("client_name", ""),
        fleet_name=fl_list[0] if fl_list else "",
        fleet_names=fl_list if len(fl_list) > 1 else None,
        time_phrase=data.get("time_phrase", ""),
        timezone=timezone,
        server_url=url,
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


# ── Assistant message renderer ────────────────────────────────────────────────

def _render_assistant_message(content: str) -> None:
    """Render an assistant message using rich Streamlit components.

    Segments:
    - Line containing 📅  → st.info() (time/context header)
    - "---" separators   → st.divider()
    - Markdown tables    → st.dataframe() (pandas required, else st.markdown())
    - Proceed/cancel     → st.caption()
    - Everything else    → st.markdown() (flushed in chunks)
    """
    lines = content.split("\n")
    buffer: list[str] = []

    def _flush() -> None:
        if buffer:
            txt = "\n".join(buffer).strip()
            if txt:
                st.markdown(txt)
            buffer.clear()

    def _parse_md_row(row: str) -> list[str]:
        return [c.strip() for c in row.strip().strip("|").split("|")]

    i = 0
    while i < len(lines):
        line = lines[i]

        # ── Time / context header (contains 📅) ───────────────────────────────
        if "📅" in line:
            _flush()
            clean = re.sub(r"\*\*|&nbsp;", "", line).strip().strip("|").strip()
            st.info(clean)
            i += 1
            continue

        # ── Horizontal rule ───────────────────────────────────────────────────
        if line.strip() == "---":
            _flush()
            st.divider()
            i += 1
            continue

        # ── Markdown table ────────────────────────────────────────────────────
        if line.strip().startswith("|"):
            _flush()
            table_lines: list[str] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                table_lines.append(lines[i])
                i += 1
            # Need at least: header row + separator row + 1 data row
            if _PANDAS_OK and len(table_lines) >= 3:
                try:
                    headers = _parse_md_row(table_lines[0])
                    data_rows = [_parse_md_row(r) for r in table_lines[2:]]
                    df = pd.DataFrame(data_rows, columns=headers)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    continue
                except Exception:
                    pass
            # Fallback: plain markdown
            st.markdown("\n".join(table_lines))
            continue

        # ── Proceed / cancel footer ───────────────────────────────────────────
        if "type **proceed**" in line.lower() or "type proceed" in line.lower():
            _flush()
            st.caption(re.sub(r"\*\*", "", line).strip())
            i += 1
            continue

        buffer.append(line)
        i += 1

    _flush()


# ── Sidebar ───────────────────────────────────────────────────────────────────

def _render_sidebar() -> tuple[str, list[str], list[str], str]:
    with st.sidebar:
        st.markdown("## 📊 Sanjaya Analytics")
        st.divider()

        # ── Client / Fleet / Sherpa dropdowns ────────────────────────────────
        # Sidebar values have PRIORITY over NLU.  Leave selections empty to let
        # NLU extract them from your prompt instead.
        if not st.session_state.clients_list:
            with st.spinner("Loading clients…"):
                try:
                    st.session_state.clients_list = _load_clients()
                except Exception as e:
                    st.error(f"Could not load clients: {e}")

        st.subheader("Query Context")
        st.caption("Select to override NLU, or leave blank to use your prompt.")

        selected_client: str = st.selectbox(
            "Client",
            options=[""] + st.session_state.clients_list,
            format_func=lambda x: "— from prompt —" if x == "" else x,
            key="sel_client",
        )

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
            "Fleet(s)", options=fleet_options, disabled=not selected_client,
            placeholder="All fleets (default)…", key="sel_fleets",
        )

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
            "Sherpa(s)", options=sherpa_options, disabled=not selected_fleets,
            placeholder="All sherpas (default)…", key="sel_sherpas",
        )

        st.divider()

        # ── Time range with custom date + time pickers ────────────────────────
        time_label: str = st.selectbox(
            "Time Range",
            options=list(TIME_OPTIONS.keys()),
            index=1,  # default: Yesterday
            key="sel_time",
        )

        if time_label == "Custom Range":
            st.caption("Pick a date and time range:")
            col1, col2 = st.columns(2)
            with col1:
                start_d: date = st.date_input(
                    "From date",
                    value=date.today() - timedelta(days=7),
                    max_value=date.today(),
                    key="custom_start",
                )
                start_t: _time = st.time_input(
                    "Start time",
                    value=_time(0, 0),
                    key="custom_start_t",
                )
            with col2:
                end_d: date = st.date_input(
                    "To date",
                    value=date.today() - timedelta(days=1),
                    max_value=date.today(),
                    key="custom_end",
                )
                end_t: _time = st.time_input(
                    "End time",
                    value=_time(23, 59),
                    key="custom_end_t",
                )
            if start_d <= end_d:
                # Format: "01 Jan 2026 9am to 07 Jan 2026 8pm" — parse_time_range handles this
                def _fmt_hour(h: int) -> str:
                    if h == 0:
                        return "12am"
                    if h < 12:
                        return f"{h}am"
                    if h == 12:
                        return "12pm"
                    return f"{h - 12}pm"
                start_h = _fmt_hour(start_t.hour)
                end_h   = _fmt_hour(end_t.hour)
                time_phrase = (
                    f"{start_d.strftime('%d %b %Y')} {start_h} to "
                    f"{end_d.strftime('%d %b %Y')} {end_h}"
                )
                time_display = f"{start_d} {start_t.strftime('%H:%M')} → {end_d} {end_t.strftime('%H:%M')}"
            else:
                st.error("Start date must be ≤ end date.")
                time_phrase = "yesterday"
                time_display = "yesterday (fallback)"
        else:
            time_phrase   = TIME_OPTIONS[time_label]
            time_display  = time_label

        st.caption(f"📅 {time_display}")
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
    """Show what the bot understood (read-only), then the original prompt editable."""
    c = confirmation
    summary = _build_confirmation_text(c)
    original = c.get("original_prompt", summary)

    with st.container(border=True):
        st.markdown("**🔍 What I understood:**")
        st.caption(f"`{summary}`")
        st.markdown("**Your query — edit if needed, then run:**")
        edited = st.text_area(
            label="Query",
            value=original,
            height=70,
            key="conf_text",
            help=(
                "This is your original message. Edit it if something was misunderstood. "
                "Tip: prefix the client name with 'client' for best accuracy — "
                "e.g. 'client YOKOHAMA-DAHEJ uptime yesterday'."
            ),
            label_visibility="collapsed",
        )
        st.caption(
            "💡 **Tip:** Use `client <name>` for reliable client detection — "
            "e.g. *client CEAT-Nagpur uptime and obstacle time for last 7 days*"
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

    ready = True  # chat is always ready; dropdowns augment NLU when filled

    # ── Chat area ─────────────────────────────────────────────────────────────
    st.markdown("# Sanjaya Analytics Chat")

    if not st.session_state.messages and not st.session_state.pending_confirmation:
        # ── Onboarding / format guide ──────────────────────────────────────
        with st.container(border=True):
            st.markdown("### 👋 Welcome to Sanjaya Analytics")
            st.markdown(
                "Type your query in plain English. Here's how to get the best results:"
            )
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(
                    "**Client name** — always prefix with `client`:\n"
                    "```\nclient YOKOHAMA-DAHEJ uptime yesterday\n"
                    "client CEAT-Nagpur total trips last week\n```\n"
                    "Fuzzy matching is active — partial or approximate names work,\n"
                    "but exact spelling avoids ambiguity.\n\n"
                    "**Fleet name** — prefix with `fleet` (optional):\n"
                    "```\nclient CEAT-Nagpur fleet BEAD uptime\n```\n"
                    "Omit the fleet to query **all fleets** for that client.\n\n"
                    "**Sherpa / Tug** — use the tug ID (e.g. `tug-104`):\n"
                    "```\nclient CEAT-Nagpur tug-104 uptime yesterday\n```\n"
                    "Use `per sherpa` to get values for **every** sherpa."
                )
            with col_b:
                st.markdown(
                    "**Time period** — plain English:\n"
                    "```\nyesterday  |  today  |  last week\n"
                    "last 7 days  |  this month\n"
                    "1 Jan 2026 to 10 Jan 2026\n```\n\n"
                    "**Multiple metrics** — comma or 'and':\n"
                    "```\nclient CEAT-Nagpur uptime and obstacle time last week\n```\n\n"
                    "**Report + schedule** — after any result:\n"
                    "- Type `proceed` → email the PDF\n"
                    "- Then choose: `daily 8` / `weekly monday 9` / `skip`"
                )
        st.divider()

        if ready:
            st.markdown("**Quick queries — click to run instantly:**")
            cols = st.columns(4)
            for i, (label, query) in enumerate(QUICK_QUERIES):
                with cols[i % 4]:
                    if st.button(label, use_container_width=True, key=f"qk_{i}"):
                        st.session_state.pending_action = ("quick_query", query)
                        st.rerun()
            st.divider()

    # Render chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                _render_assistant_message(msg["content"])
            else:
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
                "Recipient email address(es)",
                value=st.session_state.get("recipient_email") or default_email,
                placeholder="a@example.com, b@example.com",
                help="One address or comma-separated list for multiple recipients",
                key="email_input",
            )
            st.caption("Multiple recipients? Separate with commas — e.g. `a@x.com, b@y.com`")
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

            st.divider()
            st.caption("Or type a custom schedule — e.g. `daily 2pm`, `daily 14:30`, `weekly friday 9am`, `hourly`")
            custom_sched = st.text_input(
                "Custom schedule", placeholder="daily 2pm  /  weekly tuesday 14:30  /  hourly",
                key="custom_sched_input", label_visibility="collapsed",
            )
            if st.button("Set custom schedule", key="sched_custom_btn", use_container_width=True):
                if custom_sched.strip():
                    st.session_state.pending_action = ("schedule", custom_sched.strip())
                    st.rerun()

    else:
        # Normal chat input
        placeholder = (
            "e.g. 'client CEAT-Nagpur uptime yesterday'  |  "
            "'client YOKOHAMA-DAHEJ obstacle time and utilization last week'"
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
                    _render_assistant_message(response)
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
                            _render_assistant_message(response)
                        st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()


if __name__ == "__main__":
    main()
