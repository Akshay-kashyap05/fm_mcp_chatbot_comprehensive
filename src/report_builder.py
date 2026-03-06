"""Shared report builder: PDF from analytics payload + send email. Used by MCP server (auto) and report_job.py."""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Collection, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable, Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from src.email_client import send_email as _send_email

DEFAULT_TZ = os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata")
REPORT_RECIPIENT = os.environ.get("REPORT_RECIPIENT", "akshay.kashyap@atimotors.com")

try:
    from pytz import timezone as tz
except ImportError:
    tz = None

# ── Palette (matches the grey/beige style shown in the approved UI) ───────────
_HEADER_BG  = colors.HexColor("#595959")   # dark-grey table header
_ROW_ALT    = colors.HexColor("#FAFAD2")   # light-goldenrod alternating row
_ROW_BASE   = colors.white
_BORDER     = colors.HexColor("#C8C8C8")   # light-grey grid lines
_FOOTER_FG  = colors.HexColor("#808080")   # footer text
_TITLE_FG   = colors.HexColor("#222222")   # page title colour
_PAGE_W     = 11 * inch                    # landscape Letter width
_MARGIN     = 0.6 * inch
_CONTENT_W  = _PAGE_W - 2 * _MARGIN       # ~9.8 in usable


# ── Table style helpers ────────────────────────────────────────────────────────

def _table_style(num_data_rows: int = 0) -> TableStyle:
    """Table style: grey header, light-goldenrod alternating rows (matches approved UI)."""
    cmds = [
        # Header row
        ("BACKGROUND",    (0, 0), (-1, 0), _HEADER_BG),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0), 10),
        ("ALIGN",         (0, 0), (-1, 0), "LEFT"),
        ("VALIGN",        (0, 0), (-1, 0), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, 0), 7),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 7),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        # Data rows
        ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",      (0, 1), (-1, -1), 9),
        ("TOPPADDING",    (0, 1), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
        ("VALIGN",        (0, 1), (-1, -1), "MIDDLE"),
        ("ALIGN",         (0, 1), (0, -1), "LEFT"),
        ("ALIGN",         (1, 1), (-1, -1), "LEFT"),
        # Grid
        ("GRID",          (0, 0), (-1, -1), 0.5, _BORDER),
    ]
    # Alternating data row backgrounds
    for i in range(1, num_data_rows + 1):
        bg = _ROW_ALT if i % 2 == 0 else _ROW_BASE
        cmds.append(("BACKGROUND", (0, i), (-1, i), bg))
    return TableStyle(cmds)


def _make_table(
    data: List[List[Any]],
    col_widths: Optional[List[float]] = None,
) -> Table:
    """Create a styled Table with optional explicit column widths."""
    n_data = max(0, len(data) - 1)
    t = Table(data, colWidths=col_widths)
    t.setStyle(_table_style(n_data))
    return t


def _section_header(title: str, styles: Any) -> List[Any]:
    """Render a section heading as bold-italic text (matches approved UI screenshot)."""
    style = ParagraphStyle(
        "SectionHead",
        parent=styles["Normal"],
        fontName="Helvetica-BoldOblique",
        fontSize=11,
        textColor=_TITLE_FG,
        spaceAfter=2,
        spaceBefore=2,
    )
    return [Spacer(1, 0.15 * inch), Paragraph(title, style), Spacer(1, 0.06 * inch)]


# ── Column-width presets (landscape Letter, ~9.8 in usable) ──────────────────
_COL_SHERPA_2  = [6.8 * inch, 3.0 * inch]   # Sherpa Name | metric
_COL_KV        = [4.0 * inch, 5.8 * inch]   # Key | Value (summary metrics)
_COL_TAKT      = [3.8 * inch, 1.5 * inch, 1.5 * inch, 1.5 * inch, 1.5 * inch]
_COL_ROUTE_2   = [7.3 * inch, 2.5 * inch]   # Route | metric


# ── Section keys ──────────────────────────────────────────────────────────────
SECTION_TRIPS          = "trips"
SECTION_AVAILABILITY   = "availability"
SECTION_UTILIZATION    = "utilization"
SECTION_DISTANCE       = "distance"
SECTION_UPTIME         = "uptime"
SECTION_ROUTE_ANALYTICS = "route_analytics"
SECTION_SHERPA_STATUS  = "sherpa_status"
SECTION_ACTIVITY       = "activity"


# ── Helper: paragraph cell ────────────────────────────────────────────────────
def _p(text: str, styles: Any, style_key: str = "Normal") -> Paragraph:
    return Paragraph(str(text), styles[style_key])


# ── Page header builder (shared) ──────────────────────────────────────────────
def _build_page_header(
    elements: List[Any],
    styles: Any,
    fleet_name: str,
    time_range: str,
    client_name: str,
    time_strings: Dict[str, str],
    report_dir: Optional[str],
) -> None:
    logo_path = None
    if report_dir:
        logo_path = os.path.join(report_dir, "ati_new_logo.png")
    if not logo_path or not os.path.isfile(logo_path):
        logo_path = "/home/ubuntu/automated_reports/ati_new_logo.png"

    # Title bar: logo on right, title text on left
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Heading1"],
        textColor=_TITLE_FG,
        fontSize=16,
        leading=20,
    )
    sub_style = ParagraphStyle(
        "ReportSub",
        parent=styles["Normal"],
        textColor=_FOOTER_FG,
        fontSize=9,
    )
    title_para = Paragraph(f"Analytics Report — {fleet_name}", title_style)
    sub_para   = Paragraph(
        f"Client: <b>{client_name}</b> &nbsp;|&nbsp; "
        f"Period: {time_strings['start_time']} to {time_strings['end_time']}",
        sub_style,
    )

    if os.path.isfile(logo_path):
        logo = Image(logo_path)
        logo.drawWidth  = 1.2 * inch
        logo.drawHeight = 0.6 * inch
        header_table = Table(
            [[title_para, logo]],
            colWidths=[_CONTENT_W - 1.4 * inch, 1.4 * inch],
        )
        header_table.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN",  (1, 0), (1, 0),   "RIGHT"),
        ]))
        elements.append(header_table)
    else:
        elements.append(title_para)

    elements.append(Spacer(1, 0.05 * inch))
    elements.append(sub_para)
    elements.append(Spacer(1, 0.05 * inch))
    elements.append(HRFlowable(width=_CONTENT_W, thickness=1.5, color=_BORDER))
    elements.append(Spacer(1, 0.15 * inch))


# ── Main PDF builder (Airflow path — raw API payload) ─────────────────────────
def build_pdf(
    payload: Dict[str, Any],
    client_name: str,
    fleet_name: str,
    time_range: str,
    time_strings: Dict[str, str],
    pdf_path: str,
    report_dir: str | None = None,
    sections_to_include: Optional[Collection[str]] = None,
) -> None:
    """Build PDF from basic_analytics payload.
    sections_to_include: set of SECTION_* constants; None = full report.
    """
    def _include(section: str) -> bool:
        return sections_to_include is None or section in sections_to_include

    if isinstance(payload.get("data"), dict):
        payload = payload["data"]

    styles  = getSampleStyleSheet()
    doc     = SimpleDocTemplate(
        pdf_path,
        pagesize=landscape(letter),
        leftMargin=_MARGIN, rightMargin=_MARGIN,
        topMargin=_MARGIN,  bottomMargin=_MARGIN,
    )
    elements: List[Any] = []
    _build_page_header(elements, styles, fleet_name, time_range, client_name, time_strings, report_dir)

    # ── Trips ──────────────────────────────────────────────────────────────
    st          = payload.get("sherpa_wise_trips") or []
    total_trips = payload.get("total_trips")
    if _include(SECTION_TRIPS) and (total_trips is not None or st):
        elements += _section_header("Trips Summary", styles)
        data = [[_p("Metric", styles), _p("Value", styles)],
                [_p("Total Trips", styles), _p(str(total_trips or 0), styles)]]
        elements.append(_make_table(data, _COL_KV))

    if _include(SECTION_TRIPS) and st:
        elements += _section_header("Trips by Sherpa", styles)
        data = [[_p("Sherpa Name", styles), _p("Trip Count", styles)]]
        for r in st:
            data.append([_p(r.get("sherpa_name", ""), styles), _p(str(r.get("trip_count", 0)), styles)])
        elements.append(_make_table(data, _COL_SHERPA_2))

    # ── Distance ───────────────────────────────────────────────────────────
    swd     = payload.get("sherpa_wise_distance") or []
    total_km = payload.get("total_distance_km")
    if _include(SECTION_DISTANCE) and (total_km is not None or swd):
        elements += _section_header("Distance Summary", styles)
        data = [[_p("Metric", styles), _p("Value", styles)],
                [_p("Total Distance (km)", styles), _p(str(total_km or 0), styles)]]
        elements.append(_make_table(data, _COL_KV))

    if _include(SECTION_DISTANCE) and swd:
        elements += _section_header("Sherpa-wise Distance", styles)
        data = [[_p("Sherpa Name", styles), _p("Distance (km)", styles)]]
        for r in swd:
            dist = next(
                (r.get(k) for k in ("total_distance", "totalDistance", "distance_km", "total_distance_km", "distance") if r.get(k) is not None),
                0,
            )
            data.append([_p(r.get("sherpa_name", ""), styles), _p(str(dist), styles)])
        elements.append(_make_table(data, _COL_SHERPA_2))

    # ── Availability ───────────────────────────────────────────────────────
    av = payload.get("availability") or []
    if _include(SECTION_AVAILABILITY) and av:
        elements += _section_header("Availability", styles)
        data = [[_p("Sherpa Name", styles), _p("Availability (%)", styles)]]
        for r in av:
            val = r.get("availability_percentage") or r.get("availability", "")
            data.append([_p(r.get("sherpa_name", ""), styles), _p(str(val), styles)])
        elements.append(_make_table(data, _COL_SHERPA_2))

    # ── Utilization ────────────────────────────────────────────────────────
    util = payload.get("utilization") or []
    if _include(SECTION_UTILIZATION) and util:
        elements += _section_header("Utilization", styles)
        data = [[_p("Sherpa Name", styles), _p("Utilization (%)", styles)]]
        for r in util:
            data.append([_p(r.get("sherpa_name", ""), styles), _p(str(r.get("utilization", "")), styles)])
        elements.append(_make_table(data, _COL_SHERPA_2))

    # ── Uptime (Runtime) ───────────────────────────────────────────────────
    upt = payload.get("uptime") or []
    if _include(SECTION_UPTIME) and upt:
        elements += _section_header("Runtime (Uptime)", styles)
        data = [[_p("Sherpa Name", styles), _p("Uptime (%)", styles)]]
        for r in upt:
            val = r.get("uptime_percentage") or r.get("uptime", "")
            data.append([_p(r.get("sherpa_name", ""), styles), _p(str(val), styles)])
        elements.append(_make_table(data, _COL_SHERPA_2))

    # ── Route Analytics ────────────────────────────────────────────────────
    if _include(SECTION_ROUTE_ANALYTICS):
        takt = payload.get("avg_takt_per_sherpa") or []
        if takt:
            elements += _section_header("Average Takt Time per Sherpa (minutes)", styles)
            data = [[_p(h, styles) for h in ["Sherpa", "Avg (min)", "Min (min)", "Max (min)", "Trips"]]]
            for r in sorted(takt, key=lambda x: x.get("avg_takt_time_minutes", 0), reverse=True):
                data.append([
                    _p(r.get("sherpa", ""), styles),
                    _p(f"{r.get('avg_takt_time_minutes', 0):.2f}", styles),
                    _p(f"{r.get('min_takt_time_minutes', 0):.2f}", styles),
                    _p(f"{r.get('max_takt_time_minutes', 0):.2f}", styles),
                    _p(str(r.get("total_trips", 0)), styles),
                ])
            elements.append(_make_table(data, _COL_TAKT))

        obstacle = payload.get("avg_obstacle_per_sherpa") or []
        if obstacle:
            elements += _section_header("Average Obstacle Time per Sherpa (minutes)", styles)
            data = [[_p("Sherpa Name", styles), _p("Avg Obstacle Time (min)", styles)]]
            for r in sorted(obstacle, key=lambda x: x.get("avg_obstacle_time_min", 0), reverse=True):
                data.append([_p(r.get("sherpa_name", ""), styles), _p(f"{r.get('avg_obstacle_time_min', 0):.2f}", styles)])
            elements.append(_make_table(data, _COL_SHERPA_2))

        top_routes = payload.get("top_10_routes_takt") or []
        if top_routes:
            elements += _section_header("Top Routes by Takt Time (minutes)", styles)
            data = [[_p("Route", styles), _p("Avg Takt (min)", styles)]]
            for r in top_routes[:10]:
                route     = r.get("route", [])
                route_str = " → ".join(route) if isinstance(route, list) else str(route)
                data.append([_p(route_str, styles), _p(f"{r.get('avg_takt_time_minutes', 0):.2f}", styles)])
            elements.append(_make_table(data, _COL_ROUTE_2))

        route_util = payload.get("route_utilization") or []
        if route_util:
            elements += _section_header("Route Utilization", styles)
            data = [[_p("Route", styles), _p("Utilization (%)", styles)]]
            for r in sorted(route_util, key=lambda x: x.get("utilization", 0), reverse=True):
                route     = r.get("route", [])
                route_str = " → ".join(route) if isinstance(route, list) else str(route)
                data.append([_p(route_str, styles), _p(f"{r.get('utilization', 0):.2f}", styles)])
            elements.append(_make_table(data, _COL_ROUTE_2))

        obstacle_route = payload.get("avg_obstacle_per_route") or []
        if obstacle_route:
            elements += _section_header("Average Obstacle Time per Route (minutes)", styles)
            data = [[_p("Route", styles), _p("Avg Obstacle Time (min)", styles)]]
            for r in sorted(obstacle_route, key=lambda x: x.get("avg_obstacle_time_min", 0), reverse=True):
                route     = r.get("route", [])
                route_str = " → ".join(route) if isinstance(route, list) else str(route)
                data.append([_p(route_str, styles), _p(f"{r.get('avg_obstacle_time_min', 0):.2f}", styles)])
            elements.append(_make_table(data, _COL_ROUTE_2))

    # ── Footer ─────────────────────────────────────────────────────────────
    elements.append(Spacer(1, 0.3 * inch))
    elements.append(HRFlowable(width=_CONTENT_W, thickness=0.5, color=_BORDER))
    footer_style = ParagraphStyle("Footer", parent=styles["Normal"], textColor=_FOOTER_FG, fontSize=8)
    ts = datetime.now(tz(DEFAULT_TZ)).strftime("%Y-%m-%d %H:%M:%S") if tz else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elements.append(Paragraph(f"Generated at {ts}  |  ATI Motors Analytics", footer_style))

    doc.build(elements)


# ── Text-parsing helpers (for build_pdf_from_text) ────────────────────────────

def _parse_trips_by_sherpa(lines: List[str]) -> List[Tuple[str, str]]:
    """Parse lines like '- sherpa_name: 33' into (sherpa_name, value)."""
    rows: List[Tuple[str, str]] = []
    for line in lines:
        line = line.strip()
        if not line or ": " not in line:
            continue
        rest = line.lstrip("- \u2022\u2013\u2014\t")
        if ": " not in rest:
            continue
        name, value = rest.split(": ", 1)
        if name.strip():
            rows.append((name.strip(), str(value).strip()))
    return rows


def _parse_metric_table_section(lines: List[str]) -> List[Tuple[str, str]]:
    """Parse lines from _format_metric_value fixed-width table output.

    Handles:
      Header line  →  '  Sherpa                Value'         (skip)
      Separator    →  '  ------- ------'                       (skip)
      Data lines   →  '  tug-51-ceat-nagpur-05    93%'
    Returns list of (sherpa_name, value) tuples.
    """
    rows: List[Tuple[str, str]] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        # Separator line (only dashes/spaces)
        if all(c in "- " for c in stripped):
            continue
        # Header line: contains "Sherpa" and "Value" as column labels
        low = stripped.lower()
        if "sherpa" in low and "value" in low and len(stripped.split()) <= 3:
            continue
        # Data line: last whitespace-separated token is the value
        parts = stripped.rsplit(None, 1)
        if len(parts) == 2:
            name, val = parts[0].strip(), parts[1].strip()
            if name and not all(c in "-_" for c in name):
                rows.append((name, val))
    return rows


def _parse_sherpa_status(lines: List[str]) -> List[Tuple[str, str, str, str]]:
    rows: List[Tuple[str, str, str, str]] = []
    pat = re.compile(r"mode=([^,]+),\s*battery=([^,]+),\s*updated=(.+)$")
    for line in lines:
        line = line.strip()
        if not line or not line.startswith("-"):
            continue
        rest = line[1:].strip()
        if ": " in rest:
            name, tail = rest.split(": ", 1)
            m = pat.search(tail.strip())
            if m:
                rows.append((name.strip(), m.group(1).strip(), m.group(2).strip(), m.group(3).strip()))
    return rows


def _parse_activity(lines: List[str]) -> List[Tuple[str, str]]:
    rows: List[Tuple[str, str]] = []
    for line in lines:
        line = line.strip()
        if not line or not line.startswith("-"):
            continue
        rest = line[1:].strip()
        if ": " in rest:
            mode, time_val = rest.split(": ", 1)
            rows.append((mode.strip(), str(time_val).strip()))
    return rows


def _parse_summary_line(line: str) -> Optional[Tuple[str, str]]:
    line = line.strip()
    if ": " in line and not line.startswith("-"):
        label, value = line.split(": ", 1)
        return (label.strip(), value.strip())
    return None


def _parse_sherpa_value_list(value_str: str) -> Optional[List[Tuple[str, str]]]:
    value_str = (value_str or "").strip()
    if not value_str or "," not in value_str:
        return None
    segments = [s.strip() for s in value_str.split(",") if s.strip()]
    if len(segments) < 2:
        return None
    rows: List[Tuple[str, str]] = []
    for seg in segments:
        if ": " not in seg:
            return None
        name, val = seg.split(": ", 1)
        if not name.strip():
            return None
        rows.append((name.strip(), val.strip()))
    return rows if rows else None


def _append_section_as_paragraphs(elements: List[Any], title: str, lines: List[str], styles: Any) -> None:
    elements += _section_header(title, styles)
    for ln in lines:
        if ln.strip():
            elements.append(Paragraph(ln.replace("\n", "<br/>"), styles["Normal"]))
    elements.append(Spacer(1, 0.2 * inch))


# ── Chat-path PDF builder (from terminal text) ────────────────────────────────

# Section titles exactly as they appear in _format_metric_value / summarize_basic_analytics output
_SECTION_HEADERS = [
    "Trips by Sherpa (top 5):",
    "Availability:",
    "Utilization:",
    "Sherpa-wise distance:",
    "Average Takt Time per Sherpa (minutes):",
    # New _format_metric_value headings
    "Uptime:",
    "Runtime (Uptime):",
    "Total Trips:",
    "Total Distance (km):",
    "Battery Level:",
]

# Map section header → (display title, value column label, is_metric_table_format)
_SECTION_META: Dict[str, Tuple[str, str, bool]] = {
    "Trips by Sherpa (top 5):":                ("Trips by Sherpa", "Trip Count",         False),
    "Availability:":                            ("Availability",    "Availability (%)",   False),
    "Utilization:":                             ("Utilization",     "Utilization (%)",    False),
    "Sherpa-wise distance:":                    ("Sherpa-wise Distance", "Distance (km)", False),
    "Uptime:":                                  ("Runtime (Uptime)", "Uptime (%)",        True),
    "Runtime (Uptime):":                        ("Runtime (Uptime)", "Uptime (%)",        True),
    "Total Trips:":                             ("Trips by Sherpa", "Trip Count",         True),
    "Total Distance (km):":                     ("Distance by Sherpa", "Distance (km)",   True),
    "Battery Level:":                           ("Battery Level",   "Battery Level (%)",  True),
}


def build_pdf_from_text(
    report_text: str,
    client_name: str,
    fleet_name: str,
    time_range: str,
    time_strings: Dict[str, str],
    pdf_path: str,
    report_dir: str | None = None,
) -> None:
    """Build a client-ready PDF from the terminal text returned by sanjaya_chat."""
    styles = getSampleStyleSheet()
    doc    = SimpleDocTemplate(
        pdf_path,
        pagesize=landscape(letter),
        leftMargin=_MARGIN, rightMargin=_MARGIN,
        topMargin=_MARGIN,  bottomMargin=_MARGIN,
    )
    elements: List[Any] = []
    _build_page_header(elements, styles, fleet_name, time_range, client_name, time_strings, report_dir)

    full = report_text.strip()
    # Strip "Analytics Summary for X (Y):" wrapper (we have our own title)
    if full.startswith("Analytics Summary for "):
        first_nl = full.find("\n\n")
        full = full[first_nl + 2:].strip() if first_nl != -1 else ""

    # ── Parse into (section_key, lines) chunks ──────────────────────────────
    sections: List[Tuple[str, List[str]]] = []
    current: List[str] = []
    current_key: Optional[str] = None

    for raw_line in full.split("\n"):
        line = raw_line.strip()
        matched = None
        if line:
            for h in _SECTION_HEADERS:
                if line.startswith(h) or line == h.rstrip(":"):
                    matched = h
                    break
        if matched:
            if current:
                sections.append((current_key or "paragraph", current))
                current = []
            current_key = matched
            continue
        if not line:
            if current:
                sections.append((current_key or "paragraph", current))
                current = []
                current_key = None
        else:
            current.append(raw_line)

    if current:
        sections.append((current_key or "paragraph", current))

    # ── Render each section ──────────────────────────────────────────────────
    for key, lines in sections:
        if not lines:
            continue

        # ── Known tabular / list sections ────────────────────────────────
        meta = _SECTION_META.get(key)
        if meta:
            display_title, val_col_label, is_metric_fmt = meta
            rows = (
                _parse_metric_table_section(lines)
                if is_metric_fmt
                else _parse_trips_by_sherpa(lines)
            )
            if rows:
                elements += _section_header(display_title, styles)
                data = [[_p("Sherpa Name", styles), _p(val_col_label, styles)]]
                for name, val in rows:
                    data.append([_p(name, styles), _p(val, styles)])
                elements.append(_make_table(data, _COL_SHERPA_2))
                elements.append(Spacer(1, 0.2 * inch))
            else:
                _append_section_as_paragraphs(elements, display_title, lines, styles)
            continue

        # ── Takt time: keep as formatted paragraphs (complex nested format) ─
        if key == "Average Takt Time per Sherpa (minutes):":
            elements += _section_header("Average Takt Time per Sherpa (minutes)", styles)
            for ln in lines:
                if ln.strip():
                    elements.append(Paragraph(ln.replace("\n", "<br/>"), styles["Normal"]))
            elements.append(Spacer(1, 0.2 * inch))
            continue

        # ── Generic paragraph section (Total trips / Total distance) ─────
        if key == "paragraph":
            summary_pairs: List[Tuple[str, str]] = []
            other_lines:   List[str] = []
            for ln in lines:
                pair = _parse_summary_line(ln)
                if pair:
                    summary_pairs.append(pair)
                else:
                    other_lines.append(ln)

            sherpa_table_label: Optional[str] = None
            sherpa_table_rows:  Optional[List[Tuple[str, str]]] = None
            metric_value_rows:  List[Tuple[str, str]] = []
            for label, value in summary_pairs:
                parsed = _parse_sherpa_value_list(value)
                if parsed and label.lower() in ("total_trips", "trips", "total_distance_km", "total_distance", "distance"):
                    sherpa_table_label = "Trip Count" if "trip" in label.lower() else "Distance (km)"
                    sherpa_table_rows  = parsed
                else:
                    metric_value_rows.append((label, value))

            if sherpa_table_rows:
                section_t = "Trips by Sherpa" if "Trip" in (sherpa_table_label or "") else "Distance by Sherpa"
                elements += _section_header(section_t, styles)
                data = [[_p("Sherpa Name", styles), _p(sherpa_table_label or "Value", styles)]]
                for name, val in sherpa_table_rows:
                    data.append([_p(name, styles), _p(val, styles)])
                elements.append(_make_table(data, _COL_SHERPA_2))
                elements.append(Spacer(1, 0.2 * inch))

            if metric_value_rows:
                data = [[_p("Metric", styles), _p("Value", styles)]]
                for label, value in metric_value_rows:
                    data.append([_p(label, styles), _p(value, styles)])
                elements.append(_make_table(data, _COL_KV))
                elements.append(Spacer(1, 0.2 * inch))

            for ln in other_lines:
                if ln.strip():
                    elements.append(Paragraph(ln.replace("\n", "<br/>"), styles["Normal"]))
            continue

        # ── Unknown section: plain text fallback ─────────────────────────
        elements += _section_header(key.rstrip(":"), styles)
        for ln in lines:
            if ln.strip():
                elements.append(Paragraph(ln.replace("\n", "<br/>"), styles["Normal"]))
        elements.append(Spacer(1, 0.2 * inch))

    # ── Footer ─────────────────────────────────────────────────────────────
    elements.append(Spacer(1, 0.3 * inch))
    elements.append(HRFlowable(width=_CONTENT_W, thickness=0.5, color=_BORDER))
    footer_style = ParagraphStyle("Footer", parent=styles["Normal"], textColor=_FOOTER_FG, fontSize=8)
    ts = datetime.now(tz(DEFAULT_TZ)).strftime("%Y-%m-%d %H:%M:%S") if tz else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elements.append(Paragraph(f"Generated at {ts}  |  ATI Motors Analytics", footer_style))

    doc.build(elements)


# ── Email sender ──────────────────────────────────────────────────────────────

def send_report_email(
    pdf_path: str,
    subject: str,
    report_dir: str | None = None,
    recipients: Optional[List[str]] = None,
) -> None:
    """Send PDF report to recipients via SMTP (or REPORT_RECIPIENT env default)."""
    to = recipients if recipients else [REPORT_RECIPIENT]
    _send_email(pdf_path, to, [], subject)
