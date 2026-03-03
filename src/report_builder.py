"""Shared report builder: PDF from analytics payload + send email. Used by MCP server (auto) and report_job.py."""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Collection, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

DEFAULT_TZ = os.environ.get("SANJAYA_DEFAULT_TZ", "Asia/Kolkata")
REPORT_RECIPIENT = os.environ.get("REPORT_RECIPIENT", "akshay.kashyap@atimotors.com")

try:
    from pytz import timezone as tz
except ImportError:
    tz = None


def _table_style():
    """Shared table style for report tables."""
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
        ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ])


# Section keys used when filtering by prompt (sections_to_include)
SECTION_TRIPS = "trips"
SECTION_SHERPA_STATUS = "sherpa_status"
SECTION_ACTIVITY = "activity"


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
    """Build PDF from basic_analytics payload. report_dir: directory for logo lookup.
    Only includes sections for which data is present.
    If sections_to_include is set (e.g. {"trips"}), only those sections are included;
    if None, all sections with data are included (full report).
    """
    def _include(section: str) -> bool:
        if sections_to_include is None:
            return True
        return section in sections_to_include

    # Some APIs return { "data": { "total_trips": ..., "sherpa_wise_trips": ... } }; use inner dict if present
    if isinstance(payload.get("data"), dict):
        payload = payload["data"]
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(pdf_path, pagesize=landscape(letter))

    title_text = f"Analytics Report - {fleet_name} - {time_range}"
    sub_text = f"Client: {client_name}  |  Period: {time_strings['start_time']} to {time_strings['end_time']}"

    logo_path = None
    if report_dir:
        logo_path = os.path.join(report_dir, "ati_new_logo.png")
    if not logo_path or not os.path.isfile(logo_path):
        logo_path = "/home/ubuntu/automated_reports/ati_new_logo.png"
    elements = []
    if os.path.isfile(logo_path):
        logo = Image(logo_path)
        logo.drawWidth = 1 * inch
        logo.drawHeight = 0.5 * inch
        logo.hAlign = "RIGHT"
        elements.append(logo)

    elements.append(Paragraph(title_text, styles["Heading2"]))
    elements.append(Spacer(1, 0.2 * inch))
    elements.append(Paragraph(sub_text, styles["Normal"]))

    # Trips section: only when we have trip data and prompt asked for it (or include all)
    st = payload.get("sherpa_wise_trips") or []
    total_trips = payload.get("total_trips")
    total_km = payload.get("total_distance_km")
    has_trip_data = len(st) > 0 or total_trips is not None or total_km is not None
    if _include(SECTION_TRIPS) and has_trip_data:
        elements.append(Paragraph("Summary", styles["Heading3"]))
        flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Metric", "Value"]]]
        flowable_data.append([Paragraph("Total trips", styles["Normal"]), Paragraph(str(total_trips or 0), styles["Normal"])])
        flowable_data.append([Paragraph("Total distance (km)", styles["Normal"]), Paragraph(str(total_km or 0), styles["Normal"])])
        elements.append(Table(flowable_data, style=_table_style()))
        elements.append(Spacer(1, 0.3 * inch))
    if _include(SECTION_TRIPS) and st:
        header = ["Sherpa Name", "Trip Count"]
        flowable_data = [[Paragraph(c, styles["Normal"]) for c in header]]
        for r in st:
            flowable_data.append(
                [Paragraph(str(r.get("sherpa_name", "")), styles["Normal"]), Paragraph(str(r.get("trip_count", 0)), styles["Normal"])]
            )
        elements.append(Table(flowable_data, style=_table_style()))

    # Availability: only when data present
    av = payload.get("availability") or []
    if av:
        elements.append(Spacer(1, 0.3 * inch))
        elements.append(Paragraph("Availability", styles["Heading3"]))
        header = ["Sherpa Name", "Availability (%)"]
        flowable_data = [[Paragraph(c, styles["Normal"]) for c in header]]
        for r in av:
            val = r.get("availability_percentage") or r.get("availability", "")
            flowable_data.append([Paragraph(str(r.get("sherpa_name", "")), styles["Normal"]), Paragraph(str(val), styles["Normal"])])
        elements.append(Table(flowable_data, style=_table_style()))

    # Utilization: only when data present
    util = payload.get("utilization") or []
    if util:
        elements.append(Spacer(1, 0.3 * inch))
        elements.append(Paragraph("Utilization", styles["Heading3"]))
        header = ["Sherpa Name", "Utilization (%)"]
        flowable_data = [[Paragraph(c, styles["Normal"]) for c in header]]
        for r in util:
            flowable_data.append([Paragraph(str(r.get("sherpa_name", "")), styles["Normal"]), Paragraph(str(r.get("utilization", "")), styles["Normal"])])
        elements.append(Table(flowable_data, style=_table_style()))

    # Sherpa-wise distance: use payload; support common keys (total_distance, totalDistance, etc.)
    swd = payload.get("sherpa_wise_distance") or []
    if swd:
        elements.append(Spacer(1, 0.3 * inch))
        elements.append(Paragraph("Sherpa-wise distance", styles["Heading3"]))
        header = ["Sherpa Name", "Distance (km)"]
        flowable_data = [[Paragraph(c, styles["Normal"]) for c in header]]
        top_km = payload.get("total_distance_km")
        for r in swd:
            dist = next((r.get(k) for k in ("total_distance", "totalDistance", "distance_km", "total_distance_km", "distance") if r.get(k) is not None), None)
            if dist is None and top_km is not None and len(swd) == 1:
                dist = top_km
            if dist is None:
                dist = 0
            flowable_data.append([Paragraph(str(r.get("sherpa_name", "")), styles["Normal"]), Paragraph(str(dist), styles["Normal"])])
        elements.append(Table(flowable_data, style=_table_style()))

    elements.append(Spacer(1, 0.3 * inch))
    if tz:
        footer_text = datetime.now(tz(DEFAULT_TZ)).strftime("Generated at %Y-%m-%d %H:%M:%S")
    else:
        footer_text = datetime.now().strftime("Generated at %Y-%m-%d %H:%M:%S")
    elements.append(Paragraph(footer_text, styles["Normal"]))
    doc.build(elements)


def _parse_trips_by_sherpa(lines: List[str]) -> List[Tuple[str, str]]:
    """Parse lines like '- sherpa_name: 33' or '• sherpa_name: 33' into (sherpa_name, value)."""
    rows: List[Tuple[str, str]] = []
    for line in lines:
        line = line.strip()
        if not line or ": " not in line:
            continue
        # Strip leading bullet: dash, space, or Unicode bullet
        rest = line.lstrip("- \u2022\u2013\u2014\t")
        if ": " not in rest:
            continue
        name, value = rest.split(": ", 1)
        name, value = name.strip(), str(value).strip()
        if name:
            rows.append((name, value))
    return rows


def _parse_sherpa_status(lines: List[str]) -> List[Tuple[str, str, str, str]]:
    """Parse lines like '- name: mode=x, battery=y, updated=z' into (name, mode, battery, updated)."""
    rows: List[Tuple[str, str, str, str]] = []
    # mode=..., battery=..., updated=...
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
    """Parse lines like '- mode: 123' into (mode, time)."""
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
    """Parse 'Total trips: 121' or 'Total distance (km): 24.29' -> (label, value)."""
    line = line.strip()
    if ": " in line and not line.startswith("-"):
        label, value = line.split(": ", 1)
        return (label.strip(), value.strip())
    return None


def _parse_sherpa_value_list(value_str: str) -> Optional[List[Tuple[str, str]]]:
    """Parse comma-separated 'sherpa_name: count' (e.g. from single-metric total_trips response) into rows.
    Returns None if the string doesn't look like that format, so we don't break normal Metric|Value rows.
    """
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
        name, val = name.strip(), val.strip()
        if not name:
            return None
        rows.append((name, val))
    return rows if rows else None


def _append_section_as_paragraphs(
    elements: List[Any],
    title: str,
    lines: List[str],
    styles: Any,
) -> None:
    """Append a section as heading + body paragraphs (fallback when table parsing fails or format is unknown)."""
    elements.append(Paragraph(title, styles["Heading3"]))
    for ln in lines:
        if ln.strip():
            elements.append(Paragraph(ln.replace("\n", "<br/>"), styles["Normal"]))
    elements.append(Spacer(1, 0.2 * inch))


def build_pdf_from_text(
    report_text: str,
    client_name: str,
    fleet_name: str,
    time_range: str,
    time_strings: Dict[str, str],
    pdf_path: str,
    report_dir: str | None = None,
) -> None:
    """Build a client-ready PDF from the terminal text.

    Parses the same text we send to the client and renders it in a proper format:
    summary metrics and list sections are turned into tables; the rest stays as text.
    """
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(pdf_path, pagesize=landscape(letter))

    title_text = f"Analytics Report - {fleet_name} - {time_range}"
    sub_text = f"Client: {client_name}  |  Period: {time_strings['start_time']} to {time_strings['end_time']}"

    logo_path = None
    if report_dir:
        logo_path = os.path.join(report_dir, "ati_new_logo.png")
    if not logo_path or not os.path.isfile(logo_path):
        logo_path = "/home/ubuntu/automated_reports/ati_new_logo.png"

    elements = []
    if os.path.isfile(logo_path):
        logo = Image(logo_path)
        logo.drawWidth = 1 * inch
        logo.drawHeight = 0.5 * inch
        logo.hAlign = "RIGHT"
        elements.append(logo)

    elements.append(Paragraph(title_text, styles["Heading2"]))
    elements.append(Spacer(1, 0.2 * inch))
    elements.append(Paragraph(sub_text, styles["Normal"]))
    elements.append(Spacer(1, 0.3 * inch))

    full = report_text.strip()
    # Remove "Analytics Summary for X (Y):" header if present (we already have title)
    if full.startswith("Analytics Summary for "):
        first_nl = full.find("\n\n")
        if first_nl != -1:
            full = full[first_nl + 2 :].strip()
        else:
            full = ""

    # Split into sections by known headers (must match formatting.summarize_basic_analytics output)
    section_headers = [
        "Trips by Sherpa (top 5):",
        "Availability:",
        "Utilization:",
        "Sherpa-wise distance:",
        "Average Takt Time per Sherpa (minutes):",
    ]
    sections: List[Tuple[str, List[str]]] = []  # (title_or_type, lines)
    current: List[str] = []
    current_key: Optional[str] = None

    for raw_line in full.split("\n"):
        line = raw_line.strip()
        if not line:
            if current:
                if current_key:
                    sections.append((current_key, current))
                else:
                    sections.append(("paragraph", current))
                current = []
                current_key = None
            continue
        matched = None
        for h in section_headers:
            if line.startswith(h) or line == h.rstrip(":"):
                matched = h
                break
        if matched:
            if current:
                if current_key:
                    sections.append((current_key, current))
                else:
                    sections.append(("paragraph", current))
                current = []
            current_key = matched
            # Header line itself: skip or use as title (we use current_key as title)
            continue
        current.append(raw_line)

    if current:
        if current_key:
            sections.append((current_key, current))
        else:
            sections.append(("paragraph", current))

    for key, lines in sections:
        if not lines:
            continue
        if key == "paragraph":
            # Summary lines (Total trips, Total distance) or single-metric response (e.g. total_trips: sherpa1: n1, sherpa2: n2, ...)
            summary_pairs: List[Tuple[str, str]] = []
            other_lines: List[str] = []
            for ln in lines:
                pair = _parse_summary_line(ln)
                if pair:
                    summary_pairs.append(pair)
                else:
                    other_lines.append(ln)
            # Single-metric response often has "total_trips: tug-1: 37, tug-2: 32, ..." — render as proper table, not one cell
            sherpa_table_label: Optional[str] = None
            sherpa_table_rows: Optional[List[Tuple[str, str]]] = None
            metric_value_rows: List[Tuple[str, str]] = []
            for label, value in summary_pairs:
                parsed = _parse_sherpa_value_list(value)
                if parsed and label.lower() in ("total_trips", "trips", "total_distance_km", "total_distance", "distance"):
                    sherpa_table_label = "Trip Count" if label.lower() in ("total_trips", "trips") else "Distance (km)"
                    sherpa_table_rows = parsed
                else:
                    metric_value_rows.append((label, value))
            if sherpa_table_rows:
                elements.append(Paragraph("Trips by Sherpa" if "trip" in (sherpa_table_label or "").lower() else "Distance by Sherpa", styles["Heading3"]))
                flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Sherpa Name", sherpa_table_label or "Value"]]]
                for name, val in sherpa_table_rows:
                    flowable_data.append([Paragraph(name, styles["Normal"]), Paragraph(val, styles["Normal"])])
                elements.append(Table(flowable_data, style=_table_style()))
                elements.append(Spacer(1, 0.25 * inch))
            if metric_value_rows:
                flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Metric", "Value"]]]
                for label, value in metric_value_rows:
                    flowable_data.append([Paragraph(label, styles["Normal"]), Paragraph(value, styles["Normal"])])
                elements.append(Table(flowable_data, style=_table_style()))
                elements.append(Spacer(1, 0.2 * inch))
            for ln in other_lines:
                if ln.strip():
                    elements.append(Paragraph(ln.replace("\n", "<br/>"), styles["Normal"]))
            elements.append(Spacer(1, 0.2 * inch))
            continue

        if key == "Trips by Sherpa (top 5):":
            rows = _parse_trips_by_sherpa(lines)
            if rows:
                elements.append(Paragraph("Trips by Sherpa (top 5)", styles["Heading3"]))
                flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Sherpa Name", "Trip Count"]]]
                for name, count in rows:
                    flowable_data.append([Paragraph(name, styles["Normal"]), Paragraph(count, styles["Normal"])])
                elements.append(Table(flowable_data, style=_table_style()))
                elements.append(Spacer(1, 0.25 * inch))
            elif lines:
                _append_section_as_paragraphs(elements, "Trips by Sherpa (top 5)", lines, styles)
            continue

        if key == "Availability:":
            rows = _parse_trips_by_sherpa(lines)
            if rows:
                elements.append(Paragraph("Availability", styles["Heading3"]))
                flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Sherpa Name", "Availability (%)"]]]
                for name, val in rows:
                    flowable_data.append([Paragraph(name, styles["Normal"]), Paragraph(val, styles["Normal"])])
                elements.append(Table(flowable_data, style=_table_style()))
                elements.append(Spacer(1, 0.25 * inch))
            elif lines:
                rows = _parse_trips_by_sherpa([ln.strip() for ln in lines])
                if rows:
                    elements.append(Paragraph("Availability", styles["Heading3"]))
                    flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Sherpa Name", "Availability (%)"]]]
                    for name, val in rows:
                        flowable_data.append([Paragraph(name, styles["Normal"]), Paragraph(val, styles["Normal"])])
                    elements.append(Table(flowable_data, style=_table_style()))
                    elements.append(Spacer(1, 0.25 * inch))
                else:
                    _append_section_as_paragraphs(elements, "Availability", lines, styles)
            continue

        if key == "Utilization:":
            rows = _parse_trips_by_sherpa(lines)
            if rows:
                elements.append(Paragraph("Utilization", styles["Heading3"]))
                flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Sherpa Name", "Utilization (%)"]]]
                for name, val in rows:
                    flowable_data.append([Paragraph(name, styles["Normal"]), Paragraph(val, styles["Normal"])])
                elements.append(Table(flowable_data, style=_table_style()))
                elements.append(Spacer(1, 0.25 * inch))
            elif lines:
                rows = _parse_trips_by_sherpa([ln.strip() for ln in lines])
                if rows:
                    elements.append(Paragraph("Utilization", styles["Heading3"]))
                    flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Sherpa Name", "Utilization (%)"]]]
                    for name, val in rows:
                        flowable_data.append([Paragraph(name, styles["Normal"]), Paragraph(val, styles["Normal"])])
                    elements.append(Table(flowable_data, style=_table_style()))
                    elements.append(Spacer(1, 0.25 * inch))
                else:
                    _append_section_as_paragraphs(elements, "Utilization", lines, styles)
            continue

        if key == "Sherpa-wise distance:":
            rows = _parse_trips_by_sherpa(lines)
            if rows:
                elements.append(Paragraph("Sherpa-wise distance", styles["Heading3"]))
                flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Sherpa Name", "Distance (km)"]]]
                for name, val in rows:
                    flowable_data.append([Paragraph(name, styles["Normal"]), Paragraph(val, styles["Normal"])])
                elements.append(Table(flowable_data, style=_table_style()))
                elements.append(Spacer(1, 0.25 * inch))
            elif lines:
                rows = _parse_trips_by_sherpa([ln.strip() for ln in lines])
                if rows:
                    elements.append(Paragraph("Sherpa-wise distance", styles["Heading3"]))
                    flowable_data = [[Paragraph(c, styles["Normal"]) for c in ["Sherpa Name", "Distance (km)"]]]
                    for name, val in rows:
                        flowable_data.append([Paragraph(name, styles["Normal"]), Paragraph(val, styles["Normal"])])
                    elements.append(Table(flowable_data, style=_table_style()))
                    elements.append(Spacer(1, 0.25 * inch))
                else:
                    _append_section_as_paragraphs(elements, "Sherpa-wise distance", lines, styles)
            continue

        if key == "Average Takt Time per Sherpa (minutes):":
            # Keep as formatted text or could add a table parser later
            elements.append(Paragraph("Average Takt Time per Sherpa (minutes)", styles["Heading3"]))
            for ln in lines:
                if ln.strip():
                    elements.append(Paragraph(ln.replace("\n", "<br/>"), styles["Normal"]))
            elements.append(Spacer(1, 0.2 * inch))
            continue

        # Unknown section: render as paragraph
        elements.append(Paragraph(key, styles["Heading3"]))
        for ln in lines:
            if ln.strip():
                elements.append(Paragraph(ln.replace("\n", "<br/>"), styles["Normal"]))
        elements.append(Spacer(1, 0.2 * inch))

    elements.append(Spacer(1, 0.3 * inch))
    if tz:
        footer_text = datetime.now(tz(DEFAULT_TZ)).strftime("Generated at %Y-%m-%d %H:%M:%S")
    else:
        footer_text = datetime.now().strftime("Generated at %Y-%m-%d %H:%M:%S")
    elements.append(Paragraph(footer_text, styles["Normal"]))

    doc.build(elements)


def send_report_email(
    pdf_path: str,
    subject: str,
    report_dir: str | None = None,
    recipients: Optional[List[str]] = None,
) -> None:
    """Load .ses_client and send PDF to recipients (or REPORT_RECIPIENT env default).

    Args:
        pdf_path: Path to the PDF file to attach.
        subject: Email subject line.
        report_dir: Directory containing .ses_client.py. Defaults to project root.
        recipients: Override list of recipient email addresses. If None, uses REPORT_RECIPIENT env var.
    """
    if not report_dir:
        report_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    import importlib.util
    spec = importlib.util.spec_from_file_location("ses_email", os.path.join(report_dir, ".ses_client.py"))
    ses = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ses)
    to = recipients if recipients else [REPORT_RECIPIENT]
    ses.send_email(pdf_path, to, [], subject)
