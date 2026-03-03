from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import dateparser
import re


@dataclass(frozen=True)
class TimeRange:
    start: datetime
    end: datetime

    def to_strings(self) -> dict[str, str]:
        # API expects space-separated format: "YYYY-MM-DD HH:MM:SS"
        fmt = "%Y-%m-%d %H:%M:%S"
        return {
            "start_time": self.start.strftime(fmt),
            "end_time": self.end.strftime(fmt),
        }


def _start_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _end_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=23, minute=59, second=59, microsecond=0)


def _start_of_week(dt: datetime) -> datetime:
    # Monday as start of week
    monday = dt - timedelta(days=dt.weekday())
    return _start_of_day(monday)


def _end_of_week(dt: datetime) -> datetime:
    return _end_of_day(_start_of_week(dt) + timedelta(days=6))


def _quarter_start(dt: datetime) -> datetime:
    q = (dt.month - 1) // 3
    month = 1 + q * 3
    return dt.replace(month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _quarter_end(dt: datetime) -> datetime:
    qs = _quarter_start(dt)
    # next quarter start minus 1 second
    month = qs.month + 3
    year = qs.year
    if month > 12:
        year += 1
        month -= 12
    next_q = qs.replace(year=year, month=month, day=1)
    return next_q - timedelta(seconds=1)


def parse_time_range(
    text: str,
    time_zone: str = "Asia/Kolkata",
    now: Optional[datetime] = None,
    **kwargs,
) -> TimeRange:
    """
    Parse natural time expressions into (start, end) datetimes.

    ✅ Accepts BOTH `time_zone=` and `timezone=` as callers may pass either.

    Supported examples:
    - today, yesterday, day before yesterday
    - last hour, last 3 hours
    - this week, previous week, last week
    - previous month, last month, this month
    - this quarter, last quarter
    - in 2026, in 2025
    - explicit dates: "10th Jan 2026", "10-01-26", "2026-01-10"
    - explicit ranges: "10 Jan 2026 to 12 Jan 2026"

    Returns timezone-aware datetimes in the given tz.
    """

    # ---- Compatibility: allow timezone=... ----
    if "timezone" in kwargs and kwargs["timezone"]:
        tz_value = kwargs["timezone"]
        # Handle None, "null", "None" strings
        if tz_value and str(tz_value).lower() not in ("null", "none", ""):
            time_zone = str(tz_value)

    # Ensure time_zone is valid
    if not time_zone or str(time_zone).lower() in ("null", "none", ""):
        time_zone = "Asia/Kolkata"

    tz = ZoneInfo(time_zone)
    now = (now or datetime.now(tz)).astimezone(tz)

    raw = text or ""
    t = raw.strip().lower()

    # Helper to match inside longer sentences:
    # e.g. "total trips today" should match today
    def has(phrase: str) -> bool:
        return phrase in t

    # ---- Quick keywords ----
    if has("today"):
        return TimeRange(_start_of_day(now), now.replace(microsecond=0))

    if has("day before yesterday") or has("day-before-yesterday") or has("day_before_yesterday"):
        d = now - timedelta(days=2)
        return TimeRange(_start_of_day(d), _end_of_day(d))

    if has("yesterday"):
        y = now - timedelta(days=1)
        return TimeRange(_start_of_day(y), _end_of_day(y))

    # last hour
    if has("last hour"):
        return TimeRange((now - timedelta(hours=1)).replace(microsecond=0), now.replace(microsecond=0))

    # last N hours
    m = re.search(r"\blast\s+(\d+)\s*hours?\b", t)
    if m:
        hours = int(m.group(1))
        return TimeRange((now - timedelta(hours=hours)).replace(microsecond=0), now.replace(microsecond=0))

    # last N days
    m = re.search(r"\blast\s+(\d+)\s+days?\b", t)
    if m:
        days = int(m.group(1))
        start = now - timedelta(days=days)
        return TimeRange(_start_of_day(start), now.replace(microsecond=0))
    
    # N days back
    m = re.search(r"\b(\d+)\s+days?\s+back\b", t)
    if m:
        days = int(m.group(1))
        d = now - timedelta(days=days)
        return TimeRange(_start_of_day(d), _end_of_day(d))

    # ---- Week ----
    if has("this week"):
        return TimeRange(_start_of_week(now), now.replace(microsecond=0))

    if has("previous week") or has("last week"):
        last_week_ref = now - timedelta(days=7)
        return TimeRange(_start_of_week(last_week_ref), _end_of_week(last_week_ref))

    # ---- Month ----
    if has("this month"):
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return TimeRange(start, now.replace(microsecond=0))

    if has("previous month") or has("last month"):
        first_this = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_month_end = first_this - timedelta(seconds=1)
        last_month_start = last_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return TimeRange(last_month_start, _end_of_day(last_month_end))

    # ---- Quarter ----
    if has("this quarter"):
        return TimeRange(_quarter_start(now), now.replace(microsecond=0))

    if has("previous quarter") or has("last quarter"):
        prev = _quarter_start(now) - timedelta(seconds=1)
        return TimeRange(_quarter_start(prev), _quarter_end(prev))

    # ---- Year ----
    m = re.search(r"\bin\s+(\d{4})\b", t)
    if m:
        year = int(m.group(1))
        start = datetime(year, 1, 1, 0, 0, 0, tzinfo=tz)
        end = datetime(year, 12, 31, 23, 59, 59, tzinfo=tz)
        return TimeRange(start, end)

    # ---- Explicit range "X to Y" ----
    if " to " in t:
        left, right = t.split(" to ", 1)
        start_dt = _parse_date_like(left.strip(), tz)
        end_dt = _parse_date_like(right.strip(), tz)
        if start_dt and end_dt:
            return TimeRange(_start_of_day(start_dt), _end_of_day(end_dt))

    # ---- Single explicit date ----
    dt = _parse_date_like(t, tz)
    if dt:
        return TimeRange(_start_of_day(dt), _end_of_day(dt))

    # ---- Fallback: dateparser on full text ----
    parsed = dateparser.parse(
        raw,
        settings={
            "TIMEZONE": time_zone,  # ✅ FIXED (was tz_name bug)
            "RETURN_AS_TIMEZONE_AWARE": True,
            "RELATIVE_BASE": now,
            "PREFER_DAY_OF_MONTH": "first",
            "PREFER_DATES_FROM": "past",
        },
    )

    if parsed:
        parsed = parsed.astimezone(tz)
        if parsed > now:
            parsed = now

        # if user gave a time, treat as point window
        if re.search(r"\b\d{1,2}:\d{2}\b", t) or any(k in t for k in ["am", "pm", "hour", "minute"]):
            return TimeRange(parsed.replace(microsecond=0), now.replace(microsecond=0))

        return TimeRange(_start_of_day(parsed), _end_of_day(parsed))

    # Default: today so far
    return TimeRange(_start_of_day(now), now.replace(microsecond=0))


def _parse_date_like(text: str, tz: ZoneInfo) -> Optional[datetime]:
    parsed = dateparser.parse(
        text,
        settings={
            "RETURN_AS_TIMEZONE_AWARE": True,
            "TIMEZONE": str(tz),
            "PREFER_DAY_OF_MONTH": "first",
            "PREFER_DATES_FROM": "past",
        },
    )
    if not parsed:
        return None
    return parsed.astimezone(tz)

