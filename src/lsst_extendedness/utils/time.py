"""
Time conversion utilities for the LSST Extendedness Pipeline.

This module provides MJD <-> datetime conversions commonly needed
when working with astronomical data.

MJD (Modified Julian Date):
- Days since November 17, 1858 (midnight UT)
- Standard time format in astronomy
- No leap seconds to worry about

Example:
    >>> from lsst_extendedness.utils import mjd_to_datetime, datetime_to_mjd
    >>>
    >>> # Convert MJD to datetime
    >>> dt = mjd_to_datetime(60000.5)
    >>> print(dt)  # 2023-02-25 12:00:00
    >>>
    >>> # Convert datetime to MJD
    >>> from datetime import datetime
    >>> mjd = datetime_to_mjd(datetime.utcnow())
    >>> print(f"Current MJD: {mjd:.6f}")
"""

from __future__ import annotations

from datetime import datetime, timezone

# MJD epoch: November 17, 1858 at midnight UT
# In Julian Day Number (JD), this is JD 2400000.5
MJD_EPOCH_JD = 2400000.5

# Julian Day Number of Unix epoch (January 1, 1970)
UNIX_EPOCH_JD = 2440587.5

# Seconds per day
SECONDS_PER_DAY = 86400.0


def mjd_to_datetime(mjd: float) -> datetime:
    """Convert Modified Julian Date to UTC datetime.

    Args:
        mjd: Modified Julian Date

    Returns:
        UTC datetime

    Example:
        >>> dt = mjd_to_datetime(60000.5)
        >>> print(dt.isoformat())
        '2023-02-25T12:00:00+00:00'
    """
    # Convert MJD to JD
    jd = mjd + MJD_EPOCH_JD

    # Convert JD to Unix timestamp
    # Unix epoch is JD 2440587.5
    unix_timestamp = (jd - UNIX_EPOCH_JD) * SECONDS_PER_DAY

    return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)


def datetime_to_mjd(dt: datetime) -> float:
    """Convert datetime to Modified Julian Date.

    Args:
        dt: datetime (assumed UTC if no timezone)

    Returns:
        Modified Julian Date

    Example:
        >>> from datetime import datetime
        >>> mjd = datetime_to_mjd(datetime(2023, 2, 25, 12, 0, 0))
        >>> print(f"{mjd:.1f}")  # 60000.5
    """
    # Ensure we have a timestamp
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    # Get Unix timestamp
    unix_timestamp = dt.timestamp()

    # Convert to JD
    jd = (unix_timestamp / SECONDS_PER_DAY) + UNIX_EPOCH_JD

    # Convert to MJD
    return jd - MJD_EPOCH_JD


def mjd_now() -> float:
    """Get current time as MJD.

    Returns:
        Current UTC time as MJD

    Example:
        >>> mjd = mjd_now()
        >>> print(f"Current MJD: {mjd:.6f}")
    """
    return datetime_to_mjd(datetime.now(timezone.utc))


def mjd_to_iso(mjd: float) -> str:
    """Convert MJD to ISO 8601 string.

    Args:
        mjd: Modified Julian Date

    Returns:
        ISO 8601 formatted string

    Example:
        >>> print(mjd_to_iso(60000.5))
        '2023-02-25T12:00:00+00:00'
    """
    return mjd_to_datetime(mjd).isoformat()


def iso_to_mjd(iso_string: str) -> float:
    """Convert ISO 8601 string to MJD.

    Args:
        iso_string: ISO 8601 formatted string

    Returns:
        Modified Julian Date

    Example:
        >>> mjd = iso_to_mjd("2023-02-25T12:00:00Z")
        >>> print(f"{mjd:.1f}")  # 60000.5
    """
    dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    return datetime_to_mjd(dt)


def days_between_mjd(mjd1: float, mjd2: float) -> float:
    """Calculate days between two MJDs.

    Args:
        mjd1: First MJD
        mjd2: Second MJD

    Returns:
        Difference in days (positive if mjd2 > mjd1)

    Example:
        >>> days = days_between_mjd(60000.0, 60007.5)
        >>> print(f"{days} days")  # 7.5 days
    """
    return mjd2 - mjd1


def mjd_to_date_string(mjd: float, format: str = "%Y-%m-%d") -> str:
    """Format MJD as a date string.

    Args:
        mjd: Modified Julian Date
        format: strftime format string

    Returns:
        Formatted date string

    Example:
        >>> print(mjd_to_date_string(60000.5))
        '2023-02-25'
    """
    return mjd_to_datetime(mjd).strftime(format)
