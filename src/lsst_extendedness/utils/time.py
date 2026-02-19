"""
Time conversion utilities for astronomical calculations.

Provides conversions between:
- Python datetime objects
- Modified Julian Date (MJD)
- Julian Date (JD)

The Modified Julian Date is defined as MJD = JD - 2400000.5
where JD is the Julian Date. This shifts the epoch to
November 17, 1858, 00:00:00 UTC.

Example:
    >>> from datetime import datetime
    >>> from lsst_extendedness.utils.time import datetime_to_mjd, mjd_to_datetime
    >>>
    >>> now = datetime.utcnow()
    >>> mjd = datetime_to_mjd(now)
    >>> print(f"Current MJD: {mjd}")
    >>>
    >>> # Convert back
    >>> dt = mjd_to_datetime(mjd)
    >>> print(f"Datetime: {dt}")
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

# Julian Date of Unix epoch (January 1, 1970, 00:00:00 UTC)
JD_UNIX_EPOCH = 2440587.5

# Offset between JD and MJD
JD_MJD_OFFSET = 2400000.5


def datetime_to_mjd(dt: datetime) -> float:
    """Convert a datetime to Modified Julian Date.

    Args:
        dt: Python datetime object (assumes UTC if naive)

    Returns:
        Modified Julian Date as float

    Example:
        >>> from datetime import datetime
        >>> mjd = datetime_to_mjd(datetime(2024, 1, 1, 12, 0, 0))
        >>> print(f"MJD: {mjd:.5f}")
    """
    # Convert to UTC if timezone-aware
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)

    # Calculate days since Unix epoch
    unix_epoch = datetime(1970, 1, 1)
    delta = dt - unix_epoch
    days_since_epoch = delta.total_seconds() / 86400.0

    # Convert to JD then MJD
    jd = JD_UNIX_EPOCH + days_since_epoch
    mjd = jd - JD_MJD_OFFSET

    return mjd


def mjd_to_datetime(mjd: float) -> datetime:
    """Convert Modified Julian Date to datetime.

    Args:
        mjd: Modified Julian Date

    Returns:
        Python datetime object (UTC)

    Example:
        >>> dt = mjd_to_datetime(60310.5)
        >>> print(dt.strftime("%Y-%m-%d %H:%M:%S"))
    """
    # Convert MJD to JD
    jd = mjd + JD_MJD_OFFSET

    # Calculate days since Unix epoch
    days_since_epoch = jd - JD_UNIX_EPOCH

    # Convert to datetime
    unix_epoch = datetime(1970, 1, 1)
    return unix_epoch + timedelta(days=days_since_epoch)


def days_ago_mjd(days: int) -> float:
    """Get the MJD for a date N days in the past.

    Args:
        days: Number of days in the past

    Returns:
        MJD for that date

    Example:
        >>> threshold = days_ago_mjd(90)  # 90 days ago
        >>> print(f"Threshold MJD: {threshold:.2f}")
    """
    now = datetime.utcnow()
    past = now - timedelta(days=days)
    return datetime_to_mjd(past)


def current_mjd() -> float:
    """Get the current Modified Julian Date.

    Returns:
        Current MJD

    Example:
        >>> mjd = current_mjd()
        >>> print(f"Current MJD: {mjd:.5f}")
    """
    return datetime_to_mjd(datetime.utcnow())
