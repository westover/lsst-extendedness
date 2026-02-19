"""
Utility functions for the LSST Extendedness Pipeline.

This module provides common utilities:

Logging:
- setup_logging(): Configure structured logging with structlog
- get_logger(name): Get a logger instance

Time conversions:
- mjd_to_datetime(mjd): Convert MJD to datetime
- datetime_to_mjd(dt): Convert datetime to MJD
- mjd_now(): Current time as MJD

Example:
    >>> from lsst_extendedness.utils import get_logger, mjd_to_datetime
    >>>
    >>> logger = get_logger(__name__)
    >>> logger.info("Processing alert", alert_id=12345)
    >>>
    >>> dt = mjd_to_datetime(60000.5)
    >>> print(f"Alert time: {dt}")
"""

from lsst_extendedness.utils.logging import setup_logging, get_logger
from lsst_extendedness.utils.time import mjd_to_datetime, datetime_to_mjd, mjd_now

__all__ = [
    "setup_logging",
    "get_logger",
    "mjd_to_datetime",
    "datetime_to_mjd",
    "mjd_now",
]
