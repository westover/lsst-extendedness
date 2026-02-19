"""
Logging utilities for the LSST Extendedness Pipeline.

This module provides structured logging using structlog,
with support for both console and JSON output.

Example:
    >>> from lsst_extendedness.utils import setup_logging, get_logger
    >>>
    >>> setup_logging(level="INFO", format="console")
    >>> logger = get_logger(__name__)
    >>> logger.info("Processing alert", alert_id=12345, mjd=60000.5)
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def setup_logging(
    level: str = "INFO",
    format: str = "console",
    *,
    include_timestamp: bool = True,
    include_location: bool = False,
) -> None:
    """Configure structured logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        format: Output format ("console" or "json")
        include_timestamp: Include timestamps in output
        include_location: Include source file/line info

    Example:
        >>> setup_logging(level="DEBUG", format="json")
    """
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    # Build processor chain
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
    ]

    if include_timestamp:
        processors.append(structlog.processors.TimeStamper(fmt="iso"))

    if include_location:
        processors.append(
            structlog.processors.CallsiteParameterAdder(
                [
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            )
        )

    processors.append(structlog.stdlib.PositionalArgumentsFormatter())
    processors.append(structlog.processors.StackInfoRenderer())
    processors.append(structlog.processors.UnicodeDecoder())

    # Add format-specific processor
    if format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            )
        )

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a logger instance.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Structured logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing started", source="kafka")
    """
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger


def bind_context(**kwargs: Any) -> None:
    """Bind context variables for all subsequent log calls.

    Args:
        **kwargs: Context key-value pairs

    Example:
        >>> bind_context(run_id="abc123", source="kafka")
        >>> logger.info("Processing")  # Will include run_id and source
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all bound context variables."""
    structlog.contextvars.clear_contextvars()
