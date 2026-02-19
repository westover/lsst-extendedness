"""
Configurable filter engine for the LSST Extendedness Pipeline.

This module provides SQL-based filtering of alerts:

- Apply extendedness thresholds
- Filter by SSObject presence
- Custom SQL conditions
- Preset filter configurations

Filter results are stored in `alerts_filtered` table with references
to the original raw alerts for reproducibility.

Example:
    >>> from lsst_extendedness.filter import FilterEngine, FilterConfig
    >>>
    >>> config = FilterConfig(
    ...     extendedness_min=0.3,
    ...     extendedness_max=0.7,
    ...     require_sso=True,
    ... )
    >>>
    >>> engine = FilterEngine(storage, config)
    >>> filtered_count = engine.apply_filter()
    >>> print(f"Filtered to {filtered_count} alerts")

Presets are available for common use cases:
    >>> from lsst_extendedness.filter.presets import MINIMOON_CANDIDATE
    >>> engine = FilterEngine(storage, MINIMOON_CANDIDATE)
"""

from lsst_extendedness.filter.engine import FilterEngine, FilterConfig
from lsst_extendedness.filter.presets import (
    POINT_SOURCES,
    EXTENDED_SOURCES,
    SSO_ASSOCIATED,
    MINIMOON_CANDIDATE,
)

__all__ = [
    "FilterEngine",
    "FilterConfig",
    "POINT_SOURCES",
    "EXTENDED_SOURCES",
    "SSO_ASSOCIATED",
    "MINIMOON_CANDIDATE",
]
