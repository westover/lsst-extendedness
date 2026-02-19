"""
Configurable filter engine for the LSST Extendedness Pipeline.

This module provides SQL-based filtering of alerts:

- Apply extendedness thresholds
- Filter by SSObject presence
- Custom SQL conditions
- Preset filter configurations

Filter results are stored in `alerts_filtered` table with references
to the original raw alerts for reproducibility.

Example using quick filter:
    >>> from lsst_extendedness.filter import FilterEngine
    >>>
    >>> engine = FilterEngine(storage)
    >>> df = engine.filter(
    ...     extendedness_min=0.3,
    ...     extendedness_max=0.7,
    ...     has_sso=True,
    ... )
    >>> print(f"Found {len(df)} alerts")

Example using FilterConfig:
    >>> from lsst_extendedness.filter import FilterConfig, FilterCondition
    >>>
    >>> config = FilterConfig(name="my_filter")
    >>> config.add(FilterCondition.ge("snr", 10))
    >>> config.add(FilterCondition.between("extendedness_median", 0.3, 0.7))
    >>>
    >>> df = engine.apply(config)

Presets are available for common use cases:
    >>> from lsst_extendedness.filter import presets
    >>>
    >>> config = presets.minimoon_candidates()
    >>> df = engine.apply(config)
"""

from lsst_extendedness.filter.engine import (
    FilterCondition,
    FilterConfig,
    FilterEngine,
    FilterOperator,
)
from lsst_extendedness.filter import presets

__all__ = [
    "FilterCondition",
    "FilterConfig",
    "FilterEngine",
    "FilterOperator",
    "presets",
]
