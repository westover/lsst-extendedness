"""
Query utilities for the LSST Extendedness Pipeline.

This module provides scientist-friendly query shortcuts and export utilities:

Shortcuts (return pandas DataFrames):
- today(): Alerts from today
- recent(days=7): Alerts from last N days
- minimoon_candidates(): SSO with intermediate extendedness
- point_sources(): Low extendedness (star-like)
- extended_sources(): High extendedness (galaxy-like)
- sso_alerts(): All SSObject-associated alerts
- reassociations(): Reassociation events
- by_source(id): Alerts for a specific DIA source
- by_object(id): Alerts for a specific DIA object
- by_sso(id): Alerts for a specific SSObject
- in_region(ra, dec): Alerts in a sky region
- high_snr(min_snr): High signal-to-noise alerts

Export:
- export_dataframe(df, path, format): Export to CSV/Parquet/JSON/Excel
- DataExporter: Class for batch exports

Example:
    >>> from lsst_extendedness.query import shortcuts
    >>>
    >>> # Get recent data
    >>> df = shortcuts.recent(days=7)
    >>> print(f"Found {len(df)} alerts in last 7 days")
    >>>
    >>> # Get minimoon candidates
    >>> candidates = shortcuts.minimoon_candidates()
    >>> print(f"Found {len(candidates)} candidates")

Export Example:
    >>> from lsst_extendedness.query.export import DataExporter
    >>> from lsst_extendedness.storage import SQLiteStorage
    >>>
    >>> storage = SQLiteStorage("data/lsst_extendedness.db")
    >>> exporter = DataExporter(storage)
    >>> exporter.today()
    >>> exporter.minimoon_candidates()
"""

from lsst_extendedness.query import shortcuts
from lsst_extendedness.query.export import (
    DataExporter,
    export_dataframe,
    export_minimoon_candidates,
    export_query,
    export_recent,
    export_today,
)

__all__ = [
    "DataExporter",
    "export_dataframe",
    "export_minimoon_candidates",
    "export_query",
    "export_recent",
    "export_today",
    "shortcuts",
]
