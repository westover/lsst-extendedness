"""
Query utilities for the LSST Extendedness Pipeline.

This module provides scientist-friendly query shortcuts and export utilities:

Shortcuts (return pandas DataFrames):
- today(): Alerts from today
- recent(days=7): Alerts from last N days
- minimoon_candidates(): SSO with intermediate extendedness
- point_sources(): Low extendedness (star-like)
- extended_sources(): High extendedness (galaxy-like)
- by_sso_id(sso_id): All alerts for a specific SSObject
- unprocessed(): Alerts not yet post-processed

Export:
- to_csv(df, path): Export to CSV
- to_parquet(df, path): Export to Parquet (columnar)
- to_fits_table(df, path): Export to FITS table

Example:
    >>> from lsst_extendedness.query import shortcuts
    >>>
    >>> # Get recent data
    >>> df = shortcuts.recent(days=7)
    >>> print(f"Found {len(df)} alerts in last 7 days")
    >>>
    >>> # Filter and export
    >>> candidates = shortcuts.minimoon_candidates()
    >>> shortcuts.to_csv(candidates, "minimoon_candidates.csv")
"""

from lsst_extendedness.query import shortcuts
from lsst_extendedness.query.export import to_csv, to_parquet, to_fits_table

__all__ = [
    "shortcuts",
    "to_csv",
    "to_parquet",
    "to_fits_table",
]
