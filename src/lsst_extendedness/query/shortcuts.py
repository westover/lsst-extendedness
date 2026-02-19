"""
Query Shortcuts for Common Analyses.

Provides convenient functions for frequently used queries,
returning pandas DataFrames ready for analysis.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

    from ..storage.sqlite import SQLiteStorage


def _get_storage(storage: SQLiteStorage | None = None) -> SQLiteStorage:
    """Get storage instance, using default if not provided."""
    if storage is not None:
        return storage

    from ..storage.sqlite import SQLiteStorage

    default_path = Path("data/lsst_extendedness.db")
    if not default_path.exists():
        raise FileNotFoundError(
            f"Default database not found at {default_path}. "
            "Either pass a storage instance or run 'make db-init'."
        )

    return SQLiteStorage(default_path)


def _query_to_df(
    storage: SQLiteStorage,
    query: str,
    params: tuple = (),
) -> pd.DataFrame:
    """Execute query and return as DataFrame."""
    import pandas as pd

    rows = storage.query(query, params)
    return pd.DataFrame(rows)


def today(storage: SQLiteStorage | None = None) -> pd.DataFrame:
    """Get today's alerts.

    Args:
        storage: Optional storage instance

    Returns:
        DataFrame of today's alerts
    """
    storage = _get_storage(storage)
    return _query_to_df(storage, "SELECT * FROM v_today_alerts")


def recent(days: int = 7, storage: SQLiteStorage | None = None) -> pd.DataFrame:
    """Get recent alerts.

    Args:
        days: Number of days to look back
        storage: Optional storage instance

    Returns:
        DataFrame of recent alerts
    """
    from ..utils.time import days_ago_mjd

    storage = _get_storage(storage)
    threshold = days_ago_mjd(days)

    return _query_to_df(
        storage,
        "SELECT * FROM alerts_raw WHERE mjd >= ? ORDER BY mjd DESC",
        (threshold,),
    )


def point_sources(storage: SQLiteStorage | None = None) -> pd.DataFrame:
    """Get point-like sources (stars).

    Extendedness < 0.3

    Args:
        storage: Optional storage instance

    Returns:
        DataFrame of point sources
    """
    storage = _get_storage(storage)
    return _query_to_df(storage, "SELECT * FROM v_point_sources")


def extended_sources(storage: SQLiteStorage | None = None) -> pd.DataFrame:
    """Get extended sources (galaxies).

    Extendedness > 0.7

    Args:
        storage: Optional storage instance

    Returns:
        DataFrame of extended sources
    """
    storage = _get_storage(storage)
    return _query_to_df(storage, "SELECT * FROM v_extended_sources")


def minimoon_candidates(storage: SQLiteStorage | None = None) -> pd.DataFrame:
    """Get potential minimoon candidates.

    SSO sources with intermediate extendedness (0.3-0.7)

    Args:
        storage: Optional storage instance

    Returns:
        DataFrame of minimoon candidates
    """
    storage = _get_storage(storage)
    return _query_to_df(storage, "SELECT * FROM v_minimoon_candidates")


def sso_alerts(storage: SQLiteStorage | None = None) -> pd.DataFrame:
    """Get all alerts with SSObject associations.

    Args:
        storage: Optional storage instance

    Returns:
        DataFrame of SSO alerts
    """
    storage = _get_storage(storage)
    return _query_to_df(storage, "SELECT * FROM v_sso_alerts")


def reassociations(storage: SQLiteStorage | None = None) -> pd.DataFrame:
    """Get all reassociation events.

    Args:
        storage: Optional storage instance

    Returns:
        DataFrame of reassociations
    """
    storage = _get_storage(storage)
    return _query_to_df(storage, "SELECT * FROM v_reassociations")


def by_source(
    dia_source_id: int,
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Get all alerts for a specific DIA source.

    Args:
        dia_source_id: Source identifier
        storage: Optional storage instance

    Returns:
        DataFrame of alerts for this source
    """
    storage = _get_storage(storage)
    return _query_to_df(
        storage,
        "SELECT * FROM alerts_raw WHERE dia_source_id = ? ORDER BY mjd",
        (dia_source_id,),
    )


def by_object(
    dia_object_id: int,
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Get all alerts for a specific DIA object.

    Args:
        dia_object_id: Object identifier
        storage: Optional storage instance

    Returns:
        DataFrame of alerts for this object
    """
    storage = _get_storage(storage)
    return _query_to_df(
        storage,
        "SELECT * FROM alerts_raw WHERE dia_object_id = ? ORDER BY mjd",
        (dia_object_id,),
    )


def by_sso(
    ss_object_id: str,
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Get all alerts for a specific SSObject.

    Args:
        ss_object_id: Solar system object identifier
        storage: Optional storage instance

    Returns:
        DataFrame of alerts for this SSO
    """
    storage = _get_storage(storage)
    return _query_to_df(
        storage,
        "SELECT * FROM alerts_raw WHERE ss_object_id = ? ORDER BY mjd",
        (ss_object_id,),
    )


def in_region(
    ra_min: float,
    ra_max: float,
    dec_min: float,
    dec_max: float,
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Get alerts in a sky region (box query).

    Args:
        ra_min: Minimum RA (degrees)
        ra_max: Maximum RA (degrees)
        dec_min: Minimum Dec (degrees)
        dec_max: Maximum Dec (degrees)
        storage: Optional storage instance

    Returns:
        DataFrame of alerts in region
    """
    storage = _get_storage(storage)
    return _query_to_df(
        storage,
        """
        SELECT * FROM alerts_raw
        WHERE ra BETWEEN ? AND ?
        AND dec BETWEEN ? AND ?
        ORDER BY mjd DESC
        """,
        (ra_min, ra_max, dec_min, dec_max),
    )


def in_time_window(
    start_mjd: float,
    end_mjd: float,
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Get alerts in a time window.

    Args:
        start_mjd: Start MJD
        end_mjd: End MJD
        storage: Optional storage instance

    Returns:
        DataFrame of alerts in window
    """
    storage = _get_storage(storage)
    return _query_to_df(
        storage,
        "SELECT * FROM alerts_raw WHERE mjd BETWEEN ? AND ? ORDER BY mjd",
        (start_mjd, end_mjd),
    )


def with_filter(
    filter_name: str,
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Get alerts observed with a specific filter.

    Args:
        filter_name: Filter name (e.g., 'g', 'r', 'i')
        storage: Optional storage instance

    Returns:
        DataFrame of alerts with this filter
    """
    storage = _get_storage(storage)
    return _query_to_df(
        storage,
        "SELECT * FROM alerts_raw WHERE filter_name = ? ORDER BY mjd DESC",
        (filter_name,),
    )


def high_snr(
    min_snr: float = 50.0,
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Get high signal-to-noise alerts.

    Args:
        min_snr: Minimum SNR threshold
        storage: Optional storage instance

    Returns:
        DataFrame of high-SNR alerts
    """
    storage = _get_storage(storage)
    return _query_to_df(
        storage,
        "SELECT * FROM alerts_raw WHERE snr >= ? ORDER BY snr DESC",
        (min_snr,),
    )


def processing_results(
    processor_name: str | None = None,
    limit: int = 100,
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Get recent processing results.

    Args:
        processor_name: Optional filter by processor name
        limit: Maximum results
        storage: Optional storage instance

    Returns:
        DataFrame of processing results
    """
    storage = _get_storage(storage)

    if processor_name:
        return _query_to_df(
            storage,
            """
            SELECT * FROM processing_results
            WHERE processor_name = ?
            ORDER BY processed_at DESC LIMIT ?
            """,
            (processor_name, limit),
        )
    else:
        return _query_to_df(
            storage,
            "SELECT * FROM processing_results ORDER BY processed_at DESC LIMIT ?",
            (limit,),
        )


def stats(storage: SQLiteStorage | None = None) -> dict:
    """Get database statistics.

    Args:
        storage: Optional storage instance

    Returns:
        Dict with database statistics
    """
    storage = _get_storage(storage)
    return storage.get_stats()


def custom(
    sql: str,
    params: tuple = (),
    storage: SQLiteStorage | None = None,
) -> pd.DataFrame:
    """Execute custom SQL query.

    Args:
        sql: SQL query string
        params: Query parameters
        storage: Optional storage instance

    Returns:
        DataFrame with results
    """
    storage = _get_storage(storage)
    return _query_to_df(storage, sql, params)
