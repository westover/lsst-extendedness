"""
SQLite storage backend for the LSST Extendedness Pipeline.

This module provides a high-performance SQLite storage implementation
with features optimized for alert data:

- WAL mode for concurrent reads
- Batch inserts with transactions
- Connection pooling
- Query result caching (optional)
- Automatic schema management

Example:
    >>> from lsst_extendedness.storage import SQLiteStorage
    >>> from lsst_extendedness.models import AlertRecord
    >>>
    >>> storage = SQLiteStorage("data/alerts.db")
    >>> storage.initialize()
    >>>
    >>> # Write alerts
    >>> alerts = [AlertRecord(...) for _ in range(100)]
    >>> count = storage.write_batch(alerts)
    >>> print(f"Wrote {count} alerts")
    >>>
    >>> # Query
    >>> results = storage.query(
    ...     "SELECT * FROM alerts_raw WHERE mjd > ? LIMIT ?",
    ...     (60000.0, 10)
    ... )
    >>>
    >>> storage.close()
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from lsst_extendedness.models.alerts import AlertRecord, ProcessingResult
from lsst_extendedness.models.runs import IngestionRun
from lsst_extendedness.storage.schema import create_schema, get_schema_version, migrate


class SQLiteStorage:
    """SQLite storage backend for alert data.

    This class provides a complete storage implementation using SQLite,
    optimized for the LSST alert processing workflow.

    Features:
    - WAL mode for better concurrent access
    - Batch inserts with automatic transactions
    - Efficient upsert operations
    - State tracking for reassociation detection
    - Query result pagination

    Attributes:
        db_path: Path to the SQLite database file
        connection: Active database connection

    Example:
        >>> storage = SQLiteStorage("data/alerts.db")
        >>> storage.initialize()
        >>> count = storage.write_batch(alerts)
        >>> results = storage.query("SELECT * FROM v_recent_alerts LIMIT 10")
        >>> storage.close()
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        timeout: float = 30.0,
        check_same_thread: bool = False,
    ):
        """Initialize SQLite storage.

        Args:
            db_path: Path to SQLite database file
            timeout: Connection timeout in seconds
            check_same_thread: If True, check that connection is used in same thread
        """
        self.db_path = Path(db_path)
        self._timeout = timeout
        self._check_same_thread = check_same_thread
        self._connection: sqlite3.Connection | None = None

    @property
    def connection(self) -> sqlite3.Connection:
        """Get active database connection, creating if needed."""
        if self._connection is None:
            self._connect()
        return self._connection  # type: ignore

    def _connect(self) -> None:
        """Establish database connection."""
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._connection = sqlite3.connect(
            self.db_path,
            timeout=self._timeout,
            check_same_thread=self._check_same_thread,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )

        # Return rows as dictionaries
        self._connection.row_factory = sqlite3.Row

        # Enable WAL mode for better concurrency
        self._connection.execute("PRAGMA journal_mode=WAL")

        # Enable foreign keys
        self._connection.execute("PRAGMA foreign_keys=ON")

        # Optimize for performance
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.execute("PRAGMA cache_size=-64000")  # 64MB cache
        self._connection.execute("PRAGMA temp_store=MEMORY")

    def initialize(self) -> None:
        """Create tables/schema if needed.

        This method is idempotent - safe to call multiple times.
        """
        # Check if migration needed
        version = get_schema_version(self.connection)
        if version is None:
            create_schema(self.connection)
        else:
            migrate(self.connection)

    def write_batch(self, alerts: Sequence[AlertRecord]) -> int:
        """Write a batch of alerts to storage.

        Uses INSERT OR IGNORE to handle duplicates gracefully.

        Args:
            alerts: Sequence of AlertRecord instances

        Returns:
            Number of alerts successfully written
        """
        if not alerts:
            return 0

        cursor = self.connection.cursor()

        # Build INSERT statement with all columns
        columns = [
            "alert_id", "dia_source_id", "dia_object_id",
            "ra", "dec", "mjd", "ingested_at",
            "filter_name", "ps_flux", "ps_flux_err", "snr",
            "extendedness_median", "extendedness_min", "extendedness_max",
            "has_ss_source", "ss_object_id", "ss_object_reassoc_time_mjd",
            "is_reassociation", "reassociation_reason",
            "trail_data", "pixel_flags",
            "science_cutout_path", "template_cutout_path", "difference_cutout_path",
        ]

        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(columns)

        sql = f"INSERT OR IGNORE INTO alerts_raw ({column_names}) VALUES ({placeholders})"

        # Convert alerts to tuples
        rows = []
        for alert in alerts:
            db_dict = alert.to_db_dict()
            row = tuple(db_dict.get(col) for col in columns)
            rows.append(row)

        # Execute batch insert
        cursor.executemany(sql, rows)
        self.connection.commit()

        return cursor.rowcount

    def write_processing_result(self, result: ProcessingResult) -> int:
        """Write a processing result to storage.

        Args:
            result: ProcessingResult to store

        Returns:
            ID of the inserted result
        """
        cursor = self.connection.cursor()

        db_dict = result.to_db_dict()

        cursor.execute(
            """
            INSERT INTO processing_results
            (processor_name, processor_version, records, metadata, summary, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                db_dict["processor_name"],
                db_dict["processor_version"],
                db_dict["records"],
                db_dict["metadata"],
                db_dict["summary"],
                db_dict["processed_at"],
            ),
        )

        self.connection.commit()
        return cursor.lastrowid or 0

    def write_ingestion_run(self, run: IngestionRun) -> int:
        """Write or update an ingestion run record.

        Args:
            run: IngestionRun to store

        Returns:
            ID of the inserted/updated run
        """
        cursor = self.connection.cursor()

        db_dict = run.to_db_dict()

        if run.id is None:
            # Insert new run
            cursor.execute(
                """
                INSERT INTO ingestion_runs
                (source_name, source_config, started_at, completed_at,
                 alerts_ingested, alerts_failed, new_sources, reassociations_detected,
                 cutouts_saved, status, error_message, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    db_dict["source_name"],
                    db_dict["source_config"],
                    db_dict["started_at"],
                    db_dict["completed_at"],
                    db_dict["alerts_ingested"],
                    db_dict["alerts_failed"],
                    db_dict["new_sources"],
                    db_dict["reassociations_detected"],
                    db_dict["cutouts_saved"],
                    db_dict["status"],
                    db_dict["error_message"],
                    db_dict["metadata"],
                ),
            )
            run.id = cursor.lastrowid
        else:
            # Update existing run
            cursor.execute(
                """
                UPDATE ingestion_runs SET
                    completed_at = ?,
                    alerts_ingested = ?,
                    alerts_failed = ?,
                    new_sources = ?,
                    reassociations_detected = ?,
                    cutouts_saved = ?,
                    status = ?,
                    error_message = ?,
                    metadata = ?
                WHERE id = ?
                """,
                (
                    db_dict["completed_at"],
                    db_dict["alerts_ingested"],
                    db_dict["alerts_failed"],
                    db_dict["new_sources"],
                    db_dict["reassociations_detected"],
                    db_dict["cutouts_saved"],
                    db_dict["status"],
                    db_dict["error_message"],
                    db_dict["metadata"],
                    run.id,
                ),
            )

        self.connection.commit()
        return run.id or 0

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as dictionaries.

        Args:
            sql: SQL query string (use ? for parameters)
            params: Query parameters

        Returns:
            List of dictionaries (column name -> value)
        """
        cursor = self.connection.cursor()
        cursor.execute(sql, params)

        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        """Execute a non-query SQL statement.

        Args:
            sql: SQL statement
            params: Statement parameters

        Returns:
            Number of affected rows
        """
        cursor = self.connection.cursor()
        cursor.execute(sql, params)
        self.connection.commit()
        return cursor.rowcount

    def get_alert_count(self) -> int:
        """Get total number of alerts in storage."""
        result = self.query("SELECT COUNT(*) as count FROM alerts_raw")
        return result[0]["count"] if result else 0

    def get_processed_source(self, dia_source_id: int) -> dict[str, Any] | None:
        """Get tracking info for a previously processed source.

        Args:
            dia_source_id: DIASource ID to look up

        Returns:
            Dictionary with tracking info, or None if not found
        """
        results = self.query(
            "SELECT * FROM processed_sources WHERE dia_source_id = ?",
            (dia_source_id,),
        )
        return results[0] if results else None

    def update_processed_source(
        self,
        dia_source_id: int,
        last_seen_mjd: float,
        ss_object_id: str | None,
        reassoc_time: float | None,
    ) -> None:
        """Update tracking info for a processed source.

        Uses UPSERT to insert or update as needed.

        Args:
            dia_source_id: DIASource ID
            last_seen_mjd: Most recent observation MJD
            ss_object_id: Current SSObject ID (or None)
            reassoc_time: Current reassociation time (or None)
        """
        self.execute(
            """
            INSERT INTO processed_sources
            (dia_source_id, first_seen_mjd, last_seen_mjd, ss_object_id,
             ss_object_reassoc_time, observation_count, last_updated)
            VALUES (?, ?, ?, ?, ?, 1, datetime('now'))
            ON CONFLICT(dia_source_id) DO UPDATE SET
                last_seen_mjd = MAX(last_seen_mjd, excluded.last_seen_mjd),
                observation_count = observation_count + 1,
                ss_object_id = excluded.ss_object_id,
                ss_object_reassoc_time = excluded.ss_object_reassoc_time,
                last_updated = datetime('now')
            """,
            (dia_source_id, last_seen_mjd, last_seen_mjd, ss_object_id, reassoc_time),
        )

    def get_alerts_for_processing(
        self,
        window_days: int = 15,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get alerts for post-processing.

        Args:
            window_days: Number of days to look back
            limit: Maximum number of alerts to return

        Returns:
            List of alert dictionaries
        """
        sql = """
            SELECT * FROM alerts_raw
            WHERE mjd >= (SELECT MAX(mjd) FROM alerts_raw) - ?
            ORDER BY mjd DESC
        """
        params: tuple[Any, ...] = (window_days,)

        if limit:
            sql += " LIMIT ?"
            params = (window_days, limit)

        return self.query(sql, params)

    def get_stats(self) -> dict[str, Any]:
        """Get storage statistics.

        Returns:
            Dictionary with database statistics
        """
        stats: dict[str, Any] = {}

        # Table counts
        for table in ["alerts_raw", "alerts_filtered", "processed_sources",
                      "processing_results", "ingestion_runs"]:
            result = self.query(f"SELECT COUNT(*) as count FROM {table}")
            stats[f"{table}_count"] = result[0]["count"] if result else 0

        # Date range
        result = self.query(
            "SELECT MIN(mjd) as min_mjd, MAX(mjd) as max_mjd FROM alerts_raw"
        )
        if result and result[0]["min_mjd"]:
            stats["mjd_range"] = {
                "min": result[0]["min_mjd"],
                "max": result[0]["max_mjd"],
            }

        # Database file size
        if self.db_path.exists():
            stats["file_size_bytes"] = self.db_path.stat().st_size
            stats["file_size_mb"] = round(stats["file_size_bytes"] / (1024 * 1024), 2)

        # SSO statistics
        result = self.query(
            "SELECT COUNT(*) as count FROM alerts_raw WHERE has_ss_source = 1"
        )
        stats["sso_alerts"] = result[0]["count"] if result else 0

        # Reassociation statistics
        result = self.query(
            "SELECT COUNT(*) as count FROM alerts_raw WHERE is_reassociation = 1"
        )
        stats["reassociations"] = result[0]["count"] if result else 0

        return stats

    def vacuum(self) -> None:
        """Optimize storage by compacting database."""
        self.connection.execute("VACUUM")

    def backup(self, backup_path: str | Path) -> None:
        """Create a backup of the database.

        Args:
            backup_path: Path for the backup file
        """
        backup_path = Path(backup_path)
        backup_path.parent.mkdir(parents=True, exist_ok=True)

        # Use SQLite backup API for consistency
        backup_conn = sqlite3.connect(backup_path)
        with backup_conn:
            self.connection.backup(backup_conn)
        backup_conn.close()

    def close(self) -> None:
        """Close database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> "SQLiteStorage":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def __repr__(self) -> str:
        """String representation."""
        return f"SQLiteStorage({self.db_path!r})"
