"""
Storage protocol for the LSST Extendedness Pipeline.

This module defines the interface that all storage backends must implement,
enabling flexible storage to different databases.

Currently implemented:
- SQLiteStorage: Local SQLite database (default, recommended)

Future implementations:
- PostgresStorage: Production PostgreSQL
- ParquetStorage: Columnar file storage for analytics

Example:
    >>> from lsst_extendedness.storage import SQLiteStorage
    >>>
    >>> storage = SQLiteStorage("data/alerts.db")
    >>> storage.initialize()
    >>>
    >>> # Write alerts
    >>> count = storage.write_batch(alerts)
    >>>
    >>> # Query
    >>> results = storage.query("SELECT * FROM alerts_raw WHERE mjd > ?", (60000.0,))
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from lsst_extendedness.models import AlertRecord, IngestionRun, ProcessingResult


@runtime_checkable
class AlertStorage(Protocol):
    """Interface for alert storage backends.

    Implement this protocol to support different storage backends:
    - SQLiteStorage: Local SQLite database (default)
    - PostgresStorage: Production PostgreSQL (future)
    - ParquetStorage: Columnar file storage (analytics)

    All implementations must support:
    - initialize(): Set up schema/tables
    - write_batch(): Bulk insert alerts
    - write_processing_result(): Store processor output
    - query(): Execute SQL queries
    - close(): Clean up resources

    Example:
        >>> class MyStorage:
        ...     def initialize(self) -> None:
        ...         # Create tables
        ...         pass
        ...
        ...     def write_batch(self, alerts: list[AlertRecord]) -> int:
        ...         # Insert alerts
        ...         return len(alerts)
        ...
        ...     def query(self, sql: str, params=()) -> list[dict]:
        ...         # Execute query
        ...         return []
        ...
        ...     def close(self) -> None:
        ...         # Cleanup
        ...         pass
    """

    def initialize(self) -> None:
        """Create tables/schema if needed.

        This method should be idempotent - safe to call multiple times.
        It should:
        - Create all required tables
        - Create indexes
        - Set up views
        - Apply any pending migrations

        Raises:
            RuntimeError: If schema cannot be created
        """
        ...

    def write_batch(self, alerts: Sequence[AlertRecord]) -> int:
        """Write a batch of alerts to storage.

        This is the primary write method for alert data.
        It should:
        - Insert all alerts in a single transaction
        - Handle duplicates appropriately (ignore/update/error)
        - Return the number of successfully written records

        Args:
            alerts: Sequence of AlertRecord instances to write

        Returns:
            Number of alerts successfully written

        Raises:
            ValueError: If alerts are invalid
            RuntimeError: If write fails

        Example:
            >>> alerts = [AlertRecord(...), AlertRecord(...)]
            >>> count = storage.write_batch(alerts)
            >>> print(f"Wrote {count} alerts")
        """
        ...

    def write_processing_result(self, result: ProcessingResult) -> int:
        """Write a processing result to storage.

        Stores the output of a post-processor for later retrieval.

        Args:
            result: ProcessingResult to store

        Returns:
            ID of the inserted result

        Raises:
            RuntimeError: If write fails
        """
        ...

    def write_ingestion_run(self, run: IngestionRun) -> int:
        """Write an ingestion run record.

        Creates or updates an ingestion run record for audit trail.

        Args:
            run: IngestionRun to store

        Returns:
            ID of the inserted/updated run

        Raises:
            RuntimeError: If write fails
        """
        ...

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as dictionaries.

        This is the primary read method for flexible queries.

        Args:
            sql: SQL query string (use ? for parameters)
            params: Query parameters

        Returns:
            List of dictionaries (column name -> value)

        Raises:
            ValueError: If SQL is invalid
            RuntimeError: If query fails

        Example:
            >>> results = storage.query(
            ...     "SELECT * FROM alerts_raw WHERE mjd > ? LIMIT ?",
            ...     (60000.0, 100)
            ... )
            >>> for row in results:
            ...     print(row["alert_id"])
        """
        ...

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        """Execute a non-query SQL statement.

        Use for INSERT, UPDATE, DELETE operations not covered
        by other methods.

        Args:
            sql: SQL statement
            params: Statement parameters

        Returns:
            Number of affected rows

        Raises:
            RuntimeError: If execution fails
        """
        ...

    def get_alert_count(self) -> int:
        """Get total number of alerts in storage.

        Returns:
            Total alert count
        """
        ...

    def get_processed_source(self, dia_source_id: int) -> dict[str, Any] | None:
        """Get tracking info for a previously processed source.

        Used for reassociation detection.

        Args:
            dia_source_id: DIASource ID to look up

        Returns:
            Dictionary with tracking info, or None if not found
        """
        ...

    def update_processed_source(
        self,
        dia_source_id: int,
        last_seen_mjd: float,
        ss_object_id: str | None,
        reassoc_time: float | None,
    ) -> None:
        """Update tracking info for a processed source.

        Args:
            dia_source_id: DIASource ID
            last_seen_mjd: Most recent observation MJD
            ss_object_id: Current SSObject ID (or None)
            reassoc_time: Current reassociation time (or None)
        """
        ...

    def close(self) -> None:
        """Clean up resources.

        This method should:
        - Close database connections
        - Commit pending transactions
        - Release any locks

        Should be safe to call multiple times.
        """
        ...

    # =========================================================================
    # Optional Methods (have default implementations in SQLiteStorage)
    # =========================================================================

    def get_stats(self) -> dict[str, Any]:
        """Get storage statistics.

        Returns:
            Dictionary with stats (alert count, table sizes, etc.)
        """
        ...

    def vacuum(self) -> None:
        """Optimize storage (compact database)."""
        ...

    def backup(self, path: str) -> None:
        """Create a backup of the storage.

        Args:
            path: Path to backup file
        """
        ...
