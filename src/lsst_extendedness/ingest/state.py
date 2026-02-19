"""
Ingestion State Tracking.

Tracks processed sources, Kafka offsets, and ingestion run metadata
using the SQLite database as persistent storage.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..storage.sqlite import SQLiteStorage


@dataclass
class SourceState:
    """State of a processed DIA source."""

    dia_source_id: int
    first_seen_mjd: float
    last_seen_mjd: float
    alert_count: int
    last_alert_id: int
    has_ss_source: bool
    is_processed: bool


@dataclass
class KafkaState:
    """Kafka consumer state for resumption."""

    topic: str
    partition: int
    offset: int
    timestamp: datetime


class StateTracker:
    """Tracks ingestion state using SQLite storage.

    Replaces the old consumer_state.json approach with database-backed
    state management for better reliability and queryability.
    """

    def __init__(self, storage: SQLiteStorage):
        """Initialize state tracker.

        Args:
            storage: SQLiteStorage instance for persistence
        """
        self.storage = storage
        self._ensure_state_tables()

    def _ensure_state_tables(self) -> None:
        """Ensure Kafka state table exists."""
        self.storage.execute("""
            CREATE TABLE IF NOT EXISTS kafka_state (
                topic TEXT NOT NULL,
                partition INTEGER NOT NULL,
                offset_value INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (topic, partition)
            )
        """)

    def get_source_state(self, dia_source_id: int) -> SourceState | None:
        """Get state of a previously processed source.

        Args:
            dia_source_id: DIA source identifier

        Returns:
            SourceState if found, None otherwise
        """
        result = self.storage.get_processed_source(dia_source_id)
        if not result:
            return None

        return SourceState(
            dia_source_id=result["dia_source_id"],
            first_seen_mjd=result["first_seen_mjd"],
            last_seen_mjd=result["last_seen_mjd"],
            alert_count=result["alert_count"],
            last_alert_id=result["last_alert_id"],
            has_ss_source=bool(result["has_ss_source"]),
            is_processed=bool(result["is_processed"]),
        )

    def update_source_state(
        self,
        dia_source_id: int,
        mjd: float,
        alert_id: int,
        has_ss_source: bool = False,
    ) -> None:
        """Update state for a source after processing an alert.

        Args:
            dia_source_id: DIA source identifier
            mjd: Modified Julian Date of the alert
            alert_id: Alert identifier
            has_ss_source: Whether alert has SSSource association
        """
        self.storage.update_processed_source(
            dia_source_id=dia_source_id,
            mjd=mjd,
            alert_id=alert_id,
            has_ss_source=has_ss_source,
        )

    def is_source_processed(self, dia_source_id: int) -> bool:
        """Check if a source has been fully processed.

        Args:
            dia_source_id: DIA source identifier

        Returns:
            True if source is marked as processed
        """
        state = self.get_source_state(dia_source_id)
        return state.is_processed if state else False

    def mark_source_processed(self, dia_source_id: int) -> None:
        """Mark a source as fully processed.

        Args:
            dia_source_id: DIA source identifier
        """
        self.storage.execute(
            """
            UPDATE processed_sources
            SET is_processed = 1, processed_at = ?
            WHERE dia_source_id = ?
            """,
            (datetime.utcnow().isoformat(), dia_source_id),
        )

    def get_kafka_offset(self, topic: str, partition: int) -> int | None:
        """Get last committed Kafka offset.

        Args:
            topic: Kafka topic name
            partition: Partition number

        Returns:
            Last offset or None if not tracked
        """
        results = self.storage.query(
            """
            SELECT offset_value FROM kafka_state
            WHERE topic = ? AND partition = ?
            """,
            (topic, partition),
        )
        return results[0]["offset_value"] if results else None

    def save_kafka_offset(self, topic: str, partition: int, offset: int) -> None:
        """Save Kafka offset for resumption.

        Args:
            topic: Kafka topic name
            partition: Partition number
            offset: Current offset
        """
        self.storage.execute(
            """
            INSERT INTO kafka_state (topic, partition, offset_value, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (topic, partition)
            DO UPDATE SET offset_value = ?, updated_at = ?
            """,
            (
                topic,
                partition,
                offset,
                datetime.utcnow().isoformat(),
                offset,
                datetime.utcnow().isoformat(),
            ),
        )

    def get_all_kafka_state(self) -> list[KafkaState]:
        """Get all Kafka state entries.

        Returns:
            List of KafkaState objects
        """
        results = self.storage.query("SELECT * FROM kafka_state")
        return [
            KafkaState(
                topic=r["topic"],
                partition=r["partition"],
                offset=r["offset_value"],
                timestamp=datetime.fromisoformat(r["updated_at"]),
            )
            for r in results
        ]

    def get_unprocessed_sources(self, limit: int = 1000) -> list[int]:
        """Get DIA source IDs that haven't been post-processed.

        Args:
            limit: Maximum number of IDs to return

        Returns:
            List of dia_source_id values
        """
        results = self.storage.query(
            """
            SELECT dia_source_id FROM processed_sources
            WHERE is_processed = 0
            ORDER BY last_seen_mjd DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [r["dia_source_id"] for r in results]

    def get_sources_in_window(
        self, start_mjd: float, end_mjd: float
    ) -> list[SourceState]:
        """Get all sources with alerts in a time window.

        Args:
            start_mjd: Start of window (MJD)
            end_mjd: End of window (MJD)

        Returns:
            List of SourceState objects
        """
        results = self.storage.query(
            """
            SELECT * FROM processed_sources
            WHERE last_seen_mjd >= ? AND first_seen_mjd <= ?
            ORDER BY last_seen_mjd DESC
            """,
            (start_mjd, end_mjd),
        )
        return [
            SourceState(
                dia_source_id=r["dia_source_id"],
                first_seen_mjd=r["first_seen_mjd"],
                last_seen_mjd=r["last_seen_mjd"],
                alert_count=r["alert_count"],
                last_alert_id=r["last_alert_id"],
                has_ss_source=bool(r["has_ss_source"]),
                is_processed=bool(r["is_processed"]),
            )
            for r in results
        ]

    def reset_processing_flags(self) -> int:
        """Reset all processing flags to allow reprocessing.

        Returns:
            Number of sources reset
        """
        cursor = self.storage.execute(
            "UPDATE processed_sources SET is_processed = 0, processed_at = NULL"
        )
        return cursor.rowcount

    def cleanup_old_state(self, days: int = 90) -> int:
        """Remove state entries older than specified days.

        Args:
            days: Age threshold in days

        Returns:
            Number of entries removed
        """
        from ..utils.time import days_ago_mjd

        threshold_mjd = days_ago_mjd(days)
        cursor = self.storage.execute(
            "DELETE FROM processed_sources WHERE last_seen_mjd < ?",
            (threshold_mjd,),
        )
        return cursor.rowcount
