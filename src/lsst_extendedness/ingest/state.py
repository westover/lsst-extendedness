"""
Ingestion State Tracking.

Tracks processed sources, Kafka offsets, and ingestion run metadata
using the SQLite database as persistent storage.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..storage.sqlite import SQLiteStorage


@dataclass
class SourceState:
    """State of a processed DIA source."""

    dia_source_id: int
    first_seen_mjd: float
    last_seen_mjd: float
    observation_count: int
    ss_object_id: str | None = None
    ss_object_reassoc_time: float | None = None
    last_updated: str | None = None


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
        self.storage.execute(
            """
            CREATE TABLE IF NOT EXISTS kafka_state (
                topic TEXT NOT NULL,
                partition INTEGER NOT NULL,
                offset_value INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (topic, partition)
            )
        """
        )

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
            observation_count=result.get("observation_count", 1),
            ss_object_id=result.get("ss_object_id"),
            ss_object_reassoc_time=result.get("ss_object_reassoc_time"),
            last_updated=result.get("last_updated"),
        )

    def update_source_state(
        self,
        dia_source_id: int,
        mjd: float,
        _alert_id: int,
        _has_ss_source: bool = False,
        ss_object_id: str | None = None,
        reassoc_time: float | None = None,
    ) -> None:
        """Update state for a source after processing an alert.

        Args:
            dia_source_id: DIA source identifier
            mjd: Modified Julian Date of the alert
            _alert_id: Alert identifier (reserved for future use)
            _has_ss_source: Whether alert has SSSource association (reserved)
            ss_object_id: SSObject ID if associated
            reassoc_time: SSObject reassociation time if available
        """
        self.storage.update_processed_source(
            dia_source_id=dia_source_id,
            last_seen_mjd=mjd,
            ss_object_id=ss_object_id,
            reassoc_time=reassoc_time,
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

    def get_sources_in_window(self, start_mjd: float, end_mjd: float) -> list[SourceState]:
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
                observation_count=r.get("observation_count", 1),
                ss_object_id=r.get("ss_object_id"),
                ss_object_reassoc_time=r.get("ss_object_reassoc_time"),
                last_updated=r.get("last_updated"),
            )
            for r in results
        ]

    def cleanup_old_state(self, days: int = 90) -> int:
        """Remove state entries older than specified days.

        Args:
            days: Age threshold in days

        Returns:
            Number of entries removed
        """
        from ..utils.time import days_ago_mjd

        threshold_mjd = days_ago_mjd(days)
        return self.storage.execute(
            "DELETE FROM processed_sources WHERE last_seen_mjd < ?",
            (threshold_mjd,),
        )
