"""
Ingestion Pipeline Orchestrator.

Coordinates the flow of alerts from sources to storage,
handling batching, state tracking, and error recovery.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

import structlog

from ..models.alerts import AlertRecord
from ..models.runs import IngestionRun, RunStatus
from ..sources.protocol import AlertSource
from ..storage.sqlite import SQLiteStorage
from .state import StateTracker

if TYPE_CHECKING:
    from ..config.settings import PipelineConfig

logger = structlog.get_logger(__name__)


@dataclass
class PipelineStats:
    """Statistics from a pipeline run."""

    alerts_received: int = 0
    alerts_stored: int = 0
    alerts_failed: int = 0
    new_sources: int = 0
    updated_sources: int = 0
    reassociations: int = 0
    batches_written: int = 0
    elapsed_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        """Percentage of alerts successfully stored."""
        total = self.alerts_received
        return (self.alerts_stored / total * 100) if total > 0 else 0.0

    @property
    def alerts_per_second(self) -> float:
        """Processing rate."""
        if self.elapsed_seconds > 0:
            return self.alerts_stored / self.elapsed_seconds
        return 0.0


@dataclass
class PipelineOptions:
    """Options controlling pipeline behavior."""

    batch_size: int = 500
    max_alerts: int | None = None
    store_cutouts: bool = True
    cutout_dir: Path | None = None
    skip_duplicates: bool = True
    track_state: bool = True
    dry_run: bool = False


class IngestionPipeline:
    """Orchestrates alert ingestion from source to storage.

    Usage:
        source = KafkaSource(config)
        storage = SQLiteStorage(db_path)
        pipeline = IngestionPipeline(source, storage)

        with pipeline:
            stats = pipeline.run()
            print(f"Ingested {stats.alerts_stored} alerts")
    """

    def __init__(
        self,
        source: AlertSource,
        storage: SQLiteStorage,
        options: PipelineOptions | None = None,
    ):
        """Initialize pipeline.

        Args:
            source: Alert source (Kafka, File, Mock, etc.)
            storage: Storage backend
            options: Pipeline configuration options
        """
        self.source = source
        self.storage = storage
        self.options = options or PipelineOptions()
        self.state_tracker = StateTracker(storage) if self.options.track_state else None
        self._current_run: IngestionRun | None = None

    def __enter__(self) -> IngestionPipeline:
        """Context manager entry: connect source and initialize storage."""
        self.storage.initialize()
        self.source.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit: close source."""
        self.source.close()

    def run(self) -> PipelineStats:
        """Execute the ingestion pipeline.

        Returns:
            PipelineStats with run metrics
        """
        stats = PipelineStats()
        start_time = time.monotonic()

        # Create ingestion run record
        self._current_run = IngestionRun(
            started_at=datetime.utcnow(),
            source_name=self.source.source_name,
        )

        logger.info(
            "pipeline_started",
            source=self.source.source_name,
            batch_size=self.options.batch_size,
            max_alerts=self.options.max_alerts,
        )

        try:
            # Process alerts in batches
            batch: list[AlertRecord] = []

            for alert in self._fetch_alerts():
                stats.alerts_received += 1

                # Skip duplicates if enabled
                if self.options.skip_duplicates and self._is_duplicate(alert):
                    continue

                # Track reassociations
                if alert.is_reassociation:
                    stats.reassociations += 1

                batch.append(alert)

                # Write batch when full
                if len(batch) >= self.options.batch_size:
                    batch_stats = self._write_batch(batch)
                    stats.alerts_stored += batch_stats["stored"]
                    stats.alerts_failed += batch_stats["failed"]
                    stats.new_sources += batch_stats["new_sources"]
                    stats.updated_sources += batch_stats["updated_sources"]
                    stats.batches_written += 1
                    batch = []

                # Check max alerts limit
                if (
                    self.options.max_alerts
                    and stats.alerts_received >= self.options.max_alerts
                ):
                    break

            # Write remaining alerts
            if batch:
                batch_stats = self._write_batch(batch)
                stats.alerts_stored += batch_stats["stored"]
                stats.alerts_failed += batch_stats["failed"]
                stats.new_sources += batch_stats["new_sources"]
                stats.updated_sources += batch_stats["updated_sources"]
                stats.batches_written += 1

            # Complete the run
            stats.elapsed_seconds = time.monotonic() - start_time
            self._complete_run(stats)

            logger.info(
                "pipeline_completed",
                alerts_stored=stats.alerts_stored,
                alerts_failed=stats.alerts_failed,
                elapsed_seconds=round(stats.elapsed_seconds, 2),
                rate=round(stats.alerts_per_second, 1),
            )

        except Exception as e:
            stats.elapsed_seconds = time.monotonic() - start_time
            self._fail_run(str(e))
            logger.error("pipeline_failed", error=str(e))
            raise

        return stats

    def _fetch_alerts(self) -> Iterator[AlertRecord]:
        """Fetch alerts from source with limit."""
        yield from self.source.fetch_alerts(limit=self.options.max_alerts)

    def _is_duplicate(self, alert: AlertRecord) -> bool:
        """Check if alert has already been ingested."""
        if not self.state_tracker:
            return False

        # Check if we've seen this exact alert
        results = self.storage.query(
            "SELECT 1 FROM alerts_raw WHERE alert_id = ? LIMIT 1",
            (alert.alert_id,),
        )
        return len(results) > 0

    def _write_batch(self, alerts: list[AlertRecord]) -> dict[str, int]:
        """Write a batch of alerts to storage.

        Returns:
            Dict with stored, failed, new_sources, updated_sources counts
        """
        if self.options.dry_run:
            return {
                "stored": len(alerts),
                "failed": 0,
                "new_sources": 0,
                "updated_sources": 0,
            }

        stored = 0
        failed = 0
        new_sources = 0
        updated_sources = 0

        try:
            # Write to storage
            stored = self.storage.write_batch(alerts)

            # Update source state
            if self.state_tracker:
                for alert in alerts:
                    existing = self.state_tracker.get_source_state(alert.dia_source_id)
                    if existing:
                        updated_sources += 1
                    else:
                        new_sources += 1

                    self.state_tracker.update_source_state(
                        dia_source_id=alert.dia_source_id,
                        mjd=alert.mjd,
                        alert_id=alert.alert_id,
                        has_ss_source=alert.has_ss_source,
                    )

            logger.debug(
                "batch_written",
                count=stored,
                new_sources=new_sources,
                updated_sources=updated_sources,
            )

        except Exception as e:
            failed = len(alerts)
            logger.error("batch_write_failed", error=str(e), count=len(alerts))

        return {
            "stored": stored,
            "failed": failed,
            "new_sources": new_sources,
            "updated_sources": updated_sources,
        }

    def _complete_run(self, stats: PipelineStats) -> None:
        """Record successful completion of ingestion run."""
        if not self._current_run:
            return

        self._current_run.complete(
            alerts_ingested=stats.alerts_stored,
            alerts_failed=stats.alerts_failed,
            new_sources=stats.new_sources,
            reassociations=stats.reassociations,
        )

        if not self.options.dry_run:
            self.storage.write_ingestion_run(self._current_run)

    def _fail_run(self, error_message: str) -> None:
        """Record failed ingestion run."""
        if not self._current_run:
            return

        self._current_run.fail(error_message)

        if not self.options.dry_run:
            self.storage.write_ingestion_run(self._current_run)


def run_ingestion(
    source: AlertSource,
    storage: SQLiteStorage,
    *,
    batch_size: int = 500,
    max_alerts: int | None = None,
    dry_run: bool = False,
) -> PipelineStats:
    """Convenience function to run ingestion pipeline.

    Args:
        source: Alert source
        storage: Storage backend
        batch_size: Alerts per batch
        max_alerts: Maximum alerts to process
        dry_run: If True, don't write to storage

    Returns:
        PipelineStats with run metrics
    """
    options = PipelineOptions(
        batch_size=batch_size,
        max_alerts=max_alerts,
        dry_run=dry_run,
    )

    pipeline = IngestionPipeline(source, storage, options)

    with pipeline:
        return pipeline.run()
