"""
Tests for ingestion pipeline.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lsst_extendedness.ingest.pipeline import (
    IngestionPipeline,
    PipelineOptions,
    PipelineStats,
    run_ingestion,
)
from lsst_extendedness.ingest.state import StateTracker, SourceState
from lsst_extendedness.ingest.deserializer import (
    deserialize_avro,
    extract_schema_id,
    AlertDeserializer,
)
from lsst_extendedness.sources.mock import MockSource
from lsst_extendedness.storage.sqlite import SQLiteStorage


class TestPipelineStats:
    """Tests for PipelineStats."""

    def test_empty_stats(self):
        """Test empty stats."""
        stats = PipelineStats()

        assert stats.alerts_received == 0
        assert stats.alerts_stored == 0
        assert stats.success_rate == 0.0
        assert stats.alerts_per_second == 0.0

    def test_success_rate(self):
        """Test success rate calculation."""
        stats = PipelineStats(
            alerts_received=100,
            alerts_stored=95,
            alerts_failed=5,
        )

        assert stats.success_rate == 95.0

    def test_alerts_per_second(self):
        """Test processing rate calculation."""
        stats = PipelineStats(
            alerts_stored=1000,
            elapsed_seconds=10.0,
        )

        assert stats.alerts_per_second == 100.0


class TestPipelineOptions:
    """Tests for PipelineOptions."""

    def test_default_options(self):
        """Test default options."""
        options = PipelineOptions()

        assert options.batch_size == 500
        assert options.max_alerts is None
        assert options.store_cutouts is True
        assert options.skip_duplicates is True
        assert options.track_state is True
        assert options.dry_run is False

    def test_custom_options(self):
        """Test custom options."""
        options = PipelineOptions(
            batch_size=100,
            max_alerts=1000,
            dry_run=True,
        )

        assert options.batch_size == 100
        assert options.max_alerts == 1000
        assert options.dry_run is True


class TestIngestionPipeline:
    """Tests for IngestionPipeline."""

    def test_pipeline_with_mock_source(self, temp_db):
        """Test pipeline with mock source."""
        source = MockSource(count=50, seed=42)

        options = PipelineOptions(
            batch_size=10,
            max_alerts=50,
        )

        pipeline = IngestionPipeline(source, temp_db, options)

        with pipeline:
            stats = pipeline.run()

        assert stats.alerts_received == 50
        assert stats.alerts_stored == 50
        assert stats.alerts_failed == 0
        assert stats.batches_written == 5  # 50 / 10

    def test_pipeline_dry_run(self, temp_db):
        """Test pipeline in dry run mode."""
        source = MockSource(count=20)

        options = PipelineOptions(dry_run=True)

        pipeline = IngestionPipeline(source, temp_db, options)

        with pipeline:
            stats = pipeline.run()

        # Should report success but not write
        assert stats.alerts_received == 20
        assert stats.alerts_stored == 20

        # Verify nothing written
        db_stats = temp_db.get_stats()
        assert db_stats.get("alerts_raw_count", 0) == 0

    def test_pipeline_max_alerts(self, temp_db):
        """Test pipeline with max_alerts limit."""
        source = MockSource(count=100)

        options = PipelineOptions(max_alerts=25)

        pipeline = IngestionPipeline(source, temp_db, options)

        with pipeline:
            stats = pipeline.run()

        assert stats.alerts_received == 25
        assert stats.alerts_stored == 25

    def test_run_ingestion_convenience(self, temp_db):
        """Test run_ingestion convenience function."""
        source = MockSource(count=30)

        stats = run_ingestion(
            source,
            temp_db,
            batch_size=10,
            max_alerts=30,
        )

        assert stats.alerts_stored == 30


class TestStateTracker:
    """Tests for StateTracker."""

    def test_source_state_tracking(self, temp_db):
        """Test tracking source state."""
        tracker = StateTracker(temp_db)

        # Initially no state
        state = tracker.get_source_state(12345)
        assert state is None

        # Update state
        tracker.update_source_state(
            dia_source_id=12345,
            mjd=60000.0,
            alert_id=1001,
            has_ss_source=True,
        )

        # Retrieve state
        state = tracker.get_source_state(12345)
        assert state is not None
        assert state.dia_source_id == 12345
        assert state.first_seen_mjd == 60000.0
        assert state.has_ss_source is True

    def test_kafka_offset_tracking(self, temp_db):
        """Test Kafka offset tracking."""
        tracker = StateTracker(temp_db)

        # Initially no offset
        offset = tracker.get_kafka_offset("test-topic", 0)
        assert offset is None

        # Save offset
        tracker.save_kafka_offset("test-topic", 0, 1000)

        # Retrieve offset
        offset = tracker.get_kafka_offset("test-topic", 0)
        assert offset == 1000

        # Update offset
        tracker.save_kafka_offset("test-topic", 0, 2000)
        offset = tracker.get_kafka_offset("test-topic", 0)
        assert offset == 2000

    def test_mark_source_processed(self, temp_db):
        """Test marking source as processed."""
        tracker = StateTracker(temp_db)

        # Create source state
        tracker.update_source_state(
            dia_source_id=99999,
            mjd=60000.0,
            alert_id=1,
        )

        # Initially not processed
        assert not tracker.is_source_processed(99999)

        # Mark as processed
        tracker.mark_source_processed(99999)

        # Now should be processed
        assert tracker.is_source_processed(99999)

    def test_get_unprocessed_sources(self, temp_db):
        """Test getting unprocessed sources."""
        tracker = StateTracker(temp_db)

        # Add some sources
        for i in range(5):
            tracker.update_source_state(
                dia_source_id=i,
                mjd=60000.0 + i,
                alert_id=i * 100,
            )

        # Mark some as processed
        tracker.mark_source_processed(0)
        tracker.mark_source_processed(2)

        # Get unprocessed
        unprocessed = tracker.get_unprocessed_sources()

        assert len(unprocessed) == 3
        assert 0 not in unprocessed
        assert 2 not in unprocessed


class TestDeserializer:
    """Tests for AVRO deserialization."""

    def test_extract_schema_id_confluent(self):
        """Test extracting schema ID from Confluent format."""
        # Confluent wire format: [0x00] [4-byte schema ID] [data]
        data = b"\x00\x00\x00\x00\x42rest_of_data"

        schema_id = extract_schema_id(data)
        assert schema_id == 66  # 0x42

    def test_extract_schema_id_non_confluent(self):
        """Test with non-Confluent format."""
        data = b"SIMPLE  = T"  # Regular FITS-like data

        schema_id = extract_schema_id(data)
        assert schema_id is None

    def test_alert_deserializer_batch(self):
        """Test batch deserialization."""
        deserializer = AlertDeserializer()

        # With no schema, batch should return empty (can't deserialize)
        # This tests the error handling path
        messages = [b"invalid1", b"invalid2"]
        alerts = deserializer.deserialize_batch(messages)

        # Should handle errors gracefully
        assert alerts == []


# Fixtures

@pytest.fixture
def temp_db(tmp_path):
    """Create temporary database for testing."""
    db_path = tmp_path / "test.db"
    storage = SQLiteStorage(db_path)
    storage.initialize()
    yield storage
    storage.close()
