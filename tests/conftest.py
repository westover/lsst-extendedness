"""
Pytest configuration and shared fixtures for the LSST Extendedness Pipeline.

This module provides:
- Alert factories for generating test data
- Mock sources for testing without infrastructure
- Temporary database fixtures
- Sample AVRO records

Example usage in tests:
    def test_something(alert_factory, temp_db):
        alerts = alert_factory.create_batch(10)
        temp_db.write_batch(alerts)
        assert temp_db.get_alert_count() == 10
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from lsst_extendedness.models import AlertRecord
from lsst_extendedness.sources import MockSource
from lsst_extendedness.storage import SQLiteStorage

# Import fixtures from fixtures module
from tests.fixtures.factories import AlertFactory
from tests.fixtures.avro_samples import SAMPLE_AVRO_RECORD, SAMPLE_AVRO_NO_SSO


# ============================================================================
# FACTORY FIXTURES
# ============================================================================


@pytest.fixture
def alert_factory() -> AlertFactory:
    """Provide a fresh AlertFactory with counter reset.

    Returns:
        AlertFactory instance with counter at 0
    """
    AlertFactory.reset()
    return AlertFactory


# ============================================================================
# MOCK SOURCE FIXTURES
# ============================================================================


@pytest.fixture
def mock_source(alert_factory: AlertFactory) -> Iterator[MockSource]:
    """Provide a connected mock source with 100 sample alerts.

    Yields:
        Connected MockSource instance
    """
    source = MockSource(count=100, seed=42)
    source.connect()
    yield source
    source.close()


@pytest.fixture
def mock_source_large() -> Iterator[MockSource]:
    """Provide a mock source with 1000 alerts for performance tests.

    Yields:
        Connected MockSource instance
    """
    source = MockSource(count=1000, seed=42)
    source.connect()
    yield source
    source.close()


# ============================================================================
# DATABASE FIXTURES
# ============================================================================


@pytest.fixture
def temp_db() -> Iterator[SQLiteStorage]:
    """Provide a temporary SQLite database.

    Creates a fresh database in a temp directory, initializes schema,
    and cleans up after test.

    Yields:
        Initialized SQLiteStorage instance
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test.db"
        storage = SQLiteStorage(db_path)
        storage.initialize()
        yield storage
        storage.close()


@pytest.fixture
def populated_db(temp_db: SQLiteStorage, alert_factory: AlertFactory) -> SQLiteStorage:
    """Provide a database pre-populated with sample data.

    Contains:
    - 50 alerts with mixed characteristics
    - Point sources, extended sources, SSO alerts
    - Some reassociations

    Args:
        temp_db: Empty database fixture
        alert_factory: Alert factory fixture

    Returns:
        Populated SQLiteStorage instance
    """
    # Create mixed alerts
    alerts = []

    # Regular alerts
    alerts.extend(alert_factory.create_batch(20))

    # Point sources
    for _ in range(10):
        alerts.append(alert_factory.create_point_source())

    # Extended sources
    for _ in range(10):
        alerts.append(alert_factory.create_extended_source())

    # SSO alerts
    for _ in range(5):
        alerts.append(alert_factory.create_minimoon_candidate())

    # Reassociations
    for _ in range(5):
        alert = alert_factory.create_minimoon_candidate()
        alert = AlertRecord(
            **{**alert.model_dump(), "is_reassociation": True, "reassociation_reason": "new_association"}
        )
        alerts.append(alert)

    temp_db.write_batch(alerts)
    return temp_db


# ============================================================================
# AVRO SAMPLE FIXTURES
# ============================================================================


@pytest.fixture
def sample_avro_record() -> dict:
    """Provide a sample AVRO record as deserialized from Kafka.

    Returns:
        Dictionary mimicking deserialized AVRO alert
    """
    return SAMPLE_AVRO_RECORD.copy()


@pytest.fixture
def sample_avro_no_sso() -> dict:
    """Provide a sample AVRO record without SSObject.

    Returns:
        Dictionary mimicking deserialized AVRO alert (no SSO)
    """
    return SAMPLE_AVRO_NO_SSO.copy()


# ============================================================================
# CONFIGURATION FIXTURES
# ============================================================================


@pytest.fixture
def temp_config_dir() -> Iterator[Path]:
    """Provide a temporary configuration directory.

    Yields:
        Path to temporary config directory
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        config_dir = Path(temp_dir) / "config"
        config_dir.mkdir()
        yield config_dir


# ============================================================================
# PYTEST CONFIGURATION
# ============================================================================


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "slow: mark test as slow")
    config.addinivalue_line("markers", "integration: mark as integration test")
    config.addinivalue_line("markers", "unit: mark as unit test")
