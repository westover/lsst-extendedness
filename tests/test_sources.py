"""
Tests for alert sources.

Tests the source implementations:
- MockSource: Synthetic data generation
- FileSource: File-based import (basic tests)
- Protocol compliance
"""

import pytest

from lsst_extendedness.models import AlertRecord
from lsst_extendedness.sources import MockSource, AlertSource
from lsst_extendedness.sources.protocol import (
    register_source,
    get_source,
    list_sources,
    is_source_registered,
)


class TestMockSource:
    """Tests for MockSource."""

    def test_create_default(self):
        """Test creating with defaults."""
        source = MockSource()

        assert source.count == 100
        assert source.source_name == "mock"

    def test_create_with_params(self):
        """Test creating with custom parameters."""
        source = MockSource(
            count=500,
            seed=42,
            sso_probability=0.5,
        )

        assert source.count == 500
        assert source.seed == 42
        assert source.sso_probability == 0.5

    def test_connect(self):
        """Test connecting source."""
        source = MockSource()

        source.connect()

        assert source._connected is True

    def test_fetch_requires_connect(self):
        """Test that fetch_alerts requires connection."""
        source = MockSource()

        with pytest.raises(RuntimeError):
            list(source.fetch_alerts())

    def test_fetch_alerts_count(self, mock_source):
        """Test that correct number of alerts is generated."""
        alerts = list(mock_source.fetch_alerts())

        assert len(alerts) == 100

    def test_fetch_alerts_limit(self, mock_source):
        """Test limiting number of alerts."""
        alerts = list(mock_source.fetch_alerts(limit=10))

        assert len(alerts) == 10

    def test_fetch_alerts_types(self, mock_source):
        """Test that all alerts are AlertRecord instances."""
        alerts = list(mock_source.fetch_alerts(limit=10))

        for alert in alerts:
            assert isinstance(alert, AlertRecord)

    def test_fetch_alerts_unique_ids(self, mock_source):
        """Test that alert IDs are unique."""
        alerts = list(mock_source.fetch_alerts())

        ids = [a.alert_id for a in alerts]
        assert len(ids) == len(set(ids))

    def test_fetch_alerts_valid_coordinates(self, mock_source):
        """Test that coordinates are valid."""
        alerts = list(mock_source.fetch_alerts())

        for alert in alerts:
            assert 0 <= alert.ra <= 360
            assert -90 <= alert.dec <= 90

    def test_fetch_alerts_valid_extendedness(self, mock_source):
        """Test that extendedness values are valid."""
        alerts = list(mock_source.fetch_alerts())

        for alert in alerts:
            if alert.extendedness_median is not None:
                assert 0 <= alert.extendedness_median <= 1

    def test_fetch_alerts_sso_distribution(self, mock_source):
        """Test that SSO alerts are generated."""
        alerts = list(mock_source.fetch_alerts())

        sso_count = sum(1 for a in alerts if a.has_ss_source)

        # With default 30% probability, should have some SSO alerts
        assert sso_count > 0
        assert sso_count < len(alerts)

    def test_reproducibility_with_seed(self):
        """Test that same seed produces same alerts."""
        source1 = MockSource(count=10, seed=42)
        source1.connect()
        alerts1 = list(source1.fetch_alerts())

        source2 = MockSource(count=10, seed=42)
        source2.connect()
        alerts2 = list(source2.fetch_alerts())

        for a1, a2 in zip(alerts1, alerts2):
            assert a1.alert_id == a2.alert_id
            assert a1.ra == a2.ra
            assert a1.dec == a2.dec

    def test_close(self, mock_source):
        """Test closing source."""
        mock_source.close()

        assert mock_source._connected is False

    def test_repr(self):
        """Test string representation."""
        source = MockSource(count=50, seed=123)

        assert "50" in repr(source)
        assert "123" in repr(source)


class TestSourceProtocol:
    """Tests for AlertSource protocol compliance."""

    def test_mock_source_is_protocol(self):
        """Test that MockSource implements AlertSource protocol."""
        source = MockSource()

        assert isinstance(source, AlertSource)

    def test_protocol_requires_source_name(self):
        """Test that source_name is required."""
        source = MockSource()

        assert hasattr(source, "source_name")
        assert source.source_name == "mock"

    def test_protocol_requires_connect(self):
        """Test that connect() is required."""
        source = MockSource()

        assert hasattr(source, "connect")
        assert callable(source.connect)

    def test_protocol_requires_fetch_alerts(self):
        """Test that fetch_alerts() is required."""
        source = MockSource()

        assert hasattr(source, "fetch_alerts")
        assert callable(source.fetch_alerts)

    def test_protocol_requires_close(self):
        """Test that close() is required."""
        source = MockSource()

        assert hasattr(source, "close")
        assert callable(source.close)


class TestSourceRegistry:
    """Tests for source registry functions."""

    def test_mock_source_registered(self):
        """Test that MockSource is registered."""
        assert is_source_registered("mock")

    def test_kafka_source_registered(self):
        """Test that KafkaSource is registered."""
        assert is_source_registered("kafka")

    def test_file_source_registered(self):
        """Test that FileSource is registered."""
        assert is_source_registered("file")

    def test_list_sources(self):
        """Test listing registered sources."""
        sources = list_sources()

        assert "mock" in sources
        assert "kafka" in sources
        assert "file" in sources

    def test_get_source(self):
        """Test getting source by name."""
        source = get_source("mock", count=50)

        assert isinstance(source, MockSource)
        assert source.count == 50

    def test_get_source_not_found(self):
        """Test getting non-existent source."""
        with pytest.raises(KeyError):
            get_source("nonexistent")

    def test_register_custom_source(self):
        """Test registering a custom source."""

        @register_source("custom_test_source")
        class CustomSource:
            source_name = "custom_test"

            def connect(self):
                pass

            def fetch_alerts(self, limit=None):
                return iter([])

            def close(self):
                pass

        assert is_source_registered("custom_test_source")

        source = get_source("custom_test_source")
        assert source.source_name == "custom_test"


class TestMockSourcePerformance:
    """Performance tests for MockSource."""

    @pytest.mark.slow
    def test_large_batch_generation(self, mock_source_large):
        """Test generating large number of alerts."""
        alerts = list(mock_source_large.fetch_alerts())

        assert len(alerts) == 1000

    @pytest.mark.slow
    def test_memory_efficiency(self):
        """Test that alerts are generated lazily."""
        source = MockSource(count=10000, seed=42)
        source.connect()

        # Should not consume much memory
        count = 0
        for alert in source.fetch_alerts(limit=100):
            count += 1
            if count >= 100:
                break

        assert count == 100
        # Generator should not have generated all 10000

        source.close()
