"""
Tests for post-processing framework.
"""

from __future__ import annotations

import pytest
import pandas as pd

from lsst_extendedness.processing.base import (
    BaseProcessor,
    FilteringProcessor,
    AggregatingProcessor,
    ProcessorConfig,
)
from lsst_extendedness.processing.registry import (
    register_processor,
    get_processor,
    list_processors,
    is_processor_registered,
    unregister_processor,
    clear_registry,
)
from lsst_extendedness.models.alerts import ProcessingResult


class TestProcessorRegistry:
    """Tests for processor registry."""

    def setup_method(self):
        """Clear registry before each test."""
        clear_registry()

    def test_register_processor(self):
        """Test registering a processor."""

        @register_processor("test_proc")
        class TestProcessor(BaseProcessor):
            name = "test_proc"
            version = "1.0.0"

            def process(self, df):
                return ProcessingResult(
                    processor_name=self.name,
                    processor_version=self.version,
                    records=[],
                )

        assert is_processor_registered("test_proc")
        assert get_processor("test_proc") is TestProcessor

    def test_list_processors(self):
        """Test listing registered processors."""

        @register_processor("proc1")
        class Proc1(BaseProcessor):
            name = "proc1"
            version = "1.0.0"

            def process(self, df):
                return ProcessingResult(
                    processor_name=self.name,
                    processor_version=self.version,
                    records=[],
                )

        @register_processor("proc2")
        class Proc2(BaseProcessor):
            name = "proc2"
            version = "2.0.0"

            def process(self, df):
                return ProcessingResult(
                    processor_name=self.name,
                    processor_version=self.version,
                    records=[],
                )

        processors = list_processors()
        assert "proc1" in processors
        assert "proc2" in processors

    def test_unregister_processor(self):
        """Test unregistering a processor."""

        @register_processor("temp_proc")
        class TempProcessor(BaseProcessor):
            name = "temp_proc"
            version = "1.0.0"

            def process(self, df):
                return ProcessingResult(
                    processor_name=self.name,
                    processor_version=self.version,
                    records=[],
                )

        assert is_processor_registered("temp_proc")

        result = unregister_processor("temp_proc")
        assert result is True
        assert not is_processor_registered("temp_proc")

    def test_unregister_nonexistent(self):
        """Test unregistering non-existent processor."""
        result = unregister_processor("nonexistent")
        assert result is False

    def test_get_nonexistent_processor(self):
        """Test getting non-existent processor."""
        assert get_processor("nonexistent") is None


class TestProcessorConfig:
    """Tests for ProcessorConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ProcessorConfig()

        assert config.window_days == 15
        assert config.batch_size == 10000
        assert config.min_alerts == 1
        assert config.extra_params == {}

    def test_custom_config(self):
        """Test custom configuration."""
        config = ProcessorConfig(
            window_days=30,
            batch_size=5000,
            min_alerts=10,
            extra_params={"threshold": 0.5},
        )

        assert config.window_days == 30
        assert config.batch_size == 5000
        assert config.min_alerts == 10
        assert config.extra_params["threshold"] == 0.5


class TestFilteringProcessor:
    """Tests for FilteringProcessor base class."""

    def test_filtering_processor(self):
        """Test FilteringProcessor subclass."""

        class HighSNRFilter(FilteringProcessor):
            name = "high_snr_filter"
            version = "1.0.0"

            def filter_condition(self, row):
                return row.get("snr", 0) >= 50

        # Create test data
        df = pd.DataFrame([
            {"id": 1, "snr": 100},
            {"id": 2, "snr": 30},
            {"id": 3, "snr": 75},
            {"id": 4, "snr": 10},
        ])

        # Process (without storage)
        processor = HighSNRFilter.__new__(HighSNRFilter)
        processor.name = "high_snr_filter"
        processor.version = "1.0.0"

        result = processor.process(df)

        assert len(result.records) == 2
        assert result.records[0]["id"] == 1
        assert result.records[1]["id"] == 3


class TestAggregatingProcessor:
    """Tests for AggregatingProcessor base class."""

    def test_aggregating_processor(self):
        """Test AggregatingProcessor subclass."""

        class ObjectSummary(AggregatingProcessor):
            name = "object_summary"
            version = "1.0.0"
            group_by = "object_id"

            def aggregate(self, group_df):
                if len(group_df) < 2:
                    return None
                return {
                    "count": len(group_df),
                    "mean_snr": float(group_df["snr"].mean()),
                }

        # Create test data
        df = pd.DataFrame([
            {"object_id": 1, "snr": 100},
            {"object_id": 1, "snr": 80},
            {"object_id": 1, "snr": 90},
            {"object_id": 2, "snr": 50},  # Only one, should be skipped
            {"object_id": 3, "snr": 60},
            {"object_id": 3, "snr": 70},
        ])

        # Process (without storage)
        processor = ObjectSummary.__new__(ObjectSummary)
        processor.name = "object_summary"
        processor.version = "1.0.0"
        processor.group_by = "object_id"

        result = processor.process(df)

        # Object 2 should be skipped (only 1 detection)
        assert len(result.records) == 2

        # Find object 1 result
        obj1 = next(r for r in result.records if r["object_id"] == 1)
        assert obj1["count"] == 3
        assert obj1["mean_snr"] == 90.0  # (100 + 80 + 90) / 3


class TestProcessingResult:
    """Tests for ProcessingResult model."""

    def test_processing_result_creation(self):
        """Test creating a ProcessingResult."""
        result = ProcessingResult(
            processor_name="test",
            processor_version="1.0.0",
            records=[{"id": 1}, {"id": 2}],
            summary="Found 2 items",
        )

        assert result.processor_name == "test"
        assert result.processor_version == "1.0.0"
        assert len(result.records) == 2
        assert result.summary == "Found 2 items"

    def test_processing_result_metadata(self):
        """Test ProcessingResult with metadata."""
        result = ProcessingResult(
            processor_name="test",
            processor_version="1.0.0",
            records=[],
            metadata={"input_rows": 1000, "filtered": 50},
        )

        assert result.metadata["input_rows"] == 1000
        assert result.metadata["filtered"] == 50

    def test_processing_result_empty(self):
        """Test empty ProcessingResult."""
        result = ProcessingResult(
            processor_name="test",
            processor_version="1.0.0",
            records=[],
        )

        assert len(result.records) == 0
        assert result.summary == ""
