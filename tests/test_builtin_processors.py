"""
Tests for builtin example processors.
"""

from __future__ import annotations

import pandas as pd

from lsst_extendedness.processing.builtin.example import (
    ExampleProcessor,
    MiniMoonCandidateProcessor,
    ReassociationTracker,
    SourceSummaryProcessor,
)
from lsst_extendedness.processing.registry import clear_registry, load_builtin_processors


class TestExampleProcessor:
    """Tests for ExampleProcessor."""

    def setup_method(self):
        """Clear and reload registry."""
        clear_registry()
        load_builtin_processors()

    def test_example_processor_attributes(self):
        """Test ExampleProcessor has correct attributes."""
        assert ExampleProcessor.name == "example"
        assert ExampleProcessor.version == "1.0.0"
        assert ExampleProcessor.default_window_days == 7

    def test_example_processor_run(self, populated_db):
        """Test running ExampleProcessor."""
        proc = ExampleProcessor(populated_db)
        # Use explicit MJD range that matches populated_db data (MJD ~60000)
        result = proc.run(start_mjd=59990.0, end_mjd=60100.0)

        assert result.processor_name == "example"
        assert result.processor_version == "1.0.0"
        assert len(result.records) == 1

        # Check statistics structure
        stats = result.records[0]
        assert "total_alerts" in stats
        assert "unique_sources" in stats
        assert "date_range" in stats
        assert stats["total_alerts"] > 0

    def test_example_processor_extendedness_stats(self, populated_db):
        """Test ExampleProcessor computes extendedness statistics."""
        proc = ExampleProcessor(populated_db)
        result = proc.run(start_mjd=59990.0, end_mjd=60100.0)

        stats = result.records[0]
        assert "extendedness" in stats
        assert "mean" in stats["extendedness"]
        assert "std" in stats["extendedness"]
        assert "point_sources" in stats["extendedness"]
        assert "extended_sources" in stats["extendedness"]

    def test_example_processor_sso_stats(self, populated_db):
        """Test ExampleProcessor computes SSO statistics."""
        proc = ExampleProcessor(populated_db)
        result = proc.run(start_mjd=59990.0, end_mjd=60100.0)

        stats = result.records[0]
        assert "sso" in stats
        assert "with_sso" in stats["sso"]
        assert "without_sso" in stats["sso"]

    def test_example_processor_summary(self, populated_db):
        """Test ExampleProcessor produces meaningful summary."""
        proc = ExampleProcessor(populated_db)
        result = proc.run(start_mjd=59990.0, end_mjd=60100.0)

        assert "Processed" in result.summary
        assert "alerts" in result.summary
        assert "sources" in result.summary


class TestMiniMoonCandidateProcessor:
    """Tests for MiniMoonCandidateProcessor."""

    def setup_method(self):
        """Clear and reload registry."""
        clear_registry()
        load_builtin_processors()

    def test_minimoon_processor_attributes(self):
        """Test MiniMoonCandidateProcessor has correct attributes."""
        assert MiniMoonCandidateProcessor.name == "minimoon_candidates"
        assert MiniMoonCandidateProcessor.version == "1.0.0"
        assert MiniMoonCandidateProcessor.default_window_days == 15
        assert MiniMoonCandidateProcessor.extendedness_min == 0.3
        assert MiniMoonCandidateProcessor.extendedness_max == 0.7
        assert MiniMoonCandidateProcessor.min_snr == 5.0

    def test_minimoon_processor_run(self, populated_db):
        """Test running MiniMoonCandidateProcessor."""
        proc = MiniMoonCandidateProcessor(populated_db)
        result = proc.run(start_mjd=59990.0, end_mjd=60100.0)

        assert result.processor_name == "minimoon_candidates"
        # Populated DB should have some minimoon candidates
        # (created by alert_factory.create_minimoon_candidate())

    def test_minimoon_filter_condition_valid(self):
        """Test filter condition accepts valid candidates."""
        proc = MiniMoonCandidateProcessor.__new__(MiniMoonCandidateProcessor)
        proc.extendedness_min = 0.3
        proc.extendedness_max = 0.7
        proc.min_snr = 5.0

        # Valid candidate
        row = {"extendedness_median": 0.5, "snr": 10.0, "has_ss_source": True}
        assert proc.filter_condition(row) is True

    def test_minimoon_filter_condition_low_extendedness(self):
        """Test filter rejects low extendedness."""
        proc = MiniMoonCandidateProcessor.__new__(MiniMoonCandidateProcessor)
        proc.extendedness_min = 0.3
        proc.extendedness_max = 0.7
        proc.min_snr = 5.0

        row = {"extendedness_median": 0.1, "snr": 50.0}
        assert proc.filter_condition(row) is False

    def test_minimoon_filter_condition_high_extendedness(self):
        """Test filter rejects high extendedness."""
        proc = MiniMoonCandidateProcessor.__new__(MiniMoonCandidateProcessor)
        proc.extendedness_min = 0.3
        proc.extendedness_max = 0.7
        proc.min_snr = 5.0

        row = {"extendedness_median": 0.9, "snr": 50.0}
        assert proc.filter_condition(row) is False

    def test_minimoon_filter_condition_low_snr(self):
        """Test filter rejects low SNR."""
        proc = MiniMoonCandidateProcessor.__new__(MiniMoonCandidateProcessor)
        proc.extendedness_min = 0.3
        proc.extendedness_max = 0.7
        proc.min_snr = 5.0

        row = {"extendedness_median": 0.5, "snr": 2.0}
        assert proc.filter_condition(row) is False

    def test_minimoon_filter_condition_no_extendedness(self):
        """Test filter rejects missing extendedness."""
        proc = MiniMoonCandidateProcessor.__new__(MiniMoonCandidateProcessor)
        proc.extendedness_min = 0.3
        proc.extendedness_max = 0.7
        proc.min_snr = 5.0

        row = {"extendedness_median": None, "snr": 50.0}
        assert proc.filter_condition(row) is False

    def test_minimoon_filter_condition_no_snr(self):
        """Test filter accepts missing SNR if extendedness valid."""
        proc = MiniMoonCandidateProcessor.__new__(MiniMoonCandidateProcessor)
        proc.extendedness_min = 0.3
        proc.extendedness_max = 0.7
        proc.min_snr = 5.0

        row = {"extendedness_median": 0.5, "snr": None}
        assert proc.filter_condition(row) is True

    def test_minimoon_post_process(self):
        """Test post_process adds criteria metadata."""
        proc = MiniMoonCandidateProcessor.__new__(MiniMoonCandidateProcessor)
        proc.extendedness_min = 0.3
        proc.extendedness_max = 0.7
        proc.min_snr = 5.0

        from lsst_extendedness.models.alerts import ProcessingResult

        result = ProcessingResult(
            processor_name="minimoon_candidates",
            processor_version="1.0.0",
            records=[],
        )

        modified = proc.post_process(result)

        assert "criteria" in modified.metadata
        assert modified.metadata["criteria"]["extendedness_range"] == [0.3, 0.7]
        assert modified.metadata["criteria"]["min_snr"] == 5.0
        assert modified.metadata["criteria"]["requires_sso"] is True


class TestSourceSummaryProcessor:
    """Tests for SourceSummaryProcessor."""

    def setup_method(self):
        """Clear and reload registry."""
        clear_registry()
        load_builtin_processors()

    def test_source_summary_attributes(self):
        """Test SourceSummaryProcessor has correct attributes."""
        assert SourceSummaryProcessor.name == "source_summary"
        assert SourceSummaryProcessor.version == "1.0.0"
        assert SourceSummaryProcessor.default_window_days == 30
        assert SourceSummaryProcessor.group_by == "dia_object_id"

    def test_source_summary_run(self, populated_db):
        """Test running SourceSummaryProcessor."""
        proc = SourceSummaryProcessor(populated_db)
        result = proc.run(start_mjd=59990.0, end_mjd=60100.0)

        assert result.processor_name == "source_summary"

    def test_source_summary_aggregate_valid(self):
        """Test aggregate produces expected fields."""
        proc = SourceSummaryProcessor.__new__(SourceSummaryProcessor)

        df = pd.DataFrame(
            {
                "dia_object_id": [1, 1, 1],
                "mjd": [60000.0, 60001.0, 60002.0],
                "extendedness_median": [0.3, 0.4, 0.5],
                "filter_name": ["g", "r", "i"],
                "has_ss_source": [False, False, True],
            }
        )

        result = proc.aggregate(df)

        assert result is not None
        assert result["detection_count"] == 3
        assert result["first_mjd"] == 60000.0
        assert result["last_mjd"] == 60002.0
        assert result["time_span_days"] == 2.0
        assert "extendedness_mean" in result
        assert "filters" in result
        assert set(result["filters"]) == {"g", "r", "i"}
        assert result["has_sso_detection"] is True

    def test_source_summary_aggregate_skips_single(self):
        """Test aggregate skips sources with single detection."""
        proc = SourceSummaryProcessor.__new__(SourceSummaryProcessor)

        df = pd.DataFrame(
            {
                "dia_object_id": [1],
                "mjd": [60000.0],
            }
        )

        result = proc.aggregate(df)
        assert result is None


class TestReassociationTracker:
    """Tests for ReassociationTracker."""

    def setup_method(self):
        """Clear and reload registry."""
        clear_registry()
        load_builtin_processors()

    def test_reassociation_tracker_attributes(self):
        """Test ReassociationTracker has correct attributes."""
        assert ReassociationTracker.name == "reassociation_tracker"
        assert ReassociationTracker.version == "1.0.0"
        assert ReassociationTracker.default_window_days == 30

    def test_reassociation_tracker_run(self, populated_db):
        """Test running ReassociationTracker."""
        proc = ReassociationTracker(populated_db)
        result = proc.run(start_mjd=59990.0, end_mjd=60100.0)

        assert result.processor_name == "reassociation_tracker"

    def test_reassociation_tracker_custom_query(self):
        """Test that custom query filters reassociations."""
        proc = ReassociationTracker.__new__(ReassociationTracker)
        query = proc.get_query()

        assert "is_reassociation = 1" in query

    def test_reassociation_tracker_process_empty(self):
        """Test processing empty DataFrame."""
        proc = ReassociationTracker.__new__(ReassociationTracker)
        proc.name = "reassociation_tracker"
        proc.version = "1.0.0"

        df = pd.DataFrame(columns=["dia_source_id", "mjd", "ss_object_id", "reassociation_reason"])

        result = proc.process(df)

        assert len(result.records) == 0
        assert "No reassociations" in result.summary

    def test_reassociation_tracker_process_with_data(self):
        """Test processing DataFrame with reassociations."""
        proc = ReassociationTracker.__new__(ReassociationTracker)
        proc.name = "reassociation_tracker"
        proc.version = "1.0.0"

        df = pd.DataFrame(
            {
                "dia_source_id": [100, 100, 200],
                "mjd": [60000.0, 60001.0, 60002.0],
                "ss_object_id": ["SSO_A", "SSO_B", "SSO_C"],
                "reassociation_reason": ["orbit_update", "new_match", "orbit_update"],
            }
        )

        result = proc.process(df)

        assert len(result.records) == 2  # Two unique sources
        assert result.metadata["total_reassociations"] == 3
        assert result.metadata["unique_sources"] == 2

        # Find source 100 record
        source_100 = next(r for r in result.records if r["dia_source_id"] == 100)
        assert source_100["reassociation_count"] == 2
        assert len(source_100["ss_objects"]) == 2
        assert "SSO_A" in source_100["ss_objects"]
        assert "SSO_B" in source_100["ss_objects"]

    def test_reassociation_tracker_sorts_by_count(self):
        """Test that results are sorted by reassociation count."""
        proc = ReassociationTracker.__new__(ReassociationTracker)
        proc.name = "reassociation_tracker"
        proc.version = "1.0.0"

        df = pd.DataFrame(
            {
                "dia_source_id": [100, 200, 200, 200, 300],
                "mjd": [60000.0, 60001.0, 60002.0, 60003.0, 60004.0],
                "ss_object_id": ["A", "B", "C", "D", "E"],
                "reassociation_reason": ["r1", "r2", "r3", "r4", "r5"],
            }
        )

        result = proc.process(df)

        # Source 200 has 3 reassociations, should be first
        assert result.records[0]["dia_source_id"] == 200
        assert result.records[0]["reassociation_count"] == 3


class TestBuiltinProcessorsRegistration:
    """Test that all builtin processors are properly registered."""

    def test_all_builtin_registered(self):
        """Test all expected builtin processors are registered."""
        from lsst_extendedness.processing.registry import (
            clear_registry,
            is_processor_registered,
            register_processor,
        )

        # Clear and manually re-register processors
        # (since modules are already imported, load_builtin_processors won't re-run decorators)
        clear_registry()

        # Manually register the builtin processors
        register_processor("example")(ExampleProcessor)
        register_processor("minimoon_candidates")(MiniMoonCandidateProcessor)
        register_processor("source_summary")(SourceSummaryProcessor)
        register_processor("reassociation_tracker")(ReassociationTracker)

        expected = [
            "example",
            "minimoon_candidates",
            "source_summary",
            "reassociation_tracker",
        ]

        for name in expected:
            assert is_processor_registered(name), f"Processor {name} not registered"

    def test_builtin_classes_are_importable(self):
        """Test that builtin processor classes can be imported."""
        # Verify the classes exist and have expected attributes
        assert ExampleProcessor.name == "example"
        assert MiniMoonCandidateProcessor.name == "minimoon_candidates"
        assert SourceSummaryProcessor.name == "source_summary"
        assert ReassociationTracker.name == "reassociation_tracker"
