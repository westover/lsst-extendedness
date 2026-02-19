"""
Tests for query export module.
"""

from __future__ import annotations

import pandas as pd
import pytest

from lsst_extendedness.query.export import (
    DataExporter,
    export_dataframe,
    export_minimoon_candidates,
    export_processing_results,
    export_query,
    export_recent,
    export_sso_summary,
)

# Check for optional dependencies
try:
    import pyarrow

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    import openpyxl

    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


class TestExportDataframe:
    """Tests for export_dataframe function."""

    def test_export_csv(self, tmp_path):
        """Test exporting DataFrame to CSV."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        output_path = tmp_path / "output.csv"

        result = export_dataframe(df, output_path, format="csv")

        assert result == output_path
        assert output_path.exists()

        # Verify content
        loaded = pd.read_csv(output_path)
        assert len(loaded) == 3
        assert list(loaded.columns) == ["a", "b"]

    def test_export_json(self, tmp_path):
        """Test exporting DataFrame to JSON."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        output_path = tmp_path / "output.json"

        result = export_dataframe(df, output_path, format="json")

        assert result == output_path
        assert output_path.exists()

        # Verify content
        loaded = pd.read_json(output_path)
        assert len(loaded) == 2

    @pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
    def test_export_parquet(self, tmp_path):
        """Test exporting DataFrame to Parquet."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        output_path = tmp_path / "output.parquet"

        result = export_dataframe(df, output_path, format="parquet")

        assert result == output_path
        assert output_path.exists()

        # Verify content
        loaded = pd.read_parquet(output_path)
        assert len(loaded) == 3

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl not installed")
    def test_export_excel(self, tmp_path):
        """Test exporting DataFrame to Excel."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        output_path = tmp_path / "output.xlsx"

        result = export_dataframe(df, output_path, format="excel")

        assert result == output_path
        assert output_path.exists()

    def test_export_invalid_format(self, tmp_path):
        """Test exporting with invalid format raises error."""
        df = pd.DataFrame({"a": [1]})
        output_path = tmp_path / "output.txt"

        with pytest.raises(ValueError, match="Unsupported format"):
            export_dataframe(df, output_path, format="txt")  # type: ignore

    def test_export_creates_parent_dirs(self, tmp_path):
        """Test that export creates parent directories."""
        df = pd.DataFrame({"a": [1]})
        output_path = tmp_path / "nested" / "dir" / "output.csv"

        result = export_dataframe(df, output_path, format="csv")

        assert result == output_path
        assert output_path.exists()
        assert output_path.parent.exists()

    def test_export_empty_dataframe(self, tmp_path):
        """Test exporting empty DataFrame."""
        df = pd.DataFrame(columns=["a", "b"])
        output_path = tmp_path / "empty.csv"

        result = export_dataframe(df, output_path, format="csv")

        assert result == output_path
        assert output_path.exists()

        loaded = pd.read_csv(output_path)
        assert len(loaded) == 0


class TestExportQuery:
    """Tests for export_query function."""

    def test_export_query_csv(self, populated_db, tmp_path):
        """Test exporting query results to CSV."""
        output_path = tmp_path / "alerts.csv"

        result = export_query(
            populated_db,
            "SELECT dia_source_id, mjd, snr FROM alerts_raw LIMIT 10",
            output_path,
            format="csv",
        )

        assert result == output_path
        assert output_path.exists()

        loaded = pd.read_csv(output_path)
        assert len(loaded) <= 10
        assert "dia_source_id" in loaded.columns

    def test_export_query_with_params(self, populated_db, tmp_path):
        """Test exporting query with parameters."""
        output_path = tmp_path / "high_snr.csv"

        result = export_query(
            populated_db,
            "SELECT * FROM alerts_raw WHERE snr > ?",
            output_path,
            params=(50.0,),
            format="csv",
        )

        assert result == output_path
        assert output_path.exists()

    def test_export_query_empty_result(self, populated_db, tmp_path):
        """Test exporting query with no results."""
        output_path = tmp_path / "empty.csv"

        result = export_query(
            populated_db,
            "SELECT * FROM alerts_raw WHERE snr > 999999",
            output_path,
            format="csv",
        )

        assert result == output_path
        assert output_path.exists()


class TestExportRecent:
    """Tests for export_recent function."""

    def test_export_recent_default(self, populated_db, tmp_path):
        """Test exporting recent alerts with defaults."""
        result = export_recent(populated_db, output_dir=tmp_path)

        assert result.exists()
        assert "last7d" in result.name

    def test_export_recent_custom_days(self, populated_db, tmp_path):
        """Test exporting recent alerts with custom days."""
        result = export_recent(populated_db, days=14, output_dir=tmp_path)

        assert result.exists()
        assert "last14d" in result.name

    @pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
    def test_export_recent_parquet(self, populated_db, tmp_path):
        """Test exporting recent alerts as Parquet."""
        result = export_recent(
            populated_db,
            days=7,
            output_dir=tmp_path,
            format="parquet",
        )

        assert result.exists()
        assert result.suffix == ".parquet"


class TestExportMinimoonCandidates:
    """Tests for export_minimoon_candidates function."""

    def test_export_minimoon_candidates(self, populated_db, tmp_path):
        """Test exporting minimoon candidates."""
        result = export_minimoon_candidates(
            populated_db,
            output_dir=tmp_path,
        )

        assert result.exists()
        assert "minimoon_candidates" in result.name

    def test_export_minimoon_candidates_json(self, populated_db, tmp_path):
        """Test exporting minimoon candidates as JSON."""
        result = export_minimoon_candidates(
            populated_db,
            output_dir=tmp_path,
            format="json",
        )

        assert result.exists()
        assert result.suffix == ".json"


class TestExportProcessingResults:
    """Tests for export_processing_results function."""

    def test_export_processing_results_all(self, populated_db, tmp_path):
        """Test exporting all processing results."""
        result = export_processing_results(
            populated_db,
            output_dir=tmp_path,
        )

        assert result.exists()
        assert "results_all" in result.name

    def test_export_processing_results_filtered(self, populated_db, tmp_path):
        """Test exporting filtered processing results."""
        result = export_processing_results(
            populated_db,
            processor_name="example",
            output_dir=tmp_path,
        )

        assert result.exists()
        assert "results_example" in result.name


class TestExportSSOSummary:
    """Tests for export_sso_summary function."""

    def test_export_sso_summary(self, populated_db, tmp_path):
        """Test exporting SSO summary."""
        result = export_sso_summary(
            populated_db,
            output_dir=tmp_path,
        )

        assert result.exists()
        assert "sso_summary" in result.name

    @pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
    def test_export_sso_summary_parquet(self, populated_db, tmp_path):
        """Test exporting SSO summary as Parquet."""
        result = export_sso_summary(
            populated_db,
            output_dir=tmp_path,
            format="parquet",
        )

        assert result.exists()
        assert result.suffix == ".parquet"


class TestDataExporter:
    """Tests for DataExporter class."""

    def test_exporter_initialization(self, populated_db, tmp_path):
        """Test DataExporter initialization."""
        exporter = DataExporter(
            populated_db,
            output_dir=tmp_path,
            default_format="csv",
        )

        assert exporter.storage is populated_db
        assert exporter.output_dir == tmp_path
        assert exporter.default_format == "csv"

    def test_exporter_recent(self, populated_db, tmp_path):
        """Test DataExporter.recent()."""
        exporter = DataExporter(populated_db, output_dir=tmp_path)
        result = exporter.recent(days=7)

        assert result.exists()

    def test_exporter_recent_custom_format(self, populated_db, tmp_path):
        """Test DataExporter.recent() with custom format."""
        exporter = DataExporter(populated_db, output_dir=tmp_path)
        result = exporter.recent(days=7, format="json")

        assert result.exists()
        assert result.suffix == ".json"

    def test_exporter_minimoon_candidates(self, populated_db, tmp_path):
        """Test DataExporter.minimoon_candidates()."""
        exporter = DataExporter(populated_db, output_dir=tmp_path)
        result = exporter.minimoon_candidates()

        assert result.exists()

    def test_exporter_processing_results(self, populated_db, tmp_path):
        """Test DataExporter.processing_results()."""
        exporter = DataExporter(populated_db, output_dir=tmp_path)
        result = exporter.processing_results()

        assert result.exists()

    def test_exporter_processing_results_filtered(self, populated_db, tmp_path):
        """Test DataExporter.processing_results() with filter."""
        exporter = DataExporter(populated_db, output_dir=tmp_path)
        result = exporter.processing_results(processor_name="example")

        assert result.exists()

    def test_exporter_sso_summary(self, populated_db, tmp_path):
        """Test DataExporter.sso_summary()."""
        exporter = DataExporter(populated_db, output_dir=tmp_path)
        result = exporter.sso_summary()

        assert result.exists()

    def test_exporter_custom(self, populated_db, tmp_path):
        """Test DataExporter.custom()."""
        exporter = DataExporter(populated_db, output_dir=tmp_path)
        result = exporter.custom(
            "SELECT COUNT(*) as cnt FROM alerts_raw",
            "count.csv",
        )

        assert result.exists()
        assert result.name == "count.csv"

    def test_exporter_custom_with_params(self, populated_db, tmp_path):
        """Test DataExporter.custom() with parameters."""
        exporter = DataExporter(populated_db, output_dir=tmp_path)
        result = exporter.custom(
            "SELECT * FROM alerts_raw WHERE snr > ?",
            "high_snr.csv",
            params=(50.0,),
        )

        assert result.exists()

    @pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
    def test_exporter_uses_default_format(self, populated_db, tmp_path):
        """Test that exporter uses default format."""
        exporter = DataExporter(
            populated_db,
            output_dir=tmp_path,
            default_format="parquet",
        )
        result = exporter.recent()

        assert result.suffix == ".parquet"
