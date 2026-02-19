"""
Example Processors.

These serve as templates for scientists to implement their own
post-processing pipelines. Copy and modify for your use case.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..base import AggregatingProcessor, BaseProcessor, FilteringProcessor
from ..registry import register_processor
from ...models.alerts import ProcessingResult

if TYPE_CHECKING:
    import pandas as pd


@register_processor("example")
class ExampleProcessor(BaseProcessor):
    """Simple example processor that computes basic statistics.

    This demonstrates the minimal implementation of a processor.
    Copy this as a starting point for your own analysis.
    """

    name = "example"
    version = "1.0.0"
    description = "Example processor computing basic alert statistics"
    default_window_days = 7

    def process(self, df: "pd.DataFrame") -> ProcessingResult:
        """Compute basic statistics on alerts.

        Args:
            df: DataFrame with alert data

        Returns:
            ProcessingResult with statistics
        """
        # Compute statistics
        stats = {
            "total_alerts": len(df),
            "unique_sources": df["dia_source_id"].nunique(),
            "unique_objects": df["dia_object_id"].nunique() if "dia_object_id" in df else 0,
            "date_range": {
                "min_mjd": float(df["mjd"].min()),
                "max_mjd": float(df["mjd"].max()),
            },
        }

        # Extendedness statistics
        if "extendedness_median" in df.columns:
            ext = df["extendedness_median"].dropna()
            stats["extendedness"] = {
                "mean": float(ext.mean()),
                "std": float(ext.std()),
                "point_sources": int((ext < 0.3).sum()),
                "extended_sources": int((ext > 0.7).sum()),
            }

        # SSO statistics
        if "has_ss_source" in df.columns:
            stats["sso"] = {
                "with_sso": int(df["has_ss_source"].sum()),
                "without_sso": int((~df["has_ss_source"]).sum()),
            }

        return ProcessingResult(
            processor_name=self.name,
            processor_version=self.version,
            records=[stats],
            summary=f"Processed {len(df)} alerts from {stats['unique_sources']} sources",
            metadata={"type": "statistics"},
        )


@register_processor("minimoon_candidates")
class MiniMoonCandidateProcessor(FilteringProcessor):
    """Identify potential minimoon candidates.

    Minimoons are temporarily captured asteroids that orbit Earth.
    They typically appear as point sources with SSObject associations
    and intermediate extendedness values.

    Selection criteria:
    - Has SSObject association (is a known solar system object)
    - Extendedness 0.3-0.7 (neither clearly point nor extended)
    - Multiple detections preferred
    """

    name = "minimoon_candidates"
    version = "1.0.0"
    description = "Identify potential minimoon candidates from SSO alerts"
    default_window_days = 15

    # Selection thresholds
    extendedness_min = 0.3
    extendedness_max = 0.7
    min_snr = 5.0

    def get_query(self) -> str:
        """Query SSO alerts only."""
        return """
            SELECT * FROM alerts_raw
            WHERE mjd >= ? AND mjd <= ?
            AND has_ss_source = 1
            ORDER BY mjd ASC
        """

    def filter_condition(self, row: dict[str, Any]) -> bool:
        """Check if alert matches minimoon criteria.

        Args:
            row: Alert record

        Returns:
            True if potential minimoon
        """
        ext = row.get("extendedness_median")
        snr = row.get("snr")

        # Must have extendedness measurement
        if ext is None:
            return False

        # Check extendedness range
        if not (self.extendedness_min <= ext <= self.extendedness_max):
            return False

        # Check SNR if available
        if snr is not None and snr < self.min_snr:
            return False

        return True

    def post_process(self, result: ProcessingResult) -> ProcessingResult:
        """Add minimoon-specific metadata."""
        result.metadata["criteria"] = {
            "extendedness_range": [self.extendedness_min, self.extendedness_max],
            "min_snr": self.min_snr,
            "requires_sso": True,
        }
        return result


@register_processor("source_summary")
class SourceSummaryProcessor(AggregatingProcessor):
    """Aggregate alerts by DIA object to create source summaries.

    Produces one record per DIA object with:
    - Detection count
    - Time span
    - Extendedness statistics
    - Filter coverage
    """

    name = "source_summary"
    version = "1.0.0"
    description = "Aggregate alerts into per-source summaries"
    default_window_days = 30
    group_by = "dia_object_id"

    def aggregate(self, group_df: "pd.DataFrame") -> dict[str, Any] | None:
        """Compute summary for one source.

        Args:
            group_df: All alerts for one DIA object

        Returns:
            Summary dict or None to skip
        """
        # Skip sources with too few detections
        if len(group_df) < 2:
            return None

        summary = {
            "detection_count": len(group_df),
            "first_mjd": float(group_df["mjd"].min()),
            "last_mjd": float(group_df["mjd"].max()),
            "time_span_days": float(group_df["mjd"].max() - group_df["mjd"].min()),
        }

        # Extendedness stats
        if "extendedness_median" in group_df.columns:
            ext = group_df["extendedness_median"].dropna()
            if len(ext) > 0:
                summary["extendedness_mean"] = float(ext.mean())
                summary["extendedness_std"] = float(ext.std())
                summary["extendedness_range"] = [float(ext.min()), float(ext.max())]

        # Filter coverage
        if "filter_name" in group_df.columns:
            summary["filters"] = list(group_df["filter_name"].dropna().unique())

        # SSO status
        if "has_ss_source" in group_df.columns:
            summary["has_sso_detection"] = bool(group_df["has_ss_source"].any())

        return summary


@register_processor("reassociation_tracker")
class ReassociationTracker(BaseProcessor):
    """Track SSObject reassociations over time.

    Identifies sources that have been reassociated to different
    solar system objects, which could indicate orbit refinement
    or misidentification.
    """

    name = "reassociation_tracker"
    version = "1.0.0"
    description = "Track SSObject reassociation events"
    default_window_days = 30

    def get_query(self) -> str:
        """Query reassociation events."""
        return """
            SELECT * FROM alerts_raw
            WHERE mjd >= ? AND mjd <= ?
            AND is_reassociation = 1
            ORDER BY mjd ASC
        """

    def process(self, df: "pd.DataFrame") -> ProcessingResult:
        """Analyze reassociation events.

        Args:
            df: DataFrame of reassociation events

        Returns:
            ProcessingResult with reassociation analysis
        """
        if len(df) == 0:
            return ProcessingResult(
                processor_name=self.name,
                processor_version=self.version,
                records=[],
                summary="No reassociations in window",
            )

        # Group by source
        records = []
        for source_id, group in df.groupby("dia_source_id"):
            record = {
                "dia_source_id": int(source_id),
                "reassociation_count": len(group),
                "first_reassoc_mjd": float(group["mjd"].min()),
                "last_reassoc_mjd": float(group["mjd"].max()),
            }

            # Track SSObject changes
            if "ss_object_id" in group.columns:
                ss_objects = group["ss_object_id"].dropna().unique().tolist()
                record["ss_objects"] = ss_objects
                record["ss_object_count"] = len(ss_objects)

            # Track reasons
            if "reassociation_reason" in group.columns:
                reasons = group["reassociation_reason"].dropna().unique().tolist()
                record["reasons"] = reasons

            records.append(record)

        # Sort by reassociation count
        records.sort(key=lambda r: r["reassociation_count"], reverse=True)

        return ProcessingResult(
            processor_name=self.name,
            processor_version=self.version,
            records=records,
            summary=f"Found {len(records)} sources with reassociations",
            metadata={
                "total_reassociations": len(df),
                "unique_sources": len(records),
            },
        )
