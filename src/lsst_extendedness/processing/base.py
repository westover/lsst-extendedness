"""
Base Processor Interface for Post-Processing.

Scientists implement BaseProcessor to create custom analysis
pipelines that run on accumulated alert data.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog

from ..models.alerts import ProcessingResult

if TYPE_CHECKING:
    import pandas as pd

    from ..storage.sqlite import SQLiteStorage

logger = structlog.get_logger(__name__)


@dataclass
class ProcessorConfig:
    """Configuration for a processor run."""

    window_days: int = 15
    batch_size: int = 10000
    min_alerts: int = 1
    extra_params: dict[str, Any] = field(default_factory=dict)


class BaseProcessor(ABC):
    """Base class for post-processing pipelines.

    Subclass this to implement custom analysis on accumulated alerts.
    The processing framework handles:
    - Data retrieval from SQLite
    - Batching for large datasets
    - Result storage and tracking
    - Error handling and logging

    Example:
        @register_processor("minimoon_detector")
        class MiniMoonDetector(BaseProcessor):
            name = "minimoon_detector"
            version = "1.0.0"
            description = "Detect minimoon candidates from SSO alerts"

            def process(self, df: pd.DataFrame) -> ProcessingResult:
                # Your analysis here
                candidates = self._analyze_orbits(df)
                return ProcessingResult(
                    processor_name=self.name,
                    processor_version=self.version,
                    records=candidates,
                    summary=f"Found {len(candidates)} candidates",
                )
    """

    # Override in subclass
    name: str = "base_processor"
    version: str = "0.0.0"
    description: str = "Base processor - do not use directly"
    default_window_days: int = 15

    def __init__(
        self,
        storage: SQLiteStorage,
        config: ProcessorConfig | None = None,
    ):
        """Initialize processor.

        Args:
            storage: SQLiteStorage for data access
            config: Processor configuration
        """
        self.storage = storage
        self.config = config or ProcessorConfig(window_days=self.default_window_days)
        self._setup()

    def _setup(self) -> None:
        """Optional setup hook for subclasses."""
        pass

    @abstractmethod
    def process(self, df: pd.DataFrame) -> ProcessingResult:
        """Process a dataframe of alerts.

        This is the main method to implement. The dataframe contains
        alerts from the configured time window.

        Args:
            df: DataFrame with alert data

        Returns:
            ProcessingResult with analysis output
        """
        raise NotImplementedError

    def get_query(self) -> str:
        """SQL query to fetch data for processing.

        Override to customize data retrieval. Default fetches
        all alerts in the configured time window.

        Returns:
            SQL query string
        """
        return """
            SELECT * FROM alerts_raw
            WHERE mjd >= ? AND mjd <= ?
            ORDER BY mjd ASC
        """

    def get_query_params(self, start_mjd: float, end_mjd: float) -> tuple[Any, ...]:
        """Parameters for the data query.

        Args:
            start_mjd: Window start (MJD)
            end_mjd: Window end (MJD)

        Returns:
            Tuple of query parameters
        """
        return (start_mjd, end_mjd)

    def pre_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-processing hook before main processing.

        Override to add data cleaning, filtering, or feature engineering.

        Args:
            df: Raw dataframe from query

        Returns:
            Processed dataframe
        """
        return df

    def post_process(self, result: ProcessingResult) -> ProcessingResult:
        """Post-processing hook after main processing.

        Override to add result validation, enrichment, or formatting.

        Args:
            result: Result from process()

        Returns:
            Modified result
        """
        return result

    def run(
        self,
        start_mjd: float | None = None,
        end_mjd: float | None = None,
    ) -> ProcessingResult:
        """Execute the processor.

        Args:
            start_mjd: Optional window start (defaults to window_days ago)
            end_mjd: Optional window end (defaults to now)

        Returns:
            ProcessingResult with analysis output
        """
        import pandas as pd

        from ..utils.time import current_mjd, days_ago_mjd

        # Calculate time window
        if end_mjd is None:
            end_mjd = current_mjd()
        if start_mjd is None:
            start_mjd = days_ago_mjd(self.config.window_days)

        logger.info(
            "processor_started",
            processor=self.name,
            version=self.version,
            start_mjd=start_mjd,
            end_mjd=end_mjd,
            window_days=self.config.window_days,
        )

        # Fetch data
        query = self.get_query()
        params = self.get_query_params(start_mjd, end_mjd)
        rows = self.storage.query(query, params)

        if len(rows) < self.config.min_alerts:
            logger.warning(
                "insufficient_data",
                processor=self.name,
                found=len(rows),
                required=self.config.min_alerts,
            )
            return ProcessingResult(
                processor_name=self.name,
                processor_version=self.version,
                records=[],
                summary=f"Insufficient data: {len(rows)} alerts (need {self.config.min_alerts})",
                metadata={"skipped": True, "reason": "insufficient_data"},
            )

        # Convert to DataFrame
        df = pd.DataFrame(rows)
        logger.info("data_loaded", processor=self.name, rows=len(df))

        # Pre-process
        df = self.pre_process(df)

        # Main processing
        result = self.process(df)

        # Post-process
        result = self.post_process(result)

        # Add metadata
        result.metadata.update(
            {
                "window_start_mjd": start_mjd,
                "window_end_mjd": end_mjd,
                "input_rows": len(rows),
            }
        )

        logger.info(
            "processor_completed",
            processor=self.name,
            records=len(result.records),
            summary=result.summary,
        )

        return result

    def save_result(self, result: ProcessingResult) -> int:
        """Save processing result to database.

        Args:
            result: ProcessingResult to save

        Returns:
            Database row ID of saved result
        """
        return self.storage.write_processing_result(result)


class FilteringProcessor(BaseProcessor):
    """Base class for processors that filter alerts.

    Convenience class for processors that select a subset of alerts
    based on criteria rather than producing aggregate analysis.
    """

    @abstractmethod
    def filter_condition(self, row: dict[str, Any]) -> bool:
        """Return True if row should be included in output.

        Args:
            row: Alert record as dictionary

        Returns:
            True to include, False to exclude
        """
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> ProcessingResult:
        """Filter dataframe using filter_condition.

        Args:
            df: Input dataframe

        Returns:
            ProcessingResult with filtered records
        """
        records: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            row_dict: dict[str, Any] = {str(k): v for k, v in row.to_dict().items()}
            if self.filter_condition(row_dict):
                records.append(row_dict)

        return ProcessingResult(
            processor_name=self.name,
            processor_version=self.version,
            records=records,
            summary=f"Selected {len(records)} of {len(df)} alerts",
        )


class AggregatingProcessor(BaseProcessor):
    """Base class for processors that aggregate alerts by source.

    Convenience class for processors that group alerts by dia_object_id
    or dia_source_id and compute aggregate metrics.
    """

    group_by: str = "dia_object_id"

    @abstractmethod
    def aggregate(self, group_df: pd.DataFrame) -> dict[str, Any] | None:
        """Compute aggregate metrics for a group of alerts.

        Args:
            group_df: Alerts for one source/object

        Returns:
            Dict of metrics, or None to skip this group
        """
        raise NotImplementedError

    def process(self, df: pd.DataFrame) -> ProcessingResult:
        """Group and aggregate alerts.

        Args:
            df: Input dataframe

        Returns:
            ProcessingResult with aggregated records
        """
        records = []
        groups = df.groupby(self.group_by)

        for group_id, group_df in groups:
            result = self.aggregate(group_df)
            if result is not None:
                result[self.group_by] = group_id
                records.append(result)

        return ProcessingResult(
            processor_name=self.name,
            processor_version=self.version,
            records=records,
            summary=f"Aggregated {len(groups)} groups into {len(records)} results",
        )
