"""
Processing Runner.

Orchestrates execution of post-processors, handling scheduling,
parallelization, and result aggregation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

import structlog

from ..models.alerts import ProcessingResult
from .base import BaseProcessor, ProcessorConfig
from .registry import get_processor, list_processors, load_builtin_processors

if TYPE_CHECKING:
    from ..storage.sqlite import SQLiteStorage

logger = structlog.get_logger(__name__)


@dataclass
class RunResult:
    """Result from running one or more processors."""

    processor_name: str
    success: bool
    result: ProcessingResult | None = None
    error_message: str | None = None
    elapsed_seconds: float = 0.0


@dataclass
class BatchRunResult:
    """Result from running multiple processors."""

    results: list[RunResult] = field(default_factory=list)
    total_elapsed_seconds: float = 0.0
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None

    @property
    def success_count(self) -> int:
        """Number of successful processor runs."""
        return sum(1 for r in self.results if r.success)

    @property
    def failure_count(self) -> int:
        """Number of failed processor runs."""
        return sum(1 for r in self.results if not r.success)

    @property
    def all_success(self) -> bool:
        """True if all processors succeeded."""
        return all(r.success for r in self.results)


class ProcessingRunner:
    """Runs post-processors on accumulated alert data.

    Usage:
        storage = SQLiteStorage(db_path)
        runner = ProcessingRunner(storage)

        # Run specific processor
        result = runner.run("minimoon_detector", window_days=15)

        # Run all processors
        results = runner.run_all(window_days=15)
    """

    def __init__(
        self,
        storage: "SQLiteStorage",
        auto_load_builtin: bool = True,
    ):
        """Initialize runner.

        Args:
            storage: SQLiteStorage for data access
            auto_load_builtin: Whether to auto-load builtin processors
        """
        self.storage = storage

        if auto_load_builtin:
            load_builtin_processors()

    def run(
        self,
        processor_name: str,
        *,
        window_days: int = 15,
        save_result: bool = True,
        **extra_params,
    ) -> RunResult:
        """Run a single processor.

        Args:
            processor_name: Registered processor name
            window_days: Time window in days
            save_result: Whether to save result to database
            **extra_params: Additional processor parameters

        Returns:
            RunResult with outcome
        """
        start_time = time.monotonic()

        # Get processor class
        processor_cls = get_processor(processor_name)
        if not processor_cls:
            return RunResult(
                processor_name=processor_name,
                success=False,
                error_message=f"Processor not found: {processor_name}",
            )

        try:
            # Configure and instantiate
            config = ProcessorConfig(
                window_days=window_days,
                extra_params=extra_params,
            )
            processor = processor_cls(self.storage, config)

            # Run processor
            result = processor.run()

            # Save result if requested
            if save_result and result.records:
                processor.save_result(result)

            elapsed = time.monotonic() - start_time

            return RunResult(
                processor_name=processor_name,
                success=True,
                result=result,
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.monotonic() - start_time
            logger.error(
                "processor_run_failed",
                processor=processor_name,
                error=str(e),
            )
            return RunResult(
                processor_name=processor_name,
                success=False,
                error_message=str(e),
                elapsed_seconds=elapsed,
            )

    def run_all(
        self,
        *,
        window_days: int = 15,
        save_results: bool = True,
        stop_on_error: bool = False,
        processors: list[str] | None = None,
    ) -> BatchRunResult:
        """Run multiple processors.

        Args:
            window_days: Time window in days
            save_results: Whether to save results to database
            stop_on_error: Stop on first error
            processors: List of processor names (None = all)

        Returns:
            BatchRunResult with all outcomes
        """
        start_time = time.monotonic()
        batch_result = BatchRunResult(started_at=datetime.utcnow())

        # Get processor list
        if processors is None:
            processors = list(list_processors().keys())

        logger.info(
            "batch_run_started",
            processors=processors,
            window_days=window_days,
        )

        for name in processors:
            result = self.run(
                name,
                window_days=window_days,
                save_result=save_results,
            )
            batch_result.results.append(result)

            if not result.success and stop_on_error:
                logger.warning("batch_run_stopped", reason="stop_on_error", failed=name)
                break

        batch_result.total_elapsed_seconds = time.monotonic() - start_time
        batch_result.completed_at = datetime.utcnow()

        logger.info(
            "batch_run_completed",
            success=batch_result.success_count,
            failed=batch_result.failure_count,
            elapsed_seconds=round(batch_result.total_elapsed_seconds, 2),
        )

        return batch_result

    def list_processors(self) -> list[dict[str, str]]:
        """List available processors with info.

        Returns:
            List of processor info dicts
        """
        from .registry import get_processor_info
        return get_processor_info()

    def get_processor_history(
        self,
        processor_name: str,
        limit: int = 10,
    ) -> list[dict]:
        """Get recent processing results for a processor.

        Args:
            processor_name: Processor name
            limit: Maximum results to return

        Returns:
            List of recent processing results
        """
        return self.storage.query(
            """
            SELECT * FROM processing_results
            WHERE processor_name = ?
            ORDER BY processed_at DESC
            LIMIT ?
            """,
            (processor_name, limit),
        )


def run_processing(
    storage: "SQLiteStorage",
    *,
    processor: str | None = None,
    window_days: int = 15,
    save_results: bool = True,
) -> BatchRunResult | RunResult:
    """Convenience function to run processing.

    Args:
        storage: SQLiteStorage for data access
        processor: Specific processor name (None = all)
        window_days: Time window in days
        save_results: Whether to save results

    Returns:
        RunResult for single processor, BatchRunResult for all
    """
    runner = ProcessingRunner(storage)

    if processor:
        return runner.run(processor, window_days=window_days, save_result=save_results)
    else:
        return runner.run_all(window_days=window_days, save_results=save_results)
