"""
Post-processing framework for the LSST Extendedness Pipeline.

This module provides the extension point for scientific analysis:

- BaseProcessor: Interface for custom processors
- ProcessorRegistry: Auto-discovery of processors
- ProcessingRunner: Orchestration of processor execution

** INTEGRATION POINT FOR SCIENTISTS **

To add custom analysis (e.g., minimoon detection):

1. Create a processor in `processing/builtin/` or a plugin directory
2. Implement the BaseProcessor interface
3. Register with @register_processor decorator
4. Results stored in `processing_results` table

Example - Creating a Minimoon Detector:

    >>> from lsst_extendedness.processing import BaseProcessor, register_processor
    >>> import pandas as pd
    >>>
    >>> @register_processor("minimoon_detector")
    >>> class MiniMoonDetector(BaseProcessor):
    ...     '''Detect minimoon candidates from accumulated alerts.'''
    ...
    ...     name = "minimoon_detector"
    ...     version = "1.0.0"
    ...     default_window_days = 15
    ...
    ...     def process(self, df: pd.DataFrame) -> ProcessingResult:
    ...         # Your pandas/numpy analysis here
    ...         # System BLAS acceleration is automatic
    ...         candidates = self._analyze_orbits(df)
    ...         return ProcessingResult(
    ...             processor_name=self.name,
    ...             processor_version=self.version,
    ...             records=candidates,
    ...             summary=f"Found {len(candidates)} candidates"
    ...         )

Running processors:
    >>> from lsst_extendedness.processing import ProcessingRunner
    >>>
    >>> runner = ProcessingRunner(storage)
    >>> results = runner.run_all(window_days=15)
    >>> for result in results:
    ...     print(f"{result.processor_name}: {result.summary}")
"""

from lsst_extendedness.processing.base import BaseProcessor
from lsst_extendedness.processing.registry import get_processor, register_processor
from lsst_extendedness.processing.runner import ProcessingRunner

__all__ = [
    "BaseProcessor",
    "ProcessingRunner",
    "get_processor",
    "register_processor",
]
