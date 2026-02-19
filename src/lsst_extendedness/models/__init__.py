"""
Data models for the LSST Extendedness Pipeline.

This module provides Pydantic-based data models for:
- AlertRecord: Core alert data with validation
- ProcessingResult: Results from post-processors
- IngestionRun: Metadata about ingestion runs

All models support:
- Automatic validation on creation
- JSON serialization/deserialization
- Type hints for IDE support
- Database-ready dictionary conversion
"""

from lsst_extendedness.models.alerts import AlertRecord, ProcessingResult
from lsst_extendedness.models.runs import IngestionRun

__all__ = [
    "AlertRecord",
    "IngestionRun",
    "ProcessingResult",
]
