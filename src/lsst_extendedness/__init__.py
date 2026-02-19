"""
LSST Extendedness Pipeline

A data pipeline for processing LSST astronomical alerts with focus on
extendedness analysis and solar system object (SSO) reassociation detection.

Features:
- Flexible input sources (Kafka, file, database, mock)
- SQLite-based storage with Pydantic models
- Configurable filtering engine
- Post-processing framework for custom analysis
- Scientist-friendly query shortcuts

Example:
    >>> from lsst_extendedness import AlertRecord, SQLiteStorage
    >>> from lsst_extendedness.query import shortcuts
    >>>
    >>> # Query today's alerts
    >>> df = shortcuts.today()
    >>> print(f"Found {len(df)} alerts today")

For more information, see the README.md or run:
    $ lsst-extendedness --help
"""

__version__ = "2.0.0"
__author__ = "James Westover"
__email__ = "james@westover.xyz"

# Core models
# Configuration
from lsst_extendedness.config.settings import Settings, get_settings
from lsst_extendedness.models.alerts import AlertRecord, ProcessingResult
from lsst_extendedness.models.runs import IngestionRun
from lsst_extendedness.sources.file import FileSource
from lsst_extendedness.sources.kafka import KafkaSource
from lsst_extendedness.sources.mock import MockSource

# Sources
from lsst_extendedness.sources.protocol import AlertSource
from lsst_extendedness.storage.protocol import AlertStorage

# Storage
from lsst_extendedness.storage.sqlite import SQLiteStorage

__all__ = [
    "AlertRecord",
    "AlertSource",
    "AlertStorage",
    "FileSource",
    "IngestionRun",
    "KafkaSource",
    "MockSource",
    "ProcessingResult",
    "SQLiteStorage",
    "Settings",
    "__author__",
    "__email__",
    "__version__",
    "get_settings",
]
