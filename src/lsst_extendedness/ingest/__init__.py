"""
Ingestion pipeline for the LSST Extendedness Pipeline.

This module orchestrates the flow from sources to storage:

1. Connect to source (Kafka, file, etc.)
2. Deserialize AVRO alerts to AlertRecord
3. Detect reassociations by comparing with state
4. Write to SQLite database
5. Extract and save FITS cutouts
6. Update state and statistics

Example:
    >>> from lsst_extendedness.ingest import IngestionPipeline
    >>> from lsst_extendedness.sources import KafkaSource
    >>> from lsst_extendedness.storage import SQLiteStorage
    >>>
    >>> source = KafkaSource(config)
    >>> storage = SQLiteStorage("data/alerts.db")
    >>>
    >>> pipeline = IngestionPipeline(source, storage)
    >>> result = pipeline.run(max_messages=1000)
    >>> print(f"Ingested {result.alerts_ingested} alerts")

The pipeline is designed to be:
- Idempotent: Safe to restart
- Resumable: State persisted in database
- Observable: Structured logging and metrics
"""

from lsst_extendedness.ingest.deserializer import AlertDeserializer
from lsst_extendedness.ingest.pipeline import IngestionPipeline
from lsst_extendedness.ingest.state import StateTracker

__all__ = [
    "AlertDeserializer",
    "IngestionPipeline",
    "StateTracker",
]
