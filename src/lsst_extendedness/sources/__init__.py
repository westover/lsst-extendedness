"""
Alert source implementations for the LSST Extendedness Pipeline.

This module provides flexible input sources that implement the AlertSource protocol:

- KafkaSource: Real-time streaming from Kafka/ANTARES broker
- FileSource: Import from AVRO/CSV files (for backfill or testing)
- DatabaseSource: Pull from external databases
- MockSource: Generate synthetic alerts (for testing)

Example - Using different sources:
    >>> from lsst_extendedness.sources import KafkaSource, MockSource
    >>>
    >>> # Production: Kafka
    >>> source = KafkaSource(config)
    >>> source.connect()
    >>> for alert in source.fetch_alerts():
    ...     process(alert)
    >>>
    >>> # Testing: Mock
    >>> source = MockSource(count=100)
    >>> for alert in source.fetch_alerts():
    ...     process(alert)

To implement a custom source, see `sources/protocol.py` for the interface.
"""

from lsst_extendedness.sources.file import FileSource
from lsst_extendedness.sources.kafka import KafkaSource
from lsst_extendedness.sources.mock import MockSource
from lsst_extendedness.sources.protocol import AlertSource, register_source

__all__ = [
    "AlertSource",
    "FileSource",
    "KafkaSource",
    "MockSource",
    "register_source",
]
