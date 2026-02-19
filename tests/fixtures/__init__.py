"""
Test fixtures for the LSST Extendedness Pipeline.

This module provides:
- AlertFactory: Create test alerts with sensible defaults
- Sample AVRO records: For testing deserialization
"""

from tests.fixtures.avro_samples import SAMPLE_AVRO_NO_SSO, SAMPLE_AVRO_RECORD
from tests.fixtures.factories import AlertFactory

__all__ = [
    "SAMPLE_AVRO_NO_SSO",
    "SAMPLE_AVRO_RECORD",
    "AlertFactory",
]
