"""
Test fixtures for the LSST Extendedness Pipeline.

This module provides:
- AlertFactory: Create test alerts with sensible defaults
- Sample AVRO records: For testing deserialization
"""

from tests.fixtures.factories import AlertFactory
from tests.fixtures.avro_samples import SAMPLE_AVRO_RECORD, SAMPLE_AVRO_NO_SSO

__all__ = [
    "AlertFactory",
    "SAMPLE_AVRO_RECORD",
    "SAMPLE_AVRO_NO_SSO",
]
