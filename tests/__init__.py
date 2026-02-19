"""
LSST Extendedness Pipeline - Test Suite

Unit and integration tests for the alert processing pipeline.

Test Organization:
- test_models.py: Pydantic model validation and conversion
- test_sources.py: Alert source implementations
- test_storage.py: SQLite storage backend
- test_ingest.py: Ingestion pipeline integration
- test_filter.py: Filter engine tests
- test_processing.py: Post-processor framework

Fixtures are in tests/fixtures/:
- factories.py: AlertFactory for generating test data
- avro_samples.py: Sample AVRO records

Run tests:
    $ make test          # Quick test run
    $ make test-cov      # With coverage
    $ pdm run pytest tests/ -v
"""

__version__ = "2.0.0"
