"""
Storage backends for the LSST Extendedness Pipeline.

This module provides storage implementations that implement the AlertStorage protocol:

- SQLiteStorage: Local SQLite database (default, recommended)

Future implementations:
- PostgresStorage: Production PostgreSQL (for scaling)
- ParquetStorage: Columnar file storage (for analytics export)

Example:
    >>> from lsst_extendedness.storage import SQLiteStorage
    >>> from lsst_extendedness.models import AlertRecord
    >>>
    >>> storage = SQLiteStorage("data/alerts.db")
    >>> storage.initialize()  # Create tables
    >>>
    >>> # Write alerts
    >>> alerts = [AlertRecord(...), AlertRecord(...)]
    >>> count = storage.write_batch(alerts)
    >>> print(f"Wrote {count} alerts")
    >>>
    >>> # Query
    >>> results = storage.query("SELECT * FROM alerts_raw WHERE mjd > ?", (60000.0,))

The schema is defined in `storage/schema.py` and includes:
- alerts_raw: All ingested alerts (immutable)
- alerts_filtered: Filtered subset with references to raw
- processed_sources: State tracking for reassociation detection
- processing_results: Post-processing output
- ingestion_runs: Audit trail
"""

from lsst_extendedness.storage.protocol import AlertStorage
from lsst_extendedness.storage.sqlite import SQLiteStorage
from lsst_extendedness.storage.schema import SCHEMA_VERSION, create_schema, get_schema_sql

__all__ = [
    "AlertStorage",
    "SQLiteStorage",
    "SCHEMA_VERSION",
    "create_schema",
    "get_schema_sql",
]
