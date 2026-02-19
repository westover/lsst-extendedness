"""
Database schema for the LSST Extendedness Pipeline.

This module defines the SQLite schema including:
- Core tables (alerts_raw, alerts_filtered, etc.)
- Indexes for common query patterns
- Views for scientist-friendly access
- Migration support for schema updates

Schema Philosophy:
- alerts_raw: Never modified, complete audit trail
- alerts_filtered: Configurable filter results (references raw)
- processed_sources: State tracking for reassociation detection
- processing_results: Post-processor output
- ingestion_runs: Run-level audit trail

Example:
    >>> from lsst_extendedness.storage.schema import create_schema, get_schema_sql
    >>>
    >>> # Create all tables
    >>> create_schema(connection)
    >>>
    >>> # Get SQL for manual inspection
    >>> print(get_schema_sql())
"""

from __future__ import annotations

import sqlite3

# Schema version for migration tracking
SCHEMA_VERSION = 1

# ============================================================================
# TABLE DEFINITIONS
# ============================================================================

TABLES_SQL = f"""
-- ============================================================================
-- ALERTS_RAW: All ingested alerts (immutable, audit trail)
-- ============================================================================
CREATE TABLE IF NOT EXISTS alerts_raw (
    -- Primary key
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Alert identifiers
    alert_id INTEGER NOT NULL,
    dia_source_id INTEGER NOT NULL,
    dia_object_id INTEGER,

    -- Coordinates
    ra REAL NOT NULL,
    dec REAL NOT NULL,

    -- Temporal
    mjd REAL NOT NULL,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Photometry
    filter_name TEXT,
    ps_flux REAL,
    ps_flux_err REAL,
    snr REAL,

    -- Extendedness (key science metrics)
    extendedness_median REAL,
    extendedness_min REAL,
    extendedness_max REAL,

    -- Solar system association
    has_ss_source INTEGER NOT NULL DEFAULT 0,
    ss_object_id TEXT,
    ss_object_reassoc_time_mjd REAL,
    is_reassociation INTEGER NOT NULL DEFAULT 0,
    reassociation_reason TEXT,

    -- Dynamic fields (JSON)
    trail_data TEXT DEFAULT '{{}}',
    pixel_flags TEXT DEFAULT '{{}}',

    -- Cutout paths
    science_cutout_path TEXT,
    template_cutout_path TEXT,
    difference_cutout_path TEXT,

    -- Constraints
    UNIQUE(alert_id, dia_source_id)
);

-- ============================================================================
-- ALERTS_FILTERED: Filtered subset (references raw)
-- ============================================================================
CREATE TABLE IF NOT EXISTS alerts_filtered (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Reference to raw alert
    raw_alert_id INTEGER NOT NULL,

    -- Filter configuration (for reproducibility)
    filter_config_hash TEXT NOT NULL,
    filter_config TEXT NOT NULL DEFAULT '{{}}',

    -- When filtered
    filtered_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Constraints
    FOREIGN KEY (raw_alert_id) REFERENCES alerts_raw(id),
    UNIQUE(raw_alert_id, filter_config_hash)
);

-- ============================================================================
-- PROCESSED_SOURCES: State tracking for reassociation detection
-- ============================================================================
CREATE TABLE IF NOT EXISTS processed_sources (
    -- Primary key is dia_source_id
    dia_source_id INTEGER PRIMARY KEY,

    -- First and last observation
    first_seen_mjd REAL NOT NULL,
    last_seen_mjd REAL NOT NULL,

    -- Current SSObject association
    ss_object_id TEXT,
    ss_object_reassoc_time REAL,

    -- Tracking metadata
    observation_count INTEGER NOT NULL DEFAULT 1,
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),

    -- Additional state (JSON)
    metadata TEXT DEFAULT '{{}}'
);

-- ============================================================================
-- PROCESSING_RESULTS: Post-processor output
-- ============================================================================
CREATE TABLE IF NOT EXISTS processing_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Processor identification
    processor_name TEXT NOT NULL,
    processor_version TEXT NOT NULL,

    -- Results (JSON)
    records TEXT NOT NULL DEFAULT '[]',
    metadata TEXT NOT NULL DEFAULT '{{}}',
    summary TEXT,

    -- Timing
    processed_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Processing window (for incremental processing)
    window_start_mjd REAL,
    window_end_mjd REAL,

    -- Status
    status TEXT NOT NULL DEFAULT 'completed',
    error_message TEXT
);

-- ============================================================================
-- INGESTION_RUNS: Run-level audit trail
-- ============================================================================
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Source information
    source_name TEXT NOT NULL,
    source_config TEXT DEFAULT '{{}}',

    -- Timing
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,

    -- Statistics
    alerts_ingested INTEGER NOT NULL DEFAULT 0,
    alerts_failed INTEGER NOT NULL DEFAULT 0,
    new_sources INTEGER NOT NULL DEFAULT 0,
    reassociations_detected INTEGER NOT NULL DEFAULT 0,
    cutouts_saved INTEGER NOT NULL DEFAULT 0,

    -- Status
    status TEXT NOT NULL DEFAULT 'running',
    error_message TEXT,

    -- Additional metadata
    metadata TEXT DEFAULT '{{}}'
);

-- ============================================================================
-- SCHEMA_INFO: Track schema version
-- ============================================================================
CREATE TABLE IF NOT EXISTS schema_info (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Insert schema version
INSERT OR REPLACE INTO schema_info (key, value) VALUES ('version', '{SCHEMA_VERSION}');
INSERT OR REPLACE INTO schema_info (key, value) VALUES ('created_at', datetime('now'));
"""

# ============================================================================
# INDEX DEFINITIONS
# ============================================================================

INDEXES_SQL = """
-- Alerts raw indexes
CREATE INDEX IF NOT EXISTS idx_alerts_raw_mjd ON alerts_raw(mjd);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_alert_id ON alerts_raw(alert_id);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_dia_source_id ON alerts_raw(dia_source_id);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_dia_object_id ON alerts_raw(dia_object_id);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_ss_object_id ON alerts_raw(ss_object_id);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_has_ss_source ON alerts_raw(has_ss_source);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_extendedness ON alerts_raw(extendedness_median);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_ingested_at ON alerts_raw(ingested_at);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_coords ON alerts_raw(ra, dec);
CREATE INDEX IF NOT EXISTS idx_alerts_raw_reassociation ON alerts_raw(is_reassociation);

-- Alerts filtered indexes
CREATE INDEX IF NOT EXISTS idx_alerts_filtered_raw ON alerts_filtered(raw_alert_id);
CREATE INDEX IF NOT EXISTS idx_alerts_filtered_config ON alerts_filtered(filter_config_hash);

-- Processed sources indexes
CREATE INDEX IF NOT EXISTS idx_processed_sources_ss_object ON processed_sources(ss_object_id);
CREATE INDEX IF NOT EXISTS idx_processed_sources_last_seen ON processed_sources(last_seen_mjd);

-- Processing results indexes
CREATE INDEX IF NOT EXISTS idx_processing_results_processor ON processing_results(processor_name);
CREATE INDEX IF NOT EXISTS idx_processing_results_time ON processing_results(processed_at);

-- Ingestion runs indexes
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source ON ingestion_runs(source_name);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status ON ingestion_runs(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_started ON ingestion_runs(started_at);
"""

# ============================================================================
# VIEW DEFINITIONS
# ============================================================================

VIEWS_SQL = """
-- Point sources (low extendedness, star-like)
CREATE VIEW IF NOT EXISTS v_point_sources AS
SELECT * FROM alerts_raw
WHERE extendedness_median IS NOT NULL
  AND extendedness_median < 0.3;

-- Extended sources (high extendedness, galaxy-like)
CREATE VIEW IF NOT EXISTS v_extended_sources AS
SELECT * FROM alerts_raw
WHERE extendedness_median IS NOT NULL
  AND extendedness_median > 0.7;

-- Minimoon candidates (SSO with intermediate extendedness)
CREATE VIEW IF NOT EXISTS v_minimoon_candidates AS
SELECT * FROM alerts_raw
WHERE has_ss_source = 1
  AND extendedness_median IS NOT NULL
  AND extendedness_median BETWEEN 0.3 AND 0.7;

-- Recent alerts (last 7 days)
CREATE VIEW IF NOT EXISTS v_recent_alerts AS
SELECT * FROM alerts_raw
WHERE ingested_at >= datetime('now', '-7 days')
ORDER BY ingested_at DESC;

-- Today's alerts
CREATE VIEW IF NOT EXISTS v_today_alerts AS
SELECT * FROM alerts_raw
WHERE date(ingested_at) = date('now')
ORDER BY ingested_at DESC;

-- Alerts with reassociations
CREATE VIEW IF NOT EXISTS v_reassociations AS
SELECT * FROM alerts_raw
WHERE is_reassociation = 1
ORDER BY mjd DESC;

-- SSO associated alerts
CREATE VIEW IF NOT EXISTS v_sso_alerts AS
SELECT * FROM alerts_raw
WHERE has_ss_source = 1
ORDER BY mjd DESC;

-- Processing summary by processor
CREATE VIEW IF NOT EXISTS v_processing_summary AS
SELECT
    processor_name,
    processor_version,
    COUNT(*) as run_count,
    MAX(processed_at) as last_run,
    SUM(json_array_length(records)) as total_records
FROM processing_results
WHERE status = 'completed'
GROUP BY processor_name, processor_version;

-- Ingestion summary by day
CREATE VIEW IF NOT EXISTS v_ingestion_daily AS
SELECT
    date(started_at) as date,
    COUNT(*) as runs,
    SUM(alerts_ingested) as total_ingested,
    SUM(alerts_failed) as total_failed,
    SUM(new_sources) as total_new_sources,
    SUM(reassociations_detected) as total_reassociations
FROM ingestion_runs
WHERE status = 'completed'
GROUP BY date(started_at)
ORDER BY date DESC;

-- Source statistics
CREATE VIEW IF NOT EXISTS v_source_stats AS
SELECT
    dia_source_id,
    observation_count,
    first_seen_mjd,
    last_seen_mjd,
    last_seen_mjd - first_seen_mjd as arc_length_days,
    ss_object_id,
    ss_object_reassoc_time
FROM processed_sources
ORDER BY observation_count DESC;
"""

# ============================================================================
# TRIGGERS (optional, for data integrity)
# ============================================================================

TRIGGERS_SQL = """
-- Update processed_sources observation count on insert
CREATE TRIGGER IF NOT EXISTS tr_update_processed_sources
AFTER INSERT ON alerts_raw
BEGIN
    INSERT INTO processed_sources (
        dia_source_id,
        first_seen_mjd,
        last_seen_mjd,
        ss_object_id,
        ss_object_reassoc_time,
        observation_count,
        last_updated
    )
    VALUES (
        NEW.dia_source_id,
        NEW.mjd,
        NEW.mjd,
        NEW.ss_object_id,
        NEW.ss_object_reassoc_time_mjd,
        1,
        datetime('now')
    )
    ON CONFLICT(dia_source_id) DO UPDATE SET
        last_seen_mjd = MAX(last_seen_mjd, NEW.mjd),
        observation_count = observation_count + 1,
        ss_object_id = COALESCE(NEW.ss_object_id, ss_object_id),
        ss_object_reassoc_time = COALESCE(NEW.ss_object_reassoc_time_mjd, ss_object_reassoc_time),
        last_updated = datetime('now');
END;
"""


def get_schema_sql() -> str:
    """Get complete schema SQL for inspection.

    Returns:
        Complete SQL schema as a string
    """
    return "\n".join(
        [
            "-- LSST Extendedness Pipeline Schema",
            f"-- Version: {SCHEMA_VERSION}",
            "",
            "-- TABLES",
            TABLES_SQL,
            "",
            "-- INDEXES",
            INDEXES_SQL,
            "",
            "-- VIEWS",
            VIEWS_SQL,
            "",
            "-- TRIGGERS",
            TRIGGERS_SQL,
        ]
    )


def create_schema(conn: sqlite3.Connection, include_triggers: bool = True) -> None:
    """Create all database tables, indexes, and views.

    This function is idempotent - safe to call multiple times.

    Args:
        conn: SQLite connection
        include_triggers: Whether to create triggers (default: True)

    Example:
        >>> conn = sqlite3.connect("data/alerts.db")
        >>> create_schema(conn)
    """
    cursor = conn.cursor()

    # Enable WAL mode for better concurrency
    cursor.execute("PRAGMA journal_mode=WAL")

    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys=ON")

    # Create tables
    cursor.executescript(TABLES_SQL)

    # Create indexes
    cursor.executescript(INDEXES_SQL)

    # Create views
    cursor.executescript(VIEWS_SQL)

    # Create triggers (optional)
    if include_triggers:
        cursor.executescript(TRIGGERS_SQL)

    conn.commit()


def get_schema_version(conn: sqlite3.Connection) -> int | None:
    """Get current schema version from database.

    Args:
        conn: SQLite connection

    Returns:
        Schema version number, or None if not found
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM schema_info WHERE key = 'version'")
        row = cursor.fetchone()
        return int(row[0]) if row else None
    except sqlite3.OperationalError:
        return None


def needs_migration(conn: sqlite3.Connection) -> bool:
    """Check if database needs migration.

    Args:
        conn: SQLite connection

    Returns:
        True if migration is needed
    """
    current_version = get_schema_version(conn)
    return current_version is None or current_version < SCHEMA_VERSION


def migrate(conn: sqlite3.Connection) -> None:
    """Apply any pending migrations.

    Currently just ensures schema exists. Future versions
    will include incremental migrations.

    Args:
        conn: SQLite connection
    """
    current_version = get_schema_version(conn)

    if current_version is None:
        # Fresh database, create schema
        create_schema(conn)
    elif current_version < SCHEMA_VERSION:
        # Apply migrations
        _apply_migrations(conn, current_version)


def _apply_migrations(conn: sqlite3.Connection, from_version: int) -> None:
    """Apply incremental migrations.

    Args:
        conn: SQLite connection
        from_version: Current schema version
    """
    # Future migrations would be applied here
    # Example:
    # if from_version < 2:
    #     conn.execute("ALTER TABLE alerts_raw ADD COLUMN new_field TEXT")

    # Update version
    conn.execute("UPDATE schema_info SET value = ? WHERE key = 'version'", (str(SCHEMA_VERSION),))
    conn.commit()
