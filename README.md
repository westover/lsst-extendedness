# LSST Extendedness Pipeline

> **Note:** This project is under active development. APIs and interfaces may change.

A pipeline for processing LSST alerts through the ANTARES broker, with a focus on minimoon detection via extendedness analysis.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        LSST Extendedness Pipeline                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  [ANTARES Filter] → [Kafka Topic] → [Daily Consumer] → [SQLite DB]     │
│                                              │              │           │
│                                              │    ┌─────────┴─────────┐ │
│                                              │    │  alerts_raw       │ │
│                                              │    │  alerts_filtered  │ │
│                                              │    │  processed_sources│ │
│                                              │    │  processing_results│ │
│                                              │    └─────────┬─────────┘ │
│                                              │              │           │
│                                              ▼              ▼           │
│                              ┌────────────────────────────────────────┐ │
│                              │  Post-Processing (Minimoon Detection)  │ │
│                              └────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
```

## Features

- **Flexible Input Sources**: Protocol-based design supports Kafka, file import, and mock data
- **SQLite Storage**: Simple, fast, no server required
- **Pydantic Models**: Full validation and type hints
- **Systemd Timers**: Reliable daily scheduling
- **Extensible Processing**: Plugin architecture for post-processors

## Requirements

- Python 3.12+
- PDM (package manager)
- librdkafka (for Kafka support)

## Quick Start

```bash
# Clone and install
git clone https://github.com/westover/lsst-extendedness.git
cd lsst-extendedness
./scripts/bootstrap.sh

# Or manual install
pdm install

# Initialize database
make db-init

# Run with mock data (no Kafka needed)
lsst-extendedness ingest --source mock --count 1000

# Check database
make db-stats
```

## Configuration

Copy and edit the configuration:

```bash
cp config/default.toml config/local.toml
# Edit config/local.toml with your settings
```

For Kafka, also configure:

```bash
# Edit config/kafka_profiles.toml with your broker settings
```

## Usage

### Ingestion

```bash
# From Kafka (production)
lsst-extendedness ingest --config config/local.toml

# From files (backfill)
lsst-extendedness ingest --source file --path data/alerts/*.avro

# Mock data (testing)
lsst-extendedness ingest --source mock --count 500
```

### Queries

```bash
# Today's alerts
make query-today

# Minimoon candidates
make query-minimoon

# Interactive shell
make query
```

### Post-Processing

```bash
# Run all processors
make process

# With custom window
lsst-extendedness process --window 15
```

## Development

```bash
# Install dev dependencies
make dev-install

# Run tests
make test

# With coverage
make test-cov

# Lint and format
make lint
make format

# Type check
make typecheck

# All checks
make all-checks
```

## Systemd Timers

For automated daily runs:

```bash
# Install timers (runs at 2 AM and 4 AM)
make timer-install

# Check status
make timer-status

# View logs
make timer-logs

# Remove timers
make timer-uninstall
```

## Project Structure

```
lsst-extendedness/
├── src/lsst_extendedness/
│   ├── models/          # Pydantic data models
│   ├── sources/         # Input sources (Kafka, File, Mock)
│   ├── storage/         # SQLite backend
│   ├── ingest/          # Ingestion pipeline
│   ├── filter/          # Configurable filtering
│   ├── processing/      # Post-processor framework
│   ├── query/           # Query shortcuts
│   └── cli.py           # Command-line interface
├── config/              # TOML configuration
├── systemd/             # Timer units
└── tests/               # Test suite
```

## Resources

- [ANTARES Documentation](https://antares.noirlab.edu)
- [LSST Alert Schema](https://github.com/lsst/alert_packet)
- [APDB Schema](https://sdm-schemas.lsst.io/apdb.html)

## License

MIT
