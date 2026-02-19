# Quick Start Guide

> **Note:** This project is under active development.

## Prerequisites

- Python 3.12 or higher
- macOS or Linux

## 1. Clone the Repository

```bash
git clone https://github.com/westover/lsst-extendedness.git
cd lsst-extendedness
```

## 2. Install PDM (if not already installed)

```bash
# macOS
brew install pdm

# Linux
curl -sSL https://pdm-project.org/install-pdm.py | python3 -
```

## 3. Install Dependencies

```bash
# Production only
pdm install

# With development tools (recommended)
pdm install -G dev
```

## 4. Initialize the Database

```bash
make db-init
```

## 5. Verify Installation

```bash
# Run tests
make test

# Check database
make db-stats
```

## 6. Ingest Sample Data

```bash
# Generate mock alerts (no Kafka required)
pdm run python -c "
from lsst_extendedness.sources.mock import MockSource
from lsst_extendedness.storage.sqlite import SQLiteStorage
from pathlib import Path

# Initialize
storage = SQLiteStorage(Path('data/lsst_extendedness.db'))
storage.initialize()

# Generate and store 100 mock alerts
source = MockSource(count=100, seed=42)
source.connect()
alerts = list(source.fetch_alerts())
storage.write_batch(alerts)
source.close()

print(f'Ingested {len(alerts)} alerts')
print(storage.get_stats())
"
```

## 7. Query the Data

```bash
# Open SQLite shell
make db-shell

# Example queries:
sqlite> SELECT COUNT(*) FROM alerts_raw;
sqlite> SELECT * FROM v_minimoon_candidates LIMIT 5;
sqlite> SELECT * FROM v_point_sources LIMIT 5;
sqlite> .quit
```

## Next Steps

- Copy `config/default.toml` to `config/local.toml` and customize
- For Kafka ingestion, configure `config/kafka_profiles.toml`
- See `README.md` for full documentation

## Common Commands

| Command | Description |
|---------|-------------|
| `make test` | Run tests |
| `make test-cov` | Run tests with coverage |
| `make db-init` | Initialize database |
| `make db-shell` | Open SQLite shell |
| `make db-stats` | Show database statistics |
| `make lint` | Run linter |
| `make format` | Format code |

## Troubleshooting

### PDM not found

```bash
# Add to PATH
export PATH="$HOME/.local/bin:$PATH"
```

### librdkafka errors (Kafka source only)

```bash
# macOS
brew install librdkafka

# Ubuntu/Debian
sudo apt-get install librdkafka-dev
```

### Database locked errors

The database uses WAL mode for concurrent access. If you see lock errors:

```bash
# Check for stuck processes
lsof data/lsst_extendedness.db

# Reset WAL if needed
sqlite3 data/lsst_extendedness.db "PRAGMA wal_checkpoint(TRUNCATE);"
```
