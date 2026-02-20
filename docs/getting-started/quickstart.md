# Quick Start

Get up and running in under 5 minutes.

## 1. Initialize Database

```bash
pdm run lsst-extendedness db-init
```

## 2. Ingest Test Data

```bash
# Using mock data (no external dependencies)
pdm run lsst-extendedness ingest --source mock --count 100

# Check it worked
pdm run lsst-extendedness db-stats
```

## 3. Query Data (Python)

```python
from lsst_extendedness.storage import SQLiteStorage
from lsst_extendedness.sources import FinkSource

# Option A: Query existing data
storage = SQLiteStorage("data/lsst_extendedness.db")
storage.initialize()

count = storage.get_alert_count()
print(f"Total alerts: {count}")

# Option B: Use Fink fixtures (real ZTF data, no credentials)
with FinkSource() as source:
    for alert in source.fetch_alerts(limit=5):
        print(f"Alert {alert.alert_id}: RA={alert.ra:.2f}, Dec={alert.dec:.2f}")
```

## 4. Test with Real Astronomical Data

The `FinkSource` provides real ZTF alert data without requiring credentials:

```python
from lsst_extendedness.sources import FinkSource

# Load real ZTF alerts from fixtures
source = FinkSource()
source.connect()

# Fetch alerts
alerts = list(source.fetch_alerts(limit=10))
print(f"Loaded {len(alerts)} real ZTF alerts")

# Check for SSO (Solar System Objects)
sso_alerts = [a for a in alerts if a.has_ss_source]
print(f"SSO alerts: {len(sso_alerts)}")

source.close()
```

## 5. Production: ANTARES Broker

For real-time LSST alerts (requires credentials):

```bash
export ANTARES_API_KEY="your-key"
export ANTARES_API_SECRET="your-secret"

pdm run lsst-extendedness ingest \
    --source antares \
    --topics extragalactic_staging \
    --limit 1000
```

## Common Commands

| Command | Description |
|---------|-------------|
| `pdm run lsst-extendedness db-init` | Initialize database |
| `pdm run lsst-extendedness db-stats` | Show database statistics |
| `pdm run lsst-extendedness ingest --source mock --count N` | Ingest N mock alerts |
| `pdm run lsst-extendedness query --recent 7` | Query last 7 days |
| `pdm run pytest tests/ -v` | Run tests |

## Next Steps

- [Configuration](configuration.md) - Customize settings
- [Ingestion Guide](../guide/ingestion.md) - Detailed ingestion docs
- [API Reference](../api/sources.md) - Source implementations
