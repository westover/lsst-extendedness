# Contributing to LSST Extendedness Pipeline

Thank you for your interest in contributing! This guide covers the development workflow, code quality tools, and CI/CD pipeline.

## Quick Start

### Prerequisites

- Python 3.12 or 3.13
- [PDM](https://pdm-project.org/) package manager
- Git

### Setup

```bash
# Clone the repository
git clone https://github.com/westover/lsst-extendedness.git
cd lsst-extendedness

# Install PDM if you don't have it
pip install pdm

# Install all dependencies (including dev tools)
pdm install -G dev

# Install pre-commit hooks
pdm run pre-commit install
pdm run pre-commit install --hook-type commit-msg
```

## Code Quality Tools

We use several tools to maintain code quality. All are configured in `pyproject.toml`.

### Ruff (Linting & Formatting)

[Ruff](https://docs.astral.sh/ruff/) handles both linting and formatting in one fast tool.

```bash
# Check for linting issues
pdm run ruff check src/ tests/

# Auto-fix linting issues where possible
pdm run ruff check src/ tests/ --fix

# Check formatting
pdm run ruff format src/ tests/ --check

# Apply formatting
pdm run ruff format src/ tests/
```

**Rules enabled:**
- `E`, `W` - pycodestyle errors and warnings
- `F` - Pyflakes
- `I` - isort (import sorting)
- `B` - flake8-bugbear
- `C4` - flake8-comprehensions
- `UP` - pyupgrade
- `ARG` - flake8-unused-arguments
- `SIM` - flake8-simplify
- `PTH` - flake8-use-pathlib
- `RUF` - Ruff-specific rules

**Line length:** 100 characters

### Mypy (Type Checking)

We use strict type checking with [mypy](https://mypy.readthedocs.io/).

```bash
# Run type checker
pdm run mypy src/lsst_extendedness/
```

**Configuration highlights:**
- `strict = true` - All strict checks enabled
- `disallow_untyped_defs = true` - All functions must have type hints
- Test files have relaxed typing requirements

**Type stubs provided for:**
- `pandas-stubs` - pandas type hints
- `types-requests` - requests type hints

### Pre-commit Hooks

Pre-commit runs automatically on `git commit`. Hooks include:

| Hook | Purpose |
|------|---------|
| `check-added-large-files` | Prevent files > 1MB |
| `check-case-conflict` | Detect case-insensitive conflicts |
| `end-of-file-fixer` | Ensure files end with newline |
| `trailing-whitespace` | Remove trailing whitespace |
| `check-yaml`, `check-toml`, `check-json` | Validate config files |
| `check-merge-conflict` | Detect conflict markers |
| `debug-statements` | Prevent debugger imports |
| `detect-private-key` | Prevent accidental key commits |
| `ruff` | Linting with auto-fix |
| `ruff-format` | Code formatting |
| `mypy` | Type checking |
| `bandit` | Security checks |
| `pytest` | Run tests |
| `commitizen` | Validate commit messages |

**Manual run:**
```bash
# Run all hooks on all files
pdm run pre-commit run --all-files

# Run specific hook
pdm run pre-commit run ruff --all-files
```

## Testing

We use [pytest](https://docs.pytest.org/) with coverage tracking.

### Running Tests

```bash
# Run all tests
pdm run pytest tests/

# Run with verbose output
pdm run pytest tests/ -v

# Run with coverage report
pdm run pytest tests/ -v --cov=lsst_extendedness --cov-report=term-missing

# Run with HTML coverage report
pdm run pytest tests/ -v --cov=lsst_extendedness --cov-report=html
# Then open htmlcov/index.html

# Run specific test file
pdm run pytest tests/test_models.py -v

# Run tests matching a pattern
pdm run pytest tests/ -k "test_alert" -v
```

### Test Markers

```bash
# Skip slow tests
pdm run pytest tests/ -m "not slow"

# Run only integration tests
pdm run pytest tests/ -m integration

# Run only unit tests
pdm run pytest tests/ -m unit
```

### Coverage Requirements

- **Minimum coverage:** 85%
- **Branch coverage:** Enabled
- **Excluded from coverage:**
  - `cli.py` (tested via integration)
  - `__main__.py`

### Writing Tests

```python
import pytest
from lsst_extendedness.models import AlertRecord

class TestAlertRecord:
    """Tests for AlertRecord model."""

    def test_create_valid_alert(self):
        """Test creating an alert with valid data."""
        alert = AlertRecord(
            alert_id=1,
            dia_source_id=1000,
            ra=180.0,
            dec=45.0,
            mjd=60000.0,
        )
        assert alert.alert_id == 1

    def test_invalid_ra_raises(self):
        """Test that invalid RA raises ValidationError."""
        with pytest.raises(ValueError):
            AlertRecord(
                alert_id=1,
                dia_source_id=1000,
                ra=400.0,  # Invalid: must be 0-360
                dec=45.0,
                mjd=60000.0,
            )

    @pytest.mark.slow
    def test_large_batch_processing(self):
        """Test processing many alerts (slow)."""
        # ... slow test code
```

## PDM Scripts

Convenient shortcuts defined in `pyproject.toml`:

```bash
# Testing
pdm run test              # pytest tests/ -v
pdm run test-cov          # pytest with coverage + HTML report

# Linting & Formatting
pdm run lint              # ruff check src/ tests/
pdm run lint-fix          # ruff check --fix
pdm run format            # ruff format src/ tests/
pdm run format-check      # ruff format --check
pdm run typecheck         # mypy src/

# All checks at once
pdm run check             # lint + format-check + typecheck + test

# Database operations
pdm run db-init           # Initialize SQLite schema
pdm run db-shell          # Open SQLite shell
pdm run db-stats          # Show database statistics

# Pipeline operations
pdm run ingest            # Run ingestion
pdm run query             # Run queries
pdm run process           # Run post-processing
```

## CI/CD Pipeline

GitHub Actions runs on every push and pull request to `main`.

### Workflow Jobs

| Job | Description | Python Versions |
|-----|-------------|-----------------|
| `lint` | Ruff linting, formatting check, mypy | 3.13 |
| `test` | pytest with coverage | 3.12, 3.13 |
| `coverage` | Enforce 85% coverage threshold | 3.13 |
| `all-checks` | Gate job - all must pass | - |

### CI Configuration

The workflow is defined in `.github/workflows/ci.yml`.

**Key features:**
- Caches PDM dependencies for faster builds
- Installs system dependencies (`librdkafka-dev`, `libopenblas-dev`)
- Uploads coverage to Codecov
- Fails if any check doesn't pass

### Running CI Locally

You can simulate CI locally before pushing:

```bash
# Run all the same checks CI runs
pdm run check

# Or individually:
pdm run ruff check src/lsst_extendedness/ tests/
pdm run ruff format src/lsst_extendedness/ tests/ --check
pdm run mypy src/lsst_extendedness/
pdm run pytest tests/ -v --cov=lsst_extendedness --cov-fail-under=85
```

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/) with [Commitizen](https://commitizen-tools.github.io/commitizen/).

### Format

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, no code change |
| `refactor` | Code change that neither fixes nor adds |
| `perf` | Performance improvement |
| `test` | Adding or correcting tests |
| `chore` | Maintenance tasks |
| `ci` | CI/CD changes |

### Examples

```bash
feat(ingest): add ANTARES source adapter
fix(filter): correct SNR threshold validation
docs(readme): update installation instructions
test(models): add AlertRecord validation tests
ci(actions): add Python 3.13 to test matrix
```

## Project Structure

```
lsst-extendedness/
├── src/lsst_extendedness/     # Main package
│   ├── models/                # Pydantic data models
│   ├── sources/               # Alert source adapters (Kafka, File, Mock)
│   ├── storage/               # SQLite storage backend
│   ├── ingest/                # Ingestion pipeline
│   ├── filter/                # Configurable alert filtering
│   ├── processing/            # Post-processing framework
│   ├── query/                 # Query shortcuts and export
│   ├── cutouts/               # FITS cutout extraction
│   └── cli.py                 # Command-line interface
├── tests/                     # Test suite
│   ├── conftest.py            # Shared fixtures
│   ├── fixtures/              # Test data factories
│   └── test_*.py              # Test modules
├── config/                    # Configuration files
├── pyproject.toml             # Project configuration
└── .github/workflows/         # CI/CD workflows
```

## Adding New Features

### 1. Create a Branch

```bash
git checkout -b feat/my-new-feature
```

### 2. Write Tests First

Add tests in `tests/test_*.py` before implementing.

### 3. Implement the Feature

Add code in `src/lsst_extendedness/`.

### 4. Run Quality Checks

```bash
pdm run check
```

### 5. Commit with Conventional Message

```bash
git add .
git commit -m "feat(module): add new feature"
```

### 6. Push and Create PR

```bash
git push origin feat/my-new-feature
```

Then create a pull request on GitHub.

## Implementing Alert Sources

To add a new alert source, implement the `AlertSource` protocol:

```python
from typing import Iterator
from lsst_extendedness.sources.protocol import AlertSource
from lsst_extendedness.models import AlertRecord

class MyCustomSource(AlertSource):
    """Custom alert source implementation."""

    source_name: str = "my_source"

    def connect(self) -> None:
        """Establish connection to the data source."""
        self._client = MyAPIClient(self.config.api_url)

    def fetch_alerts(self, limit: int | None = None) -> Iterator[AlertRecord]:
        """Yield alerts from the source."""
        for raw_alert in self._client.get_alerts(limit=limit):
            yield AlertRecord.from_dict(raw_alert)

    def close(self) -> None:
        """Clean up resources."""
        if hasattr(self, '_client'):
            self._client.disconnect()
```

## Getting Help

- **Issues:** [GitHub Issues](https://github.com/westover/lsst-extendedness/issues)
- **Discussions:** Open an issue for questions

## License

MIT License - see [LICENSE](LICENSE) for details.
