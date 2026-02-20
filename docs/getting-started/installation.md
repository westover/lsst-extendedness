# Installation

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.12+ | [pyenv](https://github.com/pyenv/pyenv) recommended for version management |
| PDM | 2.0+ | [PDM](https://pdm-project.org/) - Modern Python package manager |
| librdkafka | latest | Required for Kafka connectivity |

## Quick Install

```bash
# Clone the repository
git clone https://github.com/fedorets/lsst-extendedness.git
cd lsst-extendedness

# Install dependencies with PDM
pdm install

# Verify installation
pdm run pytest tests/ -x -q
```

That's it! You're ready to use the pipeline.

## Detailed Setup

### 1. Install PDM

[PDM](https://pdm-project.org/) is a modern Python package manager with lockfile support.

```bash
# Using pipx (recommended)
pipx install pdm

# Or using pip
pip install --user pdm

# Verify
pdm --version
```

### 2. System Dependencies

=== "Ubuntu/Debian"

    ```bash
    sudo apt-get update
    sudo apt-get install -y librdkafka-dev
    ```

=== "macOS"

    ```bash
    brew install librdkafka
    ```

=== "RHEL/CentOS"

    ```bash
    sudo yum install -y librdkafka-devel
    ```

### 3. Clone and Install

```bash
git clone https://github.com/fedorets/lsst-extendedness.git
cd lsst-extendedness

# Production install
pdm install

# Development install (includes testing tools)
pdm install -G dev
```

### 4. Initialize Database

```bash
pdm run lsst-extendedness db-init
```

## Tool Links

| Tool | Purpose | Documentation |
|------|---------|---------------|
| [PDM](https://pdm-project.org/) | Package management | [User Guide](https://pdm-project.org/latest/usage/project/) |
| [pytest](https://pytest.org/) | Testing | [Getting Started](https://docs.pytest.org/en/stable/getting-started.html) |
| [ruff](https://docs.astral.sh/ruff/) | Linting & formatting | [Configuration](https://docs.astral.sh/ruff/configuration/) |
| [mypy](https://mypy.readthedocs.io/) | Type checking | [Cheat Sheet](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html) |
| [pre-commit](https://pre-commit.com/) | Git hooks | [Hooks](https://pre-commit.com/hooks.html) |

## Troubleshooting

### LSST RSP Environment

If installing in an existing LSST RSP environment, use a fresh virtual environment:

```bash
# Create isolated venv
pdm venv create
pdm use .venv/bin/python
pdm install
```

### Dependency Conflicts

If you see `httpx` or `urllib3` conflicts:

```bash
# Use PDM's isolated environment
pdm install --no-self

# Or create a fresh venv
python -m venv .venv
source .venv/bin/activate
pdm install
```

## Next Steps

→ [Quick Start](quickstart.md) - Run your first ingestion
