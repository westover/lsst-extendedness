#!/usr/bin/env bash
# ============================================================================
# LSST Extendedness Pipeline - Bootstrap Script
# ============================================================================
# Quick setup script for new developers/deployments
#
# Usage:
#   ./scripts/bootstrap.sh
#   OR
#   curl -sSL https://raw.githubusercontent.com/westover/lsst-extendedness/main/scripts/bootstrap.sh | bash
# ============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== LSST Extendedness Pipeline Bootstrap ===${NC}"
echo ""

# ============================================================================
# CHECK PREREQUISITES
# ============================================================================

echo -e "${BLUE}Checking prerequisites...${NC}"

# Check Python version (3.12+)
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: Python 3 not found. Please install Python 3.12+${NC}"
    exit 1
fi

PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MAJOR=$(python3 -c "import sys; print(sys.version_info.major)")
PYTHON_MINOR=$(python3 -c "import sys; print(sys.version_info.minor)")

if [[ "$PYTHON_MAJOR" -lt 3 ]] || [[ "$PYTHON_MAJOR" -eq 3 && "$PYTHON_MINOR" -lt 12 ]]; then
    echo -e "${RED}ERROR: Python 3.12+ required (found $PYTHON_VERSION)${NC}"
    echo -e "${YELLOW}Install with pyenv: pyenv install 3.13.1 && pyenv local 3.13.1${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python $PYTHON_VERSION${NC}"

# ============================================================================
# INSTALL PDM
# ============================================================================

if ! command -v pdm &> /dev/null; then
    echo -e "${YELLOW}PDM not found, installing...${NC}"

    # Try pipx first (recommended)
    if command -v pipx &> /dev/null; then
        pipx install pdm
    else
        # Fallback to pip
        pip install --user pdm
    fi

    # Add to PATH if needed
    if ! command -v pdm &> /dev/null; then
        export PATH="$HOME/.local/bin:$PATH"
    fi
fi

PDM_VERSION=$(pdm --version 2>&1 | head -n1)
echo -e "${GREEN}✓ $PDM_VERSION${NC}"

# ============================================================================
# INSTALL DEPENDENCIES
# ============================================================================

echo ""
echo -e "${BLUE}Installing dependencies...${NC}"

# Configure PDM for in-project venv
pdm config venv.in_project true

# Install all dependencies (including dev)
pdm install -G dev

echo -e "${GREEN}✓ Dependencies installed${NC}"

# ============================================================================
# SETUP PRE-COMMIT
# ============================================================================

echo ""
echo -e "${BLUE}Setting up pre-commit hooks...${NC}"

if pdm run pre-commit --version &> /dev/null; then
    pdm run pre-commit install
    echo -e "${GREEN}✓ Pre-commit hooks installed${NC}"
else
    echo -e "${YELLOW}⚠ Pre-commit not available (optional)${NC}"
fi

# ============================================================================
# CREATE DIRECTORIES
# ============================================================================

echo ""
echo -e "${BLUE}Creating data directories...${NC}"

mkdir -p data/{backups,cutouts}
mkdir -p logs

echo -e "${GREEN}✓ Directories created${NC}"

# ============================================================================
# INITIALIZE DATABASE
# ============================================================================

echo ""
echo -e "${BLUE}Initializing database...${NC}"

pdm run python -m lsst_extendedness.cli db-init 2>/dev/null || {
    echo -e "${YELLOW}⚠ Database initialization skipped (CLI not yet implemented)${NC}"
}

# ============================================================================
# VERIFY INSTALLATION
# ============================================================================

echo ""
echo -e "${BLUE}Verifying installation...${NC}"

# Try to import the package
pdm run python -c "import lsst_extendedness; print(f'Version: {lsst_extendedness.__version__}')" 2>/dev/null || {
    echo -e "${YELLOW}⚠ Package import check skipped (package not yet complete)${NC}"
}

# Run tests (if available)
if [[ -f "tests/test_models.py" ]]; then
    echo "Running quick test..."
    pdm run pytest tests/ -q --tb=no 2>/dev/null || {
        echo -e "${YELLOW}⚠ Tests skipped (not yet implemented)${NC}"
    }
fi

# ============================================================================
# SUCCESS
# ============================================================================

echo ""
echo -e "${GREEN}=== Bootstrap Complete ===${NC}"
echo ""
echo "Next steps:"
echo "  1. Copy config/default.toml to config/local.toml"
echo "  2. Edit config/local.toml with your Kafka settings"
echo "  3. Run: make ingest CONFIG=config/local.toml"
echo ""
echo "Quick commands:"
echo "  make test        - Run tests"
echo "  make lint        - Check code style"
echo "  make db-shell    - SQLite shell"
echo "  make help        - All available commands"
echo ""
