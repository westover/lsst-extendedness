#!/usr/bin/env bash
# ============================================================================
# LSST Extendedness Pipeline - System Dependencies Setup
# ============================================================================
# Install system-level dependencies (requires sudo)
#
# Supports:
#   - Ubuntu/Debian (apt)
#   - macOS (Homebrew)
#   - RHEL/CentOS/Fedora (dnf/yum)
#
# Usage: sudo ./scripts/setup-system.sh
# ============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== Installing System Dependencies ===${NC}"
echo ""

# Detect OS
if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    OS=$ID
elif [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macos"
else
    echo -e "${RED}ERROR: Unsupported operating system${NC}"
    exit 1
fi

echo -e "${BLUE}Detected OS: $OS${NC}"
echo ""

# ============================================================================
# UBUNTU / DEBIAN
# ============================================================================

install_debian() {
    echo -e "${BLUE}Installing packages via apt...${NC}"

    apt-get update

    apt-get install -y \
        python3.12 \
        python3.12-venv \
        python3.12-dev \
        build-essential \
        librdkafka-dev \
        libffi-dev \
        libssl-dev \
        sqlite3 \
        libsqlite3-dev \
        git \
        curl \
        # OpenBLAS for numpy/pandas acceleration
        libopenblas-dev \
        liblapack-dev

    # Optional: Install pipx for PDM
    apt-get install -y pipx || {
        echo -e "${YELLOW}pipx not available, will use pip for PDM${NC}"
    }

    echo -e "${GREEN}✓ Debian/Ubuntu packages installed${NC}"
}

# ============================================================================
# RHEL / CENTOS / FEDORA
# ============================================================================

install_rhel() {
    echo -e "${BLUE}Installing packages via dnf/yum...${NC}"

    # Detect package manager
    if command -v dnf &> /dev/null; then
        PKG_MGR="dnf"
    else
        PKG_MGR="yum"
    fi

    $PKG_MGR install -y \
        python3.12 \
        python3.12-devel \
        gcc \
        gcc-c++ \
        make \
        librdkafka-devel \
        libffi-devel \
        openssl-devel \
        sqlite \
        sqlite-devel \
        git \
        curl \
        openblas-devel \
        lapack-devel

    echo -e "${GREEN}✓ RHEL/CentOS/Fedora packages installed${NC}"
}

# ============================================================================
# MACOS
# ============================================================================

install_macos() {
    echo -e "${BLUE}Installing packages via Homebrew...${NC}"

    # Check for Homebrew
    if ! command -v brew &> /dev/null; then
        echo -e "${RED}ERROR: Homebrew not found. Install from https://brew.sh${NC}"
        exit 1
    fi

    brew install \
        python@3.13 \
        librdkafka \
        openssl@3 \
        sqlite \
        openblas

    # Link OpenBLAS for numpy
    echo -e "${YELLOW}Setting up OpenBLAS for numpy acceleration...${NC}"
    OPENBLAS_PATH=$(brew --prefix openblas)
    echo "export OPENBLAS=$OPENBLAS_PATH" >> ~/.zshrc 2>/dev/null || true
    echo "export OPENBLAS=$OPENBLAS_PATH" >> ~/.bashrc 2>/dev/null || true

    echo -e "${GREEN}✓ macOS packages installed${NC}"
    echo -e "${YELLOW}Note: Restart your shell or run 'source ~/.zshrc' for OPENBLAS${NC}"
}

# ============================================================================
# MAIN
# ============================================================================

case "$OS" in
    ubuntu|debian)
        install_debian
        ;;
    fedora|rhel|centos)
        install_rhel
        ;;
    macos)
        install_macos
        ;;
    *)
        echo -e "${RED}ERROR: Unsupported OS: $OS${NC}"
        echo "Please install manually:"
        echo "  - Python 3.12+"
        echo "  - librdkafka (Kafka C library)"
        echo "  - OpenBLAS (for numpy acceleration)"
        echo "  - SQLite 3"
        exit 1
        ;;
esac

# ============================================================================
# VERIFY
# ============================================================================

echo ""
echo -e "${BLUE}Verifying installations...${NC}"

# Python
PYTHON_VERSION=$(python3 --version 2>&1 || python3.12 --version 2>&1 || python3.13 --version 2>&1)
echo -e "${GREEN}✓ $PYTHON_VERSION${NC}"

# SQLite
SQLITE_VERSION=$(sqlite3 --version 2>&1 | cut -d' ' -f1)
echo -e "${GREEN}✓ SQLite $SQLITE_VERSION${NC}"

# librdkafka
if pkg-config --exists rdkafka 2>/dev/null; then
    KAFKA_VERSION=$(pkg-config --modversion rdkafka)
    echo -e "${GREEN}✓ librdkafka $KAFKA_VERSION${NC}"
else
    echo -e "${YELLOW}⚠ librdkafka version check skipped${NC}"
fi

echo ""
echo -e "${GREEN}=== System Dependencies Installed ===${NC}"
echo ""
echo "Next steps:"
echo "  1. Run: ./scripts/bootstrap.sh"
echo "  2. Or manually: pdm install"
echo ""
