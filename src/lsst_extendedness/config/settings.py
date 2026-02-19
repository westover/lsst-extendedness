"""
Configuration settings for the LSST Extendedness Pipeline.

This module handles loading and validating configuration from TOML files
and environment variables.

Configuration hierarchy (later overrides earlier):
1. Default values (built-in)
2. config/default.toml
3. config/local.toml (gitignored)
4. Environment variables (LSST_* prefix)
5. Command-line arguments

Example:
    >>> from lsst_extendedness.config import get_settings
    >>>
    >>> settings = get_settings()
    >>> print(f"Database: {settings.database_path}")
    >>> print(f"Kafka topic: {settings.kafka.topic}")
"""

from __future__ import annotations

import os
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class KafkaSettings(BaseModel):
    """Kafka connection settings."""

    model_config = ConfigDict(extra="ignore")

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers",
    )
    topic: str = Field(
        default="lsst-extendedness-filtered",
        description="Topic to consume from",
    )
    group_id: str = Field(
        default="lsst-extendedness-consumer",
        description="Consumer group ID",
    )
    auto_offset_reset: str = Field(
        default="earliest",
        description="Where to start reading",
    )
    enable_auto_commit: bool = Field(
        default=True,
        description="Enable auto-commit",
    )
    profile: str = Field(
        default="default",
        description="Kafka profile from kafka_profiles.toml",
    )

    def to_consumer_config(self) -> dict[str, Any]:
        """Convert to confluent-kafka consumer config dict."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
        }


class DatabaseSettings(BaseModel):
    """Database settings."""

    model_config = ConfigDict(extra="ignore")

    path: str = Field(
        default="lsst_extendedness.db",
        description="Database file path",
    )
    timeout_seconds: float = Field(
        default=30.0,
        description="Connection timeout",
    )
    wal_mode: bool = Field(
        default=True,
        description="Enable WAL mode",
    )


class IngestionSettings(BaseModel):
    """Ingestion settings."""

    model_config = ConfigDict(extra="ignore")

    duration_seconds: int | None = Field(
        default=3600,
        description="Maximum runtime (None = indefinite)",
    )
    max_messages: int | None = Field(
        default=10000,
        description="Maximum messages (None = unlimited)",
    )
    batch_size: int = Field(
        default=100,
        description="Batch size for database writes",
    )
    extract_cutouts: bool = Field(
        default=True,
        description="Extract FITS cutouts",
    )


class LoggingSettings(BaseModel):
    """Logging settings."""

    model_config = ConfigDict(extra="ignore")

    level: str = Field(default="INFO", description="Log level")
    format: str = Field(default="console", description="Output format")
    include_timestamp: bool = Field(default=True)
    include_location: bool = Field(default=False)


class Settings(BaseModel):
    """Main settings container."""

    model_config = ConfigDict(extra="ignore")

    # General
    name: str = Field(default="lsst-extendedness")
    base_dir: Path = Field(default=Path("data"))

    # Subsystems
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    @property
    def database_path(self) -> Path:
        """Get absolute database path."""
        db_path = Path(self.database.path)
        if db_path.is_absolute():
            return db_path
        return self.base_dir / db_path

    @property
    def cutouts_dir(self) -> Path:
        """Get cutouts directory."""
        return self.base_dir / "cutouts"

    @property
    def logs_dir(self) -> Path:
        """Get logs directory."""
        return self.base_dir.parent / "logs"


def _find_config_files() -> list[Path]:
    """Find configuration files in standard locations.

    Returns:
        List of config file paths (in order of priority)
    """
    files = []

    # Check current directory
    cwd = Path.cwd()
    for name in ["config/default.toml", "config/local.toml"]:
        path = cwd / name
        if path.exists():
            files.append(path)

    # Check environment variable
    env_config = os.environ.get("LSST_CONFIG_PATH")
    if env_config:
        path = Path(env_config)
        if path.exists():
            files.append(path)

    return files


def _load_toml(path: Path) -> dict[str, Any]:
    """Load a TOML file.

    Args:
        path: Path to TOML file

    Returns:
        Parsed TOML content
    """
    with open(path, "rb") as f:
        return tomllib.load(f)


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge dictionaries.

    Args:
        base: Base dictionary
        override: Override dictionary

    Returns:
        Merged dictionary
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = value

    return result


def _apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    """Apply environment variable overrides.

    Environment variables with LSST_ prefix override config values.
    Example: LSST_DATABASE_PATH -> database.path

    Args:
        config: Configuration dictionary

    Returns:
        Modified configuration
    """
    prefix = "LSST_"

    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue

        # Remove prefix and convert to lowercase
        config_key = key[len(prefix) :].lower()

        # Handle nested keys (e.g., LSST_KAFKA_TOPIC -> kafka.topic)
        parts = config_key.split("_")

        # Try to find matching nested key
        current = config
        for i, part in enumerate(parts[:-1]):
            if part in current and isinstance(current[part], dict):
                current = current[part]
            else:
                # Try combining remaining parts
                remaining = "_".join(parts[i:])
                if remaining in current:
                    current[remaining] = value
                    break
        else:
            # Set the final value
            final_key = parts[-1]
            if final_key in current:
                # Try to preserve type
                original = current[final_key]
                if isinstance(original, bool):
                    current[final_key] = value.lower() in ("true", "1", "yes")
                elif isinstance(original, int):
                    current[final_key] = int(value)
                elif isinstance(original, float):
                    current[final_key] = float(value)
                else:
                    current[final_key] = value

    return config


def load_settings(config_path: str | Path | None = None) -> Settings:
    """Load settings from configuration files.

    Args:
        config_path: Optional explicit config file path

    Returns:
        Settings instance
    """
    # Start with empty config
    config: dict[str, Any] = {}

    # Find and load config files
    if config_path:
        files = [Path(config_path)]
    else:
        files = _find_config_files()

    for path in files:
        file_config = _load_toml(path)
        config = _merge_dicts(config, file_config)

    # Apply environment overrides
    config = _apply_env_overrides(config)

    # Create Settings instance
    return Settings(**config)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get cached settings instance.

    Returns:
        Settings instance (cached after first call)

    Example:
        >>> settings = get_settings()
        >>> print(settings.database_path)
    """
    return load_settings()


def reload_settings() -> Settings:
    """Reload settings (clears cache).

    Returns:
        Fresh Settings instance
    """
    get_settings.cache_clear()
    return get_settings()
