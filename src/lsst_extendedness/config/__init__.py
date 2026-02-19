"""
Configuration management for the LSST Extendedness Pipeline.

This module handles loading and validating configuration from TOML files:

Configuration hierarchy:
1. Default values (built-in)
2. config/default.toml (project defaults)
3. config/local.toml (user overrides, gitignored)
4. Environment variables (LSST_* prefix)
5. Command-line arguments

Example:
    >>> from lsst_extendedness.config import get_settings
    >>>
    >>> settings = get_settings()
    >>> print(f"Database: {settings.database_path}")
    >>> print(f"Kafka topic: {settings.kafka.topic}")

Configuration files use TOML format. See config/default.toml for all options.
"""

from lsst_extendedness.config.settings import KafkaSettings, Settings, get_settings

__all__ = [
    "KafkaSettings",
    "Settings",
    "get_settings",
]
