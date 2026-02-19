"""
Tests for configuration module.
"""

from __future__ import annotations

from pathlib import Path

from lsst_extendedness.config.settings import (
    DatabaseSettings,
    IngestionSettings,
    KafkaSettings,
    LoggingSettings,
    Settings,
    _apply_env_overrides,
    _find_config_files,
    _load_toml,
    _merge_dicts,
    get_settings,
    load_settings,
    reload_settings,
)


class TestKafkaSettings:
    """Tests for KafkaSettings."""

    def test_default_values(self):
        """Test default Kafka settings."""
        settings = KafkaSettings()

        assert settings.bootstrap_servers == "localhost:9092"
        assert settings.topic == "lsst-extendedness-filtered"
        assert settings.group_id == "lsst-extendedness-consumer"
        assert settings.auto_offset_reset == "earliest"
        assert settings.enable_auto_commit is True
        assert settings.profile == "default"

    def test_custom_values(self):
        """Test custom Kafka settings."""
        settings = KafkaSettings(
            bootstrap_servers="kafka.example.com:9092",
            topic="custom-topic",
            group_id="custom-group",
        )

        assert settings.bootstrap_servers == "kafka.example.com:9092"
        assert settings.topic == "custom-topic"
        assert settings.group_id == "custom-group"

    def test_to_consumer_config(self):
        """Test conversion to consumer config dict."""
        settings = KafkaSettings(
            bootstrap_servers="kafka:9092",
            group_id="test-group",
        )

        config = settings.to_consumer_config()

        assert config["bootstrap.servers"] == "kafka:9092"
        assert config["group.id"] == "test-group"
        assert "auto.offset.reset" in config
        assert "enable.auto.commit" in config


class TestDatabaseSettings:
    """Tests for DatabaseSettings."""

    def test_default_values(self):
        """Test default database settings."""
        settings = DatabaseSettings()

        assert settings.path == "lsst_extendedness.db"
        assert settings.timeout_seconds == 30.0
        assert settings.wal_mode is True

    def test_custom_values(self):
        """Test custom database settings."""
        settings = DatabaseSettings(
            path="/data/custom.db",
            timeout_seconds=60.0,
            wal_mode=False,
        )

        assert settings.path == "/data/custom.db"
        assert settings.timeout_seconds == 60.0
        assert settings.wal_mode is False


class TestIngestionSettings:
    """Tests for IngestionSettings."""

    def test_default_values(self):
        """Test default ingestion settings."""
        settings = IngestionSettings()

        assert settings.duration_seconds == 3600
        assert settings.max_messages == 10000
        assert settings.batch_size == 100
        assert settings.extract_cutouts is True

    def test_unlimited_settings(self):
        """Test settings with unlimited duration and messages."""
        settings = IngestionSettings(
            duration_seconds=None,
            max_messages=None,
        )

        assert settings.duration_seconds is None
        assert settings.max_messages is None


class TestLoggingSettings:
    """Tests for LoggingSettings."""

    def test_default_values(self):
        """Test default logging settings."""
        settings = LoggingSettings()

        assert settings.level == "INFO"
        assert settings.format == "console"
        assert settings.include_timestamp is True
        assert settings.include_location is False

    def test_custom_values(self):
        """Test custom logging settings."""
        settings = LoggingSettings(
            level="DEBUG",
            format="json",
            include_timestamp=False,
            include_location=True,
        )

        assert settings.level == "DEBUG"
        assert settings.format == "json"
        assert settings.include_timestamp is False
        assert settings.include_location is True


class TestSettings:
    """Tests for main Settings class."""

    def test_default_values(self):
        """Test default settings."""
        settings = Settings()

        assert settings.name == "lsst-extendedness"
        assert settings.base_dir == Path("data")
        assert isinstance(settings.kafka, KafkaSettings)
        assert isinstance(settings.database, DatabaseSettings)
        assert isinstance(settings.ingestion, IngestionSettings)
        assert isinstance(settings.logging, LoggingSettings)

    def test_database_path_relative(self):
        """Test database_path property with relative path."""
        settings = Settings(
            base_dir=Path("/opt/lsst"),
            database=DatabaseSettings(path="alerts.db"),
        )

        assert settings.database_path == Path("/opt/lsst/alerts.db")

    def test_database_path_absolute(self):
        """Test database_path property with absolute path."""
        settings = Settings(
            database=DatabaseSettings(path="/data/alerts.db"),
        )

        assert settings.database_path == Path("/data/alerts.db")

    def test_cutouts_dir(self):
        """Test cutouts_dir property."""
        settings = Settings(base_dir=Path("/opt/lsst"))

        assert settings.cutouts_dir == Path("/opt/lsst/cutouts")

    def test_logs_dir(self):
        """Test logs_dir property."""
        settings = Settings(base_dir=Path("/opt/lsst/data"))

        assert settings.logs_dir == Path("/opt/lsst/logs")

    def test_nested_config(self):
        """Test creating settings with nested config."""
        settings = Settings(
            kafka=KafkaSettings(topic="custom-topic"),
            database=DatabaseSettings(path="custom.db"),
        )

        assert settings.kafka.topic == "custom-topic"
        assert settings.database.path == "custom.db"


class TestMergeDicts:
    """Tests for _merge_dicts helper function."""

    def test_simple_merge(self):
        """Test simple dictionary merge."""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}

        result = _merge_dicts(base, override)

        assert result == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self):
        """Test nested dictionary merge."""
        base = {"a": {"x": 1, "y": 2}, "b": 3}
        override = {"a": {"y": 10, "z": 20}}

        result = _merge_dicts(base, override)

        assert result == {"a": {"x": 1, "y": 10, "z": 20}, "b": 3}

    def test_base_unchanged(self):
        """Test that base dict is not modified."""
        base = {"a": 1}
        override = {"b": 2}

        _merge_dicts(base, override)

        assert base == {"a": 1}

    def test_empty_override(self):
        """Test merge with empty override."""
        base = {"a": 1, "b": 2}
        override = {}

        result = _merge_dicts(base, override)

        assert result == {"a": 1, "b": 2}

    def test_empty_base(self):
        """Test merge with empty base."""
        base = {}
        override = {"a": 1}

        result = _merge_dicts(base, override)

        assert result == {"a": 1}


class TestFindConfigFiles:
    """Tests for _find_config_files function."""

    def test_no_config_files(self, tmp_path, monkeypatch):
        """Test when no config files exist."""
        monkeypatch.chdir(tmp_path)
        # Clear env var
        monkeypatch.delenv("LSST_CONFIG_PATH", raising=False)

        files = _find_config_files()

        assert files == []

    def test_default_config_exists(self, tmp_path, monkeypatch):
        """Test finding default.toml."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LSST_CONFIG_PATH", raising=False)

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        default_file = config_dir / "default.toml"
        default_file.write_text('[database]\npath = "test.db"\n')

        files = _find_config_files()

        assert len(files) == 1
        assert files[0] == default_file

    def test_local_config_exists(self, tmp_path, monkeypatch):
        """Test finding local.toml."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LSST_CONFIG_PATH", raising=False)

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        local_file = config_dir / "local.toml"
        local_file.write_text('[database]\npath = "local.db"\n')

        files = _find_config_files()

        assert len(files) == 1
        assert files[0] == local_file

    def test_both_configs_exist(self, tmp_path, monkeypatch):
        """Test finding both config files."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LSST_CONFIG_PATH", raising=False)

        config_dir = tmp_path / "config"
        config_dir.mkdir()
        default_file = config_dir / "default.toml"
        default_file.write_text('[database]\npath = "default.db"\n')
        local_file = config_dir / "local.toml"
        local_file.write_text('[database]\npath = "local.db"\n')

        files = _find_config_files()

        assert len(files) == 2
        assert default_file in files
        assert local_file in files

    def test_env_config_path(self, tmp_path, monkeypatch):
        """Test LSST_CONFIG_PATH environment variable."""
        monkeypatch.chdir(tmp_path)

        env_file = tmp_path / "env_config.toml"
        env_file.write_text('[database]\npath = "env.db"\n')
        monkeypatch.setenv("LSST_CONFIG_PATH", str(env_file))

        files = _find_config_files()

        assert env_file in files

    def test_env_config_path_nonexistent(self, tmp_path, monkeypatch):
        """Test LSST_CONFIG_PATH with nonexistent file."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("LSST_CONFIG_PATH", "/nonexistent/config.toml")

        files = _find_config_files()

        assert Path("/nonexistent/config.toml") not in files


class TestLoadToml:
    """Tests for _load_toml function."""

    def test_load_simple_toml(self, tmp_path):
        """Test loading simple TOML file."""
        toml_file = tmp_path / "config.toml"
        toml_file.write_text(
            """
[database]
path = "test.db"
timeout_seconds = 60.0

[kafka]
topic = "test-topic"
"""
        )

        result = _load_toml(toml_file)

        assert result["database"]["path"] == "test.db"
        assert result["database"]["timeout_seconds"] == 60.0
        assert result["kafka"]["topic"] == "test-topic"

    def test_load_nested_toml(self, tmp_path):
        """Test loading nested TOML."""
        toml_file = tmp_path / "config.toml"
        toml_file.write_text(
            """
[section.subsection]
key = "value"
"""
        )

        result = _load_toml(toml_file)

        assert result["section"]["subsection"]["key"] == "value"

    def test_load_empty_toml(self, tmp_path):
        """Test loading empty TOML file."""
        toml_file = tmp_path / "empty.toml"
        toml_file.write_text("")

        result = _load_toml(toml_file)

        assert result == {}


class TestApplyEnvOverrides:
    """Tests for _apply_env_overrides function."""

    def test_no_lsst_env_vars(self, monkeypatch):
        """Test with no LSST_ environment variables."""
        monkeypatch.delenv("LSST_DATABASE_PATH", raising=False)

        config = {"database": {"path": "original.db"}}
        result = _apply_env_overrides(config)

        assert result["database"]["path"] == "original.db"

    def test_simple_override(self, monkeypatch):
        """Test simple nested override."""
        monkeypatch.setenv("LSST_DATABASE_PATH", "override.db")

        config = {"database": {"path": "original.db"}}
        result = _apply_env_overrides(config)

        assert result["database"]["path"] == "override.db"

    def test_boolean_override(self, monkeypatch):
        """Test boolean value override.

        Note: Keys with underscores (like wal_mode) are handled
        via the 'remaining' path which combines underscore-separated parts.
        """
        # Use a key at the top level to test boolean conversion
        monkeypatch.setenv("LSST_ENABLED", "false")

        config = {"enabled": True}
        result = _apply_env_overrides(config)

        assert result["enabled"] is False

    def test_boolean_override_true(self, monkeypatch):
        """Test boolean value override with true."""
        monkeypatch.setenv("LSST_ENABLED", "yes")

        config = {"enabled": False}
        result = _apply_env_overrides(config)

        assert result["enabled"] is True

    def test_int_override(self, monkeypatch):
        """Test integer value override."""
        # Use nested key with single-word final key
        monkeypatch.setenv("LSST_INGESTION_LIMIT", "500")

        config = {"ingestion": {"limit": 100}}
        result = _apply_env_overrides(config)

        assert result["ingestion"]["limit"] == 500

    def test_float_override(self, monkeypatch):
        """Test float value override."""
        monkeypatch.setenv("LSST_DATABASE_TIMEOUT", "120.5")

        config = {"database": {"timeout": 30.0}}
        result = _apply_env_overrides(config)

        assert result["database"]["timeout"] == 120.5

    def test_nonexistent_key_ignored(self, monkeypatch):
        """Test that nonexistent keys are not added."""
        monkeypatch.setenv("LSST_NONEXISTENT_KEY", "value")

        config = {"database": {"path": "test.db"}}
        result = _apply_env_overrides(config)

        # Should not add new key
        assert "nonexistent" not in result


class TestLoadSettings:
    """Tests for load_settings function."""

    def test_load_defaults(self, tmp_path, monkeypatch):
        """Test loading with defaults only."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LSST_CONFIG_PATH", raising=False)

        settings = load_settings()

        assert isinstance(settings, Settings)
        assert settings.name == "lsst-extendedness"

    def test_load_from_explicit_path(self, tmp_path):
        """Test loading from explicit config path."""
        config_file = tmp_path / "explicit.toml"
        config_file.write_text(
            """
name = "custom-name"

[database]
path = "custom.db"
"""
        )

        settings = load_settings(config_file)

        assert settings.name == "custom-name"
        assert settings.database.path == "custom.db"

    def test_load_merges_configs(self, tmp_path, monkeypatch):
        """Test that configs are merged correctly."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LSST_CONFIG_PATH", raising=False)

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Default has database settings
        default = config_dir / "default.toml"
        default.write_text(
            """
[database]
path = "default.db"
timeout_seconds = 30.0
"""
        )

        # Local overrides path only
        local = config_dir / "local.toml"
        local.write_text(
            """
[database]
path = "local.db"
"""
        )

        settings = load_settings()

        assert settings.database.path == "local.db"
        assert settings.database.timeout_seconds == 30.0  # From default

    def test_load_with_env_override(self, tmp_path, monkeypatch):
        """Test that environment variables override config."""
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            """
[database]
path = "file.db"
"""
        )

        monkeypatch.setenv("LSST_DATABASE_PATH", "env.db")

        settings = load_settings(config_file)

        assert settings.database.path == "env.db"


class TestGetSettings:
    """Tests for get_settings function."""

    def test_get_settings_cached(self, tmp_path, monkeypatch):
        """Test that get_settings returns cached instance."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LSST_CONFIG_PATH", raising=False)

        # Clear cache first
        get_settings.cache_clear()

        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2


class TestReloadSettings:
    """Tests for reload_settings function."""

    def test_reload_clears_cache(self, tmp_path, monkeypatch):
        """Test that reload_settings clears cache."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LSST_CONFIG_PATH", raising=False)

        # Clear cache and get settings
        get_settings.cache_clear()
        settings1 = get_settings()

        # Reload should return new instance
        settings2 = reload_settings()

        # After reload, cache should have new instance
        settings3 = get_settings()

        assert settings1 is not settings2
        assert settings2 is settings3
