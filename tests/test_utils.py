"""
Tests for utility modules.
"""

from __future__ import annotations

from datetime import UTC, datetime

from lsst_extendedness.utils.logging import (
    bind_context,
    clear_context,
    get_logger,
    setup_logging,
)
from lsst_extendedness.utils.time import (
    JD_MJD_OFFSET,
    JD_UNIX_EPOCH,
    current_mjd,
    datetime_to_mjd,
    days_ago_mjd,
    mjd_to_datetime,
)


class TestDatetimeToMJD:
    """Tests for datetime_to_mjd conversion."""

    def test_known_date(self):
        """Test conversion of a known date."""
        # January 1, 2000, 12:00:00 UTC is MJD 51544.5
        dt = datetime(2000, 1, 1, 12, 0, 0)
        mjd = datetime_to_mjd(dt)
        assert abs(mjd - 51544.5) < 0.0001

    def test_unix_epoch(self):
        """Test conversion of Unix epoch."""
        # January 1, 1970, 00:00:00 UTC
        dt = datetime(1970, 1, 1, 0, 0, 0)
        mjd = datetime_to_mjd(dt)
        expected_mjd = JD_UNIX_EPOCH - JD_MJD_OFFSET
        assert abs(mjd - expected_mjd) < 0.0001

    def test_timezone_aware_datetime(self):
        """Test conversion of timezone-aware datetime."""
        dt_utc = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        dt_naive = datetime(2024, 6, 15, 12, 0, 0)

        mjd_utc = datetime_to_mjd(dt_utc)
        mjd_naive = datetime_to_mjd(dt_naive)

        # Should produce the same result
        assert abs(mjd_utc - mjd_naive) < 0.0001

    def test_fractional_day(self):
        """Test that fractional days are handled correctly."""
        dt_noon = datetime(2024, 1, 1, 12, 0, 0)
        dt_midnight = datetime(2024, 1, 1, 0, 0, 0)

        mjd_noon = datetime_to_mjd(dt_noon)
        mjd_midnight = datetime_to_mjd(dt_midnight)

        # Noon should be 0.5 days after midnight
        assert abs((mjd_noon - mjd_midnight) - 0.5) < 0.0001


class TestMJDToDatetime:
    """Tests for mjd_to_datetime conversion."""

    def test_known_mjd(self):
        """Test conversion of a known MJD."""
        # MJD 51544.5 is January 1, 2000, 12:00:00 UTC
        mjd = 51544.5
        dt = mjd_to_datetime(mjd)

        assert dt.year == 2000
        assert dt.month == 1
        assert dt.day == 1
        assert dt.hour == 12
        assert dt.minute == 0

    def test_roundtrip(self):
        """Test that datetime -> MJD -> datetime roundtrip is accurate."""
        original = datetime(2024, 7, 15, 18, 30, 45)
        mjd = datetime_to_mjd(original)
        recovered = mjd_to_datetime(mjd)

        # Should be within 1 second
        diff = abs((recovered - original).total_seconds())
        assert diff < 1

    def test_recent_date(self):
        """Test conversion of a recent date."""
        # Use a date in 2024
        mjd = 60400.0  # Approximately mid-2024
        dt = mjd_to_datetime(mjd)

        assert dt.year == 2024
        assert 1 <= dt.month <= 12


class TestDaysAgoMJD:
    """Tests for days_ago_mjd function."""

    def test_zero_days(self):
        """Test that 0 days ago returns approximately current MJD."""
        mjd_now = current_mjd()
        mjd_zero_ago = days_ago_mjd(0)

        # Should be very close (within 1 second)
        assert abs(mjd_now - mjd_zero_ago) < 1 / 86400

    def test_one_day_ago(self):
        """Test that 1 day ago is MJD - 1."""
        mjd_now = current_mjd()
        mjd_one_ago = days_ago_mjd(1)

        # Should be approximately 1 MJD less
        assert abs((mjd_now - mjd_one_ago) - 1) < 0.001

    def test_ninety_days_ago(self):
        """Test that 90 days ago is MJD - 90."""
        mjd_now = current_mjd()
        mjd_90_ago = days_ago_mjd(90)

        # Should be approximately 90 MJD less
        assert abs((mjd_now - mjd_90_ago) - 90) < 0.001


class TestCurrentMJD:
    """Tests for current_mjd function."""

    def test_returns_float(self):
        """Test that current_mjd returns a float."""
        mjd = current_mjd()
        assert isinstance(mjd, float)

    def test_reasonable_range(self):
        """Test that current MJD is in a reasonable range."""
        mjd = current_mjd()
        # MJD for dates in 2024-2030 should be roughly 60000-62000
        assert 60000 < mjd < 65000

    def test_increases_over_time(self):
        """Test that MJD increases over time (monotonic)."""
        import time

        mjd1 = current_mjd()
        time.sleep(0.01)  # 10ms
        mjd2 = current_mjd()

        assert mjd2 >= mjd1


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_console_format(self):
        """Test setting up console logging."""
        setup_logging(level="INFO", format="console")
        logger = get_logger("test")
        assert logger is not None

    def test_setup_json_format(self):
        """Test setting up JSON logging."""
        setup_logging(level="DEBUG", format="json")
        logger = get_logger("test")
        assert logger is not None

    def test_setup_with_timestamp(self):
        """Test setup with timestamp enabled."""
        setup_logging(level="INFO", include_timestamp=True)
        logger = get_logger("test")
        assert logger is not None

    def test_setup_with_location(self):
        """Test setup with source location enabled."""
        setup_logging(level="INFO", include_location=True)
        logger = get_logger("test")
        assert logger is not None

    def test_setup_all_options(self):
        """Test setup with all options enabled."""
        setup_logging(
            level="WARNING",
            format="console",
            include_timestamp=True,
            include_location=True,
        )
        logger = get_logger("test")
        assert logger is not None


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_with_name(self):
        """Test getting a logger with a specific name."""
        setup_logging()
        logger = get_logger("my_module")
        assert logger is not None

    def test_get_logger_without_name(self):
        """Test getting a logger without a name."""
        setup_logging()
        logger = get_logger()
        assert logger is not None

    def test_logger_can_log(self):
        """Test that the logger can log messages."""
        setup_logging(level="DEBUG")
        logger = get_logger("test_logger")
        # These should not raise
        logger.debug("Debug message")
        logger.info("Info message", extra_field="value")
        logger.warning("Warning message")


class TestContextBindings:
    """Tests for context binding functions."""

    def test_bind_context(self):
        """Test binding context variables."""
        setup_logging()
        bind_context(run_id="test123", source="test")
        # Should not raise
        logger = get_logger("test")
        logger.info("Test with context")

    def test_clear_context(self):
        """Test clearing context variables."""
        setup_logging()
        bind_context(run_id="test123")
        clear_context()
        # Should not raise
        logger = get_logger("test")
        logger.info("Test after clear")
