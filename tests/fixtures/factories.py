"""
Test factories for generating alert data.

This module provides factory classes for creating test data
with sensible defaults and optional overrides.

Example:
    >>> from tests.fixtures import AlertFactory
    >>>
    >>> # Create a single alert
    >>> alert = AlertFactory.create(ra=180.0, dec=45.0)
    >>>
    >>> # Create multiple alerts
    >>> alerts = AlertFactory.create_batch(100)
    >>>
    >>> # Create specific types
    >>> point_source = AlertFactory.create_point_source()
    >>> minimoon = AlertFactory.create_minimoon_candidate()
"""

from __future__ import annotations

from datetime import datetime

from lsst_extendedness.models import AlertRecord


class AlertFactory:
    """Factory for creating test AlertRecord instances.

    Provides methods for creating alerts with sensible defaults,
    with support for overriding any field.

    Attributes:
        _counter: Internal counter for unique IDs

    Example:
        >>> alert = AlertFactory.create(snr=150.0)
        >>> assert alert.snr == 150.0
        >>> assert alert.alert_id > 0  # Auto-generated
    """

    _counter: int = 0

    @classmethod
    def reset(cls) -> None:
        """Reset the counter to 0."""
        cls._counter = 0

    @classmethod
    def create(cls, **overrides) -> AlertRecord:
        """Create a single alert with optional field overrides.

        Args:
            **overrides: Field values to override defaults

        Returns:
            AlertRecord instance
        """
        cls._counter += 1

        defaults = {
            "alert_id": 1000000 + cls._counter,
            "dia_source_id": 2000000 + cls._counter,
            "dia_object_id": 3000000 + cls._counter,
            "ra": 180.0 + (cls._counter * 0.001),
            "dec": 45.0 + (cls._counter * 0.001),
            "mjd": 60000.0 + (cls._counter * 0.01),
            "ingested_at": datetime.utcnow(),
            "filter_name": "g",
            "ps_flux": 1000.0,
            "ps_flux_err": 10.0,
            "snr": 100.0,
            "extendedness_median": 0.5,
            "extendedness_min": 0.45,
            "extendedness_max": 0.55,
            "has_ss_source": False,
            "ss_object_id": None,
            "ss_object_reassoc_time_mjd": None,
            "is_reassociation": False,
            "reassociation_reason": None,
            "trail_data": {},
            "pixel_flags": {},
            "science_cutout_path": None,
            "template_cutout_path": None,
            "difference_cutout_path": None,
        }

        # Apply overrides
        defaults.update(overrides)

        return AlertRecord(**defaults)

    @classmethod
    def create_batch(cls, count: int, **overrides) -> list[AlertRecord]:
        """Create multiple alerts.

        Args:
            count: Number of alerts to create
            **overrides: Field values to apply to all alerts

        Returns:
            List of AlertRecord instances
        """
        return [cls.create(**overrides) for _ in range(count)]

    @classmethod
    def create_point_source(cls, **overrides) -> AlertRecord:
        """Create an alert that looks like a point source (star).

        Point sources have low extendedness values.

        Args:
            **overrides: Additional field overrides

        Returns:
            AlertRecord with point source characteristics
        """
        defaults = {
            "extendedness_median": 0.1,
            "extendedness_min": 0.05,
            "extendedness_max": 0.15,
            "snr": 150.0,  # Stars typically have high SNR
        }
        defaults.update(overrides)
        return cls.create(**defaults)

    @classmethod
    def create_extended_source(cls, **overrides) -> AlertRecord:
        """Create an alert that looks like an extended source (galaxy).

        Extended sources have high extendedness values.

        Args:
            **overrides: Additional field overrides

        Returns:
            AlertRecord with extended source characteristics
        """
        defaults = {
            "extendedness_median": 0.85,
            "extendedness_min": 0.75,
            "extendedness_max": 0.95,
            "snr": 30.0,  # Galaxies typically have lower SNR
        }
        defaults.update(overrides)
        return cls.create(**defaults)

    @classmethod
    def create_minimoon_candidate(cls, **overrides) -> AlertRecord:
        """Create an alert that looks like a minimoon candidate.

        Minimoon candidates are SSO with intermediate extendedness
        (neither clearly point-like nor clearly extended).

        Args:
            **overrides: Additional field overrides

        Returns:
            AlertRecord with minimoon candidate characteristics
        """
        cls._counter += 1

        defaults = {
            "has_ss_source": True,
            "ss_object_id": f"SSO_{cls._counter:06d}",
            "ss_object_reassoc_time_mjd": 60000.0,
            "extendedness_median": 0.45,
            "extendedness_min": 0.35,
            "extendedness_max": 0.55,
            "snr": 50.0,
            "trail_data": {"trailLength": 5.0, "trailAngle": 45.0},
        }
        defaults.update(overrides)
        return cls.create(**defaults)

    @classmethod
    def create_reassociation(
        cls,
        reason: str = "new_association",
        **overrides,
    ) -> AlertRecord:
        """Create an alert that represents a reassociation.

        Args:
            reason: Reassociation reason
            **overrides: Additional field overrides

        Returns:
            AlertRecord with reassociation flags set
        """
        defaults = {
            "has_ss_source": True,
            "ss_object_id": f"SSO_{cls._counter:06d}",
            "is_reassociation": True,
            "reassociation_reason": reason,
        }
        defaults.update(overrides)
        return cls.create_minimoon_candidate(**defaults)

    @classmethod
    def create_with_cutouts(cls, base_path: str = "data/cutouts", **overrides) -> AlertRecord:
        """Create an alert with cutout paths.

        Args:
            base_path: Base path for cutout files
            **overrides: Additional field overrides

        Returns:
            AlertRecord with cutout paths set
        """
        cls._counter += 1
        source_id = 2000000 + cls._counter

        defaults = {
            "science_cutout_path": f"{base_path}/science/{source_id}.fits",
            "template_cutout_path": f"{base_path}/template/{source_id}.fits",
            "difference_cutout_path": f"{base_path}/difference/{source_id}.fits",
        }
        defaults.update(overrides)
        return cls.create(**defaults)

    @classmethod
    def create_varied_batch(cls, count: int) -> list[AlertRecord]:
        """Create a batch with varied alert types.

        Creates a mix of:
        - 50% regular alerts
        - 20% point sources
        - 20% extended sources
        - 10% minimoon candidates

        Args:
            count: Total number of alerts

        Returns:
            List of varied AlertRecord instances
        """
        alerts = []

        regular_count = int(count * 0.5)
        point_count = int(count * 0.2)
        extended_count = int(count * 0.2)
        minimoon_count = count - regular_count - point_count - extended_count

        alerts.extend(cls.create_batch(regular_count))
        for _ in range(point_count):
            alerts.append(cls.create_point_source())
        for _ in range(extended_count):
            alerts.append(cls.create_extended_source())
        for _ in range(minimoon_count):
            alerts.append(cls.create_minimoon_candidate())

        return alerts
