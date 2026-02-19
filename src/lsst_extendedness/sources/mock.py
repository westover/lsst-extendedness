"""
Mock alert source for testing.

This module provides a MockSource that generates synthetic alerts
for testing without requiring Kafka or real data.

Example:
    >>> from lsst_extendedness.sources import MockSource
    >>>
    >>> source = MockSource(count=100, seed=42)
    >>> source.connect()
    >>> for alert in source.fetch_alerts():
    ...     print(f"Alert {alert.alert_id}")
    >>> source.close()
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import datetime

from lsst_extendedness.models.alerts import AlertRecord
from lsst_extendedness.sources.protocol import register_source


@register_source("mock")
class MockSource:
    """Mock source for testing without real infrastructure.

    Generates synthetic alerts with configurable parameters,
    useful for testing, demos, and development.

    Attributes:
        source_name: Always "mock"
        count: Number of alerts to generate
        seed: Random seed for reproducibility

    Example:
        >>> source = MockSource(count=1000, seed=42)
        >>> source.connect()
        >>> alerts = list(source.fetch_alerts(limit=100))
        >>> print(f"Generated {len(alerts)} alerts")
    """

    source_name = "mock"

    def __init__(
        self,
        count: int = 100,
        *,
        seed: int | None = None,
        sso_probability: float = 0.3,
        reassociation_probability: float = 0.05,
        base_mjd: float = 60000.0,
    ):
        """Initialize mock source.

        Args:
            count: Number of alerts to generate
            seed: Random seed for reproducibility
            sso_probability: Probability of SSO association (0-1)
            reassociation_probability: Probability of reassociation (0-1)
            base_mjd: Base MJD for generated timestamps
        """
        self.count = count
        self.seed = seed
        self.sso_probability = sso_probability
        self.reassociation_probability = reassociation_probability
        self.base_mjd = base_mjd

        self._connected = False
        self._rng: random.Random | None = None
        self._generated = 0

    def connect(self) -> None:
        """Initialize random number generator."""
        self._rng = random.Random(self.seed)
        self._connected = True
        self._generated = 0

    def fetch_alerts(self, limit: int | None = None) -> Iterator[AlertRecord]:
        """Generate synthetic alerts.

        Args:
            limit: Maximum number of alerts (None = use count)

        Yields:
            Synthetic AlertRecord instances
        """
        if not self._connected:
            raise RuntimeError("Source not connected. Call connect() first.")

        if self._rng is None:
            self._rng = random.Random(self.seed)

        max_alerts = min(limit or self.count, self.count - self._generated)

        for i in range(max_alerts):
            alert = self._generate_alert(self._generated + i)
            self._generated += 1
            yield alert

    def _generate_alert(self, index: int) -> AlertRecord:
        """Generate a single synthetic alert.

        Args:
            index: Alert index for reproducibility

        Returns:
            Synthetic AlertRecord
        """
        rng = self._rng
        assert rng is not None

        # Generate coordinates (roughly uniform on sky)
        ra = rng.uniform(0, 360)
        dec = rng.uniform(-90, 90)

        # Generate extendedness with realistic distribution
        # Most sources are either point-like (stars) or extended (galaxies)
        ext_type = rng.random()
        if ext_type < 0.5:
            # Point source (star-like)
            extendedness = rng.gauss(0.15, 0.05)
        elif ext_type < 0.9:
            # Extended source (galaxy-like)
            extendedness = rng.gauss(0.85, 0.1)
        else:
            # Intermediate (could be minimoon candidate)
            extendedness = rng.gauss(0.5, 0.15)

        # Clamp to valid range
        extendedness = max(0.0, min(1.0, extendedness))

        # Generate SSO association
        has_sso = rng.random() < self.sso_probability
        ss_object_id = f"SSO_{index:06d}" if has_sso else None
        ss_reassoc_time = self.base_mjd + rng.uniform(-1, 0) if has_sso else None

        # Generate reassociation
        is_reassociation = has_sso and rng.random() < self.reassociation_probability
        reassoc_reason = None
        if is_reassociation:
            reassoc_reason = rng.choice(
                [
                    "new_association",
                    "changed_association",
                    "updated_reassociation",
                ]
            )

        # Generate photometry
        ps_flux = rng.gauss(1000, 500)
        ps_flux_err = abs(rng.gauss(ps_flux * 0.01, ps_flux * 0.005))
        snr = abs(ps_flux / ps_flux_err) if ps_flux_err > 0 else 100

        # Generate trail data
        has_trail = rng.random() < 0.1
        trail_data = {}
        if has_trail:
            trail_data = {
                "trailLength": rng.uniform(1, 50),
                "trailAngle": rng.uniform(0, 360),
            }

        # Generate pixel flags
        pixel_flags = {
            "pixelFlagsBad": rng.random() < 0.01,
            "pixelFlagsCr": rng.random() < 0.05,
            "pixelFlagsEdge": rng.random() < 0.02,
            "pixelFlagsSaturated": rng.random() < 0.01,
        }

        return AlertRecord(
            alert_id=1000000 + index,
            dia_source_id=2000000 + index,
            dia_object_id=3000000 + (index // 10),  # Group ~10 sources per object
            ra=ra,
            dec=dec,
            mjd=self.base_mjd + rng.uniform(0, 30),
            ingested_at=datetime.utcnow(),
            filter_name=rng.choice(["g", "r", "i", "z", "y"]),
            ps_flux=max(0, ps_flux),
            ps_flux_err=ps_flux_err,
            snr=max(0, snr),
            extendedness_median=extendedness,
            extendedness_min=max(0, extendedness - rng.uniform(0, 0.1)),
            extendedness_max=min(1, extendedness + rng.uniform(0, 0.1)),
            has_ss_source=has_sso,
            ss_object_id=ss_object_id,
            ss_object_reassoc_time_mjd=ss_reassoc_time,
            is_reassociation=is_reassociation,
            reassociation_reason=reassoc_reason,
            trail_data=trail_data,
            pixel_flags=pixel_flags,
        )

    def close(self) -> None:
        """Clean up resources."""
        self._connected = False
        self._rng = None

    def __repr__(self) -> str:
        """String representation."""
        return f"MockSource(count={self.count}, seed={self.seed})"
