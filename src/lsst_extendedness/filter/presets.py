"""
Preset Filter Configurations.

Common filter configurations for typical science use cases.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .engine import FilterCondition, FilterConfig


def point_sources(limit: int | None = None) -> FilterConfig:
    """Filter for point-like sources (stars).

    Extendedness < 0.3, high SNR preferred.
    """
    config = FilterConfig(
        name="point_sources",
        description="Point-like sources with low extendedness (stars)",
        limit=limit,
        order_by="snr",
        order_desc=True,
    )
    config.add(FilterCondition.lt("extendedness_median", 0.3))
    config.add(FilterCondition.is_not_null("extendedness_median"))
    return config


def extended_sources(limit: int | None = None) -> FilterConfig:
    """Filter for extended sources (galaxies).

    Extendedness > 0.7.
    """
    config = FilterConfig(
        name="extended_sources",
        description="Extended sources with high extendedness (galaxies)",
        limit=limit,
        order_by="extendedness_median",
        order_desc=True,
    )
    config.add(FilterCondition.gt("extendedness_median", 0.7))
    return config


def minimoon_candidates(limit: int | None = None) -> FilterConfig:
    """Filter for potential minimoon candidates.

    SSO sources with intermediate extendedness (0.3-0.7).
    """
    config = FilterConfig(
        name="minimoon_candidates",
        description="SSO sources with intermediate extendedness (potential minimoons)",
        limit=limit,
        order_by="mjd",
        order_desc=True,
    )
    config.add(FilterCondition.eq("has_ss_source", 1))
    config.add(FilterCondition.ge("extendedness_median", 0.3))
    config.add(FilterCondition.le("extendedness_median", 0.7))
    return config


def sso_alerts(limit: int | None = None) -> FilterConfig:
    """Filter for all SSObject-associated alerts."""
    config = FilterConfig(
        name="sso_alerts",
        description="All alerts with solar system object associations",
        limit=limit,
        order_by="mjd",
        order_desc=True,
    )
    config.add(FilterCondition.eq("has_ss_source", 1))
    return config


def reassociations(limit: int | None = None) -> FilterConfig:
    """Filter for reassociation events."""
    config = FilterConfig(
        name="reassociations",
        description="Alerts flagged as SSObject reassociations",
        limit=limit,
        order_by="mjd",
        order_desc=True,
    )
    config.add(FilterCondition.eq("is_reassociation", 1))
    return config


def high_snr(min_snr: float = 50.0, limit: int | None = None) -> FilterConfig:
    """Filter for high signal-to-noise detections."""
    config = FilterConfig(
        name="high_snr",
        description=f"High SNR detections (>= {min_snr})",
        limit=limit,
        order_by="snr",
        order_desc=True,
    )
    config.add(FilterCondition.ge("snr", min_snr))
    return config


def recent_days(days: int = 7, limit: int | None = None) -> FilterConfig:
    """Filter for recent alerts within N days."""
    from ..utils.time import days_ago_mjd

    threshold = days_ago_mjd(days)

    config = FilterConfig(
        name=f"recent_{days}d",
        description=f"Alerts from the last {days} days",
        limit=limit,
        order_by="mjd",
        order_desc=True,
    )
    config.add(FilterCondition.ge("mjd", threshold))
    return config


def by_filter_band(band: str, limit: int | None = None) -> FilterConfig:
    """Filter by photometric band (g, r, i, z, y)."""
    config = FilterConfig(
        name=f"band_{band}",
        description=f"Alerts observed in {band} band",
        limit=limit,
        order_by="mjd",
        order_desc=True,
    )
    config.add(FilterCondition.eq("filter_name", band))
    return config


def sky_region(
    ra_min: float,
    ra_max: float,
    dec_min: float,
    dec_max: float,
    limit: int | None = None,
) -> FilterConfig:
    """Filter by sky region (box query)."""
    config = FilterConfig(
        name="sky_region",
        description=f"RA [{ra_min:.2f}, {ra_max:.2f}], Dec [{dec_min:.2f}, {dec_max:.2f}]",
        limit=limit,
        order_by="mjd",
        order_desc=True,
    )
    config.add(FilterCondition.ge("ra", ra_min))
    config.add(FilterCondition.le("ra", ra_max))
    config.add(FilterCondition.ge("dec", dec_min))
    config.add(FilterCondition.le("dec", dec_max))
    return config


def time_window(
    start_mjd: float,
    end_mjd: float,
    limit: int | None = None,
) -> FilterConfig:
    """Filter by time window (MJD range)."""
    config = FilterConfig(
        name="time_window",
        description=f"MJD [{start_mjd:.2f}, {end_mjd:.2f}]",
        limit=limit,
        order_by="mjd",
        order_desc=False,
    )
    config.add(FilterCondition.between("mjd", start_mjd, end_mjd))
    return config


def extendedness_range(
    ext_min: float,
    ext_max: float,
    limit: int | None = None,
) -> FilterConfig:
    """Filter by extendedness range."""
    config = FilterConfig(
        name="extendedness_range",
        description=f"Extendedness [{ext_min:.2f}, {ext_max:.2f}]",
        limit=limit,
        order_by="extendedness_median",
        order_desc=False,
    )
    config.add(FilterCondition.between("extendedness_median", ext_min, ext_max))
    return config


def non_sso(limit: int | None = None) -> FilterConfig:
    """Filter for non-SSO alerts (extragalactic sources, etc.)."""
    config = FilterConfig(
        name="non_sso",
        description="Alerts without solar system object associations",
        limit=limit,
        order_by="mjd",
        order_desc=True,
    )
    config.add(FilterCondition.eq("has_ss_source", 0))
    return config


def unprocessed(limit: int | None = None) -> FilterConfig:
    """Filter for alerts from sources not yet post-processed.

    Joins with processed_sources to find unprocessed alerts.
    Note: This returns a custom query, not a standard FilterConfig.
    """
    config = FilterConfig(
        name="unprocessed",
        description="Alerts from sources not yet post-processed",
        limit=limit,
        order_by="mjd",
        order_desc=True,
    )
    # This is a simplified version - actual implementation would need JOIN
    return config


# Dictionary of all presets for easy access
PRESETS: dict[str, Callable[..., FilterConfig]] = {
    "point_sources": point_sources,
    "extended_sources": extended_sources,
    "minimoon_candidates": minimoon_candidates,
    "sso_alerts": sso_alerts,
    "reassociations": reassociations,
    "high_snr": high_snr,
    "non_sso": non_sso,
}


def get_preset(name: str, **kwargs: Any) -> FilterConfig:
    """Get a preset filter by name.

    Args:
        name: Preset name
        **kwargs: Arguments to pass to preset function

    Returns:
        FilterConfig

    Raises:
        ValueError: If preset not found
    """
    if name not in PRESETS:
        available = ", ".join(PRESETS.keys())
        raise ValueError(f"Unknown preset: {name}. Available: {available}")

    return PRESETS[name](**kwargs)


def list_presets() -> list[dict[str, str]]:
    """List all available presets.

    Returns:
        List of preset info dicts
    """
    return [
        {
            "name": name,
            "description": func().description,
        }
        for name, func in PRESETS.items()
    ]
