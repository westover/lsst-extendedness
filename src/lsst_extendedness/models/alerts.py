"""
Core alert data models for the LSST Extendedness Pipeline.

This module defines Pydantic models for:
- AlertRecord: Core alert data with validation
- ProcessingResult: Results from post-processors

All models support:
- Automatic validation on creation
- JSON serialization/deserialization
- Type hints for IDE support
- Database-ready dictionary conversion

Example:
    >>> from lsst_extendedness.models import AlertRecord
    >>>
    >>> # Create from dictionary
    >>> alert = AlertRecord(
    ...     alert_id=12345,
    ...     dia_source_id=67890,
    ...     ra=180.0,
    ...     dec=45.0,
    ...     mjd=60000.5,
    ... )
    >>>
    >>> # Validation happens automatically
    >>> alert.ra  # 180.0
    >>>
    >>> # Convert to dict for database
    >>> db_record = alert.to_db_dict()
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AlertRecord(BaseModel):
    """Core alert record with validation.

    All fields from DIASource + SSObject, validated on creation.
    Use AlertRecord.from_avro() for Kafka deserialization.

    Attributes:
        alert_id: Unique alert identifier
        dia_source_id: DIASource identifier
        dia_object_id: DIAObject identifier (optional)
        ra: Right ascension in degrees (0-360)
        dec: Declination in degrees (-90 to 90)
        mjd: Modified Julian Date
        ingested_at: When this record was ingested
        filter_name: Filter band (g, r, i, z, y, u)
        ps_flux: Point source flux
        ps_flux_err: Point source flux error
        snr: Signal-to-noise ratio
        extendedness_median: Median extendedness value (0-1)
        extendedness_min: Minimum extendedness value (0-1)
        extendedness_max: Maximum extendedness value (0-1)
        has_ss_source: Whether alert has SSSource association
        ss_object_id: Solar System Object identifier
        ss_object_reassoc_time_mjd: SSObject reassociation timestamp
        is_reassociation: Whether this is a reassociation
        reassociation_reason: Reason for reassociation
        trail_data: Dictionary of trail* fields
        pixel_flags: Dictionary of pixelFlags* fields
        science_cutout_path: Path to science cutout
        template_cutout_path: Path to template cutout
        difference_cutout_path: Path to difference cutout

    Example:
        >>> alert = AlertRecord.from_avro(avro_record)
        >>> print(f"Alert {alert.alert_id} at RA={alert.ra}, Dec={alert.dec}")
    """

    model_config = ConfigDict(
        extra="ignore",  # Ignore unexpected fields
        validate_assignment=True,  # Validate on attribute assignment
        str_strip_whitespace=True,  # Strip whitespace from strings
    )

    # Identifiers
    alert_id: int = Field(..., description="Unique alert identifier")
    dia_source_id: int = Field(..., description="DIASource identifier")
    dia_object_id: int | None = Field(default=None, description="DIAObject identifier")

    # Coordinates
    ra: float = Field(..., ge=0, le=360, description="Right ascension (degrees)")
    dec: float = Field(..., ge=-90, le=90, description="Declination (degrees)")

    # Temporal
    mjd: float = Field(..., gt=0, description="Modified Julian Date")
    ingested_at: datetime = Field(default_factory=datetime.utcnow, description="Ingestion timestamp")

    # Photometry
    filter_name: str | None = Field(default=None, description="Filter band (g, r, i, z, y, u)")
    ps_flux: float | None = Field(default=None, description="Point source flux")
    ps_flux_err: float | None = Field(default=None, description="Point source flux error")
    snr: float | None = Field(default=None, ge=0, description="Signal-to-noise ratio")

    # Extendedness (key science metrics)
    extendedness_median: float | None = Field(
        default=None, ge=0, le=1, description="Median extendedness value"
    )
    extendedness_min: float | None = Field(
        default=None, ge=0, le=1, description="Minimum extendedness value"
    )
    extendedness_max: float | None = Field(
        default=None, ge=0, le=1, description="Maximum extendedness value"
    )

    # Solar system association
    has_ss_source: bool = Field(default=False, description="Has SSSource association")
    ss_object_id: str | None = Field(default=None, description="Solar System Object ID")
    ss_object_reassoc_time_mjd: float | None = Field(
        default=None, description="SSObject reassociation timestamp (MJD)"
    )
    is_reassociation: bool = Field(default=False, description="Is this a reassociation")
    reassociation_reason: str | None = Field(default=None, description="Reason for reassociation")

    # Dynamic fields (JSON)
    trail_data: dict[str, Any] = Field(
        default_factory=dict, description="Dictionary of trail* fields"
    )
    pixel_flags: dict[str, Any] = Field(
        default_factory=dict, description="Dictionary of pixelFlags* fields"
    )

    # Cutout paths
    science_cutout_path: str | None = Field(default=None, description="Path to science cutout")
    template_cutout_path: str | None = Field(default=None, description="Path to template cutout")
    difference_cutout_path: str | None = Field(
        default=None, description="Path to difference cutout"
    )

    @field_validator("filter_name")
    @classmethod
    def validate_filter_name(cls, v: str | None) -> str | None:
        """Validate filter name is one of the LSST bands."""
        if v is None:
            return None
        valid_filters = {"g", "r", "i", "z", "y", "u"}
        if v.lower() not in valid_filters:
            # Allow unknown filters but normalize to lowercase
            return v.lower()
        return v.lower()

    @classmethod
    def from_avro(cls, avro_record: dict[str, Any]) -> "AlertRecord":
        """Create an AlertRecord from an AVRO-deserialized alert packet.

        This method handles the nested structure of LSST alert packets,
        extracting fields from diaSource and ssObject sub-dictionaries.

        Args:
            avro_record: Deserialized AVRO alert packet from Kafka

        Returns:
            AlertRecord: Validated alert record

        Example:
            >>> import fastavro
            >>> alert = fastavro.schemaless_reader(bytes_io, schema)
            >>> record = AlertRecord.from_avro(alert)
        """
        dia_source = avro_record.get("diaSource", {}) or {}
        ss_object = avro_record.get("ssObject", {})

        # Extract trail* fields
        trail_data = {
            key: value
            for key, value in dia_source.items()
            if key.startswith("trail") and value is not None
        }

        # Extract pixelFlags* fields
        pixel_flags = {
            key: value
            for key, value in dia_source.items()
            if key.startswith("pixelFlags") and value is not None
        }

        # Determine SSObject presence
        has_ss_source = ss_object is not None and len(ss_object) > 0

        return cls(
            alert_id=avro_record.get("alertId", 0),
            dia_source_id=dia_source.get("diaSourceId", 0),
            dia_object_id=dia_source.get("diaObjectId"),
            ra=dia_source.get("ra", 0.0),
            dec=dia_source.get("decl", 0.0),
            mjd=dia_source.get("midPointTai", 0.0),
            filter_name=dia_source.get("filterName"),
            ps_flux=dia_source.get("psFlux"),
            ps_flux_err=dia_source.get("psFluxErr"),
            snr=dia_source.get("snr"),
            extendedness_median=dia_source.get("extendednessMedian"),
            extendedness_min=dia_source.get("extendednessMin"),
            extendedness_max=dia_source.get("extendednessMax"),
            has_ss_source=has_ss_source,
            ss_object_id=ss_object.get("ssObjectId") if ss_object else None,
            ss_object_reassoc_time_mjd=(
                ss_object.get("ssObjectReassocTimeMjdTai") if ss_object else None
            ),
            trail_data=trail_data,
            pixel_flags=pixel_flags,
        )

    def to_db_dict(self) -> dict[str, Any]:
        """Convert to dictionary for SQLite insertion.

        JSON fields (trail_data, pixel_flags) are serialized to strings.

        Returns:
            Dictionary suitable for SQLite parameter binding
        """
        data = self.model_dump(mode="json")

        # Serialize nested dicts to JSON strings for SQLite
        data["trail_data"] = json.dumps(data["trail_data"])
        data["pixel_flags"] = json.dumps(data["pixel_flags"])

        return data

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "AlertRecord":
        """Create AlertRecord from a database row.

        Deserializes JSON fields from strings.

        Args:
            row: Dictionary from SQLite query

        Returns:
            AlertRecord instance
        """
        # Copy to avoid mutating input
        data = dict(row)

        # Parse JSON fields
        if isinstance(data.get("trail_data"), str):
            data["trail_data"] = json.loads(data["trail_data"])
        if isinstance(data.get("pixel_flags"), str):
            data["pixel_flags"] = json.loads(data["pixel_flags"])

        return cls(**data)


class ProcessingResult(BaseModel):
    """Result from a post-processor.

    This model captures the output of a processing run, including
    the processor metadata and the actual results.

    Attributes:
        processor_name: Name of the processor that generated this result
        processor_version: Version of the processor
        records: List of result records (processor-specific format)
        metadata: Additional metadata about the processing run
        summary: Human-readable summary of results
        processed_at: When the processing was completed

    Example:
        >>> result = ProcessingResult(
        ...     processor_name="minimoon_detector",
        ...     processor_version="1.0.0",
        ...     records=[{"candidate_id": 1, "score": 0.95}],
        ...     summary="Found 1 minimoon candidate"
        ... )
    """

    model_config = ConfigDict(extra="ignore")

    processor_name: str = Field(..., description="Name of the processor")
    processor_version: str = Field(..., description="Version of the processor")
    records: list[dict[str, Any]] = Field(
        default_factory=list, description="List of result records"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional processing metadata"
    )
    summary: str = Field(default="", description="Human-readable summary")
    processed_at: datetime = Field(
        default_factory=datetime.utcnow, description="Processing timestamp"
    )

    def to_db_dict(self) -> dict[str, Any]:
        """Convert to dictionary for SQLite insertion."""
        data = self.model_dump(mode="json")
        data["records"] = json.dumps(data["records"])
        data["metadata"] = json.dumps(data["metadata"])
        return data

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "ProcessingResult":
        """Create ProcessingResult from a database row."""
        data = dict(row)
        if isinstance(data.get("records"), str):
            data["records"] = json.loads(data["records"])
        if isinstance(data.get("metadata"), str):
            data["metadata"] = json.loads(data["metadata"])
        return cls(**data)
