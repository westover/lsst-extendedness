"""
AVRO Deserialization for LSST Alert Packets.

Handles deserialization of AVRO-encoded alerts from Kafka,
including Confluent Wire Format support.
"""

from __future__ import annotations

import io
import struct
from typing import Any

from ..models.alerts import AlertRecord


def deserialize_avro(data: bytes, schema: dict[str, Any] | None = None) -> dict[str, Any]:
    """Deserialize AVRO bytes to dictionary.

    Supports both raw AVRO and Confluent Wire Format (with schema registry).

    Args:
        data: Raw bytes from Kafka message
        schema: Optional AVRO schema dict. If None, schema must be embedded.

    Returns:
        Deserialized alert dictionary

    Raises:
        ImportError: If fastavro is not installed
        ValueError: If data cannot be deserialized
    """
    try:
        import fastavro
    except ImportError as e:
        raise ImportError(
            "fastavro is required for AVRO deserialization. Install with: pdm add fastavro"
        ) from e

    # Check for Confluent Wire Format (magic byte 0x00)
    if len(data) > 5 and data[0] == 0:
        # Confluent format: [magic byte (1)] [schema id (4)] [avro data]
        # Skip the 5-byte header
        data = data[5:]

    # Deserialize
    reader = io.BytesIO(data)

    if schema:
        # Use provided schema
        parsed_schema = fastavro.parse_schema(schema)
        record = fastavro.schemaless_reader(reader, parsed_schema, parsed_schema)
    else:
        # Schema embedded in data (standard AVRO container)
        records = list(fastavro.reader(reader))
        if not records:
            raise ValueError("No records found in AVRO data")
        record = records[0]

    if not isinstance(record, dict):
        raise ValueError(f"Expected dict from AVRO deserialization, got {type(record)}")
    return record


def extract_schema_id(data: bytes) -> int | None:
    """Extract schema ID from Confluent Wire Format message.

    Args:
        data: Raw bytes from Kafka message

    Returns:
        Schema ID if Confluent format, None otherwise
    """
    if len(data) > 5 and data[0] == 0:
        # Unpack 4-byte big-endian schema ID
        schema_id: int = struct.unpack(">I", data[1:5])[0]
        return schema_id
    return None


def avro_to_alert(avro_record: dict[str, Any]) -> AlertRecord:
    """Convert deserialized AVRO record to AlertRecord.

    Maps LSST alert packet fields to our internal model.

    Args:
        avro_record: Deserialized AVRO dictionary

    Returns:
        Validated AlertRecord instance
    """
    return AlertRecord.from_avro(avro_record)


def deserialize_alert(data: bytes, schema: dict[str, Any] | None = None) -> AlertRecord:
    """Convenience function: deserialize AVRO bytes directly to AlertRecord.

    Args:
        data: Raw AVRO bytes
        schema: Optional schema dict

    Returns:
        Validated AlertRecord
    """
    avro_record = deserialize_avro(data, schema)
    return avro_to_alert(avro_record)


class AlertDeserializer:
    """Stateful deserializer with schema caching.

    For high-throughput scenarios where schema lookup overhead matters.
    """

    def __init__(self, schema_registry_url: str | None = None):
        """Initialize deserializer.

        Args:
            schema_registry_url: Optional Confluent Schema Registry URL
        """
        self.schema_registry_url = schema_registry_url
        self._schema_cache: dict[int, dict[str, Any]] = {}
        self._default_schema: dict[str, Any] | None = None

    def set_default_schema(self, schema: dict[str, Any]) -> None:
        """Set default schema for schemaless messages."""
        self._default_schema = schema

    def get_schema(self, schema_id: int) -> dict[str, Any] | None:
        """Get schema by ID from cache or registry.

        Args:
            schema_id: Confluent schema registry ID

        Returns:
            Schema dict or None if not found
        """
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]

        if not self.schema_registry_url:
            return None

        # Fetch from schema registry
        try:
            import httpx

            response = httpx.get(f"{self.schema_registry_url}/schemas/ids/{schema_id}")
            if response.status_code == 200:
                schema = response.json().get("schema")
                if schema:
                    import json

                    parsed: dict[str, Any] = json.loads(schema)
                    self._schema_cache[schema_id] = parsed
                    return parsed
        except Exception:
            pass

        return None

    def deserialize(self, data: bytes) -> AlertRecord:
        """Deserialize message to AlertRecord.

        Automatically detects format and uses cached schemas.

        Args:
            data: Raw message bytes

        Returns:
            Validated AlertRecord
        """
        schema = self._default_schema

        # Check for Confluent Wire Format
        schema_id = extract_schema_id(data)
        if schema_id is not None:
            cached_schema = self.get_schema(schema_id)
            if cached_schema:
                schema = cached_schema

        return deserialize_alert(data, schema)

    def deserialize_batch(self, messages: list[bytes]) -> list[AlertRecord]:
        """Deserialize multiple messages.

        Args:
            messages: List of raw message bytes

        Returns:
            List of AlertRecords (skips failed deserializations)
        """
        alerts = []
        for data in messages:
            try:
                alerts.append(self.deserialize(data))
            except Exception:
                # Log and skip failed messages
                continue
        return alerts
