"""
Kafka alert source for the LSST Extendedness Pipeline.

This module provides a KafkaSource that consumes alerts from Kafka topics,
including the ANTARES broker.

Example:
    >>> from lsst_extendedness.sources import KafkaSource
    >>>
    >>> config = {
    ...     "bootstrap.servers": "kafka.example.com:9092",
    ...     "group.id": "my-consumer",
    ... }
    >>> source = KafkaSource(config, topic="lsst-alerts")
    >>> source.connect()
    >>> for alert in source.fetch_alerts(limit=100):
    ...     print(f"Alert {alert.alert_id}")
    >>> source.close()
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from typing import Any

from lsst_extendedness.models.alerts import AlertRecord
from lsst_extendedness.sources.protocol import register_source

# Lazy imports for optional dependencies
_confluent_kafka = None
_fastavro = None


def _import_kafka() -> Any:
    """Lazy import of confluent-kafka."""
    global _confluent_kafka
    if _confluent_kafka is None:
        try:
            import confluent_kafka

            _confluent_kafka = confluent_kafka
        except ImportError as e:
            raise ImportError(
                "confluent-kafka is required for KafkaSource. Install with: pdm install"
            ) from e
    return _confluent_kafka


def _import_fastavro() -> Any:
    """Lazy import of fastavro."""
    global _fastavro
    if _fastavro is None:
        try:
            import fastavro

            _fastavro = fastavro
        except ImportError as e:
            raise ImportError(
                "fastavro is required for KafkaSource. Install with: pdm install"
            ) from e
    return _fastavro


@register_source("kafka")
class KafkaSource:
    """Kafka source for consuming LSST alerts.

    Connects to a Kafka broker (including ANTARES) and consumes
    AVRO-serialized alert messages.

    Attributes:
        source_name: Always "kafka"
        topic: Kafka topic to consume from
        config: Kafka consumer configuration

    Example:
        >>> config = {
        ...     "bootstrap.servers": "localhost:9092",
        ...     "group.id": "lsst-consumer",
        ...     "auto.offset.reset": "earliest",
        ... }
        >>> source = KafkaSource(config, topic="alerts")
        >>> source.connect()
        >>> for alert in source.fetch_alerts(limit=1000):
        ...     process(alert)
        >>> source.close()
    """

    source_name = "kafka"

    def __init__(
        self,
        config: dict[str, Any],
        topic: str,
        *,
        poll_timeout: float = 1.0,
        schema: dict[str, Any] | None = None,
    ):
        """Initialize Kafka source.

        Args:
            config: Kafka consumer configuration dictionary
            topic: Topic name to consume from
            poll_timeout: Timeout for polling messages (seconds)
            schema: AVRO schema for deserialization (optional)
        """
        self.config = config
        self.topic = topic
        self.poll_timeout = poll_timeout
        self.schema = schema

        self._consumer = None
        self._connected = False

    def connect(self) -> None:
        """Connect to Kafka broker and subscribe to topic."""
        kafka = _import_kafka()

        # Create consumer
        self._consumer = kafka.Consumer(self.config)

        # Subscribe to topic
        if self._consumer is not None:
            self._consumer.subscribe([self.topic])

        self._connected = True

    def fetch_alerts(self, limit: int | None = None) -> Iterator[AlertRecord]:
        """Consume and yield alerts from Kafka.

        Args:
            limit: Maximum number of alerts to consume (None = unlimited)

        Yields:
            AlertRecord instances

        Raises:
            RuntimeError: If not connected
        """
        if not self._connected or self._consumer is None:
            raise RuntimeError("Source not connected. Call connect() first.")

        kafka = _import_kafka()
        fastavro = _import_fastavro()

        count = 0

        while limit is None or count < limit:
            # Poll for message
            msg = self._consumer.poll(timeout=self.poll_timeout)

            if msg is None:
                continue

            if msg.error():
                error = msg.error()
                if error.code() == kafka.KafkaError._PARTITION_EOF:
                    # End of partition, normal condition
                    continue
                else:
                    # Real error
                    raise RuntimeError(f"Kafka error: {error}")

            try:
                # Deserialize AVRO message
                bytes_reader = io.BytesIO(msg.value())

                if self.schema:
                    # Use provided schema
                    alert_data = fastavro.schemaless_reader(
                        bytes_reader,
                        self.schema,
                    )
                else:
                    # Read schema from message (slower)
                    alert_data = fastavro.reader(bytes_reader)
                    alert_data = next(alert_data)

                # Convert to AlertRecord
                alert = AlertRecord.from_avro(alert_data)
                count += 1
                yield alert

            except StopIteration:
                # Empty message
                continue
            except Exception as e:
                # Log error but continue processing
                # In production, you might want to send to dead letter queue
                import logging

                logging.getLogger(__name__).error(
                    f"Error deserializing alert: {e}",
                    exc_info=True,
                )
                continue

    def close(self) -> None:
        """Close Kafka consumer."""
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        self._connected = False

    def get_consumer_lag(self) -> dict[int, dict[str, int]]:
        """Get consumer lag per partition.

        Returns:
            Dictionary mapping partition ID to lag info
        """
        if self._consumer is None:
            raise RuntimeError("Source not connected")

        assignment = self._consumer.assignment()
        if not assignment:
            return {}

        lag_info = {}

        for tp in assignment:
            if tp.topic != self.topic:
                continue

            # Get committed offset
            committed = self._consumer.committed([tp])[0]
            committed_offset = committed.offset if committed else -1

            # Get high water mark
            _low, high = self._consumer.get_watermark_offsets(tp, timeout=5.0)

            lag = high - committed_offset if committed_offset >= 0 else high

            lag_info[tp.partition] = {
                "committed_offset": committed_offset,
                "high_water_mark": high,
                "lag": lag,
            }

        return lag_info

    def __repr__(self) -> str:
        """String representation."""
        servers = self.config.get("bootstrap.servers", "unknown")
        return f"KafkaSource(topic={self.topic!r}, servers={servers!r})"
