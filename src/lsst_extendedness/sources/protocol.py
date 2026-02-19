"""
Alert source protocol for the LSST Extendedness Pipeline.

This module defines the interface that all alert sources must implement,
enabling flexible input from different data sources (Kafka, files, databases).

Example - Implementing a custom source:
    >>> from lsst_extendedness.sources import AlertSource, register_source
    >>>
    >>> @register_source("my_custom_source")
    >>> class MyCustomSource:
    ...     '''Pull alerts from my custom API.'''
    ...
    ...     source_name = "my_custom_api"
    ...
    ...     def __init__(self, config: dict):
    ...         self.config = config
    ...
    ...     def connect(self) -> None:
    ...         self.client = MyAPIClient(self.config["api_url"])
    ...
    ...     def fetch_alerts(self, limit: int | None = None):
    ...         for raw in self.client.get_alerts(limit=limit):
    ...             yield AlertRecord.from_dict(raw)
    ...
    ...     def close(self) -> None:
    ...         self.client.disconnect()

Using registered sources:
    >>> from lsst_extendedness.sources import get_source
    >>> source = get_source("kafka", config)
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from lsst_extendedness.models import AlertRecord


# Registry of available sources
_SOURCE_REGISTRY: dict[str, type] = {}


@runtime_checkable
class AlertSource(Protocol):
    """Interface for alert data sources.

    Implement this protocol to add new input sources:
    - KafkaSource: Real-time Kafka stream (default)
    - FileSource: Import from AVRO/CSV files
    - DatabaseSource: Pull from external database
    - MockSource: Testing with synthetic data

    All sources must implement:
    - connect(): Establish connection to the data source
    - fetch_alerts(): Yield AlertRecord instances
    - close(): Clean up resources
    - source_name: Human-readable identifier

    Example:
        >>> class MySource:
        ...     source_name = "my_source"
        ...
        ...     def connect(self) -> None:
        ...         # Setup connection
        ...         pass
        ...
        ...     def fetch_alerts(self, limit=None):
        ...         # Yield AlertRecord instances
        ...         yield AlertRecord(...)
        ...
        ...     def close(self) -> None:
        ...         # Cleanup
        ...         pass
    """

    @property
    def source_name(self) -> str:
        """Human-readable source identifier.

        This name is used in logging and stored in the database
        for audit trail purposes.

        Returns:
            Source name (e.g., "kafka", "file", "mock")
        """
        ...

    def connect(self) -> None:
        """Establish connection to the data source.

        This method should:
        - Initialize any clients or connections
        - Validate configuration
        - Prepare for data fetching

        Raises:
            ConnectionError: If connection cannot be established
            ValueError: If configuration is invalid
        """
        ...

    def fetch_alerts(self, limit: int | None = None) -> Iterator["AlertRecord"]:
        """Yield alert records from the source.

        This is the main data retrieval method. It should:
        - Fetch data from the source
        - Convert to AlertRecord instances
        - Yield one record at a time (for memory efficiency)

        Args:
            limit: Maximum number of alerts to fetch (None = unlimited)

        Yields:
            AlertRecord instances

        Raises:
            RuntimeError: If not connected
            IOError: If data cannot be read
        """
        ...

    def close(self) -> None:
        """Clean up resources.

        This method should:
        - Close connections
        - Release resources
        - Commit any pending operations

        Should be safe to call multiple times.
        """
        ...


def register_source(name: str):
    """Decorator to register a source implementation.

    Use this decorator to make a source class discoverable
    by name, enabling dynamic source selection.

    Args:
        name: Unique name for the source

    Returns:
        Decorator function

    Example:
        >>> @register_source("my_source")
        >>> class MySource:
        ...     source_name = "my_source"
        ...     # ... implementation
    """

    def decorator(cls: type) -> type:
        if name in _SOURCE_REGISTRY:
            raise ValueError(f"Source '{name}' is already registered")
        _SOURCE_REGISTRY[name] = cls
        return cls

    return decorator


def get_source(name: str, *args, **kwargs) -> AlertSource:
    """Get a registered source by name.

    Args:
        name: Name of the source to get
        *args: Arguments to pass to source constructor
        **kwargs: Keyword arguments to pass to source constructor

    Returns:
        Instantiated source

    Raises:
        KeyError: If source is not registered

    Example:
        >>> source = get_source("kafka", config=kafka_config)
        >>> source.connect()
    """
    if name not in _SOURCE_REGISTRY:
        available = ", ".join(_SOURCE_REGISTRY.keys())
        raise KeyError(f"Source '{name}' not found. Available: {available}")

    source_class = _SOURCE_REGISTRY[name]
    return source_class(*args, **kwargs)


def list_sources() -> list[str]:
    """List all registered source names.

    Returns:
        List of registered source names

    Example:
        >>> print(list_sources())
        ['kafka', 'file', 'mock', 'database']
    """
    return list(_SOURCE_REGISTRY.keys())


def is_source_registered(name: str) -> bool:
    """Check if a source is registered.

    Args:
        name: Source name to check

    Returns:
        True if source is registered
    """
    return name in _SOURCE_REGISTRY
