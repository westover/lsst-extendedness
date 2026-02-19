"""
File-based alert source for the LSST Extendedness Pipeline.

This module provides a FileSource that reads alerts from AVRO or CSV files,
useful for backfilling data or testing with existing datasets.

Example:
    >>> from lsst_extendedness.sources import FileSource
    >>>
    >>> source = FileSource("data/alerts.avro")
    >>> source.connect()
    >>> for alert in source.fetch_alerts():
    ...     print(f"Alert {alert.alert_id}")
    >>> source.close()
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from lsst_extendedness.models.alerts import AlertRecord
from lsst_extendedness.sources.protocol import register_source


@register_source("file")
class FileSource:
    """File-based source for importing AVRO or CSV files.

    Supports:
    - Single files (.avro, .csv)
    - Glob patterns (data/*.avro)
    - Directories (scans for supported files)

    Attributes:
        source_name: Always "file"
        path: Path to file(s)

    Example:
        >>> source = FileSource("data/alerts/*.avro")
        >>> source.connect()
        >>> alerts = list(source.fetch_alerts())
        >>> print(f"Loaded {len(alerts)} alerts")
    """

    source_name = "file"

    def __init__(
        self,
        path: str | Path,
        *,
        file_type: str | None = None,
    ):
        """Initialize file source.

        Args:
            path: Path to file, directory, or glob pattern
            file_type: File type override ("avro" or "csv")
        """
        self.path = Path(path) if not isinstance(path, Path) else path
        self.file_type = file_type

        self._files: list[Path] = []
        self._connected = False

    def connect(self) -> None:
        """Discover files to process."""
        self._files = self._discover_files()
        self._connected = True

    def _discover_files(self) -> list[Path]:
        """Find all files matching the path pattern.

        Returns:
            List of file paths to process
        """
        path = self.path

        # Handle glob pattern in path string
        path_str = str(path)
        if "*" in path_str or "?" in path_str:
            # It's a glob pattern
            parent = Path(path_str.split("*")[0]).parent
            pattern = path.name
            return sorted(parent.glob(pattern))

        # Handle directory
        if path.is_dir():
            avro_files = list(path.glob("**/*.avro"))
            csv_files = list(path.glob("**/*.csv"))
            return sorted(avro_files + csv_files)

        # Single file
        if path.exists():
            return [path]

        # Try as glob pattern from cwd
        matches = list(Path.cwd().glob(str(path)))
        return sorted(matches)

    def fetch_alerts(self, limit: int | None = None) -> Iterator[AlertRecord]:
        """Read and yield alerts from files.

        Args:
            limit: Maximum number of alerts (None = all)

        Yields:
            AlertRecord instances
        """
        if not self._connected:
            raise RuntimeError("Source not connected. Call connect() first.")

        count = 0

        for file_path in self._files:
            if limit is not None and count >= limit:
                break

            # Determine file type
            file_type = self.file_type or self._detect_file_type(file_path)

            if file_type == "avro":
                yield from self._read_avro(file_path, limit, count)
            elif file_type == "csv":
                yield from self._read_csv(file_path, limit, count)
            else:
                # Skip unknown file types
                continue

            # Update count (approximate, actual handled in readers)
            if limit is not None:
                remaining = limit - count
                if remaining <= 0:
                    break

    def _detect_file_type(self, path: Path) -> str:
        """Detect file type from extension.

        Args:
            path: File path

        Returns:
            File type ("avro" or "csv")
        """
        suffix = path.suffix.lower()
        if suffix == ".avro":
            return "avro"
        elif suffix in (".csv", ".tsv"):
            return "csv"
        else:
            return "unknown"

    def _read_avro(
        self,
        path: Path,
        limit: int | None,
        current_count: int,
    ) -> Iterator[AlertRecord]:
        """Read alerts from AVRO file.

        Args:
            path: Path to AVRO file
            limit: Maximum total alerts
            current_count: Alerts read so far

        Yields:
            AlertRecord instances
        """
        try:
            import fastavro
        except ImportError as e:
            raise ImportError(
                "fastavro is required for AVRO files. " "Install with: pdm install"
            ) from e

        count = current_count

        with open(path, "rb") as f:
            reader = fastavro.reader(f)

            for record in reader:
                if limit is not None and count >= limit:
                    break

                try:
                    alert = AlertRecord.from_avro(record)
                    count += 1
                    yield alert
                except Exception:
                    # Skip malformed records
                    continue

    def _read_csv(
        self,
        path: Path,
        limit: int | None,
        current_count: int,
    ) -> Iterator[AlertRecord]:
        """Read alerts from CSV file.

        Args:
            path: Path to CSV file
            limit: Maximum total alerts
            current_count: Alerts read so far

        Yields:
            AlertRecord instances
        """
        import pandas as pd

        # Determine how many rows to read
        nrows = None
        if limit is not None:
            remaining = limit - current_count
            if remaining <= 0:
                return
            nrows = remaining

        # Read CSV
        df = pd.read_csv(path, nrows=nrows)

        # Map column names (handle both camelCase and snake_case)
        column_map = {
            "alertId": "alert_id",
            "diaSourceId": "dia_source_id",
            "diaObjectId": "dia_object_id",
            "decl": "dec",
            "midPointTai": "mjd",
            "filterName": "filter_name",
            "psFlux": "ps_flux",
            "psFluxErr": "ps_flux_err",
            "extendednessMedian": "extendedness_median",
            "extendednessMin": "extendedness_min",
            "extendednessMax": "extendedness_max",
            "hasSSSource": "has_ss_source",
            "ssObjectId": "ss_object_id",
            "ssObjectReassocTimeMjdTai": "ss_object_reassoc_time_mjd",
            "isReassociation": "is_reassociation",
            "reassociationReason": "reassociation_reason",
        }

        # Rename columns if needed
        df = df.rename(columns=column_map)

        # Convert each row to AlertRecord
        for _, row in df.iterrows():
            try:
                # Convert row to dict, handling NaN values
                row_dict = row.dropna().to_dict()

                # Ensure required fields exist
                if "alert_id" not in row_dict:
                    continue
                if "dia_source_id" not in row_dict:
                    continue
                if "ra" not in row_dict:
                    continue
                if "dec" not in row_dict:
                    continue
                if "mjd" not in row_dict:
                    continue

                alert = AlertRecord(**row_dict)
                yield alert
            except Exception:
                # Skip malformed rows
                continue

    def close(self) -> None:
        """Clean up resources."""
        self._files = []
        self._connected = False

    def __repr__(self) -> str:
        """String representation."""
        return f"FileSource({self.path!r})"
