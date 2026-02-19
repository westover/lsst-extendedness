"""
Data Export Utilities.

Export query results to various formats for analysis in other tools.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import structlog

if TYPE_CHECKING:
    import pandas as pd
    from ..storage.sqlite import SQLiteStorage

logger = structlog.get_logger(__name__)

ExportFormat = Literal["csv", "parquet", "json", "excel"]


def export_query(
    storage: "SQLiteStorage",
    query: str,
    output_path: Path | str,
    params: tuple = (),
    format: ExportFormat = "csv",
    **kwargs,
) -> Path:
    """Export query results to file.

    Args:
        storage: SQLiteStorage instance
        query: SQL query
        output_path: Output file path
        params: Query parameters
        format: Output format (csv, parquet, json, excel)
        **kwargs: Additional arguments for pandas export

    Returns:
        Path to exported file
    """
    import pandas as pd

    output_path = Path(output_path)
    rows = storage.query(query, params)
    df = pd.DataFrame(rows)

    return export_dataframe(df, output_path, format, **kwargs)


def export_dataframe(
    df: "pd.DataFrame",
    output_path: Path | str,
    format: ExportFormat = "csv",
    **kwargs,
) -> Path:
    """Export DataFrame to file.

    Args:
        df: DataFrame to export
        output_path: Output file path
        format: Output format
        **kwargs: Additional pandas export arguments

    Returns:
        Path to exported file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if format == "csv":
        df.to_csv(output_path, index=False, **kwargs)
    elif format == "parquet":
        df.to_parquet(output_path, index=False, **kwargs)
    elif format == "json":
        df.to_json(output_path, orient="records", indent=2, **kwargs)
    elif format == "excel":
        df.to_excel(output_path, index=False, **kwargs)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logger.info(
        "data_exported",
        format=format,
        rows=len(df),
        path=str(output_path),
    )

    return output_path


def export_today(
    storage: "SQLiteStorage",
    output_dir: Path | str = "exports",
    format: ExportFormat = "csv",
) -> Path:
    """Export today's alerts.

    Args:
        storage: SQLiteStorage instance
        output_dir: Output directory
        format: Output format

    Returns:
        Path to exported file
    """
    output_dir = Path(output_dir)
    date_str = datetime.utcnow().strftime("%Y%m%d")
    filename = f"alerts_{date_str}.{format}"

    return export_query(
        storage,
        "SELECT * FROM v_today_alerts",
        output_dir / filename,
        format=format,
    )


def export_recent(
    storage: "SQLiteStorage",
    days: int = 7,
    output_dir: Path | str = "exports",
    format: ExportFormat = "csv",
) -> Path:
    """Export recent alerts.

    Args:
        storage: SQLiteStorage instance
        days: Number of days
        output_dir: Output directory
        format: Output format

    Returns:
        Path to exported file
    """
    from ..utils.time import days_ago_mjd

    output_dir = Path(output_dir)
    date_str = datetime.utcnow().strftime("%Y%m%d")
    filename = f"alerts_last{days}d_{date_str}.{format}"

    threshold = days_ago_mjd(days)

    return export_query(
        storage,
        "SELECT * FROM alerts_raw WHERE mjd >= ? ORDER BY mjd DESC",
        output_dir / filename,
        params=(threshold,),
        format=format,
    )


def export_minimoon_candidates(
    storage: "SQLiteStorage",
    output_dir: Path | str = "exports",
    format: ExportFormat = "csv",
) -> Path:
    """Export minimoon candidates.

    Args:
        storage: SQLiteStorage instance
        output_dir: Output directory
        format: Output format

    Returns:
        Path to exported file
    """
    output_dir = Path(output_dir)
    date_str = datetime.utcnow().strftime("%Y%m%d")
    filename = f"minimoon_candidates_{date_str}.{format}"

    return export_query(
        storage,
        "SELECT * FROM v_minimoon_candidates",
        output_dir / filename,
        format=format,
    )


def export_processing_results(
    storage: "SQLiteStorage",
    processor_name: str | None = None,
    output_dir: Path | str = "exports",
    format: ExportFormat = "json",
) -> Path:
    """Export processing results.

    Args:
        storage: SQLiteStorage instance
        processor_name: Optional processor filter
        output_dir: Output directory
        format: Output format

    Returns:
        Path to exported file
    """
    output_dir = Path(output_dir)
    date_str = datetime.utcnow().strftime("%Y%m%d")

    if processor_name:
        filename = f"results_{processor_name}_{date_str}.{format}"
        query = "SELECT * FROM processing_results WHERE processor_name = ?"
        params = (processor_name,)
    else:
        filename = f"results_all_{date_str}.{format}"
        query = "SELECT * FROM processing_results"
        params = ()

    return export_query(
        storage,
        query,
        output_dir / filename,
        params=params,
        format=format,
    )


def export_sso_summary(
    storage: "SQLiteStorage",
    output_dir: Path | str = "exports",
    format: ExportFormat = "csv",
) -> Path:
    """Export SSO alert summary grouped by SSObject.

    Args:
        storage: SQLiteStorage instance
        output_dir: Output directory
        format: Output format

    Returns:
        Path to exported file
    """
    output_dir = Path(output_dir)
    date_str = datetime.utcnow().strftime("%Y%m%d")
    filename = f"sso_summary_{date_str}.{format}"

    query = """
        SELECT
            ss_object_id,
            COUNT(*) as alert_count,
            COUNT(DISTINCT dia_source_id) as source_count,
            MIN(mjd) as first_mjd,
            MAX(mjd) as last_mjd,
            AVG(extendedness_median) as avg_extendedness,
            SUM(CASE WHEN is_reassociation THEN 1 ELSE 0 END) as reassociations
        FROM alerts_raw
        WHERE has_ss_source = 1
        GROUP BY ss_object_id
        ORDER BY alert_count DESC
    """

    return export_query(
        storage,
        query,
        output_dir / filename,
        format=format,
    )


class DataExporter:
    """Convenience class for exporting data.

    Usage:
        exporter = DataExporter(storage)
        exporter.today()
        exporter.recent(days=14)
        exporter.minimoon_candidates()
    """

    def __init__(
        self,
        storage: "SQLiteStorage",
        output_dir: Path | str = "exports",
        default_format: ExportFormat = "csv",
    ):
        """Initialize exporter.

        Args:
            storage: SQLiteStorage instance
            output_dir: Default output directory
            default_format: Default export format
        """
        self.storage = storage
        self.output_dir = Path(output_dir)
        self.default_format = default_format

    def today(self, format: ExportFormat | None = None) -> Path:
        """Export today's alerts."""
        return export_today(
            self.storage,
            self.output_dir,
            format or self.default_format,
        )

    def recent(self, days: int = 7, format: ExportFormat | None = None) -> Path:
        """Export recent alerts."""
        return export_recent(
            self.storage,
            days,
            self.output_dir,
            format or self.default_format,
        )

    def minimoon_candidates(self, format: ExportFormat | None = None) -> Path:
        """Export minimoon candidates."""
        return export_minimoon_candidates(
            self.storage,
            self.output_dir,
            format or self.default_format,
        )

    def processing_results(
        self,
        processor_name: str | None = None,
        format: ExportFormat | None = None,
    ) -> Path:
        """Export processing results."""
        return export_processing_results(
            self.storage,
            processor_name,
            self.output_dir,
            format or self.default_format,
        )

    def sso_summary(self, format: ExportFormat | None = None) -> Path:
        """Export SSO summary."""
        return export_sso_summary(
            self.storage,
            self.output_dir,
            format or self.default_format,
        )

    def custom(
        self,
        query: str,
        filename: str,
        params: tuple = (),
        format: ExportFormat | None = None,
    ) -> Path:
        """Export custom query."""
        return export_query(
            self.storage,
            query,
            self.output_dir / filename,
            params=params,
            format=format or self.default_format,
        )
