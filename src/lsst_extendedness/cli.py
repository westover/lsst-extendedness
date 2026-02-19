"""
Command-line interface for the LSST Extendedness Pipeline.

This module provides the main CLI entry points using Click.

Commands:
- ingest: Run the ingestion pipeline
- query: Interactive query shell
- process: Run post-processing
- db-init: Initialize database
- db-stats: Show database statistics

Example:
    $ lsst-extendedness --help
    $ lsst-extendedness ingest --config config/local.toml
    $ lsst-extendedness db-stats
"""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from lsst_extendedness import __version__
from lsst_extendedness.config import get_settings, load_settings
from lsst_extendedness.storage import SQLiteStorage
from lsst_extendedness.utils.logging import setup_logging, get_logger

console = Console()


@click.group()
@click.version_option(version=__version__, prog_name="lsst-extendedness")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Configuration file path",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.pass_context
def main(ctx: click.Context, config: Path | None, verbose: bool) -> None:
    """LSST Extendedness Pipeline CLI.

    Process LSST astronomical alerts with focus on extendedness analysis
    and solar system object reassociation detection.
    """
    ctx.ensure_object(dict)

    # Load settings
    if config:
        settings = load_settings(config)
    else:
        settings = get_settings()

    ctx.obj["settings"] = settings
    ctx.obj["verbose"] = verbose

    # Setup logging
    log_level = "DEBUG" if verbose else settings.logging.level
    setup_logging(level=log_level, format=settings.logging.format)


@main.command("db-init")
@click.pass_context
def db_init(ctx: click.Context) -> None:
    """Initialize the database schema.

    Creates all tables, indexes, and views if they don't exist.
    Safe to run multiple times.
    """
    settings = ctx.obj["settings"]
    logger = get_logger(__name__)

    db_path = settings.database_path
    console.print(f"Initializing database: [cyan]{db_path}[/cyan]")

    # Ensure directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    storage = SQLiteStorage(db_path)
    storage.initialize()

    logger.info("Database initialized", path=str(db_path))
    console.print("[green]✓[/green] Database initialized successfully")

    storage.close()


@main.command("db-stats")
@click.pass_context
def db_stats(ctx: click.Context) -> None:
    """Show database statistics."""
    settings = ctx.obj["settings"]

    db_path = settings.database_path

    if not db_path.exists():
        console.print(f"[red]Database not found:[/red] {db_path}")
        console.print("Run 'lsst-extendedness db-init' to create it.")
        return

    storage = SQLiteStorage(db_path)
    stats = storage.get_stats()

    # Create table
    table = Table(title="Database Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")

    table.add_row("Database Path", str(db_path))
    table.add_row("File Size", f"{stats.get('file_size_mb', 0):.2f} MB")
    table.add_row("", "")
    table.add_row("Alerts (raw)", f"{stats.get('alerts_raw_count', 0):,}")
    table.add_row("Alerts (filtered)", f"{stats.get('alerts_filtered_count', 0):,}")
    table.add_row("Processed Sources", f"{stats.get('processed_sources_count', 0):,}")
    table.add_row("Processing Results", f"{stats.get('processing_results_count', 0):,}")
    table.add_row("Ingestion Runs", f"{stats.get('ingestion_runs_count', 0):,}")
    table.add_row("", "")
    table.add_row("SSO Alerts", f"{stats.get('sso_alerts', 0):,}")
    table.add_row("Reassociations", f"{stats.get('reassociations', 0):,}")

    if "mjd_range" in stats:
        mjd_range = stats["mjd_range"]
        table.add_row("", "")
        table.add_row("MJD Range", f"{mjd_range['min']:.2f} - {mjd_range['max']:.2f}")

    console.print(table)
    storage.close()


@main.command("db-shell")
@click.pass_context
def db_shell(ctx: click.Context) -> None:
    """Open interactive SQLite shell."""
    import subprocess

    settings = ctx.obj["settings"]
    db_path = settings.database_path

    if not db_path.exists():
        console.print(f"[red]Database not found:[/red] {db_path}")
        return

    console.print(f"Opening SQLite shell for: [cyan]{db_path}[/cyan]")
    console.print("Type '.help' for help, '.exit' to quit")

    subprocess.run(["sqlite3", str(db_path)])


@main.command("ingest")
@click.option(
    "--source",
    type=click.Choice(["kafka", "file", "mock"]),
    default="kafka",
    help="Data source type",
)
@click.option(
    "--topic",
    help="Kafka topic (overrides config)",
)
@click.option(
    "--path",
    type=click.Path(exists=True, path_type=Path),
    help="File path for file source",
)
@click.option(
    "--count",
    type=int,
    default=100,
    help="Number of alerts for mock source",
)
@click.option(
    "--max-messages",
    type=int,
    help="Maximum messages to process",
)
@click.option(
    "--duration",
    type=int,
    help="Maximum runtime in seconds",
)
@click.pass_context
def ingest(
    ctx: click.Context,
    source: str,
    topic: str | None,
    path: Path | None,
    count: int,
    max_messages: int | None,
    duration: int | None,
) -> None:
    """Run the ingestion pipeline.

    Consumes alerts from the specified source and writes to the database.
    """
    settings = ctx.obj["settings"]
    logger = get_logger(__name__)

    console.print(f"[bold]LSST Extendedness Pipeline - Ingestion[/bold]")
    console.print(f"Source: [cyan]{source}[/cyan]")

    # Initialize storage
    db_path = settings.database_path
    storage = SQLiteStorage(db_path)
    storage.initialize()

    # Create source
    if source == "mock":
        from lsst_extendedness.sources import MockSource
        alert_source = MockSource(count=count)
        console.print(f"Generating {count} mock alerts")
    elif source == "file":
        if path is None:
            console.print("[red]Error:[/red] --path required for file source")
            return
        from lsst_extendedness.sources import FileSource
        alert_source = FileSource(path)
        console.print(f"Reading from: {path}")
    else:
        from lsst_extendedness.sources import KafkaSource
        kafka_topic = topic or settings.kafka.topic
        alert_source = KafkaSource(
            settings.kafka.to_consumer_config(),
            topic=kafka_topic,
        )
        console.print(f"Consuming from topic: [cyan]{kafka_topic}[/cyan]")

    # Connect and process
    alert_source.connect()

    from lsst_extendedness.models import IngestionRun

    run = IngestionRun(source_name=source)
    storage.write_ingestion_run(run)

    limit = max_messages or settings.ingestion.max_messages
    batch = []
    batch_size = settings.ingestion.batch_size

    try:
        with console.status("[bold green]Processing alerts...") as status:
            for alert in alert_source.fetch_alerts(limit=limit):
                batch.append(alert)
                run.alerts_ingested += 1

                if len(batch) >= batch_size:
                    storage.write_batch(batch)
                    status.update(f"Processed {run.alerts_ingested:,} alerts")
                    batch = []

            # Write remaining
            if batch:
                storage.write_batch(batch)

        run.complete()

    except Exception as e:
        run.fail(str(e))
        logger.error("Ingestion failed", error=str(e))
        console.print(f"[red]Error:[/red] {e}")

    finally:
        storage.write_ingestion_run(run)
        alert_source.close()
        storage.close()

    # Show summary
    console.print()
    console.print("[bold]Ingestion Complete[/bold]")
    console.print(f"  Alerts ingested: [green]{run.alerts_ingested:,}[/green]")
    console.print(f"  Status: [{'green' if run.is_complete else 'red'}]{run.status.value}[/]")

    if run.duration_seconds:
        rate = run.alerts_ingested / run.duration_seconds if run.duration_seconds > 0 else 0
        console.print(f"  Duration: {run.duration_seconds:.1f}s ({rate:.0f} alerts/sec)")


@main.command("health-check")
@click.pass_context
def health_check(ctx: click.Context) -> None:
    """Run system health check."""
    settings = ctx.obj["settings"]

    console.print("[bold]System Health Check[/bold]")
    console.print()

    # Check Python
    import sys
    console.print(f"Python: [green]{sys.version.split()[0]}[/green]")

    # Check database
    db_path = settings.database_path
    if db_path.exists():
        storage = SQLiteStorage(db_path)
        count = storage.get_alert_count()
        console.print(f"Database: [green]OK[/green] ({count:,} alerts)")
        storage.close()
    else:
        console.print(f"Database: [yellow]Not initialized[/yellow]")

    # Check Kafka (optional)
    try:
        import confluent_kafka  # noqa: F401
        console.print("Kafka client: [green]Installed[/green]")
    except ImportError:
        console.print("Kafka client: [yellow]Not installed[/yellow]")

    console.print()
    console.print("[green]Health check complete[/green]")


# Entry points for scripts
def ingest_cli() -> None:
    """Entry point for lsst-ingest command."""
    main(["ingest"])


def query_cli() -> None:
    """Entry point for lsst-query command."""
    main(["query"])


def process_cli() -> None:
    """Entry point for lsst-process command."""
    main(["process"])


if __name__ == "__main__":
    main()
