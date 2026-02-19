"""
Processor Registry with Auto-Discovery.

Provides a registry for post-processors with decorator-based registration
and automatic discovery of processors in plugin directories.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Type

import structlog

if TYPE_CHECKING:
    from .base import BaseProcessor

logger = structlog.get_logger(__name__)

# Global processor registry
_PROCESSORS: dict[str, Type["BaseProcessor"]] = {}


def register_processor(name: str) -> Callable[[Type["BaseProcessor"]], Type["BaseProcessor"]]:
    """Decorator to register a processor class.

    Usage:
        @register_processor("my_analyzer")
        class MyAnalyzer(BaseProcessor):
            ...

    Args:
        name: Unique processor name for CLI/config reference

    Returns:
        Decorator function
    """

    def decorator(cls: Type["BaseProcessor"]) -> Type["BaseProcessor"]:
        if name in _PROCESSORS:
            logger.warning(
                "processor_replaced",
                name=name,
                old_class=_PROCESSORS[name].__name__,
                new_class=cls.__name__,
            )
        _PROCESSORS[name] = cls
        logger.debug("processor_registered", name=name, class_name=cls.__name__)
        return cls

    return decorator


def get_processor(name: str) -> Type["BaseProcessor"] | None:
    """Get processor class by name.

    Args:
        name: Registered processor name

    Returns:
        Processor class or None if not found
    """
    return _PROCESSORS.get(name)


def list_processors() -> dict[str, Type["BaseProcessor"]]:
    """Get all registered processors.

    Returns:
        Dict mapping names to processor classes
    """
    return dict(_PROCESSORS)


def is_processor_registered(name: str) -> bool:
    """Check if a processor is registered.

    Args:
        name: Processor name

    Returns:
        True if registered
    """
    return name in _PROCESSORS


def unregister_processor(name: str) -> bool:
    """Remove a processor from the registry.

    Args:
        name: Processor name

    Returns:
        True if removed, False if not found
    """
    if name in _PROCESSORS:
        del _PROCESSORS[name]
        return True
    return False


def clear_registry() -> None:
    """Clear all registered processors.

    Primarily for testing.
    """
    _PROCESSORS.clear()


def discover_processors(directory: Path | str) -> list[str]:
    """Discover and load processors from a directory.

    Imports all Python files in the directory, which triggers
    @register_processor decorators.

    Args:
        directory: Path to directory containing processor modules

    Returns:
        List of discovered processor names
    """
    directory = Path(directory)
    if not directory.is_dir():
        logger.warning("discovery_skipped", reason="not a directory", path=str(directory))
        return []

    discovered = []
    before = set(_PROCESSORS.keys())

    for py_file in directory.glob("*.py"):
        if py_file.name.startswith("_"):
            continue

        try:
            module_name = f"lsst_extendedness.plugins.{py_file.stem}"
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                logger.debug("plugin_loaded", file=py_file.name)
        except Exception as e:
            logger.error("plugin_load_failed", file=py_file.name, error=str(e))

    after = set(_PROCESSORS.keys())
    discovered = list(after - before)

    if discovered:
        logger.info("processors_discovered", count=len(discovered), names=discovered)

    return discovered


def load_builtin_processors() -> list[str]:
    """Load all builtin processors.

    Imports the builtin package which registers default processors.

    Returns:
        List of loaded processor names
    """
    before = set(_PROCESSORS.keys())

    try:
        from . import builtin  # noqa: F401
    except ImportError as e:
        logger.warning("builtin_load_failed", error=str(e))
        return []

    after = set(_PROCESSORS.keys())
    return list(after - before)


def get_processor_info() -> list[dict[str, str]]:
    """Get info about all registered processors.

    Returns:
        List of dicts with name, version, description
    """
    info = []
    for name, cls in _PROCESSORS.items():
        info.append({
            "name": name,
            "class": cls.__name__,
            "version": getattr(cls, "version", "unknown"),
            "description": getattr(cls, "description", "No description"),
        })
    return info


def print_processors() -> None:
    """Print registered processors to stdout."""
    from rich.console import Console
    from rich.table import Table

    console = Console()
    table = Table(title="Registered Processors")
    table.add_column("Name", style="cyan")
    table.add_column("Version", style="green")
    table.add_column("Description")

    for info in get_processor_info():
        table.add_row(info["name"], info["version"], info["description"])

    console.print(table)
