"""
FITS cutout processing for the LSST Extendedness Pipeline.

This module handles extraction and management of FITS cutouts from alerts:

- Extract science, template, and difference cutouts
- Validate FITS data integrity
- Organize by date or object
- Compress for storage efficiency

Cutout Types:
- Science: Direct observation image
- Template: Reference image for comparison
- Difference: Subtraction result showing transients

Example:
    >>> from lsst_extendedness.cutouts import CutoutProcessor, CutoutConfig
    >>>
    >>> config = CutoutConfig(output_dir=Path("data/cutouts"))
    >>> processor = CutoutProcessor(config)
    >>>
    >>> paths = processor.process_alert(alert, avro_record)
    >>> print(f"Science cutout: {paths.science}")
    >>> print(f"Template cutout: {paths.template}")
    >>> print(f"Difference cutout: {paths.difference}")

Batch processing:
    >>> results = processor.process_batch([(alert1, avro1), (alert2, avro2)])

Loading cutouts:
    >>> from lsst_extendedness.cutouts import load_cutout_as_array
    >>> image_data = load_cutout_as_array(paths.science)
"""

from lsst_extendedness.cutouts.processor import (
    CutoutConfig,
    CutoutPaths,
    CutoutProcessor,
    extract_cutout_stamps,
    load_cutout_as_array,
)

__all__ = [
    "CutoutConfig",
    "CutoutPaths",
    "CutoutProcessor",
    "extract_cutout_stamps",
    "load_cutout_as_array",
]
