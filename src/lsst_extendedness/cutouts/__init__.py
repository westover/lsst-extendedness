"""
FITS cutout processing for the LSST Extendedness Pipeline.

This module handles extraction and management of FITS cutouts from alerts:

- Extract science, template, and difference cutouts
- Validate FITS data integrity
- Generate thumbnails (optional)
- Calculate cutout statistics

Cutout Types:
- Science: Direct observation image
- Template: Reference image for comparison
- Difference: Subtraction result showing transients

Example:
    >>> from lsst_extendedness.cutouts import CutoutProcessor
    >>>
    >>> processor = CutoutProcessor(output_dir="data/cutouts")
    >>> paths = processor.extract_all(alert, dia_source_id="12345")
    >>> print(f"Saved cutouts to: {paths}")
"""

from lsst_extendedness.cutouts.processor import CutoutProcessor, extract_all_cutouts

__all__ = [
    "CutoutProcessor",
    "extract_all_cutouts",
]
