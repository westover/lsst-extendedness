"""
Built-in processors for the LSST Extendedness Pipeline.

This package contains example and utility processors:

- ExampleProcessor: Template showing basic statistics
- MiniMoonCandidateProcessor: Identify minimoon candidates
- SourceSummaryProcessor: Aggregate alerts by source
- ReassociationTracker: Track SSObject reassociations

Add your own processors here or in a separate plugin directory.
"""

from lsst_extendedness.processing.builtin.example import (
    ExampleProcessor,
    MiniMoonCandidateProcessor,
    ReassociationTracker,
    SourceSummaryProcessor,
)

__all__ = [
    "ExampleProcessor",
    "MiniMoonCandidateProcessor",
    "ReassociationTracker",
    "SourceSummaryProcessor",
]
