"""
Built-in processors for the LSST Extendedness Pipeline.

This package contains example and utility processors:

- ExampleProcessor: Template showing how to implement a processor

Add your own processors here or in a separate plugin directory.
"""

from lsst_extendedness.processing.builtin.example import ExampleProcessor

__all__ = [
    "ExampleProcessor",
]
