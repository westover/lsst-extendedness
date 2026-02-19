"""
LSST Alert Pipeline - Utilities Package
Helper functions and utilities for alert processing
"""

# Import utility modules for easy access
from . import kafka_helpers
from . import cutout_processor
from . import csv_writer

# Import commonly used classes/functions
from .cutout_processor import CutoutProcessor, extract_all_cutouts
from .kafka_helpers import (
    create_consumer,
    test_connection,
    list_topics,
    get_topic_info,
    get_consumer_lag,
    get_message_count_estimate
)
from .csv_writer import (
    CSVWriter,
    DynamicCSVWriter,
    write_csv_with_metadata,
    append_to_csv,
    merge_csv_files,
    split_csv_by_column,
    csv_stats,
    convert_csv_to_json,
    filter_csv
)

__all__ = [
    # Modules
    'kafka_helpers',
    'cutout_processor',
    'csv_writer',
    
    # Classes
    'CutoutProcessor',
    'CSVWriter',
    'DynamicCSVWriter',
    
    # Functions - Kafka
    'create_consumer',
    'test_connection',
    'list_topics',
    'get_topic_info',
    'get_consumer_lag',
    'get_message_count_estimate',
    
    # Functions - Cutouts
    'extract_all_cutouts',
    
    # Functions - CSV
    'write_csv_with_metadata',
    'append_to_csv',
    'merge_csv_files',
    'split_csv_by_column',
    'csv_stats',
    'convert_csv_to_json',
    'filter_csv',
]
