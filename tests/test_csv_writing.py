"""
Unit tests for CSV Writing Utilities
Tests CSV writer classes and utility functions
"""

import unittest
import tempfile
import shutil
from pathlib import Path

import pandas as pd

# Import CSV writer utilities
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.csv_writer import (
    CSVWriter,
    DynamicCSVWriter,
    write_csv_with_metadata,
    append_to_csv,
    merge_csv_files,
    split_csv_by_column,
    csv_stats,
    filter_csv
)


class TestCSVWriter(unittest.TestCase):
    """Test CSVWriter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.writer = CSVWriter(self.temp_dir, batch_size=3)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_writer_initialization(self):
        """Test writer initializes correctly."""
        self.assertEqual(self.writer.batch_size, 3)
        self.assertEqual(len(self.writer.record_buffer), 0)
        self.assertEqual(self.writer.rows_written, 0)
    
    def test_add_record(self):
        """Test adding records to buffer."""
        record = {'diaSourceId': 123, 'ra': 180.0}
        
        flushed = self.writer.add_record(record)
        
        self.assertFalse(flushed)  # Not flushed yet
        self.assertEqual(len(self.writer.record_buffer), 1)
    
    def test_auto_flush_on_batch_size(self):
        """Test automatic flush when batch size reached."""
        # Add records up to batch size
        for i in range(3):
            record = {'diaSourceId': 100 + i, 'ra': 180.0 + i}
            flushed = self.writer.add_record(record)
        
        # Third record should trigger flush
        self.assertTrue(flushed)
        self.assertEqual(len(self.writer.record_buffer), 0)
        self.assertEqual(self.writer.rows_written, 3)
    
    def test_manual_flush(self):
        """Test manual flush."""
        record = {'diaSourceId': 123, 'ra': 180.0}
        self.writer.add_record(record)
        
        rows = self.writer.flush()
        
        self.assertEqual(rows, 1)
        self.assertEqual(len(self.writer.record_buffer), 0)
    
    def test_flush_empty_buffer(self):
        """Test flushing empty buffer."""
        rows = self.writer.flush()
        
        self.assertEqual(rows, 0)
    
    def test_csv_file_created(self):
        """Test that CSV file is created."""
        record = {'diaSourceId': 123, 'ra': 180.0}
        self.writer.add_record(record)
        self.writer.flush()
        
        # Check file exists
        csv_files = list(Path(self.temp_dir).glob('*.csv'))
        self.assertEqual(len(csv_files), 1)
        
        # Verify contents
        df = pd.read_csv(csv_files[0])
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['diaSourceId'], 123)


class TestDynamicCSVWriter(unittest.TestCase):
    """Test DynamicCSVWriter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.writer = DynamicCSVWriter(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_dynamic_columns_tracking(self):
        """Test that columns are tracked dynamically."""
        record1 = {'diaSourceId': 123, 'ra': 180.0}
        record2 = {'diaSourceId': 456, 'dec': 45.0}
        
        self.writer.add_record(record1)
        self.writer.add_record(record2)
        
        columns = self.writer.get_column_list()
        
        self.assertIn('diaSourceId', columns)
        self.assertIn('ra', columns)
        self.assertIn('dec', columns)
    
    def test_dynamic_columns_in_output(self):
        """Test that all columns appear in output."""
        record1 = {'diaSourceId': 123, 'ra': 180.0}
        record2 = {'diaSourceId': 456, 'dec': 45.0, 'snr': 10.5}
        
        self.writer.add_record(record1)
        self.writer.add_record(record2)
        
        output_file = Path(self.temp_dir) / 'test.csv'
        self.writer.flush(output_file)
        
        # Verify all columns present
        df = pd.read_csv(output_file)
        self.assertIn('diaSourceId', df.columns)
        self.assertIn('ra', df.columns)
        self.assertIn('dec', df.columns)
        self.assertIn('snr', df.columns)
        
        # Check NaN for missing values
        self.assertTrue(pd.isna(df.iloc[0]['dec']))
        self.assertTrue(pd.isna(df.iloc[1]['ra']))


class TestWriteCSVWithMetadata(unittest.TestCase):
    """Test write_csv_with_metadata function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_write_with_metadata(self):
        """Test writing CSV with metadata comments."""
        records = [
            {'diaSourceId': 123, 'ra': 180.0},
            {'diaSourceId': 456, 'ra': 90.0},
        ]
        
        metadata = {
            'Version': '2.0.0',
            'Date': '2026-02-12',
        }
        
        output_file = Path(self.temp_dir) / 'test.csv'
        rows = write_csv_with_metadata(records, output_file, metadata)
        
        self.assertEqual(rows, 2)
        self.assertTrue(output_file.exists())
        
        # Check metadata is in file
        with open(output_file, 'r') as f:
            content = f.read()
        
        self.assertIn('# Version: 2.0.0', content)
        self.assertIn('# Date: 2026-02-12', content)
    
    def test_write_without_metadata(self):
        """Test writing without metadata."""
        records = [{'diaSourceId': 123}]
        output_file = Path(self.temp_dir) / 'test.csv'
        
        rows = write_csv_with_metadata(records, output_file)
        
        self.assertEqual(rows, 1)
        self.assertTrue(output_file.exists())


class TestAppendToCSV(unittest.TestCase):
    """Test append_to_csv function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_append_to_existing_file(self):
        """Test appending to existing file."""
        output_file = Path(self.temp_dir) / 'test.csv'
        
        # Create initial file
        initial = [{'diaSourceId': 123, 'ra': 180.0}]
        df = pd.DataFrame(initial)
        df.to_csv(output_file, index=False)
        
        # Append new records
        new_records = [{'diaSourceId': 456, 'ra': 90.0}]
        rows = append_to_csv(new_records, output_file)
        
        self.assertEqual(rows, 1)
        
        # Verify total rows
        df_final = pd.read_csv(output_file)
        self.assertEqual(len(df_final), 2)
    
    def test_create_if_missing(self):
        """Test creating file if it doesn't exist."""
        output_file = Path(self.temp_dir) / 'new.csv'
        
        records = [{'diaSourceId': 123}]
        rows = append_to_csv(records, output_file, create_if_missing=True)
        
        self.assertEqual(rows, 1)
        self.assertTrue(output_file.exists())
    
    def test_fail_if_missing(self):
        """Test failing when file doesn't exist."""
        output_file = Path(self.temp_dir) / 'missing.csv'
        
        records = [{'diaSourceId': 123}]
        rows = append_to_csv(records, output_file, create_if_missing=False)
        
        self.assertEqual(rows, 0)


class TestMergeCSVFiles(unittest.TestCase):
    """Test merge_csv_files function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_merge_multiple_files(self):
        """Test merging multiple CSV files."""
        # Create test files
        file1 = Path(self.temp_dir) / 'file1.csv'
        file2 = Path(self.temp_dir) / 'file2.csv'
        
        df1 = pd.DataFrame([{'diaSourceId': 123, 'ra': 180.0}])
        df2 = pd.DataFrame([{'diaSourceId': 456, 'ra': 90.0}])
        
        df1.to_csv(file1, index=False)
        df2.to_csv(file2, index=False)
        
        # Merge
        output_file = Path(self.temp_dir) / 'merged.csv'
        rows = merge_csv_files([file1, file2], output_file)
        
        self.assertEqual(rows, 2)
        
        # Verify merged content
        df_merged = pd.read_csv(output_file)
        self.assertEqual(len(df_merged), 2)
    
    def test_merge_with_deduplication(self):
        """Test merging with duplicate removal."""
        # Create files with duplicates
        file1 = Path(self.temp_dir) / 'file1.csv'
        file2 = Path(self.temp_dir) / 'file2.csv'
        
        df1 = pd.DataFrame([
            {'diaSourceId': 123, 'ra': 180.0},
            {'diaSourceId': 456, 'ra': 90.0},
        ])
        df2 = pd.DataFrame([
            {'diaSourceId': 123, 'ra': 180.0},  # Duplicate
            {'diaSourceId': 789, 'ra': 45.0},
        ])
        
        df1.to_csv(file1, index=False)
        df2.to_csv(file2, index=False)
        
        # Merge with deduplication
        output_file = Path(self.temp_dir) / 'merged.csv'
        rows = merge_csv_files(
            [file1, file2],
            output_file,
            dedupe_column='diaSourceId'
        )
        
        self.assertEqual(rows, 3)  # Should have 3 unique sources
    
    def test_merge_with_sorting(self):
        """Test merging with sorting."""
        file1 = Path(self.temp_dir) / 'file1.csv'
        file2 = Path(self.temp_dir) / 'file2.csv'
        
        df1 = pd.DataFrame([{'id': 3, 'value': 'c'}])
        df2 = pd.DataFrame([{'id': 1, 'value': 'a'}])
        
        df1.to_csv(file1, index=False)
        df2.to_csv(file2, index=False)
        
        output_file = Path(self.temp_dir) / 'merged.csv'
        merge_csv_files([file1, file2], output_file, sort_by='id')
        
        df_merged = pd.read_csv(output_file)
        self.assertEqual(df_merged.iloc[0]['id'], 1)
        self.assertEqual(df_merged.iloc[1]['id'], 3)


class TestSplitCSVByColumn(unittest.TestCase):
    """Test split_csv_by_column function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_split_by_column(self):
        """Test splitting CSV by column value."""
        # Create input file
        input_file = Path(self.temp_dir) / 'input.csv'
        df = pd.DataFrame([
            {'diaSourceId': 123, 'filterName': 'g'},
            {'diaSourceId': 456, 'filterName': 'r'},
            {'diaSourceId': 789, 'filterName': 'g'},
        ])
        df.to_csv(input_file, index=False)
        
        # Split
        output_dir = Path(self.temp_dir) / 'split'
        output_files = split_csv_by_column(
            input_file,
            output_dir,
            split_column='filterName',
            prefix='filter'
        )
        
        self.assertEqual(len(output_files), 2)
        self.assertIn('g', output_files)
        self.assertIn('r', output_files)
        
        # Verify split files
        df_g = pd.read_csv(output_files['g'])
        self.assertEqual(len(df_g), 2)
        
        df_r = pd.read_csv(output_files['r'])
        self.assertEqual(len(df_r), 1)


class TestCSVStats(unittest.TestCase):
    """Test csv_stats function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_basic_stats(self):
        """Test basic CSV statistics."""
        # Create test file
        csv_file = Path(self.temp_dir) / 'test.csv'
        df = pd.DataFrame([
            {'diaSourceId': 123, 'ra': 180.0, 'snr': 10.5},
            {'diaSourceId': 456, 'ra': 90.0, 'snr': 20.3},
        ])
        df.to_csv(csv_file, index=False)
        
        stats = csv_stats(csv_file)
        
        self.assertEqual(stats['rows'], 2)
        self.assertEqual(stats['columns'], 3)
        self.assertIn('column_names', stats)
        self.assertIn('file_size_kb', stats)
    
    def test_stats_with_dynamic_columns(self):
        """Test stats with trail* and pixelFlags* columns."""
        csv_file = Path(self.temp_dir) / 'test.csv'
        df = pd.DataFrame([{
            'diaSourceId': 123,
            'trailLength': 15.5,
            'trailAngle': 45.0,
            'pixelFlagsBad': False,
            'pixelFlagsCr': True,
        }])
        df.to_csv(csv_file, index=False)
        
        stats = csv_stats(csv_file)
        
        self.assertIn('trail_columns', stats)
        self.assertIn('trailLength', stats['trail_columns'])
        self.assertIn('pixel_flag_columns', stats)
        self.assertIn('pixelFlagsBad', stats['pixel_flag_columns'])
    
    def test_stats_with_reassociations(self):
        """Test stats with reassociation data."""
        csv_file = Path(self.temp_dir) / 'test.csv'
        df = pd.DataFrame([
            {'diaSourceId': 123, 'isReassociation': True, 'hasSSSource': True},
            {'diaSourceId': 456, 'isReassociation': False, 'hasSSSource': False},
        ])
        df.to_csv(csv_file, index=False)
        
        stats = csv_stats(csv_file)
        
        self.assertIn('reassociations', stats)
        self.assertEqual(stats['reassociations'], 1)
        self.assertIn('with_ssobject', stats)
        self.assertEqual(stats['with_ssobject'], 1)


class TestFilterCSV(unittest.TestCase):
    """Test filter_csv function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_basic_filtering(self):
        """Test basic CSV filtering."""
        # Create input file
        input_file = Path(self.temp_dir) / 'input.csv'
        df = pd.DataFrame([
            {'diaSourceId': 123, 'snr': 5.0},
            {'diaSourceId': 456, 'snr': 15.0},
            {'diaSourceId': 789, 'snr': 25.0},
        ])
        df.to_csv(input_file, index=False)
        
        # Filter for high SNR
        output_file = Path(self.temp_dir) / 'filtered.csv'
        rows = filter_csv(
            input_file,
            output_file,
            lambda row: row['snr'] >= 10.0
        )
        
        self.assertEqual(rows, 2)
        
        # Verify filtered content
        df_filtered = pd.read_csv(output_file)
        self.assertEqual(len(df_filtered), 2)
        self.assertTrue(all(df_filtered['snr'] >= 10.0))


def suite():
    """Create test suite."""
    suite = unittest.TestSuite()
    
    # Add all test cases
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCSVWriter))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDynamicCSVWriter))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestWriteCSVWithMetadata))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestAppendToCSV))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestMergeCSVFiles))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSplitCSVByColumn))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCSVStats))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFilterCSV))
    
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
