"""
Unit tests for Cutout Extraction and Processing
Tests FITS cutout handling and validation
"""

import unittest
import tempfile
import shutil
from pathlib import Path
import io

import numpy as np
from astropy.io import fits

from tests import get_mock_alert

# Import cutout processor
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.cutout_processor import CutoutProcessor, extract_all_cutouts


def create_mock_fits_cutout(shape=(64, 64), data_mean=100.0, data_std=10.0):
    """
    Create a mock FITS cutout for testing.
    
    Parameters:
    -----------
    shape : tuple
        Image dimensions
    data_mean : float
        Mean pixel value
    data_std : float
        Standard deviation of pixel values
        
    Returns:
    --------
    bytes
        FITS file as bytes
    """
    # Create random data
    data = np.random.normal(data_mean, data_std, shape).astype(np.float32)
    
    # Create FITS HDU
    hdu = fits.PrimaryHDU(data)
    
    # Add some header keywords
    hdu.header['FILTER'] = 'g'
    hdu.header['EXPTIME'] = 30.0
    hdu.header['MJD-OBS'] = 59945.123
    
    # Write to bytes
    bytes_io = io.BytesIO()
    hdu.writeto(bytes_io)
    bytes_io.seek(0)
    
    return bytes_io.read()


class TestCutoutProcessorInitialization(unittest.TestCase):
    """Test CutoutProcessor initialization."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_processor_initialization(self):
        """Test that processor initializes correctly."""
        processor = CutoutProcessor(self.temp_dir)
        
        self.assertIsNotNone(processor)
        self.assertEqual(processor.output_dir, Path(self.temp_dir))
        self.assertTrue(processor.output_dir.exists())


class TestCutoutExtraction(unittest.TestCase):
    """Test cutout extraction functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.processor = CutoutProcessor(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_extract_cutout_basic(self):
        """Test basic cutout extraction."""
        cutout_data = create_mock_fits_cutout()
        output_path = Path(self.temp_dir) / 'test_cutout.fits'
        
        success = self.processor.extract_cutout(cutout_data, output_path)
        
        self.assertTrue(success)
        self.assertTrue(output_path.exists())
    
    def test_extract_cutout_subdirectory(self):
        """Test extraction to subdirectory (auto-creates)."""
        cutout_data = create_mock_fits_cutout()
        output_path = Path(self.temp_dir) / 'subdir' / 'test.fits'
        
        success = self.processor.extract_cutout(cutout_data, output_path)
        
        self.assertTrue(success)
        self.assertTrue(output_path.exists())
        self.assertTrue(output_path.parent.exists())
    
    def test_extract_cutout_invalid_data(self):
        """Test handling of invalid FITS data."""
        invalid_data = b'not a fits file'
        output_path = Path(self.temp_dir) / 'invalid.fits'
        
        success = self.processor.extract_cutout(invalid_data, output_path)
        
        self.assertFalse(success)
    
    def test_extract_multiple_cutouts(self):
        """Test extracting multiple cutouts."""
        cutout_types = ['science', 'template', 'difference']
        
        for cutout_type in cutout_types:
            cutout_data = create_mock_fits_cutout()
            output_path = Path(self.temp_dir) / f'{cutout_type}.fits'
            
            success = self.processor.extract_cutout(cutout_data, output_path)
            self.assertTrue(success)
        
        # Verify all files exist
        for cutout_type in cutout_types:
            self.assertTrue((Path(self.temp_dir) / f'{cutout_type}.fits').exists())


class TestCutoutStatistics(unittest.TestCase):
    """Test cutout statistics extraction."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.processor = CutoutProcessor(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_get_cutout_statistics(self):
        """Test extracting statistics from cutout."""
        # Create cutout with known properties
        cutout_data = create_mock_fits_cutout(
            shape=(32, 32),
            data_mean=100.0,
            data_std=10.0
        )
        cutout_path = Path(self.temp_dir) / 'test.fits'
        self.processor.extract_cutout(cutout_data, cutout_path)
        
        # Get statistics
        stats = self.processor.get_cutout_statistics(cutout_path)
        
        self.assertIsNotNone(stats)
        self.assertEqual(stats['shape'], (32, 32))
        self.assertAlmostEqual(stats['mean'], 100.0, delta=5.0)
        self.assertAlmostEqual(stats['std'], 10.0, delta=2.0)
        self.assertIn('min', stats)
        self.assertIn('max', stats)
        self.assertIn('median', stats)
    
    def test_statistics_with_nan(self):
        """Test statistics calculation with NaN values."""
        # Create cutout with NaN
        data = np.random.normal(100, 10, (32, 32))
        data[0, 0] = np.nan
        
        hdu = fits.PrimaryHDU(data.astype(np.float32))
        bytes_io = io.BytesIO()
        hdu.writeto(bytes_io)
        cutout_data = bytes_io.getvalue()
        
        cutout_path = Path(self.temp_dir) / 'test_nan.fits'
        self.processor.extract_cutout(cutout_data, cutout_path)
        
        stats = self.processor.get_cutout_statistics(cutout_path)
        
        self.assertTrue(stats['has_nan'])
    
    def test_statistics_nonexistent_file(self):
        """Test statistics on nonexistent file."""
        fake_path = Path(self.temp_dir) / 'nonexistent.fits'
        
        stats = self.processor.get_cutout_statistics(fake_path)
        
        self.assertIsNone(stats)


class TestCutoutValidation(unittest.TestCase):
    """Test cutout validation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.processor = CutoutProcessor(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_validate_good_cutout(self):
        """Test validation of valid cutout."""
        cutout_data = create_mock_fits_cutout()
        cutout_path = Path(self.temp_dir) / 'valid.fits'
        self.processor.extract_cutout(cutout_data, cutout_path)
        
        is_valid, error = self.processor.validate_cutout(cutout_path)
        
        self.assertTrue(is_valid)
        self.assertIsNone(error)
    
    def test_validate_nonexistent_file(self):
        """Test validation of nonexistent file."""
        fake_path = Path(self.temp_dir) / 'nonexistent.fits'
        
        is_valid, error = self.processor.validate_cutout(fake_path)
        
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)
        self.assertIn('not exist', error.lower())
    
    def test_validate_empty_file(self):
        """Test validation of empty file."""
        empty_path = Path(self.temp_dir) / 'empty.fits'
        empty_path.touch()
        
        is_valid, error = self.processor.validate_cutout(empty_path)
        
        self.assertFalse(is_valid)
        self.assertIn('empty', error.lower())
    
    def test_validate_corrupted_fits(self):
        """Test validation of corrupted FITS file."""
        corrupted_path = Path(self.temp_dir) / 'corrupted.fits'
        with open(corrupted_path, 'wb') as f:
            f.write(b'not a fits file')
        
        is_valid, error = self.processor.validate_cutout(corrupted_path)
        
        self.assertFalse(is_valid)
        self.assertIsNotNone(error)


class TestCutoutComparison(unittest.TestCase):
    """Test cutout comparison functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.processor = CutoutProcessor(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_compare_cutouts_same_shape(self):
        """Test comparison of cutouts with same shape."""
        # Create three related cutouts
        shape = (32, 32)
        science_data = create_mock_fits_cutout(shape, data_mean=150)
        template_data = create_mock_fits_cutout(shape, data_mean=100)
        diff_data = create_mock_fits_cutout(shape, data_mean=50)
        
        science_path = Path(self.temp_dir) / 'science.fits'
        template_path = Path(self.temp_dir) / 'template.fits'
        diff_path = Path(self.temp_dir) / 'difference.fits'
        
        self.processor.extract_cutout(science_data, science_path)
        self.processor.extract_cutout(template_data, template_path)
        self.processor.extract_cutout(diff_data, diff_path)
        
        results = self.processor.compare_cutouts(
            science_path,
            template_path,
            diff_path
        )
        
        self.assertIn('shape_match', results)
        self.assertTrue(results['shape_match'])
        self.assertEqual(results['shape'], shape)
        self.assertIn('residual_rms', results)


class TestExtractAllCutouts(unittest.TestCase):
    """Test extract_all_cutouts utility function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_extract_all_from_alert(self):
        """Test extracting all cutouts from alert."""
        # Create mock alert with cutouts
        alert = get_mock_alert()
        alert['cutoutScience'] = create_mock_fits_cutout()
        alert['cutoutTemplate'] = create_mock_fits_cutout()
        alert['cutoutDifference'] = create_mock_fits_cutout()
        
        dia_source_id = str(alert['diaSource']['diaSourceId'])
        
        paths = extract_all_cutouts(alert, self.temp_dir, dia_source_id)
        
        self.assertIn('science', paths)
        self.assertIn('template', paths)
        self.assertIn('difference', paths)
        
        # Verify files exist
        for cutout_type, path in paths.items():
            if path:
                self.assertTrue(Path(path).exists())
    
    def test_extract_missing_cutouts(self):
        """Test handling of missing cutouts."""
        alert = get_mock_alert()
        alert['cutoutScience'] = None
        alert['cutoutTemplate'] = create_mock_fits_cutout()
        alert['cutoutDifference'] = None
        
        dia_source_id = str(alert['diaSource']['diaSourceId'])
        
        paths = extract_all_cutouts(alert, self.temp_dir, dia_source_id)
        
        self.assertIsNone(paths['science'])
        self.assertIsNotNone(paths['template'])
        self.assertIsNone(paths['difference'])


class TestCutoutEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.processor = CutoutProcessor(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_large_cutout(self):
        """Test handling of large cutout."""
        # Create large cutout (1024x1024)
        large_cutout = create_mock_fits_cutout(shape=(1024, 1024))
        output_path = Path(self.temp_dir) / 'large.fits'
        
        success = self.processor.extract_cutout(large_cutout, output_path)
        
        self.assertTrue(success)
        self.assertTrue(output_path.exists())
    
    def test_unusual_dimensions(self):
        """Test cutout with unusual dimensions."""
        # Create non-square cutout
        unusual_cutout = create_mock_fits_cutout(shape=(128, 64))
        output_path = Path(self.temp_dir) / 'unusual.fits'
        
        success = self.processor.extract_cutout(unusual_cutout, output_path)
        
        self.assertTrue(success)
        
        stats = self.processor.get_cutout_statistics(output_path)
        self.assertEqual(stats['shape'], (128, 64))
    
    def test_cutout_with_extreme_values(self):
        """Test cutout with extreme pixel values."""
        # Create cutout with very high values
        extreme_cutout = create_mock_fits_cutout(
            data_mean=1e6,
            data_std=1e5
        )
        output_path = Path(self.temp_dir) / 'extreme.fits'
        
        success = self.processor.extract_cutout(extreme_cutout, output_path)
        
        self.assertTrue(success)
        
        stats = self.processor.get_cutout_statistics(output_path)
        self.assertGreater(stats['max'], 1e5)


def suite():
    """Create test suite."""
    suite = unittest.TestSuite()
    
    # Add all test cases
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCutoutProcessorInitialization))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCutoutExtraction))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCutoutStatistics))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCutoutValidation))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCutoutComparison))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestExtractAllCutouts))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCutoutEdgeCases))
    
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
