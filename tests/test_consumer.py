"""
Unit tests for LSST Alert Consumer
Tests consumer initialization, alert processing, and state management
"""

import unittest
import tempfile
import shutil
import json
from pathlib import Path
from datetime import datetime

from tests import get_mock_alert, get_mock_alert_no_sso

# Import the consumer
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.lsst_alert_consumer import LSSTAlertConsumer


class TestConsumerInitialization(unittest.TestCase):
    """Test consumer initialization and setup."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer',
        }
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_consumer_initialization(self):
        """Test that consumer initializes correctly."""
        consumer = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
        
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.base_dir, Path(self.temp_dir))
        self.assertEqual(len(consumer.alert_records), 0)
        self.assertEqual(consumer.stats['messages_processed'], 0)
    
    def test_directory_creation(self):
        """Test that all required directories are created."""
        consumer = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
        
        # Check main directories exist
        self.assertTrue((Path(self.temp_dir) / 'data').exists())
        self.assertTrue((Path(self.temp_dir) / 'logs').exists())
        self.assertTrue((Path(self.temp_dir) / 'temp').exists())
        
        # Check subdirectories
        self.assertTrue((Path(self.temp_dir) / 'data' / 'processed' / 'csv').exists())
        self.assertTrue((Path(self.temp_dir) / 'data' / 'cutouts').exists())
        self.assertTrue((Path(self.temp_dir) / 'temp' / 'failed').exists())
    
    def test_state_file_initialization(self):
        """Test that state file is created."""
        consumer = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
        
        state_file = Path(self.temp_dir) / 'temp' / 'consumer_state.json'
        # State file created on first save, not at init
        self.assertFalse(state_file.exists())
        
        # But the processed_sources dict should be initialized
        self.assertIsInstance(consumer.processed_sources, dict)
        self.assertEqual(len(consumer.processed_sources), 0)


class TestAlertProcessing(unittest.TestCase):
    """Test alert processing functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer',
        }
        self.consumer = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_process_alert_basic(self):
        """Test basic alert processing."""
        alert = get_mock_alert()
        
        record = self.consumer.process_alert(alert)
        
        self.assertIsNotNone(record)
        self.assertEqual(record['diaSourceId'], alert['diaSource']['diaSourceId'])
        self.assertEqual(record['ra'], alert['diaSource']['ra'])
        self.assertEqual(record['dec'], alert['diaSource']['decl'])
    
    def test_process_alert_with_ssobject(self):
        """Test processing alert with SSObject."""
        alert = get_mock_alert()
        
        record = self.consumer.process_alert(alert)
        
        self.assertTrue(record['hasSSSource'])
        self.assertEqual(record['ssObjectId'], 'SSO123456')
        self.assertIsNotNone(record['ssObjectReassocTimeMjdTai'])
    
    def test_process_alert_without_ssobject(self):
        """Test processing alert without SSObject."""
        alert = get_mock_alert_no_sso()
        
        record = self.consumer.process_alert(alert)
        
        self.assertFalse(record['hasSSSource'])
        self.assertIsNone(record['ssObjectId'])
        self.assertIsNone(record['ssObjectReassocTimeMjdTai'])
    
    def test_process_alert_trail_flags(self):
        """Test that trail flags are extracted."""
        alert = get_mock_alert()
        
        record = self.consumer.process_alert(alert)
        
        self.assertIn('trailLength', record)
        self.assertEqual(record['trailLength'], 15.5)
        self.assertIn('trailAngle', record)
        self.assertEqual(record['trailAngle'], 45.2)
    
    def test_process_alert_pixel_flags(self):
        """Test that pixel flags are extracted."""
        alert = get_mock_alert()
        
        record = self.consumer.process_alert(alert)
        
        self.assertIn('pixelFlagsBad', record)
        self.assertFalse(record['pixelFlagsBad'])
        self.assertIn('pixelFlagsCr', record)
        self.assertTrue(record['pixelFlagsCr'])
    
    def test_statistics_update(self):
        """Test that statistics are updated correctly."""
        alert = get_mock_alert()
        
        initial_count = self.consumer.stats['messages_processed']
        self.consumer.process_alert(alert)
        
        self.assertEqual(
            self.consumer.stats['messages_processed'],
            initial_count + 1
        )
        self.assertEqual(self.consumer.stats['new_sources'], 1)


class TestReassociationDetection(unittest.TestCase):
    """Test SSObject reassociation detection."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer',
        }
        self.consumer = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_first_detection_no_reassociation(self):
        """Test that first detection is not marked as reassociation."""
        alert = get_mock_alert()
        
        record = self.consumer.process_alert(alert)
        
        self.assertFalse(record['isReassociation'])
        self.assertIsNone(record['reassociationReason'])
        self.assertEqual(self.consumer.stats['new_sources'], 1)
        self.assertEqual(self.consumer.stats['reassociations_detected'], 0)
    
    def test_new_association_detected(self):
        """Test detection of new SSObject association."""
        # First: alert without SSObject
        alert1 = get_mock_alert_no_sso()
        alert1['diaSource']['diaSourceId'] = 999888777
        
        record1 = self.consumer.process_alert(alert1)
        self.assertFalse(record1['isReassociation'])
        
        # Second: same source, now with SSObject
        alert2 = get_mock_alert()
        alert2['diaSource']['diaSourceId'] = 999888777
        
        record2 = self.consumer.process_alert(alert2)
        
        self.assertTrue(record2['isReassociation'])
        self.assertEqual(record2['reassociationReason'], 'new_association')
        self.assertEqual(self.consumer.stats['reassociations_detected'], 1)
    
    def test_changed_association_detected(self):
        """Test detection of changed SSObject association."""
        # First: alert with SSObject A
        alert1 = get_mock_alert()
        alert1['diaSource']['diaSourceId'] = 123321123
        alert1['ssObject']['ssObjectId'] = 'SSO_A'
        
        record1 = self.consumer.process_alert(alert1)
        self.assertFalse(record1['isReassociation'])
        
        # Second: same source, different SSObject
        alert2 = get_mock_alert()
        alert2['diaSource']['diaSourceId'] = 123321123
        alert2['ssObject']['ssObjectId'] = 'SSO_B'
        
        record2 = self.consumer.process_alert(alert2)
        
        self.assertTrue(record2['isReassociation'])
        self.assertEqual(record2['reassociationReason'], 'changed_association')
        self.assertEqual(self.consumer.stats['reassociations_detected'], 1)
    
    def test_updated_reassociation_detected(self):
        """Test detection of updated reassociation timestamp."""
        # First: alert with SSObject
        alert1 = get_mock_alert()
        alert1['diaSource']['diaSourceId'] = 456654456
        alert1['ssObject']['ssObjectReassocTimeMjdTai'] = 59945.100
        
        record1 = self.consumer.process_alert(alert1)
        self.assertFalse(record1['isReassociation'])
        
        # Second: same source, updated reassoc time
        alert2 = get_mock_alert()
        alert2['diaSource']['diaSourceId'] = 456654456
        alert2['ssObject']['ssObjectReassocTimeMjdTai'] = 59945.200
        
        record2 = self.consumer.process_alert(alert2)
        
        self.assertTrue(record2['isReassociation'])
        self.assertEqual(record2['reassociationReason'], 'updated_reassociation')


class TestStateManagement(unittest.TestCase):
    """Test state file management."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer',
        }
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_state_save(self):
        """Test that state is saved correctly."""
        consumer = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
        
        # Process an alert
        alert = get_mock_alert()
        consumer.process_alert(alert)
        
        # Save state
        consumer._save_state()
        
        # Check state file exists
        state_file = Path(self.temp_dir) / 'temp' / 'consumer_state.json'
        self.assertTrue(state_file.exists())
        
        # Load and verify
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        self.assertIn('processed_sources', state)
        dia_source_id = str(alert['diaSource']['diaSourceId'])
        self.assertIn(dia_source_id, state['processed_sources'])
    
    def test_state_load(self):
        """Test that state is loaded correctly."""
        # Create initial consumer and process alert
        consumer1 = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
        
        alert = get_mock_alert()
        consumer1.process_alert(alert)
        consumer1._save_state()
        
        # Create new consumer (simulates restart)
        consumer2 = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
        
        # Check that state was loaded
        dia_source_id = str(alert['diaSource']['diaSourceId'])
        self.assertIn(dia_source_id, consumer2.processed_sources)
        
        # Process same alert again - should detect as reassociation
        alert2 = get_mock_alert()
        alert2['ssObject']['ssObjectId'] = 'SSO_DIFFERENT'
        record = consumer2.process_alert(alert2)
        
        self.assertTrue(record['isReassociation'])


class TestCSVWriting(unittest.TestCase):
    """Test CSV file writing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer',
        }
        self.consumer = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_csv_batch_writing(self):
        """Test that CSV is written when batch is full."""
        # Process multiple alerts
        for i in range(5):
            alert = get_mock_alert()
            alert['diaSource']['diaSourceId'] = 1000000 + i
            record = self.consumer.process_alert(alert)
            self.consumer.alert_records.append(record)
        
        # Save to CSV
        self.consumer.save_to_csv()
        
        # Check CSV was created
        csv_file = self.consumer._get_csv_filepath()
        self.assertTrue(csv_file.exists())
        
        # Verify record count
        self.assertEqual(self.consumer.stats['csv_rows_written'], 5)
        self.assertEqual(len(self.consumer.alert_records), 0)  # Buffer cleared


class TestErrorHandling(unittest.TestCase):
    """Test error handling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer',
        }
        self.consumer = LSSTAlertConsumer(
            self.kafka_config,
            base_dir=self.temp_dir
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_malformed_alert(self):
        """Test handling of malformed alert."""
        alert = {'invalid': 'alert'}
        
        record = self.consumer.process_alert(alert)
        
        # Should return None and increment failed count
        self.assertIsNone(record)
        self.assertEqual(self.consumer.stats['messages_failed'], 1)
    
    def test_missing_required_fields(self):
        """Test handling of alert with missing required fields."""
        alert = get_mock_alert()
        del alert['diaSource']['diaSourceId']
        
        record = self.consumer.process_alert(alert)
        
        # Should still process but with 'unknown' ID
        self.assertIsNotNone(record)


def suite():
    """Create test suite."""
    suite = unittest.TestSuite()
    
    # Add all test cases
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestConsumerInitialization))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestAlertProcessing))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestReassociationDetection))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestStateManagement))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCSVWriting))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestErrorHandling))
    
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
