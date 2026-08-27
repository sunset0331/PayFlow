import unittest
import json
import logging
import io
from shared.logger import get_logger

class TestLogger(unittest.TestCase):
    def setUp(self):
        self.stream = io.StringIO()
        self.logger = get_logger("test_service")
        # Override the handler for testing
        self.logger.logger.handlers.clear()
        handler = logging.StreamHandler(self.stream)
        
        # We need to import JSONFormatter again to apply it
        from shared.logger import JSONFormatter
        handler.setFormatter(JSONFormatter())
        self.logger.logger.addHandler(handler)

    def test_basic_log(self):
        self.logger.info("Test message")
        log_output = self.stream.getvalue().strip()
        parsed = json.loads(log_output)
        
        self.assertIn("timestamp", parsed)
        self.assertEqual(parsed["level"], "INFO")
        self.assertEqual(parsed["service"], "test_service")
        self.assertEqual(parsed["message"], "Test message")

    def test_log_with_extra(self):
        self.logger.info("Test with extra", extra={"txn_id": "123", "event": "TEST_EVENT", "action": "CREATE"})
        log_output = self.stream.getvalue().strip()
        parsed = json.loads(log_output)
        
        self.assertEqual(parsed["message"], "Test with extra")
        self.assertEqual(parsed["txn_id"], "123")
        self.assertEqual(parsed["event"], "TEST_EVENT")
        self.assertEqual(parsed["action"], "CREATE")

    def test_log_with_error(self):
        try:
            1 / 0
        except ZeroDivisionError as e:
            self.logger.error("Something broke", exc_info=True)
            
        log_output = self.stream.getvalue().strip()
        parsed = json.loads(log_output)
        
        self.assertEqual(parsed["level"], "ERROR")
        self.assertEqual(parsed["message"], "Something broke")
        self.assertIn("error", parsed)
        self.assertEqual(parsed["error"]["type"], "ZeroDivisionError")
        self.assertIn("division by zero", parsed["error"]["message"])
        self.assertIn("Traceback", parsed["error"]["traceback"])
