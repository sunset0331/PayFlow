import unittest
import asyncio
from prometheus_client import REGISTRY

import services.bank_hdfc.main as hdfc
import services.ledger.main as ledger
import services.notifications.main as notification
import services.gateway.main as gateway
import services.gateway.recovery as recovery

class TestMetrics(unittest.TestCase):
    def test_metrics_registered(self):
        # Verify that the expected metrics exist in the Prometheus registry
        
        # Bank
        self.assertIn('bank_requests_total', REGISTRY._names_to_collectors)
        self.assertIn('bank_operation_duration_seconds', REGISTRY._names_to_collectors)
        
        # Ledger
        self.assertIn('ledger_events_total', REGISTRY._names_to_collectors)
        self.assertIn('ledger_dlq_total', REGISTRY._names_to_collectors)
        self.assertIn('ledger_event_processing_duration_seconds', REGISTRY._names_to_collectors)
        
        # Notification
        self.assertIn('notification_events_total', REGISTRY._names_to_collectors)
        self.assertIn('notification_processing_duration_seconds', REGISTRY._names_to_collectors)
        
        # Gateway
        self.assertIn('gateway_saga_states_total', REGISTRY._names_to_collectors)
        self.assertIn('payflow_transactions_total', REGISTRY._names_to_collectors)
        
        # Recovery
        self.assertIn('gateway_recovery_total', REGISTRY._names_to_collectors)
        self.assertIn('gateway_recovery_duration_seconds', REGISTRY._names_to_collectors)
        self.assertIn('gateway_stale_sagas', REGISTRY._names_to_collectors)

    def test_no_txn_id_labels(self):
        # We explicitly verify that NO registered metric uses txn_id as a label.
        # This is a critical requirement to avoid high cardinality issues.
        for name, collector in REGISTRY._names_to_collectors.items():
            # Standard prometheus_client metrics have '_labelnames'
            if hasattr(collector, '_labelnames'):
                labels = collector._labelnames
                self.assertNotIn('txn_id', labels, f"Metric {name} uses txn_id as a label!")
                self.assertNotIn('sender_vpa', labels, f"Metric {name} uses sender_vpa as a label!")
                self.assertNotIn('receiver_vpa', labels, f"Metric {name} uses receiver_vpa as a label!")
                self.assertNotIn('account', labels, f"Metric {name} uses account as a label!")

    def test_bank_counter_logic(self):
        # Test incrementing a bank metric
        hdfc.bank_requests_total.labels(operation="debit", status="success").inc()
        val = REGISTRY.get_sample_value('bank_requests_total', {'operation': 'debit', 'status': 'success'})
        self.assertGreaterEqual(val, 1.0)
        
    def test_ledger_counter_logic(self):
        ledger.ledger_events_total.labels(event_type="PAYMENT_SUCCESS", status="processed").inc()
        val = REGISTRY.get_sample_value('ledger_events_total', {'event_type': 'PAYMENT_SUCCESS', 'status': 'processed'})
        self.assertGreaterEqual(val, 1.0)
        
if __name__ == '__main__':
    unittest.main()
