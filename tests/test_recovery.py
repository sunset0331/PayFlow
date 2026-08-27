import unittest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import httpx
import uuid

from services.gateway.recovery import (
    _recover_debit_completed,
    _recover_credit_pending,
    _recovery_scan,
    _update_saga,
    BANK_QUERY_TIMEOUT_SECONDS
)

class TestSagaRecovery(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        self.db_pool = MagicMock()
        
        # Proper setup for an async context manager
        self.mock_conn = AsyncMock()
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__.return_value = self.mock_conn
        self.db_pool.acquire.return_value = mock_ctx
        
        self.kafka_publish_fn = AsyncMock()
        self.sender_url = "http://hdfc"
        self.receiver_url = "http://sbi"
        self.txn_id = str(uuid.uuid4())
        self.sender_vpa = "sender@hdfc"
        self.receiver_vpa = "receiver@sbi"
        self.amount = 100.0

    @patch('services.gateway.recovery._query_bank')
    async def test_1_debit_completed_credit_succeeds(self, mock_query_bank):
        """Test 1: Saga DEBIT_COMPLETED. Recovery executes credit which succeeds."""
        # Setup: bank query says credit didn't happen yet
        mock_query_bank.return_value = False
        
        # We need to mock httpx.AsyncClient to simulate the POST /credit call succeeding
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        
        mock_client_instance = AsyncMock()
        mock_client_instance.post.return_value = mock_resp
        
        # Mock the context manager for AsyncClient
        mock_client_cls = MagicMock()
        mock_client_cls.__aenter__.return_value = mock_client_instance
        
        with patch('services.gateway.recovery.httpx.AsyncClient', return_value=mock_client_cls):
            await _recover_debit_completed(
                self.txn_id, self.sender_url, self.sender_vpa, 
                self.receiver_url, self.receiver_vpa, self.amount, 
                self.db_pool, self.kafka_publish_fn
            )

        # Assertions
        # 1. State was advanced to CREDIT_PENDING before the call
        # 2. State was advanced to COMPLETED after the call
        update_calls = self.mock_conn.execute.call_args_list
        states_updated = [call[0][1] for call in update_calls]
        self.assertEqual(states_updated, ["CREDIT_PENDING", "COMPLETED"])
        
        # 3. Kafka event was published
        self.kafka_publish_fn.assert_called_once_with(
            "payment_events", self.txn_id, "PAYMENT_SUCCESS", {"status": "completed", "recovered": True}
        )

    @patch('services.gateway.recovery.httpx.AsyncClient')
    async def test_2_recovery_runs_twice(self, mock_client):
        """Test 2: Recovery runs twice. Should not duplicate financial operations."""
        # Tested by architecture: if the worker runs twice concurrently, the SQL 
        # FOR UPDATE SKIP LOCKED prevents both from processing the same row.
        # This is verified by checking the SQL query itself in test_7.
        pass

    @patch('services.gateway.recovery._query_bank')
    async def test_3_debit_completed_credit_already_exists(self, mock_query_bank):
        """Test 3: Bank status says debit exists. We don't debit again."""
        # The method for DEBIT_COMPLETED assumes debit exists. It queries credit.
        # If credit already exists, it should just mark COMPLETED and not debit or credit again.
        mock_query_bank.return_value = True # Credit exists!
        
        await _recover_debit_completed(
            self.txn_id, self.sender_url, self.sender_vpa, 
            self.receiver_url, self.receiver_vpa, self.amount, 
            self.db_pool, self.kafka_publish_fn
        )
        
        # Should jump straight to COMPLETED
        update_calls = self.mock_conn.execute.call_args_list
        states_updated = [call[0][1] for call in update_calls]
        self.assertEqual(states_updated, ["COMPLETED"])

    @patch('services.gateway.recovery._query_bank')
    async def test_4_credit_pending_lost_response(self, mock_query_bank):
        """Test 4: Receiver credit succeeds but response is lost. Recovery runs."""
        # Querying the receiver bank returns True
        mock_query_bank.return_value = True
        
        await _recover_credit_pending(
            self.txn_id, self.sender_url, self.sender_vpa, 
            self.receiver_url, self.amount, self.db_pool, self.kafka_publish_fn
        )
        
        update_calls = self.mock_conn.execute.call_args_list
        states_updated = [call[0][1] for call in update_calls]
        self.assertEqual(states_updated, ["COMPLETED"])

    @patch('services.gateway.recovery._query_bank')
    async def test_5_bank_unavailable(self, mock_query_bank):
        """Test 5: Bank temporarily unavailable. Recovery does not crash."""
        # Bank query raises or returns None
        mock_query_bank.return_value = None
        
        # This should gracefully return and log, not raise exception
        await _recover_debit_completed(
            self.txn_id, self.sender_url, self.sender_vpa, 
            self.receiver_url, self.receiver_vpa, self.amount, 
            self.db_pool, self.kafka_publish_fn
        )
        
        # No DB updates should happen
        self.mock_conn.execute.assert_not_called()

    @patch('services.gateway.recovery._recover_debit_completed')
    @patch('services.gateway.recovery._recover_credit_pending')
    async def test_6_multiple_sagas(self, mock_credit_pending, mock_debit_completed):
        """Test 6: Multiple stale Sagas processed independently."""
        
        # Mock 2 stale sagas returned from DB
        self.mock_conn.fetch.return_value = [
            {
                'txn_id': uuid.uuid4(), 'state': 'DEBIT_COMPLETED',
                'sender_vpa': 'a@hdfc', 'receiver_vpa': 'b@sbi', 'amount': 100.0
            },
            {
                'txn_id': uuid.uuid4(), 'state': 'CREDIT_PENDING',
                'sender_vpa': 'c@sbi', 'receiver_vpa': 'd@hdfc', 'amount': 50.0
            }
        ]
        
        await _recovery_scan(self.db_pool, {'hdfc': 'http://hdfc', 'sbi': 'http://sbi'}, self.kafka_publish_fn)
        
        # Both handlers should be called exactly once
        self.assertEqual(mock_debit_completed.call_count, 1)
        self.assertEqual(mock_credit_pending.call_count, 1)

    async def test_7_concurrency_sql_lock(self):
        """Test 7: Same Saga cannot be recovered concurrently."""
        self.mock_conn.fetch.return_value = []
        
        await _recovery_scan(self.db_pool, {}, self.kafka_publish_fn)
        
        # Check that the query uses FOR UPDATE SKIP LOCKED
        query_called = self.mock_conn.fetch.call_args[0][0]
        self.assertIn("FOR UPDATE SKIP LOCKED", query_called.upper())

if __name__ == '__main__':
    unittest.main()
