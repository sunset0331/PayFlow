"""
Tests for Step 17.1 — Lost-Response Ambiguity Handling.

Covers:
  - payment_worker: emits debit_ambiguous/credit_ambiguous on network errors
  - orchestrator: _query_bank_for_operation returns correct values
  - orchestrator: _process_event routes debit_ambiguous/credit_ambiguous correctly
    depending on what the bank reports (SUCCESS / NOT_FOUND / UNAVAILABLE)
"""

import unittest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import httpx
import uuid

from services.gateway.orchestrator import _query_bank_for_operation, _process_event


# ---------------------------------------------------------------------------
# Helper: build a minimal mock DB pool wired up so _process_event can run
# ---------------------------------------------------------------------------

def _make_db_pool(state: str, sender_vpa: str = "alice@hdfc",
                  receiver_vpa: str = "bob@sbi", amount: float = 100.0):
    """Return a mock db_pool whose connection yields saga data for the given state."""
    mock_pool = MagicMock()
    mock_conn = MagicMock()  # MagicMock so attribute access is sync by default

    # pool.acquire() is an async context manager
    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = mock_conn
    mock_pool.acquire.return_value = acquire_ctx

    # conn.transaction() must be an async context manager (not a coroutine)
    txn_ctx = MagicMock()
    txn_ctx.__aenter__ = AsyncMock(return_value=None)
    txn_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction.return_value = txn_ctx

    # Track execute calls for assertions
    mock_conn.execute = AsyncMock(return_value="UPDATE 1")

    async def smart_fetchrow(query, *args, **kwargs):
        if "saga_transactions" in query:
            return {
                "state": state,
                "sender_vpa": sender_vpa,
                "receiver_vpa": receiver_vpa,
                "amount": amount,
            }
        if "vpa_registry" in query:
            vpa = args[0] if args else sender_vpa
            bank_slug = vpa.split("@")[1] if "@" in vpa else "hdfc"
            url = "http://bank-hdfc:8001" if bank_slug == "hdfc" else "http://bank-sbi:8002"
            return {"bank_service_url": url}
        return None

    mock_conn.fetchrow = AsyncMock(side_effect=smart_fetchrow)
    return mock_pool, mock_conn


# ---------------------------------------------------------------------------
# Tests: _query_bank_for_operation
# ---------------------------------------------------------------------------

class TestQueryBankForOperation(unittest.IsolatedAsyncioTestCase):
    """Unit tests for the helper that queries a bank for an operation's outcome."""

    async def test_bank_returns_200_success(self):
        """Bank confirms operation executed → SUCCESS."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"status": "SUCCESS"}

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp

        with patch("services.gateway.orchestrator.httpx.AsyncClient") as cls:
            cls.return_value.__aenter__.return_value = mock_client
            result = await _query_bank_for_operation("http://bank:8001", str(uuid.uuid4()), "CREDIT")

        self.assertEqual(result, "SUCCESS")

    async def test_bank_returns_404_not_found(self):
        """Bank confirms operation NOT executed → NOT_FOUND."""
        mock_resp = MagicMock()
        mock_resp.status_code = 404

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp

        with patch("services.gateway.orchestrator.httpx.AsyncClient") as cls:
            cls.return_value.__aenter__.return_value = mock_client
            result = await _query_bank_for_operation("http://bank:8001", str(uuid.uuid4()), "DEBIT")

        self.assertEqual(result, "NOT_FOUND")

    async def test_bank_returns_500_server_error(self):
        """Bank returns 5xx → UNAVAILABLE."""
        mock_resp = MagicMock()
        mock_resp.status_code = 503

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_resp

        with patch("services.gateway.orchestrator.httpx.AsyncClient") as cls:
            cls.return_value.__aenter__.return_value = mock_client
            result = await _query_bank_for_operation("http://bank:8001", str(uuid.uuid4()), "CREDIT")

        self.assertEqual(result, "UNAVAILABLE")

    async def test_bank_network_error_is_unavailable(self):
        """Network failure querying bank → UNAVAILABLE."""
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.RequestError("Connection refused")

        with patch("services.gateway.orchestrator.httpx.AsyncClient") as cls:
            cls.return_value.__aenter__.return_value = mock_client
            result = await _query_bank_for_operation("http://bank:8001", str(uuid.uuid4()), "CREDIT")

        self.assertEqual(result, "UNAVAILABLE")

    async def test_no_bank_url_is_unavailable(self):
        """Missing bank URL → UNAVAILABLE (no network call attempted)."""
        result = await _query_bank_for_operation("", str(uuid.uuid4()), "DEBIT")
        self.assertEqual(result, "UNAVAILABLE")


# ---------------------------------------------------------------------------
# Tests: debit_ambiguous handling
# ---------------------------------------------------------------------------

class TestDebitAmbiguousHandling(unittest.IsolatedAsyncioTestCase):
    """Tests for orchestrator handling of debit_ambiguous events."""

    async def test_debit_ambiguous_bank_confirms_success_proceeds_to_credit(self):
        """
        debit_ambiguous + bank says SUCCESS → debit DID happen → move to CREDIT_PENDING
        and enqueue credit_request command. Must NOT compensate.
        """
        txn_id = str(uuid.uuid4())
        db_pool, mock_conn = _make_db_pool("DEBIT_PENDING")

        event = {
            "txn_id": txn_id,
            "event_type": "debit_ambiguous",
            "payload": {"bank_url": "http://bank-hdfc:8001", "reason": "timeout"},
        }

        with patch("services.gateway.orchestrator._query_bank_for_operation",
                   new_callable=AsyncMock, return_value="SUCCESS"):
            await _process_event(db_pool, event)

        # Should update saga to CREDIT_PENDING
        execute_calls = mock_conn.execute.call_args_list
        states = [c[0][1] for c in execute_calls if c[0][0].strip().startswith("UPDATE saga")]
        self.assertIn("CREDIT_PENDING", states)

        # Should insert a credit_request outbox event
        insert_calls = [c for c in execute_calls if "outbox_events" in c[0][0]]
        event_types_inserted = [c[0][3] for c in insert_calls]  # 4th positional arg = event_type
        self.assertTrue(
            any("credit_request" in et for et in event_types_inserted),
            f"Expected credit_request in outbox inserts; got: {event_types_inserted}"
        )

    async def test_debit_ambiguous_bank_confirms_not_found_marks_failed(self):
        """
        debit_ambiguous + bank says NOT_FOUND → debit did NOT happen → mark FAILED,
        no compensation needed.
        """
        txn_id = str(uuid.uuid4())
        db_pool, mock_conn = _make_db_pool("DEBIT_PENDING")

        event = {
            "txn_id": txn_id,
            "event_type": "debit_ambiguous",
            "payload": {"bank_url": "http://bank-hdfc:8001", "reason": "timeout"},
        }

        with patch("services.gateway.orchestrator._query_bank_for_operation",
                   new_callable=AsyncMock, return_value="NOT_FOUND"):
            await _process_event(db_pool, event)

        execute_calls = mock_conn.execute.call_args_list
        states = [c[0][1] for c in execute_calls if c[0][0].strip().startswith("UPDATE saga")]
        self.assertIn("FAILED", states)
        self.assertNotIn("COMPENSATING", states)

    async def test_debit_ambiguous_bank_unavailable_marks_indeterminate(self):
        """
        debit_ambiguous + bank UNAVAILABLE → cannot determine outcome → INDETERMINATE.
        """
        txn_id = str(uuid.uuid4())
        db_pool, mock_conn = _make_db_pool("DEBIT_PENDING")

        event = {
            "txn_id": txn_id,
            "event_type": "debit_ambiguous",
            "payload": {"bank_url": "http://bank-hdfc:8001", "reason": "timeout"},
        }

        with patch("services.gateway.orchestrator._query_bank_for_operation",
                   new_callable=AsyncMock, return_value="UNAVAILABLE"):
            await _process_event(db_pool, event)

        execute_calls = mock_conn.execute.call_args_list
        states = [c[0][1] for c in execute_calls if c[0][0].strip().startswith("UPDATE saga")]
        self.assertIn("INDETERMINATE", states)


# ---------------------------------------------------------------------------
# Tests: credit_ambiguous handling
# ---------------------------------------------------------------------------

class TestCreditAmbiguousHandling(unittest.IsolatedAsyncioTestCase):
    """Tests for orchestrator handling of credit_ambiguous events."""

    async def test_credit_ambiguous_bank_confirms_success_marks_completed(self):
        """
        credit_ambiguous + bank says SUCCESS → credit DID happen → COMPLETED.
        Must NOT compensate (that would be a double-spend).
        """
        txn_id = str(uuid.uuid4())
        db_pool, mock_conn = _make_db_pool("CREDIT_PENDING")

        event = {
            "txn_id": txn_id,
            "event_type": "credit_ambiguous",
            "payload": {"bank_url": "http://bank-sbi:8002", "reason": "timeout"},
        }

        with patch("services.gateway.orchestrator._query_bank_for_operation",
                   new_callable=AsyncMock, return_value="SUCCESS"):
            await _process_event(db_pool, event)

        execute_calls = mock_conn.execute.call_args_list
        states = [c[0][1] for c in execute_calls if c[0][0].strip().startswith("UPDATE saga")]
        self.assertIn("COMPLETED", states)
        self.assertNotIn("COMPENSATING", states)

    async def test_credit_ambiguous_bank_confirms_not_found_compensates(self):
        """
        credit_ambiguous + bank says NOT_FOUND → credit did NOT happen → compensate sender.
        """
        txn_id = str(uuid.uuid4())
        db_pool, mock_conn = _make_db_pool("CREDIT_PENDING")

        event = {
            "txn_id": txn_id,
            "event_type": "credit_ambiguous",
            "payload": {"bank_url": "http://bank-sbi:8002", "reason": "timeout"},
        }

        with patch("services.gateway.orchestrator._query_bank_for_operation",
                   new_callable=AsyncMock, return_value="NOT_FOUND"):
            await _process_event(db_pool, event)

        execute_calls = mock_conn.execute.call_args_list
        states = [c[0][1] for c in execute_calls if c[0][0].strip().startswith("UPDATE saga")]
        self.assertIn("COMPENSATING", states)

        # compensate_request must appear in outbox
        insert_calls = [c for c in execute_calls if "outbox_events" in c[0][0]]
        event_types_inserted = [c[0][3] for c in insert_calls]
        self.assertTrue(
            any("compensate_request" in et for et in event_types_inserted),
            f"Expected compensate_request in outbox inserts; got: {event_types_inserted}"
        )

    async def test_credit_ambiguous_bank_unavailable_marks_indeterminate(self):
        """
        credit_ambiguous + bank UNAVAILABLE → cannot determine outcome safely → INDETERMINATE.
        Critically: must NOT compensate, as compensation after a successful
        (but response-lost) credit would create money from nothing.
        """
        txn_id = str(uuid.uuid4())
        db_pool, mock_conn = _make_db_pool("CREDIT_PENDING")

        event = {
            "txn_id": txn_id,
            "event_type": "credit_ambiguous",
            "payload": {"bank_url": "http://bank-sbi:8002", "reason": "timeout"},
        }

        with patch("services.gateway.orchestrator._query_bank_for_operation",
                   new_callable=AsyncMock, return_value="UNAVAILABLE"):
            await _process_event(db_pool, event)

        execute_calls = mock_conn.execute.call_args_list
        states = [c[0][1] for c in execute_calls if c[0][0].strip().startswith("UPDATE saga")]
        self.assertIn("INDETERMINATE", states)
        self.assertNotIn("COMPENSATING", states)


if __name__ == "__main__":
    unittest.main()
