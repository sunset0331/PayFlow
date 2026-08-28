"""
Reliability Hardening Tests — PayFlow Gateway

BUG-2: Orchestrator retry + DLQ
BUG-4: UUID casting
BUG-1+5: Recovery Worker race condition guard
"""

import asyncio
import json
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_msg(event_type, txn_id, payload=None, partition=0, offset=0):
    msg = MagicMock()
    msg.value = {"event_type": event_type, "txn_id": txn_id, "payload": payload or {}}
    msg.partition = partition
    msg.offset = offset
    return msg


async def _async_iter(*items):
    """Helper to mock an async iterator for async for loops."""
    for item in items:
        yield item


def _make_db_pool_for_orchestrator(state="DEBIT_PENDING",
                                   sender_vpa="alice@hdfc",
                                   receiver_vpa="bob@sbi",
                                   amount=100.0):
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = mock_conn
    mock_pool.acquire.return_value = acquire_ctx

    txn_ctx = MagicMock()
    txn_ctx.__aenter__ = AsyncMock(return_value=None)
    txn_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction.return_value = txn_ctx
    mock_conn.execute = AsyncMock(return_value="UPDATE 1")

    async def smart_fetchrow(query, *args, **kwargs):
        if "saga_transactions" in query:
            return {"state": state, "sender_vpa": sender_vpa,
                    "receiver_vpa": receiver_vpa, "amount": amount}
        if "vpa_registry" in query:
            vpa = args[0] if args else sender_vpa
            bank = vpa.split("@")[1] if "@" in vpa else "hdfc"
            url = "http://bank-hdfc:8001" if bank == "hdfc" else "http://bank-sbi:8002"
            return {"bank_service_url": url}
        return None

    mock_conn.fetchrow = AsyncMock(side_effect=smart_fetchrow)
    return mock_pool, mock_conn


# ---------------------------------------------------------------------------
# BUG-2: Orchestrator Retry + DLQ
# ---------------------------------------------------------------------------

class TestOrchestratorRetryAndDLQ:

    def setup_method(self):
        from services.gateway import orchestrator as m
        m.clear_dlq()

    @pytest.mark.asyncio
    async def test_success_on_first_attempt_no_dlq(self):
        from services.gateway import orchestrator as m
        m.clear_dlq()
        mock_pool, _ = _make_db_pool_for_orchestrator()
        msg = _make_msg("debit_completed", str(uuid.uuid4()))
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__ = MagicMock(return_value=_async_iter(msg))
        mock_consumer.start = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_consumer.commit = AsyncMock()
        with patch("services.gateway.orchestrator.AIOKafkaConsumer", return_value=mock_consumer):
            with patch("services.gateway.orchestrator._process_event", new_callable=AsyncMock):
                await m.run_orchestrator(mock_pool)
        assert m.get_dlq_snapshot() == []
        mock_consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_transient_failure_retries_then_succeeds(self):
        from services.gateway import orchestrator as m
        m.clear_dlq()
        mock_pool, _ = _make_db_pool_for_orchestrator()
        msg = _make_msg("debit_completed", str(uuid.uuid4()))
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__ = MagicMock(return_value=_async_iter(msg))
        mock_consumer.start = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_consumer.commit = AsyncMock()
        call_count = 0

        async def fail_twice_then_succeed(pool, event):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("Transient DB error")

        with patch("services.gateway.orchestrator.AIOKafkaConsumer", return_value=mock_consumer):
            with patch("services.gateway.orchestrator._process_event", side_effect=fail_twice_then_succeed):
                with patch("asyncio.sleep", new_callable=AsyncMock):
                    await m.run_orchestrator(mock_pool)

        assert call_count == 3
        assert m.get_dlq_snapshot() == []
        mock_consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_exhausted_retries_route_to_dlq_and_commit_offset(self):
        from services.gateway import orchestrator as m
        m.clear_dlq()
        mock_pool, _ = _make_db_pool_for_orchestrator()
        txn_id = str(uuid.uuid4())
        msg = _make_msg("debit_completed", txn_id, partition=2, offset=99)
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__ = MagicMock(return_value=_async_iter(msg))
        mock_consumer.start = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_consumer.commit = AsyncMock()
        call_count = 0

        async def always_fail(pool, event):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Permanent failure")

        with patch("services.gateway.orchestrator.AIOKafkaConsumer", return_value=mock_consumer):
            with patch("services.gateway.orchestrator._process_event", side_effect=always_fail):
                with patch("asyncio.sleep", new_callable=AsyncMock):
                    await m.run_orchestrator(mock_pool)

        assert call_count == m.MAX_RETRIES
        dlq = m.get_dlq_snapshot()
        assert len(dlq) == 1
        assert dlq[0]["event"]["txn_id"] == txn_id
        assert dlq[0]["partition"] == 2
        assert dlq[0]["offset"] == 99
        assert "Permanent failure" in dlq[0]["error"]
        # Offset must be committed even after DLQ — no consumer block
        mock_consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_events_selective_dlq(self):
        from services.gateway import orchestrator as m
        m.clear_dlq()
        mock_pool, _ = _make_db_pool_for_orchestrator()
        txn1 = str(uuid.uuid4())
        txn2 = str(uuid.uuid4())
        msg1 = _make_msg("debit_completed", txn1, offset=10)
        msg2 = _make_msg("credit_completed", txn2, offset=11)
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__ = MagicMock(return_value=_async_iter(msg1, msg2))
        mock_consumer.start = AsyncMock()
        mock_consumer.stop = AsyncMock()
        mock_consumer.commit = AsyncMock()

        async def selective_fail(pool, event):
            if event["txn_id"] == txn1:
                raise RuntimeError("fail first")

        with patch("services.gateway.orchestrator.AIOKafkaConsumer", return_value=mock_consumer):
            with patch("services.gateway.orchestrator._process_event", side_effect=selective_fail):
                with patch("asyncio.sleep", new_callable=AsyncMock):
                    await m.run_orchestrator(mock_pool)

        dlq = m.get_dlq_snapshot()
        assert len(dlq) == 1
        assert dlq[0]["event"]["txn_id"] == txn1
        assert mock_consumer.commit.call_count == 2


# ---------------------------------------------------------------------------
# BUG-4: UUID Casting
# ---------------------------------------------------------------------------

class TestUUIDCasting:

    @pytest.mark.asyncio
    async def test_advance_saga_update_uses_uuid(self):
        from services.gateway.orchestrator import _advance_saga
        mock_conn = MagicMock()
        txn_ctx = MagicMock()
        txn_ctx.__aenter__ = AsyncMock(return_value=None)
        txn_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_conn.transaction.return_value = txn_ctx
        mock_conn.execute = AsyncMock(return_value="UPDATE 1")
        txn_id_str = str(uuid.uuid4())
        await _advance_saga(mock_conn, txn_id_str, "COMPLETED", None, [])
        update_call = mock_conn.execute.call_args_list[0]
        txn_arg = update_call[0][3]
        assert isinstance(txn_arg, uuid.UUID), (
            f"UPDATE expected uuid.UUID, got {type(txn_arg).__name__}"
        )

    @pytest.mark.asyncio
    async def test_advance_saga_outbox_insert_uses_uuid(self):
        from services.gateway.orchestrator import _advance_saga
        mock_conn = MagicMock()
        txn_ctx = MagicMock()
        txn_ctx.__aenter__ = AsyncMock(return_value=None)
        txn_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_conn.transaction.return_value = txn_ctx
        mock_conn.execute = AsyncMock(return_value="UPDATE 1")
        txn_id_str = str(uuid.uuid4())
        outbox = [{"topic": "payment_events", "event_type": "PAYMENT_SUCCESS", "payload": {}}]
        await _advance_saga(mock_conn, txn_id_str, "COMPLETED", None, outbox)
        insert_call = mock_conn.execute.call_args_list[1]
        txn_arg = insert_call[0][1]
        assert isinstance(txn_arg, uuid.UUID), (
            f"Outbox INSERT expected uuid.UUID, got {type(txn_arg).__name__}"
        )

    @pytest.mark.asyncio
    async def test_process_event_saga_fetch_uses_uuid(self):
        from services.gateway.orchestrator import _process_event
        mock_pool, mock_conn = _make_db_pool_for_orchestrator(state="DEBIT_PENDING")
        txn_id_str = str(uuid.uuid4())
        with patch("services.gateway.orchestrator._advance_saga", new_callable=AsyncMock):
            await _process_event(mock_pool, {
                "event_type": "debit_completed",
                "txn_id": txn_id_str,
                "payload": {}
            })
        saga_calls = [c for c in mock_conn.fetchrow.call_args_list
                      if "saga_transactions" in c[0][0]]
        assert saga_calls
        txn_arg = saga_calls[0][0][1]
        assert isinstance(txn_arg, uuid.UUID), (
            f"saga SELECT expected uuid.UUID, got {type(txn_arg).__name__}"
        )


# ---------------------------------------------------------------------------
# BUG-1+5: Recovery Worker Race Guard
# ---------------------------------------------------------------------------

class TestRecoveryGuardedUpdate:

    def _pool(self, result_str):
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        acquire_ctx = AsyncMock()
        acquire_ctx.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value = acquire_ctx
        mock_conn.execute = AsyncMock(return_value=result_str)
        return mock_pool

    @pytest.mark.asyncio
    async def test_guarded_returns_true_on_match(self):
        from services.gateway.recovery import _update_saga_guarded
        result = await _update_saga_guarded(
            self._pool("UPDATE 1"), str(uuid.uuid4()), "CREDIT_PENDING", "COMPLETED"
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_guarded_returns_false_when_preempted(self):
        from services.gateway.recovery import _update_saga_guarded
        result = await _update_saga_guarded(
            self._pool("UPDATE 0"), str(uuid.uuid4()), "CREDIT_PENDING", "COMPENSATING"
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_guarded_uses_uuid_for_txn_id(self):
        from services.gateway.recovery import _update_saga_guarded
        pool = self._pool("UPDATE 1")
        mock_conn = pool.acquire.return_value.__aenter__.return_value
        txn_id_str = str(uuid.uuid4())
        await _update_saga_guarded(pool, txn_id_str, "CREDIT_PENDING", "COMPLETED")
        call_args = mock_conn.execute.call_args[0]
        txn_arg = call_args[3]
        assert isinstance(txn_arg, uuid.UUID), (
            f"Expected uuid.UUID, got {type(txn_arg).__name__}"
        )


class TestRecoveryRacePrevention:

    @pytest.mark.asyncio
    async def test_credit_pending_aborts_compensation_when_guard_preempted(self):
        """
        CRITICAL: If the guarded CREDIT_PENDING→COMPENSATING update returns False,
        _credit_sender must NOT be called. This prevents double-spend when the
        Orchestrator already marked the saga COMPLETED.
        """
        from services.gateway.recovery import _recover_credit_pending
        mock_kafka = AsyncMock()
        with patch("services.gateway.recovery._query_bank", return_value=False):
            with patch("services.gateway.recovery._update_saga_guarded",
                       new_callable=AsyncMock, return_value=False):
                with patch("services.gateway.recovery._credit_sender",
                           new_callable=AsyncMock) as mock_credit:
                    await _recover_credit_pending(
                        str(uuid.uuid4()),
                        "http://bank-hdfc:8001", "alice@hdfc",
                        "http://bank-sbi:8002", 100.0,
                        MagicMock(), mock_kafka
                    )
                    mock_credit.assert_not_called()
        mock_kafka.assert_not_called()

    @pytest.mark.asyncio
    async def test_credit_pending_compensates_when_guard_succeeds(self):
        """Normal compensation path: bank NOT_FOUND, guard succeeds → PAYMENT_COMPENSATED."""
        from services.gateway.recovery import _recover_credit_pending
        mock_kafka = AsyncMock()
        with patch("services.gateway.recovery._query_bank", return_value=False):
            with patch("services.gateway.recovery._update_saga_guarded",
                       new_callable=AsyncMock, return_value=True):
                with patch("services.gateway.recovery._credit_sender",
                           new_callable=AsyncMock, return_value=True):
                    with patch("services.gateway.recovery._update_saga", new_callable=AsyncMock):
                        await _recover_credit_pending(
                            str(uuid.uuid4()),
                            "http://bank-hdfc:8001", "alice@hdfc",
                            "http://bank-sbi:8002", 100.0,
                            MagicMock(), mock_kafka
                        )
        mock_kafka.assert_called_once()
        assert mock_kafka.call_args[0][2] == "PAYMENT_COMPENSATED"

    @pytest.mark.asyncio
    async def test_credit_pending_completed_preempted_no_action(self):
        """Bank says credit done but guard preempted → no Kafka event (already handled)."""
        from services.gateway.recovery import _recover_credit_pending
        mock_kafka = AsyncMock()
        with patch("services.gateway.recovery._query_bank", return_value=True):
            with patch("services.gateway.recovery._update_saga_guarded",
                       new_callable=AsyncMock, return_value=False):
                await _recover_credit_pending(
                    str(uuid.uuid4()),
                    "http://bank-hdfc:8001", "alice@hdfc",
                    "http://bank-sbi:8002", 100.0,
                    MagicMock(), mock_kafka
                )
        mock_kafka.assert_not_called()

    @pytest.mark.asyncio
    async def test_debit_pending_preempted_no_action(self):
        """DEBIT_PENDING recovery: preempted guarded update → no further action."""
        from services.gateway.recovery import _recover_debit_pending
        mock_kafka = AsyncMock()
        with patch("services.gateway.recovery._query_bank", return_value=True):
            with patch("services.gateway.recovery._update_saga_guarded",
                       new_callable=AsyncMock, return_value=False):
                await _recover_debit_pending(
                    str(uuid.uuid4()),
                    "http://bank-hdfc:8001", "alice@hdfc", 100.0,
                    MagicMock(), mock_kafka
                )
        mock_kafka.assert_not_called()

    @pytest.mark.asyncio
    async def test_compensating_guard_preempted_no_duplicate(self):
        """COMPENSATING recovery: preempted → no duplicate COMPENSATED event."""
        from services.gateway.recovery import _recover_compensating
        mock_kafka = AsyncMock()
        with patch("services.gateway.recovery._query_bank", return_value=True):
            with patch("services.gateway.recovery._update_saga_guarded",
                       new_callable=AsyncMock, return_value=False):
                await _recover_compensating(
                    str(uuid.uuid4()),
                    "http://bank-hdfc:8001", "alice@hdfc", 100.0,
                    MagicMock(), mock_kafka
                )
        mock_kafka.assert_not_called()

    @pytest.mark.asyncio
    async def test_compensating_guard_succeeds_publishes_compensated(self):
        """COMPENSATING normal path: guard succeeds → PAYMENT_COMPENSATED published."""
        from services.gateway.recovery import _recover_compensating
        mock_kafka = AsyncMock()
        with patch("services.gateway.recovery._query_bank", return_value=True):
            with patch("services.gateway.recovery._update_saga_guarded",
                       new_callable=AsyncMock, return_value=True):
                await _recover_compensating(
                    str(uuid.uuid4()),
                    "http://bank-hdfc:8001", "alice@hdfc", 100.0,
                    MagicMock(), mock_kafka
                )
        mock_kafka.assert_called_once()
        assert mock_kafka.call_args[0][2] == "PAYMENT_COMPENSATED"
