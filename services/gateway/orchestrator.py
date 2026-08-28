"""
Saga Orchestrator for PayFlow Gateway.

This module consumes events from the 'payment_events' Kafka topic,
updates the saga_transactions state in PostgreSQL, and generates new commands
to be published to 'payment_commands' via the Outbox table.
"""

import asyncio
import json
import os
import time
import uuid
import httpx
from aiokafka import AIOKafkaConsumer
from shared.logger import get_logger

# Retry configuration for the consumer loop
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1  # sleep(attempt * RETRY_BACKOFF_SECONDS) between retries



logger = get_logger("orchestrator")
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "kafka:9092")

async def run_orchestrator(db_pool) -> None:
    """Main orchestrator consumer loop."""
    logger.info("Gateway Orchestrator started. Listening to 'payment_events'.")

    consumer = AIOKafkaConsumer(
        "payment_events",
        bootstrap_servers=KAFKA_BROKER,
        group_id="gateway-orchestrator-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=False
    )
    await consumer.start()

    try:
        async for msg in consumer:
            event = msg.value
            last_exc = None

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await _process_event(db_pool, event)
                    last_exc = None
                    break
                except Exception as e:
                    last_exc = e
                    logger.warning(
                        "Orchestrator processing attempt %d/%d failed for event_type=%s txn=%s: %s",
                        attempt, MAX_RETRIES,
                        event.get("event_type"), event.get("txn_id"), e,
                        exc_info=(attempt == MAX_RETRIES),
                    )
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(attempt * RETRY_BACKOFF_SECONDS)

            if last_exc is not None:
                # All retries exhausted — route to DLQ and commit offset so we
                # do not block the consumer partition indefinitely.
                logger.error(
                    "Orchestrator exhausted %d retries for event_type=%s txn=%s — routing to DLQ",
                    MAX_RETRIES,
                    event.get("event_type"),
                    event.get("txn_id"),
                )
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO dead_letter_queue (topic, payload, error_reason) VALUES ($1, $2, $3)",
                        "payment_events", json.dumps({"event": event, "partition": msg.partition, "offset": msg.offset}), str(last_exc)
                    )

            await consumer.commit()
    finally:
        await consumer.stop()

async def _process_event(db_pool, event: dict):
    txn_id = event.get("txn_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    
    logger.info("Orchestrator received event %s for txn %s", event_type, txn_id)

    async with db_pool.acquire() as conn:
        # Fetch current saga state — cast to uuid.UUID for asyncpg type correctness
        saga = await conn.fetchrow(
            "SELECT sender_vpa, receiver_vpa, amount, state FROM saga_transactions WHERE txn_id = $1",
            uuid.UUID(txn_id)
        )
        if not saga:
            logger.warning("Saga not found for txn %s", txn_id)
            return
            
        current_state = saga['state']
        sender_vpa = saga['sender_vpa']
        receiver_vpa = saga['receiver_vpa']
        amount = float(saga['amount'])

        # Dynamic routing lookup
        sender_url = await _get_routing_url(conn, sender_vpa)
        receiver_url = await _get_routing_url(conn, receiver_vpa)

        # Handle events
        if event_type == "debit_completed" and current_state == "DEBIT_PENDING":
            # Move to CREDIT_PENDING and emit credit_request
            await _advance_saga(
                    conn, txn_id, current_state, "CREDIT_PENDING", None,
                [
                    {
                        "topic": "payment_commands",
                        "event_type": "credit_request",
                        "payload": {
                            "vpa": receiver_vpa,
                            "amount": amount,
                            "bank_url": receiver_url
                        }
                    },
                    {
                        "topic": "payment_events",  # For Ledger/Notifications (legacy compatibility)
                        "event_type": "DEBIT_COMPLETED",
                        "payload": {}
                    }
                ]
            )

        elif event_type == "debit_failed" and current_state == "DEBIT_PENDING":
            # Move to FAILED
            await _advance_saga(
                    conn, txn_id, current_state, "FAILED", payload.get("reason"),
                [
                    {
                        "topic": "payment_events",
                        "event_type": "PAYMENT_FAILED",
                        "payload": {"reason": payload.get("reason")}
                    }
                ]
            )

        elif event_type == "credit_completed" and current_state == "CREDIT_PENDING":
            # Move to COMPLETED
            await _advance_saga(
                    conn, txn_id, current_state, "COMPLETED", None,
                [
                    {
                        "topic": "payment_events",
                        "event_type": "PAYMENT_SUCCESS",
                        "payload": {"status": "completed"}
                    }
                ]
            )

        elif event_type == "credit_failed" and current_state == "CREDIT_PENDING":
            # Receiver bank error or timeout. Must compensate sender.
            await _advance_saga(
                    conn, txn_id, current_state, "COMPENSATING", payload.get("reason", "Receiver credit failed"),
                [
                    {
                        "topic": "payment_commands",
                        "event_type": "compensate_request",
                        "payload": {
                            "vpa": sender_vpa,
                            "amount": amount,
                            "bank_url": sender_url
                        }
                    }
                ]
            )

        elif event_type == "debit_ambiguous" and current_state == "DEBIT_PENDING":
            # Network timeout during debit — outcome unknown. Query the sender bank to find out.
            bank_url = payload.get("bank_url") or sender_url
            bank_status = await _query_bank_for_operation(bank_url, txn_id, "DEBIT")
            if bank_status == "SUCCESS":
                # Debit DID happen. Proceed with credit as normal.
                logger.info("Ambiguous debit resolved: SUCCESS — proceeding with credit", extra={"txn_id": txn_id})
                await _advance_saga(
                    conn, txn_id, current_state, "CREDIT_PENDING", None,
                    [
                        {
                            "topic": "payment_commands",
                            "event_type": "credit_request",
                            "payload": {"vpa": receiver_vpa, "amount": amount, "bank_url": receiver_url}
                        },
                        {
                            "topic": "payment_events",
                            "event_type": "DEBIT_COMPLETED",
                            "payload": {}
                        }
                    ]
                )
            elif bank_status == "NOT_FOUND":
                # Debit did NOT happen. Safe to fail without compensation.
                logger.info("Ambiguous debit resolved: NOT_FOUND — marking FAILED", extra={"txn_id": txn_id})
                await _advance_saga(
                    conn, txn_id, current_state, "FAILED", "Debit not executed (confirmed by bank)",
                    [
                        {
                            "topic": "payment_events",
                            "event_type": "PAYMENT_FAILED",
                            "payload": {"reason": "Debit not executed (confirmed by bank)"}
                        }
                    ]
                )
            else:
                # Bank is UNAVAILABLE — cannot determine outcome. Move to INDETERMINATE for manual review.
                logger.warning("Ambiguous debit unresolvable: bank UNAVAILABLE — moving to INDETERMINATE", extra={"txn_id": txn_id})
                await _advance_saga(
                    conn, txn_id, current_state, "INDETERMINATE", "Debit outcome unknown (bank unreachable after timeout)",
                    []
                )

        elif event_type == "credit_ambiguous" and current_state == "CREDIT_PENDING":
            # Network timeout during credit — outcome unknown. Query the receiver bank.
            bank_url = payload.get("bank_url") or receiver_url
            bank_status = await _query_bank_for_operation(bank_url, txn_id, "CREDIT")
            if bank_status == "SUCCESS":
                # Credit DID happen. Mark COMPLETED. Compensation would be a double-spend.
                logger.info("Ambiguous credit resolved: SUCCESS — marking COMPLETED", extra={"txn_id": txn_id})
                await _advance_saga(
                    conn, txn_id, current_state, "COMPLETED", None,
                    [
                        {
                            "topic": "payment_events",
                            "event_type": "PAYMENT_SUCCESS",
                            "payload": {"status": "completed", "resolved": "ambiguous_credit"}
                        }
                    ]
                )
            elif bank_status == "NOT_FOUND":
                # Credit did NOT happen. Safe to compensate the sender.
                logger.info("Ambiguous credit resolved: NOT_FOUND — compensating sender", extra={"txn_id": txn_id})
                await _advance_saga(
                    conn, txn_id, current_state, "COMPENSATING", "Credit not executed (confirmed by bank) — compensating sender",
                    [
                        {
                            "topic": "payment_commands",
                            "event_type": "compensate_request",
                            "payload": {"vpa": sender_vpa, "amount": amount, "bank_url": sender_url}
                        }
                    ]
                )
            else:
                # Bank is UNAVAILABLE — cannot determine outcome safely. Move to INDETERMINATE.
                logger.warning("Ambiguous credit unresolvable: bank UNAVAILABLE — moving to INDETERMINATE", extra={"txn_id": txn_id})
                await _advance_saga(
                    conn, txn_id, current_state, "INDETERMINATE", "Credit outcome unknown (bank unreachable after timeout)",
                    []
                )

        elif event_type == "compensate_completed" and current_state == "COMPENSATING":
            # Move to COMPENSATED
            await _advance_saga(
                    conn, txn_id, current_state, "COMPENSATED", None,
                [
                    {
                        "topic": "payment_events",
                        "event_type": "PAYMENT_COMPENSATED",
                        "payload": {"reason": "Receiver credit failed, sender refunded"}
                    }
                ]
            )
            
        elif event_type == "compensate_failed" and current_state == "COMPENSATING":
            # Critical failure
            await _advance_saga(
                    conn, txn_id, current_state, "COMPENSATION_FAILED", payload.get("reason", "Both credit and compensation failed"),
                [
                    {
                        "topic": "payment_events",
                        "event_type": "COMPENSATION_FAILED",
                        "payload": {
                            "reason": "Both credit and compensation failed",
                            "sender": sender_vpa,
                            "receiver": receiver_vpa,
                            "amount": amount
                        }
                    }
                ]
            )

async def _advance_saga(conn, txn_id: str, expected_state: str, new_state: str, error_reason: str, outbox_events: list):
    """Update saga state and append to outbox transactionally.

    NOTE: txn_id is cast to uuid.UUID() for asyncpg type correctness.
    asyncpg does NOT auto-coerce str→UUID; passing a plain string to a UUID
    column raises asyncpg.exceptions.DataError in production.
    """
    txn_uuid = uuid.UUID(txn_id)
    async with conn.transaction():
        res = await conn.execute(
            """
            UPDATE saga_transactions
            SET state = $1, error_reason = $2, updated_at = NOW()
            WHERE txn_id = $3 AND state = $4
            """,
            new_state, error_reason, txn_uuid, expected_state
        )
        if res == "UPDATE 1":
            if outbox_events:
                for ev in outbox_events:
                    await conn.execute(
                        """
                        INSERT INTO outbox_events (txn_id, topic, event_type, payload)
                        VALUES ($1, $2, $3, $4)
                        """,
                        txn_uuid, ev["topic"], ev["event_type"], json.dumps(ev["payload"])
                    )
        else:
            logger.info("Saga update preempted (txn_id: %s, expected: %s) - ignoring event.", txn_id, expected_state)

async def _query_bank_for_operation(bank_url: str, txn_id: str, operation: str, timeout: float = 5.0) -> str:
    """
    Query a bank service to determine whether a specific operation was executed.

    Returns:
        'SUCCESS'   — the operation was executed at the bank
        'NOT_FOUND' — the operation was NOT executed (safe to retry or compensate)
        'UNAVAILABLE' — the bank is unreachable (requires manual review)
    """
    if not bank_url:
        logger.error("Cannot query bank: no URL available", extra={"txn_id": txn_id, "operation": operation})
        return "UNAVAILABLE"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{bank_url}/transaction/{txn_id}?operation={operation}")
        if resp.status_code == 200:
            return resp.json().get("status", "SUCCESS")
        elif resp.status_code == 404:
            return "NOT_FOUND"
        else:
            logger.warning("Bank returned unexpected status during ambiguity check",
                           extra={"txn_id": txn_id, "operation": operation, "status_code": resp.status_code})
            return "UNAVAILABLE"
    except (httpx.RequestError, httpx.TimeoutException) as e:
        logger.warning("Bank unreachable during ambiguity check",
                       extra={"txn_id": txn_id, "operation": operation, "error": str(e)})
        return "UNAVAILABLE"


async def _get_routing_url(conn, vpa: str) -> str:
    """Retrieve the routing URL for a VPA."""
    row = await conn.fetchrow("SELECT bank_service_url FROM vpa_registry WHERE vpa = $1 AND is_active = TRUE", vpa)
    if row:
        return row["bank_service_url"]
    
    # Fallback for test compatibility
    from services.gateway.main import BANK_URLS
    bank_slug = vpa.split("@")[1] if "@" in vpa else None
    return BANK_URLS.get(bank_slug)
