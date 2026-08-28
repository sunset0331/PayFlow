"""
Saga Orchestrator for PayFlow Gateway.

This module consumes events from the 'payment.events' Kafka topic,
updates the saga_transactions state in PostgreSQL, and generates new commands
to be published to 'payment.commands' via the Outbox table.
"""

import asyncio
import json
import os
import time
from aiokafka import AIOKafkaConsumer
from shared.logger import get_logger

logger = get_logger("orchestrator")
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "kafka:9092")

async def run_orchestrator(db_pool) -> None:
    """Main orchestrator consumer loop."""
    logger.info("Gateway Orchestrator started. Listening to 'payment.events'.")

    consumer = AIOKafkaConsumer(
        "payment.events",
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
            try:
                await _process_event(db_pool, event)
                await consumer.commit()
            except Exception as e:
                logger.error("Failed to process event %s: %s", event, e, exc_info=True)
                # Retry logic/DLQ would be here. Committing to avoid blocking for this implementation.
                await consumer.commit()
    finally:
        await consumer.stop()

async def _process_event(db_pool, event: dict):
    txn_id = event.get("txn_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    
    logger.info("Orchestrator received event %s for txn %s", event_type, txn_id)

    async with db_pool.acquire() as conn:
        # Fetch current saga state
        saga = await conn.fetchrow(
            "SELECT sender_vpa, receiver_vpa, amount, state FROM saga_transactions WHERE txn_id = $1",
            txn_id
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
                conn, txn_id, "CREDIT_PENDING", None,
                [
                    {
                        "topic": "payment.commands",
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
                conn, txn_id, "FAILED", payload.get("reason"),
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
                conn, txn_id, "COMPLETED", None,
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
                conn, txn_id, "COMPENSATING", payload.get("reason", "Receiver credit failed"),
                [
                    {
                        "topic": "payment.commands",
                        "event_type": "compensate_request",
                        "payload": {
                            "vpa": sender_vpa,
                            "amount": amount,
                            "bank_url": sender_url
                        }
                    }
                ]
            )

        elif event_type == "compensate_completed" and current_state == "COMPENSATING":
            # Move to COMPENSATED
            await _advance_saga(
                conn, txn_id, "COMPENSATED", None,
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
                conn, txn_id, "COMPENSATION_FAILED", payload.get("reason", "Both credit and compensation failed"),
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

async def _advance_saga(conn, txn_id: str, new_state: str, error_reason: str, outbox_events: list):
    """Update saga state and append to outbox transactionally."""
    async with conn.transaction():
        await conn.execute(
            """
            UPDATE saga_transactions
            SET state = $1, error_reason = $2, updated_at = NOW()
            WHERE txn_id = $3
            """,
            new_state, error_reason, txn_id
        )
        if outbox_events:
            for ev in outbox_events:
                await conn.execute(
                    """
                    INSERT INTO outbox_events (txn_id, topic, event_type, payload)
                    VALUES ($1, $2, $3, $4)
                    """,
                    txn_id, ev["topic"], ev["event_type"], json.dumps(ev["payload"])
                )

async def _get_routing_url(conn, vpa: str) -> str:
    """Retrieve the routing URL for a VPA."""
    row = await conn.fetchrow("SELECT bank_service_url FROM vpa_registry WHERE vpa = $1 AND is_active = TRUE", vpa)
    if row:
        return row["bank_service_url"]
    
    # Fallback for test compatibility
    from services.gateway.main import BANK_URLS
    bank_slug = vpa.split("@")[1] if "@" in vpa else None
    return BANK_URLS.get(bank_slug)
