"""
PayFlow Notification Service.

Consumes payment_events from Kafka and sends user-facing alerts (SMS/email).

Idempotency:
  Uses an in-memory seen-set keyed by (txn_id, event_type).
  This prevents sending duplicate notifications if Kafka redelivers a message.

  For production: replace _seen_notifications with a Redis SET or a
  'notification_dedup' DB table with TTL, so the dedup survives restarts.

Supervision:
  The consumer runs in a supervised loop — if it crashes (Kafka broker
  temporarily unavailable), it restarts automatically after a backoff.
"""

import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer
from shared.logger import get_logger

logger = get_logger("notification")

KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "127.0.0.1:9092")

# In-memory deduplication set: (txn_id, event_type) pairs we've already notified about.
# Bounded to avoid unbounded memory growth — evict oldest entries when over limit.
MAX_DEDUP_SET_SIZE = 100_000
_seen_notifications: set = set()


def _is_duplicate(txn_id: str, event_type: str) -> bool:
    """Check if we've already sent a notification for this (txn_id, event_type)."""
    return (txn_id, event_type) in _seen_notifications


def _mark_seen(txn_id: str, event_type: str) -> None:
    """Record that we've sent a notification for this (txn_id, event_type)."""
    if len(_seen_notifications) >= MAX_DEDUP_SET_SIZE:
        # Simple eviction: clear oldest half (production should use a proper TTL cache)
        entries = list(_seen_notifications)
        _seen_notifications.clear()
        _seen_notifications.update(entries[len(entries) // 2:])
    _seen_notifications.add((txn_id, event_type))


def _send_notification(event: dict) -> None:
    """
    Deliver the user-facing notification.

    In production, replace the print() statements with actual SMS/push/email
    API calls (e.g. Twilio, Firebase FCM, SendGrid).
    """
    event_type = event.get('event_type')
    txn_id = event.get('txn_id')
    payload = event.get('payload', {})

    if event_type == "PAYMENT_SUCCESS":
        amount = payload.get('amount', '?')
        logger.info(f"📱 SMS ALERT [{txn_id}]: Your payment of ₹{amount} was successful!", extra={"txn_id": txn_id, "event_type": event_type, "event": "NOTIFICATION_PROCESSED"})

    elif event_type == "PAYMENT_FAILED":
        reason = payload.get('reason', 'unknown')
        logger.info(f"⚠️ SMS ALERT [{txn_id}]: Your payment failed. Reason: {reason}", extra={"txn_id": txn_id, "event_type": event_type, "event": "NOTIFICATION_PROCESSED"})

    elif event_type == "PAYMENT_COMPENSATED":
        reason = payload.get('reason', 'unknown')
        logger.info(f"🔄 SMS ALERT [{txn_id}]: Your payment was reversed and you have been refunded. Reason: {reason}", extra={"txn_id": txn_id, "event_type": event_type, "event": "NOTIFICATION_PROCESSED"})

    elif event_type == "PAYMENT_INDETERMINATE":
        logger.warning(f"🚨 ALERT [{txn_id}]: Payment state is unclear. Please contact support.", extra={"txn_id": txn_id, "event_type": event_type, "event": "NOTIFICATION_PROCESSED"})

    elif event_type == "COMPENSATION_FAILED":
        logger.error(
            f"🚨 CRITICAL ALERT [{txn_id}]: Payment failed AND refund failed. Sender={payload.get('sender')} Amount={payload.get('amount')}. MANUAL INTERVENTION REQUIRED.",
            extra={"txn_id": txn_id, "event_type": event_type, "event": "NOTIFICATION_PROCESSED"}
        )
    # We deliberately skip PAYMENT_INITIATED to avoid spamming the user
    # before the payment is confirmed.


async def consume_events() -> None:
    """
    Kafka consumer loop with per-message idempotency deduplication and error handling.
    """
    consumer = AIOKafkaConsumer(
        "payment_events",
        bootstrap_servers=KAFKA_BROKER,
        group_id="notification-group",  # Different group_id from ledger — independent offset tracking
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    logger.info("Notification consumer started.", extra={"event": "CONSUMER_STARTED"})
    try:
        async for msg in consumer:
            event = msg.value
            txn_id = event.get('txn_id', '')
            event_type = event.get('event_type', '')

            # Idempotency check: skip if we already notified for this (txn_id, event_type)
            if _is_duplicate(txn_id, event_type):
                logger.debug("Skipping duplicate notification", extra={"txn_id": txn_id, "event": "NOTIFICATION_DUPLICATE_SKIPPED"})
                continue

            try:
                _send_notification(event)
                _mark_seen(txn_id, event_type)
            except Exception as e:
                # Notification delivery failure: log and continue.
                # We do NOT retry indefinitely — a failed SMS alert is not worth
                logger.error("Notification delivery failed", extra={"txn_id": txn_id, "event_type": event_type, "event": "NOTIFICATION_ERROR"}, exc_info=True)
    finally:
        await consumer.stop()
        logger.info("Notification consumer stopped.", extra={"event": "CONSUMER_STOPPED"})


async def _supervised_consumer() -> None:
    """
    Wraps consume_events() with a supervised restart loop.
    Restarts automatically after any crash (e.g. Kafka broker unavailable).
    """
    while True:
        try:
            await consume_events()
        except asyncio.CancelledError:
            logger.info("Notification consumer task cancelled. Shutting down.")
            break
        except Exception as e:
            logger.error("Notification consumer crashed, restarting in 5s: %s", e, exc_info=True)
            await asyncio.sleep(5)


async def start_notification_worker():
    """Start the notification worker (supervised). Called by the process entry-point."""
    await _supervised_consumer()


if __name__ == "__main__":
    asyncio.run(start_notification_worker())