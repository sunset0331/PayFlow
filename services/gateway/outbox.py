"""
Outbox Publisher Worker for PayFlow Gateway.

This module runs as a background asyncio task inside the Gateway process.
It polls the outbox_events table for unprocessed events and publishes them to Kafka.
After a successful publish, the event is marked as processed.
"""

import asyncio
import json
from shared.logger import get_logger

logger = get_logger("outbox")

# How often to run the outbox scan (seconds)
OUTBOX_POLL_INTERVAL_SECONDS = 2.0

async def run_outbox_publisher(db_pool, kafka_publish_fn) -> None:
    """
    Main outbox publisher loop. Runs indefinitely; designed to be started as an
    asyncio background task inside the Gateway process.
    """
    logger.info("Outbox publisher started. Polling every %ds for pending events.", OUTBOX_POLL_INTERVAL_SECONDS)

    while True:
        try:
            await _outbox_scan(db_pool, kafka_publish_fn)
        except Exception as e:
            logger.error("Outbox publisher scan failed: %s", e, exc_info=True)
        finally:
            await asyncio.sleep(OUTBOX_POLL_INTERVAL_SECONDS)

async def _outbox_scan(db_pool, kafka_publish_fn) -> None:
    """Single scan of pending outbox events."""
    async with db_pool.acquire() as conn:
        events = await conn.fetch(
            """
            SELECT id, txn_id, topic, event_type, payload
            FROM outbox_events
            WHERE processed_at IS NULL
            ORDER BY created_at ASC
            LIMIT 50
            """
        )

        if not events:
            return

        for event in events:
            try:
                async with conn.transaction():
                    locked = await conn.fetchrow(
                        "SELECT id FROM outbox_events WHERE id = $1 AND processed_at IS NULL FOR UPDATE SKIP LOCKED",
                        event['id']
                    )
                    if not locked:
                        continue

                    # payload is fetched as JSON string or asyncpg dict depending on parsing, we assume it's string.
                    payload_data = event['payload']
                    if isinstance(payload_data, str):
                        payload_data = json.loads(payload_data)

                    # Publish to Kafka
                    await kafka_publish_fn(
                        event['topic'], str(event['txn_id']), event['event_type'], payload_data
                    )
                    
                    # Mark as processed
                    await conn.execute(
                        "UPDATE outbox_events SET processed_at = NOW() WHERE id = $1",
                        event['id']
                    )
                    logger.debug("Published outbox event %s for txn %s", event['id'], event['txn_id'])
            except Exception as e:
                logger.error("Failed to process outbox event id=%s: %s", event['id'], e, exc_info=True)
