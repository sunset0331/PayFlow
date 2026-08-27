from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaConsumer
import asyncpg
import asyncio
import json
import uuid
import hashlib
import os
from shared.logger import get_logger

logger = get_logger("ledger")

app = FastAPI(title="Ledger Service")
DB_URL = os.getenv("DATABASE_URL", "postgresql://payflow_admin:secretpassword@127.0.0.1:5433/db_ledger")
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "127.0.0.1:9092")

# ---------------------------------------------------------------------------
# Dead-Letter Queue handling
# ---------------------------------------------------------------------------
# Messages that fail to process MAX_CONSECUTIVE_FAILURES times in a row
# are moved to the DLQ (a local in-memory list for now; replace with a
# Kafka DLQ topic or a DB table in production).
MAX_RETRIES_PER_MESSAGE = 3
_dlq: list = []  # In-memory DLQ; replace with a persistent store for production


async def _process_event(conn, event: dict) -> None:
    """
    Process a single Kafka event and persist it to the ledger.

    Idempotency: event_id = deterministic UUID from (txn_id, event_type).
    ON CONFLICT DO NOTHING ensures duplicate Kafka deliveries are no-ops.
    """
    # IDEMPOTENT CONSUMER LOGIC: Generate a deterministic event_id
    # based on the transaction ID and the event type.
    raw_string = f"{event['txn_id']}:{event['event_type']}"
    event_id = uuid.UUID(hashlib.md5(raw_string.encode()).hexdigest())

    # The Ledger is APPEND ONLY. ON CONFLICT DO NOTHING handles duplicate Kafka messages.
    await conn.execute("""
        INSERT INTO events (event_id, txn_id, event_type, payload_json)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (event_id) DO NOTHING
    """, event_id, uuid.UUID(event['txn_id']), event['event_type'], json.dumps(event['payload']))

    logger.info("Ledger entry written", extra={"txn_id": event['txn_id'], "event": "LEDGER_ENTRY_WRITTEN", "event_type": event['event_type']})


async def consume_events() -> None:
    """
    Kafka consumer loop with:
      - Idempotent event processing (ON CONFLICT DO NOTHING)
      - Per-message retry (up to MAX_RETRIES_PER_MESSAGE)
      - Dead-letter handling for poison messages
      - Structured logging (JSON format)

    Auto-commit is enabled (aiokafka default). Since we use ON CONFLICT DO NOTHING,
    reprocessing after a crash is safe — the ledger entry already exists.
    """
    consumer = AIOKafkaConsumer(
        "payment_events",
        bootstrap_servers=KAFKA_BROKER,
        group_id="ledger-group",       # Independent consumer group
        auto_offset_reset="earliest",  # Read from beginning if newly deployed
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    logger.info("Ledger consumer started.", extra={"event": "CONSUMER_STARTED"})
    try:
        async for msg in consumer:
            event = msg.value
            retries = 0
            while retries <= MAX_RETRIES_PER_MESSAGE:
                try:
                    async with app.state.pool.acquire() as conn:
                        await _process_event(conn, event)
                    break  # Success
                except Exception as e:
                    retries += 1
                    if retries > MAX_RETRIES_PER_MESSAGE:
                        # Poison message: move to DLQ, commit offset, continue
                        logger.error(
                            "Moved message to DLQ after %d retries",
                            MAX_RETRIES_PER_MESSAGE,
                            extra={"txn_id": event.get('txn_id'), "event": "LEDGER_DLQ_ROUTED", "event_type": event.get('event_type')},
                            exc_info=True
                        )
                        _dlq.append({
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "event": event,
                            "error": str(e),
                        })
                    else:
                        wait = 2 ** retries  # exponential backoff: 2s, 4s, 8s
                        logger.warning(
                            "Retrying message",
                            extra={"txn_id": event.get('txn_id'), "event": "LEDGER_RETRY", "retry_count": retries, "wait_seconds": wait},
                            exc_info=True
                        )
                        await asyncio.sleep(wait)
    finally:
        await consumer.stop()
        logger.info("Ledger consumer stopped.")


async def _supervised_consumer() -> None:
    """
    Wraps consume_events() with a restart loop.
    If the consumer crashes (e.g. Kafka broker temporarily unavailable),
    it waits 5 seconds and restarts rather than dying silently.
    """
    while True:
        try:
            await consume_events()
        except asyncio.CancelledError:
            logger.info("Ledger consumer task cancelled. Shutting down.")
            break
        except Exception as e:
            logger.error("Ledger consumer crashed, restarting in 5s: %s", e, exc_info=True)
            await asyncio.sleep(5)


@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    # Start supervised consumer — if it crashes, it restarts automatically
    app.state.consumer_task = asyncio.create_task(_supervised_consumer())

@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, 'consumer_task') and app.state.consumer_task:
        app.state.consumer_task.cancel()
        try:
            await app.state.consumer_task
        except asyncio.CancelledError:
            pass
    if app.state.pool:
        await app.state.pool.close()

@app.get("/ledger/{txn_id}")
async def get_transaction_history(txn_id: str):
    async with app.state.pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT event_type, payload_json, timestamp FROM events WHERE txn_id = $1 ORDER BY timestamp ASC",
            uuid.UUID(txn_id)
        )
        if not records:
            raise HTTPException(status_code=404, detail="Transaction not found in ledger")
        return [
            {"event_type": r['event_type'], "payload": json.loads(r['payload_json']), "timestamp": r['timestamp']}
            for r in records
        ]

@app.get("/ledger/dlq/peek")
async def peek_dlq():
    """Inspect messages that failed processing and ended up in the DLQ."""
    return {"dlq_size": len(_dlq), "messages": _dlq[-10:]}  # Return last 10 DLQ entries