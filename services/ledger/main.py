from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaConsumer
import asyncpg
import asyncio
import json
import uuid
import hashlib
import os

app = FastAPI(title="Ledger Service")
DB_URL = os.getenv("DATABASE_URL", "postgresql://payflow_admin:secretpassword@127.0.0.1:5433/db_ledger")
KAFKA_BROKER = "127.0.0.1:9092"

async def consume_events():
    consumer = AIOKafkaConsumer(
        "payment_events",
        bootstrap_servers=KAFKA_BROKER,
        group_id="ledger-group",      # Independent consumer group
        auto_offset_reset="earliest", # Read from the beginning if newly deployed
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            event = msg.value
            
            # IDEMPOTENT CONSUMER LOGIC: Generate a deterministic event_id
            # based on the transaction ID and the event type.
            raw_string = f"{event['txn_id']}:{event['event_type']}"
            event_id = uuid.UUID(hashlib.md5(raw_string.encode()).hexdigest())

            async with app.state.pool.acquire() as conn:
                # The Ledger is APPEND ONLY. ON CONFLICT DO NOTHING handles duplicate Kafka messages.
                await conn.execute("""
                    INSERT INTO events (event_id, txn_id, event_type, payload_json) 
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (event_id) DO NOTHING
                """, event_id, uuid.UUID(event['txn_id']), event['event_type'], json.dumps(event['payload']))
                
            print(f"[Ledger] Recorded: {event['txn_id']} -> {event['event_type']}")
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    # Start consumer in the background so it doesn't block FastAPI HTTP serving
    asyncio.create_task(consume_events())

@app.get("/ledger/{txn_id}")
async def get_transaction_history(txn_id: str):
    async with app.state.pool.acquire() as conn:
        records = await conn.fetch("SELECT event_type, payload_json, timestamp FROM events WHERE txn_id = $1 ORDER BY timestamp ASC", uuid.UUID(txn_id))
        if not records:
            raise HTTPException(status_code=404, detail="Transaction not found in ledger")
        return [{"event_type": r['event_type'], "payload": json.loads(r['payload_json']), "timestamp": r['timestamp']} for r in records]