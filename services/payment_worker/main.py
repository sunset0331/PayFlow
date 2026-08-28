"""
Payment Worker

Consumes commands from the 'payment_commands' Kafka topic,
executes HTTP calls to the bank APIs, and publishes results
to the 'payment_events' Kafka topic.
"""

import asyncio
import json
import logging
import os
import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from shared.logger import get_logger

logger = get_logger("payment_worker")

KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "kafka:9092")

async def process_command(event: dict, producer: AIOKafkaProducer):
    """
    Process a single command from the Gateway.
    
    event format:
    {
      "txn_id": "...",
      "event_type": "debit_request",
      "payload": {
          "vpa": "sender@hdfc",
          "amount": 100.0,
          "bank_url": "http://bank-hdfc:8001"
      }
    }
    """
    txn_id = event.get("txn_id")
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    
    bank_url = payload.get("bank_url")
    amount = payload.get("amount")
    vpa = payload.get("vpa")
    
    if not bank_url or not amount or not vpa:
        logger.error("Invalid command payload", extra={"txn_id": txn_id, "payload": payload})
        return

    logger.info("Processing command %s", event_type, extra={"txn_id": txn_id})

    # Execute HTTP call to Bank
    async with httpx.AsyncClient(timeout=10.0) as client:
        if event_type == "debit_request":
            try:
                res = await client.post(
                    f"{bank_url}/debit",
                    json={
                        "vpa": vpa,
                        "amount": amount,
                        "txn_id": txn_id,
                        "operation_type": "DEBIT",
                    }
                )
                res.raise_for_status()
                await _publish_event(producer, txn_id, "debit_completed", {"vpa": vpa, "amount": amount})
                logger.info("Debit completed successfully", extra={"txn_id": txn_id})
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    await _publish_event(producer, txn_id, "debit_failed", {"reason": "Insufficient funds"})
                elif e.response.status_code == 404:
                    await _publish_event(producer, txn_id, "debit_failed", {"reason": "Account not found"})
                else:
                    await _publish_event(producer, txn_id, "debit_failed", {"reason": "Bank error"})
                    
            except httpx.RequestError as e:
                # Network-level failure: we do NOT know whether the bank executed the debit.
                # Emit debit_ambiguous so the Orchestrator can query the bank before deciding.
                logger.warning("Debit request timed out or network error — outcome unknown", extra={"txn_id": txn_id, "error": str(e)})
                await _publish_event(producer, txn_id, "debit_ambiguous", {"bank_url": bank_url, "reason": str(e)})
                
        elif event_type == "credit_request":
            try:
                res = await client.post(
                    f"{bank_url}/credit",
                    json={
                        "vpa": vpa,
                        "amount": amount,
                        "txn_id": txn_id,
                        "operation_type": "CREDIT",
                    }
                )
                res.raise_for_status()
                await _publish_event(producer, txn_id, "credit_completed", {"vpa": vpa, "amount": amount})
                logger.info("Credit completed successfully", extra={"txn_id": txn_id})
                
            except httpx.HTTPStatusError as e:
                # The bank responded with a clear error (4xx/5xx). The outcome is known: credit did NOT happen.
                logger.warning("Credit returned HTTP error", extra={"txn_id": txn_id, "status_code": e.response.status_code, "error": str(e)})
                await _publish_event(producer, txn_id, "credit_failed", {"reason": f"Receiver bank HTTP {e.response.status_code}"})
            except httpx.RequestError as e:
                # Network-level failure: we do NOT know whether the bank executed the credit.
                # Emit credit_ambiguous so the Orchestrator can query the bank before compensating.
                logger.warning("Credit request timed out or network error — outcome unknown", extra={"txn_id": txn_id, "error": str(e)})
                await _publish_event(producer, txn_id, "credit_ambiguous", {"bank_url": bank_url, "reason": str(e)})
                
        elif event_type == "compensate_request":
            try:
                res = await client.post(
                    f"{bank_url}/credit",
                    json={
                        "vpa": vpa,
                        "amount": amount,
                        "txn_id": txn_id,
                        "operation_type": "COMPENSATION",
                    }
                )
                res.raise_for_status()
                await _publish_event(producer, txn_id, "compensate_completed", {"vpa": vpa, "amount": amount})
                logger.info("Compensation completed successfully", extra={"txn_id": txn_id})
                
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                logger.error("Compensation failed or timed out", extra={"txn_id": txn_id, "error": str(e)})
                await _publish_event(producer, txn_id, "compensate_failed", {"reason": "Sender bank error or timeout"})

async def _publish_event(producer: AIOKafkaProducer, txn_id: str, event_type: str, payload: dict):
    event = {
        "txn_id": txn_id,
        "event_type": event_type,
        "payload": payload
    }
    await producer.send_and_wait(
        topic="payment_events",
        value=event,
        key=txn_id.encode('utf-8')
    )

async def main():
    logger.info("Starting Payment Worker...")
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()

    consumer = AIOKafkaConsumer(
        "payment_commands",
        bootstrap_servers=KAFKA_BROKER,
        group_id="payment-worker-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=False  # Manual commit after successful processing & publishing
    )
    await consumer.start()
    
    logger.info("Payment Worker listening on 'payment_commands'")
    
    try:
        async for msg in consumer:
            event = msg.value
            try:
                await process_command(event, producer)
                # Commit offset only after we successfully process and publish the result event
                await consumer.commit()
            except Exception as e:
                logger.error("Failed to process command %s: %s", event, e, exc_info=True)
                # DO NOT commit offset on failure. This ensures the message is redelivered
                # if the worker restarts. We pause briefly and raise to allow crash-loop backoff.
                await asyncio.sleep(5)
                raise
                
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
