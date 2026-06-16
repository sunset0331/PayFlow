import asyncio
from aiokafka import AIOKafkaConsumer
import json

async def start_notification_worker():
    consumer = AIOKafkaConsumer(
        "payment_events",
        bootstrap_servers="127.0.0.1:9092",
        group_id="notification-group", # DIFFERENT group id than the Ledger!
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    print("Notification Service listening for events...")
    try:
        async for msg in consumer:
            event = msg.value
            event_type = event['event_type']
            
            if event_type == "PAYMENT_SUCCESS":
                print(f"📱 SMS ALERT: Payment {event['txn_id']} successful!")
            elif event_type in ["PAYMENT_FAILED", "PAYMENT_COMPENSATED"]:
                print(f"⚠️ SMS ALERT: Payment {event['txn_id']} failed. Reason: {event['payload'].get('reason')}")
            # We ignore PAYMENT_INITIATED for SMS to avoid spamming the user
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(start_notification_worker())