from aiokafka import AIOKafkaProducer
import json
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER_URL", "kafka:9092")

# Global producer instance
producer: AIOKafkaProducer = None

async def start_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()

async def stop_kafka_producer():
    global producer
    if producer:
        await producer.stop()

async def publish_event(topic: str, txn_id: str, event_type: str, payload: dict):
    global producer
    event = {
        "txn_id": txn_id,
        "event_type": event_type,
        "payload": payload
    }
    # RESUME METRIC: Partition Key
    # By using txn_id as the key, Kafka guarantees that all events for a specific 
    # transaction are routed to the same partition and processed in exact chronological order.
    await producer.send_and_wait(
        topic=topic,
        value=event,
        key=txn_id.encode('utf-8')
    )