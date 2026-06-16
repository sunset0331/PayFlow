from fastapi import FastAPI, Depends, HTTPException, Request
from pydantic import BaseModel
import hashlib
from datetime import datetime
import httpx
import uuid


from prometheus_client import Counter, Histogram, Gauge, generate_latest
from fastapi.responses import Response
import time
# Local shared modules
from shared.redis_client import get_redis
from shared.kafka_client import start_kafka_producer, stop_kafka_producer, publish_event
from shared.rate_limiter import enforce_rate_limits

app = FastAPI(title="PayFlow UPI Gateway")
# Prometheus Metrics
payflow_transactions_total = Counter(
    'payflow_transactions_total', 'Total number of transactions processed', ['status']
)
payflow_transaction_duration_seconds = Histogram(
    'payflow_transaction_duration_seconds', 'Transaction latency in seconds'
)
payflow_active_requests = Gauge(
    'payflow_active_requests', 'Number of currently active payment requests'
)
payflow_rate_limit_hits_total = Counter(
    'payflow_rate_limit_hits_total', 'Total number of rate limit blocks'
)
# Hardcoded service registry for local testing (Nginx will handle this in prod)
BANK_URLS = {
    "hdfc": "http://127.0.0.1:8001",
    "sbi": "http://127.0.0.1:8002"
}

class PaymentRequest(BaseModel):
    sender_vpa: str
    receiver_vpa: str
    amount: float

@app.on_event("startup")
async def startup_event():
    """Start the Kafka producer when the FastAPI gateway spins up."""
    await start_kafka_producer()

@app.on_event("shutdown")
async def shutdown_event():
    """Gracefully close the Kafka producer on shutdown."""
    await stop_kafka_producer()

async def check_idempotency(request: Request, redis_client=Depends(get_redis)):
    """
    Middleware/Dependency to check for duplicate payment requests
    using an atomic Redis SETNX operation.
    """
    body = await request.json()
    
    # Generate deterministic hash based on payload and current minute
    raw_string = f"{body.get('sender_vpa')}:{body.get('receiver_vpa')}:{body.get('amount')}:{datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"
    idempotency_key = hashlib.sha256(raw_string.encode()).hexdigest()
    
    # Atomic Set if Not eXists with 24-hour TTL
    is_new = await redis_client.set(name=f"idemp:{idempotency_key}", value="PROCESSING", nx=True, ex=86400)
    
    if not is_new:
        raise HTTPException(status_code=409, detail="Duplicate payment detected.")
    
    return idempotency_key

@app.get("/metrics")
async def metrics():
    """Endpoint for Prometheus to scrape metrics."""
    return Response(content=generate_latest(), media_type="text/plain")

@app.post("/pay")
async def initiate_payment(
    payload: PaymentRequest, 
    idemp_key: str = Depends(check_idempotency),
    redis_conn=Depends(get_redis)
):
    """
    Core UPI Payment Orchestrator using the Saga Pattern, fully instrumented
    with Prometheus metrics for latency, throughput, and error tracking.
    """
    # 1. METRICS: Track start time and active requests
    start_time = time.time()
    payflow_active_requests.inc() 
    
    try:
        txn_id = str(uuid.uuid4())
        sender_bank = payload.sender_vpa.split("@")[1]
        receiver_bank = payload.receiver_vpa.split("@")[1]
        
        sender_url = BANK_URLS.get(sender_bank)
        receiver_url = BANK_URLS.get(receiver_bank)

        if not sender_url or not receiver_url:
             raise HTTPException(status_code=400, detail="Invalid VPA routing")
        
        # --- RATE LIMITS WITH METRICS ---
        try:
            await enforce_rate_limits(payload.sender_vpa, payload.receiver_vpa, payload.amount, redis_conn)
        except HTTPException as e:
            if e.status_code == 429:
                payflow_rate_limit_hits_total.inc() # Record the block in Prometheus
            raise e
        # --------------------------------

        # PUBLISH EVENT: Transaction Initiated
        await publish_event("payment_events", txn_id, "PAYMENT_INITIATED", {
            "amount": payload.amount, 
            "sender": payload.sender_vpa,
            "receiver": payload.receiver_vpa
        })

        async with httpx.AsyncClient(timeout=5.0) as client:
            # SAGA STEP 1: DEBIT SENDER
            try:
                debit_res = await client.post(
                    f"{sender_url}/debit", 
                    json={"vpa": payload.sender_vpa, "amount": payload.amount, "txn_id": txn_id}
                )
                debit_res.raise_for_status()
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    await publish_event("payment_events", txn_id, "PAYMENT_FAILED", {"reason": "Insufficient funds"})
                    raise HTTPException(status_code=400, detail="Insufficient funds")
                
                await publish_event("payment_events", txn_id, "PAYMENT_FAILED", {"reason": "Sender bank unavailable"})
                raise HTTPException(status_code=502, detail="Sender bank unavailable")
                
            except httpx.RequestError:
                await publish_event("payment_events", txn_id, "PAYMENT_FAILED", {"reason": "Sender bank unreachable"})
                raise HTTPException(status_code=502, detail="Sender bank unreachable")

            # SAGA STEP 2: CREDIT RECEIVER
            try:
                credit_res = await client.post(
                    f"{receiver_url}/credit", 
                    json={"vpa": payload.receiver_vpa, "amount": payload.amount, "txn_id": txn_id}
                )
                credit_res.raise_for_status()
                
                # PUBLISH EVENT: Transaction Success
                await publish_event("payment_events", txn_id, "PAYMENT_SUCCESS", {"status": "completed"})
                
            except (httpx.HTTPStatusError, httpx.RequestError):
                # SAGA STEP 3: COMPENSATION (ROLLBACK)
                comp_txn_id = str(uuid.uuid4())
                await client.post(
                    f"{sender_url}/credit", 
                    json={"vpa": payload.sender_vpa, "amount": payload.amount, "txn_id": comp_txn_id}
                )
                
                # PUBLISH EVENT: Transaction Compensated
                await publish_event("payment_events", txn_id, "PAYMENT_COMPENSATED", {"reason": "Receiver unavailable, refunding sender"})
                raise HTTPException(status_code=500, detail="Receiver bank failed. Payment reversed and compensated.")

        # 2. METRICS: Record Successful Transaction
        payflow_transactions_total.labels(status='success').inc()

        return {
            "status": "SUCCESS",
            "txn_id": txn_id,
            "message": f"Successfully transferred ₹{payload.amount}",
            "idempotency_key": idemp_key
        }

    except HTTPException as e:
        # 3. METRICS: Record Known Failures (400, 429, 500, 502)
        payflow_transactions_total.labels(status='failed').inc()
        raise e
        
    except Exception as e:
        # 4. METRICS: Record Unknown System Crashes
        payflow_transactions_total.labels(status='error').inc()
        raise e
        
    finally:
        # 5. METRICS: Always measure latency and free up the active request gauge
        # The finally block guarantees this runs even if the request crashes halfway through
        duration = time.time() - start_time
        payflow_transaction_duration_seconds.observe(duration)
        payflow_active_requests.dec()