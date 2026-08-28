from fastapi import FastAPI, Depends, HTTPException, Request, Header
from pydantic import BaseModel
import hashlib
from datetime import datetime
import httpx
import uuid
import asyncpg
import asyncio
import os
import json
from shared.logger import get_logger
from typing import Optional

from prometheus_client import Counter, Histogram, Gauge, generate_latest
from fastapi.responses import Response
import time
# Local shared modules
from shared.redis_client import get_redis
from shared.kafka_client import start_kafka_producer, stop_kafka_producer, publish_event
from shared.rate_limiter import enforce_rate_limits
from services.gateway.recovery import run_recovery_worker
from services.gateway.outbox import run_outbox_publisher
from services.gateway.orchestrator import run_orchestrator
from contextlib import asynccontextmanager

logger = get_logger("gateway")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start Kafka producer, connect to db_gateway, and launch background workers."""
    await start_kafka_producer()
    app.state.db_pool = await asyncpg.create_pool(GATEWAY_DB_URL, min_size=2, max_size=10)
    
    # Launch background tasks
    app.state.recovery_task = asyncio.create_task(_supervised_recovery_worker(app.state.db_pool))
    app.state.outbox_task = asyncio.create_task(_supervised_outbox_worker(app.state.db_pool))
    app.state.orchestrator_task = asyncio.create_task(_supervised_orchestrator_worker(app.state.db_pool))
    
    yield
    
    """Gracefully close Kafka producer, recovery/outbox workers, and database pool."""
    for task_name in ['recovery_task', 'outbox_task', 'orchestrator_task']:
        task = getattr(app.state, task_name, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    
    await stop_kafka_producer()
    if hasattr(app.state, 'db_pool') and app.state.db_pool:
        await app.state.db_pool.close()


app = FastAPI(title="PayFlow UPI Gateway", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GATEWAY_DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://payflow_admin:secretpassword@postgres:5432/db_gateway"
)

# Hardcoded service registry for local testing (Nginx will handle this in prod)
BANK_URLS = {
    "hdfc": os.getenv("HDFC_BANK_URL", "http://bank-hdfc:8001"),
    "sbi": os.getenv("SBI_BANK_URL", "http://bank-sbi:8002")
}

# ---------------------------------------------------------------------------
# Prometheus Metrics
# ---------------------------------------------------------------------------

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
payflow_compensations_total = Counter(
    'payflow_compensations_total', 'Total number of saga compensations executed', ['outcome']
)
payflow_lost_response_recoveries_total = Counter(
    'payflow_lost_response_recoveries_total',
    'Times a credit was confirmed complete despite a network error (lost response scenario)'
)
payflow_idempotency_hits_total = Counter(
    'payflow_idempotency_hits_total',
    'Duplicate requests detected and served from idempotency cache'
)
gateway_saga_states_total = Counter(
    'gateway_saga_states_total', 'Saga states entered', ['state']
)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class PaymentRequest(BaseModel):
    sender_vpa: str
    receiver_vpa: str
    amount: float

class VpaPayload(BaseModel):
    vpa: str
    bank_service_url: str
    account_id: str

# ---------------------------------------------------------------------------
# VPA Registry Endpoints
# ---------------------------------------------------------------------------

@app.post("/vpa")
async def register_vpa(payload: VpaPayload):
    """Register a new VPA to a bank routing URL."""
    async with app.state.db_pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO vpa_registry (vpa, bank_service_url, account_id, is_active)
                VALUES ($1, $2, $3, TRUE)
                """,
                payload.vpa, payload.bank_service_url, uuid.UUID(payload.account_id)
            )
            logger.info("VPA registered", extra={"vpa": payload.vpa, "url": payload.bank_service_url})
            return {"status": "SUCCESS", "message": f"VPA {payload.vpa} registered successfully"}
        except asyncpg.exceptions.UniqueViolationError:
            raise HTTPException(status_code=409, detail="VPA already registered")

@app.get("/vpa/{vpa}")
async def get_vpa(vpa: str):
    """Get routing details for a VPA."""
    async with app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT bank_service_url, is_active FROM vpa_registry WHERE vpa = $1", vpa)
        if not row:
            raise HTTPException(status_code=404, detail="VPA not found")
        return {"vpa": vpa, "bank_service_url": row["bank_service_url"], "is_active": row["is_active"]}

# ---------------------------------------------------------------------------
# Lifecycle Background Tasks
# ---------------------------------------------------------------------------

async def _supervised_orchestrator_worker(db_pool) -> None:
    """Wraps run_orchestrator with a restart loop."""
    while True:
        try:
            await run_orchestrator(db_pool)
        except asyncio.CancelledError:
            logger.info("Orchestrator worker cancelled. Shutting down.")
            break
        except Exception as e:
            logger.error("Orchestrator worker crashed, restarting in 5s: %s", e, exc_info=True)
            await asyncio.sleep(5)

async def _supervised_outbox_worker(db_pool) -> None:
    """Wraps run_outbox_publisher with a restart loop."""
    while True:
        try:
            await run_outbox_publisher(db_pool, publish_event)
        except asyncio.CancelledError:
            logger.info("Outbox worker cancelled. Shutting down.")
            break
        except Exception as e:
            logger.error("Outbox worker crashed, restarting in 5s: %s", e, exc_info=True)
            await asyncio.sleep(5)

async def _supervised_recovery_worker(db_pool) -> None:
    """Wraps run_recovery_worker with a restart loop so a crash doesn't kill recovery permanently."""
    while True:
        try:
            # We now rely on dynamic VPA routing inside recovery worker
            await run_recovery_worker(db_pool, publish_event)
        except asyncio.CancelledError:
            logger.info("Recovery worker cancelled. Shutting down.")
            break
        except Exception as e:
            logger.error("Recovery worker crashed, restarting in 10s: %s", e, exc_info=True)
            await asyncio.sleep(10)



# ---------------------------------------------------------------------------
# Saga state helpers
# ---------------------------------------------------------------------------

async def _saga_create(txn_id: str, sender_vpa: str, receiver_vpa: str, amount: float, idempotency_key: str, outbox_events: list = None) -> None:
    """Persist a new saga in INITIATED state and append to outbox transactionally."""
    async with app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO saga_transactions
                    (txn_id, sender_vpa, receiver_vpa, amount, state, idempotency_key)
                VALUES ($1, $2, $3, $4, 'INITIATED', $5)
                """,
                uuid.UUID(txn_id), sender_vpa, receiver_vpa, amount, idempotency_key
            )
            if outbox_events:
                for ev in outbox_events:
                    await conn.execute(
                        """
                        INSERT INTO outbox_events (txn_id, topic, event_type, payload)
                        VALUES ($1, $2, $3, $4)
                        """,
                        uuid.UUID(txn_id), ev["topic"], ev["event_type"], json.dumps(ev["payload"])
                    )

async def _saga_update(txn_id: str, state: str, error_reason: str = None, outbox_events: list = None) -> None:
    """Advance the saga to a new state and append to outbox transactionally."""
    async with app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE saga_transactions
                SET state = $1, error_reason = $2, updated_at = NOW()
                WHERE txn_id = $3
                """,
                state, error_reason, uuid.UUID(txn_id)
            )
            if outbox_events:
                for ev in outbox_events:
                    await conn.execute(
                        """
                        INSERT INTO outbox_events (txn_id, topic, event_type, payload)
                        VALUES ($1, $2, $3, $4)
                        """,
                        uuid.UUID(txn_id), ev["topic"], ev["event_type"], json.dumps(ev["payload"])
                    )
    gateway_saga_states_total.labels(state=state).inc()

async def _get_routing_url(vpa: str) -> str:
    """Retrieve the routing URL for a VPA. Falls back to hardcoded for testing if missing."""
    async with app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT bank_service_url FROM vpa_registry WHERE vpa = $1 AND is_active = TRUE", vpa)
        if row:
            return row["bank_service_url"]
    # Fallback to hardcoded dict for backward compatibility with old tests
    bank_slug = vpa.split("@")[1] if "@" in vpa else None
    return BANK_URLS.get(bank_slug)

# ---------------------------------------------------------------------------
# Idempotency dependency
# ---------------------------------------------------------------------------

# Idempotency TTL: how long a key is considered active.
# After this window, the same logical payment can be re-submitted.
IDEMP_TTL_SECONDS = 86400  # 24 hours

async def check_idempotency(
    request: Request,
    redis_client=Depends(get_redis),
    idempotency_key_header: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    """
    Proper idempotency using a client-supplied Idempotency-Key header.
    ... [truncated for brevity in comment, keeping original code]
    """
    body = await request.json()

    # Determine the idempotency key
    if idempotency_key_header:
        idemp_key = idempotency_key_header.strip()
    else:
        # Fallback: content hash WITHOUT the minute component (fixes minute-boundary bug)
        raw_string = f"{body.get('sender_vpa')}:{body.get('receiver_vpa')}:{body.get('amount')}"
        idemp_key = hashlib.sha256(raw_string.encode()).hexdigest()

    redis_key = f"idemp:{idemp_key}"
    # Separate key for payload fingerprint (to detect same-key + different-payload)
    payload_sig = hashlib.sha256(
        f"{body.get('sender_vpa')}:{body.get('receiver_vpa')}:{body.get('amount')}".encode()
    ).hexdigest()
    payload_key = f"idemp_payload:{idemp_key}"

    try:
        # Check for an existing entry
        stored_value = await redis_client.get(redis_key)
        stored_payload_sig = await redis_client.get(payload_key)

        if stored_value is not None:
            stored_value = stored_value.decode() if isinstance(stored_value, bytes) else stored_value
            stored_payload_sig = (
                stored_payload_sig.decode()
                if isinstance(stored_payload_sig, bytes)
                else stored_payload_sig
            )

            # Detect same key with different payload
            if stored_payload_sig and stored_payload_sig != payload_sig:
                raise HTTPException(
                    status_code=422,
                    detail="Idempotency key reused with a different payment payload."
                )

            if stored_value == "PROCESSING":
                # Another request is currently executing — concurrent duplicate
                raise HTTPException(status_code=409, detail="A payment with this idempotency key is already in progress.")

            # A completed result is cached — return it directly
            payflow_idempotency_hits_total.inc()
            try:
                cached = json.loads(stored_value)
                return (idemp_key, cached)  # signal to the handler: return this directly
            except (json.JSONDecodeError, ValueError):
                pass

        # New request: atomically claim the key
        claimed = await redis_client.set(
            redis_key, "PROCESSING", nx=True, ex=IDEMP_TTL_SECONDS
        )
        if not claimed:
            raise HTTPException(status_code=409, detail="A payment with this idempotency key is already in progress.")

        # Store payload signature alongside the key
        await redis_client.set(payload_key, payload_sig, ex=IDEMP_TTL_SECONDS)

        return (idemp_key, None)  # None = no cached result; proceed normally

    except HTTPException:
        raise
    except Exception as redis_err:
        raise HTTPException(
            status_code=503,
            detail="Idempotency service unavailable. Please retry later."
        ) from redis_err

# ---------------------------------------------------------------------------
# Metrics endpoint
# ---------------------------------------------------------------------------

@app.get("/metrics")
async def metrics():
    """Endpoint for Prometheus to scrape metrics."""
    return Response(content=generate_latest(), media_type="text/plain")

# ---------------------------------------------------------------------------
# Compensation helpers (REMOVED - Now handled by Orchestrator)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Payment endpoint
# ---------------------------------------------------------------------------

@app.post("/pay", status_code=202)
async def initiate_payment(
    payload: PaymentRequest,
    idemp_result: tuple = Depends(check_idempotency),
    redis_conn=Depends(get_redis)
):
    idemp_key, cached_response = idemp_result

    # If this is a duplicate of a completed request, return the cached result immediately
    if cached_response is not None:
        return cached_response

    start_time = time.time()
    payflow_active_requests.inc()

    try:
        txn_id = str(uuid.uuid4())
        
        sender_url = await _get_routing_url(payload.sender_vpa)
        receiver_url = await _get_routing_url(payload.receiver_vpa)

        if not sender_url or not receiver_url:
             raise HTTPException(status_code=400, detail="Invalid VPA routing")

        # --- RATE LIMITS ---
        try:
            await enforce_rate_limits(payload.sender_vpa, payload.receiver_vpa, payload.amount, redis_conn)
        except HTTPException as e:
            if e.status_code == 429:
                payflow_rate_limit_hits_total.inc()
            raise e

        # ----------------------------------------------------------------
        # ASYNC SAGA CREATION (OUTBOX PATTERN)
        # ----------------------------------------------------------------
        await _saga_create(
            txn_id, payload.sender_vpa, payload.receiver_vpa, payload.amount, idemp_key,
            [
                {
                    "topic": "payment_events",
                    "event_type": "PAYMENT_INITIATED",
                    "payload": {
                        "amount": payload.amount,
                        "sender": payload.sender_vpa,
                        "receiver": payload.receiver_vpa
                    }
                }
            ]
        )
        
        # Advance state to DEBIT_PENDING and dispatch command to payment worker
        await _saga_update(
            txn_id, "DEBIT_PENDING", None,
            [
                {
                    "topic": "payment_commands",
                    "event_type": "debit_request",
                    "payload": {
                        "vpa": payload.sender_vpa,
                        "amount": payload.amount,
                        "bank_url": sender_url
                    }
                }
            ]
        )

        logger.info(
            f"Payment initiated asynchronously for {payload.amount} from {payload.sender_vpa} to {payload.receiver_vpa}",
            extra={"txn_id": txn_id, "event": "PAYMENT_INITIATED_ASYNC"}
        )

        # We don't cache 202 Accepted in Redis for idempotency because the status will change.
        # Idempotency middleware handles "PROCESSING" locking.
        
        return {
            "status": "PROCESSING",
            "txn_id": txn_id,
            "message": "Payment initiated and is processing in the background."
        }

    except HTTPException as e:
        payflow_transactions_total.labels(status='failed').inc()
        raise e

    except Exception as e:
        payflow_transactions_total.labels(status='error').inc()
        raise e

    finally:
        duration = time.time() - start_time
        payflow_transaction_duration_seconds.observe(duration)
        payflow_active_requests.dec()

@app.get("/pay/{txn_id}")
async def get_payment_status(txn_id: str):
    """Poll for the status of a payment."""
    async with app.state.db_pool.acquire() as conn:
        saga = await conn.fetchrow(
            "SELECT state, error_reason FROM saga_transactions WHERE txn_id = $1",
            uuid.UUID(txn_id)
        )
        if not saga:
            raise HTTPException(status_code=404, detail="Transaction not found")
            
        return {
            "txn_id": txn_id,
            "status": saga["state"],
            "error_reason": saga["error_reason"]
        }