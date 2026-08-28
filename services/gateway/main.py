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

logger = get_logger("gateway")

app = FastAPI(title="PayFlow UPI Gateway")

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
# Lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    """Start Kafka producer, connect to db_gateway, and launch recovery worker."""
    await start_kafka_producer()
    app.state.db_pool = await asyncpg.create_pool(GATEWAY_DB_URL, min_size=2, max_size=10)
    # Launch the crash recovery worker as a supervised background task.
    # If it crashes, it logs the error but does not take down the gateway.
    app.state.recovery_task = asyncio.create_task(
        _supervised_recovery_worker(app.state.db_pool)
    )

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

@app.on_event("shutdown")
async def shutdown_event():
    """Gracefully close Kafka producer, recovery worker, and database pool."""
    if hasattr(app.state, 'recovery_task') and app.state.recovery_task:
        app.state.recovery_task.cancel()
        try:
            await app.state.recovery_task
        except asyncio.CancelledError:
            pass
    await stop_kafka_producer()
    if app.state.db_pool:
        await app.state.db_pool.close()

# ---------------------------------------------------------------------------
# Saga state helpers
# ---------------------------------------------------------------------------

async def _saga_create(txn_id: str, sender_vpa: str, receiver_vpa: str, amount: float, idempotency_key: str) -> None:
    """Persist a new saga in INITIATED state."""
    async with app.state.db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO saga_transactions
                (txn_id, sender_vpa, receiver_vpa, amount, state, idempotency_key)
            VALUES ($1, $2, $3, $4, 'INITIATED', $5)
            """,
            uuid.UUID(txn_id), sender_vpa, receiver_vpa, amount, idempotency_key
        )

async def _saga_update(txn_id: str, state: str, error_reason: str = None) -> None:
    """Advance the saga to a new state, always updating the updated_at timestamp."""
    async with app.state.db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE saga_transactions
            SET state = $1, error_reason = $2, updated_at = NOW()
            WHERE txn_id = $3
            """,
            state, error_reason, uuid.UUID(txn_id)
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
# Compensation helpers
# ---------------------------------------------------------------------------

async def _query_bank_transaction(
    client: httpx.AsyncClient,
    bank_url: str,
    txn_id: str,
    operation: str,
) -> bool | None:
    try:
        resp = await client.get(
            f"{bank_url}/transaction/{txn_id}",
            params={"operation": operation},
        )
        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            return False
        return None
    except (httpx.RequestError, httpx.HTTPStatusError):
        return None


async def _execute_compensation(
    client: httpx.AsyncClient,
    sender_url: str,
    sender_vpa: str,
    amount: float,
    txn_id: str,
) -> bool:
    try:
        comp_res = await client.post(
            f"{sender_url}/credit",
            json={
                "vpa": sender_vpa,
                "amount": amount,
                "txn_id": txn_id,
                "operation_type": "COMPENSATION",
            },
        )
        comp_res.raise_for_status()
        return True
    except (httpx.HTTPStatusError, httpx.RequestError):
        return False

# ---------------------------------------------------------------------------
# Payment endpoint
# ---------------------------------------------------------------------------

@app.post("/pay")
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
        # DURABLE SAGA CREATION
        # ----------------------------------------------------------------
        await _saga_create(txn_id, payload.sender_vpa, payload.receiver_vpa, payload.amount, idemp_key)

        logger.info(
            f"Payment initiated for {payload.amount} from {payload.sender_vpa} to {payload.receiver_vpa}",
            extra={"txn_id": txn_id, "event": "PAYMENT_INITIATED"}
        )

        await publish_event("payment_events", txn_id, "PAYMENT_INITIATED", {
            "amount": payload.amount,
            "sender": payload.sender_vpa,
            "receiver": payload.receiver_vpa
        })

        async with httpx.AsyncClient(timeout=5.0) as client:
            # ----------------------------------------------------------------
            # SAGA STEP 1: DEBIT SENDER
            # ----------------------------------------------------------------
            await _saga_update(txn_id, "DEBIT_PENDING")
            try:
                debit_res = await client.post(
                    f"{sender_url}/debit",
                    json={
                        "vpa": payload.sender_vpa,
                        "amount": payload.amount,
                        "txn_id": txn_id,
                        "operation_type": "DEBIT",
                    }
                )
                debit_res.raise_for_status()
                await _saga_update(txn_id, "DEBIT_COMPLETED")
                logger.info("Debit completed successfully", extra={"txn_id": txn_id, "event": "DEBIT_COMPLETED"})

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    await _saga_update(txn_id, "FAILED", "Insufficient funds")
                    logger.warning("Debit failed: Insufficient funds", extra={"txn_id": txn_id, "event": "PAYMENT_FAILED"})
                    await publish_event("payment_events", txn_id, "PAYMENT_FAILED", {"reason": "Insufficient funds"})
                    raise HTTPException(status_code=400, detail="Insufficient funds")

                await _saga_update(txn_id, "FAILED", "Sender bank returned error")
                logger.error("Debit failed: Sender bank returned error", extra={"txn_id": txn_id, "event": "PAYMENT_FAILED"}, exc_info=True)
                await publish_event("payment_events", txn_id, "PAYMENT_FAILED", {"reason": "Sender bank unavailable"})
                raise HTTPException(status_code=502, detail="Sender bank unavailable")

            except httpx.RequestError as e:
                await _saga_update(txn_id, "FAILED", "Sender bank unreachable")
                logger.error("Debit failed: Sender bank unreachable", extra={"txn_id": txn_id, "event": "PAYMENT_FAILED"}, exc_info=True)
                await publish_event("payment_events", txn_id, "PAYMENT_FAILED", {"reason": "Sender bank unreachable"})
                raise HTTPException(status_code=502, detail="Sender bank unreachable")

            # ----------------------------------------------------------------
            # SAGA STEP 2: CREDIT RECEIVER
            # ----------------------------------------------------------------
            await _saga_update(txn_id, "CREDIT_PENDING")
            credit_confirmed = False

            try:
                credit_res = await client.post(
                    f"{receiver_url}/credit",
                    json={
                        "vpa": payload.receiver_vpa,
                        "amount": payload.amount,
                        "txn_id": txn_id,
                        "operation_type": "CREDIT",
                    }
                )
                credit_res.raise_for_status()
                credit_confirmed = True
                logger.info("Credit completed successfully", extra={"txn_id": txn_id, "event": "CREDIT_COMPLETED"})

            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                logger.warning("Credit failed or timed out. Querying bank.", extra={"txn_id": txn_id, "event": "CREDIT_NETWORK_FAILURE"}, exc_info=True)
                # Credit failed or timed out. Query the bank before compensating.
                credit_status = await _query_bank_transaction(
                    client, receiver_url, txn_id, "CREDIT"
                )

                if credit_status is True:
                    # Lost response: credit succeeded, network dropped the reply.
                    credit_confirmed = True
                    payflow_lost_response_recoveries_total.inc()

                elif credit_status is False:
                    # Confirmed not executed: safe to compensate.
                    pass

                else:
                    # Outcome unknown: do NOT compensate.
                    await _saga_update(txn_id, "INDETERMINATE",
                        "Credit outcome unknown after receiver bank query failure")
                    await publish_event("payment_events", txn_id, "PAYMENT_INDETERMINATE", {
                        "reason": "Credit outcome unknown after receiver bank query failure",
                    })
                    raise HTTPException(
                        status_code=500,
                        detail="Payment state is indeterminate. Do not retry without manual review."
                    )

            if credit_confirmed:
                # ----------------------------------------------------------------
                # SUCCESS
                # ----------------------------------------------------------------
                await _saga_update(txn_id, "COMPLETED")
                logger.info("Payment fully completed", extra={"txn_id": txn_id, "event": "PAYMENT_SUCCESS"})
                await publish_event("payment_events", txn_id, "PAYMENT_SUCCESS", {"status": "completed"})
                payflow_transactions_total.labels(status='success').inc()

                success_response = {
                    "status": "SUCCESS",
                    "txn_id": txn_id,
                    "message": f"Successfully transferred ₹{payload.amount}",
                    "idempotency_key": idemp_key
                }
                # Cache the successful result so future duplicates get the same response
                try:
                    await redis_conn.set(
                        f"idemp:{idemp_key}",
                        json.dumps(success_response),
                        ex=IDEMP_TTL_SECONDS
                    )
                except Exception:
                    pass  # Cache write failure is non-fatal for a completed payment

                return success_response

            # ----------------------------------------------------------------
            # SAGA STEP 3: COMPENSATION
            # Credit confirmed not executed; refund sender.
            # ----------------------------------------------------------------
            await _saga_update(txn_id, "COMPENSATING", "Receiver credit failed")
            comp_success = await _execute_compensation(
                client, sender_url, payload.sender_vpa, payload.amount, txn_id
            )

            if comp_success:
                await _saga_update(txn_id, "COMPENSATED")
                logger.info("Compensation successful. Sender refunded.", extra={"txn_id": txn_id, "event": "PAYMENT_COMPENSATED"})
                payflow_compensations_total.labels(outcome='success').inc()
                await publish_event("payment_events", txn_id, "PAYMENT_COMPENSATED", {
                    "reason": "Receiver credit failed, sender refunded"
                })
                raise HTTPException(
                    status_code=500,
                    detail="Receiver bank failed. Payment reversed and sender refunded."
                )
            else:
                await _saga_update(txn_id, "COMPENSATION_FAILED",
                    "Both credit and compensation failed — manual intervention required")
                payflow_compensations_total.labels(outcome='failed').inc()
                await publish_event("payment_events", txn_id, "COMPENSATION_FAILED", {
                    "reason": "Both credit and compensation failed",
                    "sender": payload.sender_vpa,
                    "receiver": payload.receiver_vpa,
                    "amount": payload.amount,
                })
                raise HTTPException(
                    status_code=500,
                    detail="Critical: payment reversal also failed. Manual intervention required."
                )

    except HTTPException as e:
        payflow_transactions_total.labels(status='failed').inc()
        raise e

    except Exception as e:
        payflow_transactions_total.labels(status='error').inc()
        raise e

    finally:
        # Always measure latency and free up the active request gauge.
        # The finally block guarantees this runs even if the request crashes halfway through.
        duration = time.time() - start_time
        payflow_transaction_duration_seconds.observe(duration)
        payflow_active_requests.dec()