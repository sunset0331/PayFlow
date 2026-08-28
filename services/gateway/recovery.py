"""
Crash Recovery Worker for PayFlow Gateway.

This module runs as a background asyncio task inside the Gateway process.
It polls the saga_transactions table for sagas that are stuck in transient
states (DEBIT_PENDING, CREDIT_PENDING, COMPENSATING) for longer than
SAGA_STALE_THRESHOLD_SECONDS and attempts to resolve them.

Why this works safely:
  - Bank-side idempotency (UNIQUE(txn_id, operation_type)) ensures that
    repeating a bank call for an already-completed operation is a no-op.
  - The GET /transaction/{txn_id} query endpoint lets us check the actual
    bank state before taking any action.
  - Saga state updates are durable in PostgreSQL; the recovery worker never
    guesses — it only acts on confirmed bank state.

Recovery rules:
  DEBIT_PENDING   -> query sender bank for DEBIT
                     200 = debit completed, advance to DEBIT_COMPLETED
                     404 = debit not executed, mark FAILED
                     none = leave for next poll cycle

  CREDIT_PENDING  -> query receiver bank for CREDIT
                     200 = credit completed (lost response), mark COMPLETED
                     404 = credit not executed, compensate sender
                     none = leave for next poll cycle

  COMPENSATING    -> query sender bank for COMPENSATION
                     200 = compensation completed, mark COMPENSATED
                     404 = compensation not executed, retry compensation
                     none = leave for next poll cycle
"""

import asyncio
import logging
import httpx
import uuid
import time
import os
from typing import Optional
from prometheus_client import Counter, Histogram, Gauge
from shared.logger import get_logger

logger = get_logger("recovery")

gateway_recovery_total = Counter('gateway_recovery_total', 'Total recovery attempts', ['status', 'saga_state'])
gateway_recovery_duration_seconds = Histogram('gateway_recovery_duration_seconds', 'Latency of recovery scans')
gateway_stale_sagas = Gauge('gateway_stale_sagas', 'Number of stale sagas found in the last scan')

# How long a saga must be stuck before the recovery worker intervenes (seconds)
SAGA_STALE_THRESHOLD_SECONDS = 120

# How often to run the recovery scan (seconds)
RECOVERY_POLL_INTERVAL_SECONDS = 15

# Timeout for bank queries inside the recovery worker
BANK_QUERY_TIMEOUT_SECONDS = 5.0


async def _query_bank(
    bank_url: str,
    txn_id: str,
    operation: str,
) -> Optional[bool]:
    """
    Query a bank to confirm whether a specific operation was executed.

    Returns:
        True  — confirmed executed
        False — confirmed NOT executed
        None  — bank unreachable or returned unexpected status
    """
    try:
        async with httpx.AsyncClient(timeout=BANK_QUERY_TIMEOUT_SECONDS) as client:
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


async def _credit_sender(
    bank_url: str,
    sender_vpa: str,
    amount: float,
    txn_id: str,
) -> bool:
    """Attempt to credit (compensate) the sender. Returns True on success."""
    try:
        async with httpx.AsyncClient(timeout=BANK_QUERY_TIMEOUT_SECONDS) as client:
            resp = await client.post(
                f"{bank_url}/credit",
                json={
                    "vpa": sender_vpa,
                    "amount": amount,
                    "txn_id": txn_id,
                    "operation_type": "COMPENSATION",
                },
            )
            resp.raise_for_status()
            return True
    except (httpx.RequestError, httpx.HTTPStatusError):
        return False


async def run_recovery_worker(db_pool, kafka_publish_fn) -> None:
    """
    Main recovery loop. Runs indefinitely; designed to be started as an
    asyncio background task inside the Gateway process.

    Args:
        db_pool: asyncpg connection pool for db_gateway
        kafka_publish_fn: async function to publish Kafka events
    """
    logger.info("Recovery worker started. Polling every %ds for stale sagas.", RECOVERY_POLL_INTERVAL_SECONDS)

    while True:
        try:
            await _recovery_scan(db_pool, kafka_publish_fn)
        except Exception as e:
            logger.error("Recovery worker scan failed: %s", e, exc_info=True)
        finally:
            await asyncio.sleep(RECOVERY_POLL_INTERVAL_SECONDS)


async def _recovery_scan(db_pool, kafka_publish_fn) -> None:
    """Single scan of stale sagas."""
    async with db_pool.acquire() as conn:
        stale_sagas = await conn.fetch(
            """
            SELECT txn_id, sender_vpa, receiver_vpa, amount, state
            FROM saga_transactions
            WHERE state IN ('DEBIT_PENDING', 'DEBIT_COMPLETED', 'CREDIT_PENDING', 'COMPENSATING')
              AND updated_at < NOW() - INTERVAL '%s seconds'
            ORDER BY updated_at ASC
            LIMIT 50
            FOR UPDATE SKIP LOCKED
            """ % SAGA_STALE_THRESHOLD_SECONDS
        )

    gateway_stale_sagas.set(len(stale_sagas) if stale_sagas else 0)

    if not stale_sagas:
        return

    logger.info("Recovery worker found %d stale saga(s).", len(stale_sagas), extra={"event": "RECOVERY_SCAN", "stale_count": len(stale_sagas)})

    for row in stale_sagas:
        txn_id = str(row['txn_id'])
        state = row['state']
        sender_vpa = row['sender_vpa']
        receiver_vpa = row['receiver_vpa']
        amount = float(row['amount'])

        sender_bank = sender_vpa.split("@")[1]
        receiver_bank = receiver_vpa.split("@")[1]
        
        # Hardcoded fallback for tests and docker networking
        BANK_URLS_FALLBACK = {
            "hdfc": os.getenv("HDFC_BANK_URL", "http://bank-hdfc:8001"),
            "sbi": os.getenv("SBI_BANK_URL", "http://bank-sbi:8002")
        }

        # Resolve sender URL
        sender_row = await conn.fetchrow("SELECT bank_service_url FROM vpa_registry WHERE vpa = $1", sender_vpa)
        sender_url = sender_row["bank_service_url"] if sender_row else BANK_URLS_FALLBACK.get(sender_bank)

        # Resolve receiver URL
        receiver_row = await conn.fetchrow("SELECT bank_service_url FROM vpa_registry WHERE vpa = $1", receiver_vpa)
        receiver_url = receiver_row["bank_service_url"] if receiver_row else BANK_URLS_FALLBACK.get(receiver_bank)

        logger.info("Recovering saga", extra={"txn_id": txn_id, "saga_state": state, "event": "RECOVERY_INITIATED"})

        start_time = time.time()
        try:
            if state == "DEBIT_PENDING":
                await _recover_debit_pending(
                    txn_id, sender_url, sender_vpa, amount, db_pool, kafka_publish_fn
                )

            elif state == "DEBIT_COMPLETED":
                await _recover_debit_completed(
                    txn_id, sender_url, sender_vpa, receiver_url, receiver_vpa, amount, db_pool, kafka_publish_fn
                )

            elif state == "CREDIT_PENDING":
                await _recover_credit_pending(
                    txn_id, sender_url, sender_vpa, receiver_url, amount, db_pool, kafka_publish_fn
                )

            elif state == "COMPENSATING":
                await _recover_compensating(
                    txn_id, sender_url, sender_vpa, amount, db_pool, kafka_publish_fn
                )

        except Exception as e:
            gateway_recovery_total.labels(status="failed", saga_state=state).inc()
            logger.error("Recovery failed", extra={"txn_id": txn_id, "event": "RECOVERY_ERROR"}, exc_info=True)
        finally:
            gateway_recovery_duration_seconds.observe(time.time() - start_time)


async def _update_saga(db_pool, txn_id: str, state: str, error_reason: str = None) -> None:
    """Update saga state in the database."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE saga_transactions SET state = $1, error_reason = $2, updated_at = NOW() WHERE txn_id = $3",
            state, error_reason, uuid.UUID(txn_id)
        )


async def _recover_debit_pending(
    txn_id: str, sender_url: str, sender_vpa: str, amount: float,
    db_pool, kafka_publish_fn
) -> None:
    """
    DEBIT_PENDING: the debit request was sent but we crashed before getting a response.
    Query the bank to find out what actually happened.
    """
    if not sender_url:
        logger.warning("No URL for sender bank in txn_id=%s — skipping.", txn_id)
        return

    debit_status = await _query_bank(sender_url, txn_id, "DEBIT")

    if debit_status is True:
        # Debit was completed successfully before the crash
        logger.info("DEBIT confirmed completed. Advancing to DEBIT_COMPLETED.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
        gateway_recovery_total.labels(status="success", saga_state="DEBIT_PENDING").inc()
        await _update_saga(db_pool, txn_id, "DEBIT_COMPLETED")
        # Note: CREDIT_PENDING will be handled in the next scan cycle

    elif debit_status is False:
        # Debit was never executed — safe to mark as FAILED
        logger.info("DEBIT confirmed not executed. Marking FAILED.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
        gateway_recovery_total.labels(status="success", saga_state="DEBIT_PENDING").inc()
        await _update_saga(db_pool, txn_id, "FAILED", "Recovery: debit confirmed not executed")
        await kafka_publish_fn("payment_events", txn_id, "PAYMENT_FAILED",
                               {"reason": "Recovery: debit confirmed not executed"})

    else:
        # Bank unreachable — leave for next cycle, bump updated_at to avoid busy-loop
        gateway_recovery_total.labels(status="retrying", saga_state="DEBIT_PENDING").inc()
        logger.warning("DEBIT state unknown, bank unreachable. Retrying next cycle.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})


async def _recover_debit_completed(
    txn_id: str, sender_url: str, sender_vpa: str,
    receiver_url: str, receiver_vpa: str, amount: float, db_pool, kafka_publish_fn
) -> None:
    """
    DEBIT_COMPLETED: debit succeeded, but we crashed before sending the CREDIT.
    (Or maybe we sent it but crashed before CREDIT_PENDING transition).
    """
    if not receiver_url:
        logger.warning("No URL for receiver bank in txn_id=%s — skipping.", txn_id)
        return

    credit_status = await _query_bank(receiver_url, txn_id, "CREDIT")

    if credit_status is True:
        # Credit completed — we sent it but crashed before state update
        logger.info("CREDIT confirmed completed. Marking COMPLETED.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
        gateway_recovery_total.labels(status="success", saga_state="DEBIT_COMPLETED").inc()
        await _update_saga(db_pool, txn_id, "COMPLETED")
        await kafka_publish_fn("payment_events", txn_id, "PAYMENT_SUCCESS",
                               {"status": "completed", "recovered": True})

    elif credit_status is False:
        # Credit not executed — we need to send it now
        logger.info("CREDIT not executed. Sending credit now.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
        
        # Advance to CREDIT_PENDING so next cycle will handle it properly if we crash during HTTP
        await _update_saga(db_pool, txn_id, "CREDIT_PENDING")
        
        try:
            async with httpx.AsyncClient(timeout=BANK_QUERY_TIMEOUT_SECONDS) as client:
                resp = await client.post(
                    f"{receiver_url}/credit",
                    json={
                        "vpa": receiver_vpa,
                        "amount": amount,
                        "txn_id": txn_id,
                        "operation_type": "CREDIT",
                    },
                )
                resp.raise_for_status()
                
            logger.info("CREDIT successful. Marking COMPLETED.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
            await _update_saga(db_pool, txn_id, "COMPLETED")
            await kafka_publish_fn("payment_events", txn_id, "PAYMENT_SUCCESS",
                                   {"status": "completed", "recovered": True})
                                   
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            gateway_recovery_total.labels(status="retrying", saga_state="DEBIT_COMPLETED").inc()
            logger.warning("CREDIT attempt failed. Leaving for next cycle.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"}, exc_info=True)

    else:
        # Receiver bank unreachable
        gateway_recovery_total.labels(status="retrying", saga_state="DEBIT_COMPLETED").inc()
        logger.warning("CREDIT state unknown, bank unreachable. Retrying next cycle.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})


async def _recover_credit_pending(
    txn_id: str, sender_url: str, sender_vpa: str,
    receiver_url: str, amount: float, db_pool, kafka_publish_fn
) -> None:
    """
    CREDIT_PENDING: debit succeeded, credit request was sent but we crashed.
    Query the receiver bank to find out the actual credit state.
    """
    if not receiver_url:
        logger.warning("No URL for receiver bank in txn_id=%s — skipping.", txn_id)
        return

    credit_status = await _query_bank(receiver_url, txn_id, "CREDIT")

    if credit_status is True:
        # Credit completed — this is the classic lost-response scenario
        logger.info("CREDIT confirmed completed. Marking COMPLETED.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
        gateway_recovery_total.labels(status="success", saga_state="CREDIT_PENDING").inc()
        await _update_saga(db_pool, txn_id, "COMPLETED")
        await kafka_publish_fn("payment_events", txn_id, "PAYMENT_SUCCESS",
                               {"status": "completed", "recovered": True})

    elif credit_status is False:
        # Credit not executed — safe to compensate
        logger.info("CREDIT confirmed not executed. Compensating.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
        await _update_saga(db_pool, txn_id, "COMPENSATING", "Recovery: credit not executed, compensating")

        if sender_url:
            comp_ok = await _credit_sender(sender_url, sender_vpa, amount, txn_id)
            if comp_ok:
                logger.info("Compensation successful. Marking COMPENSATED.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
                gateway_recovery_total.labels(status="success", saga_state="CREDIT_PENDING").inc()
                await _update_saga(db_pool, txn_id, "COMPENSATED")
                await kafka_publish_fn("payment_events", txn_id, "PAYMENT_COMPENSATED",
                                       {"reason": "Recovery: credit not executed, sender refunded"})
            else:
                logger.error("Compensation FAILED. Marking COMPENSATION_FAILED.", extra={"txn_id": txn_id, "event": "RECOVERY_ERROR"})
                gateway_recovery_total.labels(status="failed", saga_state="CREDIT_PENDING").inc()
                await _update_saga(db_pool, txn_id, "COMPENSATION_FAILED",
                                   "Recovery: compensation attempt failed — manual intervention required")
                await kafka_publish_fn("payment_events", txn_id, "COMPENSATION_FAILED",
                                       {"reason": "Recovery compensation failed — manual intervention required"})
        else:
            logger.error("No sender URL, cannot compensate.", extra={"txn_id": txn_id, "event": "RECOVERY_ERROR"})
            gateway_recovery_total.labels(status="failed", saga_state="CREDIT_PENDING").inc()
            await _update_saga(db_pool, txn_id, "COMPENSATION_FAILED",
                               "Recovery: no sender URL, cannot compensate — manual intervention required")

    else:
        # Receiver bank unreachable — leave for next cycle
        gateway_recovery_total.labels(status="retrying", saga_state="CREDIT_PENDING").inc()
        logger.warning("CREDIT state unknown, bank unreachable. Retrying next cycle.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})


async def _recover_compensating(
    txn_id: str, sender_url: str, sender_vpa: str, amount: float,
    db_pool, kafka_publish_fn
) -> None:
    """
    COMPENSATING: compensation request was sent but we crashed before confirmation.
    Query the sender bank to find out if it went through.
    """
    if not sender_url:
        logger.warning("No URL for sender bank in txn_id=%s — skipping.", txn_id)
        return

    comp_status = await _query_bank(sender_url, txn_id, "COMPENSATION")

    if comp_status is True:
        # Compensation completed before the crash
        logger.info("COMPENSATION confirmed completed. Marking COMPENSATED.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
        gateway_recovery_total.labels(status="success", saga_state="COMPENSATING").inc()
        await _update_saga(db_pool, txn_id, "COMPENSATED")
        await kafka_publish_fn("payment_events", txn_id, "PAYMENT_COMPENSATED",
                               {"reason": "Recovery: compensation confirmed completed"})

    elif comp_status is False:
        # Compensation not executed — retry it
        logger.info("COMPENSATION not executed, retrying.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
        comp_ok = await _credit_sender(sender_url, sender_vpa, amount, txn_id)
        if comp_ok:
            logger.info("Compensation retry successful.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
            gateway_recovery_total.labels(status="success", saga_state="COMPENSATING").inc()
            await _update_saga(db_pool, txn_id, "COMPENSATED")
            await kafka_publish_fn("payment_events", txn_id, "PAYMENT_COMPENSATED",
                                   {"reason": "Recovery: compensation completed on retry"})
        else:
            logger.error("Compensation retry FAILED.", extra={"txn_id": txn_id, "event": "RECOVERY_ERROR"})
            gateway_recovery_total.labels(status="failed", saga_state="COMPENSATING").inc()
            await _update_saga(db_pool, txn_id, "COMPENSATION_FAILED",
                               "Recovery: compensation retry failed — manual intervention required")
            await kafka_publish_fn("payment_events", txn_id, "COMPENSATION_FAILED",
                                   {"reason": "Recovery compensation retry failed"})

    else:
        gateway_recovery_total.labels(status="retrying", saga_state="COMPENSATING").inc()
        logger.warning("COMPENSATION state unknown. Retrying next cycle.", extra={"txn_id": txn_id, "event": "RECOVERY_ACTION"})
