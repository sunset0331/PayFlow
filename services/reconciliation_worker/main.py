import os
import asyncio
import json
import logging
import uuid
import httpx
import asyncpg
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from shared.logger import get_logger
from prometheus_client import Counter, Histogram, Gauge, start_http_server

logger = get_logger("reconciliation_worker")

DB_URL = os.getenv("DATABASE_URL", "postgresql://payflow_admin:secretpassword@postgres:5432/db_gateway")
LEDGER_URL = os.getenv("LEDGER_URL", "http://ledger:8004")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "120"))
INTERVAL_SECONDS = int(os.getenv("RECONCILIATION_INTERVAL_SECONDS", "30"))

# Metrics
reconciliation_runs_total = Counter("reconciliation_runs_total", "Number of reconciliation batches run")
reconciliation_results_total = Counter("reconciliation_results_total", "Reconciliation results", ["status", "discrepancy_type"])
reconciliation_duration_seconds = Histogram("reconciliation_duration_seconds", "Duration of a single transaction reconciliation")
reconciliation_stale_transactions = Gauge("reconciliation_stale_transactions", "Number of transactions pending reconciliation")

async def fetch_bank_status(client: httpx.AsyncClient, bank_url: str, txn_id: str, operation: str) -> Optional[str]:
    """
    Fetch status from a bank.
    Returns:
       'SUCCESS' if executed
       'NOT_FOUND' if not executed
       'UNAVAILABLE' if network failure / timeout
    """
    try:
        resp = await client.get(f"{bank_url}/transaction/{txn_id}?operation={operation}", timeout=5.0)
        if resp.status_code == 200:
            return resp.json().get("status")
        elif resp.status_code == 404:
            return "NOT_FOUND"
        else:
            return "UNAVAILABLE"
    except (httpx.RequestError, httpx.TimeoutException):
        return "UNAVAILABLE"

async def fetch_ledger_status(client: httpx.AsyncClient, txn_id: str) -> Dict[str, bool]:
    """
    Fetch ledger events for the txn_id.
    Returns a dict mapping event_type -> exists (bool)
    Or empty dict if UNAVAILABLE (timeout)
    """
    try:
        resp = await client.get(f"{LEDGER_URL}/ledger/{txn_id}", timeout=5.0)
        if resp.status_code == 200:
            events = resp.json()
            return {e["event_type"]: True for e in events}
        elif resp.status_code == 404:
            return {"NOT_FOUND": True}
        else:
            return {"UNAVAILABLE": True}
    except (httpx.RequestError, httpx.TimeoutException):
        return {"UNAVAILABLE": True}

def evaluate_reconciliation(
    saga_state: str,
    debit_status: str,
    credit_status: str,
    ledger_events: Dict[str, bool],
    comp_status: str = "NOT_FOUND"
) -> tuple[str, str]:
    """
    Evaluate business rules to determine reconciliation result.
    Returns (reconciliation_status, discrepancy_type)
    """
    if "UNAVAILABLE" in (debit_status, credit_status, comp_status) or "UNAVAILABLE" in ledger_events:
        return "INDETERMINATE", "BANK_STATUS_UNAVAILABLE"

    if saga_state == "COMPLETED":
        if debit_status != "SUCCESS":
            return "MISMATCH", "DEBIT_MISSING"
        if credit_status != "SUCCESS":
            return "MISMATCH", "CREDIT_MISSING"
        if not ledger_events.get("PAYMENT_SUCCESS"):
            return "MISMATCH", "LEDGER_MISSING"
        return "MATCHED", "NONE"

    elif saga_state == "COMPENSATED":
        if debit_status != "SUCCESS":
            return "MISMATCH", "DEBIT_MISSING"
        if credit_status == "SUCCESS":
            return "MISMATCH", "CREDIT_EXISTS_FOR_COMPENSATED"
        if comp_status != "SUCCESS":
            return "MISMATCH", "COMPENSATION_MISSING"
        return "MATCHED", "NONE"

    elif saga_state == "FAILED":
        # Failed means it aborted BEFORE debiting
        if debit_status == "SUCCESS":
            return "MISMATCH", "DEBIT_EXISTS_FOR_FAILED"
        if credit_status == "SUCCESS":
            return "MISMATCH", "CREDIT_EXISTS_FOR_FAILED"
        return "MATCHED", "NONE"

    elif saga_state in ("INDETERMINATE", "COMPENSATION_FAILED"):
        return "INDETERMINATE", "SAGA_STUCK"

    return "INDETERMINATE", "UNKNOWN"


async def reconcile_batch():
    """Run one batch of reconciliation."""
    logger.info("Starting reconciliation batch")
    async with asyncpg.create_pool(DB_URL) as pool:
        async with pool.acquire() as conn:
            # Query for un-reconciled (or INDETERMINATE) terminal sagas
            lookback_time = datetime.utcnow() - timedelta(minutes=LOOKBACK_MINUTES)
            
            # Use SKIP LOCKED to avoid colliding with other workers
            # Re-check INDETERMINATE states to see if they resolved
            sagas = await conn.fetch(
                """
                SELECT t.txn_id, t.state, t.sender_vpa, t.receiver_vpa
                FROM saga_transactions t
                LEFT JOIN reconciliation_results r ON t.txn_id = r.txn_id
                WHERE t.state IN ('COMPLETED', 'FAILED', 'INDETERMINATE', 'COMPENSATION_FAILED', 'COMPENSATED')
                  AND t.created_at >= $1
                  AND (r.id IS NULL OR r.reconciliation_status = 'INDETERMINATE')
                ORDER BY t.created_at ASC
                LIMIT $2
                FOR UPDATE OF t SKIP LOCKED
                """, lookback_time, BATCH_SIZE
            )

            if not sagas:
                logger.info("No transactions to reconcile in this batch")
                reconciliation_stale_transactions.set(0)
                return

            reconciliation_stale_transactions.set(len(sagas))

            # Fetch VPA registry mappings
            vpa_rows = await conn.fetch("SELECT vpa, bank_service_url FROM vpa_registry")
            registry = {r["vpa"]: r["bank_service_url"] for r in vpa_rows}

            async with httpx.AsyncClient() as client:
                for saga in sagas:
                    start_time = asyncio.get_event_loop().time()
                    txn_id = str(saga["txn_id"])
                    sender_url = registry.get(saga["sender_vpa"])
                    receiver_url = registry.get(saga["receiver_vpa"])

                    if not sender_url or not receiver_url:
                        logger.error("VPA not found in registry", extra={"txn_id": txn_id})
                        continue

                    # Fetch statuses concurrently
                    debit_task = fetch_bank_status(client, sender_url, txn_id, "DEBIT")
                    credit_task = fetch_bank_status(client, receiver_url, txn_id, "CREDIT")
                    comp_task = fetch_bank_status(client, sender_url, txn_id, "COMPENSATION")
                    ledger_task = fetch_ledger_status(client, txn_id)

                    debit_status, credit_status, comp_status, ledger_events = await asyncio.gather(
                        debit_task, credit_task, comp_task, ledger_task
                    )

                    status, discrepancy = evaluate_reconciliation(
                        saga_state=saga["state"],
                        debit_status=debit_status,
                        credit_status=credit_status,
                        ledger_events=ledger_events,
                        comp_status=comp_status
                    )

                    logger.info(
                        "Reconciliation evaluated",
                        extra={
                            "txn_id": txn_id,
                            "reconciliation_status": status,
                            "discrepancy_type": discrepancy,
                            "saga_state": saga["state"]
                        }
                    )

                    reconciliation_results_total.labels(status=status, discrepancy_type=discrepancy).inc()

                    # Upsert result
                    await conn.execute(
                        """
                        INSERT INTO reconciliation_results (
                            txn_id, reconciliation_status, saga_state,
                            sender_bank_status, receiver_bank_status, ledger_status, discrepancy_type, details
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (txn_id) DO UPDATE SET
                            reconciliation_status = EXCLUDED.reconciliation_status,
                            saga_state = EXCLUDED.saga_state,
                            sender_bank_status = EXCLUDED.sender_bank_status,
                            receiver_bank_status = EXCLUDED.receiver_bank_status,
                            ledger_status = EXCLUDED.ledger_status,
                            discrepancy_type = EXCLUDED.discrepancy_type,
                            details = EXCLUDED.details,
                            last_checked_at = CURRENT_TIMESTAMP
                        """,
                        uuid.UUID(txn_id), status, saga["state"],
                        debit_status, credit_status, 
                        "OK" if ledger_events and "UNAVAILABLE" not in ledger_events else "ERROR",
                        discrepancy,
                        json.dumps(ledger_events)
                    )
                    
                    reconciliation_duration_seconds.observe(asyncio.get_event_loop().time() - start_time)
            
            reconciliation_runs_total.inc()


async def main():
    logger.info(f"Starting reconciliation worker. Interval: {INTERVAL_SECONDS}s, Batch: {BATCH_SIZE}")
    start_http_server(8006)
    while True:
        try:
            await reconcile_batch()
        except Exception as e:
            logger.error(f"Error in reconciliation batch: {e}", exc_info=True)
        
        await asyncio.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
