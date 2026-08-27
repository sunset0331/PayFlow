from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg
import asyncpg.exceptions
import os
import uuid
from decimal import Decimal
from typing import Optional
from shared.logger import get_logger

logger = get_logger("bank_sbi")
app = FastAPI(title="SBI Bank Service")
DB_URL = os.getenv("DATABASE_URL", "postgresql://payflow_admin:secretpassword@localhost:5432/db_bank_sbi")

class TransactionPayload(BaseModel):
    vpa: str
    amount: Decimal
    txn_id: str
    # operation_type lets the gateway distinguish DEBIT from COMPENSATION credits.
    # Defaults preserve backward compatibility if caller omits the field.
    operation_type: Optional[str] = None  # will be overridden per endpoint

@app.on_event("startup")
async def startup():
    # Connection pooling prevents opening a new TCP connection to Postgres per request
    app.state.pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)

# ---------------------------------------------------------------------------
# Internal helper: idempotent transaction record
# ---------------------------------------------------------------------------

async def _record_transaction_idempotent(
    conn: asyncpg.Connection,
    txn_id: str,
    operation_type: str,
    vpa: str,
    amount: Decimal,
) -> bool:
    """
    Attempt to insert a transaction record using the idempotency constraint
    UNIQUE(txn_id, operation_type).

    Returns:
        True  — record was newly inserted (this is the first execution)
        False — record already existed (this is a duplicate/retry)
    """
    result = await conn.fetchrow(
        """
        INSERT INTO transactions (txn_id, operation_type, amount, vpa, status)
        VALUES ($1, $2, $3, $4, 'SUCCESS')
        ON CONFLICT (txn_id, operation_type) DO NOTHING
        RETURNING id
        """,
        uuid.UUID(txn_id), operation_type, amount, vpa
    )
    return result is not None  # True = newly inserted, False = conflict (duplicate)

# ---------------------------------------------------------------------------
# POST /debit
# ---------------------------------------------------------------------------

@app.post("/debit")
async def debit_account(payload: TransactionPayload):
    """
    Debit the sender's account.

    Idempotency: if the same (txn_id, 'DEBIT') pair is received again, the
    conflict is silently resolved and the original result is returned — the
    balance is NOT debited a second time.
    """
    op = payload.operation_type or "DEBIT"
    logger.info("Received debit request", extra={"txn_id": payload.txn_id, "event": "DEBIT_REQUEST_RECEIVED", "amount": float(payload.amount), "vpa": payload.vpa})

    async with app.state.pool.acquire() as connection:
        # First: check whether this operation was already processed.
        # We do this BEFORE acquiring the FOR UPDATE lock to avoid holding
        # the lock longer than necessary for the common retry case.
        existing = await connection.fetchrow(
            "SELECT id FROM transactions WHERE txn_id = $1 AND operation_type = $2",
            uuid.UUID(payload.txn_id), op
        )
        if existing:
            # Already processed: fetch current balance and return the prior result.
            account = await connection.fetchrow(
                "SELECT balance FROM accounts WHERE vpa = $1", payload.vpa
            )
            result = {
                "status": "SUCCESS",
                "new_balance": float(account['balance']) if account else None,
                "idempotent": True,  # signals to caller this was a duplicate
            }
            logger.info("Debit request already processed", extra={"txn_id": payload.txn_id, "event": "DEBIT_IDEMPOTENT_HIT"})
            return result

        async with connection.transaction():
            # SELECT FOR UPDATE: serialises concurrent debits on the same account.
            account = await connection.fetchrow(
                "SELECT balance FROM accounts WHERE vpa = $1 FOR UPDATE", payload.vpa
            )

            if not account:
                logger.warning("Account not found", extra={"txn_id": payload.txn_id, "event": "DEBIT_FAILED", "reason": "Account not found"})
                raise HTTPException(status_code=404, detail="Account not found")
            if account['balance'] < payload.amount:
                logger.warning("Insufficient funds", extra={"txn_id": payload.txn_id, "event": "DEBIT_FAILED", "reason": "Insufficient funds"})
                raise HTTPException(status_code=400, detail="Insufficient funds")

            new_balance = account['balance'] - payload.amount
            await connection.execute(
                "UPDATE accounts SET balance = $1 WHERE vpa = $2", new_balance, payload.vpa
            )

            inserted = await _record_transaction_idempotent(
                connection, payload.txn_id, op, payload.vpa, payload.amount
            )
            if not inserted:
                logger.warning("Lost race to concurrent request", extra={"txn_id": payload.txn_id, "event": "DEBIT_CONCURRENT_RACE_LOST"})
                raise asyncpg.exceptions.RaiseError("concurrent_duplicate")

            logger.info("Debit successful", extra={"txn_id": payload.txn_id, "event": "DEBIT_COMMITTED"})
            return {"status": "SUCCESS", "new_balance": float(new_balance)}

# ---------------------------------------------------------------------------
# POST /credit
# ---------------------------------------------------------------------------

@app.post("/credit")
async def credit_account(payload: TransactionPayload):
    """
    Credit the receiver's account (or the sender's account for compensation).

    operation_type should be:
      - 'CREDIT'       for a normal receive
      - 'COMPENSATION' for a saga rollback credit back to the sender

    Idempotency: if the same (txn_id, operation_type) pair arrives again,
    the credit is NOT applied a second time.
    """
    op = payload.operation_type or "CREDIT"
    logger.info("Received credit request", extra={"txn_id": payload.txn_id, "event": "CREDIT_REQUEST_RECEIVED", "amount": float(payload.amount), "vpa": payload.vpa, "operation_type": op})

    async with app.state.pool.acquire() as connection:
        # Pre-check for existing record before acquiring the row lock
        existing = await connection.fetchrow(
            "SELECT id FROM transactions WHERE txn_id = $1 AND operation_type = $2",
            uuid.UUID(payload.txn_id), op
        )
        if existing:
            account = await connection.fetchrow(
                "SELECT balance FROM accounts WHERE vpa = $1", payload.vpa
            )
            result = {
                "status": "SUCCESS",
                "new_balance": float(account['balance']) if account else None,
                "idempotent": True,
            }
            logger.info("Credit request already processed", extra={"txn_id": payload.txn_id, "event": "CREDIT_IDEMPOTENT_HIT"})
            return result

        async with connection.transaction():
            account = await connection.fetchrow(
                "SELECT balance FROM accounts WHERE vpa = $1 FOR UPDATE", payload.vpa
            )
            if not account:
                logger.warning("Account not found", extra={"txn_id": payload.txn_id, "event": "CREDIT_FAILED", "reason": "Account not found"})
                raise HTTPException(status_code=404, detail="Account not found")

            new_balance = account['balance'] + payload.amount
            await connection.execute(
                "UPDATE accounts SET balance = $1 WHERE vpa = $2", new_balance, payload.vpa
            )

            inserted = await _record_transaction_idempotent(
                connection, payload.txn_id, op, payload.vpa, payload.amount
            )
            if not inserted:
                logger.warning("Lost race to concurrent request", extra={"txn_id": payload.txn_id, "event": "CREDIT_CONCURRENT_RACE_LOST"})
                raise asyncpg.exceptions.RaiseError("concurrent_duplicate")

            logger.info("Credit successful", extra={"txn_id": payload.txn_id, "event": "CREDIT_COMMITTED"})
            return {"status": "SUCCESS", "new_balance": float(new_balance)}

# ---------------------------------------------------------------------------
# GET /transaction/{txn_id} — gateway query endpoint
# ---------------------------------------------------------------------------

@app.get("/transaction/{txn_id}")
async def get_transaction(txn_id: str, operation: str = "CREDIT"):
    """
    Query whether a specific operation for a txn_id was executed.

    The gateway calls this after a network timeout to determine the true
    outcome before deciding whether to compensate.

    Returns:
        200 { status: "SUCCESS", ... } — operation was executed
        404                            — operation was NOT executed (safe to retry or compensate)
    """
    async with app.state.pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT id, operation_type, amount, vpa, status, created_at "
            "FROM transactions WHERE txn_id = $1 AND operation_type = $2",
            uuid.UUID(txn_id), operation
        )
        if not row:
            raise HTTPException(status_code=404, detail="Transaction not found")
        return {
            "txn_id": txn_id,
            "operation_type": row['operation_type'],
            "amount": float(row['amount']),
            "vpa": row['vpa'],
            "status": row['status'],
            "created_at": row['created_at'].isoformat(),
        }
