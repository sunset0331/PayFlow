from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg
import os
import uuid
from decimal import Decimal

app = FastAPI(title="HDFC Bank Service")
DB_URL = os.getenv("DATABASE_URL", "postgresql://payflow_admin:secretpassword@localhost:5432/db_bank_hdfc")

class TransactionPayload(BaseModel):
    vpa: str
    amount: Decimal
    txn_id: str

@app.on_event("startup")
async def startup():
    # Connection pooling prevents opening a new TCP connection to Postgres per request
    app.state.pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)

@app.post("/debit")
async def debit_account(payload: TransactionPayload):
    async with app.state.pool.acquire() as connection:
        async with connection.transaction(): # Begins a local DB transaction
            # RESUME METRIC/INTERVIEW POINT: SELECT FOR UPDATE
            # This locks the specific row. If two concurrent requests try to debit 
            # utkarsh@hdfc at the exact same millisecond, Postgres forces one to wait.
            # This mathematically prevents the double-spend problem.
            account = await connection.fetchrow(
                "SELECT balance FROM accounts WHERE vpa = $1 FOR UPDATE", payload.vpa
            )
            
            if not account:
                raise HTTPException(status_code=404, detail="Account not found")
            if account['balance'] < payload.amount:
                raise HTTPException(status_code=400, detail="Insufficient funds")

            new_balance = account['balance'] - payload.amount
            await connection.execute(
                "UPDATE accounts SET balance = $1 WHERE vpa = $2", new_balance, payload.vpa
            )
            
            # Record local transaction
            await connection.execute(
                "INSERT INTO transactions (txn_id, amount, type, status) VALUES ($1, $2, 'DEBIT', 'SUCCESS')",
                uuid.UUID(payload.txn_id), payload.amount
            )
            return {"status": "SUCCESS", "new_balance": float(new_balance)}

@app.post("/credit")
async def credit_account(payload: TransactionPayload):
    # Used for receiving money AND for Saga Compensation
    async with app.state.pool.acquire() as connection:
        async with connection.transaction():
            account = await connection.fetchrow(
                "SELECT balance FROM accounts WHERE vpa = $1 FOR UPDATE", payload.vpa
            )
            if not account:
                raise HTTPException(status_code=404, detail="Account not found")

            new_balance = account['balance'] + payload.amount
            await connection.execute(
                "UPDATE accounts SET balance = $1 WHERE vpa = $2", new_balance, payload.vpa
            )
            
            await connection.execute(
                "INSERT INTO transactions (txn_id, amount, type, status) VALUES ($1, $2, 'CREDIT', 'SUCCESS')",
                uuid.UUID(payload.txn_id), payload.amount
            )
            return {"status": "SUCCESS", "new_balance": float(new_balance)}