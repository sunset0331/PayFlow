import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_hdfc_debit_success(hdfc_client: AsyncClient, mock_asyncpg):
    # Setup the mock for a successful debit
    mock_conn = mock_asyncpg.acquire.return_value.__aenter__.return_value
    mock_conn.fetchrow.return_value = {"balance": 1000.0}
    mock_conn.execute.return_value = "UPDATE 1"
    
    response = await hdfc_client.post("/debit", json={
        "vpa": "alice@hdfc",
        "amount": 100.0,
        "txn_id": "123e4567-e89b-12d3-a456-426614174000"
    })
    
    assert response.status_code == 200
    assert response.json() == {"status": "SUCCESS", "new_balance": 900.0}

@pytest.mark.asyncio
async def test_sbi_credit_success(sbi_client: AsyncClient, mock_asyncpg):
    mock_conn = mock_asyncpg.acquire.return_value.__aenter__.return_value
    mock_conn.fetchrow.return_value = {"balance": 1000.0}
    mock_conn.execute.return_value = "UPDATE 1"
    
    response = await sbi_client.post("/credit", json={
        "vpa": "bob@sbi",
        "amount": 100.0,
        "txn_id": "123e4567-e89b-12d3-a456-426614174000"
    })
    
    assert response.status_code == 200
    assert response.json() == {"status": "SUCCESS", "new_balance": 1100.0}

@pytest.mark.asyncio
async def test_hdfc_debit_insufficient_funds(hdfc_client: AsyncClient, mock_asyncpg):
    mock_conn = mock_asyncpg.acquire.return_value.__aenter__.return_value
    
    from decimal import Decimal
    async def fetchrow_override(query, *args, **kwargs):
        if "FROM transactions" in query:
            return None
        return {"balance": Decimal("50.0")} # less than 100
        
    mock_conn.fetchrow.side_effect = fetchrow_override
    
    response = await hdfc_client.post("/debit", json={
        "vpa": "alice@hdfc",
        "amount": 100.0,
        "txn_id": "123e4567-e89b-12d3-a456-426614174001"
    })
    
    assert response.status_code == 400
    assert response.json()["detail"] == "Insufficient funds"

@pytest.mark.asyncio
async def test_sbi_account_not_found(sbi_client: AsyncClient, mock_asyncpg):
    mock_conn = mock_asyncpg.acquire.return_value.__aenter__.return_value
    
    async def fetchrow_override(query, *args, **kwargs):
        if "FROM transactions" in query:
            return None
        return None # No account found
        
    mock_conn.fetchrow.side_effect = fetchrow_override
    
    response = await sbi_client.post("/credit", json={
        "vpa": "unknown@sbi",
        "amount": 100.0,
        "txn_id": "123e4567-e89b-12d3-a456-426614174002"
    })
    
    assert response.status_code == 404
    assert response.json()["detail"] == "Account not found"
