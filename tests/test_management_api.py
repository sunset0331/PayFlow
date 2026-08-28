import pytest
import httpx
from decimal import Decimal
from unittest.mock import AsyncMock

pytestmark = pytest.mark.asyncio

async def test_bank_account_creation(hdfc_client, mock_asyncpg):
    """Test creating an account in HDFC bank."""
    payload = {
        "vpa": "newuser@hdfc",
        "user_name": "New User",
        "initial_balance": 5000.0
    }
    
    # Let's mock execute for the INSERT
    mock_asyncpg.execute = AsyncMock(return_value="INSERT 0 1")
    
    response = await hdfc_client.post("/accounts", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert data["vpa"] == "newuser@hdfc"
    assert "account_id" in data

    # Verify DB execute was called
    # mock_asyncpg is the pool, we need to check if the connection's execute was called.
    # Actually mock_asyncpg in conftest is just the pool. The conn is pool.acquire().__aenter__.return_value
    conn = await mock_asyncpg.acquire().__aenter__()
    assert conn.execute.called

async def test_gateway_vpa_registration(gateway_client, mock_asyncpg):
    """Test registering a VPA in the gateway."""
    payload = {
        "vpa": "newuser@hdfc",
        "bank_service_url": "http://127.0.0.1:8001",
        "account_id": "00000000-0000-0000-0000-000000000000"
    }

    conn = await mock_asyncpg.acquire().__aenter__()
    conn.execute = AsyncMock(return_value="INSERT 0 1")

    response = await gateway_client.post("/vpa", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"

    assert conn.execute.called

async def test_get_vpa(gateway_client, mock_asyncpg):
    """Test retrieving a VPA from the gateway."""
    conn = await mock_asyncpg.acquire().__aenter__()
    conn.fetchrow = AsyncMock(return_value={
        "bank_service_url": "http://127.0.0.1:8001",
        "is_active": True
    })

    response = await gateway_client.get("/vpa/testuser@hdfc")
    assert response.status_code == 200
    data = response.json()
    assert data["bank_service_url"] == "http://127.0.0.1:8001"
    assert data["is_active"] is True
