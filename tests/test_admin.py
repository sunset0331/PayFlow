import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock
import uuid

# Set environment before importing
import os
os.environ["ADMIN_TOKEN"] = "supersecretadmin"

from services.gateway import main
main.ADMIN_TOKEN = "supersecretadmin"
from services.gateway.main import app

client = TestClient(app)

@pytest.fixture
def mock_db_pool():
    pool = MagicMock()
    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.fetchrow = AsyncMock()
    conn.fetch = AsyncMock()
    
    # Mock pool.acquire() as an async context manager
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=None)
    pool.acquire.return_value = acquire_ctx
    
    # Mock conn.transaction() as an async context manager
    tx_ctx = MagicMock()
    tx_ctx.__aenter__ = AsyncMock(return_value=None)
    tx_ctx.__aexit__ = AsyncMock(return_value=None)
    conn.transaction.return_value = tx_ctx
    
    app.state.db_pool = pool
    return conn

@pytest.fixture
def auth_headers():
    return {"X-Admin-Token": "supersecretadmin"}

def test_unauthorized_rejects_missing_token():
    response = client.get("/admin/sagas/indeterminate")
    assert response.status_code == 401

def test_unauthorized_rejects_wrong_token():
    response = client.get("/admin/sagas/indeterminate", headers={"X-Admin-Token": "wrong"})
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_list_indeterminate(mock_db_pool, auth_headers):
    mock_db_pool.fetch.return_value = [
        {"txn_id": uuid.uuid4(), "sender_vpa": "a@hdfc", "receiver_vpa": "b@sbi", "amount": 100.0, "state": "INDETERMINATE", "updated_at": "2026-08-28T00:00:00Z", "error_reason": "timeout"}
    ]
    response = client.get("/admin/sagas/indeterminate", headers=auth_headers)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["state"] == "INDETERMINATE"

@pytest.mark.asyncio
async def test_get_admin_saga(mock_db_pool, auth_headers):
    txn_id = str(uuid.uuid4())
    mock_db_pool.fetchrow.return_value = {
        "txn_id": uuid.UUID(txn_id),
        "state": "INDETERMINATE"
    }
    response = client.get(f"/admin/sagas/{txn_id}", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["txn_id"] == txn_id
    assert response.json()["state"] == "INDETERMINATE"

@pytest.mark.asyncio
@patch("services.gateway.main._get_routing_url")
@patch("httpx.AsyncClient.get")
async def test_resolve_debit_success_credit_success_to_completed(mock_get, mock_routing, mock_db_pool, auth_headers):
    mock_routing.return_value = "http://mockbank"
    txn_id = str(uuid.uuid4())
    mock_db_pool.fetchrow.return_value = {
        "txn_id": uuid.UUID(txn_id),
        "sender_vpa": "a@hdfc",
        "receiver_vpa": "b@sbi",
        "amount": 100.0,
        "state": "INDETERMINATE"
    }
    mock_db_pool.execute.return_value = "UPDATE 1"
    
    # Both banks return 200 SUCCESS
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    
    response = client.post(f"/admin/sagas/{txn_id}/resolve", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["new_state"] == "COMPLETED"
    
    # Verify audit log insert
    calls = mock_db_pool.execute.call_args_list
    assert "INSERT INTO admin_audit_log" in calls[0][0][0]
    assert "COMPLETED" in calls[0][0][3]

@pytest.mark.asyncio
@patch("services.gateway.main._get_routing_url")
@patch("httpx.AsyncClient.get")
async def test_resolve_debit_not_found_to_failed(mock_get, mock_routing, mock_db_pool, auth_headers):
    mock_routing.return_value = "http://mockbank"
    txn_id = str(uuid.uuid4())
    mock_db_pool.fetchrow.return_value = {
        "txn_id": uuid.UUID(txn_id),
        "sender_vpa": "a@hdfc",
        "receiver_vpa": "b@sbi",
        "amount": 100.0,
        "state": "INDETERMINATE"
    }
    mock_db_pool.execute.return_value = "UPDATE 1"
    
    # Sender bank returns 404
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response
    
    response = client.post(f"/admin/sagas/{txn_id}/resolve", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["new_state"] == "FAILED"

@pytest.mark.asyncio
@patch("services.gateway.main._get_routing_url")
@patch("httpx.AsyncClient.get")
async def test_resolve_debit_success_credit_not_found_to_compensating(mock_get, mock_routing, mock_db_pool, auth_headers):
    mock_routing.return_value = "http://mockbank"
    txn_id = str(uuid.uuid4())
    mock_db_pool.fetchrow.return_value = {
        "txn_id": uuid.UUID(txn_id),
        "sender_vpa": "a@hdfc",
        "receiver_vpa": "b@sbi",
        "amount": 100.0,
        "state": "INDETERMINATE"
    }
    mock_db_pool.execute.return_value = "UPDATE 1"
    
    # First call (debit) returns 200, second call (credit) returns 404
    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_404 = MagicMock()
    resp_404.status_code = 404
    mock_get.side_effect = [resp_200, resp_404]
    
    response = client.post(f"/admin/sagas/{txn_id}/resolve", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["new_state"] == "COMPENSATING"
    
    # Check that outbox event was inserted
    calls = mock_db_pool.execute.call_args_list
    assert any("INSERT INTO outbox_events" in call[0][0] for call in calls)

@pytest.mark.asyncio
@patch("services.gateway.main._get_routing_url")
@patch("httpx.AsyncClient.get")
async def test_resolve_banks_unavailable_remains_indeterminate(mock_get, mock_routing, mock_db_pool, auth_headers):
    mock_routing.return_value = "http://mockbank"
    txn_id = str(uuid.uuid4())
    mock_db_pool.fetchrow.return_value = {
        "txn_id": uuid.UUID(txn_id),
        "sender_vpa": "a@hdfc",
        "receiver_vpa": "b@sbi",
        "amount": 100.0,
        "state": "INDETERMINATE"
    }
    
    # Network error / timeout
    mock_get.side_effect = Exception("Timeout")
    
    response = client.post(f"/admin/sagas/{txn_id}/resolve", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["new_state"] == "INDETERMINATE"

@pytest.mark.asyncio
async def test_resolve_non_indeterminate_rejected(mock_db_pool, auth_headers):
    txn_id = str(uuid.uuid4())
    mock_db_pool.fetchrow.return_value = {
        "txn_id": uuid.UUID(txn_id),
        "state": "COMPLETED"
    }
    response = client.post(f"/admin/sagas/{txn_id}/resolve", headers=auth_headers)
    assert response.status_code == 400
    assert "not INDETERMINATE" in response.json()["detail"]
